module Network (
    Peer(..),
    PeerAddress(..),
    startServer,
    sendToPeer,
) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M
import qualified Data.Text as T

import Network.Socket
import Network.Socket.ByteString (recvFrom, sendTo)

import Channel
import Identity
import PubKey
import Storage


discoveryPort :: ServiceName
discoveryPort = "29665"


data Peer = Peer
    { peerAddress :: PeerAddress
    , peerIdentity :: Maybe (Stored Identity)
    , peerChannels :: [Channel]
    , peerSocket :: Socket
    }
    deriving (Show)

data PeerAddress = DatagramAddress SockAddr
    deriving (Show)


data TransportHeader = AnnouncePacket Ref
                     | IdentityRequest Ref Ref
                     | IdentityResponse Ref
                     | TrChannelRequest Ref
                     | TrChannelAccept Ref

data ServiceHeader = ServiceHeader T.Text Ref

transportToObject :: TransportHeader -> Object
transportToObject = \case
    AnnouncePacket ref -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "announce")
        , (BC.pack "identity", RecRef ref)
        ]
    IdentityRequest ref from -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "idreq")
        , (BC.pack "identity", RecRef ref)
        , (BC.pack "from", RecRef from)
        ]
    IdentityResponse ref -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "idresp")
        , (BC.pack "identity", RecRef ref)
        ]
    TrChannelRequest ref -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "chreq")
        , (BC.pack "req", RecRef ref)
        ]
    TrChannelAccept ref -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "chacc")
        , (BC.pack "acc", RecRef ref)
        ]

transportFromObject :: Object -> Maybe TransportHeader
transportFromObject (Rec items)
    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "announce"
    , Just (RecRef ref) <- lookup (BC.pack "identity") items
    = Just $ AnnouncePacket ref

    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "idreq"
    , Just (RecRef ref) <- lookup (BC.pack "identity") items
    , Just (RecRef from) <- lookup (BC.pack "from") items
    = Just $ IdentityRequest ref from

    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "idresp"
    , Just (RecRef ref) <- lookup (BC.pack "identity") items
    = Just $ IdentityResponse ref

    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "chreq"
    , Just (RecRef ref) <- lookup (BC.pack "req") items
    = Just $ TrChannelRequest ref

    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "chacc"
    , Just (RecRef ref) <- lookup (BC.pack "acc") items
    = Just $ TrChannelAccept ref

transportFromObject _ = Nothing

serviceToObject :: ServiceHeader -> Object
serviceToObject (ServiceHeader svc ref) = Rec
        [ (BC.pack "SVC", RecText svc)
        , (BC.pack "ref", RecRef ref)
        ]

serviceFromObject :: Object -> Maybe ServiceHeader
serviceFromObject (Rec items)
    | Just (RecText svc) <- lookup (BC.pack "SVC") items
    , Just (RecRef ref) <- lookup (BC.pack "ref") items
    = Just $ ServiceHeader svc ref
serviceFromObject _ = Nothing


startServer :: String -> Stored Identity -> IO (Chan Peer, Chan (Peer, T.Text, Ref))
startServer bhost sidentity = do
    chanPeer <- newChan
    chanSvc <- newChan
    peers <- newMVar M.empty

    let open addr = do
            sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
            setSocketOption sock ReuseAddr 1
            setSocketOption sock Broadcast 1
            setCloseOnExecIfNeeded =<< fdSocket sock
            bind sock (addrAddress addr)
            return sock

        loop sock = do
            baddr:_ <- getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just bhost) (Just discoveryPort)
            void $ sendTo sock (BL.toStrict $ serializeObject $ transportToObject $ AnnouncePacket $ storedRef sidentity) (addrAddress baddr)
            forever $ do
                (msg, paddr) <- recvFrom sock 4096
                mbpeer <- M.lookup paddr <$> readMVar peers
                if | Just peer <- mbpeer
                   , ch:_ <- peerChannels peer
                   , Just plain <- channelDecrypt ch msg
                   , Right (obj:objs) <- runExcept $ deserializeObjects (storedStorage sidentity) $ BL.fromStrict plain
                   , Just (ServiceHeader svc ref) <- serviceFromObject obj
                   -> do forM_ objs $ store $ storedStorage sidentity
                         writeChan chanSvc (peer, svc, ref)

                   | Right (obj:objs) <- runExcept $ deserializeObjects (storedStorage sidentity) $ BL.fromStrict msg
                   , Just tpack <- transportFromObject obj
                   -> packet sock paddr tpack objs

                   | otherwise -> putStrLn $ show paddr ++ ": invalid packet"

        packet sock paddr (AnnouncePacket ref) _ = do
            putStrLn $ "Got announce: " ++ show ref ++ " from " ++ show paddr
            when (ref /= storedRef sidentity) $ void $ sendTo sock (BL.toStrict $ BL.concat
                [ serializeObject $ transportToObject $ IdentityRequest ref (storedRef sidentity)
                , BL.concat $ map (lazyLoadBytes . storedRef) $ collectStoredObjects $ wrappedLoad $ storedRef sidentity
                ]) paddr

        packet _ paddr (IdentityRequest ref from) [] = do
            putStrLn $ "Got identity request: for " ++ show ref ++ " by " ++ show from ++ " from " ++ show paddr ++ " without content"

        packet sock paddr (IdentityRequest ref from) (obj:objs) = do
            putStrLn $ "Got identity request: for " ++ show ref ++ " by " ++ show from ++ " from " ++ show paddr
            print (obj:objs)
            from' <- store (storedStorage sidentity) obj
            if from == from'
               then do forM_ objs $ store $ storedStorage sidentity
                       let peer = Peer (DatagramAddress paddr) (Just $ wrappedLoad from) [] sock
                       modifyMVar_ peers $ return . M.insert paddr peer
                       writeChan chanPeer peer
                       void $ sendTo sock (BL.toStrict $ BL.concat
                           [ serializeObject $ transportToObject $ IdentityResponse (storedRef sidentity)
                           , BL.concat $ map (lazyLoadBytes . storedRef) $ collectStoredObjects $ wrappedLoad $ storedRef sidentity
                           ]) paddr
               else putStrLn $ "Mismatched content"

        packet _ paddr (IdentityResponse ref) [] = do
            putStrLn $ "Got identity response: by " ++ show ref ++ " from " ++ show paddr ++ " without content"

        packet sock paddr (IdentityResponse ref) (obj:objs) = do
            putStrLn $ "Got identity response: by " ++ show ref ++ " from " ++ show paddr
            print (obj:objs)
            ref' <- store (storedStorage sidentity) obj
            if ref == ref'
               then do forM_ objs $ store $ storedStorage sidentity
                       let pidentity = wrappedLoad ref
                           peer = Peer (DatagramAddress paddr) (Just pidentity) [] sock
                       modifyMVar_ peers $ return . M.insert paddr peer
                       writeChan chanPeer peer
                       req <- createChannelRequest sidentity pidentity
                       void $ sendTo sock (BL.toStrict $ BL.concat
                           [ serializeObject $ transportToObject $ TrChannelRequest (storedRef req)
                           , lazyLoadBytes $ storedRef req
                           , lazyLoadBytes $ storedRef $ signedData $ fromStored req
                           , lazyLoadBytes $ storedRef $ crKey $ fromStored $ signedData $ fromStored req
                           , BL.concat $ map (lazyLoadBytes . storedRef) $ signedSignature $ fromStored req
                           ]) paddr
               else putStrLn $ "Mismatched content"

        packet _ paddr (TrChannelRequest _) [] = do
            putStrLn $ "Got channel request: from " ++ show paddr ++ " without content"

        packet sock paddr (TrChannelRequest ref) (obj:objs) = do
            putStrLn $ "Got channel request: from " ++ show paddr
            print (obj:objs)
            ref' <- store (storedStorage sidentity) obj
            if ref == ref'
               then do forM_ objs $ store $ storedStorage sidentity
                       let request = wrappedLoad ref :: Stored ChannelRequest
                       modifyMVar_ peers $ \pval -> case M.lookup paddr pval of
                           Just peer | Just pid <- peerIdentity peer ->
                               runExceptT (acceptChannelRequest sidentity pid request) >>= \case
                                   Left errs -> do mapM_ putStrLn ("Invalid channel request" : errs)
                                                   return pval
                                   Right (acc, channel) -> do
                                       putStrLn $ "Got channel: " ++ show (storedRef channel)
                                       let peer' = peer { peerChannels = fromStored channel : peerChannels peer }
                                       writeChan chanPeer peer'
                                       void $ sendTo sock (BL.toStrict $ BL.concat
                                           [ serializeObject $ transportToObject $ TrChannelAccept (storedRef acc)
                                           , lazyLoadBytes $ storedRef acc
                                           , lazyLoadBytes $ storedRef $ signedData $ fromStored acc
                                           , lazyLoadBytes $ storedRef $ caKey $ fromStored $ signedData $ fromStored acc
                                           , BL.concat $ map (lazyLoadBytes . storedRef) $ signedSignature $ fromStored acc
                                           ]) paddr
                                       return $ M.insert paddr peer' pval

                           _ -> do putStrLn $ "Invalid channel request - no peer identity"
                                   return pval
               else putStrLn $ "Mismatched content"

        packet _ paddr (TrChannelAccept _) [] = do
            putStrLn $ "Got channel accept: from " ++ show paddr ++ " without content"

        packet _ paddr (TrChannelAccept ref) (obj:objs) = do
            putStrLn $ "Got channel accept: from " ++ show paddr
            print (obj:objs)
            ref' <- store (storedStorage sidentity) obj
            if ref == ref'
               then do forM_ objs $ store $ storedStorage sidentity
                       let accepted = wrappedLoad ref :: Stored ChannelAccept
                       modifyMVar_ peers $ \pval -> case M.lookup paddr pval of
                           Just peer | Just pid <- peerIdentity peer ->
                               runExceptT (acceptedChannel sidentity pid accepted) >>= \case
                                   Left errs -> do mapM_ putStrLn ("Invalid channel accept" : errs)
                                                   return pval
                                   Right channel -> do
                                       putStrLn $ "Got channel: " ++ show (storedRef channel)
                                       let peer' = peer { peerChannels = fromStored channel : peerChannels peer }
                                       writeChan chanPeer peer'
                                       return $ M.insert paddr peer' pval
                           _ -> do putStrLn $ "Invalid channel accept - no peer identity"
                                   return pval

               else putStrLn $ "Mismatched content"

    void $ forkIO $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just discoveryPort)
        bracket (open addr) close loop

    return (chanPeer, chanSvc)


sendToPeer :: Storable a => Stored Identity -> Peer -> T.Text -> a -> IO ()
sendToPeer self peer@Peer { peerChannels = ch:_ } svc obj = do
    let st = storedStorage self
    ref <- store st obj
    let plain = BL.toStrict $ BL.concat
            [ serializeObject $ serviceToObject $ ServiceHeader svc ref
            , lazyLoadBytes ref
            ]
    ctext <- channelEncrypt ch plain
    let DatagramAddress paddr = peerAddress peer
    void $ sendTo (peerSocket peer) ctext paddr

sendToPeer _ _ _ _ = putStrLn $ "No channel to peer"

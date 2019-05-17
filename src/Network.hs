module Network (
    Peer(..),
    PeerAddress,
    peerDiscovery,
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
    }
    deriving (Show)

data PeerAddress = DatagramAddress SockAddr
    deriving (Show)


data TransportHeader = AnnouncePacket Ref
                     | IdentityRequest Ref Ref
                     | IdentityResponse Ref
                     | TrChannelRequest Ref
                     | TrChannelAccept Ref

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


peerDiscovery :: String -> Stored Identity -> IO (Chan Peer)
peerDiscovery bhost sidentity = do
    chanPeer <- newChan
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
                let packet' = packet sock paddr
                case runExcept $ deserializeObjects (storedStorage sidentity) $ BL.fromStrict msg of
                     Left err -> putStrLn $ show paddr ++ ": " ++ err
                     Right (obj:objs) | Just tpack <- transportFromObject obj -> packet' tpack objs
                     _ -> putStrLn $ show paddr ++ ": invalid transport packet"

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
                       let peer = Peer (DatagramAddress paddr) (Just $ wrappedLoad from) []
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
                           peer = Peer (DatagramAddress paddr) (Just pidentity) []
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

    return chanPeer

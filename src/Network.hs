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
    , peerInStorage :: PartialStorage
    }
    deriving (Show)

data PeerAddress = DatagramAddress SockAddr
    deriving (Show)


data TransportHeader = AnnouncePacket PartialRef
                     | IdentityRequest PartialRef PartialRef
                     | IdentityResponse PartialRef
                     | TrChannelRequest PartialRef
                     | TrChannelAccept PartialRef

data ServiceHeader = ServiceHeader T.Text PartialRef

transportToObject :: TransportHeader -> PartialObject
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

transportFromObject :: PartialObject -> Maybe TransportHeader
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

serviceToObject :: ServiceHeader -> PartialObject
serviceToObject (ServiceHeader svc ref) = Rec
        [ (BC.pack "SVC", RecText svc)
        , (BC.pack "ref", RecRef ref)
        ]

serviceFromObject :: PartialObject -> Maybe ServiceHeader
serviceFromObject (Rec items)
    | Just (RecText svc) <- lookup (BC.pack "SVC") items
    , Just (RecRef ref) <- lookup (BC.pack "ref") items
    = Just $ ServiceHeader svc ref
serviceFromObject _ = Nothing


startServer :: (String -> IO ()) -> String -> Stored Identity -> IO (Chan Peer, Chan (Peer, T.Text, Ref))
startServer logd bhost sidentity = do
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
            st <- derivePartialStorage $ storedStorage sidentity
            baddr:_ <- getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just bhost) (Just discoveryPort)
            void $ sendTo sock (BL.toStrict $ serializeObject $ transportToObject $ AnnouncePacket $ partialRef st $ storedRef sidentity) (addrAddress baddr)
            forever $ do
                (msg, paddr) <- recvFrom sock 4096
                mbpeer <- M.lookup paddr <$> readMVar peers
                if | Just peer <- mbpeer
                   , ch:_ <- peerChannels peer
                   , Just plain <- channelDecrypt ch msg
                   , Right (obj:objs) <- runExcept $ deserializeObjects (peerInStorage peer) $ BL.fromStrict plain
                   , Just (ServiceHeader svc ref) <- serviceFromObject obj
                   -> do forM_ objs $ storeObject $ peerInStorage peer
                         copyRef (storedStorage sidentity) ref >>= \case
                             Just pref -> writeChan chanSvc (peer, svc, pref)
                             Nothing   -> logd $ show paddr ++ ": incomplete service packet"

                   | otherwise -> do
                       ist <- case mbpeer of
                                   Just peer -> return $ peerInStorage peer
                                   Nothing   -> derivePartialStorage $ storedStorage sidentity
                       if | Right (obj:objs) <- runExcept $ deserializeObjects ist $ BL.fromStrict msg
                          , Just tpack <- transportFromObject obj
                          -> packet sock paddr tpack objs ist

                          | otherwise -> logd $ show paddr ++ ": invalid packet"

        packet sock paddr (AnnouncePacket ref) _ ist = do
            logd $ "Got announce: " ++ show ref ++ " from " ++ show paddr
            when (refDigest ref /= refDigest (storedRef sidentity)) $ void $ sendTo sock (BL.toStrict $ BL.concat
                [ serializeObject $ transportToObject $ IdentityRequest ref (partialRef ist $ storedRef sidentity)
                , BL.concat $ map (lazyLoadBytes . storedRef) $ collectStoredObjects $ wrappedLoad $ storedRef sidentity
                ]) paddr

        packet _ paddr (IdentityRequest ref from) [] _ = do
            logd $ "Got identity request: for " ++ show ref ++ " by " ++ show from ++ " from " ++ show paddr ++ " without content"

        packet sock paddr (IdentityRequest ref from) (obj:objs) ist = do
            logd $ "Got identity request: for " ++ show ref ++ " by " ++ show from ++ " from " ++ show paddr
            logd $ show (obj:objs)
            from' <- storeObject ist obj
            if from == from'
               then do forM_ objs $ storeObject ist
                       copyRef (storedStorage sidentity) from >>= \case
                           Nothing -> logd $ "Incomplete peer identity"
                           Just sfrom -> do
                               let peer = Peer (DatagramAddress paddr) (Just $ wrappedLoad sfrom) [] sock ist
                               modifyMVar_ peers $ return . M.insert paddr peer
                               writeChan chanPeer peer
                               void $ sendTo sock (BL.toStrict $ BL.concat
                                   [ serializeObject $ transportToObject $ IdentityResponse (partialRef ist $ storedRef sidentity)
                                   , BL.concat $ map (lazyLoadBytes . storedRef) $ collectStoredObjects $ wrappedLoad $ storedRef sidentity
                                   ]) paddr
               else logd $ "Mismatched content"

        packet _ paddr (IdentityResponse ref) [] _ = do
            logd $ "Got identity response: by " ++ show ref ++ " from " ++ show paddr ++ " without content"

        packet sock paddr (IdentityResponse ref) (obj:objs) ist = do
            logd $ "Got identity response: by " ++ show ref ++ " from " ++ show paddr
            logd $ show (obj:objs)
            ref' <- storeObject ist obj
            if ref == ref'
               then do forM_ objs $ storeObject ist
                       copyRef (storedStorage sidentity) ref >>= \case
                           Nothing -> logd $ "Incomplete peer identity"
                           Just sref -> do
                               let pidentity = wrappedLoad sref
                                   peer = Peer (DatagramAddress paddr) (Just pidentity) [] sock ist
                               modifyMVar_ peers $ return . M.insert paddr peer
                               writeChan chanPeer peer
                               req <- createChannelRequest sidentity pidentity
                               void $ sendTo sock (BL.toStrict $ BL.concat
                                   [ serializeObject $ transportToObject $ TrChannelRequest (partialRef ist $ storedRef req)
                                   , lazyLoadBytes $ storedRef req
                                   , lazyLoadBytes $ storedRef $ signedData $ fromStored req
                                   , lazyLoadBytes $ storedRef $ crKey $ fromStored $ signedData $ fromStored req
                                   , BL.concat $ map (lazyLoadBytes . storedRef) $ signedSignature $ fromStored req
                                   ]) paddr
               else logd $ "Mismatched content"

        packet _ paddr (TrChannelRequest _) [] _ = do
            logd $ "Got channel request: from " ++ show paddr ++ " without content"

        packet sock paddr (TrChannelRequest ref) (obj:objs) ist = do
            logd $ "Got channel request: from " ++ show paddr
            logd $ show (obj:objs)
            ref' <- storeObject ist obj
            if ref == ref'
               then do forM_ objs $ storeObject ist
                       copyRef (storedStorage sidentity) ref >>= \case
                           Nothing -> logd $ "Incomplete channel request"
                           Just sref -> do
                               let request = wrappedLoad sref :: Stored ChannelRequest
                               modifyMVar_ peers $ \pval -> case M.lookup paddr pval of
                                   Just peer | Just pid <- peerIdentity peer ->
                                       runExceptT (acceptChannelRequest sidentity pid request) >>= \case
                                           Left errs -> do mapM_ logd ("Invalid channel request" : errs)
                                                           return pval
                                           Right (acc, channel) -> do
                                               logd $ "Got channel: " ++ show (storedRef channel)
                                               let peer' = peer { peerChannels = fromStored channel : peerChannels peer }
                                               writeChan chanPeer peer'
                                               void $ sendTo sock (BL.toStrict $ BL.concat
                                                   [ serializeObject $ transportToObject $ TrChannelAccept (partialRef ist $ storedRef acc)
                                                   , lazyLoadBytes $ storedRef acc
                                                   , lazyLoadBytes $ storedRef $ signedData $ fromStored acc
                                                   , lazyLoadBytes $ storedRef $ caKey $ fromStored $ signedData $ fromStored acc
                                                   , BL.concat $ map (lazyLoadBytes . storedRef) $ signedSignature $ fromStored acc
                                                   ]) paddr
                                               return $ M.insert paddr peer' pval

                                   _ -> do logd $ "Invalid channel request - no peer identity"
                                           return pval
               else logd $ "Mismatched content"

        packet _ paddr (TrChannelAccept _) [] _ = do
            logd $ "Got channel accept: from " ++ show paddr ++ " without content"

        packet _ paddr (TrChannelAccept ref) (obj:objs) ist = do
            logd $ "Got channel accept: from " ++ show paddr
            logd $ show (obj:objs)
            ref' <- storeObject ist obj
            if ref == ref'
               then do forM_ objs $ storeObject ist
                       copyRef (storedStorage sidentity) ref >>= \case
                           Nothing -> logd $ "Incomplete channel accept"
                           Just sref -> do
                               let accepted = wrappedLoad sref :: Stored ChannelAccept
                               modifyMVar_ peers $ \pval -> case M.lookup paddr pval of
                                   Just peer | Just pid <- peerIdentity peer ->
                                       runExceptT (acceptedChannel sidentity pid accepted) >>= \case
                                           Left errs -> do mapM_ logd ("Invalid channel accept" : errs)
                                                           return pval
                                           Right channel -> do
                                               logd $ "Got channel: " ++ show (storedRef channel)
                                               let peer' = peer { peerChannels = fromStored channel : peerChannels peer }
                                               writeChan chanPeer peer'
                                               return $ M.insert paddr peer' pval
                                   _ -> do logd $ "Invalid channel accept - no peer identity"
                                           return pval

               else logd $ "Mismatched content"

    void $ forkIO $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just discoveryPort)
        bracket (open addr) close loop

    return (chanPeer, chanSvc)


sendToPeer :: Storable a => Stored Identity -> Peer -> T.Text -> a -> IO ()
sendToPeer _ peer@Peer { peerChannels = ch:_ } svc obj = do
    let st = peerInStorage peer
    ref <- store st obj
    Just bytes <- return $ lazyLoadBytes ref
    let plain = BL.toStrict $ BL.concat
            [ serializeObject $ serviceToObject $ ServiceHeader svc ref
            , bytes
            ]
    ctext <- channelEncrypt ch plain
    let DatagramAddress paddr = peerAddress peer
    void $ sendTo (peerSocket peer) ctext paddr

sendToPeer _ _ _ _ = putStrLn $ "No channel to peer"

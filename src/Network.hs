module Network (
    Peer(..),
    PeerAddress(..),
    PeerIdentity(..),
    PeerChannel(..),
    WaitingRef, wrDigest,
    Service(..),
    startServer,
    sendToPeer, sendToPeerWith,
) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.State

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Either
import Data.List
import qualified Data.Map as M
import Data.Maybe
import Data.Typeable

import Network.Socket
import Network.Socket.ByteString (recvFrom, sendTo)

import Channel
import Identity
import PubKey
import Service
import State
import Storage


discoveryPort :: ServiceName
discoveryPort = "29665"

announceIntervalSeconds :: Int
announceIntervalSeconds = 60


data Peer = Peer
    { peerAddress :: PeerAddress
    , peerIdentity :: PeerIdentity
    , peerIdentityUpdate :: [WaitingRef]
    , peerChannel :: PeerChannel
    , peerSocket :: Socket
    , peerStorage :: Storage
    , peerInStorage :: PartialStorage
    , peerServiceState :: MVar (M.Map ServiceID SomeServiceState)
    , peerServiceQueue :: [(ServiceID, WaitingRef)]
    , peerWaitingRefs :: [WaitingRef]
    }

data PeerAddress = DatagramAddress SockAddr
    deriving (Show)

data PeerIdentity = PeerIdentityUnknown
                  | PeerIdentityRef WaitingRef
                  | PeerIdentityFull UnifiedIdentity

data PeerChannel = ChannelWait
                 | ChannelOurRequest (Stored ChannelRequest)
                 | ChannelPeerRequest WaitingRef
                 | ChannelOurAccept (Stored ChannelAccept) (Stored Channel)
                 | ChannelEstablished Channel


data TransportHeaderItem
    = Acknowledged PartialRef
    | DataRequest PartialRef
    | DataResponse PartialRef
    | AnnounceSelf PartialRef
    | AnnounceUpdate PartialRef
    | TrChannelRequest PartialRef
    | TrChannelAccept PartialRef
    | ServiceType ServiceID
    | ServiceRef PartialRef

data TransportHeader = TransportHeader [TransportHeaderItem]

transportToObject :: TransportHeader -> PartialObject
transportToObject (TransportHeader items) = Rec $ map single items
    where single = \case
              Acknowledged ref -> (BC.pack "ACK", RecRef ref)
              DataRequest ref -> (BC.pack "REQ", RecRef ref)
              DataResponse ref -> (BC.pack "RSP", RecRef ref)
              AnnounceSelf ref -> (BC.pack "ANN", RecRef ref)
              AnnounceUpdate ref -> (BC.pack "ANU", RecRef ref)
              TrChannelRequest ref -> (BC.pack "CRQ", RecRef ref)
              TrChannelAccept ref -> (BC.pack "CAC", RecRef ref)
              ServiceType stype -> (BC.pack "STP", RecUUID $ toUUID stype)
              ServiceRef ref -> (BC.pack "SRF", RecRef ref)

transportFromObject :: PartialObject -> Maybe TransportHeader
transportFromObject (Rec items) = case catMaybes $ map single items of
                                       [] -> Nothing
                                       titems -> Just $ TransportHeader titems
    where single (name, content) = if
              | name == BC.pack "ACK", RecRef ref <- content -> Just $ Acknowledged ref
              | name == BC.pack "REQ", RecRef ref <- content -> Just $ DataRequest ref
              | name == BC.pack "RSP", RecRef ref <- content -> Just $ DataResponse ref
              | name == BC.pack "ANN", RecRef ref <- content -> Just $ AnnounceSelf ref
              | name == BC.pack "ANU", RecRef ref <- content -> Just $ AnnounceUpdate ref
              | name == BC.pack "CRQ", RecRef ref <- content -> Just $ TrChannelRequest ref
              | name == BC.pack "CAC", RecRef ref <- content -> Just $ TrChannelAccept ref
              | name == BC.pack "STP", RecUUID uuid <- content -> Just $ ServiceType $ fromUUID uuid
              | name == BC.pack "SRF", RecRef ref <- content -> Just $ ServiceRef ref
              | otherwise -> Nothing
transportFromObject _ = Nothing

lookupServiceType :: [TransportHeaderItem] -> Maybe ServiceID
lookupServiceType (ServiceType stype : _) = Just stype
lookupServiceType (_ : hs) = lookupServiceType hs
lookupServiceType [] = Nothing


data WaitingRef = WaitingRef Storage PartialRef (MVar [RefDigest])

wrDigest :: WaitingRef -> RefDigest
wrDigest (WaitingRef _ pref _) = refDigest pref

newWaitingRef :: Storage -> PartialRef -> PacketHandler WaitingRef
newWaitingRef st pref = do
    wref <- WaitingRef st pref <$> liftIO (newMVar [])
    updatePeer $ \p -> p { peerWaitingRefs = wref : peerWaitingRefs p }
    return wref

copyOrRequestRef :: Storage -> PartialRef -> PacketHandler (Either WaitingRef Ref)
copyOrRequestRef st pref = copyRef st pref >>= \case
    Right ref -> return $ Right ref
    Left dgst -> do
        addHeader $ DataRequest $ partialRefFromDigest (refStorage pref) dgst
        wref <- WaitingRef st pref <$> liftIO (newMVar [dgst])
        updatePeer $ \p -> p { peerWaitingRefs = wref : peerWaitingRefs p }
        return $ Left wref

checkWaitingRef :: WaitingRef -> PacketHandler (Maybe Ref)
checkWaitingRef (WaitingRef st pref mvar) = do
    liftIO (readMVar mvar) >>= \case
        [] -> copyRef st pref >>= \case
                  Right ref -> return $ Just ref
                  Left dgst -> do liftIO $ modifyMVar_ mvar $ return . (dgst:)
                                  addHeader $ DataRequest $ partialRefFromDigest (refStorage pref) dgst
                                  return Nothing
        _  -> return Nothing

receivedWaitingRef :: PartialRef -> WaitingRef -> PacketHandler (Maybe Ref)
receivedWaitingRef nref wr@(WaitingRef _ _ mvar) = do
    liftIO $ modifyMVar_ mvar $ return . filter (/= refDigest nref)
    checkWaitingRef wr


startServer :: Head -> (String -> IO ()) -> String -> [SomeService] -> IO (Chan Peer)
startServer origHead logd bhost services = do
    let storage = refStorage $ headRef origHead
    chanPeer <- newChan
    chanSvc <- newChan
    peers <- newMVar M.empty
    midentity <- newMVar $ headLocalIdentity origHead

    let open addr = do
            sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
            setSocketOption sock ReuseAddr 1
            setSocketOption sock Broadcast 1
            setCloseOnExecIfNeeded =<< fdSocket sock
            bind sock (addrAddress addr)
            return sock

        loop sock = do
            let announce identity = do
                    st <- derivePartialStorage storage
                    baddr:_ <- getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just bhost) (Just discoveryPort)
                    void $ sendTo sock (BL.toStrict $ serializeObject $ transportToObject $ TransportHeader [ AnnounceSelf $ partialRef st $ storedRef $ idData identity ]) (addrAddress baddr)

            void $ forkIO $ forever $ do
                announce =<< readMVar midentity
                threadDelay $ announceIntervalSeconds * 1000 * 1000

            watchHead origHead $ \h -> do
                let idt = headLocalIdentity h
                modifyMVar_ midentity $ \_ -> return idt
                announce idt

            forever $ do
                (msg, paddr) <- recvFrom sock 4096
                mbpeer <- M.lookup paddr <$> readMVar peers
                (peer, content, secure) <- if
                    | Just peer <- mbpeer
                    , Just ch <- case peerChannel peer of
                                      ChannelEstablished ch  -> Just ch
                                      ChannelOurAccept _ sch -> Just $ fromStored sch
                                      _                      -> Nothing
                    , Right plain <- runExcept $ channelDecrypt ch msg
                    -> return (peer, plain, True)

                    | Just peer <- mbpeer
                    -> return (peer, msg, False)

                    | otherwise -> do
                          pst <- deriveEphemeralStorage storage
                          ist <- derivePartialStorage pst
                          svcs <- newMVar M.empty
                          let peer = Peer
                                  { peerAddress = DatagramAddress paddr
                                  , peerIdentity = PeerIdentityUnknown
                                  , peerIdentityUpdate = []
                                  , peerChannel = ChannelWait
                                  , peerSocket = sock
                                  , peerStorage = pst
                                  , peerInStorage = ist
                                  , peerServiceState = svcs
                                  , peerServiceQueue = []
                                  , peerWaitingRefs = []
                                  }
                          return (peer, msg, False)

                case runExcept $ deserializeObjects (peerInStorage peer) $ BL.fromStrict content of
                     Right (obj:objs)
                         | Just header <- transportFromObject obj -> do
                               forM_ objs $ storeObject $ peerInStorage peer
                               identity <- readMVar midentity
                               handlePacket logd identity secure peer chanSvc header >>= \case
                                   Just peer' -> do
                                       modifyMVar_ peers $ return . M.insert paddr peer'
                                       writeChan chanPeer peer'
                                   Nothing -> return ()

                         | otherwise -> do
                               logd $ show paddr ++ ": invalid objects"
                               logd $ show objs

                     _ -> logd $ show paddr ++ ": invalid objects"

    void $ forkIO $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just discoveryPort)
        bracket (open addr) close loop

    void $ forkIO $ forever $ readChan chanSvc >>= \case
        (peer, svc, ref)
            | PeerIdentityFull peerId <- peerIdentity peer
            -> modifyMVar_ (peerServiceState peer) $ \svcs ->
                   case maybe (someServiceEmptyState <$> find ((svc ==) . someServiceID) services) Just $ M.lookup svc svcs of
                        Nothing -> do logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"
                                      return svcs
                        Just (SomeServiceState s) -> do
                            let inp = ServiceInput
                                    { svcPeer = peerId
                                    , svcPrintOp = logd
                                    }
                            (rsp, s') <- handleServicePacket storage inp s (wrappedLoad ref)
                            identity <- readMVar midentity
                            runExceptT (maybe (return ()) (sendToPeer identity peer) rsp) >>= \case
                                Left err -> logd $ "failed to send response to peer: " ++ show err
                                Right () -> return ()
                            return $ M.insert svc (SomeServiceState s') svcs

            | DatagramAddress paddr <- peerAddress peer -> do
                logd $ "service packet from peer with incomplete identity " ++ show paddr

    return chanPeer

type PacketHandler a = StateT PacketHandlerState (ExceptT String IO) a

data PacketHandlerState = PacketHandlerState
    { phPeer :: Peer
    , phPeerChanged :: Bool
    , phHead :: [TransportHeaderItem]
    , phBody :: [Ref]
    }

updatePeer :: (Peer -> Peer) -> PacketHandler ()
updatePeer f = modify $ \ph -> ph { phPeer = f (phPeer ph), phPeerChanged = True }

addHeader :: TransportHeaderItem -> PacketHandler ()
addHeader h = modify $ \ph -> ph { phHead = h : phHead ph }

addBody :: Ref -> PacketHandler ()
addBody r = modify $ \ph -> ph { phBody = r : phBody ph }

handlePacket :: (String -> IO ()) -> UnifiedIdentity -> Bool
    -> Peer -> Chan (Peer, ServiceID, Ref)
    -> TransportHeader -> IO (Maybe Peer)
handlePacket logd identity secure opeer chanSvc (TransportHeader headers) = do
    let sidentity = idData identity
        DatagramAddress paddr = peerAddress opeer
        plaintextRefs = map (refDigest . storedRef) $ concatMap (collectStoredObjects . wrappedLoad) $ concat
            [ [ storedRef sidentity ]
            , map storedRef $ idUpdates identity
            , case peerChannel opeer of
                   ChannelOurRequest req  -> [ storedRef req ]
                   ChannelOurAccept acc _ -> [ storedRef acc ]
                   _                      -> []
            ]

    res <- runExceptT $ flip execStateT (PacketHandlerState opeer False [] []) $ do
        forM_ headers $ \case
            Acknowledged ref -> do
                gets (peerChannel . phPeer) >>= \case
                    ChannelOurAccept acc ch | refDigest (storedRef acc) == refDigest ref -> do
                        updatePeer $ \p -> p { peerChannel = ChannelEstablished (fromStored ch) }
                        sendIdentityUpdate identity
                    _ -> return ()

            DataRequest ref
                | secure || refDigest ref `elem` plaintextRefs -> do
                    Right mref <- copyRef (storedStorage sidentity) ref
                    addHeader $ DataResponse ref
                    addBody $ mref
                | otherwise -> throwError $ "unauthorized data request for " ++ show ref

            DataResponse ref -> do
                liftIO (ioLoadBytes ref) >>= \case
                    Right _  -> do
                        addHeader $ Acknowledged ref
                        wait <- gets $ peerWaitingRefs . phPeer
                        wait' <- flip filterM wait $ receivedWaitingRef ref >=> \case
                            Just _  -> return False
                            Nothing -> return True
                        updatePeer $ \p -> p { peerWaitingRefs = wait' }
                    Left _ -> throwError $ "mismatched data response " ++ show ref

            AnnounceSelf ref -> do
                peer <- gets phPeer
                if | PeerIdentityRef wref <- peerIdentity peer, wrDigest wref == refDigest ref -> return ()
                   | PeerIdentityFull pid <- peerIdentity peer, refDigest ref == (refDigest $ storedRef $ idData pid) -> return ()
                   | refDigest ref == refDigest (storedRef sidentity) -> return ()
                   | otherwise -> do
                        copyOrRequestRef (peerStorage peer) ref >>= \case
                            Right pref
                                | Just idt <- validateIdentity $ wrappedLoad pref ->
                                    case peerIdentity peer of
                                         PeerIdentityFull prev | not (prev `sameIdentity` idt) ->
                                             throwError $ "peer identity does not follow"
                                         _ -> updatePeer $ \p -> p { peerIdentity = PeerIdentityFull idt }
                                | otherwise -> throwError $ "broken identity " ++ show pref
                            Left wref -> do
                                addHeader $ AnnounceSelf $ partialRef (peerInStorage peer) $ storedRef $ idData identity
                                updatePeer $ \p -> p { peerIdentity = PeerIdentityRef wref }

            AnnounceUpdate ref -> do
                peer <- gets phPeer
                case peerIdentity peer of
                     PeerIdentityFull pid -> copyOrRequestRef (peerStorage peer) ref >>= \case
                         Right upd -> updatePeer $ \p -> p { peerIdentity = PeerIdentityFull $ updateOwners [wrappedLoad upd] pid }
                         Left wref -> updatePeer $ \p -> p { peerIdentityUpdate = wref : peerIdentityUpdate p }
                     _ -> return ()

            TrChannelRequest reqref -> do
                addHeader $ Acknowledged reqref
                pst <- gets $ peerStorage . phPeer
                let process = handleChannelRequest identity =<< newWaitingRef pst reqref
                gets (peerChannel . phPeer) >>= \case
                    ChannelWait {} -> process
                    ChannelOurRequest our | refDigest reqref < refDigest (storedRef our) -> process
                                          | otherwise -> return ()
                    ChannelPeerRequest {} -> process
                    ChannelOurAccept {} -> return ()
                    ChannelEstablished {} -> process

            TrChannelAccept accref -> do
                addHeader $ Acknowledged accref
                let process = handleChannelAccept identity accref
                gets (peerChannel . phPeer) >>= \case
                    ChannelWait {} -> process
                    ChannelOurRequest {} -> process
                    ChannelPeerRequest {} -> process
                    ChannelOurAccept our _ | refDigest accref < refDigest (storedRef our) -> process
                                           | otherwise -> return ()
                    ChannelEstablished {} -> process

            ServiceType _ -> return ()
            ServiceRef pref
                | not secure -> throwError $ "service packet without secure channeel"
                | Just svc <- lookupServiceType headers -> do
                    liftIO (ioLoadBytes pref) >>= \case
                        Right _ -> do
                            addHeader $ Acknowledged pref
                            pst <- gets $ peerStorage . phPeer
                            wref <- newWaitingRef pst pref
                            updatePeer $ \p -> p { peerServiceQueue = (svc, wref) : peerServiceQueue p }
                        Left _ -> throwError $ "missing service object " ++ show pref
                | otherwise -> throwError $ "service ref without type"
                
        setupChannel identity
        handleIdentityUpdate
        handleServices chanSvc

    case res of
        Left err -> do
            logd $ "Error in handling packet from " ++ show paddr ++ ": " ++ err
            return Nothing
        Right ph -> do
            when (not $ null $ phHead ph) $ do
                let plain = BL.toStrict $ BL.concat
                        [ serializeObject $ transportToObject $ TransportHeader $ reverse $ phHead ph
                        , BL.concat $ map lazyLoadBytes $ phBody ph
                        ]
                case peerChannel $ phPeer ph of
                     ChannelEstablished ch -> do
                        x <- runExceptT (channelEncrypt ch plain)
                        case x of Right ctext -> void $ sendTo (peerSocket $ phPeer ph) ctext paddr
                                  Left err -> logd $ "Failed to encrypt data: " ++ err
                     _ -> void $ sendTo (peerSocket $ phPeer ph) plain paddr

            return $ if phPeerChanged ph then Just $ phPeer ph
                                         else Nothing


getOrRequestIdentity :: PeerIdentity -> PacketHandler (Maybe UnifiedIdentity)
getOrRequestIdentity = \case
    PeerIdentityUnknown -> return Nothing
    PeerIdentityRef wref -> checkWaitingRef wref >>= \case
        Just ref -> case validateIdentity (wrappedLoad ref) of
                         Nothing  -> throwError $ "broken identity"
                         Just idt -> return $ Just idt
        Nothing -> return Nothing
    PeerIdentityFull idt -> return $ Just idt


setupChannel :: UnifiedIdentity -> PacketHandler ()
setupChannel identity = gets phPeer >>= \case
    peer@Peer { peerChannel = ChannelWait } -> do
        getOrRequestIdentity (peerIdentity peer) >>= \case
            Just pid | Just upid <- toUnifiedIdentity pid -> do
                let ist = peerInStorage peer
                req <- createChannelRequest (peerStorage peer) identity upid
                updatePeer $ \p -> p { peerChannel = ChannelOurRequest req }
                addHeader $ TrChannelRequest $ partialRef ist $ storedRef req
                addHeader $ AnnounceSelf $ partialRef ist $ storedRef $ idData identity
                addBody $ storedRef req
            _ -> return ()

    Peer { peerChannel = ChannelPeerRequest wref } -> do
        handleChannelRequest identity wref

    _ -> return ()

handleChannelRequest :: UnifiedIdentity -> WaitingRef -> PacketHandler ()
handleChannelRequest identity reqref = do
    ist <- gets $ peerInStorage . phPeer
    checkWaitingRef reqref >>= \case
        Just req -> do
            pid <- gets (peerIdentity . phPeer) >>= \case
                PeerIdentityFull pid -> return pid
                PeerIdentityRef wref -> do
                    Just idref <- checkWaitingRef wref
                    Just pid <- return $ validateIdentity $ wrappedLoad idref
                    return pid
                PeerIdentityUnknown -> throwError $ "unknown peer identity"

            (acc, ch) <- case toUnifiedIdentity pid of
                Just upid -> acceptChannelRequest identity upid (wrappedLoad req)
                Nothing   -> throwError $ "non-unified peer identity"
            updatePeer $ \p -> p
                { peerIdentity = PeerIdentityFull pid
                , peerChannel = ChannelOurAccept acc ch
                }
            addHeader $ TrChannelAccept (partialRef ist $ storedRef acc)
            mapM_ addBody $ concat
                [ [ storedRef $ acc ]
                , [ storedRef $ signedData $ fromStored acc ]
                , [ storedRef $ caKey $ fromStored $ signedData $ fromStored acc ]
                , map storedRef $ signedSignature $ fromStored acc
                ]
        Nothing -> do
            updatePeer $ \p -> p { peerChannel = ChannelPeerRequest reqref }

handleChannelAccept :: UnifiedIdentity -> PartialRef -> PacketHandler ()
handleChannelAccept identity accref = do
    pst <- gets $ peerStorage . phPeer
    copyRef pst accref >>= \case
        Right acc -> do
            pid <- gets (peerIdentity . phPeer) >>= \case
                PeerIdentityFull pid -> return pid
                PeerIdentityRef wref -> do
                    Just idref <- checkWaitingRef wref
                    Just pid <- return $ validateIdentity $ wrappedLoad idref
                    return pid
                PeerIdentityUnknown -> throwError $ "unknown peer identity"

            ch <- case toUnifiedIdentity pid of
                Just upid -> acceptedChannel identity upid (wrappedLoad acc)
                Nothing   -> throwError $ "non-unified peer identity"
            updatePeer $ \p -> p
                { peerIdentity = PeerIdentityFull pid
                , peerChannel = ChannelEstablished $ fromStored ch
                }
            sendIdentityUpdate identity
        Left dgst -> throwError $ "missing accept data " ++ BC.unpack (showRefDigest dgst)


sendIdentityUpdate :: UnifiedIdentity -> PacketHandler ()
sendIdentityUpdate self = do
    ist <- gets $ peerInStorage . phPeer
    mapM_ addHeader . map (AnnounceUpdate . partialRef ist . storedRef) . idUpdates $ self


handleIdentityUpdate :: PacketHandler ()
handleIdentityUpdate = do
    peer <- gets phPeer
    case (peerIdentity peer, peerIdentityUpdate peer) of
         (PeerIdentityFull pid, wrefs@(_:_)) -> do
             (wrefs', upds) <- fmap partitionEithers $ forM wrefs $ \wref -> checkWaitingRef wref >>= \case
                 Just upd -> return $ Right $ wrappedLoad upd
                 Nothing -> return $ Left wref
             updatePeer $ \p -> p
                 { peerIdentity = PeerIdentityFull $ updateOwners upds pid
                 , peerIdentityUpdate = wrefs'
                 }
         _ -> return ()


handleServices :: Chan (Peer, ServiceID, Ref) -> PacketHandler ()
handleServices chan = gets (peerServiceQueue . phPeer) >>= \case
    [] -> return ()
    queue -> do
        queue' <- flip filterM queue $ \case
            (svc, wref) -> checkWaitingRef wref >>= \case
                Just ref -> do
                    peer <- gets phPeer
                    liftIO $ writeChan chan (peer, svc, ref)
                    return False
                Nothing -> return True
        updatePeer $ \p -> p { peerServiceQueue = queue' }


sendToPeer :: (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> ServicePacket s -> m ()
sendToPeer _ peer@Peer { peerChannel = ChannelEstablished ch } packet = do
    let st = peerInStorage peer
    ref <- liftIO $ store st packet
    bytes <- case lazyLoadBytes ref of
                  Right bytes -> return bytes
                  Left dgst -> throwError $ "incomplete ref " ++ show ref ++ ", missing " ++ BC.unpack (showRefDigest dgst)
    let plain = BL.toStrict $ BL.concat
            [ serializeObject $ transportToObject $ TransportHeader [ServiceType $ serviceID packet, ServiceRef ref]
            , bytes
            ]
    ctext <- channelEncrypt ch plain
    let DatagramAddress paddr = peerAddress peer
    void $ liftIO $ sendTo (peerSocket peer) ctext paddr

sendToPeer _ _ _ = throwError $ "no channel to peer"

sendToPeerWith :: forall s m. (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> (ServiceState s -> ExceptT String IO (Maybe (ServicePacket s), ServiceState s)) -> m ()
sendToPeerWith identity peer fobj = do
    let sid = serviceID @s Proxy
    res <- liftIO $ modifyMVar (peerServiceState peer) $ \svcs -> do
        runExceptT (fobj $ fromMaybe emptyServiceState $ fromServiceState =<< M.lookup sid svcs) >>= \case
            Right (obj, s') -> return $ (M.insert sid (SomeServiceState s') svcs, Right obj)
            Left err -> return $ (svcs, Left err)
    case res of
         Right (Just obj) -> sendToPeer identity peer obj
         Right Nothing -> return ()
         Left err -> throwError err

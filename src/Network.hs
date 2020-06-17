module Network (
    Server,
    startServer,
    serverChanPeer,

    Peer(..),
    PeerAddress(..),
    PeerIdentity(..),
    PeerChannel(..),
    WaitingRef, wrDigest,
    Service(..),
    serverPeer,
    sendToPeer, sendToPeerStored, sendToPeerWith,

    discoveryPort,
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
import Data.Map (Map)
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
import Sync


discoveryPort :: ServiceName
discoveryPort = "29665"

announceIntervalSeconds :: Int
announceIntervalSeconds = 60


data Server = Server
    { serverStorage :: Storage
    , serverIdentity :: MVar UnifiedIdentity
    , serverSocket :: MVar Socket
    , serverPeers :: MVar (Map SockAddr Peer)
    , serverChanPeer' :: Chan Peer
    }

serverChanPeer :: Server -> Chan Peer
serverChanPeer = serverChanPeer'


data Peer = Peer
    { peerAddress :: PeerAddress
    , peerIdentity :: PeerIdentity
    , peerIdentityUpdate :: [WaitingRef]
    , peerChannel :: PeerChannel
    , peerSocket :: Socket
    , peerStorage :: Storage
    , peerInStorage :: PartialStorage
    , peerServiceState :: MVar (M.Map ServiceID SomeServiceState)
    , peerServiceInQueue :: [(ServiceID, WaitingRef)]
    , peerServiceOutQueue :: MVar [TransportPacket]
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


data TransportPacket = TransportPacket TransportHeader [Ref]


data TransportHeaderItem
    = Acknowledged PartialRef
    | Rejected PartialRef
    | DataRequest PartialRef
    | DataResponse PartialRef
    | AnnounceSelf PartialRef
    | AnnounceUpdate PartialRef
    | TrChannelRequest PartialRef
    | TrChannelAccept PartialRef
    | ServiceType ServiceID
    | ServiceRef PartialRef
    deriving (Eq)

data TransportHeader = TransportHeader [TransportHeaderItem]

transportToObject :: TransportHeader -> PartialObject
transportToObject (TransportHeader items) = Rec $ map single items
    where single = \case
              Acknowledged ref -> (BC.pack "ACK", RecRef ref)
              Rejected ref -> (BC.pack "REJ", RecRef ref)
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
              | name == BC.pack "REJ", RecRef ref <- content -> Just $ Rejected ref
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


startServer :: Head LocalState -> (String -> IO ()) -> String -> [SomeService] -> IO Server
startServer origHead logd bhost services = do
    let storage = refStorage $ headRef origHead
    chanPeer <- newChan
    chanSvc <- newChan
    svcStates <- newMVar M.empty
    peers <- newMVar M.empty
    midentity <- newMVar $ headLocalIdentity origHead
    mshared <- newMVar $ lsShared $ load $ headRef origHead
    ssocket <- newEmptyMVar

    let open addr = do
            sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
            putMVar ssocket sock
            setSocketOption sock ReuseAddr 1
            setSocketOption sock Broadcast 1
            setCloseOnExecIfNeeded =<< fdSocket sock
            bind sock (addrAddress addr)
            return sock

        loop sock = do
            void $ forkIO $ forever $ do
                readMVar midentity >>= \identity -> do
                    st <- derivePartialStorage storage
                    baddr:_ <- getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just bhost) (Just discoveryPort)
                    void $ sendTo sock (BL.toStrict $ serializeObject $ transportToObject $ TransportHeader [ AnnounceSelf $ partialRef st $ storedRef $ idData identity ]) (addrAddress baddr)
                threadDelay $ announceIntervalSeconds * 1000 * 1000

            let announceUpdate identity = do
                    st <- derivePartialStorage storage
                    let plaintext = BL.toStrict $ serializeObject $ transportToObject $ TransportHeader $
                            (AnnounceSelf $ partialRef st $ storedRef $ idData identity) :
                            map (AnnounceUpdate . partialRef st . storedRef) (idUpdates identity)

                    ps <- readMVar peers
                    forM_ ps $ \case
                      peer
                        | PeerIdentityFull _ <- peerIdentity peer
                        , ChannelEstablished ch <- peerChannel peer
                        , DatagramAddress paddr <- peerAddress peer
                        -> runExceptT (channelEncrypt ch plaintext) >>= \case
                               Right ctext -> void $ sendTo (peerSocket peer) ctext paddr
                               Left err -> logd $ "Failed to encrypt data: " ++ err
                        | otherwise -> return ()

            let shareState self shared peer
                    | PeerIdentityFull pid <- peerIdentity peer
                    , finalOwner pid `sameIdentity` finalOwner self = do
                        forM_ shared $ \s -> runExceptT (sendToPeer self peer $ SyncPacket s) >>= \case
                            Left err -> logd $ "failed to sync state with peer: " ++ show err
                            Right () -> return ()
                    | otherwise = return ()

            watchHead origHead $ \h -> do
                let idt = headLocalIdentity h
                changedId <- modifyMVar midentity $ \cur ->
                    return (idt, cur /= idt)
                when changedId $ announceUpdate idt

                let shared = lsShared $ load $ headRef h
                changedShared <- modifyMVar mshared $ \cur ->
                    return (shared, cur /= shared)
                when changedShared $ do
                    mapM_ (shareState idt shared) =<< readMVar peers

            forever $ do
                (msg, paddr) <- recvFrom sock 4096
                modifyMVar_ peers $ \pvalue -> do
                    let mbpeer = M.lookup paddr pvalue
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

                        | otherwise
                        -> (, msg, False) <$> mkPeer storage sock paddr

                    case runExcept $ deserializeObjects (peerInStorage peer) $ BL.fromStrict content of
                         Right (obj:objs)
                             | Just header <- transportFromObject obj -> do
                                   forM_ objs $ storeObject $ peerInStorage peer
                                   identity <- readMVar midentity
                                   let svcs = map someServiceID services
                                   handlePacket logd origHead identity secure peer chanSvc svcs header >>= \case
                                       Just peer' -> do
                                           writeChan chanPeer peer'
                                           return $ M.insert paddr peer' pvalue
                                       Nothing -> return pvalue

                             | otherwise -> do
                                   logd $ show paddr ++ ": invalid objects"
                                   logd $ show objs
                                   return pvalue

                         _ -> do logd $ show paddr ++ ": invalid objects"
                                 return pvalue

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
            -> modifyMVar_ svcStates $ \global ->
               modifyMVar (peerServiceState peer) $ \svcs ->
                   case (maybe (someServiceEmptyState <$> find ((svc ==) . someServiceID) services) Just $ M.lookup svc svcs,
                            maybe (someServiceEmptyGlobalState <$> find ((svc ==) . someServiceID) services) Just $ M.lookup svc global) of
                        (Just (SomeServiceState (proxy :: Proxy s) s),
                            Just (SomeServiceGlobalState (_ :: Proxy gs) gs))
                            | Just (Refl :: s :~: gs) <- eqT -> do
                            let inp = ServiceInput
                                    { svcPeer = peerId
                                    , svcPrintOp = logd
                                    }
                            reloadHead origHead >>= \case
                                Nothing -> do
                                    logd $ "current head deleted"
                                    return (svcs, global)
                                Just h -> do
                                    (rsp, (s', gs')) <- handleServicePacket h inp s gs (wrappedLoad ref :: Stored s)
                                    identity <- readMVar midentity
                                    runExceptT (sendToPeerList identity peer rsp) >>= \case
                                        Left err -> logd $ "failed to send response to peer: " ++ show err
                                        Right () -> return ()
                                    return (M.insert svc (SomeServiceState proxy s') svcs,
                                        M.insert svc (SomeServiceGlobalState proxy gs') global)
                        _ -> do
                            logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"
                            return (svcs, global)

            | DatagramAddress paddr <- peerAddress peer -> do
                logd $ "service packet from peer with incomplete identity " ++ show paddr

    return Server
        { serverStorage = storage
        , serverIdentity = midentity
        , serverSocket = ssocket
        , serverPeers = peers
        , serverChanPeer' = chanPeer
        }

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
addHeader h = modify $ \ph -> ph { phHead = h `appendDistinct` phHead ph }

addBody :: Ref -> PacketHandler ()
addBody r = modify $ \ph -> ph { phBody = r `appendDistinct` phBody ph }

appendDistinct :: Eq a => a -> [a] -> [a]
appendDistinct x (y:ys) | x == y    = y : ys
                        | otherwise = y : appendDistinct x ys
appendDistinct x [] = [x]

handlePacket :: (String -> IO ()) -> Head LocalState -> UnifiedIdentity -> Bool
    -> Peer -> Chan (Peer, ServiceID, Ref) -> [ServiceID]
    -> TransportHeader -> IO (Maybe Peer)
handlePacket logd origHead identity secure opeer chanSvc svcs (TransportHeader headers) = do
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
                        finalizedChannel origHead identity
                    _ -> return ()

            Rejected _ -> return ()

            DataRequest ref
                | secure || refDigest ref `elem` plaintextRefs -> do
                    Right mref <- copyRef (storedStorage sidentity) ref
                    addHeader $ DataResponse ref
                    addBody $ mref
                | otherwise -> do
                    liftIO $ logd $ "unauthorized data request for " ++ show ref
                    addHeader $ Rejected ref

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
                pst <- gets $ peerStorage . phPeer
                let process = do
                        addHeader $ Acknowledged reqref
                        handleChannelRequest identity =<< newWaitingRef pst reqref
                    reject = addHeader $ Rejected reqref

                gets (peerChannel . phPeer) >>= \case
                    ChannelWait {} -> process
                    ChannelOurRequest our | refDigest reqref < refDigest (storedRef our) -> process
                                          | otherwise -> reject
                    ChannelPeerRequest {} -> process
                    ChannelOurAccept {} -> reject
                    ChannelEstablished {} -> process

            TrChannelAccept accref -> do
                let process = do
                        addHeader $ Acknowledged accref
                        handleChannelAccept origHead identity accref
                gets (peerChannel . phPeer) >>= \case
                    ChannelWait {} -> process
                    ChannelOurRequest {} -> process
                    ChannelPeerRequest {} -> process
                    ChannelOurAccept our _ | refDigest accref < refDigest (storedRef our) -> process
                                           | otherwise -> addHeader $ Rejected accref
                    ChannelEstablished {} -> process

            ServiceType _ -> return ()
            ServiceRef pref
                | not secure -> throwError $ "service packet without secure channel"
                | Just svc <- lookupServiceType headers -> if
                    | svc `elem` svcs -> do
                        liftIO (ioLoadBytes pref) >>= \case
                            Right _ -> do
                                addHeader $ Acknowledged pref
                                pst <- gets $ peerStorage . phPeer
                                wref <- newWaitingRef pst pref
                                updatePeer $ \p -> p { peerServiceInQueue = (svc, wref) : peerServiceInQueue p }
                            Left _ -> throwError $ "missing service object " ++ show pref
                    | otherwise -> addHeader $ Rejected pref
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
                        [ serializeObject $ transportToObject $ TransportHeader $ phHead ph
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

handleChannelAccept :: Head LocalState -> UnifiedIdentity -> PartialRef -> PacketHandler ()
handleChannelAccept oh identity accref = do
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
            finalizedChannel oh identity
        Left dgst -> throwError $ "missing accept data " ++ BC.unpack (showRefDigest dgst)


finalizedChannel :: Head LocalState -> UnifiedIdentity -> PacketHandler ()
finalizedChannel oh self = do
    -- Identity update
    ist <- gets $ peerInStorage . phPeer
    addHeader $ AnnounceSelf $ partialRef ist $ storedRef $ idData $ self
    mapM_ addHeader . map (AnnounceUpdate . partialRef ist . storedRef) . idUpdates $ self

    -- Shared state
    gets phPeer >>= \case
        peer | PeerIdentityFull pid <- peerIdentity peer
             , finalOwner pid `sameIdentity` finalOwner self -> do
                 Just h <- liftIO $ reloadHead oh
                 let shared = lsShared $ headObject h
                 addHeader $ ServiceType $ serviceID @SyncService Proxy
                 mapM_ (addHeader . ServiceRef . partialRef ist . storedRef) shared
                 mapM_ (addBody . storedRef) shared
             | otherwise -> return ()

    -- Outstanding service packets
    gets phPeer >>= \case
        Peer { peerChannel = ChannelEstablished ch
             , peerAddress = DatagramAddress paddr
             , peerServiceOutQueue = oqueue
             , peerSocket = sock
             } -> do
                 ps <- liftIO $ modifyMVar oqueue $ return . ([],)
                 forM_ ps $ sendPacket sock paddr ch
        _ -> return ()


handleIdentityUpdate :: PacketHandler ()
handleIdentityUpdate = do
    peer <- gets phPeer
    case (peerIdentity peer, peerIdentityUpdate peer) of
         (PeerIdentityRef wref, _) -> checkWaitingRef wref >>= \case
            Just ref | Just pid <- validateIdentity $ wrappedLoad ref -> do
                updatePeer $ \p -> p { peerIdentity = PeerIdentityFull pid }
                handleIdentityUpdate
            _ -> return ()

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
handleServices chan = gets (peerServiceInQueue . phPeer) >>= \case
    [] -> return ()
    queue -> do
        queue' <- flip filterM queue $ \case
            (svc, wref) -> checkWaitingRef wref >>= \case
                Just ref -> do
                    peer <- gets phPeer
                    liftIO $ writeChan chan (peer, svc, ref)
                    return False
                Nothing -> return True
        updatePeer $ \p -> p { peerServiceInQueue = queue' }


mkPeer :: Storage -> Socket -> SockAddr -> IO Peer
mkPeer st sock paddr = do
    pst <- deriveEphemeralStorage st
    ist <- derivePartialStorage pst
    svcs <- newMVar M.empty
    oqueue <- newMVar []
    return $ Peer
        { peerAddress = DatagramAddress paddr
        , peerIdentity = PeerIdentityUnknown
        , peerIdentityUpdate = []
        , peerChannel = ChannelWait
        , peerSocket = sock
        , peerStorage = pst
        , peerInStorage = ist
        , peerServiceState = svcs
        , peerServiceInQueue = []
        , peerServiceOutQueue = oqueue
        , peerWaitingRefs = []
        }

serverPeer :: Server -> SockAddr -> IO Peer
serverPeer server paddr = do
    sock <- readMVar $ serverSocket server
    (peer, hello) <- modifyMVar (serverPeers server) $ \pvalue -> do
        case M.lookup paddr pvalue of
             Just peer -> return (pvalue, (peer, False))
             Nothing -> do
                 peer <- mkPeer (serverStorage server) sock paddr
                 return (M.insert paddr peer pvalue, (peer, True))
    when hello $ do
        identity <- readMVar (serverIdentity server)
        void $ sendTo sock
            (BL.toStrict $ serializeObject $ transportToObject $ TransportHeader
                [ AnnounceSelf $ partialRef (peerInStorage peer) $ storedRef $ idData identity ]
            ) paddr
    return peer


sendToPeer :: (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> s -> m ()
sendToPeer self peer packet = sendToPeerList self peer [ServiceReply (Left packet) True]

sendToPeerStored :: (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> Stored s -> m ()
sendToPeerStored self peer spacket = sendToPeerList self peer [ServiceReply (Right spacket) True]

sendToPeerList :: (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> [ServiceReply s] -> m ()
sendToPeerList _ peer parts = do
    let st = peerStorage peer
        pst = peerInStorage peer
    srefs <- liftIO $ forM parts $ \case ServiceReply (Left x) _ -> store st x
                                         ServiceReply (Right sx) _ -> return $ storedRef sx
    prefs <- mapM (copyRef pst) srefs
    let content = map snd $ filter (\(ServiceReply _ use, _) -> use) (zip parts srefs)
        header = TransportHeader (ServiceType (serviceID $ head parts) : map ServiceRef prefs)
        packet = TransportPacket header content
    case peerChannel peer of
         ChannelEstablished ch -> do
             let DatagramAddress paddr = peerAddress peer
             sendPacket (peerSocket peer) paddr ch packet
         _ -> liftIO $ modifyMVar_ (peerServiceOutQueue peer) $ return . (packet:)

sendPacket :: (MonadIO m, MonadError String m) => Socket -> SockAddr -> Channel -> TransportPacket -> m ()
sendPacket sock addr ch (TransportPacket header content) = do
    let plain = BL.toStrict $ BL.concat $
            (serializeObject $ transportToObject header)
            : map lazyLoadBytes content
    ctext <- channelEncrypt ch plain
    void $ liftIO $ sendTo sock ctext addr

sendToPeerWith :: forall s m. (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> (ServiceState s -> ExceptT String IO (Maybe s, ServiceState s)) -> m ()
sendToPeerWith identity peer fobj = do
    let sproxy = Proxy @s
        sid = serviceID sproxy
    res <- liftIO $ modifyMVar (peerServiceState peer) $ \svcs -> do
        runExceptT (fobj $ fromMaybe (emptyServiceState sproxy) $ fromServiceState sproxy =<< M.lookup sid svcs) >>= \case
            Right (obj, s') -> return $ (M.insert sid (SomeServiceState sproxy s') svcs, Right obj)
            Left err -> return $ (svcs, Left err)
    case res of
         Right (Just obj) -> sendToPeer identity peer obj
         Right Nothing -> return ()
         Left err -> throwError err

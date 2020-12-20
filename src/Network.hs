module Network (
    Server,
    startServer,
    getNextPeerChange,

    Peer,
    PeerAddress(..), peerAddress,
    PeerIdentity(..), peerIdentity,
    PeerChannel(..),
    WaitingRef, wrDigest,
    Service(..),
    serverPeer, serverPeerIce,
    sendToPeer, sendToPeerStored, sendToPeerWith,

    discoveryPort,
) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.State

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Function
import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe
import Data.Typeable

import GHC.Conc.Sync (unsafeIOToSTM)

import Network.Socket
import qualified Network.Socket.ByteString as S

import Channel
import ICE
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
    , serverChanPacket :: Chan (PeerAddress, BC.ByteString)
    , serverOutQueue :: TQueue (Peer, Bool, TransportPacket)
    , serverDataResponse :: TQueue (Peer, Maybe PartialRef)
    , serverIOActions :: TQueue (ExceptT String IO ())
    , serverPeers :: MVar (Map PeerAddress Peer)
    , serverChanPeer :: TChan Peer
    , serverErrorLog :: TQueue String
    }

getNextPeerChange :: Server -> IO Peer
getNextPeerChange = atomically . readTChan . serverChanPeer


data Peer = Peer
    { peerAddress :: PeerAddress
    , peerServer :: Server
    , peerIdentityVar :: TVar PeerIdentity
    , peerChannel :: TVar PeerChannel
    , peerStorage :: Storage
    , peerInStorage :: PartialStorage
    , peerServiceState :: TMVar (M.Map ServiceID SomeServiceState)
    , peerServiceOutQueue :: TVar [TransportPacket]
    , peerWaitingRefs :: TMVar [WaitingRef]
    }

instance Eq Peer where
    (==) = (==) `on` peerIdentityVar

data PeerAddress = DatagramAddress Socket SockAddr
                 | PeerIceSession IceSession

instance Show PeerAddress where
    show (DatagramAddress _ addr) = show addr
    show (PeerIceSession ice) = show ice

instance Eq PeerAddress where
    DatagramAddress _ addr == DatagramAddress _ addr' = addr == addr'
    PeerIceSession ice     == PeerIceSession ice'     = ice == ice'
    _                      == _                       = False

instance Ord PeerAddress where
    compare (DatagramAddress _ addr) (DatagramAddress _ addr') = compare addr addr'
    compare (DatagramAddress _ _   ) _                         = LT
    compare _                        (DatagramAddress _ _    ) = GT
    compare (PeerIceSession ice    ) (PeerIceSession ice')     = compare ice ice'


data PeerIdentity = PeerIdentityUnknown (TVar [UnifiedIdentity -> ExceptT String IO ()])
                  | PeerIdentityRef WaitingRef (TVar [UnifiedIdentity -> ExceptT String IO ()])
                  | PeerIdentityFull UnifiedIdentity

data PeerChannel = ChannelWait
                 | ChannelOurRequest (Stored ChannelRequest)
                 | ChannelPeerRequest WaitingRef
                 | ChannelOurAccept (Stored ChannelAccept) (Stored Channel)
                 | ChannelEstablished Channel

peerIdentity :: MonadIO m => Peer -> m PeerIdentity
peerIdentity = liftIO . atomically . readTVar . peerIdentityVar


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


data WaitingRef = WaitingRef
    { wrefStorage :: Storage
    , wrefPartial :: PartialRef
    , wrefAction :: Ref -> WaitingRefCallback
    , wrefStatus :: TVar (Either [RefDigest] Ref)
    }

type WaitingRefCallback = ExceptT String IO ()

wrDigest :: WaitingRef -> RefDigest
wrDigest = refDigest . wrefPartial

newWaitingRef :: PartialRef -> (Ref -> WaitingRefCallback) -> PacketHandler WaitingRef
newWaitingRef pref act = do
    peer <- gets phPeer
    wref <- WaitingRef (peerStorage peer) pref act <$> liftSTM (newTVar (Left []))
    modifyTMVarP (peerWaitingRefs peer) (wref:)
    liftSTM $ writeTQueue (serverDataResponse $ peerServer peer) (peer, Nothing)
    return wref


startServer :: Head LocalState -> (String -> IO ()) -> String -> [SomeService] -> IO Server
startServer origHead logd' bhost services = do
    let storage = refStorage $ headRef origHead
    chanPacket <- newChan
    outQueue <- newTQueueIO
    dataResponse <- newTQueueIO
    ioActions <- newTQueueIO
    chanPeer <- newTChanIO
    chanSvc <- newTQueueIO
    svcStates <- newTMVarIO M.empty
    peers <- newMVar M.empty
    midentity <- newMVar $ headLocalIdentity origHead
    mshared <- newMVar $ lsShared $ load $ headRef origHead
    ssocket <- newEmptyMVar
    errlog <- newTQueueIO

    let server = Server
            { serverStorage = storage
            , serverIdentity = midentity
            , serverSocket = ssocket
            , serverChanPacket = chanPacket
            , serverOutQueue = outQueue
            , serverDataResponse = dataResponse
            , serverIOActions = ioActions
            , serverPeers = peers
            , serverChanPeer = chanPeer
            , serverErrorLog = errlog
            }

    let logd = writeTQueue errlog
    void $ forkIO $ forever $ do
        logd' =<< atomically (readTQueue errlog)

    void $ forkIO $ sendWorker server
    void $ forkIO $ dataResponseWorker server
    void $ forkIO $ forever $ do
        either (atomically . logd) return =<< runExceptT =<<
            atomically (readTQueue $ serverIOActions server)

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
                    void $ S.sendTo sock (BL.toStrict $ serializeObject $ transportToObject $ TransportHeader [ AnnounceSelf $ partialRef st $ storedRef $ idData identity ]) (addrAddress baddr)
                threadDelay $ announceIntervalSeconds * 1000 * 1000

            let announceUpdate identity = do
                    st <- derivePartialStorage storage
                    let hitems = (AnnounceSelf $ partialRef st $ storedRef $ idData identity) :
                            map (AnnounceUpdate . partialRef st . storedRef) (idUpdates identity)
                    let packet = TransportPacket (TransportHeader hitems) []

                    ps <- readMVar peers
                    forM_ ps $ \peer -> atomically $ do
                        ((,) <$> readTVar (peerIdentityVar peer) <*> readTVar (peerChannel peer)) >>= \case
                            (PeerIdentityFull _, ChannelEstablished _) ->
                                writeTQueue outQueue (peer, True, packet)
                            _  -> return ()

            let shareState self shared peer = do
                    let hitems = (ServiceType $ serviceID @SyncService Proxy) : 
                            map (ServiceRef . partialRef (peerInStorage peer) . storedRef) shared
                        packet = TransportPacket (TransportHeader hitems) $
                            map storedRef shared
                    atomically $ readTVar (peerIdentityVar peer) >>= \case
                        PeerIdentityFull pid | finalOwner pid `sameIdentity` finalOwner self -> do
                            sendToPeerS peer packet
                        _  -> return ()

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

            void $ forkIO $ forever $ do
                (msg, saddr) <- S.recvFrom sock 4096
                writeChan chanPacket (DatagramAddress sock saddr, msg)

            forever $ do
                (paddr, msg) <- readChan chanPacket
                (peer, content, secure) <- modifyMVar peers $ \pvalue -> do
                    case M.lookup paddr pvalue of
                        Just peer -> do
                            mbch <- atomically (readTVar (peerChannel peer)) >>= return . \case
                                ChannelEstablished ch  -> Just ch
                                ChannelOurAccept _ sch -> Just $ fromStored sch
                                _                      -> Nothing

                            if  | Just ch <- mbch
                                , Right plain <- runExcept $ channelDecrypt ch msg
                                -> return (pvalue, (peer, plain, True))

                                | otherwise
                                -> return (pvalue, (peer, msg, False))

                        Nothing -> do
                            peer <- mkPeer server paddr
                            return (M.insert paddr peer pvalue, (peer, msg, False))

                case runExcept $ deserializeObjects (peerInStorage peer) $ BL.fromStrict content of
                     Right (obj:objs)
                         | Just header <- transportFromObject obj -> do
                               prefs <- forM objs $ storeObject $ peerInStorage peer
                               identity <- readMVar midentity
                               let svcs = map someServiceID services
                               handlePacket origHead identity secure peer chanSvc svcs header prefs >>= \case
                                   Just peer' -> atomically $ writeTChan chanPeer peer'
                                   Nothing -> return ()

                         | otherwise -> atomically $ do
                               logd $ show paddr ++ ": invalid objects"
                               logd $ show objs

                     _ -> do atomically $ logd $ show paddr ++ ": invalid objects"

    void $ forkIO $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just discoveryPort)
        bracket (open addr) close loop

    void $ forkIO $ forever $ do
        (peer, svc, ref) <- atomically $ readTQueue chanSvc
        atomically (readTVar (peerIdentityVar peer)) >>= \case
            PeerIdentityFull peerId -> do
                (global, svcs) <- atomically $ (,)
                    <$> takeTMVar svcStates
                    <*> takeTMVar (peerServiceState peer)
                case (maybe (someServiceEmptyState <$> find ((svc ==) . someServiceID) services) Just $ M.lookup svc svcs,
                         maybe (someServiceEmptyGlobalState <$> find ((svc ==) . someServiceID) services) Just $ M.lookup svc global) of
                     (Just (SomeServiceState (proxy :: Proxy s) s),
                         Just (SomeServiceGlobalState (_ :: Proxy gs) gs))
                         | Just (Refl :: s :~: gs) <- eqT -> do
                         let inp = ServiceInput
                                 { svcPeer = peerId
                                 , svcPrintOp = atomically . logd
                                 }
                         reloadHead origHead >>= \case
                             Nothing -> atomically $ do
                                 logd $ "current head deleted"
                                 putTMVar (peerServiceState peer) svcs
                                 putTMVar svcStates global
                             Just h -> do
                                 (rsp, (s', gs')) <- handleServicePacket h inp s gs (wrappedLoad ref :: Stored s)
                                 identity <- readMVar midentity
                                 sendToPeerList identity peer rsp
                                 atomically $ do
                                     putTMVar (peerServiceState peer) $ M.insert svc (SomeServiceState proxy s') svcs
                                     putTMVar svcStates $ M.insert svc (SomeServiceGlobalState proxy gs') global
                     _ -> atomically $ do
                         logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"
                         putTMVar (peerServiceState peer) svcs
                         putTMVar svcStates global

            _ -> do
                atomically $ logd $ "service packet from peer with incomplete identity " ++ show (peerAddress peer)

    return server

sendWorker :: Server -> IO ()
sendWorker server = forever $ do
    (peer, secure, packet@(TransportPacket header content)) <-
        atomically (readTQueue $ serverOutQueue server)

    let logd = atomically . writeTQueue (serverErrorLog $ peerServer peer)
    let plain = BL.toStrict $ BL.concat $
            (serializeObject $ transportToObject header)
            : map lazyLoadBytes content
    mbs <- do
        mbch <- atomically $ do
            readTVar (peerChannel peer) >>= \case
                ChannelEstablished ch -> return (Just ch)
                _ -> do when secure $ modifyTVar' (peerServiceOutQueue peer) (packet:)
                        return Nothing

        case mbch of
            Just ch -> do
                runExceptT (channelEncrypt ch plain) >>= \case
                    Right ctext -> return $ Just ctext
                    Left err -> do logd $ "Failed to encrypt data: " ++ err
                                   return Nothing
            Nothing | secure    -> return Nothing
                    | otherwise -> return $ Just plain

    case mbs of
        Just bs -> case peerAddress peer of
            DatagramAddress sock addr -> void $ S.sendTo sock bs addr
            PeerIceSession ice        -> iceSend ice bs
        Nothing -> return ()

dataResponseWorker :: Server -> IO ()
dataResponseWorker server = forever $ do
    (peer, npref) <- atomically (readTQueue $ serverDataResponse server)

    wait <- atomically $ takeTMVar (peerWaitingRefs peer)
    list <- forM wait $ \wr@WaitingRef { wrefStatus = tvar } ->
        atomically (readTVar tvar) >>= \case
            Left ds -> case maybe id (filter . (/=) . refDigest) npref $ ds of
                [] -> copyRef (wrefStorage wr) (wrefPartial wr) >>= \case
                          Right ref -> do
                              atomically (writeTVar tvar $ Right ref)
                              void $ forkIO $ runExceptT (wrefAction wr ref) >>= \case
                                  Left err -> atomically $ writeTQueue (serverErrorLog server) err
                                  Right () -> return ()

                              return (Nothing, [])
                          Left dgst -> do
                              atomically (writeTVar tvar $ Left [dgst])
                              return (Just wr, [partialRefFromDigest (refStorage $ wrefPartial wr) dgst])
                ds' -> do
                    atomically (writeTVar tvar $ Left ds')
                    return (Just wr, [])
            Right _ -> return (Nothing, [])
    atomically $ putTMVar (peerWaitingRefs peer) $ catMaybes $ map fst list

    let reqs = concat $ map snd list
    when (not $ null reqs) $ do
        let packet = TransportPacket (TransportHeader $ map DataRequest reqs) []
        atomically $ sendToPeerPlain peer packet


newtype PacketHandler a = PacketHandler { unPacketHandler :: StateT PacketHandlerState (ExceptT String STM) a }
    deriving (Functor, Applicative, Monad, MonadState PacketHandlerState, MonadError String)

instance MonadFail PacketHandler where
    fail = throwError

liftSTM :: STM a -> PacketHandler a
liftSTM = PacketHandler . lift . lift

readTVarP :: TVar a -> PacketHandler a
readTVarP = liftSTM . readTVar

writeTVarP :: TVar a -> a -> PacketHandler ()
writeTVarP v = liftSTM . writeTVar v

modifyTMVarP :: TMVar a -> (a -> a) -> PacketHandler ()
modifyTMVarP v f = liftSTM $ putTMVar v . f =<< takeTMVar v

data PacketHandlerState = PacketHandlerState
    { phPeer :: Peer
    , phPeerChanged :: Bool
    , phHead :: [TransportHeaderItem]
    , phBody :: [Ref]
    }

addHeader :: TransportHeaderItem -> PacketHandler ()
addHeader h = modify $ \ph -> ph { phHead = h `appendDistinct` phHead ph }

addBody :: Ref -> PacketHandler ()
addBody r = modify $ \ph -> ph { phBody = r `appendDistinct` phBody ph }

appendDistinct :: Eq a => a -> [a] -> [a]
appendDistinct x (y:ys) | x == y    = y : ys
                        | otherwise = y : appendDistinct x ys
appendDistinct x [] = [x]

handlePacket :: Head LocalState -> UnifiedIdentity -> Bool
    -> Peer -> TQueue (Peer, ServiceID, Ref) -> [ServiceID]
    -> TransportHeader -> [PartialRef] -> IO (Maybe Peer)
handlePacket origHead identity secure peer chanSvc svcs (TransportHeader headers) prefs = atomically $ do
    let server = peerServer peer
    ochannel <- readTVar $ peerChannel peer
    let sidentity = idData identity
        plaintextRefs = map (refDigest . storedRef) $ concatMap (collectStoredObjects . wrappedLoad) $ concat
            [ [ storedRef sidentity ]
            , map storedRef $ idUpdates identity
            , case ochannel of
                   ChannelOurRequest req  -> [ storedRef req ]
                   ChannelOurAccept acc _ -> [ storedRef acc ]
                   _                      -> []
            ]

    res <- runExceptT $ flip execStateT (PacketHandlerState peer False [] []) $ unPacketHandler $ do
        let logd = liftSTM . writeTQueue (serverErrorLog server)
        forM_ headers $ \case
            Acknowledged ref -> do
                readTVarP (peerChannel peer) >>= \case
                    ChannelOurAccept acc ch | refDigest (storedRef acc) == refDigest ref -> do
                        writeTVarP (peerChannel peer) $ ChannelEstablished (fromStored ch)
                        liftSTM $ finalizedChannel peer origHead identity
                    _ -> return ()

            Rejected ref -> do
                logd $ "rejected by peer: " ++ show (refDigest ref)

            DataRequest ref
                | secure || refDigest ref `elem` plaintextRefs -> do
                    Right mref <- liftSTM $ unsafeIOToSTM $ copyRef (storedStorage sidentity) ref
                    addHeader $ DataResponse ref
                    addBody $ mref
                | otherwise -> do
                    logd $ "unauthorized data request for " ++ show ref
                    addHeader $ Rejected ref

            DataResponse ref -> if
                | ref `elem` prefs -> do
                    addHeader $ Acknowledged ref
                    liftSTM $ writeTQueue (serverDataResponse server) (peer, Just ref)
                | otherwise -> throwError $ "mismatched data response " ++ show ref

            AnnounceSelf pref
                | refDigest pref == refDigest (storedRef sidentity) -> return ()
                | otherwise -> readTVarP (peerIdentityVar peer) >>= \case
                    PeerIdentityUnknown idwait -> do
                        wref <- newWaitingRef pref $ handleIdentityAnnounce identity peer
                        addHeader $ AnnounceSelf $ partialRef (peerInStorage peer) $ storedRef $ idData identity
                        writeTVarP (peerIdentityVar peer) $ PeerIdentityRef wref idwait
                        modify $ \ph -> ph { phPeerChanged = True }
                    _ -> return ()

            AnnounceUpdate ref -> do
                readTVarP (peerIdentityVar peer) >>= \case
                    PeerIdentityFull _ -> do
                        void $ newWaitingRef ref $ handleIdentityUpdate peer
                    _ -> return ()

            TrChannelRequest reqref -> do
                let process = do
                        addHeader $ Acknowledged reqref
                        wref <- newWaitingRef reqref $ handleChannelRequest peer identity
                        writeTVarP (peerChannel peer) $ ChannelPeerRequest wref
                    reject = addHeader $ Rejected reqref

                readTVarP (peerChannel peer) >>= \case
                    ChannelWait {} -> process
                    ChannelOurRequest our | refDigest reqref < refDigest (storedRef our) -> process
                                          | otherwise -> reject
                    ChannelPeerRequest {} -> process
                    ChannelOurAccept {} -> reject
                    ChannelEstablished {} -> process

            TrChannelAccept accref -> do
                let process = do
                        handleChannelAccept origHead identity accref
                readTVarP (peerChannel peer) >>= \case
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
                        if pref `elem` prefs || True {- TODO: used by Message service to confirm receive -}
                           then do
                                addHeader $ Acknowledged pref
                                void $ newWaitingRef pref $ \ref ->
                                    liftIO $ atomically $ writeTQueue chanSvc (peer, svc, ref)
                           else throwError $ "missing service object " ++ show pref
                    | otherwise -> addHeader $ Rejected pref
                | otherwise -> throwError $ "service ref without type"

    let logd = writeTQueue (serverErrorLog server)
    case res of
        Left err -> do
            logd $ "Error in handling packet from " ++ show (peerAddress peer) ++ ": " ++ err
            return Nothing
        Right ph -> do
            when (not $ null $ phHead ph) $ do
                let packet = TransportPacket (TransportHeader $ phHead ph) (phBody ph)
                writeTQueue (serverOutQueue server) (peer, secure, packet)

            return $ if phPeerChanged ph then Just $ phPeer ph
                                         else Nothing


withPeerIdentity :: MonadIO m => Peer -> (UnifiedIdentity -> ExceptT String IO ()) -> m ()
withPeerIdentity peer act = liftIO $ atomically $ readTVar (peerIdentityVar peer) >>= \case
    PeerIdentityUnknown tvar -> modifyTVar' tvar (act:)
    PeerIdentityRef _ tvar -> modifyTVar' tvar (act:)
    PeerIdentityFull idt -> writeTQueue (serverIOActions $ peerServer peer) (act idt)


setupChannel :: UnifiedIdentity -> Peer -> UnifiedIdentity -> WaitingRefCallback
setupChannel identity peer upid = do
    req <- createChannelRequest (peerStorage peer) identity upid
    let ist = peerInStorage peer
    let hitems =
            [ TrChannelRequest $ partialRef ist $ storedRef req
            , AnnounceSelf $ partialRef ist $ storedRef $ idData identity
            ]
    liftIO $ atomically $ do
        readTVar (peerChannel peer) >>= \case
            ChannelWait -> do
                sendToPeerPlain peer $ TransportPacket (TransportHeader hitems) [storedRef req]
                writeTVar (peerChannel peer) $ ChannelOurRequest req
            _ -> return ()

handleChannelRequest :: Peer -> UnifiedIdentity -> Ref -> WaitingRefCallback
handleChannelRequest peer identity req = do
    withPeerIdentity peer $ \upid -> do
        (acc, ch) <- acceptChannelRequest identity upid (wrappedLoad req)
        liftIO $ atomically $ do
            readTVar (peerChannel peer) >>= \case
                ChannelPeerRequest wr | wrDigest wr == refDigest req -> do
                    writeTVar (peerChannel peer) $ ChannelOurAccept acc ch
                    let header = TrChannelAccept (partialRef (peerInStorage peer) $ storedRef acc)
                    sendToPeerPlain peer $ TransportPacket (TransportHeader [header]) $ concat
                        [ [ storedRef $ acc ]
                        , [ storedRef $ signedData $ fromStored acc ]
                        , [ storedRef $ caKey $ fromStored $ signedData $ fromStored acc ]
                        , map storedRef $ signedSignature $ fromStored acc
                        ]
                _ -> writeTQueue (serverErrorLog $ peerServer peer) $ "unexpected channel request"

handleChannelAccept :: Head LocalState -> UnifiedIdentity -> PartialRef -> PacketHandler ()
handleChannelAccept oh identity accref = do
    peer <- gets phPeer
    liftSTM $ writeTQueue (serverIOActions $ peerServer peer) $ do
        withPeerIdentity peer $ \upid -> do
            copyRef (peerStorage peer) accref >>= \case
                Right acc -> do
                    ch <- acceptedChannel identity upid (wrappedLoad acc)
                    liftIO $ atomically $ do
                        sendToPeerS peer $ TransportPacket (TransportHeader [Acknowledged accref]) []
                        writeTVar (peerChannel peer) $ ChannelEstablished $ fromStored ch
                        finalizedChannel peer oh identity

                Left dgst -> throwError $ "missing accept data " ++ BC.unpack (showRefDigest dgst)


finalizedChannel :: Peer -> Head LocalState -> UnifiedIdentity -> STM ()
finalizedChannel peer oh self = do
    -- Identity update
    let ist = peerInStorage peer
    sendToPeerS peer $ flip TransportPacket [] $ TransportHeader $
        ( AnnounceSelf $ partialRef ist $ storedRef $ idData $ self ) :
        ( map (AnnounceUpdate . partialRef ist . storedRef) $ idUpdates $ self )

    -- Shared state
    readTVar (peerIdentityVar peer) >>= \case
        PeerIdentityFull pid | finalOwner pid `sameIdentity` finalOwner self -> do
            writeTQueue (serverIOActions $ peerServer peer) $ do
                Just h <- liftIO $ reloadHead oh
                let shared = lsShared $ headObject h
                let hitems = (ServiceType $ serviceID @SyncService Proxy) : 
                        map (ServiceRef . partialRef ist . storedRef) shared
                liftIO $ atomically $ sendToPeerS peer $
                    TransportPacket (TransportHeader hitems) $ map storedRef shared
        _ -> return ()

    -- Outstanding service packets
    mapM_ (sendToPeerS peer) =<< swapTVar (peerServiceOutQueue peer) []


handleIdentityAnnounce :: UnifiedIdentity -> Peer -> Ref -> WaitingRefCallback
handleIdentityAnnounce self peer ref = liftIO $ atomically $ do
    pidentity <- readTVar (peerIdentityVar peer)
    if  | PeerIdentityRef wref wact <- pidentity
        , wrDigest wref == refDigest ref
        -> case validateIdentity $ wrappedLoad ref of
            Just pid -> do
                writeTVar (peerIdentityVar peer) $ PeerIdentityFull pid
                writeTChan (serverChanPeer $ peerServer peer) peer
                mapM_ (writeTQueue (serverIOActions $ peerServer peer) . ($pid)) .
                    reverse =<< readTVar wact
                writeTQueue (serverIOActions $ peerServer peer) $ do
                    setupChannel self peer pid
            Nothing -> return ()

        | otherwise -> return ()

handleIdentityUpdate :: Peer -> Ref -> WaitingRefCallback
handleIdentityUpdate peer ref = liftIO $ atomically $ do
    pidentity <- readTVar (peerIdentityVar peer)
    if  | PeerIdentityFull pid <- pidentity
        -> do
            writeTVar (peerIdentityVar peer) $ PeerIdentityFull $
                updateOwners [wrappedLoad ref] pid
            writeTChan (serverChanPeer $ peerServer peer) peer

        | otherwise -> return ()


mkPeer :: Server -> PeerAddress -> IO Peer
mkPeer server paddr = do
    pst <- deriveEphemeralStorage $ serverStorage server
    Peer
        <$> pure paddr
        <*> pure server
        <*> (newTVarIO . PeerIdentityUnknown =<< newTVarIO [])
        <*> newTVarIO ChannelWait
        <*> pure pst
        <*> derivePartialStorage pst
        <*> newTMVarIO M.empty
        <*> newTVarIO []
        <*> newTMVarIO []

serverPeer :: Server -> SockAddr -> IO Peer
serverPeer server paddr = do
    sock <- readMVar $ serverSocket server
    serverPeer' server (DatagramAddress sock paddr)

serverPeerIce :: Server -> IceSession -> IO Peer
serverPeerIce server ice = do
    let paddr = PeerIceSession ice
    peer <- serverPeer' server paddr
    iceSetChan ice (paddr,) $ serverChanPacket server
    return peer

serverPeer' :: Server -> PeerAddress -> IO Peer
serverPeer' server paddr = do
    (peer, hello) <- modifyMVar (serverPeers server) $ \pvalue -> do
        case M.lookup paddr pvalue of
             Just peer -> return (pvalue, (peer, False))
             Nothing -> do
                 peer <- mkPeer server paddr
                 return (M.insert paddr peer pvalue, (peer, True))
    when hello $ do
        identity <- readMVar (serverIdentity server)
        atomically $ writeTQueue (serverOutQueue server) $ (peer, False,) $
            TransportPacket
                (TransportHeader [ AnnounceSelf $ partialRef (peerInStorage peer) $ storedRef $ idData identity ])
                []
    return peer


sendToPeer :: (Service s, MonadIO m) => UnifiedIdentity -> Peer -> s -> m ()
sendToPeer self peer packet = sendToPeerList self peer [ServiceReply (Left packet) True]

sendToPeerStored :: (Service s, MonadIO m) => UnifiedIdentity -> Peer -> Stored s -> m ()
sendToPeerStored self peer spacket = sendToPeerList self peer [ServiceReply (Right spacket) True]

sendToPeerList :: (Service s, MonadIO m) => UnifiedIdentity -> Peer -> [ServiceReply s] -> m ()
sendToPeerList _ peer parts = do
    let st = peerStorage peer
        pst = peerInStorage peer
    srefs <- liftIO $ forM parts $ \case ServiceReply (Left x) _ -> store st x
                                         ServiceReply (Right sx) _ -> return $ storedRef sx
    prefs <- mapM (copyRef pst) srefs
    let content = map snd $ filter (\(ServiceReply _ use, _) -> use) (zip parts srefs)
        header = TransportHeader (ServiceType (serviceID $ head parts) : map ServiceRef prefs)
        packet = TransportPacket header content
    liftIO $ atomically $ sendToPeerS peer packet

sendToPeerS :: Peer -> TransportPacket -> STM ()
sendToPeerS peer packet = writeTQueue (serverOutQueue $ peerServer peer) (peer, True, packet)

sendToPeerPlain :: Peer -> TransportPacket -> STM ()
sendToPeerPlain peer packet = writeTQueue (serverOutQueue $ peerServer peer) (peer, False, packet)

sendToPeerWith :: forall s m. (Service s, MonadIO m, MonadError String m) => UnifiedIdentity -> Peer -> (ServiceState s -> ExceptT String IO (Maybe s, ServiceState s)) -> m ()
sendToPeerWith identity peer fobj = do
    let sproxy = Proxy @s
        sid = serviceID sproxy
    res <- liftIO $ do
        svcs <- atomically $ takeTMVar (peerServiceState peer)
        (svcs', res) <- runExceptT (fobj $ fromMaybe (emptyServiceState sproxy) $ fromServiceState sproxy =<< M.lookup sid svcs) >>= \case
            Right (obj, s') -> return $ (M.insert sid (SomeServiceState sproxy s') svcs, Right obj)
            Left err -> return $ (svcs, Left err)
        atomically $ putTMVar (peerServiceState peer) svcs'
        return res

    case res of
         Right (Just obj) -> sendToPeer identity peer obj
         Right Nothing -> return ()
         Left err -> throwError err

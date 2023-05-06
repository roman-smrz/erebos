module Network (
    Server,
    startServer,
    getNextPeerChange,
    ServerOptions(..), serverIdentity, defaultServerOptions,

    Peer, peerServer, peerStorage,
    PeerAddress(..), peerAddress,
    PeerIdentity(..), peerIdentity,
    PeerChannel(..),
    WaitingRef, wrDigest,
    Service(..),
    serverPeer, serverPeerIce,
    sendToPeer, sendToPeerStored, sendToPeerWith,
    runPeerService,

    discoveryPort,
) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.State

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Function
import Data.IP qualified as IP
import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe
import Data.Typeable
import Data.Word

import Foreign.Ptr
import Foreign.Storable

import GHC.Conc.Sync (unsafeIOToSTM)

import Network.Socket
import qualified Network.Socket.ByteString as S

import System.Clock

import Channel
import ICE
import Identity
import PubKey
import Service
import State
import Storage
import Storage.Merge
import Sync


discoveryPort :: PortNumber
discoveryPort = 29665

announceIntervalSeconds :: Int
announceIntervalSeconds = 60


data Server = Server
    { serverStorage :: Storage
    , serverOrigHead :: Head LocalState
    , serverIdentity_ :: MVar UnifiedIdentity
    , serverSocket :: MVar Socket
    , serverChanPacket :: Chan (PeerAddress, BC.ByteString)
    , serverDataResponse :: TQueue (Peer, Maybe PartialRef)
    , serverIOActions :: TQueue (ExceptT String IO ())
    , serverServices :: [SomeService]
    , serverServiceStates :: TMVar (M.Map ServiceID SomeServiceGlobalState)
    , serverPeers :: MVar (Map PeerAddress Peer)
    , serverChanPeer :: TChan Peer
    , serverErrorLog :: TQueue String
    }

serverIdentity :: Server -> IO UnifiedIdentity
serverIdentity = readMVar . serverIdentity_

getNextPeerChange :: Server -> IO Peer
getNextPeerChange = atomically . readTChan . serverChanPeer

data ServerOptions = ServerOptions
    { serverPort :: PortNumber
    , serverLocalDiscovery :: Bool
    }

defaultServerOptions :: ServerOptions
defaultServerOptions = ServerOptions
    { serverPort = discoveryPort
    , serverLocalDiscovery = True
    }


data Peer = Peer
    { peerAddress :: PeerAddress
    , peerServer_ :: Server
    , peerIdentityVar :: TVar PeerIdentity
    , peerChannel :: TVar PeerChannel
    , peerStorage_ :: Storage
    , peerInStorage :: PartialStorage
    , peerOutQueue :: TQueue (Bool, [TransportHeaderItem], TransportPacket)
    , peerSentPackets :: TVar [SentPacket]
    , peerServiceState :: TMVar (M.Map ServiceID SomeServiceState)
    , peerServiceOutQueue :: TVar [([TransportHeaderItem], TransportPacket)]
    , peerWaitingRefs :: TMVar [WaitingRef]
    }

data SentPacket = SentPacket
    { spTime :: TimeSpec
    , spRetryCount :: Int
    , spAckedBy :: [TransportHeaderItem]
    , spData :: BC.ByteString
    }

peerServer :: Peer -> Server
peerServer = peerServer_

peerStorage :: Peer -> Storage
peerStorage = peerStorage_

instance Eq Peer where
    (==) = (==) `on` peerIdentityVar

data PeerAddress = DatagramAddress Socket SockAddr
                 | PeerIceSession IceSession

instance Show PeerAddress where
    show (DatagramAddress _ saddr) = unwords $ case IP.fromSockAddr saddr of
        Just (IP.IPv6 ipv6, port)
            | (0, 0, 0xffff, ipv4) <- IP.fromIPv6w ipv6
            -> [show (IP.toIPv4w ipv4), show port]
        Just (addr, port)
            -> [show addr, show port]
        _ -> [show saddr]
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
                 | ChannelOurAccept (Stored ChannelAccept) Channel
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


startServer :: ServerOptions -> Head LocalState -> (String -> IO ()) -> [SomeService] -> IO Server
startServer opt origHead logd' services = do
    let storage = refStorage $ headRef origHead
    chanPacket <- newChan
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
            , serverOrigHead = origHead
            , serverIdentity_ = midentity
            , serverSocket = ssocket
            , serverChanPacket = chanPacket
            , serverDataResponse = dataResponse
            , serverIOActions = ioActions
            , serverServices = services
            , serverServiceStates = svcStates
            , serverPeers = peers
            , serverChanPeer = chanPeer
            , serverErrorLog = errlog
            }

    let logd = writeTQueue errlog
    void $ forkIO $ forever $ do
        logd' =<< atomically (readTQueue errlog)

    void $ forkIO $ dataResponseWorker server
    void $ forkIO $ forever $ do
        either (atomically . logd) return =<< runExceptT =<<
            atomically (readTQueue $ serverIOActions server)

    broadcastAddreses <- getBroadcastAddresses discoveryPort

    let open addr = do
            sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
            putMVar ssocket sock
            setSocketOption sock ReuseAddr 1
            setSocketOption sock Broadcast 1
            withFdSocket sock setCloseOnExecIfNeeded
            bind sock (addrAddress addr)
            return sock

        loop sock = do
            when (serverLocalDiscovery opt) $ void $ forkIO $ forever $ do
                readMVar midentity >>= \identity -> do
                    st <- derivePartialStorage storage
                    let packet = BL.toStrict $ serializeObject $ transportToObject $ TransportHeader [ AnnounceSelf $ partialRef st $ storedRef $ idData identity ]
                    mapM_ (void . S.sendTo sock packet) broadcastAddreses
                threadDelay $ announceIntervalSeconds * 1000 * 1000

            let announceUpdate identity = do
                    st <- derivePartialStorage storage
                    let selfRef = partialRef st $ storedRef $ idData identity
                        updateRefs = selfRef : map (partialRef st . storedRef) (idUpdates identity)
                        ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- updateRefs ]
                        hitems = map AnnounceUpdate updateRefs
                        packet = TransportPacket (TransportHeader $  hitems) []

                    ps <- readMVar peers
                    forM_ ps $ \peer -> atomically $ do
                        ((,) <$> readTVar (peerIdentityVar peer) <*> readTVar (peerChannel peer)) >>= \case
                            (PeerIdentityFull _, ChannelEstablished _) ->
                                writeTQueue (peerOutQueue peer) (True, ackedBy, packet)
                            _  -> return ()

            let shareState self shared peer = do
                    let refs = map (partialRef (peerInStorage peer) . storedRef) shared
                        hitems = (ServiceType $ serviceID @SyncService Proxy) : map ServiceRef refs
                        ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- refs ]
                        packet = TransportPacket (TransportHeader hitems) $
                            map storedRef shared
                    atomically $ readTVar (peerIdentityVar peer) >>= \case
                        PeerIdentityFull pid | finalOwner pid `sameIdentity` finalOwner self -> do
                            sendToPeerS peer ackedBy packet
                        _  -> return ()

            void $ watchHead origHead $ \h -> do
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
                                ChannelEstablished ch -> Just ch
                                ChannelOurAccept _ ch -> Just ch
                                _                     -> Nothing

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
                               handlePacket origHead identity secure peer chanSvc svcs header prefs

                         | otherwise -> atomically $ do
                               logd $ show paddr ++ ": invalid objects"
                               logd $ show objs

                     _ -> do atomically $ logd $ show paddr ++ ": invalid objects"

    void $ forkIO $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrFamily = AF_INET6
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just $ show $ serverPort opt)
        bracket (open addr) close loop

    void $ forkIO $ forever $ do
        (peer, svc, ref) <- atomically $ readTQueue chanSvc
        case find ((svc ==) . someServiceID) (serverServices server) of
            Just service@(SomeService (_ :: Proxy s) attr) -> runPeerServiceOn (Just (service, attr)) peer (serviceHandler $ wrappedLoad @s ref)
            _ -> atomically $ logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"

    return server

sendWorker :: Peer -> IO ()
sendWorker peer = do
    startTime <- getTime MonotonicRaw
    nowVar <- newTVarIO startTime
    waitVar <- newTVarIO startTime

    let waitTill time = void $ forkIO $ do
            now <- getTime MonotonicRaw
            when (time > now) $
                threadDelay $ fromInteger (toNanoSecs (time - now)) `div` 1000
            atomically . writeTVar nowVar =<< getTime MonotonicRaw

    let sendBytes sp = do
            when (not $ null $ spAckedBy sp) $ do
                now <- getTime MonotonicRaw
                atomically $ modifyTVar' (peerSentPackets peer) $ (:) sp
                    { spTime = now
                    , spRetryCount = spRetryCount sp + 1
                    }
            case peerAddress peer of
                DatagramAddress sock addr -> void $ S.sendTo sock (spData sp) addr
                PeerIceSession ice        -> iceSend ice (spData sp)

    let sendNextPacket = do
            (secure, ackedBy, packet@(TransportPacket header content)) <-
                readTQueue (peerOutQueue peer)

            let logd = atomically . writeTQueue (serverErrorLog $ peerServer peer)
            let plain = BL.toStrict $ BL.concat $
                    (serializeObject $ transportToObject header)
                    : map lazyLoadBytes content

            mbch <- readTVar (peerChannel peer) >>= \case
                ChannelEstablished ch -> return (Just ch)
                _ -> do when secure $ modifyTVar' (peerServiceOutQueue peer) ((ackedBy, packet):)
                        return Nothing

            return $ do
                mbs <- case mbch of
                    Just ch -> do
                        runExceptT (channelEncrypt ch plain) >>= \case
                            Right ctext -> return $ Just ctext
                            Left err -> do logd $ "Failed to encrypt data: " ++ err
                                           return Nothing
                    Nothing | secure    -> return Nothing
                            | otherwise -> return $ Just plain

                case mbs of
                    Just bs -> do
                        sendBytes $ SentPacket
                            { spTime = undefined
                            , spRetryCount = -1
                            , spAckedBy = ackedBy
                            , spData = bs
                            }
                    Nothing -> return ()

    let retransmitPacket = do
            now <- readTVar nowVar
            (sp, rest) <- readTVar (peerSentPackets peer) >>= \case
                sps@(_:_) -> return (last sps, init sps)
                _         -> retry
            let nextTry = spTime sp + fromNanoSecs 1000000000
            if now < nextTry
              then do
                wait <- readTVar waitVar
                if wait <= now || nextTry < wait
                   then do writeTVar waitVar nextTry
                           return $ waitTill nextTry
                   else retry
              else do
                writeTVar (peerSentPackets peer) rest
                return $ sendBytes sp

    forever $ join $ atomically $ do
        retransmitPacket <|> sendNextPacket

processAcknowledgements :: Peer -> [TransportHeaderItem] -> STM ()
processAcknowledgements peer = mapM_ $ \hitem -> do
    modifyTVar' (peerSentPackets peer) $ filter $ (hitem `notElem`) . spAckedBy

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
            ackedBy = concat [[ Rejected r, DataResponse r ] | r <- reqs ]
        atomically $ sendToPeerPlain peer ackedBy packet


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
    , phHead :: [TransportHeaderItem]
    , phAckedBy :: [TransportHeaderItem]
    , phBody :: [Ref]
    }

addHeader :: TransportHeaderItem -> PacketHandler ()
addHeader h = modify $ \ph -> ph { phHead = h `appendDistinct` phHead ph }

addAckedBy :: [TransportHeaderItem] -> PacketHandler ()
addAckedBy hs = modify $ \ph -> ph { phAckedBy = foldr appendDistinct (phAckedBy ph) hs }

addBody :: Ref -> PacketHandler ()
addBody r = modify $ \ph -> ph { phBody = r `appendDistinct` phBody ph }

appendDistinct :: Eq a => a -> [a] -> [a]
appendDistinct x (y:ys) | x == y    = y : ys
                        | otherwise = y : appendDistinct x ys
appendDistinct x [] = [x]

handlePacket :: Head LocalState -> UnifiedIdentity -> Bool
    -> Peer -> TQueue (Peer, ServiceID, Ref) -> [ServiceID]
    -> TransportHeader -> [PartialRef] -> IO ()
handlePacket origHead identity secure peer chanSvc svcs (TransportHeader headers) prefs = atomically $ do
    let server = peerServer peer
    processAcknowledgements peer headers
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

    res <- runExceptT $ flip execStateT (PacketHandlerState peer [] [] []) $ unPacketHandler $ do
        let logd = liftSTM . writeTQueue (serverErrorLog server)
        forM_ headers $ \case
            Acknowledged ref -> do
                readTVarP (peerChannel peer) >>= \case
                    ChannelOurAccept acc ch | refDigest (storedRef acc) == refDigest ref -> do
                        writeTVarP (peerChannel peer) $ ChannelEstablished ch
                        liftSTM $ finalizedChannel peer origHead identity
                    _ -> return ()

            Rejected ref -> do
                logd $ "rejected by peer: " ++ show (refDigest ref)

            DataRequest ref
                | secure || refDigest ref `elem` plaintextRefs -> do
                    Right mref <- liftSTM $ unsafeIOToSTM $ copyRef (storedStorage sidentity) ref
                    addHeader $ DataResponse ref
                    addAckedBy [ Acknowledged ref, Rejected ref ]
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
                | otherwise -> do
                    wref <- newWaitingRef pref $ handleIdentityAnnounce identity peer
                    readTVarP (peerIdentityVar peer) >>= \case
                        PeerIdentityUnknown idwait -> do
                            let ref = partialRef (peerInStorage peer) $ storedRef $ idData identity
                            addHeader $ AnnounceSelf ref
                            writeTVarP (peerIdentityVar peer) $ PeerIdentityRef wref idwait
                            liftSTM $ writeTChan (serverChanPeer $ peerServer peer) peer
                        _ -> return ()

            AnnounceUpdate ref -> do
                readTVarP (peerIdentityVar peer) >>= \case
                    PeerIdentityFull _ -> do
                        void $ newWaitingRef ref $ handleIdentityUpdate peer
                        addHeader $ Acknowledged ref
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
        Right ph -> do
            when (not $ null $ phHead ph) $ do
                let packet = TransportPacket (TransportHeader $ phHead ph) (phBody ph)
                writeTQueue (peerOutQueue peer) (secure, phAckedBy ph, packet)


withPeerIdentity :: MonadIO m => Peer -> (UnifiedIdentity -> ExceptT String IO ()) -> m ()
withPeerIdentity peer act = liftIO $ atomically $ readTVar (peerIdentityVar peer) >>= \case
    PeerIdentityUnknown tvar -> modifyTVar' tvar (act:)
    PeerIdentityRef _ tvar -> modifyTVar' tvar (act:)
    PeerIdentityFull idt -> writeTQueue (serverIOActions $ peerServer peer) (act idt)


setupChannel :: UnifiedIdentity -> Peer -> UnifiedIdentity -> WaitingRefCallback
setupChannel identity peer upid = do
    req <- createChannelRequest (peerStorage peer) identity upid
    let ist = peerInStorage peer
    let reqref = partialRef ist $ storedRef req
    let hitems =
            [ TrChannelRequest reqref
            , AnnounceSelf $ partialRef ist $ storedRef $ idData identity
            ]
    liftIO $ atomically $ do
        readTVar (peerChannel peer) >>= \case
            ChannelWait -> do
                sendToPeerPlain peer [ Acknowledged reqref, Rejected reqref ] $
                    TransportPacket (TransportHeader hitems) [storedRef req]
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
                    let accref = (partialRef (peerInStorage peer) $ storedRef acc)
                        header = TrChannelAccept accref
                        ackedBy = [ Acknowledged accref, Rejected accref ]
                    sendToPeerPlain peer ackedBy $ TransportPacket (TransportHeader [header]) $ concat
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
                        sendToPeerS peer [] $ TransportPacket (TransportHeader [Acknowledged accref]) []
                        writeTVar (peerChannel peer) $ ChannelEstablished ch
                        finalizedChannel peer oh identity

                Left dgst -> throwError $ "missing accept data " ++ BC.unpack (showRefDigest dgst)


finalizedChannel :: Peer -> Head LocalState -> UnifiedIdentity -> STM ()
finalizedChannel peer oh self = do
    let ist = peerInStorage peer

    -- Identity update
    do
        let selfRef = partialRef ist $ storedRef $ idData $ self
            updateRefs = selfRef : map (partialRef ist . storedRef) (idUpdates self)
            ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- updateRefs ]
        sendToPeerS peer ackedBy $ flip TransportPacket [] $ TransportHeader $ map AnnounceUpdate updateRefs

    -- Shared state
    readTVar (peerIdentityVar peer) >>= \case
        PeerIdentityFull pid | finalOwner pid `sameIdentity` finalOwner self -> do
            writeTQueue (serverIOActions $ peerServer peer) $ do
                Just h <- liftIO $ reloadHead oh
                let shared = lsShared $ headObject h
                let hitems = (ServiceType $ serviceID @SyncService Proxy) : map ServiceRef srefs
                    srefs = map (partialRef ist . storedRef) shared
                    ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- srefs ]
                liftIO $ atomically $ sendToPeerS peer ackedBy $
                    TransportPacket (TransportHeader hitems) $ map storedRef shared
        _ -> return ()

    -- Outstanding service packets
    mapM_ (uncurry $ sendToPeerS peer) =<< swapTVar (peerServiceOutQueue peer) []


handleIdentityAnnounce :: UnifiedIdentity -> Peer -> Ref -> WaitingRefCallback
handleIdentityAnnounce self peer ref = liftIO $ atomically $ do
    let validateAndUpdate upds act = case validateIdentity $ wrappedLoad ref of
            Just pid' -> do
                let pid = fromMaybe pid' $ toUnifiedIdentity (updateIdentity upds pid')
                writeTVar (peerIdentityVar peer) $ PeerIdentityFull pid
                writeTChan (serverChanPeer $ peerServer peer) peer
                act pid
                writeTQueue (serverIOActions $ peerServer peer) $ do
                    setupChannel self peer pid
            Nothing -> return ()

    readTVar (peerIdentityVar peer) >>= \case
        PeerIdentityRef wref wact
            | wrDigest wref == refDigest ref
            -> validateAndUpdate [] $ \pid -> do
                mapM_ (writeTQueue (serverIOActions $ peerServer peer) . ($ pid)) .
                    reverse =<< readTVar wact

        PeerIdentityFull pid
            | idData pid `precedes` wrappedLoad ref
            -> validateAndUpdate (idUpdates pid) $ \_ -> return ()

        _ -> return ()

handleIdentityUpdate :: Peer -> Ref -> WaitingRefCallback
handleIdentityUpdate peer ref = liftIO $ atomically $ do
    pidentity <- readTVar (peerIdentityVar peer)
    if  | PeerIdentityFull pid <- pidentity
        , Just pid' <- toUnifiedIdentity $ updateIdentity [wrappedLoad ref] pid
        -> do
            writeTVar (peerIdentityVar peer) $ PeerIdentityFull pid'
            writeTChan (serverChanPeer $ peerServer peer) peer

        | otherwise -> return ()


mkPeer :: Server -> PeerAddress -> IO Peer
mkPeer server paddr = do
    pst <- deriveEphemeralStorage $ serverStorage server
    peer <- Peer
        <$> pure paddr
        <*> pure server
        <*> (newTVarIO . PeerIdentityUnknown =<< newTVarIO [])
        <*> newTVarIO ChannelWait
        <*> pure pst
        <*> derivePartialStorage pst
        <*> newTQueueIO
        <*> newTVarIO []
        <*> newTMVarIO M.empty
        <*> newTVarIO []
        <*> newTMVarIO []
    void $ forkIO $ sendWorker peer
    return peer

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
        identity <- serverIdentity server
        atomically $ writeTQueue (peerOutQueue peer) $ (False, [],) $
            TransportPacket
                (TransportHeader [ AnnounceSelf $ partialRef (peerInStorage peer) $ storedRef $ idData identity ])
                []
    return peer


sendToPeer :: (Service s, MonadIO m) => Peer -> s -> m ()
sendToPeer peer packet = sendToPeerList peer [ServiceReply (Left packet) True]

sendToPeerStored :: (Service s, MonadIO m) => Peer -> Stored s -> m ()
sendToPeerStored peer spacket = sendToPeerList peer [ServiceReply (Right spacket) True]

sendToPeerList :: (Service s, MonadIO m) => Peer -> [ServiceReply s] -> m ()
sendToPeerList peer parts = do
    let st = peerStorage peer
        pst = peerInStorage peer
    srefs <- liftIO $ fmap catMaybes $ forM parts $ \case
        ServiceReply (Left x) use -> Just . (,use) <$> store st x
        ServiceReply (Right sx) use -> return $ Just (storedRef sx, use)
        ServiceFinally act -> act >> return Nothing
    prefs <- mapM (copyRef pst . fst) srefs
    let content = map fst $ filter snd srefs
        header = TransportHeader (ServiceType (serviceID $ head parts) : map ServiceRef prefs)
        packet = TransportPacket header content
        ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- prefs ]
    liftIO $ atomically $ sendToPeerS peer ackedBy packet

sendToPeerS :: Peer -> [TransportHeaderItem] -> TransportPacket -> STM ()
sendToPeerS peer ackedBy packet = writeTQueue (peerOutQueue peer) (True, ackedBy, packet)

sendToPeerPlain :: Peer -> [TransportHeaderItem] -> TransportPacket -> STM ()
sendToPeerPlain peer ackedBy packet = writeTQueue (peerOutQueue peer) (False, ackedBy, packet)

sendToPeerWith :: forall s m. (Service s, MonadIO m, MonadError String m) => Peer -> (ServiceState s -> ExceptT String IO (Maybe s, ServiceState s)) -> m ()
sendToPeerWith peer fobj = do
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
         Right (Just obj) -> sendToPeer peer obj
         Right Nothing -> return ()
         Left err -> throwError err


lookupService :: forall s. Service s => Proxy s -> [SomeService] -> Maybe (SomeService, ServiceAttributes s)
lookupService proxy (service@(SomeService (_ :: Proxy t) attr) : rest)
    | Just (Refl :: s :~: t) <- eqT = Just (service, attr)
    | otherwise = lookupService proxy rest
lookupService _ [] = Nothing

runPeerService :: forall s m. (Service s, MonadIO m) => Peer -> ServiceHandler s () -> m ()
runPeerService = runPeerServiceOn Nothing

runPeerServiceOn :: forall s m. (Service s, MonadIO m) => Maybe (SomeService, ServiceAttributes s) -> Peer -> ServiceHandler s () -> m ()
runPeerServiceOn mbservice peer handler = liftIO $ do
    let server = peerServer peer
        proxy = Proxy @s
        svc = serviceID proxy
        logd = writeTQueue (serverErrorLog server)
    case mbservice `mplus` lookupService proxy (serverServices server) of
        Just (service, attr) ->
            atomically (readTVar (peerIdentityVar peer)) >>= \case
                PeerIdentityFull peerId -> do
                    (global, svcs) <- atomically $ (,)
                        <$> takeTMVar (serverServiceStates server)
                        <*> takeTMVar (peerServiceState peer)
                    case (fromMaybe (someServiceEmptyState service) $ M.lookup svc svcs,
                            fromMaybe (someServiceEmptyGlobalState service) $ M.lookup svc global) of
                        ((SomeServiceState (_ :: Proxy ps) ps),
                                (SomeServiceGlobalState (_ :: Proxy gs) gs)) -> do
                            Just (Refl :: s :~: ps) <- return $ eqT
                            Just (Refl :: s :~: gs) <- return $ eqT

                            let inp = ServiceInput
                                    { svcAttributes = attr
                                    , svcPeer = peer
                                    , svcPeerIdentity = peerId
                                    , svcServer = server
                                    , svcPrintOp = atomically . logd
                                    }
                            reloadHead (serverOrigHead server) >>= \case
                                Nothing -> atomically $ do
                                    logd $ "current head deleted"
                                    putTMVar (peerServiceState peer) svcs
                                    putTMVar (serverServiceStates server) global
                                Just h -> do
                                    (rsp, (s', gs')) <- runServiceHandler h inp ps gs handler
                                    when (not (null rsp)) $ do
                                        sendToPeerList peer rsp
                                    atomically $ do
                                        putTMVar (peerServiceState peer) $ M.insert svc (SomeServiceState proxy s') svcs
                                        putTMVar (serverServiceStates server) $ M.insert svc (SomeServiceGlobalState proxy gs') global
                _ -> do
                    atomically $ logd $ "can't run service handler on peer with incomplete identity " ++ show (peerAddress peer)

        _ -> atomically $ do
            logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"


foreign import ccall unsafe "Network/ifaddrs.h broadcast_addresses" cBroadcastAddresses :: IO (Ptr Word32)
foreign import ccall unsafe "stdlib.h free" cFree :: Ptr Word32 -> IO ()

getBroadcastAddresses :: PortNumber -> IO [SockAddr]
getBroadcastAddresses port = do
    ptr <- cBroadcastAddresses
    let parse i = do
            w <- peekElemOff ptr i
            if w == 0 then return []
                      else (SockAddrInet port w:) <$> parse (i + 1)
    addrs <- parse 0
    cFree ptr
    return addrs

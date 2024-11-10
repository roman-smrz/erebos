{-# LANGUAGE CPP #-}

module Erebos.Network (
    Server,
    startServer,
    stopServer,
    getCurrentPeerList,
    getNextPeerChange,
    ServerOptions(..), serverIdentity, defaultServerOptions,

    Peer, peerServer, peerStorage,
    PeerAddress(..), peerAddress,
    PeerIdentity(..), peerIdentity,
    WaitingRef, wrDigest,
    Service(..),
    serverPeer,
#ifdef ENABLE_ICE_SUPPORT
    serverPeerIce,
#endif
    dropPeer,
    isPeerDropped,
    sendToPeer, sendManyToPeer,
    sendToPeerStored, sendManyToPeerStored,
    sendToPeerWith,
    runPeerService,

    discoveryPort,
) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State

import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
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

import Network.Socket hiding (ControlMessage)
import qualified Network.Socket.ByteString as S

import Foreign.C.Types
import Foreign.Marshal.Alloc

#ifdef ENABLE_ICE_SUPPORT
import Erebos.ICE
#endif
import Erebos.Identity
import Erebos.Network.Channel
import Erebos.Network.Protocol
import Erebos.Object.Internal
import Erebos.PubKey
import Erebos.Service
import Erebos.State
import Erebos.Storage.Key
import Erebos.Storage.Merge


discoveryPort :: PortNumber
discoveryPort = 29665

discoveryMulticastGroup :: HostAddress6
discoveryMulticastGroup = tupleToHostAddress6 (0xff12, 0xb6a4, 0x6b1f, 0x0969, 0xcaee, 0xacc2, 0x5c93, 0x73e1) -- ff12:b6a4:6b1f:969:caee:acc2:5c93:73e1

announceIntervalSeconds :: Int
announceIntervalSeconds = 60


data Server = Server
    { serverStorage :: Storage
    , serverOrigHead :: Head LocalState
    , serverIdentity_ :: MVar UnifiedIdentity
    , serverThreads :: MVar [ThreadId]
    , serverSocket :: MVar Socket
    , serverRawPath :: SymFlow (PeerAddress, BC.ByteString)
    , serverControlFlow :: Flow (ControlMessage PeerAddress) (ControlRequest PeerAddress)
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

getCurrentPeerList :: Server -> IO [Peer]
getCurrentPeerList = fmap M.elems . readMVar . serverPeers

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
    , peerState :: TVar PeerState
    , peerIdentityVar :: TVar PeerIdentity
    , peerStorage_ :: Storage
    , peerInStorage :: PartialStorage
    , peerServiceState :: TMVar (M.Map ServiceID SomeServiceState)
    , peerWaitingRefs :: TMVar [WaitingRef]
    }

peerServer :: Peer -> Server
peerServer = peerServer_

peerStorage :: Peer -> Storage
peerStorage = peerStorage_

getPeerChannel :: Peer -> STM ChannelState
getPeerChannel Peer {..} =
    readTVar peerState >>= \case
        PeerInit _ -> return ChannelNone
        PeerConnected conn -> connGetChannel conn
        PeerDropped -> return ChannelClosed

setPeerChannel :: Peer -> ChannelState -> STM ()
setPeerChannel Peer {..} ch = do
    readTVar peerState >>= \case
        PeerInit _ -> retry
        PeerConnected conn -> connSetChannel conn ch
        PeerDropped -> return ()

instance Eq Peer where
    (==) = (==) `on` peerIdentityVar

data PeerAddress = DatagramAddress SockAddr
#ifdef ENABLE_ICE_SUPPORT
                 | PeerIceSession IceSession
#endif

instance Show PeerAddress where
    show (DatagramAddress saddr) = unwords $ case IP.fromSockAddr saddr of
        Just (IP.IPv6 ipv6, port)
            | (0, 0, 0xffff, ipv4) <- IP.fromIPv6w ipv6
            -> [show (IP.toIPv4w ipv4), show port]
        Just (addr, port)
            -> [show addr, show port]
        _ -> [show saddr]
#ifdef ENABLE_ICE_SUPPORT
    show (PeerIceSession ice) = show ice
#endif

instance Eq PeerAddress where
    DatagramAddress addr == DatagramAddress addr' = addr == addr'
#ifdef ENABLE_ICE_SUPPORT
    PeerIceSession ice   == PeerIceSession ice'   = ice == ice'
    _                    == _                     = False
#endif

instance Ord PeerAddress where
    compare (DatagramAddress addr) (DatagramAddress addr') = compare addr addr'
#ifdef ENABLE_ICE_SUPPORT
    compare (DatagramAddress _   ) _                       = LT
    compare _                      (DatagramAddress _    ) = GT
    compare (PeerIceSession ice  ) (PeerIceSession ice')   = compare ice ice'
#endif


data PeerIdentity = PeerIdentityUnknown (TVar [UnifiedIdentity -> ExceptT String IO ()])
                  | PeerIdentityRef WaitingRef (TVar [UnifiedIdentity -> ExceptT String IO ()])
                  | PeerIdentityFull UnifiedIdentity

peerIdentity :: MonadIO m => Peer -> m PeerIdentity
peerIdentity = liftIO . atomically . readTVar . peerIdentityVar


data PeerState = PeerInit [(SecurityRequirement, TransportPacket Ref, [TransportHeaderItem])]
               | PeerConnected (Connection PeerAddress)
               | PeerDropped


lookupServiceType :: [TransportHeaderItem] -> Maybe ServiceID
lookupServiceType (ServiceType stype : _) = Just stype
lookupServiceType (_ : hs) = lookupServiceType hs
lookupServiceType [] = Nothing

lookupNewStreams :: [TransportHeaderItem] -> [Word8]
lookupNewStreams (StreamOpen num : rest) = num : lookupNewStreams rest
lookupNewStreams (_ : rest) = lookupNewStreams rest
lookupNewStreams [] = []


newWaitingRef :: RefDigest -> (Ref -> WaitingRefCallback) -> PacketHandler WaitingRef
newWaitingRef dgst act = do
    peer@Peer {..} <- gets phPeer
    wref <- WaitingRef peerStorage_ (partialRefFromDigest peerInStorage dgst) act <$> liftSTM (newTVar (Left []))
    modifyTMVarP peerWaitingRefs (wref:)
    liftSTM $ writeTQueue (serverDataResponse $ peerServer peer) (peer, Nothing)
    return wref


forkServerThread :: Server -> IO () -> IO ()
forkServerThread server act = do
    modifyMVar_ (serverThreads server) $ \ts -> do
        t <- forkIO $ do
            t <- myThreadId
            act
            modifyMVar_ (serverThreads server) $ return . filter (/=t)
        return (t:ts)

startServer :: ServerOptions -> Head LocalState -> (String -> IO ()) -> [SomeService] -> IO Server
startServer opt serverOrigHead logd' serverServices = do
    let serverStorage = headStorage serverOrigHead
    serverIdentity_ <- newMVar $ headLocalIdentity serverOrigHead
    serverThreads <- newMVar []
    serverSocket <- newEmptyMVar
    (serverRawPath, protocolRawPath) <- newFlowIO
    (serverControlFlow, protocolControlFlow) <- newFlowIO
    serverDataResponse <- newTQueueIO
    serverIOActions <- newTQueueIO
    serverServiceStates <- newTMVarIO M.empty
    serverPeers <- newMVar M.empty
    serverChanPeer <- newTChanIO
    serverErrorLog <- newTQueueIO
    let server = Server {..}

    chanSvc <- newTQueueIO

    let logd = writeTQueue serverErrorLog
    forkServerThread server $ forever $ do
        logd' =<< atomically (readTQueue serverErrorLog)

    forkServerThread server $ dataResponseWorker server
    forkServerThread server $ forever $ do
        either (atomically . logd) return =<< runExceptT =<<
            atomically (readTQueue serverIOActions)

    let open addr = do
            sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
            putMVar serverSocket sock
            setSocketOption sock ReuseAddr 1
            setSocketOption sock Broadcast 1
            withFdSocket sock setCloseOnExecIfNeeded
            bind sock (addrAddress addr)
            return sock

        loop sock = do
            when (serverLocalDiscovery opt) $ forkServerThread server $ do
                announceAddreses <- fmap concat $ sequence $
                    [ map (SockAddrInet6 discoveryPort 0 discoveryMulticastGroup) <$> joinMulticast sock
                    , getBroadcastAddresses discoveryPort
                    ]
                forever $ do
                    atomically $ writeFlowBulk serverControlFlow $ map (SendAnnounce . DatagramAddress) announceAddreses
                    threadDelay $ announceIntervalSeconds * 1000 * 1000

            let announceUpdate identity = do
                    st <- derivePartialStorage serverStorage
                    let selfRef = partialRef st $ storedRef $ idExtData identity
                        updateRefs = map refDigest $ selfRef : map (partialRef st . storedRef) (idUpdates identity)
                        ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- updateRefs ]
                        hitems = map AnnounceUpdate updateRefs
                        packet = TransportPacket (TransportHeader $  hitems) []

                    ps <- readMVar serverPeers
                    forM_ ps $ \peer -> atomically $ do
                        ((,) <$> readTVar (peerIdentityVar peer) <*> getPeerChannel peer) >>= \case
                            (PeerIdentityFull _, ChannelEstablished _) ->
                                sendToPeerS peer ackedBy packet
                            _  -> return ()

            void $ watchHead serverOrigHead $ \h -> do
                let idt = headLocalIdentity h
                changedId <- modifyMVar serverIdentity_ $ \cur ->
                    return (idt, cur /= idt)
                when changedId $ do
                    writeFlowIO serverControlFlow $ UpdateSelfIdentity idt
                    announceUpdate idt

            forM_ serverServices $ \(SomeService service _) -> do
                forM_ (serviceStorageWatchers service) $ \(SomeStorageWatcher sel act) -> do
                    watchHeadWith serverOrigHead (sel . headStoredObject) $ \x -> do
                        withMVar serverPeers $ mapM_ $ \peer -> atomically $ do
                            readTVar (peerIdentityVar peer) >>= \case
                                PeerIdentityFull _ -> writeTQueue serverIOActions $ do
                                    runPeerService peer $ act x
                                _ -> return ()

            forkServerThread server $ forever $ do
                (msg, saddr) <- S.recvFrom sock 4096
                writeFlowIO serverRawPath (DatagramAddress saddr, msg)

            forkServerThread server $ forever $ do
                (paddr, msg) <- readFlowIO serverRawPath
                handle (\(e :: IOException) -> atomically . logd $ "failed to send packet to " ++ show paddr ++ ": " ++ show e) $ do
                    case paddr of
                        DatagramAddress addr -> void $ S.sendTo sock msg addr
#ifdef ENABLE_ICE_SUPPORT
                        PeerIceSession ice   -> iceSend ice msg
#endif

            forkServerThread server $ forever $ do
                readFlowIO serverControlFlow >>= \case
                    NewConnection conn mbpid -> do
                        let paddr = connAddress conn
                        peer <- modifyMVar serverPeers $ \pvalue -> do
                            case M.lookup paddr pvalue of
                                Just peer -> return (pvalue, peer)
                                Nothing -> do
                                    peer <- mkPeer server paddr
                                    return (M.insert paddr peer pvalue, peer)

                        forkServerThread server $ do
                            atomically $ do
                                readTVar (peerState peer) >>= \case
                                    PeerInit packets -> do
                                        writeFlowBulk (connData conn) $ reverse packets
                                        writeTVar (peerState peer) (PeerConnected conn)
                                    PeerConnected _ -> do
                                        writeTVar (peerState peer) (PeerConnected conn)
                                    PeerDropped -> do
                                        connClose conn

                            case mbpid of
                                Just dgst -> do
                                    identity <- readMVar serverIdentity_
                                    atomically $ runPacketHandler False peer $ do
                                        wref <- newWaitingRef dgst $ handleIdentityAnnounce identity peer
                                        readTVarP (peerIdentityVar peer) >>= \case
                                            PeerIdentityUnknown idwait -> do
                                                addHeader $ AnnounceSelf $ refDigest $ storedRef $ idData identity
                                                writeTVarP (peerIdentityVar peer) $ PeerIdentityRef wref idwait
                                                liftSTM $ writeTChan serverChanPeer peer
                                            _ -> return ()
                                Nothing -> return ()

                            let peerLoop = readFlowIO (connData conn) >>= \case
                                    Just (secure, TransportPacket header objs) -> do
                                        prefs <- forM objs $ storeObject $ peerInStorage peer
                                        identity <- readMVar serverIdentity_
                                        let svcs = map someServiceID serverServices
                                        handlePacket identity secure peer chanSvc svcs header prefs
                                        peerLoop
                                    Nothing -> do
                                        dropPeer peer
                                        atomically $ writeTChan serverChanPeer peer
                            peerLoop

                    ReceivedAnnounce addr _ -> do
                        void $ serverPeer' server addr

            erebosNetworkProtocol (headLocalIdentity serverOrigHead) logd protocolRawPath protocolControlFlow

    forkServerThread server $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrFamily = AF_INET6
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just $ show $ serverPort opt)
        bracket (open addr) close loop

    forkServerThread server $ forever $ do
        (peer, svc, ref) <- atomically $ readTQueue chanSvc
        case find ((svc ==) . someServiceID) serverServices of
            Just service@(SomeService (_ :: Proxy s) attr) -> runPeerServiceOn (Just (service, attr)) peer (serviceHandler $ wrappedLoad @s ref)
            _ -> atomically $ logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"

    return server

stopServer :: Server -> IO ()
stopServer Server {..} = do
    mapM_ killThread =<< takeMVar serverThreads

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
                              forkServerThread server $ runExceptT (wrefAction wr ref) >>= \case
                                  Left err -> atomically $ writeTQueue (serverErrorLog server) err
                                  Right () -> return ()

                              return (Nothing, [])
                          Left dgst -> do
                              atomically (writeTVar tvar $ Left [dgst])
                              return (Just wr, [dgst])
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

runPacketHandler :: Bool -> Peer -> PacketHandler () -> STM ()
runPacketHandler secure peer@Peer {..} act = do
    let logd = writeTQueue $ serverErrorLog peerServer_
    runExceptT (flip execStateT (PacketHandlerState peer [] [] [] Nothing False) $ unPacketHandler act) >>= \case
        Left err -> do
            logd $ "Error in handling packet from " ++ show peerAddress ++ ": " ++ err
        Right ph -> do
            when (not $ null $ phHead ph) $ do
                body <- case phBodyStream ph of
                    Nothing -> return $ phBody ph
                    Just stream -> do
                        writeTQueue (serverIOActions peerServer_) $ void $ liftIO $ forkIO $ do
                            writeByteStringToStream stream $ BL.concat $ map lazyLoadBytes $ phBody ph
                        return []
                let packet = TransportPacket (TransportHeader $ phHead ph) body
                    secreq = case (secure, phPlaintextReply ph) of
                                  (True, _) -> EncryptedOnly
                                  (False, False) -> PlaintextAllowed
                                  (False, True) -> PlaintextOnly
                sendToPeerS' secreq peer (phAckedBy ph) packet

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
    , phBodyStream :: Maybe RawStreamWriter
    , phPlaintextReply :: Bool
    }

addHeader :: TransportHeaderItem -> PacketHandler ()
addHeader h = modify $ \ph -> ph { phHead = h `appendDistinct` phHead ph }

addAckedBy :: [TransportHeaderItem] -> PacketHandler ()
addAckedBy hs = modify $ \ph -> ph { phAckedBy = foldr appendDistinct (phAckedBy ph) hs }

addBody :: Ref -> PacketHandler ()
addBody r = modify $ \ph -> ph { phBody = r `appendDistinct` phBody ph }

sendBodyAsStream :: PacketHandler ()
sendBodyAsStream = do
    gets phBodyStream >>= \case
        Nothing -> do
            stream <- openStream
            modify $ \ph -> ph { phBodyStream = Just stream }
        Just _ -> return ()

keepPlaintextReply :: PacketHandler ()
keepPlaintextReply = modify $ \ph -> ph { phPlaintextReply = True }

openStream :: PacketHandler RawStreamWriter
openStream = do
    Peer {..} <- gets phPeer
    conn <- readTVarP peerState >>= \case
        PeerConnected conn -> return conn
        _                  -> throwError "can't open stream without established connection"
    (hdr, writer, handler) <- liftSTM (connAddWriteStream conn) >>= \case
        Right res -> return res
        Left err -> throwError err

    liftSTM $ writeTQueue (serverIOActions peerServer_) (liftIO $ forkServerThread peerServer_ handler)
    addHeader hdr
    return writer

acceptStream :: Word8 -> PacketHandler RawStreamReader
acceptStream streamNumber = do
    Peer {..} <- gets phPeer
    conn <- readTVarP peerState >>= \case
        PeerConnected conn -> return conn
        _                  -> throwError "can't accept stream without established connection"
    liftSTM $ connAddReadStream conn streamNumber

appendDistinct :: Eq a => a -> [a] -> [a]
appendDistinct x (y:ys) | x == y    = y : ys
                        | otherwise = y : appendDistinct x ys
appendDistinct x [] = [x]

handlePacket :: UnifiedIdentity -> Bool
    -> Peer -> TQueue (Peer, ServiceID, Ref) -> [ServiceID]
    -> TransportHeader -> [PartialRef] -> IO ()
handlePacket identity secure peer chanSvc svcs (TransportHeader headers) prefs = atomically $ do
    let server = peerServer peer
    ochannel <- getPeerChannel peer
    let sidentity = idData identity
        plaintextRefs = map (refDigest . storedRef) $ concatMap (collectStoredObjects . wrappedLoad) $ concat
            [ [ storedRef sidentity ]
            , map storedRef $ idUpdates identity
            , case ochannel of
                   ChannelOurRequest req  -> [ storedRef req ]
                   ChannelOurAccept acc _ -> [ storedRef acc ]
                   _                      -> []
            ]

    runPacketHandler secure peer $ do
        let logd = liftSTM . writeTQueue (serverErrorLog server)
        forM_ headers $ \case
            Acknowledged dgst -> do
                liftSTM (getPeerChannel peer) >>= \case
                    ChannelOurAccept acc ch | refDigest (storedRef acc) == dgst -> do
                        liftSTM $ finalizedChannel peer ch identity
                    _ -> return ()

            Rejected dgst
                | peerRequest : _ <- mapMaybe (\case TrChannelRequest d -> Just d; _ -> Nothing) headers
                , peerRequest < dgst
                -> return () -- Our request was rejected due to lower priority

                | otherwise -> logd $ "rejected by peer: " ++ show dgst

            DataRequest dgst
                | secure || dgst `elem` plaintextRefs -> do
                    Right mref <- liftSTM $ unsafeIOToSTM $
                        copyRef (peerStorage peer) $
                        partialRefFromDigest (peerInStorage peer) dgst
                    addHeader $ DataResponse dgst
                    addAckedBy [ Acknowledged dgst, Rejected dgst ]

                    -- Plaintext request may indicate the peer has restarted/changed or
                    -- otherwise lost the channel, so keep the reply plaintext as well.
                    when (not secure) keepPlaintextReply

                    -- TODO: MTU
                    when (secure && BL.length (lazyLoadBytes mref) > 500)
                        sendBodyAsStream

                    addBody $ mref
                | otherwise -> do
                    logd $ "unauthorized data request for " ++ show dgst
                    addHeader $ Rejected dgst

            DataResponse dgst -> if
                | Just pref <- find ((==dgst) . refDigest) prefs -> do
                    when (not secure) $ do
                        addHeader $ Acknowledged dgst
                    liftSTM $ writeTQueue (serverDataResponse server) (peer, Just pref)

                | streamNumber : _ <- lookupNewStreams headers -> do
                    streamReader <- acceptStream streamNumber
                    liftSTM $ writeTQueue (serverIOActions server) $ void $ liftIO $ forkIO $ do
                        (runExcept <$> readObjectsFromStream (peerInStorage peer) streamReader) >>= \case
                            Left err -> atomically $ writeTQueue (serverErrorLog server) $
                                "failed to receive object from stream: " <> err
                            Right objs -> do
                                forM_ objs $ \obj -> do
                                    pref <- storeObject (peerInStorage peer) obj
                                    atomically $ writeTQueue (serverDataResponse server) (peer, Just pref)

                | otherwise -> throwError $ "mismatched data response " ++ show dgst

            AnnounceSelf dgst
                | dgst == refDigest (storedRef sidentity) -> return ()
                | otherwise -> do
                    wref <- newWaitingRef dgst $ handleIdentityAnnounce identity peer
                    readTVarP (peerIdentityVar peer) >>= \case
                        PeerIdentityUnknown idwait -> do
                            addHeader $ AnnounceSelf $ refDigest $ storedRef $ idData identity
                            writeTVarP (peerIdentityVar peer) $ PeerIdentityRef wref idwait
                            liftSTM $ writeTChan (serverChanPeer $ peerServer peer) peer
                        _ -> return ()

            AnnounceUpdate dgst -> do
                readTVarP (peerIdentityVar peer) >>= \case
                    PeerIdentityFull _ -> do
                        void $ newWaitingRef dgst $ handleIdentityUpdate peer
                    _ -> return ()

            TrChannelRequest dgst -> do
                let process = do
                        addHeader $ Acknowledged dgst
                        wref <- newWaitingRef dgst $ handleChannelRequest peer identity
                        liftSTM $ setPeerChannel peer $ ChannelPeerRequest wref
                    reject = addHeader $ Rejected dgst

                liftSTM (getPeerChannel peer) >>= \case
                    ChannelNone {} -> return ()
                    ChannelCookieWait {} -> return ()
                    ChannelCookieReceived {} -> process
                    ChannelCookieConfirmed {} -> process
                    ChannelOurRequest our
                        | dgst < refDigest (storedRef our) -> process
                        | otherwise -> do
                            -- Reject peer channel request with lower priority
                            addHeader $ TrChannelRequest $ refDigest $ storedRef our
                            reject
                    ChannelPeerRequest prev
                        | dgst == wrDigest prev -> addHeader $ Acknowledged dgst
                        | otherwise -> process
                    ChannelOurAccept {} -> reject
                    ChannelEstablished {} -> process
                    ChannelClosed {} -> return ()

            TrChannelAccept dgst -> do
                let process = do
                        handleChannelAccept identity $ partialRefFromDigest (peerInStorage peer) dgst
                    reject = addHeader $ Rejected dgst
                liftSTM (getPeerChannel peer) >>= \case
                    ChannelNone {} -> reject
                    ChannelCookieWait {} -> reject
                    ChannelCookieReceived {} -> reject
                    ChannelCookieConfirmed {} -> reject
                    ChannelOurRequest {} -> process
                    ChannelPeerRequest {} -> process
                    ChannelOurAccept our _ | dgst < refDigest (storedRef our) -> process
                                           | otherwise -> addHeader $ Rejected dgst
                    ChannelEstablished {} -> process
                    ChannelClosed {} -> return ()

            ServiceType _ -> return ()
            ServiceRef dgst
                | not secure -> throwError $ "service packet without secure channel"
                | Just svc <- lookupServiceType headers -> if
                    | svc `elem` svcs -> do
                        if dgst `elem` map refDigest prefs || True {- TODO: used by Message service to confirm receive -}
                           then do
                                void $ newWaitingRef dgst $ \ref ->
                                    liftIO $ atomically $ writeTQueue chanSvc (peer, svc, ref)
                           else throwError $ "missing service object " ++ show dgst
                    | otherwise -> addHeader $ Rejected dgst
                | otherwise -> throwError $ "service ref without type"

            _ -> return ()


withPeerIdentity :: MonadIO m => Peer -> (UnifiedIdentity -> ExceptT String IO ()) -> m ()
withPeerIdentity peer act = liftIO $ atomically $ readTVar (peerIdentityVar peer) >>= \case
    PeerIdentityUnknown tvar -> modifyTVar' tvar (act:)
    PeerIdentityRef _ tvar -> modifyTVar' tvar (act:)
    PeerIdentityFull idt -> writeTQueue (serverIOActions $ peerServer peer) (act idt)


setupChannel :: UnifiedIdentity -> Peer -> UnifiedIdentity -> WaitingRefCallback
setupChannel identity peer upid = do
    req <- flip runReaderT (peerStorage peer) $ createChannelRequest identity upid
    let reqref = refDigest $ storedRef req
    let hitems =
            [ TrChannelRequest reqref
            , AnnounceSelf $ refDigest $ storedRef $ idData identity
            ]
    let sendChannelRequest = do
            sendToPeerPlain peer [ Acknowledged reqref, Rejected reqref ] $
                TransportPacket (TransportHeader hitems) [storedRef req]
            setPeerChannel peer $ ChannelOurRequest req
    liftIO $ atomically $ do
        getPeerChannel peer >>= \case
            ChannelCookieReceived -> sendChannelRequest
            ChannelCookieConfirmed -> sendChannelRequest
            _ -> return ()

handleChannelRequest :: Peer -> UnifiedIdentity -> Ref -> WaitingRefCallback
handleChannelRequest peer identity req = do
    withPeerIdentity peer $ \upid -> do
        (acc, ch) <- flip runReaderT (peerStorage peer) $ acceptChannelRequest identity upid (wrappedLoad req)
        liftIO $ atomically $ do
            getPeerChannel peer >>= \case
                ChannelPeerRequest wr | wrDigest wr == refDigest req -> do
                    setPeerChannel peer $ ChannelOurAccept acc ch
                    let accref = refDigest $ storedRef acc
                        header = TrChannelAccept accref
                        ackedBy = [ Acknowledged accref, Rejected accref ]
                    sendToPeerPlain peer ackedBy $ TransportPacket (TransportHeader [header]) $ concat
                        [ [ storedRef $ acc ]
                        , [ storedRef $ signedData $ fromStored acc ]
                        , [ storedRef $ caKey $ fromStored $ signedData $ fromStored acc ]
                        , map storedRef $ signedSignature $ fromStored acc
                        ]
                _ -> writeTQueue (serverErrorLog $ peerServer peer) $ "unexpected channel request"

handleChannelAccept :: UnifiedIdentity -> PartialRef -> PacketHandler ()
handleChannelAccept identity accref = do
    peer <- gets phPeer
    liftSTM $ writeTQueue (serverIOActions $ peerServer peer) $ do
        withPeerIdentity peer $ \upid -> do
            copyRef (peerStorage peer) accref >>= \case
                Right acc -> do
                    ch <- acceptedChannel identity upid (wrappedLoad acc)
                    liftIO $ atomically $ do
                        sendToPeerS peer [] $ TransportPacket (TransportHeader [Acknowledged $ refDigest accref]) []
                        finalizedChannel peer ch identity

                Left dgst -> throwError $ "missing accept data " ++ BC.unpack (showRefDigest dgst)


finalizedChannel :: Peer -> Channel -> UnifiedIdentity -> STM ()
finalizedChannel peer@Peer {..} ch self = do
    setPeerChannel peer $ ChannelEstablished ch

    -- Identity update
    writeTQueue (serverIOActions peerServer_) $ liftIO $ atomically $ do
        let selfRef = refDigest $ storedRef $ idExtData $ self
            updateRefs = selfRef : map (refDigest . storedRef) (idUpdates self)
            ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- updateRefs ]
        sendToPeerS peer ackedBy $ flip TransportPacket [] $ TransportHeader $ map AnnounceUpdate updateRefs

    -- Notify services about new peer
    readTVar peerIdentityVar >>= \case
        PeerIdentityFull _ -> notifyServicesOfPeer peer
        _ -> return ()


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
            -> validateAndUpdate (idUpdates pid) $ \_ -> do
                notifyServicesOfPeer peer

        _ -> return ()

handleIdentityUpdate :: Peer -> Ref -> WaitingRefCallback
handleIdentityUpdate peer ref = liftIO $ atomically $ do
    pidentity <- readTVar (peerIdentityVar peer)
    if  | PeerIdentityFull pid <- pidentity
        , Just pid' <- toUnifiedIdentity $ updateIdentity [wrappedLoad ref] pid
        -> do
            writeTVar (peerIdentityVar peer) $ PeerIdentityFull pid'
            writeTChan (serverChanPeer $ peerServer peer) peer
            when (idData pid /= idData pid') $ notifyServicesOfPeer peer

        | otherwise -> return ()

notifyServicesOfPeer :: Peer -> STM ()
notifyServicesOfPeer peer@Peer { peerServer_ = Server {..} } = do
    writeTQueue serverIOActions $ do
        forM_ serverServices $ \service@(SomeService _ attrs) ->
            runPeerServiceOn (Just (service, attrs)) peer serviceNewPeer


mkPeer :: Server -> PeerAddress -> IO Peer
mkPeer peerServer_ peerAddress = do
    peerState <- newTVarIO (PeerInit [])
    peerIdentityVar <- newTVarIO . PeerIdentityUnknown =<< newTVarIO []
    peerStorage_ <- deriveEphemeralStorage $ serverStorage peerServer_
    peerInStorage <- derivePartialStorage peerStorage_
    peerServiceState <- newTMVarIO M.empty
    peerWaitingRefs <- newTMVarIO []
    return Peer {..}

serverPeer :: Server -> SockAddr -> IO Peer
serverPeer server paddr = do
    let paddr' = case IP.fromSockAddr paddr of
            Just (IP.IPv4 ipv4, port)
                -> IP.toSockAddr (IP.IPv6 $ IP.toIPv6w (0, 0, 0xffff, IP.fromIPv4w ipv4), port)
            _   -> paddr
    serverPeer' server (DatagramAddress paddr')

#ifdef ENABLE_ICE_SUPPORT
serverPeerIce :: Server -> IceSession -> IO Peer
serverPeerIce server@Server {..} ice = do
    let paddr = PeerIceSession ice
    peer <- serverPeer' server paddr
    iceSetChan ice $ mapFlow undefined (paddr,) serverRawPath
    return peer
#endif

serverPeer' :: Server -> PeerAddress -> IO Peer
serverPeer' server paddr = do
    (peer, hello) <- modifyMVar (serverPeers server) $ \pvalue -> do
        case M.lookup paddr pvalue of
             Just peer -> return (pvalue, (peer, False))
             Nothing -> do
                 peer <- mkPeer server paddr
                 return (M.insert paddr peer pvalue, (peer, True))
    when hello $ atomically $ do
        writeFlow (serverControlFlow server) (RequestConnection paddr)
    return peer

dropPeer :: MonadIO m => Peer -> m ()
dropPeer peer = liftIO $ do
    modifyMVar_ (serverPeers $ peerServer peer) $ \pvalue -> do
        atomically $ do
            readTVar (peerState peer) >>= \case
                PeerConnected conn -> connClose conn
                _ -> return()
            writeTVar (peerState peer) PeerDropped
        return $ M.delete (peerAddress peer) pvalue

isPeerDropped :: MonadIO m => Peer -> m Bool
isPeerDropped peer = liftIO $ atomically $ readTVar (peerState peer) >>= \case
    PeerDropped -> return True
    _           -> return False

sendToPeer :: (Service s, MonadIO m) => Peer -> s -> m ()
sendToPeer peer = sendManyToPeer peer . (: [])

sendManyToPeer :: (Service s, MonadIO m) => Peer -> [ s ] -> m ()
sendManyToPeer peer = sendToPeerList peer . map (\part -> ServiceReply (Left part) True)

sendToPeerStored :: (Service s, MonadIO m) => Peer -> Stored s -> m ()
sendToPeerStored peer = sendManyToPeerStored peer . (: [])

sendManyToPeerStored :: (Service s, MonadIO m) => Peer -> [ Stored s ] -> m ()
sendManyToPeerStored peer = sendToPeerList peer . map (\part -> ServiceReply (Right part) True)

sendToPeerList :: (Service s, MonadIO m) => Peer -> [ServiceReply s] -> m ()
sendToPeerList peer parts = do
    let st = peerStorage peer
    srefs <- liftIO $ fmap catMaybes $ forM parts $ \case
        ServiceReply (Left x) use -> Just . (,use) <$> store st x
        ServiceReply (Right sx) use -> return $ Just (storedRef sx, use)
        ServiceFinally act -> act >> return Nothing
    let dgsts = map (refDigest . fst) srefs
    let content = map fst $ filter (\(ref, use) -> use && BL.length (lazyLoadBytes ref) < 500) srefs -- TODO: MTU
        header = TransportHeader (ServiceType (serviceID $ head parts) : map ServiceRef dgsts)
        packet = TransportPacket header content
        ackedBy = concat [[ Acknowledged r, Rejected r, DataRequest r ] | r <- dgsts ]
    liftIO $ atomically $ sendToPeerS peer ackedBy packet

sendToPeerS' :: SecurityRequirement -> Peer -> [TransportHeaderItem] -> TransportPacket Ref -> STM ()
sendToPeerS' secure Peer {..} ackedBy packet = do
    readTVar peerState >>= \case
        PeerInit xs -> writeTVar peerState $ PeerInit $ (secure, packet, ackedBy) : xs
        PeerConnected conn -> writeFlow (connData conn) (secure, packet, ackedBy)
        PeerDropped -> return ()

sendToPeerS :: Peer -> [TransportHeaderItem] -> TransportPacket Ref -> STM ()
sendToPeerS = sendToPeerS' EncryptedOnly

sendToPeerPlain :: Peer -> [TransportHeaderItem] -> TransportPacket Ref -> STM ()
sendToPeerPlain = sendToPeerS' PlaintextAllowed

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
                                    moveKeys (peerStorage peer) (serverStorage server)
                                    when (not (null rsp)) $ do
                                        sendToPeerList peer rsp
                                    atomically $ do
                                        putTMVar (peerServiceState peer) $ M.insert svc (SomeServiceState proxy s') svcs
                                        putTMVar (serverServiceStates server) $ M.insert svc (SomeServiceGlobalState proxy gs') global
                _ -> do
                    atomically $ logd $ "can't run service handler on peer with incomplete identity " ++ show (peerAddress peer)

        _ -> atomically $ do
            logd $ "unhandled service '" ++ show (toUUID svc) ++ "'"


foreign import ccall unsafe "Network/ifaddrs.h join_multicast" cJoinMulticast :: CInt -> Ptr CSize -> IO (Ptr Word32)
foreign import ccall unsafe "Network/ifaddrs.h broadcast_addresses" cBroadcastAddresses :: IO (Ptr Word32)
foreign import ccall unsafe "stdlib.h free" cFree :: Ptr Word32 -> IO ()

joinMulticast :: Socket -> IO [ Word32 ]
joinMulticast sock =
    withFdSocket sock $ \fd ->
    alloca $ \pcount -> do
        ptr <- cJoinMulticast fd pcount
        count <- fromIntegral <$> peek pcount
        forM [ 0 .. count - 1 ] $ \i ->
            peekElemOff ptr i

getBroadcastAddresses :: PortNumber -> IO [SockAddr]
getBroadcastAddresses port = do
    ptr <- cBroadcastAddresses
    let parse i = do
            w <- peekElemOff ptr i
            if w == 0 then return []
                      else (SockAddrInet port w:) <$> parse (i + 1)
    if ptr == nullPtr
      then return []
      else do
        addrs <- parse 0
        cFree ptr
        return addrs

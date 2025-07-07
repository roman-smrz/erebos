{-# LANGUAGE CPP #-}

module Erebos.Discovery (
    DiscoveryService(..),
    DiscoveryAttributes(..),
    DiscoveryConnection(..),

    discoverySearch,
) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.IP qualified as IP
import Data.List
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as M
import Data.Maybe
import Data.Proxy
import Data.Set (Set)
import Data.Set qualified as S
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word

import Network.Socket

#ifdef ENABLE_ICE_SUPPORT
import Erebos.ICE
#endif
import Erebos.Identity
import Erebos.Network
import Erebos.Service
import Erebos.Storage


#ifndef ENABLE_ICE_SUPPORT
type IceConfig = ()
type IceSession = ()
type IceRemoteInfo = Stored Object
#endif


data DiscoveryService
    = DiscoverySelf [ Text ] (Maybe Int)
    | DiscoveryAcknowledged [ Text ] (Maybe Text) (Maybe Word16) (Maybe Text) (Maybe Word16)
    | DiscoverySearch Ref
    | DiscoveryResult Ref [ Text ]
    | DiscoveryConnectionRequest DiscoveryConnection
    | DiscoveryConnectionResponse DiscoveryConnection

data DiscoveryAttributes = DiscoveryAttributes
    { discoveryStunPort :: Maybe Word16
    , discoveryStunServer :: Maybe Text
    , discoveryTurnPort :: Maybe Word16
    , discoveryTurnServer :: Maybe Text
    }

defaultDiscoveryAttributes :: DiscoveryAttributes
defaultDiscoveryAttributes = DiscoveryAttributes
    { discoveryStunPort = Nothing
    , discoveryStunServer = Nothing
    , discoveryTurnPort = Nothing
    , discoveryTurnServer = Nothing
    }

data DiscoveryConnection = DiscoveryConnection
    { dconnSource :: Ref
    , dconnTarget :: Ref
    , dconnAddress :: Maybe Text
    , dconnIceInfo :: Maybe IceRemoteInfo
    }

emptyConnection :: Ref -> Ref -> DiscoveryConnection
emptyConnection dconnSource dconnTarget = DiscoveryConnection {..}
  where
    dconnAddress = Nothing
    dconnIceInfo = Nothing

instance Storable DiscoveryService where
    store' x = storeRec $ do
        case x of
            DiscoverySelf addrs priority -> do
                mapM_ (storeText "self") addrs
                mapM_ (storeInt "priority") priority
            DiscoveryAcknowledged addrs stunServer stunPort turnServer turnPort -> do
                if null addrs then storeEmpty "ack"
                              else mapM_ (storeText "ack") addrs
                storeMbText "stun-server" stunServer
                storeMbInt "stun-port" stunPort
                storeMbText "turn-server" turnServer
                storeMbInt "turn-port" turnPort
            DiscoverySearch ref -> storeRawRef "search" ref
            DiscoveryResult ref addr -> do
                storeRawRef "result" ref
                mapM_ (storeText "address") addr
            DiscoveryConnectionRequest conn -> storeConnection "request" conn
            DiscoveryConnectionResponse conn -> storeConnection "response" conn

      where
        storeConnection ctype DiscoveryConnection {..} = do
            storeText "connection" $ ctype
            storeRawRef "source" dconnSource
            storeRawRef "target" dconnTarget
            storeMbText "address" dconnAddress
            storeMbRef "ice-info" dconnIceInfo

    load' = loadRec $ msum
            [ do
                addrs <- loadTexts "self"
                guard (not $ null addrs)
                DiscoverySelf addrs
                    <$> loadMbInt "priority"
            , do
                addrs <- loadTexts "ack"
                mbEmpty <- loadMbEmpty "ack"
                guard (not (null addrs) || isJust mbEmpty)
                DiscoveryAcknowledged
                    <$> pure addrs
                    <*> loadMbText "stun-server"
                    <*> loadMbInt "stun-port"
                    <*> loadMbText "turn-server"
                    <*> loadMbInt "turn-port"
            , DiscoverySearch <$> loadRawRef "search"
            , DiscoveryResult
                <$> loadRawRef "result"
                <*> loadTexts "address"
            , loadConnection "request" DiscoveryConnectionRequest
            , loadConnection "response" DiscoveryConnectionResponse
            ]
      where
        loadConnection ctype ctor = do
            ctype' <- loadText "connection"
            guard $ ctype == ctype'
            dconnSource <- loadRawRef "source"
            dconnTarget <- loadRawRef "target"
            dconnAddress <- loadMbText "address"
            dconnIceInfo <- loadMbRef "ice-info"
            return $ ctor DiscoveryConnection {..}

data DiscoveryPeer = DiscoveryPeer
    { dpPriority :: Int
    , dpPeer :: Maybe Peer
    , dpAddress :: [ Text ]
    , dpIceSession :: Maybe IceSession
    }

emptyPeer :: DiscoveryPeer
emptyPeer = DiscoveryPeer
    { dpPriority = 0
    , dpPeer = Nothing
    , dpAddress = []
    , dpIceSession = Nothing
    }

data DiscoveryPeerState = DiscoveryPeerState
    { dpsStunServer :: Maybe ( Text, Word16 )
    , dpsTurnServer :: Maybe ( Text, Word16 )
    , dpsIceConfig :: Maybe IceConfig
    }

data DiscoveryGlobalState = DiscoveryGlobalState
    { dgsPeers :: Map RefDigest DiscoveryPeer
    , dgsSearchingFor :: Set RefDigest
    }

instance Service DiscoveryService where
    serviceID _ = mkServiceID "dd59c89c-69cc-4703-b75b-4ddcd4b3c23c"

    type ServiceAttributes DiscoveryService = DiscoveryAttributes
    defaultServiceAttributes _ = defaultDiscoveryAttributes

    type ServiceState DiscoveryService = DiscoveryPeerState
    emptyServiceState _ = DiscoveryPeerState
        { dpsStunServer = Nothing
        , dpsTurnServer = Nothing
        , dpsIceConfig = Nothing
        }

    type ServiceGlobalState DiscoveryService = DiscoveryGlobalState
    emptyServiceGlobalState _ = DiscoveryGlobalState
        { dgsPeers = M.empty
        , dgsSearchingFor = S.empty
        }

    serviceHandler msg = case fromStored msg of
        DiscoverySelf addrs priority -> do
            pid <- asks svcPeerIdentity
            peer <- asks svcPeer
            let insertHelper new old | dpPriority new > dpPriority old = new
                                     | otherwise                       = old
            matchedAddrs <- fmap catMaybes $ forM addrs $ \addr -> if
                | addr == T.pack "ICE" -> do
                    return $ Just addr

                | [ ipaddr, port ] <- words (T.unpack addr)
                , DatagramAddress paddr <- peerAddress peer -> do
                    saddr <- liftIO $ head <$> getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                    return $ if paddr == addrAddress saddr
                                then Just addr
                                else Nothing

                | otherwise -> return Nothing

            forM_ (idDataF =<< unfoldOwners pid) $ \sdata -> do
                let dp = DiscoveryPeer
                        { dpPriority = fromMaybe 0 priority
                        , dpPeer = Just peer
                        , dpAddress = addrs
                        , dpIceSession = Nothing
                        }
                svcModifyGlobal $ \s -> s { dgsPeers = M.insertWith insertHelper (refDigest $ storedRef sdata) dp $ dgsPeers s }
            attrs <- asks svcAttributes
            replyPacket $ DiscoveryAcknowledged matchedAddrs
                (discoveryStunServer attrs)
                (discoveryStunPort attrs)
                (discoveryTurnServer attrs)
                (discoveryTurnPort attrs)

        DiscoveryAcknowledged _ stunServer stunPort turnServer turnPort -> do
            paddr <- asks (peerAddress . svcPeer) >>= return . \case
                (DatagramAddress saddr) -> case IP.fromSockAddr saddr of
                    Just (IP.IPv6 ipv6, _)
                        | (0, 0, 0xffff, ipv4) <- IP.fromIPv6w ipv6
                        -> Just $ T.pack $ show (IP.toIPv4w ipv4)
                    Just (addr, _)
                        -> Just $ T.pack $ show addr
                    _ -> Nothing
                _ -> Nothing

            let toIceServer Nothing Nothing = Nothing
                toIceServer Nothing (Just port) = ( , port) <$> paddr
                toIceServer (Just server) Nothing = Just ( server, 0 )
                toIceServer (Just server) (Just port) = Just ( server, port )

            svcModify $ \s -> s
                { dpsStunServer = toIceServer stunServer stunPort
                , dpsTurnServer = toIceServer turnServer turnPort
                }

        DiscoverySearch ref -> do
            dpeer <- M.lookup (refDigest ref) . dgsPeers <$> svcGetGlobal
            replyPacket $ DiscoveryResult ref $ maybe [] dpAddress dpeer

        DiscoveryResult _ [] -> do
            -- not found
            return ()

        DiscoveryResult ref addrs -> do
            let dgst = refDigest ref
            -- TODO: check if we really requested that
            server <- asks svcServer
            self <- svcSelf
            discoveryPeer <- asks svcPeer
            let runAsService = runPeerService @DiscoveryService discoveryPeer

            forM_ addrs $ \addr -> if
                | addr == T.pack "ICE"
                -> do
#ifdef ENABLE_ICE_SUPPORT
                    getIceConfig >>= \case
                        Just config -> void $ liftIO $ forkIO $ do
                            ice <- iceCreateSession config PjIceSessRoleControlling $ \ice -> do
                                rinfo <- iceRemoteInfo ice

                                res <- runExceptT $ sendToPeer discoveryPeer $
                                    DiscoveryConnectionRequest (emptyConnection (storedRef $ idData self) ref) { dconnIceInfo = Just rinfo }
                                case res of
                                    Right _ -> return ()
                                    Left err -> putStrLn $ "Discovery: failed to send connection request: " ++ err

                            runAsService $ do
                                let upd dp = dp { dpIceSession = Just ice }
                                svcModifyGlobal $ \s -> s { dgsPeers = M.alter (Just . upd . fromMaybe emptyPeer) dgst $ dgsPeers s }

                        Nothing -> do
                            return ()
#endif
                    return ()

                | [ ipaddr, port ] <- words (T.unpack addr) -> do
                    void $ liftIO $ forkIO $ do
                        saddr <- head <$>
                            getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                        peer <- serverPeer server (addrAddress saddr)
                        runAsService $ do
                            let upd dp = dp { dpPeer = Just peer }
                            svcModifyGlobal $ \s -> s { dgsPeers = M.alter (Just . upd . fromMaybe emptyPeer) dgst $ dgsPeers s }

                | otherwise -> do
                    svcPrint $ "Discovery: invalid address in result: " ++ T.unpack addr

        DiscoveryConnectionRequest conn -> do
            self <- svcSelf
            let rconn = emptyConnection (dconnSource conn) (dconnTarget conn)
            if refDigest (dconnTarget conn) `elem` identityDigests self
              then if
#ifdef ENABLE_ICE_SUPPORT
                -- request for us, create ICE sesssion
                | Just prinfo <- dconnIceInfo conn -> do
                    server <- asks svcServer
                    peer <- asks svcPeer
                    getIceConfig >>= \case
                        Just config -> do
                            liftIO $ void $ iceCreateSession config PjIceSessRoleControlled $ \ice -> do
                                rinfo <- iceRemoteInfo ice
                                res <- runExceptT $ sendToPeer peer $ DiscoveryConnectionResponse rconn { dconnIceInfo = Just rinfo }
                                case res of
                                    Right _ -> iceConnect ice prinfo $ void $ serverPeerIce server ice
                                    Left err -> putStrLn $ "Discovery: failed to send connection response: " ++ err
                        Nothing -> do
                            return ()
#endif

                | otherwise -> do
                    svcPrint $ "Discovery: unsupported connection request"

              else do
                    -- request to some of our peers, relay
                    mbdp <- M.lookup (refDigest $ dconnTarget conn) . dgsPeers <$> svcGetGlobal
                    case mbdp of
                        Nothing -> replyPacket $ DiscoveryConnectionResponse rconn
                        Just dp
                            | Just dpeer <- dpPeer dp -> do
                                sendToPeer dpeer $ DiscoveryConnectionRequest conn
                            | otherwise -> svcPrint $ "Discovery: failed to relay connection request"

        DiscoveryConnectionResponse conn -> do
            self <- svcSelf
            dpeers <- dgsPeers <$> svcGetGlobal
            if refDigest (dconnSource conn) `elem` identityDigests self
               then do
                    -- response to our request, try to connect to the peer
                    server <- asks svcServer
                    if  | Just addr <- dconnAddress conn
                        , [ipaddr, port] <- words (T.unpack addr) -> do
                            saddr <- liftIO $ head <$>
                                getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                            peer <- liftIO $ serverPeer server (addrAddress saddr)
                            let upd dp = dp { dpPeer = Just peer }
                            svcModifyGlobal $ \s -> s
                                { dgsPeers = M.alter (Just . upd . fromMaybe emptyPeer) (refDigest $ dconnTarget conn) $ dgsPeers s }

#ifdef ENABLE_ICE_SUPPORT
                        | Just dp <- M.lookup (refDigest $ dconnTarget conn) dpeers
                        , Just ice <- dpIceSession dp
                        , Just rinfo <- dconnIceInfo conn -> do
                            liftIO $ iceConnect ice rinfo $ void $ serverPeerIce server ice
#endif

                        | otherwise -> svcPrint $ "Discovery: connection request failed"
               else do
                    -- response to relayed request
                    case M.lookup (refDigest $ dconnSource conn) dpeers of
                        Just dp | Just dpeer <- dpPeer dp -> do
                            sendToPeer dpeer $ DiscoveryConnectionResponse conn
                        _ -> svcPrint $ "Discovery: failed to relay connection response"

    serviceNewPeer = do
        server <- asks svcServer
        peer <- asks svcPeer
        st <- getStorage

        let addrToText saddr = do
                ( addr, port ) <- IP.fromSockAddr saddr
                Just $ T.pack $ show addr <> " " <> show port
        addrs <- concat <$> sequence
            [ catMaybes . map addrToText <$> liftIO (getServerAddresses server)
#ifdef ENABLE_ICE_SUPPORT
            , return [ T.pack "ICE" ]
#endif
            ]

        pid <- asks svcPeerIdentity
        gs <- svcGetGlobal
        let searchingFor = foldl' (flip S.delete) (dgsSearchingFor gs) (identityDigests pid)
        svcModifyGlobal $ \s -> s { dgsSearchingFor = searchingFor }

        when (not $ null addrs) $ do
            sendToPeer peer $ DiscoverySelf addrs Nothing
        forM_ searchingFor $ \dgst -> do
            liftIO (refFromDigest st dgst) >>= \case
                Just ref -> sendToPeer peer $ DiscoverySearch ref
                Nothing -> return ()

#ifdef ENABLE_ICE_SUPPORT
    serviceStopServer _ _ _ pstates = do
        forM_ pstates $ \( _, DiscoveryPeerState {..} ) -> do
            mapM_ iceStopThread dpsIceConfig
#endif


identityDigests :: Foldable f => Identity f -> [ RefDigest ]
identityDigests pid = map (refDigest . storedRef) $ idDataF =<< unfoldOwners pid


getIceConfig :: ServiceHandler DiscoveryService (Maybe IceConfig)
getIceConfig = do
    dpsIceConfig <$> svcGet >>= \case
        Just cfg -> return $ Just cfg
        Nothing -> do
#ifdef ENABLE_ICE_SUPPORT
            stun <- dpsStunServer <$> svcGet
            turn <- dpsTurnServer <$> svcGet
            liftIO (iceCreateConfig stun turn) >>= \case
                Just cfg -> do
                    svcModify $ \s -> s { dpsIceConfig = Just cfg }
                    return $ Just cfg
                Nothing -> do
                    svcPrint $ "Discovery: failed to create ICE config"
                    return Nothing
#else
            return Nothing
#endif


discoverySearch :: (MonadIO m, MonadError String m) => Server -> Ref -> m ()
discoverySearch server ref = do
    peers <- liftIO $ getCurrentPeerList server
    match <- forM peers $ \peer -> do
        peerIdentity peer >>= \case
            PeerIdentityFull pid -> do
                return $ refDigest ref `elem` identityDigests pid
            _ -> return False
    when (not $ or match) $ do
        modifyServiceGlobalState server (Proxy @DiscoveryService) $ \s -> (, ()) s
            { dgsSearchingFor = S.insert (refDigest ref) $ dgsSearchingFor s
            }
        forM_ peers $ \peer -> do
            sendToPeer peer $ DiscoverySearch ref

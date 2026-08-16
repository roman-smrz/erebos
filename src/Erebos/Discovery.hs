{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Erebos.Discovery (
    DiscoveryService(..),
    DiscoveryAttributes(..),
    DiscoveryConnection(..),

    discoverySearch,
    discoverySetupTunnel,
) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

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

import System.Clock

import Text.Read

#ifdef ENABLE_ICE_SUPPORT
import Erebos.ICE
#endif
import Erebos.Identity
import Erebos.Network
import Erebos.Network.Address
import Erebos.Object
import Erebos.Service
import Erebos.Service.Stream
import Erebos.State
import Erebos.Storable


#ifndef ENABLE_ICE_SUPPORT
type IceConfig = ()
type IceSession = ()
type IceRemoteInfo = Stored Object
#endif


data DiscoveryService
    = DiscoverySelf [ DiscoveryAddress ] (Maybe Int)
    | DiscoveryAcknowledged [ DiscoveryAddress ] (Maybe Text) (Maybe Word16) (Maybe Text) (Maybe Word16)
    | DiscoverySearch (Either Ref RefDigest)
    | DiscoveryResult (Either Ref RefDigest) [ DiscoveryAddress ] [ DiscoveryVia ]
    | DiscoveryConnectionRequest DiscoveryConnection
    | DiscoveryConnectionResponse DiscoveryConnection

data DiscoveryAddress
    = DiscoveryIP InetAddress PortNumber
    | DiscoveryICE
    | DiscoveryTunnel
    | DiscoveryOther Text

data DiscoveryVia = DiscoveryVia
    { viaIdentity :: RefDigest
    , viaAddress :: [ DiscoveryAddress ]
    }

data DiscoveryAttributes = DiscoveryAttributes
    { discoveryStunPort :: Maybe Word16
    , discoveryStunServer :: Maybe Text
    , discoveryTurnPort :: Maybe Word16
    , discoveryTurnServer :: Maybe Text
    , discoveryProvideTunnel :: Peer -> PeerAddress -> Bool
    , discoveryDebugLog :: Bool
    , discoverySearchForOwner :: Bool
    }

defaultDiscoveryAttributes :: DiscoveryAttributes
defaultDiscoveryAttributes = DiscoveryAttributes
    { discoveryStunPort = Nothing
    , discoveryStunServer = Nothing
    , discoveryTurnPort = Nothing
    , discoveryTurnServer = Nothing
    , discoveryProvideTunnel = \_ _ -> False
    , discoveryDebugLog = False
    , discoverySearchForOwner = True
    }

data DiscoveryConnection = DiscoveryConnection
    { dconnSource :: Either Ref RefDigest
    , dconnTarget :: Either Ref RefDigest
    , dconnAddress :: Maybe Text
    , dconnTunnel :: Bool
    , dconnIceInfo :: Maybe IceRemoteInfo
    }

emptyConnection :: Either Ref RefDigest -> Either Ref RefDigest -> DiscoveryConnection
emptyConnection dconnSource dconnTarget = DiscoveryConnection {..}
  where
    dconnAddress = Nothing
    dconnTunnel = False
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
            DiscoverySearch edgst -> either (storeRawRef "search") (storeRawWeak "search") edgst
            DiscoveryResult edgst addr via -> do
                either (storeRawRef "result") (storeRawWeak "result") edgst
                mapM_ (storeText "address") addr
                mapM_ (storeRef "via") via
            DiscoveryConnectionRequest conn -> storeConnection "request" conn
            DiscoveryConnectionResponse conn -> storeConnection "response" conn

      where
        storeConnection (ctype :: Text) DiscoveryConnection {..} = do
            storeText "connection" $ ctype
            either (storeRawRef "source") (storeRawWeak "source") dconnSource
            either (storeRawRef "target") (storeRawWeak "target") dconnTarget
            storeMbText "address" dconnAddress
            when dconnTunnel $ storeEmpty "tunnel"
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
            , DiscoverySearch <$> msum
                [ Left <$> loadRawRef "search"
                , Right <$> loadRawWeak "search"
                ]
            , DiscoveryResult
                <$> msum
                    [ Left <$> loadRawRef "result"
                    , Right <$> loadRawWeak "result"
                    ]
                <*> loadTexts "address"
                <*> loadRefs "via"
            , loadConnection "request" DiscoveryConnectionRequest
            , loadConnection "response" DiscoveryConnectionResponse
            ]
      where
        loadConnection (ctype :: Text) ctor = do
            ctype' <- loadText "connection"
            guard $ ctype == ctype'
            dconnSource <- msum
                [ Left <$> loadRawRef "source"
                , Right <$> loadRawWeak "source"
                ]
            dconnTarget <- msum
                [ Left <$> loadRawRef "target"
                , Right <$> loadRawWeak "target"
                ]
            dconnAddress <- loadMbText "address"
            dconnTunnel <- isJust <$> loadMbEmpty "tunnel"
            dconnIceInfo <- loadMbRef "ice-info"
            return $ ctor DiscoveryConnection {..}

instance StorableText DiscoveryAddress where
    toText = \case
        DiscoveryIP addr port -> T.unwords [ T.pack $ show addr, T.pack $ show port ]
        DiscoveryICE -> "ICE"
        DiscoveryTunnel -> "tunnel"
        DiscoveryOther str -> str

    fromText str = return $ if
        | [ addrStr, portStr ] <- T.words str
        , Just addr <- readMaybe $ T.unpack addrStr
        , Just port <- readMaybe $ T.unpack portStr
        -> DiscoveryIP addr port

        | "ice" <- T.toLower str
        -> DiscoveryICE

        | "tunnel" <- str
        -> DiscoveryTunnel

        | otherwise
        -> DiscoveryOther str

instance Storable DiscoveryVia where
    store' DiscoveryVia {..} = storeRec $ do
        storeRawWeak "id" viaIdentity
        mapM_ (storeText "address") viaAddress
    load' = loadRec $ do
        viaIdentity <- loadRawWeak "id"
        viaAddress <- loadTexts "address"
        return DiscoveryVia {..}


data DiscoveryPeer = DiscoveryPeer
    { dpPriority :: Int
    , dpPeer :: Maybe Peer
    , dpAddress :: [ DiscoveryAddress ]
    , dpIceSession :: Maybe IceSession
    }

emptyPeer :: DiscoveryPeer
emptyPeer = DiscoveryPeer
    { dpPriority = 0
    , dpPeer = Nothing
    , dpAddress = []
    , dpIceSession = Nothing
    }

viaFromPeer :: DiscoveryPeer -> IO (Maybe DiscoveryVia)
viaFromPeer DiscoveryPeer {..}
    | Just peer <- dpPeer
    , viaAddress@(_ : _) <- dpAddress
    = do
        getPeerIdentity peer >>= \case
            PeerIdentityFull pid -> do
                let viaIdentity = refDigest $ storedRef $ idData pid
                return $ Just DiscoveryVia {..}
            _ -> return Nothing
    | otherwise
    = return Nothing



data DiscoveryPeerState = DiscoveryPeerState
    { dpsWeAskedFor :: Map RefDigest SearchStatus
    , dpsPeerSearchingFor :: Map RefDigest SearchStatus
    , dpsOurTunnelRequests :: [ ( RefDigest, StreamWriter ) ]
    -- ( original target, our write stream )
    , dpsRelayedTunnelRequests :: [ ( RefDigest, ( StreamReader, StreamWriter )) ]
    -- ( original source, ( from source, to target ))
    , dpsStunServer :: Maybe ( Text, Word16 )
    , dpsTurnServer :: Maybe ( Text, Word16 )
    , dpsIceConfig :: Maybe IceConfig
    }

data DiscoveryGlobalState = DiscoveryGlobalState
    { dgsPeers :: Map RefDigest ResultValue
    , dgsSearchingFor :: Set RefDigest
    }

data ResultValue = ResultValue
    { rvDirect :: Maybe DiscoveryPeer
    , rvVia :: [ DiscoveryPeer ]
    }

instance Semigroup ResultValue where
    new <> old = ResultValue
        { rvDirect = if (dpPriority <$> rvDirect new) > (dpPriority <$> rvDirect old)
                       then rvDirect new else rvDirect old
        , rvVia = rvVia new ++ rvVia old
        }

instance Monoid ResultValue where
    mempty = ResultValue
        { rvDirect = Nothing
        , rvVia = []
        }

data SearchStatus
    = SearchingSince TimeSpec

instance Service DiscoveryService where
    serviceID _ = mkServiceID "dd59c89c-69cc-4703-b75b-4ddcd4b3c23c"

    type ServiceAttributes DiscoveryService = DiscoveryAttributes
    defaultServiceAttributes _ = defaultDiscoveryAttributes

    type ServiceState DiscoveryService = DiscoveryPeerState
    emptyServiceState _ = DiscoveryPeerState
        { dpsWeAskedFor = M.empty
        , dpsPeerSearchingFor = M.empty
        , dpsOurTunnelRequests = []
        , dpsRelayedTunnelRequests = []
        , dpsStunServer = Nothing
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
            paddrs <- getPeerAddresses peer

            debugLog $ unwords
                [ "new peer"
                , show [ refDigest $ storedRef $ idData pid, refDigest $ storedRef $ idExtData pid ]
                , show $ map (refDigest . storedRef) $ idDataF $ finalOwner pid
                , show paddrs
                ]

            let matchedAddrs = flip filter addrs $ \case
                    DiscoveryICE -> True
                    DiscoveryIP ipaddr port ->
                        DatagramAddress (inetToSockAddr ( ipaddr, port )) `elem` paddrs
                    _ -> False

            forM_ (idDataF =<< unfoldOwners pid) $ \sdata -> do
                let dp = DiscoveryPeer
                        { dpPriority = fromMaybe 0 priority
                        , dpPeer = Just peer
                        , dpAddress = matchedAddrs
                        , dpIceSession = Nothing
                        }
                    rv = ResultValue
                        { rvDirect = Just dp
                        , rvVia = [ dp ]
                        }
                svcModifyGlobal $ \s -> s { dgsPeers = M.insertWith (<>) (refDigest $ storedRef sdata) rv $ dgsPeers s }
            attrs <- asks svcAttributes
            replyPacket $ DiscoveryAcknowledged matchedAddrs
                (discoveryStunServer attrs)
                (discoveryStunPort attrs)
                (discoveryTurnServer attrs)
                (discoveryTurnPort attrs)

            server <- asks svcServer
            afterCommit $ void $ forkIO $ do
                peers <- getCurrentPeerList server
                let dgsts = identityDigests pid
                forM_ peers $ \sp -> do
                    runPeerService @DiscoveryService sp $ do
                        peerSearchingFor <- dpsPeerSearchingFor <$> svcGet
                        when (any (`M.member` peerSearchingFor) dgsts) $ do
                            let peerSearchingFor' = foldl' (flip M.delete) peerSearchingFor dgsts
                            svcModify $ \s -> s { dpsPeerSearchingFor = peerSearchingFor' }
                            spid <- asks svcPeerIdentity
                            st <- getStorage
                            forM_ dgsts $ \dgst -> do
                                when (dgst `M.member` peerSearchingFor) $ do
                                    offerTunnel <- offerTunnelBetween attrs peer sp >>= return . \case
                                        True  -> (++ [ DiscoveryTunnel ])
                                        False -> id
                                    let discoveryAddrs = offerTunnel matchedAddrs
                                    let ( results, via )
                                            | dgst == (refDigest $ storedRef $ idData pid)
                                            = ( discoveryAddrs, [] )
                                            | otherwise
                                            -- Results should be empty for this case (not searching exactly for the device id),
                                            -- but keep compatibility for now.
                                            = ( discoveryAddrs, [ DiscoveryVia (refDigest $ storedRef $ idData pid) discoveryAddrs ] )

                                    debugLog $
                                        "found for " <> show (refDigest $ storedRef $ idData spid) <>
                                        " dgst " <> show dgst <>
                                        " result [" <> T.unpack (T.intercalate "," $ map toText results) <> "]" <>
                                        " via " <> show (map (\v -> ( viaIdentity v, map toText $ viaAddress v )) via)
                                    -- Try to promote weak ref to normal one for older peers:
                                    edgst <- maybe (Right dgst) Left <$> liftIO (refFromDigest st dgst)
                                    replyPacket $ DiscoveryResult edgst results via
                            debugLog $
                                "remains asked by " <> show (refDigest $ storedRef $ idData spid) <>
                                ": " <> show (M.keys peerSearchingFor')

        DiscoveryAcknowledged _ stunServer stunPort turnServer turnPort -> do
            paddr <- asks svcPeerAddress >>= return . \case
                (DatagramAddress saddr) -> T.pack . show . fst <$> inetFromSockAddr saddr
                _ -> Nothing

            let toIceServer Nothing Nothing = Nothing
                toIceServer Nothing (Just port) = ( , port) <$> paddr
                toIceServer (Just server) Nothing = Just ( server, 0 )
                toIceServer (Just server) (Just port) = Just ( server, port )

            svcModify $ \s -> s
                { dpsStunServer = toIceServer stunServer stunPort
                , dpsTurnServer = toIceServer turnServer turnPort
                }

        DiscoverySearch edgst -> do
            let dgst = either refDigest id edgst
            pid <- asks svcPeerIdentity
            (M.lookup dgst . dgsPeers <$> svcGetGlobal) >>= \case
                Just rv -> do
                    peer <- asks svcPeer
                    attrs <- asks svcAttributes
                    offerTunnel <- case dpPeer =<< rvDirect rv of
                        Just dpeer -> offerTunnelBetween attrs peer dpeer >>= return . \case
                            True  -> (++ [ DiscoveryTunnel ])
                            False -> id
                        Nothing -> return id
                    let results = offerTunnel $ maybe [] dpAddress $ rvDirect rv
                    via <- liftIO $ fmap catMaybes $ mapM viaFromPeer $ rvVia rv
                    replyPacket $ DiscoveryResult edgst results via
                    debugLog $ "search by " <> show (refDigest $ storedRef $ idData pid) <>
                        " for " <> show (either refDigest id edgst) <>
                        " result [" <> T.unpack (T.intercalate "," $ map toText results) <> "]" <>
                        " via " <> show (map (\v -> ( viaIdentity v, map toText $ viaAddress v )) via)

                Nothing -> do
                    now <- liftIO $ getTime Monotonic
                    searchingFor <- dpsPeerSearchingFor <$> svcGet
                    let seachingFor' = M.insert dgst (SearchingSince now) searchingFor
                    svcModify $ \s -> s { dpsPeerSearchingFor = seachingFor' }
                    debugLog $ "search by " <> show (refDigest $ storedRef $ idData pid) <>
                        " for " <> show (either refDigest id edgst) <>
                        " not found"
                    debugLog $ "peer " <> show (refDigest $ storedRef $ idData pid) <>
                        " searching for " <> show (M.keys seachingFor')

        DiscoveryResult edgst addrs via -> do
            let dgst = either refDigest id edgst
            server <- asks svcServer
            st <- getStorage
            self <- svcSelf
            discoveryPeer <- asks svcPeer
            pid <- asks svcPeerIdentity

            weAskedFor <- dpsWeAskedFor <$> svcGet
            let askedFor = M.member dgst weAskedFor
            debugLog $
                "result from " <> show (refDigest $ storedRef $ idData pid) <>
                " for " <> show dgst <> ": [" <> T.unpack (T.intercalate "," $ map toText addrs) <> "]" <>
                " via " <> show (map (\v -> ( viaIdentity v, map toText $ viaAddress v )) via) <>
                (if askedFor then "" else " (not asked for)")

            when askedFor $ do
                let weAskedFor' = M.delete dgst weAskedFor
                svcModify $ \s -> s { dpsWeAskedFor = weAskedFor' }
                debugLog $
                    "remains asked " <> show (refDigest $ storedRef $ idData pid) <>
                    " for " <> show (M.keys weAskedFor')

            let runAsService = runPeerService @DiscoveryService @IO discoveryPeer

            let tryAddresses = \case
                    DiscoveryIP ipaddr port : _ -> do
                        void $ liftIO $ forkIO $ do
                            let saddr = inetToSockAddr ( ipaddr, port )
                            peer <- serverPeer server saddr
                            runAsService $ do
                                let upd rv = rv { rvDirect = Just $ (fromMaybe emptyPeer $ rvDirect rv) { dpPeer = Just peer } }
                                svcModifyGlobal $ \s -> s { dgsPeers = M.alter (Just . upd . fromMaybe mempty) dgst $ dgsPeers s }

                    DiscoveryICE : rest -> do
#ifdef ENABLE_ICE_SUPPORT
                        getIceConfig >>= \case
                            Just config -> do
                                printOp <- asks svcPrintOp
                                void $ liftIO $ forkIO $ do
                                    ice <- iceCreateSession config PjIceSessRoleControlling $ \ice -> do
                                        rinfo <- iceRemoteInfo ice

                                        -- Try to promote weak ref to normal one for older peers:
                                        edgst' <- case edgst of
                                            Left  r -> return (Left r)
                                            Right d -> refFromDigest st d >>= \case
                                                Just  r -> return (Left  r)
                                                Nothing -> return (Right d)

                                        res <- runExceptT $ sendToPeer discoveryPeer $
                                            DiscoveryConnectionRequest (emptyConnection (Left $ storedRef $ idData self) edgst') { dconnIceInfo = Just rinfo }
                                        case res of
                                            Right _ -> return ()
                                            Left err -> printOp $ "Discovery: failed to send connection request: " ++ err

                                    runAsService $ do
                                        let upd rv = rv { rvDirect = Just $ (fromMaybe emptyPeer $ rvDirect rv) { dpIceSession = Just ice } }
                                        svcModifyGlobal $ \s -> s { dgsPeers = M.alter (Just . upd . fromMaybe mempty) dgst $ dgsPeers s }

                            Nothing -> do
#endif
                                tryAddresses rest

                    DiscoveryTunnel : _ -> do
                        discoverySetupTunnelResponse dgst

                    addr : rest -> do
                        debugLog $ "unsupported address in result: " ++ T.unpack (toText addr)
                        tryAddresses rest

                    [] -> debugLog $ "no (supported) address received for " <> show dgst

            when askedFor $ do
                tryAddresses $ concat
                    -- ignore direct connections for self/owner
                    [ if dgst `elem` identityDigests self then [] else addrs
                    ] ++
                    -- ignore connections via ourselves
                    concat (map viaAddress $ filter ((refDigest (storedRef (idData self)) /=) . viaIdentity) via)

        DiscoveryConnectionRequest conn -> do
            self <- svcSelf
            attrs <- asks svcAttributes
            let rconn = emptyConnection (dconnSource conn) (dconnTarget conn)
            if either refDigest id (dconnTarget conn) `elem` identityDigests self
              then if
                -- request for us, create ICE sesssion or tunnel
                | dconnTunnel conn -> do
                    receivedStreams >>= \case
                        (tunnelReader : _) -> do
                            tunnelWriter <- openStream
                            replyPacket $ DiscoveryConnectionResponse rconn
                                { dconnTunnel = True
                                }
                            tunnelVia <- asks svcPeer
                            tunnelIdentity <- asks svcPeerIdentity
                            server <- asks svcServer
                            void $ liftIO $ forkIO $ do
                                tunnelStreamNumber <- getStreamWriterNumber tunnelWriter
                                let addr = TunnelAddress {..}
                                void $ serverPeerCustom server addr
                                receiveFromTunnel server addr

                        [] -> do
                            svcPrint $ "Discovery: missing stream on tunnel request (endpoint)"

#ifdef ENABLE_ICE_SUPPORT
                | Just prinfo <- dconnIceInfo conn -> do
                    server <- asks svcServer
                    peer <- asks svcPeer
                    getIceConfig >>= \case
                        Just config -> do
                            printOp <- asks svcPrintOp
                            liftIO $ void $ iceCreateSession config PjIceSessRoleControlled $ \ice -> do
                                rinfo <- iceRemoteInfo ice
                                res <- runExceptT $ sendToPeer peer $ DiscoveryConnectionResponse rconn { dconnIceInfo = Just rinfo }
                                case res of
                                    Right _ -> iceConnect ice prinfo $ void $ serverPeerIce server ice
                                    Left err -> printOp $ "Discovery: failed to send connection response: " ++ err
                        Nothing -> do
                            return ()
#endif

                | otherwise -> do
                    svcPrint $ "Discovery: unsupported connection request"

              else do
                -- request to some of our peers, relay
                peer <- asks svcPeer
                mbrv <- M.lookup (either refDigest id $ dconnTarget conn) . dgsPeers <$> svcGetGlobal
                streams <- receivedStreams
                case rvDirect =<< mbrv of
                        Nothing -> replyPacket $ DiscoveryConnectionResponse rconn
                        Just dp
                            | Just dpeer <- dpPeer dp -> if
                                | dconnTunnel conn -> offerTunnelBetween attrs peer dpeer >>= \case
                                    False -> do
                                        replyPacket $ DiscoveryConnectionResponse rconn
                                    True | fromSource : _ <- streams -> do
                                        void $ liftIO $ forkIO $ runPeerService @DiscoveryService dpeer $ do
                                            debugLog $ "setting up tunnel from " <> show (either refDigest id $ dconnSource conn) <>
                                                " to " <> show (either refDigest id $ dconnTarget conn)
                                            toTarget <- openStream
                                            svcModify $ \s -> s { dpsRelayedTunnelRequests =
                                                ( either refDigest id $ dconnSource conn, ( fromSource, toTarget )) : dpsRelayedTunnelRequests s }
                                            replyPacket $ DiscoveryConnectionRequest conn
                                    _ | otherwise -> do
                                        svcPrint $ "Discovery: missing stream on tunnel request (relay)"
                                | otherwise -> do
                                    sendToPeer dpeer $ DiscoveryConnectionRequest conn
                            | otherwise -> svcPrint $ "Discovery: failed to relay connection request"

        DiscoveryConnectionResponse conn -> do
            self <- svcSelf
            dps <- svcGet
            dpeers <- dgsPeers <$> svcGetGlobal

            if either refDigest id (dconnSource conn) `elem` identityDigests self
              then do
                    -- response to our request, try to connect to the peer
                    server <- asks svcServer
                    if
                        | Just addr <- dconnAddress conn
                        , [ addrStr, portStr ] <- words (T.unpack addr)
                        , Just ipaddr <- readMaybe addrStr
                        , Just port <- readMaybe portStr
                        -> do
                            let saddr = inetToSockAddr ( ipaddr, port )
                            peer <- liftIO $ serverPeer server saddr
                            let upd rv = rv { rvDirect = Just $ (fromMaybe emptyPeer $ rvDirect rv) { dpPeer = Just peer } }
                            svcModifyGlobal $ \s -> s
                                { dgsPeers = M.alter (Just . upd . fromMaybe mempty) (either refDigest id $ dconnTarget conn) $ dgsPeers s }

                        | dconnTunnel conn
                        , Just tunnelWriter <- lookup (either refDigest id (dconnTarget conn)) (dpsOurTunnelRequests dps)
                        -> do
                            receivedStreams >>= \case
                                tunnelReader : _ -> do
                                    tunnelVia <- asks svcPeer
                                    tunnelIdentity <- asks svcPeerIdentity
                                    void $ liftIO $ forkIO $ do
                                        tunnelStreamNumber <- getStreamWriterNumber tunnelWriter
                                        let addr = TunnelAddress {..}
                                        void $ serverPeerCustom server addr
                                        receiveFromTunnel server addr
                                [] -> do
                                    svcPrint $ "Discovery: missing stream in tunnel response"
                                    liftIO $ closeStream tunnelWriter

                        | Just tunnelWriter <- lookup (either refDigest id (dconnTarget conn)) (dpsOurTunnelRequests dps)
                        -> do
                            svcPrint $ "Discovery: tunnel request failed"
                            liftIO $ closeStream tunnelWriter

#ifdef ENABLE_ICE_SUPPORT
                        | Just rv <- M.lookup (either refDigest id $ dconnTarget conn) dpeers
                        , Just ice <- dpIceSession =<< rvDirect rv
                        , Just rinfo <- dconnIceInfo conn -> do
                            liftIO $ iceConnect ice rinfo $ void $ serverPeerIce server ice
#endif

                        | otherwise -> svcPrint $ "Discovery: connection request failed"
              else do
                -- response to relayed request
                streams <- receivedStreams
                svcModify $ \s -> s { dpsRelayedTunnelRequests =
                    filter ((either refDigest id (dconnSource conn) /=) . fst) (dpsRelayedTunnelRequests s) }

                case M.lookup (either refDigest id $ dconnSource conn) dpeers of
                    Just ResultValue { rvDirect = Just dp } | Just dpeer <- dpPeer dp -> if
                        -- successful tunnel request
                        | dconnTunnel conn
                        , Just ( fromSource, toTarget ) <- lookup (either refDigest id (dconnSource conn)) (dpsRelayedTunnelRequests dps)
                        , fromTarget : _ <- streams
                        -> liftIO $ do
                            toSourceVar <- newEmptyMVar
                            void $ forkIO $ runPeerService @DiscoveryService dpeer $ do
                                liftIO . putMVar toSourceVar =<< openStream
                                svcModify $ \s -> s { dpsRelayedTunnelRequests =
                                    ( either refDigest id $ dconnSource conn, ( fromSource, toTarget )) : dpsRelayedTunnelRequests s }
                                replyPacket $ DiscoveryConnectionResponse conn
                            void $ forkIO $ do
                                relayStream fromSource toTarget
                            void $ forkIO $ do
                                toSource <- readMVar toSourceVar
                                relayStream fromTarget toSource

                        -- failed tunnel request
                        | Just ( _, toTarget ) <- lookup (either refDigest id (dconnSource conn)) (dpsRelayedTunnelRequests dps)
                        -> do
                            liftIO $ closeStream toTarget
                            sendToPeer dpeer $ DiscoveryConnectionResponse conn

                        | otherwise -> do
                            sendToPeer dpeer $ DiscoveryConnectionResponse conn
                    _ -> svcPrint $ "Discovery: failed to relay connection response"

    serviceNewPeer = do
        server <- asks svcServer
        peer <- asks svcPeer

        addrs <- concat <$> sequence
            [ catMaybes . map (fmap (uncurry DiscoveryIP) . inetFromSockAddr) <$> liftIO (getServerAddresses server)
#ifdef ENABLE_ICE_SUPPORT
            , return [ DiscoveryICE ]
#endif
            ]

        pid <- asks svcPeerIdentity
        gs <- svcGetGlobal
        let searchingFor = foldl' (flip S.delete) (dgsSearchingFor gs) (identityDigests pid)
        svcModifyGlobal $ \s -> s { dgsSearchingFor = searchingFor }

        searchForOwner <- asks (discoverySearchForOwner . svcAttributes) >>= \case
            True -> do
                lookupSharedValueM >>= \case
                    Just (self :: ComposedIdentity) -> do
                        return $ S.fromList $ map (refDigest . storedRef) $ idDataF self
                    Nothing -> do
                        return S.empty
            False -> return S.empty
        let searchingFor' = searchingFor `S.union` searchForOwner

        when (not $ null addrs) $ do
            sendToPeer peer $ DiscoverySelf addrs Nothing

        when (not $ null searchingFor') $ do
            forM_ searchingFor' $ \dgst -> do
                sendToPeer peer $ DiscoverySearch (Right dgst)

            now <- liftIO $ getTime Monotonic
            let weAskedFor' = M.fromAscList $ map (, SearchingSince now) $ S.toAscList searchingFor'
            svcModify $ \s -> s { dpsWeAskedFor = weAskedFor' }
            debugLog $
                "we asked new peer " <> show (refDigest $ storedRef $ idData pid) <>
                " for " <> show (M.keys weAskedFor')

    serviceUpdatedPeer = do
        pid <- asks svcPeerIdentity
        peer <- asks svcPeer
        isPeerDropped peer >>= \case
            True -> do
                peers <- dgsPeers <$> svcGetGlobal
                let peers' = M.mapMaybe removePeer peers
                    removePeer rv =
                        let rv' = rv { rvDirect = if (dpPeer =<< rvDirect rv) == Just peer then Nothing else rvDirect rv
                                     , rvVia = filter ((Just peer /=) . dpPeer) $ rvVia rv
                                     }
                         in if isJust (rvDirect rv') || not (null (rvVia rv'))
                              then Just rv'
                              else Nothing
                svcModifyGlobal $ \s -> s { dgsPeers = peers' }
                debugLog $ "dropped peer " <> show [ refDigest $ storedRef $ idData pid, refDigest $ storedRef $ idExtData pid ] <>
                    ", map size " <> show (M.size peers) <> " -> " <> show (M.size peers')
            False -> do
                debugLog $ "updated peer " <> show [ refDigest $ storedRef $ idData pid, refDigest $ storedRef $ idExtData pid ]

#ifdef ENABLE_ICE_SUPPORT
    serviceStopServer _ _ _ pstates = do
        forM_ pstates $ \( _, DiscoveryPeerState {..} ) -> do
            mapM_ iceStopThread dpsIceConfig
#endif


debugLog :: String -> ServiceHandler DiscoveryService ()
debugLog str = do
    asks (discoveryDebugLog . svcAttributes) >>= \case
        True -> svcPrint $ "discovery: " <> str
        False -> return ()


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


-- | Start search for an identity identified by given ref using the discovery
-- service.
discoverySearch
    :: forall m e. (MonadIO m, MonadError e m, FromErebosError e)
    => Server -- ^ `Server' object to run the discovery
    -> RefDigest -- ^ Reference identifying the intended peer
    -> m ()
discoverySearch server dgst = do
    flip catchError (\e -> case toErebosError e of
            Just (UnhandledService svc) | svc == serviceID (Proxy @DiscoveryService) -> return ()
            _ -> throwError e) $ do
        peers <- liftIO $ getCurrentPeerList server
        match <- forM peers $ \peer -> do
            getPeerIdentity peer >>= \case
                PeerIdentityFull pid -> do
                    return $ dgst `elem` identityDigests pid
                _ -> return False
        when (not $ or match) $ do
            _ <- modifyServiceGlobalState server (Proxy @DiscoveryService) $ \s ->
                ( s { dgsSearchingFor =  S.insert dgst $ dgsSearchingFor s }, () )
            now <- liftIO $ getTime Monotonic
            forM_ peers $ \peer -> do
                runPeerService peer $ do
                    weAskedFor <- dpsWeAskedFor <$> svcGet
                    when (not $ M.member dgst weAskedFor) $ do
                        let weAskedFor' = M.insert dgst (SearchingSince now) weAskedFor
                        svcModify $ \s -> s { dpsWeAskedFor = weAskedFor' }
                        pid <- asks svcPeerIdentity
                        debugLog $
                            "we asked " <> show (refDigest $ storedRef $ idData pid) <>
                            " for " <> show dgst <> " " <> show (M.keys weAskedFor')
                        replyPacket $ DiscoverySearch $ Right dgst


data TunnelAddress = TunnelAddress
    { tunnelVia :: Peer
    , tunnelIdentity :: UnifiedIdentity
    , tunnelStreamNumber :: Int
    , tunnelReader :: StreamReader
    , tunnelWriter :: StreamWriter
    }

instance Eq TunnelAddress where
    x == y  =  (==)
        (idData (tunnelIdentity x), tunnelStreamNumber x)
        (idData (tunnelIdentity y), tunnelStreamNumber y)

instance Ord TunnelAddress where
    compare x y = compare
        (idData (tunnelIdentity x), tunnelStreamNumber x)
        (idData (tunnelIdentity y), tunnelStreamNumber y)

instance Show TunnelAddress where
    show tunnel = concat
        [ "tunnel@"
        , show $ refDigest $ storedRef $ idData $ tunnelIdentity tunnel
        , "/" <> show (tunnelStreamNumber tunnel)
        ]

instance PeerAddressType TunnelAddress where
    sendBytesToAddress TunnelAddress {..} bytes = do
        writeStream tunnelWriter bytes

    connectionToAddressClosed TunnelAddress {..} = do
        closeStream tunnelWriter

offerTunnelBetween :: MonadIO m => DiscoveryAttributes -> Peer -> Peer -> m Bool
offerTunnelBetween attrs p1 p2 =
    offerTunnelFor p1 >>= \case
        True -> return True
        False -> offerTunnelFor p2
  where
    offerTunnelFor peer = do
        addrs <- getPeerAddresses peer
        return $ any (discoveryProvideTunnel attrs peer) addrs

relayStream :: StreamReader -> StreamWriter -> IO ()
relayStream r w = do
    p <- readStreamPacket r
    writeStreamPacket w p
    case p of
        StreamClosed {} -> return ()
        _ -> relayStream r w

receiveFromTunnel :: Server -> TunnelAddress -> IO ()
receiveFromTunnel server taddr = do
    p <- readStreamPacket (tunnelReader taddr)
    case p of
        StreamData {..} -> do
            receivedFromCustomAddress server taddr stpData
            receiveFromTunnel server taddr
        StreamClosed {} -> do
            dropPeerAddress server $ CustomPeerAddress taddr


discoverySetupTunnel :: Peer -> RefDigest -> IO ()
discoverySetupTunnel via target = do
    runPeerService via $ do
        discoverySetupTunnelResponse target

discoverySetupTunnelResponse :: RefDigest -> ServiceHandler DiscoveryService ()
discoverySetupTunnelResponse target = do
        self <- refDigest . storedRef . idData <$> svcSelf
        stream <- openStream
        svcModify $ \s -> s { dpsOurTunnelRequests = ( target, stream ) : dpsOurTunnelRequests s }
        replyPacket $ DiscoveryConnectionRequest
            (emptyConnection (Right self) (Right target))
            { dconnTunnel = True
            }

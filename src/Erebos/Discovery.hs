{-# LANGUAGE CPP #-}

module Erebos.Discovery (
    DiscoveryService(..),
    DiscoveryAttributes(..),
    DiscoveryConnection(..)
) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.IP qualified as IP
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as M
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T
import Data.Word

import Network.Socket

#ifdef ENABLE_ICE_SUPPORT
import Erebos.ICE
#endif
import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Service
import Erebos.Storable


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
#ifdef ENABLE_ICE_SUPPORT
    , dconnIceInfo :: Maybe IceRemoteInfo
#else
    , dconnIceInfo :: Maybe (Stored Object)
#endif
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

        where storeConnection ctype conn = do
                  storeText "connection" $ ctype
                  storeRawRef "source" $ dconnSource conn
                  storeRawRef "target" $ dconnTarget conn
                  storeMbText "address" $ dconnAddress conn
                  storeMbRef "ice-info" $ dconnIceInfo conn

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
        where loadConnection ctype ctor = do
                  ctype' <- loadText "connection"
                  guard $ ctype == ctype'
                  return . ctor =<< DiscoveryConnection
                      <$> loadRawRef "source"
                      <*> loadRawRef "target"
                      <*> loadMbText "address"
                      <*> loadMbRef "ice-info"

data DiscoveryPeer = DiscoveryPeer
    { dpPriority :: Int
    , dpPeer :: Maybe Peer
    , dpAddress :: [ Text ]
#ifdef ENABLE_ICE_SUPPORT
    , dpIceSession :: Maybe IceSession
#endif
    }

instance Service DiscoveryService where
    serviceID _ = mkServiceID "dd59c89c-69cc-4703-b75b-4ddcd4b3c23c"

    type ServiceAttributes DiscoveryService = DiscoveryAttributes
    defaultServiceAttributes _ = defaultDiscoveryAttributes

#ifdef ENABLE_ICE_SUPPORT
    type ServiceState DiscoveryService = Maybe IceConfig
    emptyServiceState _ = Nothing
#endif

    type ServiceGlobalState DiscoveryService = Map RefDigest DiscoveryPeer
    emptyServiceGlobalState _ = M.empty

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

            forM_ (idDataF =<< unfoldOwners pid) $ \s ->
                svcModifyGlobal $ M.insertWith insertHelper (refDigest $ storedRef s) DiscoveryPeer
                    { dpPriority = fromMaybe 0 priority
                    , dpPeer = Just peer
                    , dpAddress = addrs
#ifdef ENABLE_ICE_SUPPORT
                    , dpIceSession = Nothing
#endif
                    }
            attrs <- asks svcAttributes
            replyPacket $ DiscoveryAcknowledged matchedAddrs
                (discoveryStunServer attrs)
                (discoveryStunPort attrs)
                (discoveryTurnServer attrs)
                (discoveryTurnPort attrs)

        DiscoveryAcknowledged _ stunServer stunPort turnServer turnPort -> do
#ifdef ENABLE_ICE_SUPPORT
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

            cfg <- liftIO $ iceCreateConfig
                (toIceServer stunServer stunPort)
                (toIceServer turnServer turnPort)
            svcSet cfg
#endif
            return ()

        DiscoverySearch ref -> do
            dpeer <- M.lookup (refDigest ref) <$> svcGetGlobal
            replyPacket $ DiscoveryResult ref $ maybe [] dpAddress dpeer

        DiscoveryResult ref [] -> do
            svcPrint $ "Discovery: " ++ show (refDigest ref) ++ " not found"

        DiscoveryResult ref addrs -> do
            -- TODO: check if we really requested that
            server <- asks svcServer
            self <- svcSelf
            mbIceConfig <- svcGet
            discoveryPeer <- asks svcPeer
            let runAsService = runPeerService @DiscoveryService discoveryPeer

            liftIO $ void $ forkIO $ forM_ addrs $ \addr -> if
                | addr == T.pack "ICE"
#ifdef ENABLE_ICE_SUPPORT
                , Just config <- mbIceConfig
                -> do
                    ice <- iceCreateSession config PjIceSessRoleControlling $ \ice -> do
                        rinfo <- iceRemoteInfo ice
                        res <- runExceptT $ sendToPeer discoveryPeer $
                            DiscoveryConnectionRequest (emptyConnection (storedRef $ idData self) ref) { dconnIceInfo = Just rinfo }
                        case res of
                            Right _ -> return ()
                            Left err -> putStrLn $ "Discovery: failed to send connection request: " ++ err

                    runAsService $ do
                        svcModifyGlobal $ M.insert (refDigest ref) DiscoveryPeer
                            { dpPriority = 0
                            , dpPeer = Nothing
                            , dpAddress = []
                            , dpIceSession = Just ice
                            }
#else
                -> do
                    return ()
#endif

                | [ ipaddr, port ] <- words (T.unpack addr) -> do
                    saddr <- head <$>
                        getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                    peer <- serverPeer server (addrAddress saddr)
                    runAsService $ do
                        svcModifyGlobal $ M.insert (refDigest ref) DiscoveryPeer
                            { dpPriority = 0
                            , dpPeer = Just peer
                            , dpAddress = []
#ifdef ENABLE_ICE_SUPPORT
                            , dpIceSession = Nothing
#endif
                        }

                | otherwise -> do
                    runAsService $ do
                        svcPrint $ "Discovery: invalid address in result: " ++ T.unpack addr

        DiscoveryConnectionRequest conn -> do
            self <- svcSelf
            let rconn = emptyConnection (dconnSource conn) (dconnTarget conn)
            if refDigest (dconnTarget conn) `elem` (map (refDigest . storedRef) $ idDataF =<< unfoldOwners self)
               then do
#ifdef ENABLE_ICE_SUPPORT
                    -- request for us, create ICE sesssion
                    server <- asks svcServer
                    peer <- asks svcPeer
                    svcGet >>= \case
                        Just config -> do
                            liftIO $ void $ iceCreateSession config PjIceSessRoleControlled $ \ice -> do
                                rinfo <- iceRemoteInfo ice
                                res <- runExceptT $ sendToPeer peer $ DiscoveryConnectionResponse rconn { dconnIceInfo = Just rinfo }
                                case res of
                                    Right _ -> do
                                        case dconnIceInfo conn of
                                            Just prinfo -> iceConnect ice prinfo $ void $ serverPeerIce server ice
                                            Nothing -> putStrLn $ "Discovery: connection request without ICE remote info"
                                    Left err -> putStrLn $ "Discovery: failed to send connection response: " ++ err
                        Nothing -> do
                            svcPrint $ "Discovery: ICE request from peer without ICE configuration"
#else
                    return ()
#endif

               else do
                    -- request to some of our peers, relay
                    mbdp <- M.lookup (refDigest $ dconnTarget conn) <$> svcGetGlobal
                    case mbdp of
                        Nothing -> replyPacket $ DiscoveryConnectionResponse rconn
                        Just dp | addr : _ <- dpAddress dp -> do
                                    replyPacket $ DiscoveryConnectionResponse rconn { dconnAddress = Just addr }
                                | Just dpeer <- dpPeer dp -> do
                                    sendToPeer dpeer $ DiscoveryConnectionRequest conn
                                | otherwise -> svcPrint $ "Discovery: failed to relay connection request"

        DiscoveryConnectionResponse conn -> do
            self <- svcSelf
            dpeers <- svcGetGlobal
            if refDigest (dconnSource conn) `elem` (map (refDigest . storedRef) $ idDataF =<< unfoldOwners self)
               then do
                    -- response to our request, try to connect to the peer
#ifdef ENABLE_ICE_SUPPORT
                    server <- asks svcServer
                    if  | Just addr <- dconnAddress conn
                        , [ipaddr, port] <- words (T.unpack addr) -> do
                            saddr <- liftIO $ head <$>
                                getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                            peer <- liftIO $ serverPeer server (addrAddress saddr)
                            svcModifyGlobal $ M.insert (refDigest $ dconnTarget conn) $
                                DiscoveryPeer 0 (Just peer) [] Nothing

                        | Just dp <- M.lookup (refDigest $ dconnTarget conn) dpeers
                        , Just ice <- dpIceSession dp
                        , Just rinfo <- dconnIceInfo conn -> do
                            liftIO $ iceConnect ice rinfo $ void $ serverPeerIce server ice

                        | otherwise -> svcPrint $ "Discovery: connection request failed"
#else
                    return ()
#endif
               else do
                    -- response to relayed request
                    case M.lookup (refDigest $ dconnSource conn) dpeers of
                        Just dp | Just dpeer <- dpPeer dp -> do
                            sendToPeer dpeer $ DiscoveryConnectionResponse conn
                        _ -> svcPrint $ "Discovery: failed to relay connection response"

    serviceNewPeer = do
        server <- asks svcServer
        peer <- asks svcPeer

        let addrToText saddr = do
                ( addr, port ) <- IP.fromSockAddr saddr
                Just $ T.pack $ show addr <> " " <> show port
        addrs <- concat <$> sequence
            [ catMaybes . map addrToText <$> liftIO (getServerAddresses server)
#ifdef ENABLE_ICE_SUPPORT
            , return [ T.pack "ICE" ]
#endif
            ]

        when (not $ null addrs) $ do
            sendToPeer peer $ DiscoverySelf addrs Nothing

{-# LANGUAGE CPP #-}

module Erebos.Discovery (
    DiscoveryService(..),
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

import Network.Socket

#ifdef ENABLE_ICE_SUPPORT
import Erebos.ICE
#endif
import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Service
import Erebos.Storable


data DiscoveryService = DiscoverySelf [ Text ] (Maybe Int)
                      | DiscoveryAcknowledged Text
                      | DiscoverySearch Ref
                      | DiscoveryResult Ref [ Text ]
                      | DiscoveryConnectionRequest DiscoveryConnection
                      | DiscoveryConnectionResponse DiscoveryConnection

data DiscoveryConnection = DiscoveryConnection
    { dconnSource :: Ref
    , dconnTarget :: Ref
    , dconnAddress :: Maybe Text
#ifdef ENABLE_ICE_SUPPORT
    , dconnIceSession :: Maybe IceRemoteInfo
#endif
    }

emptyConnection :: Ref -> Ref -> DiscoveryConnection
emptyConnection dconnSource dconnTarget = DiscoveryConnection {..}
  where
    dconnAddress = Nothing
#ifdef ENABLE_ICE_SUPPORT
    dconnIceSession = Nothing
#endif

instance Storable DiscoveryService where
    store' x = storeRec $ do
        case x of
            DiscoverySelf addrs priority -> do
                mapM_ (storeText "self") addrs
                mapM_ (storeInt "priority") priority
            DiscoveryAcknowledged addr -> do
                storeText "ack" addr
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
#ifdef ENABLE_ICE_SUPPORT
                  storeMbRef "ice-session" $ dconnIceSession conn
#endif

    load' = loadRec $ msum
            [ do
                addrs <- loadTexts "self"
                guard (not $ null addrs)
                DiscoverySelf addrs
                    <$> loadMbInt "priority"
            , DiscoveryAcknowledged
                <$> loadText "ack"
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
#ifdef ENABLE_ICE_SUPPORT
                      <*> loadMbRef "ice-session"
#endif

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

    type ServiceGlobalState DiscoveryService = Map RefDigest DiscoveryPeer
    emptyServiceGlobalState _ = M.empty

    serviceHandler msg = case fromStored msg of
        DiscoverySelf addrs priority -> do
            pid <- asks svcPeerIdentity
            peer <- asks svcPeer
            let insertHelper new old | dpPriority new > dpPriority old = new
                                     | otherwise                       = old
            mbaddr <- fmap (listToMaybe . catMaybes) $ forM addrs $ \addr -> case words (T.unpack addr) of
                [ipaddr, port] | DatagramAddress paddr <- peerAddress peer -> do
                    saddr <- liftIO $ head <$> getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                    return $ if paddr == addrAddress saddr
                                then Just addr
                                else Nothing
                _ -> return Nothing
            forM_ (idDataF =<< unfoldOwners pid) $ \s ->
                svcModifyGlobal $ M.insertWith insertHelper (refDigest $ storedRef s) DiscoveryPeer
                    { dpPriority = fromMaybe 0 priority
                    , dpPeer = Just peer
                    , dpAddress = addrs
#ifdef ENABLE_ICE_SUPPORT
                    , dpIceSession = Nothing
#endif
                    }
            replyPacket $ DiscoveryAcknowledged $ fromMaybe (T.pack "ICE") mbaddr

        DiscoveryAcknowledged _ -> do
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
            discoveryPeer <- asks svcPeer
            let runAsService = runPeerService @DiscoveryService discoveryPeer

            liftIO $ void $ forkIO $ forM_ addrs $ \addr -> if
                | addr == T.pack "ICE" -> do
#ifdef ENABLE_ICE_SUPPORT
                    ice <- iceCreate PjIceSessRoleControlling $ \ice -> do
                        rinfo <- iceRemoteInfo ice
                        res <- runExceptT $ sendToPeer discoveryPeer $
                            DiscoveryConnectionRequest (emptyConnection (storedRef $ idData self) ref) { dconnIceSession = Just rinfo }
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
#ifdef ENABLE_ICE_SUPPORT
            self <- svcSelf
            let rconn = emptyConnection (dconnSource conn) (dconnTarget conn)
            if refDigest (dconnTarget conn) `elem` (map (refDigest . storedRef) $ idDataF =<< unfoldOwners self)
               then do
                    -- request for us, create ICE sesssion
                    server <- asks svcServer
                    peer <- asks svcPeer
                    liftIO $ void $ iceCreate PjIceSessRoleControlled $ \ice -> do
                        rinfo <- iceRemoteInfo ice
                        res <- runExceptT $ sendToPeer peer $ DiscoveryConnectionResponse rconn { dconnIceSession = Just rinfo }
                        case res of
                            Right _ -> do
                                case dconnIceSession conn of
                                    Just prinfo -> iceConnect ice prinfo $ void $ serverPeerIce server ice
                                    Nothing -> putStrLn $ "Discovery: connection request without ICE remote info"
                            Left err -> putStrLn $ "Discovery: failed to send connection response: " ++ err

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
#else
            return ()
#endif

        DiscoveryConnectionResponse conn -> do
#ifdef ENABLE_ICE_SUPPORT
            self <- svcSelf
            dpeers <- svcGetGlobal
            if refDigest (dconnSource conn) `elem` (map (refDigest . storedRef) $ idDataF =<< unfoldOwners self)
               then do
                    -- response to our request, try to connect to the peer
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
                        , Just rinfo <- dconnIceSession conn -> do
                            liftIO $ iceConnect ice rinfo $ void $ serverPeerIce server ice

                        | otherwise -> svcPrint $ "Discovery: connection request failed"
               else do
                    -- response to relayed request
                    case M.lookup (refDigest $ dconnSource conn) dpeers of
                        Just dp | Just dpeer <- dpPeer dp -> do
                            sendToPeer dpeer $ DiscoveryConnectionResponse conn
                        _ -> svcPrint $ "Discovery: failed to relay connection response"
#else
            return ()
#endif

    serviceNewPeer = do
        server <- asks svcServer
        peer <- asks svcPeer

        let addrToText saddr = do
                ( addr, port ) <- IP.fromSockAddr saddr
                Just $ T.pack $ show addr <> " " <> show port
        addrs <- catMaybes . map addrToText <$> liftIO (getServerAddresses server)

        sendToPeer peer $ DiscoverySelf addrs Nothing

module Erebos.Discovery (
    DiscoveryService(..),
    DiscoveryConnection(..)
) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as M
import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T

import Network.Socket

import Erebos.ICE
import Erebos.Identity
import Erebos.Network
import Erebos.Object.Internal
import Erebos.Service


keepaliveSeconds :: Int
keepaliveSeconds = 20


data DiscoveryService = DiscoverySelf Text Int
                      | DiscoveryAcknowledged Text
                      | DiscoverySearch Ref
                      | DiscoveryResult Ref (Maybe Text)
                      | DiscoveryConnectionRequest DiscoveryConnection
                      | DiscoveryConnectionResponse DiscoveryConnection

data DiscoveryConnection = DiscoveryConnection
    { dconnSource :: Ref
    , dconnTarget :: Ref
    , dconnAddress :: Maybe Text
    , dconnIceSession :: Maybe IceRemoteInfo
    }

emptyConnection :: Ref -> Ref -> DiscoveryConnection
emptyConnection source target = DiscoveryConnection source target Nothing Nothing

instance Storable DiscoveryService where
    store' x = storeRec $ do
        case x of
            DiscoverySelf addr priority -> do
                storeText "self" addr
                storeInt "priority" priority
            DiscoveryAcknowledged addr -> do
                storeText "ack" addr
            DiscoverySearch ref -> storeRawRef "search" ref
            DiscoveryResult ref addr -> do
                storeRawRef "result" ref
                storeMbText "address" addr
            DiscoveryConnectionRequest conn -> storeConnection "request" conn
            DiscoveryConnectionResponse conn -> storeConnection "response" conn

        where storeConnection ctype conn = do
                  storeText "connection" $ ctype
                  storeRawRef "source" $ dconnSource conn
                  storeRawRef "target" $ dconnTarget conn
                  storeMbText "address" $ dconnAddress conn
                  storeMbRef "ice-session" $ dconnIceSession conn

    load' = loadRec $ msum
            [ DiscoverySelf
                <$> loadText "self"
                <*> loadInt "priority"
            , DiscoveryAcknowledged
                <$> loadText "ack"
            , DiscoverySearch <$> loadRawRef "search"
            , DiscoveryResult
                <$> loadRawRef "result"
                <*> loadMbText "address"
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
                      <*> loadMbRef "ice-session"

data DiscoveryPeer = DiscoveryPeer
    { dpPriority :: Int
    , dpPeer :: Maybe Peer
    , dpAddress :: Maybe Text
    , dpIceSession :: Maybe IceSession
    }

instance Service DiscoveryService where
    serviceID _ = mkServiceID "dd59c89c-69cc-4703-b75b-4ddcd4b3c23b"

    type ServiceGlobalState DiscoveryService = Map RefDigest DiscoveryPeer
    emptyServiceGlobalState _ = M.empty

    serviceHandler msg = case fromStored msg of
        DiscoverySelf addr priority -> do
            pid <- asks svcPeerIdentity
            peer <- asks svcPeer
            let insertHelper new old | dpPriority new > dpPriority old = new
                                     | otherwise                       = old
            mbaddr <- case words (T.unpack addr) of
                [ipaddr, port] | DatagramAddress paddr <- peerAddress peer -> do
                    saddr <- liftIO $ head <$> getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                    return $ if paddr == addrAddress saddr
                                then Just addr
                                else Nothing
                _ -> return Nothing
            forM_ (idDataF =<< unfoldOwners pid) $ \s ->
                svcModifyGlobal $ M.insertWith insertHelper (refDigest $ storedRef s) $
                    DiscoveryPeer priority (Just peer) mbaddr Nothing
            replyPacket $ DiscoveryAcknowledged $ fromMaybe (T.pack "ICE") mbaddr

        DiscoveryAcknowledged addr -> do
            when (addr == T.pack "ICE") $ do
                -- keep-alive packet from behind NAT
                peer <- asks svcPeer
                liftIO $ void $ forkIO $ do
                    threadDelay (keepaliveSeconds * 1000 * 1000)
                    res <- runExceptT $ sendToPeer peer $ DiscoverySelf addr 0
                    case res of
                        Right _ -> return ()
                        Left err -> putStrLn $ "Discovery: failed to send keep-alive: " ++ err

        DiscoverySearch ref -> do
            addr <- M.lookup (refDigest ref) <$> svcGetGlobal
            replyPacket $ DiscoveryResult ref $ fromMaybe (T.pack "ICE") . dpAddress <$> addr

        DiscoveryResult ref Nothing -> do
            svcPrint $ "Discovery: " ++ show (refDigest ref) ++ " not found"

        DiscoveryResult ref (Just addr) -> do
            -- TODO: check if we really requested that
            server <- asks svcServer
            if addr == T.pack "ICE"
               then do
                    self <- svcSelf
                    peer <- asks svcPeer
                    ice <- liftIO $ iceCreate PjIceSessRoleControlling $ \ice -> do
                        rinfo <- iceRemoteInfo ice
                        res <- runExceptT $ sendToPeer peer $
                            DiscoveryConnectionRequest (emptyConnection (storedRef $ idData self) ref) { dconnIceSession = Just rinfo }
                        case res of
                            Right _ -> return ()
                            Left err -> putStrLn $ "Discovery: failed to send connection request: " ++ err

                    svcModifyGlobal $ M.insert (refDigest ref) $
                        DiscoveryPeer 0 Nothing Nothing (Just ice)
               else do
                    case words (T.unpack addr) of
                        [ipaddr, port] -> do
                            saddr <- liftIO $ head <$>
                                getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                            peer <- liftIO $ serverPeer server (addrAddress saddr)
                            svcModifyGlobal $ M.insert (refDigest ref) $
                                DiscoveryPeer 0 (Just peer) Nothing Nothing

                        _ -> svcPrint $ "Discovery: invalid address in result: " ++ T.unpack addr

        DiscoveryConnectionRequest conn -> do
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
                        Just dp | Just addr <- dpAddress dp -> do
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
                    server <- asks svcServer
                    if  | Just addr <- dconnAddress conn
                        , [ipaddr, port] <- words (T.unpack addr) -> do
                            saddr <- liftIO $ head <$>
                                getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just ipaddr) (Just port)
                            peer <- liftIO $ serverPeer server (addrAddress saddr)
                            svcModifyGlobal $ M.insert (refDigest $ dconnTarget conn) $
                                DiscoveryPeer 0 (Just peer) Nothing Nothing

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

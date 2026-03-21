module WebSocket (
    WebSocketAddress(..),
    WebSocketOptions(..), defaultWebSocketOptions,
    startWebsocketServer,
) where

import Control.Concurrent
import Control.Exception
import Control.Monad

import Data.ByteString.Lazy qualified as BL
import Data.Unique

import Erebos.Network

import Network.WebSockets qualified as WS


data WebSocketAddress = WebSocketAddress Unique WS.Connection

instance Eq WebSocketAddress where
    WebSocketAddress u _ == WebSocketAddress u' _ = u == u'

instance Ord WebSocketAddress where
    compare (WebSocketAddress u _) (WebSocketAddress u' _) = compare u u'

instance Show WebSocketAddress where
    show (WebSocketAddress _ _) = "websocket"

instance PeerAddressType WebSocketAddress where
    sendBytesToAddress (WebSocketAddress _ conn) msg = do
        WS.sendDataMessage conn $ WS.Binary $ BL.fromStrict msg
    connectionToAddressClosed (WebSocketAddress _ conn) = do
        WS.sendClose conn BL.empty `catch` \e -> if
            | Just WS.ConnectionClosed <- fromException e -> return ()
            | otherwise -> throwIO e


data WebSocketOptions = WebSocketOptions
    { wsAddress :: String
    , wsPort :: Int
    , wsDebugLog :: Bool
    }

defaultWebSocketOptions :: WebSocketOptions
defaultWebSocketOptions = WebSocketOptions
    { wsAddress = "::"
    , wsPort = 80
    , wsDebugLog = False
    }


startWebsocketServer :: Server -> (String -> IO ()) -> WebSocketOptions -> IO ()
startWebsocketServer server logd WebSocketOptions {..} = do
    void $ forkIO $ do
        WS.runServer wsAddress wsPort $ \pending -> do
            when wsDebugLog $ do
                logd $ "WebSocket request: " <> show (WS.pendingRequest pending)
            conn <- WS.acceptRequest pending
            u <- newUnique
            let paddr = WebSocketAddress u conn
            void $ serverPeerCustom server paddr

            let handler e
                    | Just WS.CloseRequest {} <- fromException e = do
                        dropPeerAddress server $ CustomPeerAddress paddr
                    | Just WS.ConnectionClosed <- fromException e = do
                        dropPeerAddress server $ CustomPeerAddress paddr
                    | otherwise = do
                        logd $ "WebSocket thread exception: " ++ show e
            handle handler $ do
                WS.withPingThread conn 30 (return ()) $ do
                    forever $ do
                        WS.receiveDataMessage conn >>= \case
                            WS.Binary msg -> receivedFromCustomAddress server paddr $ BL.toStrict msg
                            WS.Text {} -> logd $ "unexpected websocket text message"

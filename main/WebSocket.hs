module WebSocket (
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

startWebsocketServer :: Server -> String -> Int -> (String -> IO ()) -> IO ()
startWebsocketServer server addr port logd = do
    void $ forkIO $ do
        WS.runServer addr port $ \pending -> do
            conn <- WS.acceptRequest pending
            u <- newUnique
            let paddr = WebSocketAddress u conn
            void $ serverPeerCustom server paddr
            handle (\(e :: SomeException) -> logd $ "WebSocket thread exception: " ++ show e) $ do
                WS.withPingThread conn 30 (return ()) $ do
                    forever $ do
                        WS.receiveDataMessage conn >>= \case
                            WS.Binary msg -> receivedFromCustomAddress server paddr $ BL.toStrict msg
                            WS.Text {} -> logd $ "unexpected websocket text message"

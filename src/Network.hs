module Network (
    Peer(..),
    PeerAddress,
    peerDiscovery,
) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.Text as T

import Network.Socket
import Network.Socket.ByteString (recvFrom, sendTo)

import Identity
import PubKey
import Storage


discoveryPort :: ServiceName
discoveryPort = "29665"


data Peer = Peer
    { peerIdentity :: Stored Identity
    , peerAddress :: PeerAddress
    }
    deriving (Show)

data PeerAddress = DatagramAddress SockAddr
    deriving (Show)


data TransportHeader = AnnouncePacket Ref
                     | IdentityRequest Ref Ref
                     | IdentityResponse Ref

transportToObject :: TransportHeader -> Object
transportToObject = \case
    AnnouncePacket ref -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "announce")
        , (BC.pack "identity", RecRef ref)
        ]
    IdentityRequest ref from -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "idreq")
        , (BC.pack "identity", RecRef ref)
        , (BC.pack "from", RecRef from)
        ]
    IdentityResponse ref -> Rec
        [ (BC.pack "TRANS", RecText $ T.pack "idresp")
        , (BC.pack "identity", RecRef ref)
        ]

transportFromObject :: Object -> Maybe TransportHeader
transportFromObject (Rec items)
    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "announce"
    , Just (RecRef ref) <- lookup (BC.pack "identity") items
    = Just $ AnnouncePacket ref

    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "idreq"
    , Just (RecRef ref) <- lookup (BC.pack "identity") items
    , Just (RecRef from) <- lookup (BC.pack "from") items
    = Just $ IdentityRequest ref from

    | Just (RecText trans) <- lookup (BC.pack "TRANS") items, trans == T.pack "idresp"
    , Just (RecRef ref) <- lookup (BC.pack "identity") items
    = Just $ IdentityResponse ref

transportFromObject _ = Nothing


peerDiscovery :: String -> Stored Identity -> IO (Chan Peer)
peerDiscovery bhost sidentity = do
    chan <- newChan
    void $ forkIO $ withSocketsDo $ do
        let hints = defaultHints
              { addrFlags = [AI_PASSIVE]
              , addrSocketType = Datagram
              }
        addr:_ <- getAddrInfo (Just hints) Nothing (Just discoveryPort)
        bracket (open addr) close (loop chan)
    return chan
  where
    open addr = do
        sock <- socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
        setSocketOption sock ReuseAddr 1
        setSocketOption sock Broadcast 1
        setCloseOnExecIfNeeded =<< fdSocket sock
        bind sock (addrAddress addr)
        return sock

    loop chan sock = do
        baddr:_ <- getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just bhost) (Just discoveryPort)
        void $ sendTo sock (BL.toStrict $ serializeObject $ transportToObject $ AnnouncePacket $ storedRef sidentity) (addrAddress baddr)
        forever $ do
            (msg, peer) <- recvFrom sock 4096
            let packet' = packet chan sock peer
            case runExcept $ deserializeObjects (storedStorage sidentity) $ BL.fromStrict msg of
                 Left err -> putStrLn $ show peer ++ ": " ++ err
                 Right (obj:objs) | Just tpack <- transportFromObject obj -> packet' tpack objs
                 _ -> putStrLn $ show peer ++ ": invalid transport packet"

    packet _ sock peer (AnnouncePacket ref) _ = do
        putStrLn $ "Got announce: " ++ show ref ++ " from " ++ show peer
        when (ref /= storedRef sidentity) $ void $ sendTo sock (BL.toStrict $ BL.concat
            [ serializeObject $ transportToObject $ IdentityRequest ref (storedRef sidentity)
            , lazyLoadBytes $ storedRef sidentity
            , lazyLoadBytes $ storedRef $ signedData $ fromStored sidentity
            , lazyLoadBytes $ storedRef $ idKeyIdentity $ fromStored $ signedData $ fromStored sidentity
            , lazyLoadBytes $ storedRef $ signedSignature $ fromStored sidentity
            ]) peer

    packet _ _ peer (IdentityRequest ref from) [] = do
        putStrLn $ "Got identity request: for " ++ show ref ++ " by " ++ show from ++ " from " ++ show peer ++ " without content"

    packet chan sock peer (IdentityRequest ref from) (obj:objs) = do
        putStrLn $ "Got identity request: for " ++ show ref ++ " by " ++ show from ++ " from " ++ show peer
        print (obj:objs)
        from' <- store (storedStorage sidentity) obj
        if from == from'
           then do forM_ objs $ store $ storedStorage sidentity
                   writeChan chan $ Peer (wrappedLoad from) (DatagramAddress peer)
                   void $ sendTo sock (BL.toStrict $ BL.concat
                       [ serializeObject $ transportToObject $ IdentityResponse (storedRef sidentity)
                       , lazyLoadBytes $ storedRef sidentity
                       , lazyLoadBytes $ storedRef $ signedData $ fromStored sidentity
                       , lazyLoadBytes $ storedRef $ idKeyIdentity $ fromStored $ signedData $ fromStored sidentity
                       , lazyLoadBytes $ storedRef $ signedSignature $ fromStored sidentity
                       ]) peer
           else putStrLn $ "Mismatched content"

    packet _ _ peer (IdentityResponse ref) [] = do
        putStrLn $ "Got identity response: by " ++ show ref ++ " from " ++ show peer ++ " without content"

    packet chan _ peer (IdentityResponse ref) (obj:objs) = do
        putStrLn $ "Got identity response: by " ++ show ref ++ " from " ++ show peer
        print (obj:objs)
        ref' <- store (storedStorage sidentity) obj
        if ref == ref'
           then do forM_ objs $ store $ storedStorage sidentity
                   writeChan chan $ Peer (wrappedLoad ref) (DatagramAddress peer)
           else putStrLn $ "Mismatched content"

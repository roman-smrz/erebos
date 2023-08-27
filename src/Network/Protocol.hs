module Network.Protocol (
    TransportPacket(..),
    transportToObject,
    TransportHeader(..),
    TransportHeaderItem(..),

    WaitingRef(..),
    WaitingRefCallback,
    wrDigest,

    ChannelState(..),

    ControlRequest(..),
    ControlMessage(..),
    erebosNetworkProtocol,

    Connection,
    connAddress,
    connData,
    connGetChannel,
    connSetChannel,

    module Flow,
) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Except

import Data.Bits
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.List
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T

import System.Clock

import Channel
import Flow
import Identity
import Service
import Storage


protocolVersion :: Text
protocolVersion = T.pack "0.1"

protocolVersions :: [Text]
protocolVersions = [protocolVersion]


data TransportPacket a = TransportPacket TransportHeader [a]

data TransportHeader = TransportHeader [TransportHeaderItem]

data TransportHeaderItem
    = Acknowledged RefDigest
    | Rejected RefDigest
    | ProtocolVersion Text
    | Initiation RefDigest
    | CookieSet Cookie
    | CookieEcho Cookie
    | DataRequest RefDigest
    | DataResponse RefDigest
    | AnnounceSelf RefDigest
    | AnnounceUpdate RefDigest
    | TrChannelRequest RefDigest
    | TrChannelAccept RefDigest
    | ServiceType ServiceID
    | ServiceRef RefDigest
    deriving (Eq)

newtype Cookie = Cookie ByteString
    deriving (Eq)

transportToObject :: PartialStorage -> TransportHeader -> PartialObject
transportToObject st (TransportHeader items) = Rec $ map single items
    where single = \case
              Acknowledged dgst -> (BC.pack "ACK", RecRef $ partialRefFromDigest st dgst)
              Rejected dgst -> (BC.pack "REJ", RecRef $ partialRefFromDigest st dgst)
              ProtocolVersion ver -> (BC.pack "VER", RecText ver)
              Initiation dgst -> (BC.pack "INI", RecRef $ partialRefFromDigest st dgst)
              CookieSet (Cookie bytes) -> (BC.pack "CKS", RecBinary bytes)
              CookieEcho (Cookie bytes) -> (BC.pack "CKE", RecBinary bytes)
              DataRequest dgst -> (BC.pack "REQ", RecRef $ partialRefFromDigest st dgst)
              DataResponse dgst -> (BC.pack "RSP", RecRef $ partialRefFromDigest st dgst)
              AnnounceSelf dgst -> (BC.pack "ANN", RecRef $ partialRefFromDigest st dgst)
              AnnounceUpdate dgst -> (BC.pack "ANU", RecRef $ partialRefFromDigest st dgst)
              TrChannelRequest dgst -> (BC.pack "CRQ", RecRef $ partialRefFromDigest st dgst)
              TrChannelAccept dgst -> (BC.pack "CAC", RecRef $ partialRefFromDigest st dgst)
              ServiceType stype -> (BC.pack "STP", RecUUID $ toUUID stype)
              ServiceRef dgst -> (BC.pack "SRF", RecRef $ partialRefFromDigest st dgst)

transportFromObject :: PartialObject -> Maybe TransportHeader
transportFromObject (Rec items) = case catMaybes $ map single items of
                                       [] -> Nothing
                                       titems -> Just $ TransportHeader titems
    where single (name, content) = if
              | name == BC.pack "ACK", RecRef ref <- content -> Just $ Acknowledged $ refDigest ref
              | name == BC.pack "REJ", RecRef ref <- content -> Just $ Rejected $ refDigest ref
              | name == BC.pack "VER", RecText ver <- content -> Just $ ProtocolVersion ver
              | name == BC.pack "INI", RecRef ref <- content -> Just $ Initiation $ refDigest ref
              | name == BC.pack "CKS", RecBinary bytes <- content -> Just $ CookieSet (Cookie bytes)
              | name == BC.pack "CKE", RecBinary bytes <- content -> Just $ CookieEcho (Cookie bytes)
              | name == BC.pack "REQ", RecRef ref <- content -> Just $ DataRequest $ refDigest ref
              | name == BC.pack "RSP", RecRef ref <- content -> Just $ DataResponse $ refDigest ref
              | name == BC.pack "ANN", RecRef ref <- content -> Just $ AnnounceSelf $ refDigest ref
              | name == BC.pack "ANU", RecRef ref <- content -> Just $ AnnounceUpdate $ refDigest ref
              | name == BC.pack "CRQ", RecRef ref <- content -> Just $ TrChannelRequest $ refDigest ref
              | name == BC.pack "CAC", RecRef ref <- content -> Just $ TrChannelAccept $ refDigest ref
              | name == BC.pack "STP", RecUUID uuid <- content -> Just $ ServiceType $ fromUUID uuid
              | name == BC.pack "SRF", RecRef ref <- content -> Just $ ServiceRef $ refDigest ref
              | otherwise -> Nothing
transportFromObject _ = Nothing


data GlobalState addr = (Eq addr, Show addr) => GlobalState
    { gIdentity :: TVar (UnifiedIdentity, [UnifiedIdentity])
    , gConnections :: TVar [Connection addr]
    , gDataFlow :: SymFlow (addr, ByteString)
    , gControlFlow :: Flow (ControlRequest addr) (ControlMessage addr)
    , gLog :: String -> STM ()
    , gStorage :: PartialStorage
    , gNowVar :: TVar TimeSpec
    , gNextTimeout :: TVar TimeSpec
    , gInitConfig :: Ref
    }

data Connection addr = Connection
    { cAddress :: addr
    , cDataUp :: Flow (Bool, TransportPacket PartialObject) (Bool, TransportPacket Ref, [TransportHeaderItem])
    , cDataInternal :: Flow (Bool, TransportPacket Ref, [TransportHeaderItem]) (Bool, TransportPacket PartialObject)
    , cChannel :: TVar ChannelState
    , cSecureOutQueue :: TQueue (Bool, TransportPacket Ref, [TransportHeaderItem])
    , cSentPackets :: TVar [SentPacket]
    }

connAddress :: Connection addr -> addr
connAddress = cAddress

connData :: Connection addr -> Flow (Bool, TransportPacket PartialObject) (Bool, TransportPacket Ref, [TransportHeaderItem])
connData = cDataUp

connGetChannel :: Connection addr -> STM ChannelState
connGetChannel Connection {..} = readTVar cChannel

connSetChannel :: Connection addr -> ChannelState -> STM ()
connSetChannel Connection {..} ch = do
    writeTVar cChannel ch


data WaitingRef = WaitingRef
    { wrefStorage :: Storage
    , wrefPartial :: PartialRef
    , wrefAction :: Ref -> WaitingRefCallback
    , wrefStatus :: TVar (Either [RefDigest] Ref)
    }

type WaitingRefCallback = ExceptT String IO ()

wrDigest :: WaitingRef -> RefDigest
wrDigest = refDigest . wrefPartial


data ChannelState = ChannelNone
                  | ChannelCookieWait
                  | ChannelCookieReceived Cookie
                  | ChannelOurRequest (Stored ChannelRequest)
                  | ChannelPeerRequest WaitingRef
                  | ChannelOurAccept (Stored ChannelAccept) Channel
                  | ChannelEstablished Channel


data SentPacket = SentPacket
    { spTime :: TimeSpec
    , spRetryCount :: Int
    , spAckedBy :: Maybe (TransportHeaderItem -> Bool)
    , spData :: BC.ByteString
    }


data ControlRequest addr = RequestConnection addr
                         | SendAnnounce addr
                         | UpdateSelfIdentity UnifiedIdentity

data ControlMessage addr = NewConnection (Connection addr) (Maybe RefDigest)
                         | ReceivedAnnounce addr RefDigest


erebosNetworkProtocol :: (Eq addr, Ord addr, Show addr)
                      => UnifiedIdentity
                      -> (String -> STM ())
                      -> SymFlow (addr, ByteString)
                      -> Flow (ControlRequest addr) (ControlMessage addr)
                      -> IO ()
erebosNetworkProtocol initialIdentity gLog gDataFlow gControlFlow = do
    gIdentity <- newTVarIO (initialIdentity, [])
    gConnections <- newTVarIO []
    mStorage <- memoryStorage
    gStorage <- derivePartialStorage mStorage

    startTime <- getTime MonotonicRaw
    gNowVar <- newTVarIO startTime
    gNextTimeout <- newTVarIO startTime
    gInitConfig <- store mStorage $ (Rec [] :: Object)

    let gs = GlobalState {..}

    let signalTimeouts = forever $ do
            now <- getTime MonotonicRaw
            next <- atomically $ do
                writeTVar gNowVar now
                readTVar gNextTimeout

            let waitTill time
                    | time > now = threadDelay $ fromInteger (toNanoSecs (time - now)) `div` 1000
                    | otherwise = threadDelay maxBound
                waitForUpdate = atomically $ do
                    next' <- readTVar gNextTimeout
                    when (next' == next) retry

            race_ (waitTill next) waitForUpdate

    race_ signalTimeouts $ forever $ join $ atomically $
        processIncomming gs <|> processOutgoing gs



getConnection :: GlobalState addr -> addr -> STM (Connection addr)
getConnection gs addr = do
    maybe (newConnection gs addr) return =<< findConnection gs addr

findConnection :: GlobalState addr -> addr -> STM (Maybe (Connection addr))
findConnection GlobalState {..} addr = do
    find ((addr==) . cAddress) <$> readTVar gConnections

newConnection :: GlobalState addr -> addr -> STM (Connection addr)
newConnection GlobalState {..} addr = do
    conns <- readTVar gConnections

    let cAddress = addr
    (cDataUp, cDataInternal) <- newFlow
    cChannel <- newTVar ChannelNone
    cSecureOutQueue <- newTQueue
    cSentPackets <- newTVar []
    let conn = Connection {..}

    writeTVar gConnections (conn : conns)
    return conn

processIncomming :: GlobalState addr -> STM (IO ())
processIncomming gs@GlobalState {..} = do
    (addr, msg) <- readFlow gDataFlow
    mbconn <- findConnection gs addr

    mbch <- case mbconn of
        Nothing -> return Nothing
        Just conn -> readTVar (cChannel conn) >>= return . \case
            ChannelEstablished ch -> Just ch
            ChannelOurAccept _ ch -> Just ch
            _                     -> Nothing

    return $ do
        let deserialize = liftEither . runExcept . deserializeObjects gStorage . BL.fromStrict
        let parse = case B.uncons msg of
                Just (b, enc)
                    | b .&. 0xE0 == 0x80 -> do
                        ch <- maybe (throwError "unexpected encrypted packet") return mbch
                        (dec, _) <- channelDecrypt ch enc

                        case B.uncons dec of
                            Just (0x00, content) -> do
                                objs <- deserialize content
                                return (True, objs)

                            Just (_, _) -> do
                                throwError "streams not implemented"

                            Nothing -> do
                                throwError "empty decrypted content"

                    | b .&. 0xE0 == 0x60 -> do
                        objs <- deserialize msg
                        return (False, objs)

                    | otherwise -> throwError "invalid packet"

                Nothing -> throwError "empty packet"

        runExceptT parse >>= \case
            Right (secure, objs)
                | hobj:content <- objs
                , Just header@(TransportHeader items) <- transportFromObject hobj
                -> processPacket gs (maybe (Left addr) Right mbconn) secure (TransportPacket header content) >>= \case
                    Just (conn@Connection {..}, mbup) -> atomically $ do
                        processAcknowledgements gs conn items
                        case mbup of
                            Just up -> writeFlow cDataInternal (secure, up)
                            Nothing -> return ()
                    Nothing -> return ()

                | otherwise -> atomically $ do
                      gLog $ show addr ++ ": invalid objects"
                      gLog $ show objs

            Left err -> do
                atomically $ gLog $ show addr <> ": failed to parse packet: " <> err

processPacket :: GlobalState addr -> Either addr (Connection addr) -> Bool -> TransportPacket a -> IO (Maybe (Connection addr, Maybe (TransportPacket a)))
processPacket gs@GlobalState {..} econn secure packet@(TransportPacket (TransportHeader header) _) = if
    | Right conn <- econn, secure
    -> return $ Just (conn, Just packet)

    | _:_ <- mapMaybe (\case Initiation x -> Just x; _ -> Nothing) header
    , Just ver <- version
    -> do
        cookie <- createCookie gs addr
        atomically $ do
            identity <- fst <$> readTVar gIdentity
            let reply = BL.toStrict $ serializeObject $ transportToObject gStorage $ TransportHeader
                    [ CookieSet cookie
                    , AnnounceSelf $ refDigest $ storedRef $ idData identity
                    , ProtocolVersion ver
                    ]
            writeFlow gDataFlow (addr, reply)
        return Nothing

    | cookie:_ <- mapMaybe (\case CookieSet x -> Just x; _ -> Nothing) header
    , Just _ <- version
    , Right conn@Connection {..} <- econn
    -> do
        atomically $ readTVar cChannel >>= \case
            ChannelCookieWait -> do
                writeTVar cChannel $ ChannelCookieReceived cookie
                writeFlow gControlFlow (NewConnection conn mbpid)
                return $ Just (conn, Nothing)
            _ -> return Nothing

    | Right conn <- econn
    -> return $ Just (conn, Just packet)

    | cookie:_ <- mapMaybe (\case CookieEcho x -> Just x; _ -> Nothing) header
    , Just _ <- version
    -> verifyCookie gs addr cookie >>= \case
        True -> do
            conn <- atomically $ findConnection gs addr >>= \case
                Just conn -> return conn
                Nothing -> do
                    conn <- newConnection gs addr
                    writeFlow gControlFlow (NewConnection conn mbpid)
                    return conn
            return $ Just (conn, Just packet)
        False -> return Nothing

    | dgst:_ <- mapMaybe (\case AnnounceSelf x -> Just x; _ -> Nothing) header
    , Just _ <- version
    -> do
        atomically $ do
            (cur, past) <- readTVar gIdentity
            when (not $ dgst `elem` map (refDigest . storedRef . idData) (cur : past)) $ do
                writeFlow gControlFlow $ ReceivedAnnounce addr dgst
        return Nothing

    | otherwise -> return Nothing

  where
    addr = either id cAddress econn
    mbpid = listToMaybe $ mapMaybe (\case AnnounceSelf dgst -> Just dgst; _ -> Nothing) header
    version = listToMaybe $ filter (\v -> ProtocolVersion v `elem` header) protocolVersions


createCookie :: GlobalState addr -> addr -> IO Cookie
createCookie GlobalState {} addr = return (Cookie $ BC.pack $ show addr)

verifyCookie :: GlobalState addr -> addr -> Cookie -> IO Bool
verifyCookie GlobalState {} addr (Cookie cookie) = return $ show addr == BC.unpack cookie

resendBytes :: GlobalState addr -> Connection addr -> SentPacket -> IO ()
resendBytes GlobalState {..} Connection {..} sp = do
    now <- getTime MonotonicRaw
    atomically $ do
        when (isJust $ spAckedBy sp) $ do
            modifyTVar' cSentPackets $ (:) sp
                { spTime = now
                , spRetryCount = spRetryCount sp + 1
                }
        writeFlow gDataFlow (cAddress, spData sp)

sendBytes :: GlobalState addr -> Connection addr -> ByteString -> Maybe (TransportHeaderItem -> Bool) -> IO ()
sendBytes gs conn bs ackedBy = resendBytes gs conn
    SentPacket
        { spTime = undefined
        , spRetryCount = -1
        , spAckedBy = ackedBy
        , spData = bs
        }

processOutgoing :: forall addr. GlobalState addr -> STM (IO ())
processOutgoing gs@GlobalState {..} = do

    let sendNextPacket :: Connection addr -> STM (IO ())
        sendNextPacket conn@Connection {..} = do
            mbch <- readTVar cChannel >>= return . \case
                ChannelEstablished ch -> Just ch
                _                     -> Nothing

            let checkOutstanding
                    | isJust mbch = readTQueue cSecureOutQueue
                    | otherwise = retry

            (secure, packet@(TransportPacket (TransportHeader hitems) content), ackedBy) <-
                checkOutstanding <|> readFlow cDataInternal

            when (isNothing mbch && secure) $ do
                writeTQueue cSecureOutQueue (secure, packet, ackedBy)

            header <- readTVar cChannel >>= return . TransportHeader . \case
                ChannelCookieReceived cookie -> CookieEcho cookie : ProtocolVersion protocolVersion : hitems
                _                            -> hitems

            let plain = BL.concat $
                    (serializeObject $ transportToObject gStorage header)
                    : map lazyLoadBytes content

            return $ do
                mbs <- case mbch of
                    Just ch -> do
                        runExceptT (channelEncrypt ch $ BL.toStrict $ 0x00 `BL.cons` plain) >>= \case
                            Right (ctext, _) -> return $ Just $ 0x80 `B.cons` ctext
                            Left err -> do atomically $ gLog $ "Failed to encrypt data: " ++ err
                                           return Nothing
                    Nothing | secure    -> return Nothing
                            | otherwise -> return $ Just $ BL.toStrict plain

                case mbs of
                    Just bs -> sendBytes gs conn bs $ guard (not $ null ackedBy) >> Just (`elem` ackedBy)
                    Nothing -> return ()

    let retransmitPacket :: Connection addr -> STM (IO ())
        retransmitPacket conn@Connection {..} = do
            now <- readTVar gNowVar
            (sp, rest) <- readTVar cSentPackets >>= \case
                sps@(_:_) -> return (last sps, init sps)
                _         -> retry
            let nextTry = spTime sp + fromNanoSecs 1000000000
            if now < nextTry
              then do
                nextTimeout <- readTVar gNextTimeout
                if nextTimeout <= now || nextTry < nextTimeout
                   then do writeTVar gNextTimeout nextTry
                           return $ return ()
                   else retry
              else do
                writeTVar cSentPackets rest
                return $ resendBytes gs conn sp

    let handleControlRequests = readFlow gControlFlow >>= \case
            RequestConnection addr -> do
                conn@Connection {..} <- getConnection gs addr
                identity <- fst <$> readTVar gIdentity
                readTVar cChannel >>= \case
                    ChannelNone -> do
                        let packet = BL.toStrict $ BL.concat
                                [ serializeObject $ transportToObject gStorage $ TransportHeader $
                                    [ Initiation $ refDigest gInitConfig
                                    , AnnounceSelf $ refDigest $ storedRef $ idData identity
                                    ] ++ map ProtocolVersion protocolVersions
                                , lazyLoadBytes gInitConfig
                                ]
                        writeTVar cChannel ChannelCookieWait
                        return $ sendBytes gs conn packet $ Just $ \case CookieSet {} -> True; _ -> False
                    _ -> return $ return ()

            SendAnnounce addr -> do
                identity <- fst <$> readTVar gIdentity
                let packet = BL.toStrict $ serializeObject $ transportToObject gStorage $ TransportHeader $
                        [ AnnounceSelf $ refDigest $ storedRef $ idData identity
                        ] ++ map ProtocolVersion protocolVersions
                writeFlow gDataFlow (addr, packet)
                return $ return ()

            UpdateSelfIdentity nid -> do
                (cur, past) <- readTVar gIdentity
                writeTVar gIdentity (nid, cur : past)
                return $ return ()

    conns <- readTVar gConnections
    msum $ concat $
        [ map retransmitPacket conns
        , map sendNextPacket conns
        , [ handleControlRequests ]
        ]

processAcknowledgements :: GlobalState addr -> Connection addr -> [TransportHeaderItem] -> STM ()
processAcknowledgements GlobalState {} Connection {..} = mapM_ $ \hitem -> do
    modifyTVar' cSentPackets $ filter $ \sp -> not (fromJust (spAckedBy sp) hitem)
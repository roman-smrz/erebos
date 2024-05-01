module Erebos.Network.Protocol (
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

    RawStreamReader, RawStreamWriter,
    connAddWriteStream,
    connAddReadStream,
    readStreamToList,
    readObjectsFromStream,
    writeByteStringToStream,

    module Erebos.Flow,
) where

import Control.Applicative
import Control.Concurrent
import Control.Concurrent.Async
import Control.Concurrent.STM
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans

import Data.Bits
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.List
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T
import Data.Void
import Data.Word

import System.Clock

import Erebos.Channel
import Erebos.Flow
import Erebos.Identity
import Erebos.Service
import Erebos.Storage


protocolVersion :: Text
protocolVersion = T.pack "0.1"

protocolVersions :: [Text]
protocolVersions = [protocolVersion]


data TransportPacket a = TransportPacket TransportHeader [a]

data TransportHeader = TransportHeader [TransportHeaderItem]
    deriving (Show)

data TransportHeaderItem
    = Acknowledged RefDigest
    | AcknowledgedSingle Integer
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
    | StreamOpen Word8
    deriving (Eq, Show)

newtype Cookie = Cookie ByteString
    deriving (Eq, Show)

isHeaderItemAcknowledged :: TransportHeaderItem -> Bool
isHeaderItemAcknowledged = \case
    Acknowledged {} -> False
    AcknowledgedSingle {} -> False
    Rejected {} -> False
    ProtocolVersion {} -> False
    Initiation {} -> False
    CookieSet {} -> False
    CookieEcho {} -> False
    _ -> True

transportToObject :: PartialStorage -> TransportHeader -> PartialObject
transportToObject st (TransportHeader items) = Rec $ map single items
    where single = \case
              Acknowledged dgst -> (BC.pack "ACK", RecRef $ partialRefFromDigest st dgst)
              AcknowledgedSingle num -> (BC.pack "ACK", RecInt num)
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
              ServiceType stype -> (BC.pack "SVT", RecUUID $ toUUID stype)
              ServiceRef dgst -> (BC.pack "SVR", RecRef $ partialRefFromDigest st dgst)
              StreamOpen num -> (BC.pack "STO", RecInt $ fromIntegral num)

transportFromObject :: PartialObject -> Maybe TransportHeader
transportFromObject (Rec items) = case catMaybes $ map single items of
                                       [] -> Nothing
                                       titems -> Just $ TransportHeader titems
    where single (name, content) = if
              | name == BC.pack "ACK", RecRef ref <- content -> Just $ Acknowledged $ refDigest ref
              | name == BC.pack "ACK", RecInt num <- content -> Just $ AcknowledgedSingle num
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
              | name == BC.pack "SVT", RecUUID uuid <- content -> Just $ ServiceType $ fromUUID uuid
              | name == BC.pack "SVR", RecRef ref <- content -> Just $ ServiceRef $ refDigest ref
              | name == BC.pack "STO", RecInt num <- content -> Just $ StreamOpen $ fromIntegral num
              | otherwise -> Nothing
transportFromObject _ = Nothing


data GlobalState addr = (Eq addr, Show addr) => GlobalState
    { gIdentity :: TVar (UnifiedIdentity, [UnifiedIdentity])
    , gConnections :: TVar [Connection addr]
    , gDataFlow :: SymFlow (addr, ByteString)
    , gControlFlow :: Flow (ControlRequest addr) (ControlMessage addr)
    , gNextUp :: TMVar (Connection addr, (Bool, TransportPacket PartialObject))
    , gLog :: String -> STM ()
    , gStorage :: PartialStorage
    , gNowVar :: TVar TimeSpec
    , gNextTimeout :: TVar TimeSpec
    , gInitConfig :: Ref
    }

data Connection addr = Connection
    { cGlobalState :: GlobalState addr
    , cAddress :: addr
    , cDataUp :: Flow (Bool, TransportPacket PartialObject) (Bool, TransportPacket Ref, [TransportHeaderItem])
    , cDataInternal :: Flow (Bool, TransportPacket Ref, [TransportHeaderItem]) (Bool, TransportPacket PartialObject)
    , cChannel :: TVar ChannelState
    , cSecureOutQueue :: TQueue (Bool, TransportPacket Ref, [TransportHeaderItem])
    , cMaxInFlightPackets :: TVar Int
    , cReservedPackets :: TVar Int
    , cSentPackets :: TVar [SentPacket]
    , cToAcknowledge :: TVar [Integer]
    , cInStreams :: TVar [(Word8, Stream)]
    , cOutStreams :: TVar [(Word8, Stream)]
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

connAddWriteStream :: Connection addr -> STM (Either String (TransportHeaderItem, RawStreamWriter, IO ()))
connAddWriteStream conn@Connection {..} = do
    outStreams <- readTVar cOutStreams
    let doInsert :: Word8 -> [(Word8, Stream)] -> ExceptT String STM ((Word8, Stream), [(Word8, Stream)])
        doInsert n (s@(n', _) : rest) | n == n' =
            fmap (s:) <$> doInsert (n + 1) rest
        doInsert n streams | n < 63 = lift $ do
            sState <- newTVar StreamOpening
            (sFlowIn, sFlowOut) <- newFlow
            sNextSequence <- newTVar 0
            let info = (n, Stream {..})
            return (info, info : streams)
        doInsert _ _ = throwError "all outbound streams in use"

    runExceptT $ do
        ((streamNumber, stream), outStreams') <- doInsert 1 outStreams
        lift $ writeTVar cOutStreams outStreams'
        return (StreamOpen streamNumber, sFlowIn stream, go cGlobalState streamNumber stream)

  where
    go gs@GlobalState {..} streamNumber stream = do
            (reserved, msg) <- atomically $ do
                readTVar (sState stream) >>= \case
                    StreamRunning -> return ()
                    _             -> retry
                (,) <$> reservePacket conn
                    <*> readFlow (sFlowOut stream)
            let (plain, cont, onAck) = case msg of
                    StreamData {..} -> (stpData, True, return ())
                    StreamClosed {} -> (BC.empty, False, streamClosed conn streamNumber)
                    -- TODO: send channel closed only after delivering all previous data packets
            let secure = True
                plainAckedBy = []
                mbReserved = Just reserved

            mbch <- atomically (readTVar cChannel) >>= return . \case
                ChannelEstablished ch   -> Just ch
                ChannelOurAccept _ _ ch -> Just ch
                _                       -> Nothing

            mbs <- case mbch of
                Just ch -> do
                    runExceptT (channelEncrypt ch $ B.concat
                            [ B.singleton streamNumber
                            , B.singleton (fromIntegral (stpSequence msg) :: Word8)
                            , plain
                            ] ) >>= \case
                        Right (ctext, counter) -> do
                            let isAcked = True
                            return $ Just (0x80 `B.cons` ctext, if isAcked then [ AcknowledgedSingle $ fromIntegral counter ] else [])
                        Left err -> do atomically $ gLog $ "Failed to encrypt data: " ++ err
                                       return Nothing
                Nothing | secure    -> return Nothing
                        | otherwise -> return $ Just (plain, plainAckedBy)

            case mbs of
                Just (bs, ackedBy) -> do
                    let mbReserved' = (\rs -> rs
                            { rsAckedBy = guard (not $ null ackedBy) >> Just (`elem` ackedBy)
                            , rsOnAck = rsOnAck rs >> onAck
                            }) <$> mbReserved
                    sendBytes conn mbReserved' bs
                Nothing -> return ()

            when cont $ go gs streamNumber stream

connAddReadStream :: Connection addr -> Word8 -> STM RawStreamReader
connAddReadStream Connection {..} streamNumber = do
    inStreams <- readTVar cInStreams
    let doInsert (s@(n, _) : rest)
            | streamNumber <  n = fmap (s:) <$> doInsert rest
            | streamNumber == n = doInsert rest
        doInsert streams = do
            sState <- newTVar StreamRunning
            (sFlowIn, sFlowOut) <- newFlow
            sNextSequence <- newTVar 0
            let stream = Stream {..}
            return (stream, (streamNumber, stream) : streams)
    (stream, inStreams') <- doInsert inStreams
    writeTVar cInStreams inStreams'
    return $ sFlowOut stream


type RawStreamReader = Flow StreamPacket Void
type RawStreamWriter = Flow Void StreamPacket

data Stream = Stream
    { sState :: TVar StreamState
    , sFlowIn :: Flow Void StreamPacket
    , sFlowOut :: Flow StreamPacket Void
    , sNextSequence :: TVar Word64
    }

data StreamState = StreamOpening | StreamRunning

data StreamPacket
    = StreamData
        { stpSequence :: Word64
        , stpData :: BC.ByteString
        }
    | StreamClosed
        { stpSequence :: Word64
        }

streamAccepted :: Connection addr -> Word8 -> IO ()
streamAccepted Connection {..} snum = atomically $ do
    (lookup snum <$> readTVar cOutStreams) >>= \case
        Just Stream {..} -> do
            modifyTVar' sState $ \case
                StreamOpening -> StreamRunning
                x             -> x
        Nothing -> return ()

streamClosed :: Connection addr -> Word8 -> IO ()
streamClosed Connection {..} snum = atomically $ do
    modifyTVar' cOutStreams $ filter ((snum /=) . fst)

readStreamToList :: RawStreamReader -> IO (Word64, [(Word64, BC.ByteString)])
readStreamToList stream = readFlowIO stream >>= \case
    StreamData sq bytes -> fmap ((sq, bytes) :) <$> readStreamToList stream
    StreamClosed sqEnd  -> return (sqEnd, [])

readObjectsFromStream :: PartialStorage -> RawStreamReader -> IO (Except String [PartialObject])
readObjectsFromStream st stream = do
    (seqEnd, list) <- readStreamToList stream
    print (seqEnd, length list, list)
    let validate s ((s', bytes) : rest)
            | s == s'   = (bytes : ) <$> validate (s + 1) rest
            | s >  s'   = validate s rest
            | otherwise = throwError "missing object chunk"
        validate s []
            | s == seqEnd = return []
            | otherwise = throwError "content length mismatch"
    return $ do
        content <- BL.fromChunks <$> validate 0 list
        deserializeObjects st content

writeByteStringToStream :: RawStreamWriter -> BL.ByteString -> IO ()
writeByteStringToStream stream = go 0
  where
    go seqNum bstr
        | BL.null bstr = writeFlowIO stream $ StreamClosed seqNum
        | otherwise    = do
            let (cur, rest) = BL.splitAt 500 bstr -- TODO: MTU
            writeFlowIO stream $ StreamData seqNum (BL.toStrict cur)
            go (seqNum + 1) rest


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
                  | ChannelCookieWait -- sent initiation, waiting for response
                  | ChannelCookieReceived Cookie -- received cookie, but no cookie echo yet
                  | ChannelCookieConfirmed Cookie -- received cookie echo, no need to send from our side
                  | ChannelOurRequest (Maybe Cookie) (Stored ChannelRequest)
                  | ChannelPeerRequest (Maybe Cookie) WaitingRef
                  | ChannelOurAccept (Maybe Cookie) (Stored ChannelAccept) Channel
                  | ChannelEstablished Channel

data ReservedToSend = ReservedToSend
    { rsAckedBy :: Maybe (TransportHeaderItem -> Bool)
    , rsOnAck :: IO ()
    }

data SentPacket = SentPacket
    { spTime :: TimeSpec
    , spRetryCount :: Int
    , spAckedBy :: Maybe (TransportHeaderItem -> Bool)
    , spOnAck :: IO ()
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
    gNextUp <- newEmptyTMVarIO
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
        passUpIncoming gs <|> processIncoming gs <|> processOutgoing gs


getConnection :: GlobalState addr -> addr -> STM (Connection addr)
getConnection gs addr = do
    maybe (newConnection gs addr) return =<< findConnection gs addr

findConnection :: GlobalState addr -> addr -> STM (Maybe (Connection addr))
findConnection GlobalState {..} addr = do
    find ((addr==) . cAddress) <$> readTVar gConnections

newConnection :: GlobalState addr -> addr -> STM (Connection addr)
newConnection cGlobalState@GlobalState {..} addr = do
    conns <- readTVar gConnections

    let cAddress = addr
    (cDataUp, cDataInternal) <- newFlow
    cChannel <- newTVar ChannelNone
    cSecureOutQueue <- newTQueue
    cMaxInFlightPackets <- newTVar 4
    cReservedPackets <- newTVar 0
    cSentPackets <- newTVar []
    cToAcknowledge <- newTVar []
    cInStreams <- newTVar []
    cOutStreams <- newTVar []
    let conn = Connection {..}

    writeTVar gConnections (conn : conns)
    return conn

passUpIncoming :: GlobalState addr -> STM (IO ())
passUpIncoming GlobalState {..} = do
    (Connection {..}, up) <- takeTMVar gNextUp
    writeFlow cDataInternal up
    return $ return ()

processIncoming :: GlobalState addr -> STM (IO ())
processIncoming gs@GlobalState {..} = do
    guard =<< isEmptyTMVar gNextUp
    guard =<< canWriteFlow gControlFlow

    (addr, msg) <- readFlow gDataFlow
    mbconn <- findConnection gs addr

    mbch <- case mbconn of
        Nothing -> return Nothing
        Just conn -> readTVar (cChannel conn) >>= return . \case
            ChannelEstablished ch   -> Just ch
            ChannelOurAccept _ _ ch -> Just ch
            _                       -> Nothing

    return $ do
        let deserialize = liftEither . runExcept . deserializeObjects gStorage . BL.fromStrict
        let parse = case B.uncons msg of
                Just (b, enc)
                    | b .&. 0xE0 == 0x80 -> do
                        ch <- maybe (throwError "unexpected encrypted packet") return mbch
                        (dec, counter) <- channelDecrypt ch enc

                        case B.uncons dec of
                            Just (0x00, content) -> do
                                objs <- deserialize content
                                return $ Left (True, objs, Just counter)

                            Just (snum, dec')
                                | snum < 64
                                , Just (seq8, content) <- B.uncons dec'
                                -> do
                                    return $ Right (snum, seq8, content, counter)

                            Just (_, _) -> do
                                throwError "unexpected stream header"

                            Nothing -> do
                                throwError "empty decrypted content"

                    | b .&. 0xE0 == 0x60 -> do
                        objs <- deserialize msg
                        return $ Left (False, objs, Nothing)

                    | otherwise -> throwError "invalid packet"

                Nothing -> throwError "empty packet"

        runExceptT parse >>= \case
            Right (Left (secure, objs, mbcounter))
                | hobj:content <- objs
                , Just header@(TransportHeader items) <- transportFromObject hobj
                -> processPacket gs (maybe (Left addr) Right mbconn) secure (TransportPacket header content) >>= \case
                    Just (conn@Connection {..}, mbup) -> do
                        ioAfter <- atomically $ do
                            case mbcounter of
                                Just counter | any isHeaderItemAcknowledged items ->
                                    modifyTVar' cToAcknowledge (fromIntegral counter :)
                                _ -> return ()
                            case mbup of
                                Just up -> putTMVar gNextUp (conn, (secure, up))
                                Nothing -> return ()
                            processAcknowledgements gs conn items
                        ioAfter
                    Nothing -> return ()

                | otherwise -> atomically $ do
                      gLog $ show addr ++ ": invalid objects"
                      gLog $ show objs

            Right (Right (snum, seq8, content, counter))
                | Just Connection {..} <- mbconn
                -> atomically $ do
                    (lookup snum <$> readTVar cInStreams) >>= \case
                        Nothing ->
                            gLog $ "unexpected stream number " ++ show snum

                        Just Stream {..} -> do
                            expectedSequence <- readTVar sNextSequence
                            let seqFull = expectedSequence - 0x80 + fromIntegral (seq8 - fromIntegral expectedSequence + 0x80 :: Word8)
                            sdata <- if
                                | B.null content -> do
                                    modifyTVar' cInStreams $ filter ((/=snum) . fst)
                                    return $ StreamClosed seqFull
                                | otherwise -> do
                                    writeTVar sNextSequence $ max expectedSequence (seqFull + 1)
                                    return $ StreamData seqFull content
                            writeFlow sFlowIn sdata
                            modifyTVar' cToAcknowledge (fromIntegral counter :)

                | otherwise -> do
                    atomically $ gLog $ show addr <> ": stream packet without connection"

            Left err -> do
                atomically $ gLog $ show addr <> ": failed to parse packet: " <> err

processPacket :: GlobalState addr -> Either addr (Connection addr) -> Bool -> TransportPacket a -> IO (Maybe (Connection addr, Maybe (TransportPacket a)))
processPacket gs@GlobalState {..} econn secure packet@(TransportPacket (TransportHeader header) _) = if
    -- Established secure communication
    | Right conn <- econn, secure
    -> return $ Just (conn, Just packet)

    -- Plaintext communication with cookies to prove origin
    | cookie:_ <- mapMaybe (\case CookieEcho x -> Just x; _ -> Nothing) header
    -> verifyCookie gs addr cookie >>= \case
        True -> do
            atomically $ do
                conn@Connection {..} <- getConnection gs addr
                channel <- readTVar cChannel
                let received = listToMaybe $ mapMaybe (\case CookieSet x -> Just x; _ -> Nothing) header
                case received `mplus` channelCurrentCookie channel of
                    Just current -> do
                        cookieEchoReceived gs conn mbpid current
                        return $ Just (conn, Just packet)
                    Nothing -> do
                        gLog $ show addr <> ": missing cookie set, dropping " <> show header
                        return $ Nothing

        False -> do
            atomically $ gLog $ show addr <> ": cookie verification failed, dropping " <> show header
            return Nothing

    -- Response to initiation packet
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

    -- Initiation packet
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

    -- Announce packet outside any connection
    | dgst:_ <- mapMaybe (\case AnnounceSelf x -> Just x; _ -> Nothing) header
    , Just _ <- version
    -> do
        atomically $ do
            (cur, past) <- readTVar gIdentity
            when (not $ dgst `elem` map (refDigest . storedRef . idData) (cur : past)) $ do
                writeFlow gControlFlow $ ReceivedAnnounce addr dgst
        return Nothing

    | otherwise -> do
        atomically $ gLog $ show addr <> ": dropping packet " <> show header
        return Nothing

  where
    addr = either id cAddress econn
    mbpid = listToMaybe $ mapMaybe (\case AnnounceSelf dgst -> Just dgst; _ -> Nothing) header
    version = listToMaybe $ filter (\v -> ProtocolVersion v `elem` header) protocolVersions

channelCurrentCookie :: ChannelState -> Maybe Cookie
channelCurrentCookie = \case
    ChannelCookieReceived cookie -> Just cookie
    ChannelCookieConfirmed cookie -> Just cookie
    ChannelOurRequest mbcookie _ -> mbcookie
    ChannelPeerRequest mbcookie _ -> mbcookie
    ChannelOurAccept mbcookie _ _ -> mbcookie
    _ -> Nothing

cookieEchoReceived :: GlobalState addr -> Connection addr -> Maybe RefDigest -> Cookie -> STM ()
cookieEchoReceived GlobalState {..} conn@Connection {..} mbpid cookieSet = do
    readTVar cChannel >>= \case
        ChannelNone -> newConn
        ChannelCookieWait -> newConn
        ChannelCookieReceived {} -> update
        _ -> return ()
  where
    update = do
        writeTVar cChannel $ ChannelCookieConfirmed cookieSet
    newConn = do
        update
        writeFlow gControlFlow (NewConnection conn mbpid)

generateCookieHeaders :: GlobalState addr -> addr -> ChannelState -> IO [TransportHeaderItem]
generateCookieHeaders gs addr ch = catMaybes <$> sequence [ echoHeader, setHeader ]
  where
    echoHeader = return $ CookieEcho <$> channelCurrentCookie ch
    setHeader = case ch of
        ChannelCookieWait {} -> Just . CookieSet <$> createCookie gs addr
        ChannelCookieReceived {} -> Just . CookieSet <$> createCookie gs addr
        _ -> return Nothing

createCookie :: GlobalState addr -> addr -> IO Cookie
createCookie GlobalState {} addr = return (Cookie $ BC.pack $ show addr)

verifyCookie :: GlobalState addr -> addr -> Cookie -> IO Bool
verifyCookie GlobalState {} addr (Cookie cookie) = return $ show addr == BC.unpack cookie


reservePacket :: Connection addr -> STM ReservedToSend
reservePacket Connection {..} = do
    maxPackets <- readTVar cMaxInFlightPackets
    reserved <- readTVar cReservedPackets
    sent <- length <$> readTVar cSentPackets

    when (sent + reserved >= maxPackets) $ do
        retry

    writeTVar cReservedPackets $ reserved + 1
    return $ ReservedToSend Nothing (return ())

resendBytes :: Connection addr -> Maybe ReservedToSend -> SentPacket -> IO ()
resendBytes Connection {..} reserved sp = do
    let GlobalState {..} = cGlobalState
    now <- getTime MonotonicRaw
    atomically $ do
        when (isJust reserved) $ do
            modifyTVar' cReservedPackets (subtract 1)

        when (isJust $ spAckedBy sp) $ do
            modifyTVar' cSentPackets $ (:) sp
                { spTime = now
                , spRetryCount = spRetryCount sp + 1
                }
        writeFlow gDataFlow (cAddress, spData sp)

sendBytes :: Connection addr -> Maybe ReservedToSend -> ByteString -> IO ()
sendBytes conn reserved bs = resendBytes conn reserved
    SentPacket
        { spTime = undefined
        , spRetryCount = -1
        , spAckedBy = rsAckedBy =<< reserved
        , spOnAck = maybe (return ()) rsOnAck reserved
        , spData = bs
        }

processOutgoing :: forall addr. GlobalState addr -> STM (IO ())
processOutgoing gs@GlobalState {..} = do

    let sendNextPacket :: Connection addr -> STM (IO ())
        sendNextPacket conn@Connection {..} = do
            channel <- readTVar cChannel
            let mbch = case channel of
                    ChannelEstablished ch -> Just ch
                    _                     -> Nothing

            let checkOutstanding
                    | isJust mbch = do
                        (,) <$> readTQueue cSecureOutQueue <*> (Just <$> reservePacket conn)
                    | otherwise = retry

                checkDataInternal = do
                    (,) <$> readFlow cDataInternal <*> (Just <$> reservePacket conn)

                checkAcknowledgements
                    | isJust mbch = do
                        acks <- readTVar cToAcknowledge
                        if null acks then retry
                                     else return ((True, TransportPacket (TransportHeader []) [], []), Nothing)
                    | otherwise = retry

            ((secure, packet@(TransportPacket (TransportHeader hitems) content), plainAckedBy), mbReserved) <-
                checkOutstanding <|> checkDataInternal <|> checkAcknowledgements

            when (isNothing mbch && secure) $ do
                writeTQueue cSecureOutQueue (secure, packet, plainAckedBy)

            acknowledge <- case mbch of
                Nothing -> return []
                Just _ -> swapTVar cToAcknowledge []

            return $ do
                cookieHeaders <- generateCookieHeaders gs cAddress channel
                let header = TransportHeader $ map AcknowledgedSingle acknowledge ++ cookieHeaders ++ hitems

                let plain = BL.concat $
                        (serializeObject $ transportToObject gStorage header)
                        : map lazyLoadBytes content

                let onAck = case catMaybes (map (\case StreamOpen n -> Just n; _ -> Nothing) hitems) of
                                 [] -> return ()
                                 xs -> sequence_ $ map (streamAccepted conn) xs

                mbs <- case mbch of
                    Just ch -> do
                        runExceptT (channelEncrypt ch $ BL.toStrict $ 0x00 `BL.cons` plain) >>= \case
                            Right (ctext, counter) -> do
                                let isAcked = any isHeaderItemAcknowledged hitems
                                return $ Just (0x80 `B.cons` ctext, if isAcked then [ AcknowledgedSingle $ fromIntegral counter ] else [])
                            Left err -> do atomically $ gLog $ "Failed to encrypt data: " ++ err
                                           return Nothing
                    Nothing | secure    -> return Nothing
                            | otherwise -> return $ Just (BL.toStrict plain, plainAckedBy)

                case mbs of
                    Just (bs, ackedBy) -> do
                        let mbReserved' = (\rs -> rs
                                { rsAckedBy = guard (not $ null ackedBy) >> Just (`elem` ackedBy)
                                , rsOnAck = rsOnAck rs >> onAck
                                }) <$> mbReserved
                        sendBytes conn mbReserved' bs
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
                reserved <- reservePacket conn
                writeTVar cSentPackets rest
                return $ resendBytes conn (Just reserved) sp

    let handleControlRequests = readFlow gControlFlow >>= \case
            RequestConnection addr -> do
                conn@Connection {..} <- getConnection gs addr
                identity <- fst <$> readTVar gIdentity
                readTVar cChannel >>= \case
                    ChannelNone -> do
                        reserved <- reservePacket conn
                        let packet = BL.toStrict $ BL.concat
                                [ serializeObject $ transportToObject gStorage $ TransportHeader $
                                    [ Initiation $ refDigest gInitConfig
                                    , AnnounceSelf $ refDigest $ storedRef $ idData identity
                                    ] ++ map ProtocolVersion protocolVersions
                                , lazyLoadBytes gInitConfig
                                ]
                        writeTVar cChannel ChannelCookieWait
                        let reserved' = reserved { rsAckedBy = Just $ \case CookieSet {} -> True; _ -> False }
                        return $ sendBytes conn (Just reserved') packet
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

processAcknowledgements :: GlobalState addr -> Connection addr -> [TransportHeaderItem] -> STM (IO ())
processAcknowledgements GlobalState {} Connection {..} header = do
    (acked, notAcked) <- partition (\sp -> any (fromJust (spAckedBy sp)) header) <$> readTVar cSentPackets
    writeTVar cSentPackets notAcked
    return $ sequence_ $ map spOnAck acked

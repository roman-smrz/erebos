module Erebos.Network.Protocol (
    TransportPacket(..),
    transportToObject,
    TransportHeader(..),
    TransportHeaderItem(..),
    ServiceID(..),
    SecurityRequirement(..),

    WaitingRef(..),
    DataRequestResult(..),
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
    connClose,

    RawStreamReader(..), RawStreamWriter(..),
    StreamPacket(..),
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
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Trans

import Crypto.Cipher.ChaChaPoly1305 qualified as C
import Crypto.MAC.Poly1305 qualified as C (Auth(..), authTag)
import Crypto.Error
import Crypto.Random

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.ByteArray (Bytes, ScrubbedBytes)
import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.Function
import Data.List
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T
import Data.Void

import System.Clock

import Erebos.Flow
import Erebos.Identity
import Erebos.Network.Channel
import Erebos.Object
import Erebos.Storable
import Erebos.Storage
import Erebos.UUID (UUID)


protocolVersion :: Text
protocolVersion = T.pack "0.1"

protocolVersions :: [Text]
protocolVersions = [protocolVersion]

keepAliveInternal :: TimeSpec
keepAliveInternal = fromNanoSecs $ 30 * 10^(9 :: Int)


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

newtype ServiceID = ServiceID UUID
    deriving (Eq, Ord, StorableUUID)

instance Show ServiceID where
    show = show . toUUID

newtype Cookie = Cookie ByteString
    deriving (Eq, Show)

data SecurityRequirement = PlaintextOnly
                         | PlaintextAllowed
                         | EncryptedOnly
    deriving (Eq, Ord)

data ParsedCookie = ParsedCookie
    { cookieNonce :: C.Nonce
    , cookieValidity :: Word32
    , cookieContent :: ByteString
    , cookieMac :: C.Auth
    }

instance Eq ParsedCookie where
    (==) = (==) `on` (\c -> ( BA.convert (cookieNonce c) :: ByteString, cookieValidity c, cookieContent c, cookieMac c ))

instance Show ParsedCookie where
    show ParsedCookie {..} = show (nonce, cookieValidity, cookieContent, mac)
      where C.Auth mac = cookieMac
            nonce = BA.convert cookieNonce :: ByteString

instance Binary ParsedCookie where
    put ParsedCookie {..} = do
        putByteString $ BA.convert cookieNonce
        putWord32be cookieValidity
        putByteString $ BA.convert cookieMac
        putByteString cookieContent

    get = do
        Just cookieNonce <- maybeCryptoError . C.nonce12 <$> getByteString 12
        cookieValidity <- getWord32be
        Just cookieMac <- maybeCryptoError . C.authTag <$> getByteString 16
        cookieContent <- BL.toStrict <$> getRemainingLazyByteString
        return ParsedCookie {..}

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
    , gTestLog :: String -> STM ()
    , gStorage :: PartialStorage
    , gStartTime :: TimeSpec
    , gNowVar :: TVar TimeSpec
    , gNextTimeout :: TVar TimeSpec
    , gInitConfig :: Ref
    , gCookieKey :: ScrubbedBytes
    , gCookieStartTime :: Word32
    }

data Connection addr = Connection
    { cGlobalState :: GlobalState addr
    , cAddress :: addr
    , cDataUp :: Flow
        (Maybe (Bool, TransportPacket PartialObject))
        (SecurityRequirement, TransportPacket Ref, [TransportHeaderItem])
    , cDataInternal :: Flow
        (SecurityRequirement, TransportPacket Ref, [TransportHeaderItem])
        (Maybe (Bool, TransportPacket PartialObject))
    , cChannel :: TVar ChannelState
    , cCookie :: TVar (Maybe Cookie)
    , cSecureOutQueue :: TQueue (SecurityRequirement, TransportPacket Ref, [TransportHeaderItem])
    , cMaxInFlightPackets :: TVar Int
    , cReservedPackets :: TVar Int
    , cSentPackets :: TVar [SentPacket]
    , cToAcknowledge :: TVar [Integer]
    , cNextKeepAlive :: TVar (Maybe TimeSpec)
    , cInStreams :: TVar [(Word8, Stream)]
    , cOutStreams :: TVar [(Word8, Stream)]
    }

instance Eq (Connection addr) where
    (==) = (==) `on` cChannel

connAddress :: Connection addr -> addr
connAddress = cAddress

showConnAddress :: forall addr. Connection addr -> String
showConnAddress Connection {..} = helper cGlobalState cAddress
  where
    helper :: GlobalState addr -> addr -> String
    helper GlobalState {} = show

connData :: Connection addr -> Flow
    (Maybe (Bool, TransportPacket PartialObject))
    (SecurityRequirement, TransportPacket Ref, [TransportHeaderItem])
connData = cDataUp

connGetChannel :: Connection addr -> STM ChannelState
connGetChannel Connection {..} = readTVar cChannel

connSetChannel :: Connection addr -> ChannelState -> STM ()
connSetChannel Connection {..} ch = do
    writeTVar cChannel ch

connClose :: Connection addr -> STM ()
connClose conn@Connection {..} = do
    let GlobalState {..} = cGlobalState
    readTVar cChannel >>= \case
        ChannelClosed -> return ()
        _ -> do
            writeTVar cChannel ChannelClosed
            writeTVar gConnections . filter (/=conn) =<< readTVar gConnections
            writeFlow cDataInternal Nothing

connAddWriteStream :: Connection addr -> STM (Either String (TransportHeaderItem, RawStreamWriter, IO ()))
connAddWriteStream conn@Connection {..} = do
    let GlobalState {..} = cGlobalState
    outStreams <- readTVar cOutStreams
    let doInsert :: Word8 -> [(Word8, Stream)] -> ExceptT String STM ((Word8, Stream), [(Word8, Stream)])
        doInsert n (s@(n', _) : rest) | n == n' =
            fmap (s:) <$> doInsert (n + 1) rest
        doInsert n streams | n < 63 = lift $ do
            sState <- newTVar StreamOpening
            (sFlowIn, sFlowOut) <- newFlow
            sNextSequence <- newTVar 0
            sWaitingForAck <- newTVar 0
            let info = (n, Stream {..})
            return (info, info : streams)
        doInsert _ _ = throwError "all outbound streams in use"

    runExceptT $ do
        ((streamNumber, stream), outStreams') <- doInsert 1 outStreams
        lift $ writeTVar cOutStreams outStreams'
        lift $ gTestLog $ "net-ostream-open " <> showConnAddress conn <> " " <> show streamNumber <> " " <> show (length outStreams')
        return
            ( StreamOpen streamNumber
            , RawStreamWriter (fromIntegral streamNumber) (sFlowIn stream)
            , go streamNumber stream
            )

  where
    go streamNumber stream = do
            let GlobalState {..} = cGlobalState
            (reserved, msg) <- atomically $ do
                readTVar (sState stream) >>= \case
                    StreamRunning -> return ()
                    _             -> retry
                (,) <$> reservePacket conn
                    <*> readFlow (sFlowOut stream)

            (plain, cont, onAck) <- case msg of
                StreamData {..} -> do
                    return (stpData, True, return ())
                StreamClosed {} -> do
                    atomically $ do
                        gTestLog $ "net-ostream-close-send " <> showConnAddress conn <> " " <> show streamNumber
                    atomically $ do
                        -- wait for ack on all sent stream data
                        waits <- readTVar (sWaitingForAck stream)
                        when (waits > 0) retry
                    return (BC.empty, False, streamClosed conn streamNumber)

            let secure = True
                plainAckedBy = []
                mbReserved = Just reserved

            mbch <- atomically (readTVar cChannel) >>= return . \case
                ChannelEstablished ch -> Just ch
                ChannelOurAccept _ ch -> Just ch
                _                     -> Nothing

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
                        Left err -> do atomically $ gLog $ "Failed to encrypt data: " ++ showErebosError err
                                       return Nothing
                Nothing | secure    -> return Nothing
                        | otherwise -> return $ Just (plain, plainAckedBy)

            case mbs of
                Just (bs, ackedBy) -> do
                    atomically $ do
                        modifyTVar' (sWaitingForAck stream) (+ 1)
                    let mbReserved' = (\rs -> rs
                            { rsAckedBy = guard (not $ null ackedBy) >> Just (`elem` ackedBy)
                            , rsOnAck = do
                                rsOnAck rs
                                onAck
                                atomically $ modifyTVar' (sWaitingForAck stream) (subtract 1)
                            }) <$> mbReserved
                    sendBytes conn mbReserved' bs
                Nothing -> return ()

            when cont $ go streamNumber stream

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
            sWaitingForAck <- newTVar 0
            let stream = Stream {..}
            return ( streamNumber, stream, (streamNumber, stream) : streams )
    ( num, stream, inStreams' ) <- doInsert inStreams
    writeTVar cInStreams inStreams'
    return $ RawStreamReader (fromIntegral num) (sFlowOut stream)


data RawStreamReader = RawStreamReader
    { rsrNum :: Int
    , rsrFlow :: Flow StreamPacket Void
    }

data RawStreamWriter = RawStreamWriter
    { rswNum :: Int
    , rswFlow :: Flow Void StreamPacket
    }

data Stream = Stream
    { sState :: TVar StreamState
    , sFlowIn :: Flow Void StreamPacket
    , sFlowOut :: Flow StreamPacket Void
    , sNextSequence :: TVar Word64
    , sWaitingForAck :: TVar Word64
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
streamClosed conn@Connection {..} snum = atomically $ do
    streams <- filter ((snum /=) . fst) <$> readTVar cOutStreams
    writeTVar cOutStreams streams
    gTestLog cGlobalState $ "net-ostream-close-ack " <> showConnAddress conn <> " " <> show snum <> " " <> show (length streams)

readStreamToList :: RawStreamReader -> IO (Word64, [(Word64, BC.ByteString)])
readStreamToList stream = readFlowIO (rsrFlow stream) >>= \case
    StreamData sq bytes -> fmap ((sq, bytes) :) <$> readStreamToList stream
    StreamClosed sqEnd  -> return (sqEnd, [])

readObjectsFromStream :: PartialStorage -> RawStreamReader -> IO (Except ErebosError [PartialObject])
readObjectsFromStream st stream = do
    (seqEnd, list) <- readStreamToList stream
    let validate s ((s', bytes) : rest)
            | s == s'   = (bytes : ) <$> validate (s + 1) rest
            | s >  s'   = validate s rest
            | otherwise = throwOtherError "missing object chunk"
        validate s []
            | s == seqEnd = return []
            | otherwise = throwOtherError "content length mismatch"
    return $ do
        content <- BL.fromChunks <$> validate 0 list
        deserializeObjects st content

writeByteStringToStream :: RawStreamWriter -> BL.ByteString -> IO ()
writeByteStringToStream stream = go 0
  where
    go seqNum bstr
        | BL.null bstr = writeFlowIO (rswFlow stream) $ StreamClosed seqNum
        | otherwise    = do
            let (cur, rest) = BL.splitAt 500 bstr -- TODO: MTU
            writeFlowIO (rswFlow stream) $ StreamData seqNum (BL.toStrict cur)
            go (seqNum + 1) rest


data WaitingRef = WaitingRef
    { wrefStorage :: Storage
    , wrefPartial :: PartialRef
    , wrefBound :: Word64
    , wrefAction :: DataRequestResult -> WaitingRefCallback
    , wrefStatus :: TVar (Either [ RefDigest ] DataRequestResult)
    }

data DataRequestResult
    = DataRequestFulfilled Ref
    | DataRequestRejected
    | DataRequestBrokenBound

type WaitingRefCallback = ExceptT ErebosError IO ()

wrDigest :: WaitingRef -> RefDigest
wrDigest = refDigest . wrefPartial


data ChannelState = ChannelNone
                  | ChannelCookieWait -- sent initiation, waiting for response
                  | ChannelCookieReceived -- received cookie, but no cookie echo yet
                  | ChannelCookieConfirmed -- received cookie echo, no need to send from our side
                  | ChannelOurRequest (Stored ChannelRequest)
                  | ChannelPeerRequest WaitingRef
                  | ChannelOurAccept (Stored ChannelAccept) Channel
                  | ChannelEstablished Channel
                  | ChannelClosed

data ReservedToSend = ReservedToSend
    { rsAckedBy :: Maybe (TransportHeaderItem -> Bool)
    , rsOnAck :: IO ()
    , rsOnFail :: IO ()
    }

data SentPacket = SentPacket
    { spTime :: TimeSpec
    , spRetryCount :: Int
    , spAckedBy :: Maybe (TransportHeaderItem -> Bool)
    , spOnAck :: IO ()
    , spOnFail :: IO ()
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
                      -> (String -> STM ())
                      -> SymFlow (addr, ByteString)
                      -> Flow (ControlRequest addr) (ControlMessage addr)
                      -> IO ()
erebosNetworkProtocol initialIdentity gLog gTestLog gDataFlow gControlFlow = do
    gIdentity <- newTVarIO (initialIdentity, [])
    gConnections <- newTVarIO []
    gNextUp <- newEmptyTMVarIO
    mStorage <- memoryStorage
    gStorage <- derivePartialStorage mStorage

    gStartTime <- getTime Monotonic
    gNowVar <- newTVarIO gStartTime
    gNextTimeout <- newTVarIO gStartTime
    gInitConfig <- store mStorage $ (Rec [] :: Object)

    gCookieKey <- getRandomBytes 32
    gCookieStartTime <- runGet getWord32host . BL.pack . BA.unpack @ScrubbedBytes <$> getRandomBytes 4

    let gs = GlobalState {..}

    let signalTimeouts = forever $ do
            now <- getTime Monotonic
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

    race_ signalTimeouts $ forever $ do
        io <- atomically $ do
            passUpIncoming gs <|> processIncoming gs <|> processOutgoing gs
        catch io $ \(e :: SomeException) -> atomically $ gLog $ "exception during network protocol handling: " <> show e


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
    cCookie <- newTVar Nothing
    cSecureOutQueue <- newTQueue
    cMaxInFlightPackets <- newTVar 4
    cReservedPackets <- newTVar 0
    cSentPackets <- newTVar []
    cToAcknowledge <- newTVar []
    cNextKeepAlive <- newTVar Nothing
    cInStreams <- newTVar []
    cOutStreams <- newTVar []
    let conn = Connection {..}

    gTestLog $ "net-conn-new " <> show cAddress
    writeTVar gConnections (conn : conns)
    return conn

passUpIncoming :: GlobalState addr -> STM (IO ())
passUpIncoming GlobalState {..} = do
    (Connection {..}, up) <- takeTMVar gNextUp
    writeFlow cDataInternal (Just up)
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
            ChannelEstablished ch -> Just ch
            ChannelOurAccept _ ch -> Just ch
            _                     -> Nothing

    return $ do
        let deserialize = liftEither . runExcept . deserializeObjects gStorage . BL.fromStrict
        let parse = case B.uncons msg of
                Just (b, enc)
                    | b .&. 0xE0 == 0x80 -> do
                        ch <- maybe (throwOtherError "unexpected encrypted packet") return mbch
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
                                throwOtherError "unexpected stream header"

                            Nothing -> do
                                throwOtherError "empty decrypted content"

                    | b .&. 0xE0 == 0x60 -> do
                        objs <- deserialize msg
                        return $ Left (False, objs, Nothing)

                    | otherwise -> throwOtherError "invalid packet"

                Nothing -> throwOtherError "empty packet"

        now <- getTime Monotonic
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
                            updateKeepAlive conn now
                            processAcknowledgements gs conn items
                        ioAfter
                    Nothing -> return ()

                | otherwise -> atomically $ do
                      gLog $ show addr ++ ": invalid objects"
                      gLog $ show objs

            Right (Right (snum, seq8, content, counter))
                | Just conn@Connection {..} <- mbconn
                -> atomically $ do
                    updateKeepAlive conn now
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
                atomically $ gLog $ show addr <> ": failed to parse packet: " <> showErebosError err

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
                oldCookie <- readTVar cCookie
                let received = listToMaybe $ mapMaybe (\case CookieSet x -> Just x; _ -> Nothing) header
                case received `mplus` oldCookie of
                    Just current -> do
                        writeTVar cCookie (Just current)
                        cookieEchoReceived gs conn mbpid
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
                writeTVar cChannel $ ChannelCookieReceived
                writeTVar cCookie $ Just cookie
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

cookieEchoReceived :: GlobalState addr -> Connection addr -> Maybe RefDigest -> STM ()
cookieEchoReceived GlobalState {..} conn@Connection {..} mbpid = do
    readTVar cChannel >>= \case
        ChannelNone -> newConn
        ChannelCookieWait -> newConn
        ChannelCookieReceived {} -> update
        _ -> return ()
  where
    update = do
        writeTVar cChannel ChannelCookieConfirmed
    newConn = do
        update
        writeFlow gControlFlow (NewConnection conn mbpid)

generateCookieHeaders :: Connection addr -> ChannelState -> IO [TransportHeaderItem]
generateCookieHeaders Connection {..} ch = catMaybes <$> sequence [ echoHeader, setHeader ]
  where
    echoHeader = fmap CookieEcho <$> atomically (readTVar cCookie)
    setHeader = case ch of
        ChannelCookieWait {} -> Just . CookieSet <$> createCookie cGlobalState cAddress
        ChannelCookieReceived {} -> Just . CookieSet <$> createCookie cGlobalState cAddress
        _ -> return Nothing

createCookie :: GlobalState addr -> addr -> IO Cookie
createCookie GlobalState {..} addr = do
    (nonceBytes :: Bytes) <- getRandomBytes 12
    validUntil <- (fromNanoSecs (60 * 10^(9 :: Int)) +) <$> getTime Monotonic
    let validSecondsFromStart = fromIntegral $ toNanoSecs (validUntil - gStartTime) `div` (10^(9 :: Int))
        cookieValidity = validSecondsFromStart - gCookieStartTime
        plainContent = BC.pack (show addr)
    throwCryptoErrorIO $ do
        cookieNonce <- C.nonce12 nonceBytes
        st1 <- C.initialize gCookieKey cookieNonce
        let st2 = C.finalizeAAD $ C.appendAAD (BL.toStrict $ runPut $ putWord32be cookieValidity) st1
            (cookieContent, st3) = C.encrypt plainContent st2
            cookieMac = C.finalize st3
        return $ Cookie $ BL.toStrict $ encode $ ParsedCookie {..}

verifyCookie :: GlobalState addr -> addr -> Cookie -> IO Bool
verifyCookie GlobalState {..} addr (Cookie cookie) = do
    ctime <- getTime Monotonic
    return $ fromMaybe False $ do
        ( _, _, ParsedCookie {..} ) <- either (const Nothing) Just $ decodeOrFail $ BL.fromStrict cookie
        maybeCryptoError $ do
            st1 <- C.initialize gCookieKey cookieNonce
            let st2 = C.finalizeAAD $ C.appendAAD (BL.toStrict $ runPut $ putWord32be cookieValidity) st1
                (plainContent, st3) = C.decrypt cookieContent st2
                mac = C.finalize st3

                validSecondsFromStart = fromIntegral $ cookieValidity + gCookieStartTime
                validUntil = gStartTime + fromNanoSecs (validSecondsFromStart * (10^(9 :: Int)))
            return $ and
                [ mac == cookieMac
                , ctime <= validUntil
                , show addr == BC.unpack plainContent
                ]

reservePacket :: Connection addr -> STM ReservedToSend
reservePacket conn@Connection {..} = do
    maxPackets <- readTVar cMaxInFlightPackets
    reserved <- readTVar cReservedPackets
    sent <- length <$> readTVar cSentPackets

    when (sent + reserved >= maxPackets) $ do
        retry

    writeTVar cReservedPackets $ reserved + 1
    return $ ReservedToSend Nothing (return ()) (atomically $ connClose conn)

resendBytes :: Connection addr -> Maybe ReservedToSend -> SentPacket -> IO ()
resendBytes conn@Connection {..} reserved sp = do
    let GlobalState {..} = cGlobalState
    now <- getTime Monotonic
    atomically $ do
        when (isJust reserved) $ do
            modifyTVar' cReservedPackets (subtract 1)

        when (isJust $ spAckedBy sp) $ do
            modifyTVar' cSentPackets $ (:) sp
                { spTime = now
                , spRetryCount = spRetryCount sp + 1
                }
        writeFlow gDataFlow (cAddress, spData sp)
        updateKeepAlive conn now

sendBytes :: Connection addr -> Maybe ReservedToSend -> ByteString -> IO ()
sendBytes conn reserved bs = resendBytes conn reserved
    SentPacket
        { spTime = undefined
        , spRetryCount = -1
        , spAckedBy = rsAckedBy =<< reserved
        , spOnAck = maybe (return ()) rsOnAck reserved
        , spOnFail = maybe (return ()) rsOnFail reserved
        , spData = bs
        }

updateKeepAlive :: Connection addr -> TimeSpec -> STM ()
updateKeepAlive Connection {..} now = do
    let next = now + keepAliveInternal
    writeTVar cNextKeepAlive $ Just next


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
                                     else return ((EncryptedOnly, TransportPacket (TransportHeader []) [], []), Nothing)
                    | otherwise = retry

            ((secure, packet@(TransportPacket (TransportHeader hitems) content), plainAckedBy), mbReserved) <-
                checkOutstanding <|> checkDataInternal <|> checkAcknowledgements

            when (isNothing mbch && secure >= EncryptedOnly) $ do
                writeTQueue cSecureOutQueue (secure, packet, plainAckedBy)

            acknowledge <- case mbch of
                Nothing -> return []
                Just _ -> swapTVar cToAcknowledge []

            return $ do
                let onAck = sequence_ $ map (streamAccepted conn) $
                        catMaybes (map (\case StreamOpen n -> Just n; _ -> Nothing) hitems)

                let mkPlain extraHeaders
                        | combinedHeaderItems@(_:_) <- map AcknowledgedSingle acknowledge ++ extraHeaders ++ hitems =
                             BL.concat $
                                (serializeObject $ transportToObject gStorage $ TransportHeader combinedHeaderItems)
                                : map lazyLoadBytes content
                        | otherwise = BL.empty

                let usePlaintext = do
                        plain <- mkPlain <$> generateCookieHeaders conn channel
                        return $ Just (BL.toStrict plain, plainAckedBy)

                let useEncryption ch = do
                        plain <- mkPlain <$> return []
                        runExceptT (channelEncrypt ch $ BL.toStrict $ 0x00 `BL.cons` plain) >>= \case
                            Right (ctext, counter) -> do
                                let isAcked = any isHeaderItemAcknowledged hitems
                                return $ Just (0x80 `B.cons` ctext, if isAcked then [ AcknowledgedSingle $ fromIntegral counter ] else [])
                            Left err -> do atomically $ gLog $ "Failed to encrypt data: " ++ showErebosError err
                                           return Nothing

                mbs <- case (secure, mbch) of
                    (PlaintextOnly, _)          -> usePlaintext
                    (PlaintextAllowed, Nothing) -> usePlaintext
                    (_, Just ch)                -> useEncryption ch
                    (EncryptedOnly, Nothing)    -> return Nothing

                case mbs of
                    Just (bs, ackedBy) -> do
                        let mbReserved' = (\rs -> rs
                                { rsAckedBy = guard (not $ null ackedBy) >> Just (`elem` ackedBy)
                                , rsOnAck = rsOnAck rs >> onAck
                                }) <$> mbReserved
                        sendBytes conn mbReserved' bs
                    Nothing -> do
                        when (isJust mbReserved) $ do
                            atomically $ do
                                modifyTVar' cReservedPackets (subtract 1)

    let waitUntil :: TimeSpec -> TimeSpec -> STM ()
        waitUntil now till = do
            nextTimeout <- readTVar gNextTimeout
            if nextTimeout <= now || till < nextTimeout
               then writeTVar gNextTimeout till
               else retry

    let retransmitPacket :: Connection addr -> STM (IO ())
        retransmitPacket conn@Connection {..} = do
            now <- readTVar gNowVar
            (sp, rest) <- readTVar cSentPackets >>= \case
                sps@(_:_) -> return (last sps, init sps)
                _         -> retry
            let nextTry = spTime sp + fromNanoSecs 1000000000
            if | now < nextTry -> do
                    waitUntil now nextTry
                    return $ return ()
               | spRetryCount sp < 2 -> do
                    reserved <- reservePacket conn
                    writeTVar cSentPackets rest
                    return $ resendBytes conn (Just reserved) sp
               | otherwise -> do
                    return $ spOnFail sp

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

    let sendKeepAlive :: Connection addr -> STM (IO ())
        sendKeepAlive Connection {..} = do
            readTVar cNextKeepAlive >>= \case
                Nothing -> retry
                Just next -> do
                    now <- readTVar gNowVar
                    if next <= now
                      then do
                        writeTVar cNextKeepAlive Nothing
                        identity <- fst <$> readTVar gIdentity
                        let header = TransportHeader [ AnnounceSelf $ refDigest $ storedRef $ idData identity ]
                        writeTQueue cSecureOutQueue (EncryptedOnly, TransportPacket header [], [])
                      else do
                        waitUntil now next
                    return $ return ()

    conns <- readTVar gConnections
    msum $ concat $
        [ map retransmitPacket conns
        , map sendNextPacket conns
        , [ handleControlRequests ]
        , map sendKeepAlive conns
        ]

processAcknowledgements :: GlobalState addr -> Connection addr -> [TransportHeaderItem] -> STM (IO ())
processAcknowledgements GlobalState {} Connection {..} header = do
    (acked, notAcked) <- partition (\sp -> any (fromJust (spAckedBy sp)) header) <$> readTVar cSentPackets
    writeTVar cSentPackets notAcked
    return $ sequence_ $ map spOnAck acked

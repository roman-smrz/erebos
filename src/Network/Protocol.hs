module Network.Protocol (
    TransportPacket(..),
    transportToObject,
    TransportHeader(..),
    TransportHeaderItem(..),

    WaitingRef(..),
    WaitingRefCallback,
    wrDigest,

    ChannelState(..),

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

import System.Clock

import Channel
import Flow
import Service
import Storage


data TransportPacket a = TransportPacket TransportHeader [a]

data TransportHeader = TransportHeader [TransportHeaderItem]

data TransportHeaderItem
    = Acknowledged RefDigest
    | Rejected RefDigest
    | DataRequest RefDigest
    | DataResponse RefDigest
    | AnnounceSelf RefDigest
    | AnnounceUpdate RefDigest
    | TrChannelRequest RefDigest
    | TrChannelAccept RefDigest
    | ServiceType ServiceID
    | ServiceRef RefDigest
    deriving (Eq)

transportToObject :: PartialStorage -> TransportHeader -> PartialObject
transportToObject st (TransportHeader items) = Rec $ map single items
    where single = \case
              Acknowledged dgst -> (BC.pack "ACK", RecRef $ partialRefFromDigest st dgst)
              Rejected dgst -> (BC.pack "REJ", RecRef $ partialRefFromDigest st dgst)
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
    { gConnections :: TVar [Connection addr]
    , gDataFlow :: SymFlow (addr, ByteString)
    , gConnectionFlow :: Flow addr (Connection addr)
    , gLog :: String -> STM ()
    , gStorage :: PartialStorage
    , gNowVar :: TVar TimeSpec
    , gNextTimeout :: TVar TimeSpec
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
                  | ChannelOurRequest (Stored ChannelRequest)
                  | ChannelPeerRequest WaitingRef
                  | ChannelOurAccept (Stored ChannelAccept) Channel
                  | ChannelEstablished Channel


data SentPacket = SentPacket
    { spTime :: TimeSpec
    , spRetryCount :: Int
    , spAckedBy :: [TransportHeaderItem]
    , spData :: BC.ByteString
    }


erebosNetworkProtocol :: (Eq addr, Ord addr, Show addr)
                      => (String -> STM ())
                      -> SymFlow (addr, ByteString)
                      -> Flow addr (Connection addr)
                      -> IO ()
erebosNetworkProtocol gLog gDataFlow gConnectionFlow = do
    gConnections <- newTVarIO []
    gStorage <- derivePartialStorage =<< memoryStorage

    startTime <- getTime MonotonicRaw
    gNowVar <- newTVarIO startTime
    gNextTimeout <- newTVarIO startTime

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
getConnection GlobalState {..} addr = do
    conns <- readTVar gConnections
    case find ((addr==) . cAddress) conns of
        Just conn -> return conn
        Nothing -> do
            let cAddress = addr
            (cDataUp, cDataInternal) <- newFlow
            cChannel <- newTVar ChannelNone
            cSecureOutQueue <- newTQueue
            cSentPackets <- newTVar []
            let conn = Connection {..}

            writeTVar gConnections (conn : conns)
            writeFlow gConnectionFlow conn
            return conn

processIncomming :: GlobalState addr -> STM (IO ())
processIncomming gs@GlobalState {..} = do
    (addr, msg) <- readFlow gDataFlow
    conn@Connection {..} <- getConnection gs addr

    mbch <- readTVar cChannel >>= return . \case
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
                -> atomically $ do
                    processAcknowledgements gs conn items
                    writeFlow cDataInternal (secure, TransportPacket header content)

                | otherwise -> atomically $ do
                      gLog $ show cAddress ++ ": invalid objects"
                      gLog $ show objs

            Left err -> do
                atomically $ gLog $ show cAddress <> ": failed to parse packet: " <> err


processOutgoing :: forall addr. GlobalState addr -> STM (IO ())
processOutgoing gs@GlobalState {..} = do
    let sendBytes :: Connection addr -> SentPacket -> IO ()
        sendBytes Connection {..} sp = do
            now <- getTime MonotonicRaw
            atomically $ do
                when (not $ null $ spAckedBy sp) $ do
                    modifyTVar' cSentPackets $ (:) sp
                        { spTime = now
                        , spRetryCount = spRetryCount sp + 1
                        }
                writeFlow gDataFlow (cAddress, spData sp)

    let sendNextPacket :: Connection addr -> STM (IO ())
        sendNextPacket conn@Connection {..} = do
            mbch <- readTVar cChannel >>= return . \case
                ChannelEstablished ch -> Just ch
                _                     -> Nothing

            let checkOutstanding
                    | isJust mbch = readTQueue cSecureOutQueue
                    | otherwise = retry

            (secure, packet@(TransportPacket header content), ackedBy) <-
                checkOutstanding <|> readFlow cDataInternal

            let plain = BL.concat $
                    (serializeObject $ transportToObject gStorage header)
                    : map lazyLoadBytes content

            when (isNothing mbch && secure) $ do
                writeTQueue cSecureOutQueue (secure, packet, ackedBy)

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
                    Just bs -> do
                        sendBytes conn $ SentPacket
                            { spTime = undefined
                            , spRetryCount = -1
                            , spAckedBy = ackedBy
                            , spData = bs
                            }
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
                return $ sendBytes conn sp

    let establishNewConnection = do
            _ <- getConnection gs =<< readFlow gConnectionFlow
            return $ return ()

    conns <- readTVar gConnections
    msum $ concat $
        [ map retransmitPacket conns
        , map sendNextPacket conns
        , [ establishNewConnection ]
        ]

processAcknowledgements :: GlobalState addr -> Connection addr -> [TransportHeaderItem] -> STM ()
processAcknowledgements GlobalState {} Connection {..} = mapM_ $ \hitem -> do
    modifyTVar' cSentPackets $ filter $ (hitem `notElem`) . spAckedBy

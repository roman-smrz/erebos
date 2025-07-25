{-# LANGUAGE OverloadedStrings #-}

module Test (
    runTestTool,
) where

import Control.Arrow
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State

import Crypto.Random

import Data.Bool
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Lazy.Char8 qualified as BL
import Data.Foldable
import Data.Ord
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding
import Data.Text.IO qualified as T
import Data.Typeable
import Data.UUID.Types qualified as U

import Network.Socket

import System.IO
import System.IO.Error

import Erebos.Attach
import Erebos.Chatroom
import Erebos.Contact
import Erebos.DirectMessage
import Erebos.Discovery
import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Pairing
import Erebos.PubKey
import Erebos.Service
import Erebos.Service.Stream
import Erebos.Set
import Erebos.State
import Erebos.Storable
import Erebos.Storage
import Erebos.Storage.Head
import Erebos.Storage.Merge
import Erebos.Sync

import Test.Service


data TestState = TestState
    { tsHead :: Maybe (Head LocalState)
    , tsServer :: Maybe RunningServer
    , tsWatchedHeads :: [ ( Int, WatchedHead ) ]
    , tsWatchedHeadNext :: Int
    , tsWatchedLocalIdentity :: Maybe WatchedHead
    , tsWatchedSharedIdentity :: Maybe WatchedHead
    }

data RunningServer = RunningServer
    { rsServer :: Server
    , rsPeers :: MVar ( Int, [ TestPeer ] )
    , rsPeerThread :: ThreadId
    }

data TestPeer = TestPeer
    { tpIndex :: Int
    , tpPeer :: Peer
    , tpStreamReaders :: MVar [ (Int, StreamReader ) ]
    , tpStreamWriters :: MVar [ (Int, StreamWriter ) ]
    }

initTestState :: TestState
initTestState = TestState
    { tsHead = Nothing
    , tsServer = Nothing
    , tsWatchedHeads = []
    , tsWatchedHeadNext = 1
    , tsWatchedLocalIdentity = Nothing
    , tsWatchedSharedIdentity = Nothing
    }

data TestInput = TestInput
    { tiOutput :: Output
    , tiStorage :: Storage
    , tiParams :: [Text]
    }


runTestTool :: Storage -> IO ()
runTestTool st = do
    out <- newMVar ()
    let testLoop = getLineMb >>= \case
            Just line -> do
                case T.words line of
                    (cname:params)
                        | Just (CommandM cmd) <- lookup cname commands -> do
                            runReaderT cmd $ TestInput out st params
                        | otherwise -> fail $ "Unknown command '" ++ T.unpack cname ++ "'"
                    [] -> return ()
                testLoop

            Nothing -> return ()

    runExceptT (evalStateT testLoop initTestState) >>= \case
        Left x -> B.hPutStr stderr $ (`BC.snoc` '\n') $ BC.pack (showErebosError x)
        Right () -> return ()

getLineMb :: MonadIO m => m (Maybe Text)
getLineMb = liftIO $ catchIOError (Just <$> T.getLine) (\e -> if isEOFError e then return Nothing else ioError e)

getLines :: MonadIO m => m [Text]
getLines = getLineMb >>= \case
    Just line | not (T.null line) -> (line:) <$> getLines
    _ -> return []

getHead :: CommandM (Head LocalState)
getHead = do
    h <- maybe (fail "failed to reload head") return =<< maybe (fail "no current head") reloadHead =<< gets tsHead
    modify $ \s -> s { tsHead = Just h }
    return h


type Output = MVar ()

outLine :: Output -> String -> IO ()
outLine mvar line = do
    evaluate $ foldl' (flip seq) () line
    withMVar mvar $ \() -> do
        B.putStr $ (`BC.snoc` '\n') $ BC.pack line
        hFlush stdout

cmdOut :: String -> Command
cmdOut line = do
    out <- asks tiOutput
    liftIO $ outLine out line


getPeer :: Text -> CommandM Peer
getPeer spidx = tpPeer <$> getTestPeer spidx

getTestPeer :: Text -> CommandM TestPeer
getTestPeer spidx = do
    Just RunningServer {..} <- gets tsServer
    Just peer <- find (((read $ T.unpack spidx) ==) . tpIndex) . snd <$> liftIO (readMVar rsPeers)
    return peer

getPeerIndex :: MVar ( Int, [ TestPeer ] ) -> ServiceHandler s Int
getPeerIndex pmvar = do
    peer <- asks svcPeer
    maybe 0 tpIndex . find ((peer ==) . tpPeer) . snd <$> liftIO (readMVar pmvar)

pairingAttributes :: PairingResult a => proxy (PairingService a) -> Output -> MVar ( Int, [ TestPeer ] ) -> String -> PairingAttributes a
pairingAttributes _ out peers prefix = PairingAttributes
    { pairingHookRequest = return ()

    , pairingHookResponse = \confirm -> do
        index <- show <$> getPeerIndex peers
        afterCommit $ outLine out $ unwords [prefix ++ "-response", index, confirm]

    , pairingHookRequestNonce = \confirm -> do
        index <- show <$> getPeerIndex peers
        afterCommit $ outLine out $ unwords [prefix ++ "-request", index, confirm]

    , pairingHookRequestNonceFailed = failed "nonce"

    , pairingHookConfirmedResponse = return ()
    , pairingHookConfirmedRequest = return ()

    , pairingHookAcceptedResponse = do
        index <- show <$> getPeerIndex peers
        afterCommit $ outLine out $ unwords [prefix ++ "-response-done", index]

    , pairingHookAcceptedRequest = do
        index <- show <$> getPeerIndex peers
        afterCommit $ outLine out $ unwords [prefix ++ "-request-done", index]

    , pairingHookFailed = \case
        PairingUserRejected -> failed "user"
        PairingUnexpectedMessage pstate packet -> failed $ "unexpected " ++ strState pstate ++ " " ++ strPacket packet
        PairingFailedOther err -> failed $ "other " ++ showErebosError err
    , pairingHookVerifyFailed = failed "verify"
    , pairingHookRejected = failed "rejected"
    }
    where
        failed :: PairingResult a => String -> ServiceHandler (PairingService a) ()
        failed detail = do
            ptype <- svcGet >>= return . \case
                OurRequest {} -> "response"
                OurRequestConfirm {} -> "response"
                OurRequestReady -> "response"
                PeerRequest {} -> "request"
                PeerRequestConfirm -> "request"
                _ -> fail "unexpected pairing state"

            index <- show <$> getPeerIndex peers
            afterCommit $ outLine out $ prefix ++ "-" ++ ptype ++ "-failed " ++ index ++ " " ++ detail

        strState :: PairingState a -> String
        strState = \case
            NoPairing -> "none"
            OurRequest {} -> "our-request"
            OurRequestConfirm {} -> "our-request-confirm"
            OurRequestReady -> "our-request-ready"
            PeerRequest {} -> "peer-request"
            PeerRequestConfirm -> "peer-request-confirm"
            PairingDone -> "done"

        strPacket :: PairingService a -> String
        strPacket = \case
            PairingRequest {} -> "request"
            PairingResponse {} -> "response"
            PairingRequestNonce {} -> "nonce"
            PairingAccept {} -> "accept"
            PairingReject -> "reject"

directMessageAttributes :: Output -> DirectMessageAttributes
directMessageAttributes out = DirectMessageAttributes
    { dmOwnerMismatch = afterCommit $ outLine out "dm-owner-mismatch"
    }

discoveryAttributes :: DiscoveryAttributes
discoveryAttributes = (defaultServiceAttributes Proxy)
    { discoveryProvideTunnel = const False
    }

dmReceivedWatcher :: Output -> Stored DirectMessage -> IO ()
dmReceivedWatcher out smsg = do
    let msg = fromStored smsg
    outLine out $ unwords
        [ "dm-received"
        , "from", maybe "<unnamed>" T.unpack $ idName $ msgFrom msg
        , "text", T.unpack $ msgText msg
        ]


newtype CommandM a = CommandM (ReaderT TestInput (StateT TestState (ExceptT ErebosError IO)) a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader TestInput, MonadState TestState, MonadError ErebosError)

instance MonadFail CommandM where
    fail = throwOtherError

instance MonadRandom CommandM where
    getRandomBytes = liftIO . getRandomBytes

instance MonadStorage CommandM where
    getStorage = asks tiStorage

instance MonadHead LocalState CommandM where
    updateLocalHead f = do
        Just h <- gets tsHead
        (Just h', x) <- maybe (fail "failed to reload head") (flip updateHead f) =<< reloadHead h
        modify $ \s -> s { tsHead = Just h' }
        return x

type Command = CommandM ()

commands :: [(Text, Command)]
commands = map (T.pack *** id)
    [ ("store", cmdStore)
    , ("load", cmdLoad)
    , ("stored-generation", cmdStoredGeneration)
    , ("stored-roots", cmdStoredRoots)
    , ("stored-set-add", cmdStoredSetAdd)
    , ("stored-set-list", cmdStoredSetList)
    , ("head-create", cmdHeadCreate)
    , ("head-replace", cmdHeadReplace)
    , ("head-watch", cmdHeadWatch)
    , ("head-unwatch", cmdHeadUnwatch)
    , ("create-identity", cmdCreateIdentity)
    , ("identity-info", cmdIdentityInfo)
    , ("start-server", cmdStartServer)
    , ("stop-server", cmdStopServer)
    , ("peer-add", cmdPeerAdd)
    , ("peer-drop", cmdPeerDrop)
    , ("peer-list", cmdPeerList)
    , ("test-message-send", cmdTestMessageSend)
    , ("test-stream-open", cmdTestStreamOpen)
    , ("test-stream-close", cmdTestStreamClose)
    , ("test-stream-send", cmdTestStreamSend)
    , ("local-state-get", cmdLocalStateGet)
    , ("local-state-replace", cmdLocalStateReplace)
    , ("local-state-wait", cmdLocalStateWait)
    , ("shared-state-get", cmdSharedStateGet)
    , ("shared-state-wait", cmdSharedStateWait)
    , ("watch-local-identity", cmdWatchLocalIdentity)
    , ("watch-shared-identity", cmdWatchSharedIdentity)
    , ("update-local-identity", cmdUpdateLocalIdentity)
    , ("update-shared-identity", cmdUpdateSharedIdentity)
    , ("attach-to", cmdAttachTo)
    , ("attach-accept", cmdAttachAccept)
    , ("attach-reject", cmdAttachReject)
    , ("contact-request", cmdContactRequest)
    , ("contact-accept", cmdContactAccept)
    , ("contact-reject", cmdContactReject)
    , ("contact-list", cmdContactList)
    , ("contact-set-name", cmdContactSetName)
    , ("dm-send-peer", cmdDmSendPeer)
    , ("dm-send-contact", cmdDmSendContact)
    , ("dm-send-identity", cmdDmSendIdentity)
    , ("dm-list-peer", cmdDmListPeer)
    , ("dm-list-contact", cmdDmListContact)
    , ("chatroom-create", cmdChatroomCreate)
    , ("chatroom-delete", cmdChatroomDelete)
    , ("chatroom-list-local", cmdChatroomListLocal)
    , ("chatroom-watch-local", cmdChatroomWatchLocal)
    , ("chatroom-set-name", cmdChatroomSetName)
    , ("chatroom-subscribe", cmdChatroomSubscribe)
    , ("chatroom-unsubscribe", cmdChatroomUnsubscribe)
    , ("chatroom-members", cmdChatroomMembers)
    , ("chatroom-join", cmdChatroomJoin)
    , ("chatroom-join-as", cmdChatroomJoinAs)
    , ("chatroom-leave", cmdChatroomLeave)
    , ("chatroom-message-send", cmdChatroomMessageSend)
    , ("discovery-connect", cmdDiscoveryConnect)
    , ("discovery-tunnel", cmdDiscoveryTunnel)
    ]

cmdStore :: Command
cmdStore = do
    st <- asks tiStorage
    pst <- liftIO $ derivePartialStorage st
    [otype] <- asks tiParams
    ls <- getLines

    let cnt = encodeUtf8 $ T.unlines ls
        full = BL.fromChunks
            [ encodeUtf8 otype
            , BC.singleton ' '
            , BC.pack (show $ B.length cnt)
            , BC.singleton '\n', cnt
            ]
    liftIO (copyRef st =<< storeRawBytes pst full) >>= \case
        Right ref -> cmdOut $ "store-done " ++ show (refDigest ref)
        Left _ -> cmdOut $ "store-failed"

cmdLoad :: Command
cmdLoad = do
    st <- asks tiStorage
    [ tref ] <- asks tiParams
    Just ref <- liftIO $ readRef st $ encodeUtf8 tref
    let obj = load @Object ref
    header : content <- return $ BL.lines $ serializeObject obj
    cmdOut $ "load-type " <> T.unpack (decodeUtf8 $ BL.toStrict header)
    forM_ content $ \line -> do
        cmdOut $ "load-line " <> T.unpack (decodeUtf8 $ BL.toStrict line)
    cmdOut "load-done"

cmdStoredGeneration :: Command
cmdStoredGeneration = do
    st <- asks tiStorage
    [tref] <- asks tiParams
    Just ref <- liftIO $ readRef st (encodeUtf8 tref)
    cmdOut $ "stored-generation " ++ T.unpack tref ++ " " ++ showGeneration (storedGeneration $ wrappedLoad @Object ref)

cmdStoredRoots :: Command
cmdStoredRoots = do
    st <- asks tiStorage
    [tref] <- asks tiParams
    Just ref <- liftIO $ readRef st (encodeUtf8 tref)
    cmdOut $ "stored-roots " ++ T.unpack tref ++ concatMap ((' ':) . show . refDigest . storedRef) (storedRoots $ wrappedLoad @Object ref)

cmdStoredSetAdd :: Command
cmdStoredSetAdd = do
    st <- asks tiStorage
    (item, set) <- asks tiParams >>= liftIO . mapM (readRef st . encodeUtf8) >>= \case
        [Just iref, Just sref] -> return (wrappedLoad iref, loadSet @[Stored Object] sref)
        [Just iref] -> return (wrappedLoad iref, emptySet)
        _ -> fail "unexpected parameters"
    set' <- storeSetAdd st [item] set
    cmdOut $ "stored-set-add" ++ concatMap ((' ':) . show . refDigest . storedRef) (toComponents set')

cmdStoredSetList :: Command
cmdStoredSetList = do
    st <- asks tiStorage
    [tref] <- asks tiParams
    Just ref <- liftIO $ readRef st (encodeUtf8 tref)
    let items = fromSetBy compare $ loadSet @[Stored Object] ref
    forM_ items $ \item -> do
        cmdOut $ "stored-set-item" ++ concatMap ((' ':) . show . refDigest . storedRef) item
    cmdOut $ "stored-set-done"

cmdHeadCreate :: Command
cmdHeadCreate = do
    [ ttid, tref ] <- asks tiParams
    st <- asks tiStorage
    Just tid <- return $ fromUUID <$> U.fromText ttid
    Just ref <- liftIO $ readRef st (encodeUtf8 tref)

    h <- storeHeadRaw st tid ref
    cmdOut $ unwords $ [ "head-create-done", show (toUUID tid), show (toUUID h) ]

cmdHeadReplace :: Command
cmdHeadReplace = do
    [ ttid, thid, told, tnew ] <- asks tiParams
    st <- asks tiStorage
    Just tid <- return $ fmap fromUUID $ U.fromText ttid
    Just hid <- return $ fmap fromUUID $ U.fromText thid
    Just old <- liftIO $ readRef st (encodeUtf8 told)
    Just new <- liftIO $ readRef st (encodeUtf8 tnew)

    replaceHeadRaw st tid hid old new >>= cmdOut . unwords . \case
        Left Nothing  -> [ "head-replace-fail", T.unpack ttid, T.unpack thid, T.unpack told, T.unpack tnew ]
        Left (Just r) -> [ "head-replace-fail", T.unpack ttid, T.unpack thid, T.unpack told, T.unpack tnew, show (refDigest r) ]
        Right _       -> [ "head-replace-done", T.unpack ttid, T.unpack thid, T.unpack told, T.unpack tnew ]

cmdHeadWatch :: Command
cmdHeadWatch = do
    [ ttid, thid ] <- asks tiParams
    st <- asks tiStorage
    Just tid <- return $ fmap fromUUID $ U.fromText ttid
    Just hid <- return $ fmap fromUUID $ U.fromText thid

    out <- asks tiOutput
    wid <- gets tsWatchedHeadNext

    watched <- liftIO $ watchHeadRaw st tid hid id $ \r -> do
        outLine out $ unwords [ "head-watch-cb", show wid, show $ refDigest r ]

    modify $ \s -> s
        { tsWatchedHeads = ( wid, watched ) : tsWatchedHeads s
        , tsWatchedHeadNext = wid + 1
        }

    cmdOut $ unwords $ [ "head-watch-done", T.unpack ttid, T.unpack thid, show wid ]

cmdHeadUnwatch :: Command
cmdHeadUnwatch = do
    [ twid ] <- asks tiParams
    let wid = read (T.unpack twid)
    Just watched <- lookup wid <$> gets tsWatchedHeads
    liftIO $ unwatchHead watched
    cmdOut $ unwords [ "head-unwatch-done", show wid ]

initTestHead :: Head LocalState -> Command
initTestHead h = do
    _ <- liftIO . watchReceivedMessages h . dmReceivedWatcher =<< asks tiOutput
    modify $ \s -> s { tsHead = Just h }

loadTestHead :: CommandM (Head LocalState)
loadTestHead = do
    st <- asks tiStorage
    h <- loadHeads st >>= \case
        h : _ -> return h
        [] -> fail "no local head found"
    initTestHead h
    return h

getOrLoadHead :: CommandM (Head LocalState)
getOrLoadHead = do
    gets tsHead >>= \case
        Just h -> return h
        Nothing -> loadTestHead

cmdCreateIdentity :: Command
cmdCreateIdentity = do
    st <- asks tiStorage
    names <- asks tiParams

    h <- liftIO $ do
        Just identity <- if null names
            then Just <$> createIdentity st Nothing Nothing
            else foldrM (\n o -> Just <$> createIdentity st (Just n) o) Nothing names

        shared <- case names of
            _:_:_ -> (:[]) <$> makeSharedStateUpdate st (Just $ finalOwner identity) []
            _ -> return []

        storeHead st $ LocalState
            { lsPrev = Nothing
            , lsIdentity = idExtData identity
            , lsShared = shared
            , lsOther = []
            }
    initTestHead h
    cmdOut $ unwords [ "create-identity-done", "ref", show $ refDigest $ storedRef $ lsIdentity $ headObject h ]

cmdIdentityInfo :: Command
cmdIdentityInfo = do
    st <- asks tiStorage
    [ tref ] <- asks tiParams
    Just ref <- liftIO $ readRef st $ encodeUtf8 tref
    let sidata = wrappedLoad ref
        idata = fromSigned sidata
    cmdOut $ unwords $ concat
        [ [ "identity-info" ]
        , [ "ref", T.unpack tref ]
        , [ "base", show $ refDigest $ storedRef $ eiddStoredBase sidata ]
        , maybe [] (\owner -> [ "owner", show $ refDigest $ storedRef owner ]) $ eiddOwner idata
        , maybe [] (\name -> [ "name", T.unpack name ]) $ eiddName idata
        ]

cmdStartServer :: Command
cmdStartServer = do
    out <- asks tiOutput

    let parseParams = \case
            (name : value : rest)
                | name == "services" -> second ( map splitServiceParams (T.splitOn "," value) ++ ) (parseParams rest)
            (name : rest)
                | name == "test-log" -> first (\o -> o { serverTestLog = True }) (parseParams rest)
                | otherwise -> parseParams rest
            _ -> ( defaultServerOptions { serverErrorPrefix = "server-error-message " }, [] )

        splitServiceParams svc =
            case T.splitOn ":" svc of
                name : params -> ( name, params )
                _ -> ( svc, [] )

    ( serverOptions, serviceNames ) <- parseParams <$> asks tiParams

    h <- getOrLoadHead
    rsPeers <- liftIO $ newMVar (1, [])
    services <- forM serviceNames $ \case
        ( "attach", _ ) -> return $ someServiceAttr $ pairingAttributes (Proxy @AttachService) out rsPeers "attach"
        ( "chatroom", _ ) -> return $ someService @ChatroomService Proxy
        ( "contact", _ ) -> return $ someServiceAttr $ pairingAttributes (Proxy @ContactService) out rsPeers "contact"
        ( "discovery", params ) -> return $ someServiceAttr $ discoveryAttributes
            { discoveryProvideTunnel = const $ "tunnel" `elem` params
            }
        ( "dm", _ ) -> return $ someServiceAttr $ directMessageAttributes out
        ( "sync", _ ) -> return $ someService @SyncService Proxy
        ( "test", _ ) -> return $ someServiceAttr $ (defaultServiceAttributes Proxy)
            { testMessageReceived = \obj otype len sref -> do
                liftIO $ do
                    void $ store (headStorage h) obj
                    outLine out $ unwords [ "test-message-received", otype, len, sref ]
            , testStreamsReceived = \streams -> do
                pidx <- getPeerIndex rsPeers
                liftIO $ do
                    nums <- mapM getStreamReaderNumber streams
                    outLine out $ unwords $ "test-stream-open-from" : show pidx : map show nums
                    forM_ (zip nums streams) $ \( num, stream ) -> void $ forkIO $ do
                        let go = readStreamPacket stream >>= \case
                                StreamData seqNum bytes -> do
                                    outLine out $ unwords [ "test-stream-received", show pidx, show num, show seqNum, BC.unpack bytes ]
                                    go
                                StreamClosed seqNum -> do
                                    outLine out $ unwords [ "test-stream-closed-from", show pidx, show num, show seqNum ]
                        go
            }
        ( sname, _ ) -> throwOtherError $ "unknown service `" <> T.unpack sname <> "'"

    let logPrint str = do BC.hPutStrLn stdout (BC.pack str)
                          hFlush stdout
    rsServer <- liftIO $ startServer serverOptions h logPrint services

    rsPeerThread <- liftIO $ forkIO $ void $ forever $ do
        peer <- getNextPeerChange rsServer

        let printPeer TestPeer {..} = do
                params <- peerIdentity tpPeer >>= return . \case
                    PeerIdentityFull pid -> ("id":) $ map (maybe "<unnamed>" T.unpack . idName) (unfoldOwners pid)
                    _ -> [ "addr", show (peerAddress tpPeer) ]
                outLine out $ unwords $ [ "peer", show tpIndex ] ++ params

            update ( tpIndex, [] ) = do
                tpPeer <- return peer
                tpStreamReaders <- newMVar []
                tpStreamWriters <- newMVar []
                let tp = TestPeer {..}
                printPeer tp
                return ( tpIndex + 1, [ tp ] )

            update cur@( nid, p : ps )
                | tpPeer p == peer = printPeer p >> return cur
                | otherwise = fmap (p :) <$> update ( nid, ps )

        modifyMVar_ rsPeers update

    modify $ \s -> s { tsServer = Just RunningServer {..} }

cmdStopServer :: Command
cmdStopServer = do
    Just RunningServer {..} <- gets tsServer
    liftIO $ do
        killThread rsPeerThread
        stopServer rsServer
    modify $ \s -> s { tsServer = Nothing }
    cmdOut "stop-server-done"

cmdPeerAdd :: Command
cmdPeerAdd = do
    Just RunningServer {..} <- gets tsServer
    host:rest <- map T.unpack <$> asks tiParams

    let port = case rest of [] -> show discoveryPort
                            (p:_) -> p
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just host) (Just port)
    void $ liftIO $ serverPeer rsServer (addrAddress addr)

cmdPeerDrop :: Command
cmdPeerDrop = do
    [spidx] <- asks tiParams
    peer <- getPeer spidx
    liftIO $ dropPeer peer

cmdPeerList :: Command
cmdPeerList = do
    Just RunningServer {..} <- gets tsServer
    peers <- liftIO $ getCurrentPeerList rsServer
    tpeers <- liftIO $ readMVar rsPeers
    forM_ peers $ \peer -> do
        Just tp <- return $ find ((peer ==) . tpPeer) . snd $ tpeers
        mbpid <- peerIdentity peer
        cmdOut $ unwords $ concat
            [ [ "peer-list-item", show (tpIndex tp) ]
            , [ "addr", show (peerAddress peer) ]
            , case mbpid of PeerIdentityFull pid -> ("id":) $ map (maybe "<unnamed>" T.unpack . idName) (unfoldOwners pid)
                            _ -> []
            ]
    cmdOut "peer-list-done"


cmdTestMessageSend :: Command
cmdTestMessageSend = do
    spidx : trefs <- asks tiParams
    st <- asks tiStorage
    Just refs <- liftIO $ fmap sequence $ mapM (readRef st . encodeUtf8) trefs
    peer <- getPeer spidx
    sendManyToPeer peer $ map (TestMessage . wrappedLoad) refs
    cmdOut "test-message-send done"

cmdTestStreamOpen :: Command
cmdTestStreamOpen = do
    spidx : rest <- asks tiParams
    tp <- getTestPeer spidx
    count <- case rest of
        [] -> return 1
        tcount : _ -> return $ read $ T.unpack tcount

    out <- asks tiOutput
    runPeerService (tpPeer tp) $ do
        streams <- openTestStreams count
        afterCommit $ do
            nums <- mapM getStreamWriterNumber streams
            modifyMVar_ (tpStreamWriters tp) $ return . (++ zip nums streams)
            outLine out $ unwords $ "test-stream-open-done"
                : T.unpack spidx
                : map show nums

cmdTestStreamClose :: Command
cmdTestStreamClose = do
    [ spidx, sid ] <- asks tiParams
    tp <- getTestPeer spidx
    Just stream <- lookup (read $ T.unpack sid) <$> liftIO (readMVar (tpStreamWriters tp))
    liftIO $ closeStream stream
    cmdOut $ unwords [ "test-stream-close-done", T.unpack spidx, T.unpack sid ]

cmdTestStreamSend :: Command
cmdTestStreamSend = do
    [ spidx, sid, content ] <- asks tiParams
    tp <- getTestPeer spidx
    Just stream <- lookup (read $ T.unpack sid) <$> liftIO (readMVar (tpStreamWriters tp))
    liftIO $ writeStream stream $ encodeUtf8 content
    cmdOut $ unwords [ "test-stream-send-done", T.unpack spidx, T.unpack sid ]

cmdLocalStateGet :: Command
cmdLocalStateGet = do
    h <- getHead
    cmdOut $ unwords $ "local-state-get" : map (show . refDigest . storedRef) [ headStoredObject h ]

cmdLocalStateReplace :: Command
cmdLocalStateReplace = do
    st <- asks tiStorage
    [ told, tnew ] <- asks tiParams
    Just rold <- liftIO $ readRef st $ encodeUtf8 told
    Just rnew <- liftIO $ readRef st $ encodeUtf8 tnew
    ok <- updateLocalHead @LocalState $ \ls -> do
        if storedRef ls == rold
          then return ( wrappedLoad rnew, True )
          else return ( ls, False )
    cmdOut $ if ok then "local-state-replace-done" else "local-state-replace-failed"

localStateWaitHelper :: Storable a => String -> (Head LocalState -> [ Stored a ]) -> Command
localStateWaitHelper label sel = do
    st <- asks tiStorage
    out <- asks tiOutput
    h <- getOrLoadHead
    trefs <- asks tiParams

    liftIO $ do
        mvar <- newEmptyMVar
        w <- watchHeadWith h sel $ \cur -> do
            mbobjs <- mapM (readRef st . encodeUtf8) trefs
            case map wrappedLoad <$> sequence mbobjs of
                Just objs | filterAncestors (cur ++ objs) == cur -> do
                    outLine out $ unwords $ label : map T.unpack trefs
                    void $ forkIO $ unwatchHead =<< takeMVar mvar
                _ -> return ()
        putMVar mvar w

cmdLocalStateWait :: Command
cmdLocalStateWait = localStateWaitHelper "local-state-wait" ((: []) . headStoredObject)

cmdSharedStateGet :: Command
cmdSharedStateGet = do
    h <- getHead
    cmdOut $ unwords $ "shared-state-get" : map (show . refDigest . storedRef) (lsShared $ headObject h)

cmdSharedStateWait :: Command
cmdSharedStateWait = localStateWaitHelper "shared-state-wait" (lsShared . headObject)

cmdWatchLocalIdentity :: Command
cmdWatchLocalIdentity = do
    h <- getOrLoadHead
    Nothing <- gets tsWatchedLocalIdentity

    out <- asks tiOutput
    w <- liftIO $ watchHeadWith h headLocalIdentity $ \idt -> do
        outLine out $ unwords $ "local-identity" : map (maybe "<unnamed>" T.unpack . idName) (unfoldOwners idt)
    modify $ \s -> s { tsWatchedLocalIdentity = Just w }

cmdWatchSharedIdentity :: Command
cmdWatchSharedIdentity = do
    h <- getOrLoadHead
    Nothing <- gets tsWatchedSharedIdentity

    out <- asks tiOutput
    w <- liftIO $ watchHeadWith h (lookupSharedValue . lsShared . headObject) $ \case
        Just (idt :: ComposedIdentity) -> do
            outLine out $ unwords $ "shared-identity" : map (maybe "<unnamed>" T.unpack . idName) (unfoldOwners idt)
        Nothing -> do
            outLine out $ "shared-identity-failed"
    modify $ \s -> s { tsWatchedSharedIdentity = Just w }

cmdUpdateLocalIdentity :: Command
cmdUpdateLocalIdentity = do
    [name] <- asks tiParams
    updateLocalState_ $ \ls -> do
        Just identity <- return $ validateExtendedIdentity $ lsIdentity $ fromStored ls
        let public = idKeyIdentity identity

        secret <- loadKey public
        nidata <- maybe (error "created invalid identity") (return . idExtData) . validateExtendedIdentity =<<
            mstore =<< sign secret =<< mstore . ExtendedIdentityData =<< return (emptyIdentityExtension $ idData identity)
            { idePrev = toList $ idExtDataF identity
            , ideName = Just name
            }
        mstore (fromStored ls) { lsIdentity = nidata }

cmdUpdateSharedIdentity :: Command
cmdUpdateSharedIdentity = do
    [name] <- asks tiParams
    updateLocalState_ $ updateSharedState_ $ \case
        Nothing -> throwOtherError "no existing shared identity"
        Just identity -> do
            let public = idKeyIdentity identity
            secret <- loadKey public
            uidentity <- mergeIdentity identity
            maybe (error "created invalid identity") (return . Just . toComposedIdentity) . validateExtendedIdentity =<<
                mstore =<< sign secret =<< mstore . ExtendedIdentityData =<< return (emptyIdentityExtension $ idData uidentity)
                { idePrev = toList $ idExtDataF identity
                , ideName = Just name
                }

cmdAttachTo :: Command
cmdAttachTo = do
    [spidx] <- asks tiParams
    attachToOwner =<< getPeer spidx

cmdAttachAccept :: Command
cmdAttachAccept = do
    [spidx] <- asks tiParams
    attachAccept =<< getPeer spidx

cmdAttachReject :: Command
cmdAttachReject = do
    [spidx] <- asks tiParams
    attachReject =<< getPeer spidx

cmdContactRequest :: Command
cmdContactRequest = do
    [spidx] <- asks tiParams
    contactRequest =<< getPeer spidx

cmdContactAccept :: Command
cmdContactAccept = do
    [spidx] <- asks tiParams
    contactAccept =<< getPeer spidx

cmdContactReject :: Command
cmdContactReject = do
    [spidx] <- asks tiParams
    contactReject =<< getPeer spidx

cmdContactList :: Command
cmdContactList = do
    h <- getHead
    let contacts = fromSetBy (comparing contactName) . lookupSharedValue . lsShared . headObject $ h
    forM_ contacts $ \c -> do
        r:_ <- return $ filterAncestors $ concatMap storedRoots $ toComponents c
        cmdOut $ concat
            [ "contact-list-item "
            , show $ refDigest $ storedRef r
            , " "
            , T.unpack $ contactName c
            , case contactIdentity c of Nothing -> ""; Just idt -> " " ++ T.unpack (displayIdentity idt)
            ]
    cmdOut "contact-list-done"

getContact :: Text -> CommandM Contact
getContact cid = do
    h <- getHead
    let contacts = fromSetBy (comparing contactName) . lookupSharedValue . lsShared . headObject $ h
    [contact] <- flip filterM contacts $ \c -> do
        r:_ <- return $ filterAncestors $ concatMap storedRoots $ toComponents c
        return $ T.pack (show $ refDigest $ storedRef r) == cid
    return contact

cmdContactSetName :: Command
cmdContactSetName = do
    [cid, name] <- asks tiParams
    contact <- getContact cid
    updateLocalState_ $ updateSharedState_ $ contactSetName contact name
    cmdOut "contact-set-name-done"

cmdDmSendPeer :: Command
cmdDmSendPeer = do
    [spidx, msg] <- asks tiParams
    PeerIdentityFull to <- peerIdentity =<< getPeer spidx
    void $ sendDirectMessage to msg

cmdDmSendContact :: Command
cmdDmSendContact = do
    [cid, msg] <- asks tiParams
    Just to <- contactIdentity <$> getContact cid
    void $ sendDirectMessage to msg

cmdDmSendIdentity :: Command
cmdDmSendIdentity = do
    st <- asks tiStorage
    [ tid, msg ] <- asks tiParams
    Just ref <- liftIO $ readRef st $ encodeUtf8 tid
    Just to <- return $ validateExtendedIdentity $ wrappedLoad ref
    void $ sendDirectMessage to msg

dmList :: Foldable f => Identity f -> Command
dmList peer = do
    threads <- toThreadList . lookupSharedValue . lsShared . headObject <$> getHead
    case find (sameIdentity peer . msgPeer) threads of
        Just thread -> do
            forM_ (reverse $ threadToList thread) $ \DirectMessage {..} -> cmdOut $ "dm-list-item"
                <> " from " <> (maybe "<unnamed>" T.unpack $ idName msgFrom)
                <> " text " <> (T.unpack msgText)
        Nothing -> return ()
    cmdOut "dm-list-done"

cmdDmListPeer :: Command
cmdDmListPeer = do
    [spidx] <- asks tiParams
    PeerIdentityFull to <- peerIdentity =<< getPeer spidx
    dmList to

cmdDmListContact :: Command
cmdDmListContact = do
    [cid] <- asks tiParams
    Just to <- contactIdentity <$> getContact cid
    dmList to

cmdChatroomCreate :: Command
cmdChatroomCreate = do
    [name] <- asks tiParams
    room <- createChatroom (Just name) Nothing
    cmdOut $ unwords $ "chatroom-create-done" : chatroomInfo room

cmdChatroomDelete :: Command
cmdChatroomDelete = do
    [ cid ] <- asks tiParams
    sdata <- getChatroomStateData cid
    deleteChatroomByStateData sdata
    cmdOut $ unwords [ "chatroom-delete-done", T.unpack cid ]

getChatroomStateData :: Text -> CommandM (Stored ChatroomStateData)
getChatroomStateData tref = do
    st <- asks tiStorage
    Just ref <- liftIO $ readRef st (encodeUtf8 tref)
    return $ wrappedLoad ref

cmdChatroomSetName :: Command
cmdChatroomSetName = do
    [cid, name] <- asks tiParams
    sdata <- getChatroomStateData cid
    updateChatroomByStateData sdata (Just name) Nothing >>= \case
        Just room -> cmdOut $ unwords $ "chatroom-set-name-done" : chatroomInfo room
        Nothing -> cmdOut "chatroom-set-name-failed"

cmdChatroomListLocal :: Command
cmdChatroomListLocal = do
    [] <- asks tiParams
    rooms <- listChatrooms
    forM_ rooms $ \room -> do
        cmdOut $ unwords $ "chatroom-list-item" : chatroomInfo room
    cmdOut "chatroom-list-done"

cmdChatroomWatchLocal :: Command
cmdChatroomWatchLocal = do
    [] <- asks tiParams
    h <- getOrLoadHead
    out <- asks tiOutput
    void $ watchChatrooms h $ \_ -> \case
        Nothing -> return ()
        Just diff -> forM_ diff $ \case
            AddedChatroom room -> outLine out $ unwords $ "chatroom-watched-added" : chatroomInfo room
            RemovedChatroom room -> outLine out $ unwords $ "chatroom-watched-removed" : chatroomInfo room
            UpdatedChatroom oldroom room -> do
                when (any ((\rsd -> not (null (rsdRoom rsd)) || not (null (rsdSubscribe rsd))) . fromStored) (roomStateData room)) $ do
                    outLine out $ unwords $ concat
                        [ [ "chatroom-watched-updated" ], chatroomInfo room
                        , [ "old" ], map (show . refDigest . storedRef) (roomStateData oldroom)
                        , [ "new" ], map (show . refDigest . storedRef) (roomStateData room)
                        ]
                when (any (not . null . rsdMessages . fromStored) (roomStateData room)) $ do
                    forM_ (reverse $ getMessagesSinceState room oldroom) $ \msg -> do
                        outLine out $ unwords $ concat
                            [ [ "chatroom-message-new" ]
                            , [ show . refDigest . storedRef . head . filterAncestors . concatMap storedRoots . toComponents $ room ]
                            , [ "room", maybe "<unnamed>" T.unpack $ roomName =<< cmsgRoom msg ]
                            , [ "from", maybe "<unnamed>" T.unpack $ idName $ cmsgFrom msg ]
                            , if cmsgLeave msg then [ "leave" ] else []
                            , maybe [] (("text":) . (:[]) . T.unpack) $ cmsgText msg
                            ]

chatroomInfo :: ChatroomState -> [String]
chatroomInfo room =
    [ show . refDigest . storedRef . head . filterAncestors . concatMap storedRoots . toComponents $ room
    , maybe "<unnamed>" T.unpack $ roomName =<< roomStateRoom room
    , "sub " <> bool "false" "true" (roomStateSubscribe room)
    ]

cmdChatroomSubscribe :: Command
cmdChatroomSubscribe = do
    [ cid ] <- asks tiParams
    to <- getChatroomStateData cid
    void $ chatroomSetSubscribe to True

cmdChatroomUnsubscribe :: Command
cmdChatroomUnsubscribe = do
    [ cid ] <- asks tiParams
    to <- getChatroomStateData cid
    void $ chatroomSetSubscribe to False

cmdChatroomMembers :: Command
cmdChatroomMembers = do
    [ cid ] <- asks tiParams
    Just chatroom <- findChatroomByStateData =<< getChatroomStateData cid
    forM_ (chatroomMembers chatroom) $ \user -> do
        cmdOut $ unwords [ "chatroom-members-item", maybe "<unnamed>" T.unpack $ idName user ]
    cmdOut "chatroom-members-done"

cmdChatroomJoin :: Command
cmdChatroomJoin = do
    [ cid ] <- asks tiParams
    joinChatroomByStateData =<< getChatroomStateData cid
    cmdOut "chatroom-join-done"

cmdChatroomJoinAs :: Command
cmdChatroomJoinAs = do
    [ cid, name ] <- asks tiParams
    st <- asks tiStorage
    identity <- liftIO $ createIdentity st (Just name) Nothing
    joinChatroomAsByStateData identity =<< getChatroomStateData cid
    cmdOut $ unwords [ "chatroom-join-as-done", T.unpack cid ]

cmdChatroomLeave :: Command
cmdChatroomLeave = do
    [ cid ] <- asks tiParams
    leaveChatroomByStateData =<< getChatroomStateData cid
    cmdOut "chatroom-leave-done"

cmdChatroomMessageSend :: Command
cmdChatroomMessageSend = do
    [cid, msg] <- asks tiParams
    to <- getChatroomStateData cid
    void $ sendChatroomMessageByStateData to msg

cmdDiscoveryConnect :: Command
cmdDiscoveryConnect = do
    [ tref ] <- asks tiParams
    Just dgst <- return $ readRefDigest $ encodeUtf8 tref
    Just RunningServer {..} <- gets tsServer
    discoverySearch rsServer dgst

cmdDiscoveryTunnel :: Command
cmdDiscoveryTunnel = do
    [ tvia, ttarget ] <- asks tiParams
    via <- getPeer tvia
    Just target <- return $ readRefDigest $ encodeUtf8 ttarget
    liftIO $ discoverySetupTunnel via target

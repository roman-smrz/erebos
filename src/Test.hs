module Test (
    runTestTool,
) where

import Control.Arrow
import Control.Concurrent
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State

import Crypto.Random

import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.Foldable
import Data.IP (fromSockAddr)
import Data.Ord
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding
import Data.Text.IO qualified as T
import Data.Typeable

import System.IO
import System.IO.Error

import Attach
import Contact
import Identity
import Network
import Pairing
import PubKey
import Service
import Set
import State
import Storage
import Storage.Internal (unsafeStoreRawBytes)
import Storage.Merge
import Sync


data TestState = TestState
    { tsHead :: Maybe (Head LocalState)
    , tsServer :: Maybe Server
    , tsPeers :: Maybe (MVar (Int, [(Int, Peer)]))
    , tsWatchedLocalIdentity :: Maybe WatchedHead
    , tsWatchedSharedIdentity :: Maybe WatchedHead
    }

initTestState :: TestState
initTestState = TestState
    { tsHead = Nothing
    , tsServer = Nothing
    , tsPeers = Nothing
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
        Left x -> hPutStrLn stderr x
        Right () -> return ()

getLineMb :: MonadIO m => m (Maybe Text)
getLineMb = liftIO $ catchIOError (Just <$> T.getLine) (\e -> if isEOFError e then return Nothing else ioError e)

getLines :: MonadIO m => m [Text]
getLines = getLineMb >>= \case
    Just line | not (T.null line) -> (line:) <$> getLines
    _ -> return []


type Output = MVar ()

outLine :: Output -> String -> IO ()
outLine mvar line = withMVar mvar $ \() -> do
    putStrLn line
    hFlush stdout

cmdOut :: String -> Command
cmdOut line = do
    out <- asks tiOutput
    liftIO $ outLine out line


getPeer :: Text -> CommandM Peer
getPeer spidx = do
    Just pmvar <- gets tsPeers
    Just peer <- lookup (read $ T.unpack spidx) . snd <$> liftIO (readMVar pmvar)
    return peer

getPeerIndex :: MVar (Int, [(Int, Peer)]) -> ServiceHandler (PairingService a) Int
getPeerIndex pmvar = do
    peer <- asks svcPeer
    maybe 0 fst . find ((==peer) . snd) . snd <$> liftIO (readMVar pmvar)

pairingAttributes :: PairingResult a => proxy (PairingService a) -> Output -> MVar (Int, [(Int, Peer)]) -> String -> PairingAttributes a
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
        PairingFailedOther str -> failed $ "other " ++ str
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


newtype CommandM a = CommandM (ReaderT TestInput (StateT TestState (ExceptT String IO)) a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader TestInput, MonadState TestState, MonadError String)

instance MonadFail CommandM where
    fail = throwError

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
    , ("stored-generation", cmdStoredGeneration)
    , ("stored-roots", cmdStoredRoots)
    , ("stored-set-add", cmdStoredSetAdd)
    , ("stored-set-list", cmdStoredSetList)
    , ("create-identity", cmdCreateIdentity)
    , ("start-server", cmdStartServer)
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
    ]

cmdStore :: Command
cmdStore = do
    st <- asks tiStorage
    [otype] <- asks tiParams
    ls <- getLines

    let cnt = encodeUtf8 $ T.unlines ls
    ref <- liftIO $ unsafeStoreRawBytes st $ BL.fromChunks [encodeUtf8 otype, BC.singleton ' ', BC.pack (show $ B.length cnt), BC.singleton '\n', cnt]
    cmdOut $ "store-done " ++ show (refDigest ref)

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
            { lsIdentity = idData identity
            , lsShared = shared
            }

    modify $ \s -> s { tsHead = Just h }

cmdStartServer :: Command
cmdStartServer = do
    out <- asks tiOutput

    Just h <- gets tsHead
    peers <- liftIO $ newMVar (1, [])
    server <- liftIO $ startServer defaultServerOptions h (hPutStrLn stderr)
        [ someServiceAttr $ pairingAttributes (Proxy @AttachService) out peers "attach"
        , someServiceAttr $ pairingAttributes (Proxy @ContactService) out peers "contact"
        , someService @SyncService Proxy
        ]

    void $ liftIO $ forkIO $ void $ forever $ do
        peer <- getNextPeerChange server

        let printPeer (idx, p) = do
                params <- peerIdentity p >>= return . \case
                    PeerIdentityFull pid -> ("id":) $ map (maybe "<unnamed>" T.unpack . idName) (unfoldOwners pid)
                    _ -> ("addr":) $ case peerAddress p of
                        DatagramAddress _ saddr
                            | Just (addr, port) <- fromSockAddr saddr -> [show addr, show port]
                            | otherwise -> []
                        PeerIceSession ice -> [show ice]
                outLine out $ unwords $ [ "peer", show idx ] ++ params

            update (nid, []) = printPeer (nid, peer) >> return (nid + 1, [(nid, peer)])
            update cur@(nid, p:ps) | snd p == peer = printPeer p >> return cur
                                   | otherwise = fmap (p:) <$> update (nid, ps)

        modifyMVar_ peers update

    modify $ \s -> s { tsServer = Just server, tsPeers = Just peers }

cmdSharedStateGet :: Command
cmdSharedStateGet = do
    h <- maybe (fail "failed to reload head") return =<< maybe (fail "no current head") reloadHead =<< gets tsHead
    cmdOut $ unwords $ "shared-state-get" : map (show . refDigest . storedRef) (lsShared $ headObject h)

cmdSharedStateWait :: Command
cmdSharedStateWait = do
    st <- asks tiStorage
    out <- asks tiOutput
    Just h <- gets tsHead
    trefs <- asks tiParams

    liftIO $ do
        mvar <- newEmptyMVar
        w <- watchHeadWith h (lsShared . headObject) $ \cur -> do
            mbobjs <- mapM (readRef st . encodeUtf8) trefs
            case map wrappedLoad <$> sequence mbobjs of
                Just objs | filterAncestors (cur ++ objs) == cur -> do
                    outLine out $ unwords $ "shared-state-wait" : map T.unpack trefs
                    void $ forkIO $ unwatchHead =<< takeMVar mvar
                _ -> return ()
        putMVar mvar w

cmdWatchLocalIdentity :: Command
cmdWatchLocalIdentity = do
    Just h <- gets tsHead
    Nothing <- gets tsWatchedLocalIdentity

    out <- asks tiOutput
    w <- liftIO $ watchHeadWith h headLocalIdentity $ \idt -> do
        outLine out $ unwords $ "local-identity" : map (maybe "<unnamed>" T.unpack . idName) (unfoldOwners idt)
    modify $ \s -> s { tsWatchedLocalIdentity = Just w }

cmdWatchSharedIdentity :: Command
cmdWatchSharedIdentity = do
    Just h <- gets tsHead
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
    updateLocalState_ $ \ls -> liftIO $ do
        let Just identity = validateIdentity $ lsIdentity $ fromStored ls
            st = storedStorage ls
            public = idKeyIdentity identity

        Just secret <- loadKey public
        nidata <- maybe (error "created invalid identity") (return . idData) . validateIdentity =<<
            wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
            { iddPrev = toList $ idDataF identity
            , iddName = Just name
            }
        wrappedStore st $ (fromStored ls) { lsIdentity = nidata }

cmdUpdateSharedIdentity :: Command
cmdUpdateSharedIdentity = do
    [name] <- asks tiParams
    updateSharedState_ $ \(Just identity) -> liftIO $ do
        let st = storedStorage $ head $ idDataF identity
            public = idKeyIdentity identity

        Just secret <- loadKey public
        maybe (error "created invalid identity") (return . Just . toComposedIdentity) . validateIdentity =<<
            wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
            { iddPrev = toList $ idDataF identity
            , iddName = Just name
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
    h <- maybe (fail "failed to reload head") return =<< maybe (fail "no current head") reloadHead =<< gets tsHead
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

cmdContactSetName :: Command
cmdContactSetName = do
    [cid, name] <- asks tiParams
    h <- maybe (fail "failed to reload head") return =<< maybe (fail "no current head") reloadHead =<< gets tsHead
    let contacts = fromSetBy (comparing contactName) . lookupSharedValue . lsShared . headObject $ h
    [contact] <- flip filterM contacts $ \c -> do
        r:_ <- return $ filterAncestors $ concatMap storedRoots $ toComponents c
        return $ T.pack (show $ refDigest $ storedRef r) == cid
    updateSharedState_ $ contactSetName contact name
    cmdOut "contact-set-name-done"

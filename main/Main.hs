{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Maybe
import Control.Monad.Writer

import Crypto.Random

import Data.Bifunctor
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.Char
import Data.List
import Data.Maybe
import Data.Ord
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.IO qualified as T
import Data.Time.Format
import Data.Time.LocalTime
import Data.Typeable

import Network.Socket

import System.Console.GetOpt
import System.Directory
import System.Environment
import System.Exit
import System.IO

import Erebos.Attach
import Erebos.Chatroom
import Erebos.Contact
import Erebos.Conversation
import Erebos.DirectMessage
import Erebos.Discovery
import Erebos.Identity
import Erebos.Invite
import Erebos.Network
import Erebos.Object
import Erebos.PubKey
import Erebos.Service
import Erebos.Set
import Erebos.State
import Erebos.Storable
import Erebos.Storage
import Erebos.Storage.Merge
import Erebos.Sync
import Erebos.TextFormat

import State
import Terminal
import Test
import Version
import WebSocket

data Options = Options
    { optServer :: ServerOptions
    , optServices :: [ServiceOption]
    , optStorage :: StorageOption
    , optCreateIdentity :: Maybe ( Maybe Text, [ Maybe Text ] )
    , optChatroomAutoSubscribe :: Maybe Int
    , optDmBotEcho :: Maybe Text
    , optWebSocketServer :: Maybe Int
    , optShowHelp :: Bool
    , optShowVersion :: Bool
    }

data StorageOption
    = DefaultStorage
    | FilesystemStorage FilePath
    | MemoryStorage

data ServiceOption = ServiceOption
    { soptName :: String
    , soptService :: SomeService
    , soptEnabled :: Bool
    , soptDescription :: String
    }

defaultOptions :: Options
defaultOptions = Options
    { optServer = defaultServerOptions
    , optServices = availableServices
    , optStorage = DefaultStorage
    , optCreateIdentity = Nothing
    , optChatroomAutoSubscribe = Nothing
    , optDmBotEcho = Nothing
    , optWebSocketServer = Nothing
    , optShowHelp = False
    , optShowVersion = False
    }

availableServices :: [ServiceOption]
availableServices =
    [ ServiceOption "attach" (someService @AttachService Proxy)
        True "attach (to) other devices"
    , ServiceOption "sync" (someService @SyncService Proxy)
        True "synchronization with attached devices"
    , ServiceOption "chatroom" (someService @ChatroomService Proxy)
        True "chatrooms with multiple participants"
    , ServiceOption "contact" (someService @ContactService Proxy)
        True "create contacts with network peers"
    , ServiceOption "dm" (someService @DirectMessage Proxy)
        True "direct messages"
    , ServiceOption "discovery" (someService @DiscoveryService Proxy)
        True "peer discovery"
    , ServiceOption "invite" (someService @InviteService Proxy)
        True "invites handling"
    ]

options :: [ OptDescr (Options -> Writer [ String ] Options) ]
options =
    [ Option [ 'p' ] [ "port" ]
        (ReqArg (\p -> so $ \opts -> opts { serverPort = read p }) "<port>")
        "local port to bind"
    , Option [ 's' ] [ "silent" ]
        (NoArg (so $ \opts -> opts { serverLocalDiscovery = False }))
        "do not send announce packets for local discovery"
    , Option [] [ "storage" ]
        (ReqArg (\path -> \opts -> return opts { optStorage = FilesystemStorage path }) "<path>")
        "use storage in <path>"
    , Option [] [ "memory-storage" ]
        (NoArg (\opts -> return opts { optStorage = MemoryStorage }))
        "use memory storage"
    , Option [] [ "create-identity" ]
        (OptArg (\value -> \opts -> return opts
            { optCreateIdentity =
                let devName = T.pack <$> value
                 in maybe (Just ( devName, [] )) (Just . first (const devName)) (optCreateIdentity opts)
            }) "<name>")
        "create a new (device) identity in a new local state"
    , Option [] [ "create-owner" ]
        (OptArg (\value -> \opts -> return opts
            { optCreateIdentity =
                let ownerName = T.pack <$> value
                 in maybe (Just ( Nothing, [ ownerName ] )) (Just . second (ownerName :)) (optCreateIdentity opts)
            }) "<name>")
        "create owner for a new device identity"
    , Option [] [ "chatroom-auto-subscribe" ]
        (ReqArg (\count -> \opts -> return opts { optChatroomAutoSubscribe = Just (read count) }) "<count>")
        "automatically subscribe for up to <count> chatrooms"
    , Option [] [ "discovery-stun-port" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> return attrs { discoveryStunPort = Just (read value) }) "<port>")
        "offer specified <port> to discovery peers for STUN protocol"
    , Option [] [ "discovery-stun-server" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> return attrs { discoveryStunServer = Just (read value) }) "<server>")
        "offer <server> (domain name or IP address) to discovery peers for STUN protocol"
    , Option [] [ "discovery-turn-port" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> return attrs { discoveryTurnPort = Just (read value) }) "<port>")
        "offer specified <port> to discovery peers for TURN protocol"
    , Option [] [ "discovery-turn-server" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> return attrs { discoveryTurnServer = Just (read value) }) "<server>")
        "offer <server> (domain name or IP address) to discovery peers for TURN protocol"
    , Option [] [ "discovery-tunnel" ]
        (OptArg (\value -> \opts -> do
            fun <- provideTunnelFun value
            serviceAttr (\attrs -> return attrs { discoveryProvideTunnel = fun }) opts) "<peer-type>")
        "offer to provide tunnel for peers of given <peer-type>, possible values: all, none, websocket"
    , Option [] [ "dm-bot-echo" ]
        (ReqArg (\prefix -> \opts -> return opts { optDmBotEcho = Just (T.pack prefix) }) "<prefix>")
        "automatically reply to direct messages with the same text prefixed with <prefix>"
    , Option [] [ "websocket-server" ]
        (ReqArg (\value -> \opts -> return opts { optWebSocketServer = Just (read value) }) "<port>")
        "start WebSocket server on given port"
    , Option [ 'h' ] [ "help" ]
        (NoArg $ \opts -> return opts { optShowHelp = True })
        "show this help and exit"
    , Option [ 'V' ] [ "version" ]
        (NoArg $ \opts -> return opts { optShowVersion = True })
        "show version and exit"
    ]
  where
    so f opts = return opts { optServer = f $ optServer opts }

    updateService :: (Service s, Monad m, Typeable m) => (ServiceAttributes s -> m (ServiceAttributes s)) -> SomeService -> m SomeService
    updateService f some@(SomeService proxy attrs)
        | Just f' <- cast f = SomeService proxy <$> f' attrs
        | otherwise = return some

    serviceAttr :: (Service s, Monad m, Typeable m) => (ServiceAttributes s -> m (ServiceAttributes s)) -> Options -> m Options
    serviceAttr f opts = do
        services' <- forM (optServices opts) $ \sopt -> do
            service <- updateService f (soptService sopt)
            return sopt { soptService = service }
        return opts { optServices = services' }

    provideTunnelFun :: Maybe String -> Writer [ String ] (Peer -> PeerAddress -> Bool)
    provideTunnelFun Nothing = return $ \_ _ -> True
    provideTunnelFun (Just "all") = return $ \_ _ -> True
    provideTunnelFun (Just "none") = return $ \_ _ -> False
    provideTunnelFun (Just "websocket") = return $ \_ -> \case
        CustomPeerAddress addr | Just WebSocketAddress {} <- cast addr -> True
        _ -> False
    provideTunnelFun (Just name) = do
        tell [ "Invalid value of --discovery-tunnel: ‘" <> name <> "’\n" ]
        return $ \_ _ -> False

servicesOptions :: [ OptDescr (Options -> Writer [ String ] Options) ]
servicesOptions = concatMap helper $ "all" : map soptName availableServices
  where
    helper name =
        [ Option [] [ "enable-" <> name ] (NoArg $ so $ change name $ \sopt -> sopt { soptEnabled = True }) ""
        , Option [] [ "disable-" <> name ] (NoArg $ so $ change name $ \sopt -> sopt { soptEnabled = False }) ""
        ]
    so f opts = return opts { optServices = f $ optServices opts }
    change :: String -> (ServiceOption -> ServiceOption) -> [ServiceOption] -> [ServiceOption]
    change name f (s : ss)
        | soptName s == name || name == "all"
                    = f s : change name f ss
        | otherwise =   s : change name f ss
    change _ _ []   = []

getDefaultStorageDir :: IO FilePath
getDefaultStorageDir = do
    lookupEnv "EREBOS_DIR" >>= \case
        Just dir -> return dir
        Nothing -> doesFileExist "./.erebos/erebos-storage" >>= \case
            True -> return "./.erebos"
            False -> getXdgDirectory XdgData "erebos"

main :: IO ()
main = do
    let printErrors errs = do
            progName <- getProgName
            hPutStrLn stderr $ concat errs <> "Try `" <> progName <> " --help' for more information."
            exitFailure
    (opts, args) <- (getOpt RequireOrder (options ++ servicesOptions) <$> getArgs) >>= \case
        (wo, args, []) ->
            case runWriter (foldM (flip ($)) defaultOptions wo) of
                ( o, [] )   -> return ( o, args )
                ( _, errs ) -> printErrors errs
        (_, _, errs) -> printErrors errs

    st <- liftIO $ case optStorage opts of
        DefaultStorage         -> openStorage =<< getDefaultStorageDir
        FilesystemStorage path -> openStorage path
        MemoryStorage          -> memoryStorage

    case args of
        [ "cat-file", sref ] -> do
            readRef st (BC.pack sref) >>= \case
                Nothing -> error "ref does not exist"
                Just ref -> BL.putStr $ lazyLoadBytes ref

        ("cat-file" : objtype : srefs@(_:_)) -> do
            sequence <$> (mapM (readRef st . BC.pack) srefs) >>= \case
                Nothing -> error "ref does not exist"
                Just refs -> case objtype of
                    "signed" -> forM_ refs $ \ref -> do
                        let signed = load ref :: Signed Object
                        BL.putStr $ lazyLoadBytes $ storedRef $ signedData signed
                        forM_ (signedSignature signed) $ \sig -> do
                            putStr $ "SIG "
                            BC.putStrLn $ showRef $ storedRef $ sigKey $ fromStored sig
                    "identity" -> case validateExtendedIdentityF (wrappedLoad <$> refs) of
                        Just identity -> do
                            let disp :: Identity m -> IO ()
                                disp idt = do
                                    maybe (return ()) (T.putStrLn . (T.pack "Name: " `T.append`)) $ idName idt
                                    BC.putStrLn . (BC.pack "KeyId: " `BC.append`) . showRefDigest . refDigest . storedRef $ idKeyIdentity idt
                                    BC.putStrLn . (BC.pack "KeyMsg: " `BC.append`) . showRefDigest . refDigest . storedRef $ idKeyMessage idt
                                    case idOwner idt of
                                         Nothing -> return ()
                                         Just owner -> do
                                             mapM_ (putStrLn . ("OWNER " ++) . BC.unpack . showRefDigest . refDigest . storedRef) $ idExtDataF owner
                                             disp owner
                            disp identity
                        Nothing -> putStrLn $ "Identity verification failed"
                    _ -> error $ "unknown object type '" ++ objtype ++ "'"

        [ "show-generation", sref ] -> readRef st (BC.pack sref) >>= \case
            Nothing -> error "ref does not exist"
            Just ref -> print $ storedGeneration (wrappedLoad ref :: Stored Object)

        [ "identity" ] -> do
            loadHeads st >>= \case
                (h : _) -> do
                    T.putStr $ showIdentityDetails $ headLocalIdentity h
                [] -> do
                    T.putStrLn "no local state head"
                    exitFailure

        [ "update-identity" ] -> do
            withTerminal noCompletion $ \term -> do
                either (fail . showErebosError) return <=< runExceptT $ do
                    runReaderT (updateSharedIdentity term) =<< runReaderT (loadLocalStateHead term) st

        ("update-identity" : srefs) -> do
            withTerminal noCompletion $ \term -> do
                sequence <$> mapM (readRef st . BC.pack) srefs >>= \case
                    Nothing -> error "ref does not exist"
                    Just refs
                        | Just idt <- validateIdentityF $ map wrappedLoad refs -> do
                            BC.putStrLn . showRefDigest . refDigest . storedRef . idData =<<
                                (either (fail . showErebosError) return <=< runExceptT $ runReaderT (interactiveIdentityUpdate term idt) st)
                        | otherwise -> error "invalid identity"

        [ "test" ] -> runTestTool st

        [] -> do
            let header = "Usage: erebos [OPTION...]"
                serviceDesc ServiceOption {..} = padService ("  " <> soptName) <> soptDescription

                padTo n str = str <> replicate (n - length str) ' '
                padOpt = padTo 37
                padService = padTo 16

            if | optShowHelp opts -> putStr $ usageInfo header options <> unlines
                  (
                    [ padOpt "  --enable-<service>"  <> "enable network service <service>"
                    , padOpt "  --disable-<service>" <> "disable network service <service>"
                    , padOpt "  --enable-all"        <> "enable all network services"
                    , padOpt "  --disable-all"       <> "disable all network services"
                    , ""
                    , "Available network services:"
                    ] ++ map serviceDesc availableServices
                  )
               | optShowVersion opts -> putStrLn versionLine
               | otherwise -> interactiveLoop st opts

        (cmdname : _) -> do
            hPutStrLn stderr $ "Unknown command ‘" <> cmdname <> "’"
            exitFailure


interactiveLoop :: Storage -> Options -> IO ()
interactiveLoop st opts = withTerminal commandCompletion $ \term -> do
    showPrompt term
    erebosHead <- either (fail . showErebosError) return <=< runExceptT . flip runReaderT st $ do
        case optCreateIdentity opts of
            Nothing -> loadLocalStateHead term
            Just ( devName, names ) -> createLocalStateHead (names ++ [ devName ])
    void $ printLine term $ plainText $ displayIdentity $ headLocalIdentity erebosHead

    let tui = hasTerminalUI term
    let extPrintLn = void . printLine term

    let getInputLinesTui :: Either CommandState String -> MaybeT IO String
        getInputLinesTui eprompt = do
            prompt <- case eprompt of
                Left cstate -> do
                    pname <- case csContext cstate of
                        NoContext -> return ""
                        SelectedPeer peer -> getPeerIdentity peer >>= return . \case
                            PeerIdentityFull pid -> maybe "<unnamed>" T.unpack $ idName $ finalOwner pid
                            PeerIdentityRef wref _ -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                            PeerIdentityUnknown _  -> "<unknown>"
                        SelectedContact contact -> return $ T.unpack $ contactName contact
                        SelectedChatroom rstate -> return $ T.unpack $ fromMaybe (T.pack "<unnamed>") $ roomName =<< roomStateRoom rstate
                        SelectedConversation conv -> return $ T.unpack $ conversationName conv
                    return $ pname ++ "> "
                Right prompt -> return prompt
            lift $ setPrompt term $ plainText $ T.pack prompt
            join $ lift $ getInputLine term $ \case
                Just input@('/' : _) -> KeepPrompt $ return input
                Just input -> ErasePrompt $ case reverse input of
                    _ | all isSpace input -> getInputLinesTui eprompt
                    '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLinesTui (Right ">> ")
                    _         -> return input
                Nothing
                    | tui       -> KeepPrompt mzero
                    | otherwise -> KeepPrompt $ liftIO $ forever $ threadDelay 100000000

        getInputCommandTui cstate = do
            let parseCommand cmdline =
                    case dropWhile isSpace <$> span (\c -> isAlphaNum c || c == '-') cmdline of
                        ( scmd, args )
                            | not (null scmd) && all isDigit scmd
                            -> ( cmdSelectContext, scmd )

                            | otherwise
                            -> ( fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args )

            ( CommandM cmd, line ) <- getInputLinesTui cstate >>= return . \case
                '/' : input     -> parseCommand input
                input | not tui -> parseCommand input
                input           -> ( cmdSend, input )
            return ( cmd, line )

    let getInputCommand = getInputCommandTui . Left

    contextVar <- liftIO $ newMVar NoContext

    _ <- liftIO $ do
        tzone <- getCurrentTimeZone
        let self = finalOwner $ headLocalIdentity erebosHead
        watchDirectMessageThreads erebosHead $ \prev cur -> do
            forM_ (reverse $ dmThreadToListSince prev cur) $ \msg -> do
                withMVar contextVar $ \ctx -> do
                    mbpid <- case ctx of
                        SelectedPeer peer -> getPeerIdentity peer >>= return . \case
                            PeerIdentityFull pid -> Just $ finalOwner pid
                            _ -> Nothing
                        SelectedContact contact
                            | Just cid <- contactIdentity contact -> return (Just cid)
                        SelectedConversation conv -> return $ conversationPeer conv
                        _ -> return Nothing
                    when (not tui || maybe False (msgPeer cur `sameIdentity`) mbpid) $ do
                        extPrintLn $ plainText $ T.pack $ formatDirectMessage tzone msg

                case optDmBotEcho opts of
                    Just prefix
                        | not (msgFrom msg `sameIdentity` self)
                        -> do
                            void $ forkIO $ do
                                res <- runExceptT $ flip runReaderT erebosHead $ sendDirectMessage (msgFrom msg) (prefix <> msgText msg)
                                case res of
                                    Right _ -> return ()
                                    Left err -> extPrintLn $ "Failed to send dm echo: " <> err
                    _ -> return ()

            updatePromptStatus term erebosHead
    updatePromptStatus term erebosHead

    peers <- liftIO $ newMVar []
    contextOptions <- liftIO $ newMVar ( Nothing, [] )
    chatroomSetVar <- liftIO $ newEmptyMVar

    let autoSubscribe = optChatroomAutoSubscribe opts
        chatroomList = fromSetBy (comparing roomStateData) . lookupSharedValue . lsShared . headObject $ erebosHead
    watched <- if isJust autoSubscribe || any roomStateSubscribe chatroomList
      then do
        fmap Just $ liftIO $ watchChatroomsForCli tui extPrintLn erebosHead
            chatroomSetVar contextVar contextOptions autoSubscribe
      else do
        return Nothing

    server <- liftIO $ do
        startServer (optServer opts) erebosHead (extPrintLn . plainText . T.pack) $
            map soptService $ filter soptEnabled $ optServices opts

    case optWebSocketServer opts of
        Just port -> startWebsocketServer server "::" port (extPrintLn . plainText . T.pack)
        Nothing -> return ()

    void $ liftIO $ forkIO $ void $ forever $ do
        peer <- getNextPeerChange server
        getPeerIdentity peer >>= \case
            pid@(PeerIdentityFull _) -> do
                dropped <- isPeerDropped peer
                shown <- showPeer pid <$> getPeerAddress peer
                let update [] = ([(peer, shown)], (Nothing, "NEW"))
                    update ((p,s):ps)
                        | p == peer && dropped = (ps, (Nothing, "DEL"))
                        | p == peer = ((peer, shown) : ps, (Just s, "UPD"))
                        | otherwise = first ((p,s):) $ update ps
                let ctxUpdate n [] = ([SelectedPeer peer], n)
                    ctxUpdate n (ctx:ctxs)
                        | SelectedPeer p <- ctx, p == peer = (ctx:ctxs, n)
                        | otherwise = first (ctx:) $ ctxUpdate (n + 1) ctxs
                (op, updateType) <- modifyMVar peers (return . update)
                let updateType' = if dropped then "DEL" else updateType
                modifyMVar_ contextOptions $ \case
                    ( watch, clist )
                      | watch == Just WatchPeers || not tui
                      -> do
                        let ( clist', idx ) = ctxUpdate (1 :: Int) clist
                        when (Just shown /= op) $ do
                            extPrintLn $ plainText $ T.pack $ "[" <> show idx <> "] PEER " <> updateType' <> " " <> shown
                        return ( Just WatchPeers, clist' )
                    cur -> return cur
            _ -> return ()

    let process :: CommandState -> MaybeT IO CommandState
        process cstate = do
            (cmd, line) <- getInputCommand cstate
            h <- liftIO (reloadHead $ csHead cstate) >>= \case
                Just h  -> return h
                Nothing -> do lift $ extPrintLn "current head deleted"
                              mzero
            res <- liftIO $ modifyMVar contextVar $ \ctx -> do
                res <- runExceptT $ flip execStateT cstate { csHead = h, csContext = ctx } $ runReaderT cmd CommandInput
                    { ciServer = server
                    , ciTerminal = term
                    , ciLine = line
                    , ciPrint = extPrintLn
                    , ciOptions = opts
                    , ciPeers = liftIO $ modifyMVar peers $ \ps -> do
                        ps' <- filterM (fmap not . isPeerDropped . fst) ps
                        return (ps', ps')
                    , ciContextOptions = liftIO $ snd <$> readMVar contextOptions
                    , ciSetContextOptions = \watch ctxs -> liftIO $ modifyMVar_ contextOptions $ const $ return ( Just watch, ctxs )
                    , ciContextVar = contextVar
                    , ciContextOptionsVar = contextOptions
                    , ciChatroomSetVar = chatroomSetVar
                    }
                return ( either (const ctx) csContext res, res )
            case res of
                Right cstate'
                    | csQuit cstate' -> mzero
                    | otherwise      -> return cstate'
                Left err -> do
                    lift $ extPrintLn $ withStyle (setForegroundColor BrightRed noStyle) $ plainText $ T.pack $ "Error: " ++ showErebosError err
                    return cstate

    let loop (Just cstate) = runMaybeT (process cstate) >>= loop
        loop Nothing = return ()
    loop $ Just $ CommandState
        { csHead = erebosHead
        , csContext = NoContext
        , csWatchChatrooms = watched
        , csQuit = False
        }
    hidePrompt term


data CommandInput = CommandInput
    { ciServer :: Server
    , ciTerminal :: Terminal
    , ciLine :: String
    , ciPrint :: FormattedText -> IO ()
    , ciOptions :: Options
    , ciPeers :: CommandM [(Peer, String)]
    , ciContextOptions :: CommandM [ CommandContext ]
    , ciSetContextOptions :: ContextWatchOptions -> [ CommandContext ] -> Command
    , ciContextVar :: MVar CommandContext
    , ciContextOptionsVar :: MVar ( Maybe ContextWatchOptions, [ CommandContext ] )
    , ciChatroomSetVar :: MVar (Set ChatroomState)
    }

data CommandState = CommandState
    { csHead :: Head LocalState
    , csContext :: CommandContext
    , csWatchChatrooms :: Maybe WatchedHead
    , csQuit :: Bool
    }

data CommandContext
    = NoContext
    | SelectedPeer Peer
    | SelectedContact Contact
    | SelectedChatroom ChatroomState
    | SelectedConversation Conversation

data ContextWatchOptions
    = WatchPeers
    | WatchContacts
    | WatchChatrooms
    | WatchConversations
    deriving (Eq)

newtype CommandM a = CommandM (ReaderT CommandInput (StateT CommandState (ExceptT ErebosError IO)) a)
    deriving (Functor, Applicative, Monad, MonadReader CommandInput, MonadState CommandState, MonadError ErebosError)

instance MonadFail CommandM where
    fail = throwOtherError

instance MonadIO CommandM where
    liftIO act = CommandM (liftIO (try act)) >>= \case
        Left (e :: SomeException) -> throwOtherError (show e)
        Right x -> return x

instance MonadRandom CommandM where
    getRandomBytes = liftIO . getRandomBytes

instance MonadStorage CommandM where
    getStorage = gets $ headStorage . csHead

instance MonadHead LocalState CommandM where
    updateLocalHead f = do
        h <- gets csHead
        (Just h', x) <- maybe (fail "failed to reload head") (flip updateHead f) =<< reloadHead h
        modify $ \s -> s { csHead = h' }
        return x

type Command = CommandM ()

getSelectedPeer :: CommandM Peer
getSelectedPeer = gets csContext >>= \case
    SelectedPeer peer -> return peer
    _ -> throwOtherError "no peer selected"

getSelectedChatroom :: CommandM ChatroomState
getSelectedChatroom = gets csContext >>= \case
    SelectedChatroom rstate -> return rstate
    _ -> throwOtherError "no chatroom selected"

getSelectedConversation :: CommandM Conversation
getSelectedConversation = gets csContext >>= getConversationFromContext

getConversationFromContext :: CommandContext -> CommandM Conversation
getConversationFromContext = \case
    SelectedPeer peer -> getPeerIdentity peer >>= \case
        PeerIdentityFull pid -> directMessageConversation $ finalOwner pid
        _ -> throwOtherError "incomplete peer identity"
    SelectedContact contact -> case contactIdentity contact of
        Just cid -> directMessageConversation cid
        Nothing -> throwOtherError "contact without erebos identity"
    SelectedChatroom rstate ->
        chatroomConversation rstate >>= \case
            Just conv -> return conv
            Nothing -> throwOtherError "invalid chatroom"
    SelectedConversation conv -> reloadConversation conv
    _ -> throwOtherError "no contact, peer or conversation selected"

getSelectedOrManualContext :: CommandM CommandContext
getSelectedOrManualContext = do
    asks ciLine >>= \case
        "" -> gets csContext
        str | all isDigit str -> getContextByIndex id (read str)
        _ -> throwOtherError "invalid index"

updatePromptStatus :: Terminal -> Head LocalState -> IO ()
updatePromptStatus term h = do
    conversations <- mapMaybe checkNew <$> flip runReaderT h lookupConversations
    setPromptStatus term $ withStyle (setForegroundColor BrightYellow noStyle) $ formatStatus (length conversations) <> " "
  where
    checkNew conv
        | (msg : _) <- conversationHistory conv
        , messageUnread msg
        = Just ( conv, msg )
    checkNew _ = Nothing

    formatStatus 0 = withStyle (setForegroundColor BrightBlack noStyle) "--"
    formatStatus 1 = withStyle (setForegroundColor BrightYellow noStyle) " *"
    formatStatus n
        | n < 10 = withStyle (setForegroundColor BrightYellow noStyle) $ plainText $ T.pack (show n) <> "*"
        | otherwise = withStyle (setForegroundColor BrightYellow noStyle) $ plainText $ "X*"


commands :: [(String, Command)]
commands =
    [ ( "history", cmdHistory )
    , ( "identity", cmdIdentity )
    , ( "peers", cmdPeers )
    , ( "peer-add", cmdPeerAdd )
    , ( "peer-add-public", cmdPeerAddPublic )
    , ( "peer-drop", cmdPeerDrop )
    , ( "send", cmdSend )
    , ( "delete", cmdDelete )
    , ( "update-identity", cmdUpdateIdentity )
    , ( "attach", cmdAttach )
    , ( "attach-accept", cmdAttachAccept )
    , ( "attach-reject", cmdAttachReject )
    , ( "chatrooms", cmdChatrooms )
    , ( "chatroom-create-public", cmdChatroomCreatePublic )
    , ( "contacts", cmdContacts )
    , ( "contact-add", cmdContactAdd )
    , ( "contact-accept", cmdContactAccept )
    , ( "contact-reject", cmdContactReject )
    , ( "invite-contact", cmdInviteContact )
    , ( "invite-accept", cmdInviteAccept )
    , ( "conversations", cmdConversations )
    , ( "new", cmdNew )
    , ( "details", cmdDetails )
    , ( "discovery", cmdDiscovery )
    , ( "join", cmdJoin )
    , ( "join-as", cmdJoinAs )
    , ( "leave", cmdLeave )
    , ( "members", cmdMembers )
    , ( "select", cmdSelectContext )
    , ( "quit", cmdQuit )
    ]

commandCompletion :: CompletionFunc IO
commandCompletion = completeWordWithPrev Nothing [ ' ', '\t', '\n', '\r' ] $ curry $ \case
    ([], '/':pref) -> return . map (simpleCompletion . ('/':)) . filter (pref `isPrefixOf`) $ sortedCommandNames
    _ -> return []
  where
    sortedCommandNames = sort $ map fst commands


cmdPutStrLn :: FormattedText -> Command
cmdPutStrLn str = do
    term <- asks ciTerminal
    void $ liftIO $ printLine term str

cmdUnknown :: String -> Command
cmdUnknown cmd = cmdPutStrLn $ withStyle (setForegroundColor BrightRed noStyle) $ plainText $ "Unknown command: ‘" <> T.pack cmd <> "’"

cmdPeers :: Command
cmdPeers = do
    peers <- join $ asks ciPeers
    set <- asks ciSetContextOptions
    set WatchPeers $ map (SelectedPeer . fst) peers
    forM_ (zip [1..] peers) $ \(i :: Int, (_, name)) -> do
        cmdPutStrLn $ plainText $ T.pack $ "[" ++ show i ++ "] " ++ name

cmdPeerAdd :: Command
cmdPeerAdd = void $ do
    server <- asks ciServer
    (hostname, port) <- (words <$> asks ciLine) >>= \case
        hostname:p:_ -> return (hostname, p)
        [hostname] -> return (hostname, show discoveryPort)
        [] -> throwOtherError "missing peer address"
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just hostname) (Just port)
    contextOptsVar <- asks ciContextOptionsVar
    liftIO $ modifyMVar_ contextOptsVar $ return . first (const $ Just WatchPeers)
    liftIO $ serverPeer server (addrAddress addr)

cmdPeerAddPublic :: Command
cmdPeerAddPublic = do
    server <- asks ciServer
    contextOptsVar <- asks ciContextOptionsVar
    liftIO $ modifyMVar_ contextOptsVar $ return . first (const $ Just WatchPeers)
    liftIO $ mapM_ (serverPeer server . addrAddress) =<< gather 'a'
  where
    gather c
        | c <= 'z' = do
            let hints = Just $ defaultHints { addrSocketType = Datagram }
                hostname = Just $ c : ".discovery.erebosprotocol.net"
                service = Just $ show discoveryPort
            handle (\(_ :: IOException) -> return []) (getAddrInfo hints hostname service) >>= \case
                addr : _ -> (addr :) <$> gather (succ c)
                [] -> return []

        | otherwise = do
            return []

cmdPeerDrop :: Command
cmdPeerDrop = do
    dropPeer =<< getSelectedPeer
    modify $ \s -> s { csContext = NoContext }

showPeer :: PeerIdentity -> PeerAddress -> String
showPeer pidentity paddr =
    let name = case pidentity of
                    PeerIdentityUnknown _  -> "<noid>"
                    PeerIdentityRef wref _ -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                    PeerIdentityFull pid   -> T.unpack $ displayIdentity pid
     in name ++ " [" ++ show paddr ++ "]"

cmdJoin :: Command
cmdJoin = joinChatroom =<< getSelectedChatroom

cmdJoinAs :: Command
cmdJoinAs = do
    name <- asks ciLine
    identity <- createIdentity (Just $ T.pack name) Nothing
    joinChatroomAs identity =<< getSelectedChatroom

cmdLeave :: Command
cmdLeave = leaveChatroom =<< getSelectedChatroom

cmdMembers :: Command
cmdMembers = do
    Just room <- findChatroomByStateData . head . roomStateData =<< getSelectedChatroom
    forM_ (chatroomMembers room) $ \x -> do
        cmdPutStrLn $ plainText $ fromMaybe "<unnamed>" $ idName x

getContextByIndex :: (Maybe ContextWatchOptions -> Maybe ContextWatchOptions) -> Int -> CommandM CommandContext
getContextByIndex f n = do
    contextOptsVar <- asks ciContextOptionsVar
    join $ liftIO $ modifyMVar contextOptsVar $ \cur@( watch, ctxs ) -> if
        | n > 0, (ctx : _) <- drop (n - 1) ctxs
        -> return ( ( f watch, ctxs ), return ctx )

        | otherwise
        -> return ( cur, throwOtherError "invalid index" )

cmdSelectContext :: Command
cmdSelectContext = do
    n <- read <$> asks ciLine
    ctx <- getContextByIndex (const Nothing) n
    modify $ \s -> s { csContext = ctx }
    case ctx of
        SelectedChatroom rstate -> do
            when (not (roomStateSubscribe rstate)) $ do
                chatroomSetSubscribe (head $ roomStateData rstate) True
        _ -> return ()
    flip catchError (\_ -> return ()) $ do
        conv <- getConversationFromContext ctx
        tzone <- liftIO $ getCurrentTimeZone
        mapM_ (cmdPutStrLn . formatMessageFT tzone) $ takeWhile messageUnread $ conversationHistory conv

cmdSend :: Command
cmdSend = void $ do
    text <- asks ciLine
    conv <- getSelectedConversation
    sendMessage conv (T.pack text)

cmdDelete :: Command
cmdDelete = void $ do
    deleteConversation =<< getConversationFromContext =<< getSelectedOrManualContext
    modify $ \s -> s { csContext = NoContext }

cmdHistory :: Command
cmdHistory = void $ do
    conv <- getConversationFromContext =<< getSelectedOrManualContext
    case conversationHistory conv of
        thread@(_:_) -> do
            tzone <- liftIO $ getCurrentTimeZone
            mapM_ (cmdPutStrLn . formatMessageFT tzone) $ reverse $ take 50 thread
        [] -> do
            cmdPutStrLn $ withStyle (setForegroundColor BrightBlack noStyle) $ plainText "(empty history)"

showIdentityDetails :: Foldable f => Identity f -> Text
showIdentityDetails identity = T.unlines $ go $ reverse $ unfoldOwners identity
  where
    go (i : is) = concat
        [ maybeToList $ ("Name: " <>) <$> idName i
        , map (("Ref: " <>) . T.pack . show . refDigest . storedRef) $ idDataF i
        , map (("ExtRef: " <>) . T.pack . show . refDigest . storedRef) $ filter isExtension $ idExtDataF i
        , do guard $ not (null is)
             "" : "Device:" : map ("  " <>) (go is)
        ]
    go [] = []
    isExtension x = case fromSigned x of BaseIdentityData {} -> False
                                         _ -> True

cmdIdentity :: Command
cmdIdentity = do
    cmdPutStrLn . plainText . showIdentityDetails . localIdentity . fromStored =<< getLocalHead

cmdUpdateIdentity :: Command
cmdUpdateIdentity = void $ do
    term <- asks ciTerminal
    runReaderT (updateSharedIdentity term) =<< gets csHead

cmdAttach :: Command
cmdAttach = attachToOwner =<< getSelectedPeer

cmdAttachAccept :: Command
cmdAttachAccept = attachAccept =<< getSelectedPeer

cmdAttachReject :: Command
cmdAttachReject = attachReject =<< getSelectedPeer

watchChatroomsForCli
    :: Bool -> (FormattedText -> IO ()) -> Head LocalState -> MVar (Set ChatroomState)
    -> MVar CommandContext -> MVar ( Maybe ContextWatchOptions, [ CommandContext ] )
    -> Maybe Int -> IO WatchedHead
watchChatroomsForCli tui eprint h chatroomSetVar contextVar contextOptsVar autoSubscribe = do
    subscribedNumVar <- newEmptyMVar

    let ctxUpdate updateType (idx :: Int) rstate = \case
            SelectedChatroom rstate' : rest
                | currentRoots <- filterAncestors (concatMap storedRoots $ roomStateData rstate)
                , any ((`intersectsSorted` currentRoots) . storedRoots) $ roomStateData rstate'
                -> do
                    eprint $ plainText $ T.pack $ "[" <> show idx <> "] CHATROOM " <> updateType <> " " <> name
                    return (SelectedChatroom rstate : rest)
            selected : rest
                -> do
                    (selected : ) <$> ctxUpdate updateType (idx + 1) rstate rest
            []
                -> do
                    eprint $ plainText $ T.pack $ "[" <> show idx <> "] CHATROOM " <> updateType <> " " <> name
                    return [ SelectedChatroom rstate ]
          where
            name = maybe "<unnamed>" T.unpack $ roomName =<< roomStateRoom rstate

    watchChatrooms h $ \set -> \case
        Nothing -> do
            let chatroomList = filter (not . roomStateDeleted) $ fromSetBy (comparing roomStateData) set
                (subscribed, notSubscribed) = partition roomStateSubscribe chatroomList
                subscribedNum = length subscribed

            putMVar chatroomSetVar set
            putMVar subscribedNumVar subscribedNum

            case autoSubscribe of
                Nothing -> return ()
                Just num -> do
                    forM_ (take (num - subscribedNum) notSubscribed) $ \rstate -> do
                        (runExceptT $ flip runReaderT h $ chatroomSetSubscribe (head $ roomStateData rstate) True) >>= \case
                             Right () -> return ()
                             Left err -> eprint $ plainText $ T.pack $ showErebosError err

        Just diff -> do
            modifyMVar_ chatroomSetVar $ return . const set
            modifyMVar_ contextOptsVar $ \case
                ( watch, clist )
                  | watch == Just WatchChatrooms || not tui
                  -> do
                    let upd c = \case
                            AddedChatroom rstate -> ctxUpdate "NEW" 1 rstate c
                            RemovedChatroom rstate -> ctxUpdate "DEL" 1 rstate c
                            UpdatedChatroom _ rstate
                                | any ((\rsd -> not (null (rsdRoom rsd))) . fromStored) (roomStateData rstate)
                                -> do
                                    ctxUpdate "UPD" 1 rstate c
                                | otherwise -> return c
                    ( watch, ) <$> foldM upd clist diff
                cur -> return cur

            forM_ diff $ \case
                AddedChatroom rstate -> do
                    modifyMVar_ subscribedNumVar $ return . if roomStateSubscribe rstate then (+ 1) else id

                RemovedChatroom rstate -> do
                    modifyMVar_ subscribedNumVar $ return . if roomStateSubscribe rstate then subtract 1 else id

                UpdatedChatroom oldroom rstate -> do
                    when (any (not . null . rsdMessages . fromStored) (roomStateData rstate)) $ do
                        withMVar contextVar $ \ctx -> do
                            isSelected <- case ctx of
                                SelectedChatroom rstate' -> return $ isSameChatroom rstate' rstate
                                SelectedConversation conv -> return $ isChatroomStateConversation rstate conv
                                _ -> return False
                            when (not tui || isSelected) $ do
                                tzone <- getCurrentTimeZone
                                forM_ (reverse $ getMessagesSinceState rstate oldroom) $ \msg -> do
                                    eprint $ plainText $ T.concat
                                        [ T.pack $ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ cmsgTime msg
                                        , fromMaybe "<unnamed>" $ idName $ cmsgFrom msg
                                        , if cmsgLeave msg then " left" else ""
                                        , maybe (if cmsgLeave msg then "" else " joined") ((": " <>)) $ cmsgText msg
                                        ]
                    modifyMVar_ subscribedNumVar $ return
                        . (if roomStateSubscribe rstate then (+ 1) else id)
                        . (if roomStateSubscribe oldroom then subtract 1 else id)

ensureWatchedChatrooms :: Command
ensureWatchedChatrooms = do
    gets csWatchChatrooms >>= \case
        Nothing -> do
            eprint <- asks ciPrint
            h <- gets csHead
            chatroomSetVar <- asks ciChatroomSetVar
            contextVar <- asks ciContextVar
            contextOptsVar <- asks ciContextOptionsVar
            autoSubscribe <- asks $ optChatroomAutoSubscribe . ciOptions
            tui <- asks $ hasTerminalUI . ciTerminal
            watched <- liftIO $ watchChatroomsForCli tui eprint h chatroomSetVar contextVar contextOptsVar autoSubscribe
            modify $ \s -> s { csWatchChatrooms = Just watched }
        Just _ -> return ()

cmdChatrooms :: Command
cmdChatrooms = do
    ensureWatchedChatrooms
    chatroomSetVar <- asks ciChatroomSetVar
    chatroomList <- filter (not . roomStateDeleted) . fromSetBy (comparing roomStateData) <$> liftIO (readMVar chatroomSetVar)
    set <- asks ciSetContextOptions
    set WatchChatrooms $ map SelectedChatroom chatroomList
    forM_ (zip [1..] chatroomList) $ \(i :: Int, rstate) -> do
        cmdPutStrLn $ plainText $ "[" <> T.pack (show i) <> "] " <> fromMaybe "<unnamed>" (roomName =<< roomStateRoom rstate)

cmdChatroomCreatePublic :: Command
cmdChatroomCreatePublic = do
    term <- asks ciTerminal
    name <- asks ciLine >>= \case
        line | not (null line) -> return $ T.pack line
        _ -> liftIO $ do
            setPrompt term "Name: "
            getInputLine term $ KeepPrompt . maybe T.empty T.pack

    ensureWatchedChatrooms
    contextOptsVar <- asks ciContextOptionsVar
    liftIO $ modifyMVar_ contextOptsVar $ return . first (const $ Just WatchChatrooms)
    void $ createChatroom
        (if T.null name then Nothing else Just name)
        Nothing


cmdContacts :: Command
cmdContacts = do
    args <- words <$> asks ciLine
    ehead <- gets csHead
    let contacts = fromSetBy (comparing contactName) $ lookupSharedValue $ lsShared $ headObject ehead
        verbose = "-v" `elem` args
    set <- asks ciSetContextOptions
    set WatchContacts $ map SelectedContact contacts
    forM_ (zip [1..] contacts) $ \(i :: Int, c) -> do
        cmdPutStrLn $ plainText $ T.concat
            [ "[", T.pack (show i), "] ", contactName c
            , case contactIdentity c of
                   Just idt | cname <- displayIdentity idt
                            , cname /= contactName c
                            -> " (" <> cname <> ")"
                   _ -> ""
            , if verbose then " " <> (T.unwords $ map (T.decodeUtf8 . showRef . storedRef) $ maybe [] idDataF $ contactIdentity c)
                         else ""
            ]

cmdContactAdd :: Command
cmdContactAdd = contactRequest =<< getSelectedPeer

cmdContactAccept :: Command
cmdContactAccept = contactAccept =<< getSelectedPeer

cmdContactReject :: Command
cmdContactReject = contactReject =<< getSelectedPeer

cmdInviteContact :: Command
cmdInviteContact = do
    term <- asks ciTerminal
    name <- asks ciLine >>= \case
        line | not (null line) -> return $ T.pack line
        _ -> liftIO $ do
            setPrompt term "Name: "
            getInputLine term $ KeepPrompt . maybe T.empty T.pack
    (lookupSharedValue . lsShared . fromStored <$> getLocalHead) >>= \case
        Just (self :: ComposedIdentity) -> do
            invite <- createSingleContactInvite name
            dgst : _ <- return $ refDigest . storedRef <$> idDataF self
            cmdPutStrLn $ plainText $ "https://app.erebosprotocol.net/#inv" <> (maybe "" (("=" <>) . textInviteToken) (inviteToken invite)) <> "&from=blake2%23" <> T.pack (drop 7 (show dgst))
        Nothing -> do
            throwOtherError "no shared identity"

cmdInviteAccept :: Command
cmdInviteAccept = do
    term <- asks ciTerminal
    url <- asks ciLine >>= \case
        line | not (null line) -> return $ T.pack line
        _ -> liftIO $ do
            setPrompt term "URL: "
            getInputLine term $ KeepPrompt . maybe T.empty T.pack
    case T.breakOn "://" url of
        ( proto, url' )
            | proto `elem` [ "http", "https" ]
            , ( _, url'' ) <- T.breakOn "#" url'
            , Just ( '#', params ) <- T.uncons url''
            , [ pfrom, pinv ] <- sort $ T.splitOn "&" params
            , ( nfrom, tfrom ) <- T.splitAt 14 pfrom, nfrom == "from=blake2%23"
            , ( ninv, tinv ) <- T.splitAt 4 pinv, ninv == "inv="
            , Just from <- readRefDigest $ T.encodeUtf8 $ "blake2#" <> tfrom
            , Just token <- parseInviteToken tinv
            -> do
                acceptInvite from token
        _ -> throwOtherError "invalit invite URL"

cmdConversations :: Command
cmdConversations = do
    conversations <- lookupConversations
    set <- asks ciSetContextOptions
    set WatchConversations $ map SelectedConversation conversations
    forM_ (zip [1..] conversations) $ \(i :: Int, conv) -> do
        cmdPutStrLn $ plainText $ "[" <> T.pack (show i) <> "] " <> conversationName conv

cmdNew :: Command
cmdNew = do
    conversations <- mapMaybe checkNew <$> lookupConversations
    set <- asks ciSetContextOptions
    set WatchConversations $ map (SelectedConversation . fst) conversations
    tzone <- liftIO $ getCurrentTimeZone
    forM_ (zip [1..] conversations) $ \(i :: Int, ( conv, msg )) -> do
        cmdPutStrLn $ plainText ("[" <> T.pack (show i) <> "] " <> conversationName conv <> " ") <> formatMessageFT tzone msg
  where
    checkNew conv
        | (msg : _) <- conversationHistory conv
        , messageUnread msg
        = Just ( conv, msg )
    checkNew _ = Nothing


cmdDetails :: Command
cmdDetails = do
    getSelectedOrManualContext >>= \case
        SelectedPeer peer -> do
            paddr <- getPeerAddress peer
            cmdPutStrLn $ plainText $ T.unlines
                [ "Network peer:"
                , "  " <> T.pack (show paddr)
                ]
            getPeerIdentity peer >>= \case
                PeerIdentityUnknown _ -> do
                    cmdPutStrLn $ "unknown identity"
                PeerIdentityRef wref _ -> do
                    cmdPutStrLn $ "Identity ref:"
                    cmdPutStrLn $ plainText $ "  " <> T.decodeUtf8 (showRefDigest $ wrDigest wref)
                PeerIdentityFull pid -> printContactOrIdentityDetails pid

        SelectedContact contact -> do
            printContactDetails contact

        SelectedChatroom rstate -> do
            cmdPutStrLn $ plainText $ "Chatroom: " <> (fromMaybe "<unnamed>" $ roomName =<< roomStateRoom rstate)

        SelectedConversation conv -> do
            case conversationPeer conv of
                Just pid -> printContactOrIdentityDetails pid
                Nothing -> cmdPutStrLn $ "(conversation without peer)"

        NoContext -> cmdPutStrLn "nothing selected"
  where
    printContactOrIdentityDetails cid = do
        contacts <- fromSetBy (comparing contactName) . lookupSharedValue . lsShared . fromStored <$> getLocalHead
        case find (maybe False (sameIdentity cid) . contactIdentity) contacts of
            Just contact -> printContactDetails contact
            Nothing -> printIdentityDetails cid

    printContactDetails contact = do
        cmdPutStrLn $ "Contact:"
        prefix <- case contactCustomName contact of
            Just name -> do
                cmdPutStrLn $ plainText $ "  " <> name
                return $ Just "alias of"
            Nothing -> do
                return $ Nothing

        case contactIdentity contact of
            Just cid -> do
                printIdentityDetailsBody prefix cid
            Nothing -> do
                cmdPutStrLn $ "  (without erebos identity)"

    printIdentityDetails identity = do
        cmdPutStrLn $ "Identity:"
        printIdentityDetailsBody Nothing identity

    printIdentityDetailsBody prefix identity = do
        forM_ (zip (False : repeat True) $ unfoldOwners identity) $ \(owned, cpid) -> do
            cmdPutStrLn $ plainText $ T.unwords $ concat
                [ [ "  " ]
                , if owned then [ "owned by" ] else maybeToList prefix
                , [ fromMaybe "<unnamed>" (idName cpid) ]
                , map (T.decodeUtf8 . showRefDigest . refDigest . storedRef) $ idExtDataF cpid
                ]

cmdDiscovery :: Command
cmdDiscovery = void $ do
    server <- asks ciServer
    sref <- asks ciLine
    case readRefDigest (BC.pack sref) of
        Nothing -> throwOtherError "failed to parse ref"
        Just dgst -> discoverySearch server dgst

cmdQuit :: Command
cmdQuit = modify $ \s -> s { csQuit = True }


intersectsSorted :: Ord a => [a] -> [a] -> Bool
intersectsSorted (x:xs) (y:ys) | x < y     = intersectsSorted xs (y:ys)
                               | x > y     = intersectsSorted (x:xs) ys
                               | otherwise = True
intersectsSorted _ _ = False

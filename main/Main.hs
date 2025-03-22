{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Control.Arrow (first)
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Maybe

import Crypto.Random

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
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
import Erebos.Contact
import Erebos.Chatroom
import Erebos.Conversation
import Erebos.DirectMessage
import Erebos.Discovery
#ifdef ENABLE_ICE_SUPPORT
import Erebos.ICE
#endif
import Erebos.Identity
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

import State
import Terminal
import Test
import Version

data Options = Options
    { optServer :: ServerOptions
    , optServices :: [ServiceOption]
    , optStorage :: StorageOption
    , optChatroomAutoSubscribe :: Maybe Int
    , optDmBotEcho :: Maybe Text
    , optShowHelp :: Bool
    , optShowVersion :: Bool
    }

data StorageOption = DefaultStorage
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
    , optChatroomAutoSubscribe = Nothing
    , optDmBotEcho = Nothing
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
    ]

options :: [OptDescr (Options -> Options)]
options =
    [ Option ['p'] ["port"]
        (ReqArg (\p -> so $ \opts -> opts { serverPort = read p }) "<port>")
        "local port to bind"
    , Option ['s'] ["silent"]
        (NoArg (so $ \opts -> opts { serverLocalDiscovery = False }))
        "do not send announce packets for local discovery"
    , Option [] [ "storage" ]
        (ReqArg (\path -> \opts -> opts { optStorage = FilesystemStorage path }) "<path>")
        "use storage in <path>"
    , Option [] [ "memory-storage" ]
        (NoArg (\opts -> opts { optStorage = MemoryStorage }))
        "use memory storage"
    , Option [] ["chatroom-auto-subscribe"]
        (ReqArg (\count -> \opts -> opts { optChatroomAutoSubscribe = Just (read count) }) "<count>")
        "automatically subscribe for up to <count> chatrooms"
#ifdef ENABLE_ICE_SUPPORT
    , Option [] [ "discovery-stun-port" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> attrs { discoveryStunPort = Just (read value) }) "<port>")
        "offer specified <port> to discovery peers for STUN protocol"
    , Option [] [ "discovery-stun-server" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> attrs { discoveryStunServer = Just (read value) }) "<server>")
        "offer <server> (domain name or IP address) to discovery peers for STUN protocol"
    , Option [] [ "discovery-turn-port" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> attrs { discoveryTurnPort = Just (read value) }) "<port>")
        "offer specified <port> to discovery peers for TURN protocol"
    , Option [] [ "discovery-turn-server" ]
        (ReqArg (\value -> serviceAttr $ \attrs -> attrs { discoveryTurnServer = Just (read value) }) "<server>")
        "offer <server> (domain name or IP address) to discovery peers for TURN protocol"
#endif
    , Option [] ["dm-bot-echo"]
        (ReqArg (\prefix -> \opts -> opts { optDmBotEcho = Just (T.pack prefix) }) "<prefix>")
        "automatically reply to direct messages with the same text prefixed with <prefix>"
    , Option ['h'] ["help"]
        (NoArg $ \opts -> opts { optShowHelp = True })
        "show this help and exit"
    , Option ['V'] ["version"]
        (NoArg $ \opts -> opts { optShowVersion = True })
        "show version and exit"
    ]
  where
    so f opts = opts { optServer = f $ optServer opts }

    updateService :: Service s => (ServiceAttributes s -> ServiceAttributes s) -> SomeService -> SomeService
    updateService f some@(SomeService proxy attrs)
        | Just f' <- cast f = SomeService proxy (f' attrs)
        | otherwise = some

    serviceAttr :: Service s => (ServiceAttributes s -> ServiceAttributes s) -> Options -> Options
    serviceAttr f opts = opts { optServices = map (\sopt -> sopt { soptService = updateService f (soptService sopt) }) (optServices opts) }

servicesOptions :: [OptDescr (Options -> Options)]
servicesOptions = concatMap helper $ "all" : map soptName availableServices
  where
    helper name =
        [ Option [] ["enable-" <> name] (NoArg $ so $ change name $ \sopt -> sopt { soptEnabled = True }) ""
        , Option [] ["disable-" <> name] (NoArg $ so $ change name $ \sopt -> sopt { soptEnabled = False }) ""
        ]
    so f opts = opts { optServices = f $ optServices opts }
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
    (opts, args) <- (getOpt RequireOrder (options ++ servicesOptions) <$> getArgs) >>= \case
        (o, args, []) -> do
            return (foldl (flip id) defaultOptions o, args)
        (_, _, errs) -> do
            progName <- getProgName
            hPutStrLn stderr $ concat errs <> "Try `" <> progName <> " --help' for more information."
            exitFailure

    st <- liftIO $ case optStorage opts of
        DefaultStorage         -> openStorage =<< getDefaultStorageDir
        FilesystemStorage path -> openStorage path
        MemoryStorage          -> memoryStorage

    case args of
        ["cat-file", sref] -> do
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

        ["show-generation", sref] -> readRef st (BC.pack sref) >>= \case
            Nothing -> error "ref does not exist"
            Just ref -> print $ storedGeneration (wrappedLoad ref :: Stored Object)

        ["update-identity"] -> do
            withTerminal noCompletion $ \term -> do
                either (fail . showErebosError) return <=< runExceptT $ do
                    runReaderT (updateSharedIdentity term) =<< loadLocalStateHead term st

        ("update-identity" : srefs) -> do
            withTerminal noCompletion $ \term -> do
                sequence <$> mapM (readRef st . BC.pack) srefs >>= \case
                    Nothing -> error "ref does not exist"
                    Just refs
                        | Just idt <- validateIdentityF $ map wrappedLoad refs -> do
                            BC.putStrLn . showRefDigest . refDigest . storedRef . idData =<<
                                (either (fail . showErebosError) return <=< runExceptT $ runReaderT (interactiveIdentityUpdate term idt) st)
                        | otherwise -> error "invalid identity"

        ["test"] -> runTestTool st

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
            hPutStrLn stderr $ "Unknown command `" <> cmdname <> "'"
            exitFailure


interactiveLoop :: Storage -> Options -> IO ()
interactiveLoop st opts = withTerminal commandCompletion $ \term -> do
    erebosHead <- liftIO $ loadLocalStateHead term st
    void $ printLine term $ T.unpack $ displayIdentity $ headLocalIdentity erebosHead

    let tui = hasTerminalUI term
    let extPrintLn = void . printLine term

    let getInputLinesTui :: Either CommandState String -> MaybeT IO String
        getInputLinesTui eprompt = do
            prompt <- case eprompt of
                Left cstate -> do
                    pname <- case csContext cstate of
                        NoContext -> return ""
                        SelectedPeer peer -> peerIdentity peer >>= return . \case
                            PeerIdentityFull pid -> maybe "<unnamed>" T.unpack $ idName $ finalOwner pid
                            PeerIdentityRef wref _ -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                            PeerIdentityUnknown _  -> "<unknown>"
                        SelectedContact contact -> return $ T.unpack $ contactName contact
                        SelectedChatroom rstate -> return $ T.unpack $ fromMaybe (T.pack "<unnamed>") $ roomName =<< roomStateRoom rstate
                        SelectedConversation conv -> return $ T.unpack $ conversationName conv
                    return $ pname ++ "> "
                Right prompt -> return prompt
            lift $ setPrompt term prompt
            join $ lift $ getInputLine term $ \case
                Just input@('/' : _) -> KeepPrompt $ return input
                Just input -> ErasePrompt $ case reverse input of
                    _ | all isSpace input -> getInputLinesTui eprompt
                    '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLinesTui (Right ">> ")
                    _         -> return input
                Nothing -> KeepPrompt mzero

        getInputCommandTui cstate = do
            input <- getInputLinesTui cstate
            let (CommandM cmd, line) = case input of
                    '/':rest -> let (scmd, args) = dropWhile isSpace <$> span (\c -> isAlphaNum c || c == '-') rest
                                 in if not (null scmd) && all isDigit scmd
                                       then (cmdSelectContext, scmd)
                                       else (fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args)
                    _        -> (cmdSend, input)
            return (cmd, line)

        getInputLinesPipe = do
            join $ lift $ getInputLine term $ KeepPrompt . \case
                Just input -> return input
                Nothing -> liftIO $ forever $ threadDelay 100000000

        getInputCommandPipe _ = do
            input <- getInputLinesPipe
            let (scmd, args) = dropWhile isSpace <$> span (\c -> isAlphaNum c || c == '-') input
            let (CommandM cmd, line) = (fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args)
            return (cmd, line)

    let getInputCommand = if tui then getInputCommandTui . Left
                                 else getInputCommandPipe

    _ <- liftIO $ do
        tzone <- getCurrentTimeZone
        watchReceivedMessages erebosHead $ \smsg -> do
            let msg = fromStored smsg
            extPrintLn $ formatDirectMessage tzone msg
            case optDmBotEcho opts of
                Nothing -> return ()
                Just prefix -> do
                    res <- runExceptT $ flip runReaderT erebosHead $ sendDirectMessage (msgFrom msg) (prefix <> msgText msg)
                    case res of
                        Right reply -> extPrintLn $ formatDirectMessage tzone $ fromStored reply
                        Left err -> extPrintLn $ "Failed to send dm echo: " <> err

    peers <- liftIO $ newMVar []
    contextOptions <- liftIO $ newMVar []
    chatroomSetVar <- liftIO $ newEmptyMVar

    let autoSubscribe = optChatroomAutoSubscribe opts
        chatroomList = fromSetBy (comparing roomStateData) . lookupSharedValue . lsShared . headObject $ erebosHead
    watched <- if isJust autoSubscribe || any roomStateSubscribe chatroomList
        then fmap Just $ liftIO $ watchChatroomsForCli extPrintLn erebosHead chatroomSetVar contextOptions autoSubscribe
        else return Nothing

    server <- liftIO $ do
        startServer (optServer opts) erebosHead extPrintLn $
            map soptService $ filter soptEnabled $ optServices opts

    void $ liftIO $ forkIO $ void $ forever $ do
        peer <- getNextPeerChange server
        peerIdentity peer >>= \case
            pid@(PeerIdentityFull _) -> do
                dropped <- isPeerDropped peer
                let shown = showPeer pid $ peerAddress peer
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
                idx <- modifyMVar contextOptions (return . ctxUpdate (1 :: Int))
                when (Just shown /= op) $ extPrintLn $ "[" <> show idx <> "] PEER " <> updateType' <> " " <> shown
            _ -> return ()

    let process :: CommandState -> MaybeT IO CommandState
        process cstate = do
            (cmd, line) <- getInputCommand cstate
            h <- liftIO (reloadHead $ csHead cstate) >>= \case
                Just h  -> return h
                Nothing -> do lift $ extPrintLn "current head deleted"
                              mzero
            res <- liftIO $ runExceptT $ flip execStateT cstate { csHead = h } $ runReaderT cmd CommandInput
                { ciServer = server
                , ciTerminal = term
                , ciLine = line
                , ciPrint = extPrintLn
                , ciOptions = opts
                , ciPeers = liftIO $ modifyMVar peers $ \ps -> do
                    ps' <- filterM (fmap not . isPeerDropped . fst) ps
                    return (ps', ps')
                , ciContextOptions = liftIO $ readMVar contextOptions
                , ciSetContextOptions = \ctxs -> liftIO $ modifyMVar_ contextOptions $ const $ return ctxs
                , ciContextOptionsVar = contextOptions
                , ciChatroomSetVar = chatroomSetVar
                }
            case res of
                Right cstate'
                    | csQuit cstate' -> mzero
                    | otherwise      -> return cstate'
                Left err -> do
                    lift $ extPrintLn $ "Error: " ++ showErebosError err
                    return cstate

    let loop (Just cstate) = runMaybeT (process cstate) >>= loop
        loop Nothing = return ()
    loop $ Just $ CommandState
        { csHead = erebosHead
        , csContext = NoContext
#ifdef ENABLE_ICE_SUPPORT
        , csIceSessions = []
#endif
        , csIcePeer = Nothing
        , csWatchChatrooms = watched
        , csQuit = False
        }


data CommandInput = CommandInput
    { ciServer :: Server
    , ciTerminal :: Terminal
    , ciLine :: String
    , ciPrint :: String -> IO ()
    , ciOptions :: Options
    , ciPeers :: CommandM [(Peer, String)]
    , ciContextOptions :: CommandM [CommandContext]
    , ciSetContextOptions :: [CommandContext] -> Command
    , ciContextOptionsVar :: MVar [ CommandContext ]
    , ciChatroomSetVar :: MVar (Set ChatroomState)
    }

data CommandState = CommandState
    { csHead :: Head LocalState
    , csContext :: CommandContext
#ifdef ENABLE_ICE_SUPPORT
    , csIceSessions :: [IceSession]
#endif
    , csIcePeer :: Maybe Peer
    , csWatchChatrooms :: Maybe WatchedHead
    , csQuit :: Bool
    }

data CommandContext = NoContext
                    | SelectedPeer Peer
                    | SelectedContact Contact
                    | SelectedChatroom ChatroomState
                    | SelectedConversation Conversation

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
getSelectedConversation = gets csContext >>= \case
    SelectedPeer peer -> peerIdentity peer >>= \case
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

commands :: [(String, Command)]
commands =
    [ ("history", cmdHistory)
    , ("peers", cmdPeers)
    , ("peer-add", cmdPeerAdd)
    , ("peer-add-public", cmdPeerAddPublic)
    , ("peer-drop", cmdPeerDrop)
    , ("send", cmdSend)
    , ("delete", cmdDelete)
    , ("update-identity", cmdUpdateIdentity)
    , ("attach", cmdAttach)
    , ("attach-accept", cmdAttachAccept)
    , ("attach-reject", cmdAttachReject)
    , ("chatrooms", cmdChatrooms)
    , ("chatroom-create-public", cmdChatroomCreatePublic)
    , ("contacts", cmdContacts)
    , ("contact-add", cmdContactAdd)
    , ("contact-accept", cmdContactAccept)
    , ("contact-reject", cmdContactReject)
    , ("conversations", cmdConversations)
    , ("details", cmdDetails)
    , ("discovery-init", cmdDiscoveryInit)
    , ("discovery", cmdDiscovery)
#ifdef ENABLE_ICE_SUPPORT
    , ("ice-create", cmdIceCreate)
    , ("ice-destroy", cmdIceDestroy)
    , ("ice-show", cmdIceShow)
    , ("ice-connect", cmdIceConnect)
    , ("ice-send", cmdIceSend)
#endif
    , ("join", cmdJoin)
    , ("join-as", cmdJoinAs)
    , ("leave", cmdLeave)
    , ("members", cmdMembers)
    , ("select", cmdSelectContext)
    , ("quit", cmdQuit)
    ]

commandCompletion :: CompletionFunc IO
commandCompletion = completeWordWithPrev Nothing [ ' ', '\t', '\n', '\r' ] $ curry $ \case
    ([], '/':pref) -> return . map (simpleCompletion . ('/':)) . filter (pref `isPrefixOf`) $ sortedCommandNames
    _ -> return []
  where
    sortedCommandNames = sort $ map fst commands


cmdPutStrLn :: String -> Command
cmdPutStrLn str = do
    term <- asks ciTerminal
    void $ liftIO $ printLine term str

cmdUnknown :: String -> Command
cmdUnknown cmd = cmdPutStrLn $ "Unknown command: " ++ cmd

cmdPeers :: Command
cmdPeers = do
    peers <- join $ asks ciPeers
    set <- asks ciSetContextOptions
    set $ map (SelectedPeer . fst) peers
    forM_ (zip [1..] peers) $ \(i :: Int, (_, name)) -> do
        cmdPutStrLn $ "[" ++ show i ++ "] " ++ name

cmdPeerAdd :: Command
cmdPeerAdd = void $ do
    server <- asks ciServer
    (hostname, port) <- (words <$> asks ciLine) >>= \case
        hostname:p:_ -> return (hostname, p)
        [hostname] -> return (hostname, show discoveryPort)
        [] -> throwOtherError "missing peer address"
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just hostname) (Just port)
    liftIO $ serverPeer server (addrAddress addr)

cmdPeerAddPublic :: Command
cmdPeerAddPublic = do
    server <- asks ciServer
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
    st <- getStorage
    identity <- liftIO $ createIdentity st (Just $ T.pack name) Nothing
    joinChatroomAs identity =<< getSelectedChatroom

cmdLeave :: Command
cmdLeave = leaveChatroom =<< getSelectedChatroom

cmdMembers :: Command
cmdMembers = do
    Just room <- findChatroomByStateData . head . roomStateData =<< getSelectedChatroom
    forM_ (chatroomMembers room) $ \x -> do
        cmdPutStrLn $ maybe "<unnamed>" T.unpack $ idName x


cmdSelectContext :: Command
cmdSelectContext = do
    n <- read <$> asks ciLine
    join (asks ciContextOptions) >>= \ctxs -> if
        | n > 0, (ctx : _) <- drop (n - 1) ctxs -> do
            modify $ \s -> s { csContext = ctx }
            case ctx of
                SelectedChatroom rstate -> do
                    when (not (roomStateSubscribe rstate)) $ do
                        chatroomSetSubscribe (head $ roomStateData rstate) True
                _ -> return ()
        | otherwise -> throwOtherError "invalid index"

cmdSend :: Command
cmdSend = void $ do
    text <- asks ciLine
    conv <- getSelectedConversation
    sendMessage conv (T.pack text) >>= \case
        Just msg -> do
            tzone <- liftIO $ getCurrentTimeZone
            cmdPutStrLn $ formatMessage tzone msg
        Nothing -> return ()

cmdDelete :: Command
cmdDelete = void $ do
    deleteConversation =<< getSelectedConversation
    modify $ \s -> s { csContext = NoContext }

cmdHistory :: Command
cmdHistory = void $ do
    conv <- getSelectedConversation
    case conversationHistory conv of
        thread@(_:_) -> do
            tzone <- liftIO $ getCurrentTimeZone
            mapM_ (cmdPutStrLn . formatMessage tzone) $ reverse $ take 50 thread
        [] -> do
            cmdPutStrLn $ "<empty history>"

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

watchChatroomsForCli :: (String -> IO ()) -> Head LocalState -> MVar (Set ChatroomState) -> MVar [ CommandContext ] -> Maybe Int -> IO WatchedHead
watchChatroomsForCli eprint h chatroomSetVar contextVar autoSubscribe = do
    subscribedNumVar <- newEmptyMVar

    let ctxUpdate updateType (idx :: Int) rstate = \case
            SelectedChatroom rstate' : rest
                | currentRoots <- filterAncestors (concatMap storedRoots $ roomStateData rstate)
                , any ((`intersectsSorted` currentRoots) . storedRoots) $ roomStateData rstate'
                -> do
                    eprint $ "[" <> show idx <> "] CHATROOM " <> updateType <> " " <> name
                    return (SelectedChatroom rstate : rest)
            selected : rest
                -> do
                    (selected : ) <$> ctxUpdate updateType (idx + 1) rstate rest
            []
                -> do
                    eprint $ "[" <> show idx <> "] CHATROOM " <> updateType <> " " <> name
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
                             Left err -> eprint (showErebosError err)

        Just diff -> do
            modifyMVar_ chatroomSetVar $ return . const set
            forM_ diff $ \case
                AddedChatroom rstate -> do
                    modifyMVar_ contextVar $ ctxUpdate "NEW" 1 rstate
                    modifyMVar_ subscribedNumVar $ return . if roomStateSubscribe rstate then (+ 1) else id

                RemovedChatroom rstate -> do
                    modifyMVar_ contextVar $ ctxUpdate "DEL" 1 rstate
                    modifyMVar_ subscribedNumVar $ return . if roomStateSubscribe rstate then subtract 1 else id

                UpdatedChatroom oldroom rstate -> do
                    when (any ((\rsd -> not (null (rsdRoom rsd))) . fromStored) (roomStateData rstate)) $ do
                        modifyMVar_ contextVar $ ctxUpdate "UPD" 1 rstate
                    when (any (not . null . rsdMessages . fromStored) (roomStateData rstate)) $ do
                        tzone <- getCurrentTimeZone
                        forM_ (reverse $ getMessagesSinceState rstate oldroom) $ \msg -> do
                            eprint $ concat $
                                [ maybe "<unnamed>" T.unpack $ roomName =<< cmsgRoom msg
                                , formatTime defaultTimeLocale " [%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ cmsgTime msg
                                , maybe "<unnamed>" T.unpack $ idName $ cmsgFrom msg
                                , if cmsgLeave msg then " left" else ""
                                , maybe (if cmsgLeave msg then "" else " joined") ((": " ++) . T.unpack) $ cmsgText msg
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
            contextVar <- asks ciContextOptionsVar
            autoSubscribe <- asks $ optChatroomAutoSubscribe . ciOptions
            watched <- liftIO $ watchChatroomsForCli eprint h chatroomSetVar contextVar autoSubscribe
            modify $ \s -> s { csWatchChatrooms = Just watched }
        Just _ -> return ()

cmdChatrooms :: Command
cmdChatrooms = do
    ensureWatchedChatrooms
    chatroomSetVar <- asks ciChatroomSetVar
    chatroomList <- filter (not . roomStateDeleted) . fromSetBy (comparing roomStateData) <$> liftIO (readMVar chatroomSetVar)
    set <- asks ciSetContextOptions
    set $ map SelectedChatroom chatroomList
    forM_ (zip [1..] chatroomList) $ \(i :: Int, rstate) -> do
        cmdPutStrLn $ "[" ++ show i ++ "] " ++ maybe "<unnamed>" T.unpack (roomName =<< roomStateRoom rstate)

cmdChatroomCreatePublic :: Command
cmdChatroomCreatePublic = do
    term <- asks ciTerminal
    name <- asks ciLine >>= \case
        line | not (null line) -> return $ T.pack line
        _ -> liftIO $ do
            setPrompt term "Name: "
            getInputLine term $ KeepPrompt . maybe T.empty T.pack

    ensureWatchedChatrooms
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
    set $ map SelectedContact contacts
    forM_ (zip [1..] contacts) $ \(i :: Int, c) -> do
        cmdPutStrLn $ T.unpack $ T.concat
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

cmdConversations :: Command
cmdConversations = do
    conversations <- lookupConversations
    set <- asks ciSetContextOptions
    set $ map SelectedConversation conversations
    forM_ (zip [1..] conversations) $ \(i :: Int, conv) -> do
        cmdPutStrLn $ "[" ++ show i ++ "] " ++ T.unpack (conversationName conv)

cmdDetails :: Command
cmdDetails = do
    gets csContext >>= \case
        SelectedPeer peer -> do
            cmdPutStrLn $ unlines
                [ "Network peer:"
                , "  " <> show (peerAddress peer)
                ]
            peerIdentity peer >>= \case
                PeerIdentityUnknown _ -> do
                    cmdPutStrLn $ "unknown identity"
                PeerIdentityRef wref _ -> do
                    cmdPutStrLn $ "Identity ref:"
                    cmdPutStrLn $ "  " <> BC.unpack (showRefDigest $ wrDigest wref)
                PeerIdentityFull pid -> printContactOrIdentityDetails pid

        SelectedContact contact -> do
            printContactDetails contact

        SelectedChatroom rstate -> do
            cmdPutStrLn $ "Chatroom: " <> (T.unpack $ fromMaybe (T.pack "<unnamed>") $ roomName =<< roomStateRoom rstate)

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
                cmdPutStrLn $ "  " <> T.unpack name
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
            cmdPutStrLn $ unwords $ concat
                [ [ "  " ]
                , if owned then [ "owned by" ] else maybeToList prefix
                , [ maybe "<unnamed>" T.unpack (idName cpid) ]
                , map (BC.unpack . showRefDigest . refDigest . storedRef) $ idExtDataF cpid
                ]

cmdDiscoveryInit :: Command
cmdDiscoveryInit = void $ do
    server <- asks ciServer

    (hostname, port) <- (words <$> asks ciLine) >>= return . \case
        hostname:p:_ -> (hostname, p)
        [hostname] -> (hostname, show discoveryPort)
        [] -> ("discovery.erebosprotocol.net", show discoveryPort)
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just hostname) (Just port)
    peer <- liftIO $ serverPeer server (addrAddress addr)
    sendToPeer peer $ DiscoverySelf [ T.pack "ICE" ] Nothing
    modify $ \s -> s { csIcePeer = Just peer }

cmdDiscovery :: Command
cmdDiscovery = void $ do
    Just peer <- gets csIcePeer
    st <- getStorage
    sref <- asks ciLine
    eprint <- asks ciPrint
    liftIO $ readRef st (BC.pack sref) >>= \case
        Nothing -> error "ref does not exist"
        Just ref -> do
            res <- runExceptT $ sendToPeer peer $ DiscoverySearch ref
            case res of
                 Right _ -> return ()
                 Left err -> eprint err

#ifdef ENABLE_ICE_SUPPORT

cmdIceCreate :: Command
cmdIceCreate = do
    let getRole = \case
            'm':_ -> PjIceSessRoleControlling
            's':_ -> PjIceSessRoleControlled
            _ -> PjIceSessRoleUnknown

    ( role, stun, turn ) <- asks (words . ciLine) >>= \case
        [] -> return ( PjIceSessRoleControlling, Nothing, Nothing )
        [ role ] -> return
            ( getRole role, Nothing, Nothing )
        [ role, server ] -> return
            ( getRole role
            , Just ( T.pack server, 0 )
            , Just ( T.pack server, 0 )
            )
        [ role, server, port ] -> return
            ( getRole role
            , Just ( T.pack server, read port )
            , Just ( T.pack server, read port )
            )
        [ role, stunServer, stunPort, turnServer, turnPort ] -> return
            ( getRole role
            , Just ( T.pack stunServer, read stunPort )
            , Just ( T.pack turnServer, read turnPort )
            )
        _ -> throwOtherError "invalid parameters"

    eprint <- asks ciPrint
    Just cfg <- liftIO $ iceCreateConfig stun turn
    sess <- liftIO $ iceCreateSession cfg role $ eprint <=< iceShow
    modify $ \s -> s { csIceSessions = sess : csIceSessions s }

cmdIceDestroy :: Command
cmdIceDestroy = do
    s:ss <- gets csIceSessions
    modify $ \st -> st { csIceSessions = ss }
    liftIO $ iceDestroy s

cmdIceShow :: Command
cmdIceShow = do
    sess <- gets csIceSessions
    eprint <- asks ciPrint
    liftIO $ forM_ (zip [1::Int ..] sess) $ \(i, s) -> do
        eprint $ "[" ++ show i ++ "]"
        eprint =<< iceShow s

cmdIceConnect :: Command
cmdIceConnect = do
    s:_ <- gets csIceSessions
    server <- asks ciServer
    term <- asks ciTerminal
    let loadInfo =
            getInputLine term (KeepPrompt . maybe BC.empty BC.pack) >>= \case
                line | BC.null line -> return []
                     | otherwise    -> (line :) <$> loadInfo
    Right remote <- liftIO $ do
        st <- memoryStorage
        pst <- derivePartialStorage st
        setPrompt term ""
        rbytes <- (BL.fromStrict . BC.unlines) <$> loadInfo
        copyRef st =<< storeRawBytes pst (BL.fromChunks [ BC.pack "rec ", BC.pack (show (BL.length rbytes)), BC.singleton '\n' ] `BL.append` rbytes)
    liftIO $ iceConnect s (load remote) $ void $ serverPeerIce server s

cmdIceSend :: Command
cmdIceSend = void $ do
    s:_ <- gets csIceSessions
    server <- asks ciServer
    liftIO $ serverPeerIce server s

#endif

cmdQuit :: Command
cmdQuit = modify $ \s -> s { csQuit = True }


intersectsSorted :: Ord a => [a] -> [a] -> Bool
intersectsSorted (x:xs) (y:ys) | x < y     = intersectsSorted xs (y:ys)
                               | x > y     = intersectsSorted (x:xs) ys
                               | otherwise = True
intersectsSorted _ _ = False

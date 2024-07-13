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
import System.Console.Haskeline
import System.Environment
import System.Exit
import System.IO

import Erebos.Attach
import Erebos.Contact
import Erebos.Chatroom
import Erebos.Conversation
#ifdef ENABLE_ICE_SUPPORT
import Erebos.Discovery
import Erebos.ICE
#endif
import Erebos.Identity
import Erebos.Message hiding (formatMessage)
import Erebos.Network
import Erebos.PubKey
import Erebos.Service
import Erebos.Set
import Erebos.State
import Erebos.Storage
import Erebos.Storage.Merge
import Erebos.Sync

import Test
import Version

data Options = Options
    { optServer :: ServerOptions
    , optServices :: [ServiceOption]
    , optChatroomAutoSubscribe :: Maybe Int
    , optDmBotEcho :: Maybe Text
    , optShowHelp :: Bool
    , optShowVersion :: Bool
    }

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
#ifdef ENABLE_ICE_SUPPORT
    , ServiceOption "discovery" (someService @DiscoveryService Proxy)
        True "peer discovery"
#endif
    ]

options :: [OptDescr (Options -> Options)]
options =
    [ Option ['p'] ["port"]
        (ReqArg (\p -> so $ \opts -> opts { serverPort = read p }) "<port>")
        "local port to bind"
    , Option ['s'] ["silent"]
        (NoArg (so $ \opts -> opts { serverLocalDiscovery = False }))
        "do not send announce packets for local discovery"
    , Option [] ["chatroom-auto-subscribe"]
        (ReqArg (\count -> \opts -> opts { optChatroomAutoSubscribe = Just (read count) }) "<count>")
        "automatically subscribe for up to <count> chatrooms"
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
    where so f opts = opts { optServer = f $ optServer opts }

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

main :: IO ()
main = do
    st <- liftIO $ openStorage . fromMaybe "./.erebos" =<< lookupEnv "EREBOS_DIR"
    getArgs >>= \case
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
                    "identity" -> case validateIdentityF (wrappedLoad <$> refs) of
                        Just identity -> do
                            let disp :: Identity m -> IO ()
                                disp idt = do
                                    maybe (return ()) (T.putStrLn . (T.pack "Name: " `T.append`)) $ idName idt
                                    BC.putStrLn . (BC.pack "KeyId: " `BC.append`) . showRefDigest . refDigest . storedRef $ idKeyIdentity idt
                                    BC.putStrLn . (BC.pack "KeyMsg: " `BC.append`) . showRefDigest . refDigest . storedRef $ idKeyMessage idt
                                    case idOwner idt of
                                         Nothing -> return ()
                                         Just owner -> do
                                             mapM_ (putStrLn . ("OWNER " ++) . BC.unpack . showRefDigest . refDigest . storedRef) $ idDataF owner
                                             disp owner
                            disp identity
                        Nothing -> putStrLn $ "Identity verification failed"
                    _ -> error $ "unknown object type '" ++ objtype ++ "'"

        ["show-generation", sref] -> readRef st (BC.pack sref) >>= \case
            Nothing -> error "ref does not exist"
            Just ref -> print $ storedGeneration (wrappedLoad ref :: Stored Object)

        ["update-identity"] -> either fail return <=< runExceptT $ do
            runReaderT updateSharedIdentity =<< loadLocalStateHead st

        ("update-identity" : srefs) -> do
            sequence <$> mapM (readRef st . BC.pack) srefs >>= \case
                Nothing -> error "ref does not exist"
                Just refs
                    | Just idt <- validateIdentityF $ map wrappedLoad refs -> do
                        BC.putStrLn . showRefDigest . refDigest . storedRef . idData =<<
                            (either fail return <=< runExceptT $ runReaderT (interactiveIdentityUpdate idt) st)
                    | otherwise -> error "invalid identity"

        ["test"] -> runTestTool st

        args -> case getOpt Permute (options ++ servicesOptions) args of
            (o, [], []) -> do
                let opts = foldl (flip id) defaultOptions o
                    header = "Usage: erebos [OPTION...]"
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
            (_, _, errs) -> do
                progName <- getProgName
                hPutStrLn stderr $ concat errs <> "Try `" <> progName <> " --help' for more information."
                exitFailure


inputSettings :: Settings IO
inputSettings = setComplete commandCompletion $ defaultSettings

interactiveLoop :: Storage -> Options -> IO ()
interactiveLoop st opts = runInputT inputSettings $ do
    erebosHead <- liftIO $ loadLocalStateHead st
    outputStrLn $ T.unpack $ displayIdentity $ headLocalIdentity erebosHead

    tui <- haveTerminalUI
    extPrint <- getExternalPrint
    let extPrintLn str = extPrint $ case reverse str of ('\n':_) -> str
                                                        _ -> str ++ "\n";

    let getInputLinesTui eprompt = do
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
            Just input <- lift $ getInputLine prompt
            case reverse input of
                 _ | all isSpace input -> getInputLinesTui eprompt
                 '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLinesTui (Right ">> ")
                 _         -> return input

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
            lift (getInputLine "") >>= \case
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

    watched <- case optChatroomAutoSubscribe opts of
        auto@(Just _) -> fmap Just $ liftIO $ watchChatroomsForCli extPrintLn erebosHead chatroomSetVar contextOptions auto
        Nothing       -> return Nothing

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

    let process :: CommandState -> MaybeT (InputT IO) CommandState
        process cstate = do
            (cmd, line) <- getInputCommand cstate
            h <- liftIO (reloadHead $ csHead cstate) >>= \case
                Just h  -> return h
                Nothing -> do lift $ lift $ extPrintLn "current head deleted"
                              mzero
            res <- liftIO $ runExceptT $ flip execStateT cstate { csHead = h } $ runReaderT cmd CommandInput
                { ciServer = server
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
                    lift $ lift $ extPrintLn $ "Error: " ++ err
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

newtype CommandM a = CommandM (ReaderT CommandInput (StateT CommandState (ExceptT String IO)) a)
    deriving (Functor, Applicative, Monad, MonadReader CommandInput, MonadState CommandState, MonadError String)

instance MonadFail CommandM where
    fail = throwError

instance MonadIO CommandM where
    liftIO act = CommandM (liftIO (try act)) >>= \case
        Left (e :: SomeException) -> throwError (show e)
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
    _ -> throwError "no peer selected"

getSelectedConversation :: CommandM Conversation
getSelectedConversation = gets csContext >>= \case
    SelectedPeer peer -> peerIdentity peer >>= \case
        PeerIdentityFull pid -> directMessageConversation $ finalOwner pid
        _ -> throwError "incomplete peer identity"
    SelectedContact contact -> case contactIdentity contact of
        Just cid -> directMessageConversation cid
        Nothing -> throwError "contact without erebos identity"
    SelectedChatroom rstate ->
        chatroomConversation rstate >>= \case
            Just conv -> return conv
            Nothing -> throwError "invalid chatroom"
    SelectedConversation conv -> reloadConversation conv
    _ -> throwError "no contact, peer or conversation selected"

commands :: [(String, Command)]
commands =
    [ ("history", cmdHistory)
    , ("peers", cmdPeers)
    , ("peer-add", cmdPeerAdd)
    , ("peer-add-public", cmdPeerAddPublic)
    , ("peer-drop", cmdPeerDrop)
    , ("send", cmdSend)
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
#ifdef ENABLE_ICE_SUPPORT
    , ("discovery-init", cmdDiscoveryInit)
    , ("discovery", cmdDiscovery)
    , ("ice-create", cmdIceCreate)
    , ("ice-destroy", cmdIceDestroy)
    , ("ice-show", cmdIceShow)
    , ("ice-connect", cmdIceConnect)
    , ("ice-send", cmdIceSend)
#endif
    , ("select", cmdSelectContext)
    , ("quit", cmdQuit)
    ]

commandCompletion :: CompletionFunc IO
commandCompletion = completeWordWithPrev Nothing [ ' ', '\t', '\n', '\r' ] $ curry $ \case
    ([], '/':pref) -> return . map (simpleCompletion . ('/':)) . filter (pref `isPrefixOf`) $ sortedCommandNames
    _ -> return []
  where
    sortedCommandNames = sort $ map fst commands


cmdUnknown :: String -> Command
cmdUnknown cmd = liftIO $ putStrLn $ "Unknown command: " ++ cmd

cmdPeers :: Command
cmdPeers = do
    peers <- join $ asks ciPeers
    set <- asks ciSetContextOptions
    set $ map (SelectedPeer . fst) peers
    forM_ (zip [1..] peers) $ \(i :: Int, (_, name)) -> do
        liftIO $ putStrLn $ "[" ++ show i ++ "] " ++ name

cmdPeerAdd :: Command
cmdPeerAdd = void $ do
    server <- asks ciServer
    (hostname, port) <- (words <$> asks ciLine) >>= \case
        hostname:p:_ -> return (hostname, p)
        [hostname] -> return (hostname, show discoveryPort)
        [] -> throwError "missing peer address"
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just hostname) (Just port)
    liftIO $ serverPeer server (addrAddress addr)

cmdPeerAddPublic :: Command
cmdPeerAddPublic = do
    server <- asks ciServer
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just "discovery1.erebosprotocol.net") (Just (show discoveryPort))
    void $ liftIO $ serverPeer server (addrAddress addr)

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

cmdSelectContext :: Command
cmdSelectContext = do
    n <- read <$> asks ciLine
    join (asks ciContextOptions) >>= \ctxs -> if
        | n > 0, (ctx : _) <- drop (n - 1) ctxs -> modify $ \s -> s { csContext = ctx }
        | otherwise -> throwError "invalid index"

cmdSend :: Command
cmdSend = void $ do
    text <- asks ciLine
    conv <- getSelectedConversation
    sendMessage conv (T.pack text) >>= \case
        Just msg -> do
            tzone <- liftIO $ getCurrentTimeZone
            liftIO $ putStrLn $ formatMessage tzone msg
        Nothing -> return ()

cmdHistory :: Command
cmdHistory = void $ do
    conv <- getSelectedConversation
    case conversationHistory conv of
        thread@(_:_) -> do
            tzone <- liftIO $ getCurrentTimeZone
            liftIO $ mapM_ (putStrLn . formatMessage tzone) $ reverse $ take 50 thread
        [] -> do
            liftIO $ putStrLn $ "<empty history>"

cmdUpdateIdentity :: Command
cmdUpdateIdentity = void $ do
    runReaderT updateSharedIdentity =<< gets csHead

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
            let chatroomList = fromSetBy (comparing roomStateData) set
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
                             Left err -> eprint err

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
                                , ": "
                                , maybe "<no message>" T.unpack $ cmsgText msg
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
    chatroomList <- fromSetBy (comparing roomStateData) <$> liftIO (readMVar chatroomSetVar)
    set <- asks ciSetContextOptions
    set $ map SelectedChatroom chatroomList
    forM_ (zip [1..] chatroomList) $ \(i :: Int, rstate) -> do
        liftIO $ putStrLn $ "[" ++ show i ++ "] " ++ maybe "<unnamed>" T.unpack (roomName =<< roomStateRoom rstate)

cmdChatroomCreatePublic :: Command
cmdChatroomCreatePublic = do
    name <- asks ciLine >>= \case
        line | not (null line) -> return $ T.pack line
        _ -> liftIO $ do
            T.putStr $ T.pack "Name: "
            hFlush stdout
            T.getLine

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
    forM_ (zip [1..] contacts) $ \(i :: Int, c) -> liftIO $ do
        T.putStrLn $ T.concat
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
        liftIO $ putStrLn $ "[" ++ show i ++ "] " ++ T.unpack (conversationName conv)

cmdDetails :: Command
cmdDetails = do
    gets csContext >>= \case
        SelectedPeer peer -> do
            liftIO $ putStr $ unlines
                [ "Network peer:"
                , "  " <> show (peerAddress peer)
                ]
            peerIdentity peer >>= \case
                PeerIdentityUnknown _ -> liftIO $ do
                    putStrLn $ "unknown identity"
                PeerIdentityRef wref _ -> liftIO $ do
                    putStrLn $ "Identity ref:"
                    putStrLn $ "  " <> BC.unpack (showRefDigest $ wrDigest wref)
                PeerIdentityFull pid -> printContactOrIdentityDetails pid

        SelectedContact contact -> do
            printContactDetails contact

        SelectedChatroom rstate -> do
            liftIO $ putStrLn $ "Chatroom: " <> (T.unpack $ fromMaybe (T.pack "<unnamed>") $ roomName =<< roomStateRoom rstate)

        SelectedConversation conv -> do
            case conversationPeer conv of
                Just pid -> printContactOrIdentityDetails pid
                Nothing -> liftIO $ putStrLn $ "(conversation without peer)"

        NoContext -> liftIO $ putStrLn "nothing selected"
  where
    printContactOrIdentityDetails cid = do
        contacts <- fromSetBy (comparing contactName) . lookupSharedValue . lsShared . fromStored <$> getLocalHead
        case find (maybe False (sameIdentity cid) . contactIdentity) contacts of
            Just contact -> printContactDetails contact
            Nothing -> printIdentityDetails cid

    printContactDetails contact = liftIO $ do
        putStrLn $ "Contact:"
        prefix <- case contactCustomName contact of
            Just name -> do
                putStrLn $ "  " <> T.unpack name
                return $ Just "alias of"
            Nothing -> do
                return $ Nothing

        case contactIdentity contact of
            Just cid -> do
                printIdentityDetailsBody prefix cid
            Nothing -> do
                putStrLn $ "  (without erebos identity)"

    printIdentityDetails identity = liftIO $ do
        putStrLn $ "Identity:"
        printIdentityDetailsBody Nothing identity

    printIdentityDetailsBody prefix identity = do
        forM_ (zip (False : repeat True) $ unfoldOwners identity) $ \(owned, cpid) -> do
            putStrLn $ unwords $ concat
                [ [ "  " ]
                , if owned then [ "owned by" ] else maybeToList prefix
                , [ maybe "<unnamed>" T.unpack (idName cpid) ]
                , map (BC.unpack . showRefDigest . refDigest . storedRef) $ idExtDataF cpid
                ]

#ifdef ENABLE_ICE_SUPPORT

cmdDiscoveryInit :: Command
cmdDiscoveryInit = void $ do
    server <- asks ciServer

    (hostname, port) <- (words <$> asks ciLine) >>= return . \case
        hostname:p:_ -> (hostname, p)
        [hostname] -> (hostname, show discoveryPort)
        [] -> ("discovery.erebosprotocol.net", show discoveryPort)
    addr:_ <- liftIO $ getAddrInfo (Just $ defaultHints { addrSocketType = Datagram }) (Just hostname) (Just port)
    peer <- liftIO $ serverPeer server (addrAddress addr)
    sendToPeer peer $ DiscoverySelf (T.pack "ICE") 0
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

cmdIceCreate :: Command
cmdIceCreate = do
    role <- asks ciLine >>= return . \case
        'm':_ -> PjIceSessRoleControlling
        's':_ -> PjIceSessRoleControlled
        _ -> PjIceSessRoleUnknown
    eprint <- asks ciPrint
    sess <- liftIO $ iceCreate role $ eprint <=< iceShow
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
    let loadInfo = BC.getLine >>= \case line | BC.null line -> return []
                                             | otherwise    -> (line:) <$> loadInfo
    Right remote <- liftIO $ do
        st <- memoryStorage
        pst <- derivePartialStorage st
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

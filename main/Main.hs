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
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.IO as T
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
    , optShowHelp = False
    , optShowVersion = False
    }

availableServices :: [ServiceOption]
availableServices =
    [ ServiceOption "attach" (someService @AttachService Proxy)
        True "attach (to) other devices"
    , ServiceOption "sync" (someService @SyncService Proxy)
        True "synchronization with attached devices"
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
                    padOpt = padTo 28
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

    haveTerminalUI >>= \case True -> return ()
                             False -> error "Requires terminal"
    extPrint <- getExternalPrint
    let extPrintLn str = extPrint $ case reverse str of ('\n':_) -> str
                                                        _ -> str ++ "\n";

    _ <- liftIO $ do
        tzone <- getCurrentTimeZone
        watchReceivedMessages erebosHead $
            extPrintLn . formatDirectMessage tzone . fromStored

    server <- liftIO $ do
        startServer (optServer opts) erebosHead extPrintLn $
            map soptService $ filter soptEnabled $ optServices opts

    peers <- liftIO $ newMVar []
    contextOptions <- liftIO $ newMVar []

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
                when (Just shown /= op) $ extPrint $ "[" <> show idx <> "] PEER " <> updateType' <> " " <> shown
            _ -> return ()

    let getInputLines prompt = do
            Just input <- lift $ getInputLine prompt
            case reverse input of
                 _ | all isSpace input -> getInputLines prompt
                 '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLines ">> "
                 _         -> return input

    let process :: CommandState -> MaybeT (InputT IO) CommandState
        process cstate = do
            pname <- case csContext cstate of
                        NoContext -> return ""
                        SelectedPeer peer -> peerIdentity peer >>= return . \case
                            PeerIdentityFull pid -> maybe "<unnamed>" T.unpack $ idName $ finalOwner pid
                            PeerIdentityRef wref _ -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                            PeerIdentityUnknown _  -> "<unknown>"
                        SelectedContact contact -> return $ T.unpack $ contactName contact
                        SelectedConversation conv -> return $ T.unpack $ conversationName conv
            input <- getInputLines $ pname ++ "> "
            let (CommandM cmd, line) = case input of
                    '/':rest -> let (scmd, args) = dropWhile isSpace <$> span (\c -> isAlphaNum c || c == '-') rest
                                 in if not (null scmd) && all isDigit scmd
                                       then (cmdSelectContext $ read scmd, args)
                                       else (fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args)
                    _        -> (cmdSend, input)
            h <- liftIO (reloadHead $ csHead cstate) >>= \case
                Just h  -> return h
                Nothing -> do lift $ lift $ extPrint "current head deleted"
                              mzero
            res <- liftIO $ runExceptT $ flip execStateT cstate { csHead = h } $ runReaderT cmd CommandInput
                { ciServer = server
                , ciLine = line
                , ciPrint = extPrintLn
                , ciPeers = liftIO $ modifyMVar peers $ \ps -> do
                    ps' <- filterM (fmap not . isPeerDropped . fst) ps
                    return (ps', ps')
                , ciContextOptions = liftIO $ readMVar contextOptions
                , ciSetContextOptions = \ctxs -> liftIO $ modifyMVar_ contextOptions $ const $ return ctxs
                }
            case res of
                 Right cstate' -> return cstate'
                 Left err -> do lift $ lift $ extPrint $ "Error: " ++ err
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
        }


data CommandInput = CommandInput
    { ciServer :: Server
    , ciLine :: String
    , ciPrint :: String -> IO ()
    , ciPeers :: CommandM [(Peer, String)]
    , ciContextOptions :: CommandM [CommandContext]
    , ciSetContextOptions :: [CommandContext] -> Command
    }

data CommandState = CommandState
    { csHead :: Head LocalState
    , csContext :: CommandContext
#ifdef ENABLE_ICE_SUPPORT
    , csIceSessions :: [IceSession]
#endif
    , csIcePeer :: Maybe Peer
    }

data CommandContext = NoContext
                    | SelectedPeer Peer
                    | SelectedContact Contact
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
    SelectedConversation conv -> reloadConversation conv
    _ -> throwError "no contact, peer or conversation selected"

commands :: [(String, Command)]
commands =
    [ ("history", cmdHistory)
    , ("peers", cmdPeers)
    , ("peer-add", cmdPeerAdd)
    , ("peer-drop", cmdPeerDrop)
    , ("send", cmdSend)
    , ("update-identity", cmdUpdateIdentity)
    , ("attach", cmdAttach)
    , ("attach-accept", cmdAttachAccept)
    , ("attach-reject", cmdAttachReject)
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

cmdSelectContext :: Int -> Command
cmdSelectContext n = join (asks ciContextOptions) >>= \ctxs -> if
    | n > 0, (ctx : _) <- drop (n - 1) ctxs -> modify $ \s -> s { csContext = ctx }
    | otherwise -> throwError "invalid index"

cmdSend :: Command
cmdSend = void $ do
    text <- asks ciLine
    conv <- getSelectedConversation
    msg <- sendMessage conv $ T.pack text
    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ putStrLn $ formatMessage tzone msg

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

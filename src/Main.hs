module Main (main) where

import Control.Arrow (first)
import Control.Concurrent
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
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Time.LocalTime
import Data.Typeable

import Network.Socket

import System.Console.GetOpt
import System.Console.Haskeline
import System.Environment

import Attach
import Contact
import Discovery
import ICE
import Identity
import Message
import Network
import PubKey
import Service
import State
import Storage
import Storage.Merge
import Sync

data Options = Options
    { optServer :: ServerOptions
    }

defaultOptions :: Options
defaultOptions = Options
    { optServer = defaultServerOptions
    }

options :: [OptDescr (Options -> Options)]
options =
    [ Option ['p'] ["port"]
        (ReqArg (\p -> so $ \opts -> opts { serverPort = read p }) "PORT")
        "local port to bind"
    , Option ['s'] ["silent"]
        (NoArg (so $ \opts -> opts { serverLocalDiscovery = False }))
        "do not send announce packets for local discovery"
    ]
    where so f opts = opts { optServer = f $ optServer opts }

main :: IO ()
main = do
    st <- liftIO $ openStorage . fromMaybe "./.erebos" =<< lookupEnv "EREBOS_DIR"
    getArgs >>= \case
        ["cat-file", sref] -> do
            readRef st (BC.pack sref) >>= \case
                Nothing -> error "ref does not exist"
                Just ref -> BL.putStr $ lazyLoadBytes ref

        ["cat-file", objtype, sref] -> do
            readRef st (BC.pack sref) >>= \case
                Nothing -> error "ref does not exist"
                Just ref -> case objtype of
                    "signed" -> do
                        let signed = load ref :: Signed Object
                        BL.putStr $ lazyLoadBytes $ storedRef $ signedData signed
                        forM_ (signedSignature signed) $ \sig -> do
                            putStr $ "SIG "
                            BC.putStrLn $ showRef $ storedRef $ sigKey $ fromStored sig
                    "identity" -> case validateIdentity (wrappedLoad ref) of
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

        ["update-identity"] -> runReaderT updateSharedIdentity =<< loadLocalStateHead st

        ("update-identity" : srefs) -> do
            sequence <$> mapM (readRef st . BC.pack) srefs >>= \case
                Nothing -> error "ref does not exist"
                Just refs
                    | Just idt <- validateIdentityF $ map wrappedLoad refs -> do
                        BC.putStrLn . showRefDigest . refDigest . storedRef . idData =<< interactiveIdentityUpdate idt
                    | otherwise -> error "invalid identity"

        args -> do
            opts <- case getOpt Permute options args of
                (o, [], []) -> return (foldl (flip id) defaultOptions o)
                (_, _, errs) -> ioError (userError (concat errs ++ usageInfo header options))
                    where header = "Usage: erebos [OPTION...]"
            interactiveLoop st opts


interactiveLoop :: Storage -> Options -> IO ()
interactiveLoop st opts = runInputT defaultSettings $ do
    erebosHead <- liftIO $ loadLocalStateHead st
    outputStrLn $ T.unpack $ displayIdentity $ headLocalIdentity erebosHead

    haveTerminalUI >>= \case True -> return ()
                             False -> error "Requires terminal"
    extPrint <- getExternalPrint
    let extPrintLn str = extPrint $ case reverse str of ('\n':_) -> str
                                                        _ -> str ++ "\n";
    server <- liftIO $ do
        startServer (optServer opts) erebosHead extPrintLn
            [ someService @AttachService Proxy
            , someService @SyncService Proxy
            , someService @ContactService Proxy
            , someService @DirectMessage Proxy
            , someService @DiscoveryService Proxy
            ]

    peers <- liftIO $ newMVar []

    void $ liftIO $ forkIO $ void $ forever $ do
        peer <- getNextPeerChange server
        peerIdentity peer >>= \case
            pid@(PeerIdentityFull _) -> do
                let shown = showPeer pid $ peerAddress peer
                let update [] = ([(peer, shown)], Nothing)
                    update ((p,s):ps) | p == peer = ((peer, shown) : ps, Just s)
                                      | otherwise = first ((p,s):) $ update ps
                op <- modifyMVar peers (return . update)
                when (Just shown /= op) $ extPrint shown
            _ -> return ()

    let getInputLines prompt = do
            Just input <- lift $ getInputLine prompt
            case reverse input of
                 _ | all isSpace input -> getInputLines prompt
                 '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLines ">> "
                 _         -> return input

    let process :: CommandState -> MaybeT (InputT IO) CommandState
        process cstate = do
            pname <- case csPeer cstate of
                        Nothing -> return ""
                        Just peer -> peerIdentity peer >>= return . \case
                            PeerIdentityFull pid -> maybe "<unnamed>" T.unpack $ idName $ finalOwner pid
                            PeerIdentityRef wref _ -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                            PeerIdentityUnknown _  -> "<unknown>"
            input <- getInputLines $ pname ++ "> "
            let (CommandM cmd, line) = case input of
                    '/':rest -> let (scmd, args) = dropWhile isSpace <$> span (\c -> isAlphaNum c || c == '-') rest
                                 in if all isDigit scmd
                                       then (cmdSetPeer $ read scmd, args)
                                       else (fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args)
                    _        -> (cmdSend, input)
            h <- liftIO (reloadHead erebosHead) >>= \case
                Just h  -> return h
                Nothing -> do lift $ lift $ extPrint "current head deleted"
                              mzero
            res <- liftIO $ runExceptT $ flip execStateT cstate $ runReaderT cmd CommandInput
                { ciHead = h
                , ciServer = server
                , ciLine = line
                , ciPrint = extPrintLn
                , ciPeers = liftIO $ readMVar peers
                }
            case res of
                 Right cstate' -> return cstate'
                 Left err -> do lift $ lift $ extPrint $ "Error: " ++ err
                                return cstate

    let loop (Just cstate) = runMaybeT (process cstate) >>= loop
        loop Nothing = return ()
    loop $ Just $ CommandState
        { csPeer = Nothing
        , csIceSessions = []
        , csIcePeer = Nothing
        }


data CommandInput = CommandInput
    { ciHead :: Head LocalState
    , ciServer :: Server
    , ciLine :: String
    , ciPrint :: String -> IO ()
    , ciPeers :: CommandM [(Peer, String)]
    }

data CommandState = CommandState
    { csPeer :: Maybe Peer
    , csIceSessions :: [IceSession]
    , csIcePeer :: Maybe Peer
    }

newtype CommandM a = CommandM (ReaderT CommandInput (StateT CommandState (ExceptT String IO)) a)
    deriving (Functor, Applicative, Monad, MonadIO, MonadReader CommandInput, MonadState CommandState, MonadError String)

instance MonadFail CommandM where
    fail = throwError

instance MonadRandom CommandM where
    getRandomBytes = liftIO . getRandomBytes

type Command = CommandM ()

commands :: [(String, Command)]
commands =
    [ ("history", cmdHistory)
    , ("peers", cmdPeers)
    , ("send", cmdSend)
    , ("update-identity", cmdUpdateIdentity)
    , ("attach", cmdAttach)
    , ("attach-accept", cmdAttachAccept)
    , ("contacts", cmdContacts)
    , ("contact-add", cmdContactAdd)
    , ("contact-accept", cmdContactAccept)
    , ("discovery-init", cmdDiscoveryInit)
    , ("discovery", cmdDiscovery)
    , ("ice-create", cmdIceCreate)
    , ("ice-destroy", cmdIceDestroy)
    , ("ice-show", cmdIceShow)
    , ("ice-connect", cmdIceConnect)
    , ("ice-send", cmdIceSend)
    ]

cmdUnknown :: String -> Command
cmdUnknown cmd = liftIO $ putStrLn $ "Unknown command: " ++ cmd

cmdPeers :: Command
cmdPeers = do
    peers <- join $ asks ciPeers
    forM_ (zip [1..] peers) $ \(i :: Int, (_, name)) -> do
        liftIO $ putStrLn $ show i ++ ": " ++ name

showPeer :: PeerIdentity -> PeerAddress -> String
showPeer pidentity paddr =
    let name = case pidentity of
                    PeerIdentityUnknown _  -> "<noid>"
                    PeerIdentityRef wref _ -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                    PeerIdentityFull pid   -> T.unpack $ displayIdentity pid
     in name ++ " [" ++ show paddr ++ "]"

cmdSetPeer :: Int -> Command
cmdSetPeer n | n < 1     = liftIO $ putStrLn "Invalid peer index"
             | otherwise = do peers <- join $ asks ciPeers
                              modify $ \s -> s { csPeer = fmap fst $ listToMaybe $ drop (n - 1) peers }

cmdSend :: Command
cmdSend = void $ do
    ehead <- asks ciHead
    Just peer <- gets csPeer
    text <- asks ciLine
    smsg <- sendDirectMessage ehead peer $ T.pack text
    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ putStrLn $ formatMessage tzone $ fromStored smsg

cmdHistory :: Command
cmdHistory = void $ do
    ehead <- asks ciHead
    Just peer <- gets csPeer
    PeerIdentityFull pid <- peerIdentity peer
    let powner = finalOwner pid

    case find (sameIdentity powner . msgPeer) $
            messageThreadView $ lookupSharedValue $ lsShared $ headObject ehead of
        Just thread -> do
            tzone <- liftIO $ getCurrentTimeZone
            liftIO $ mapM_ (putStrLn . formatMessage tzone) $ reverse $ take 50 $ threadToList thread
        Nothing -> do
            liftIO $ putStrLn $ "<empty history>"

cmdUpdateIdentity :: Command
cmdUpdateIdentity = void $ do
    runReaderT updateSharedIdentity =<< asks ciHead

cmdAttach :: Command
cmdAttach = join $ attachToOwner
    <$> asks ciPrint
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

cmdAttachAccept :: Command
cmdAttachAccept = join $ attachAccept
    <$> asks ciPrint
    <*> asks ciHead
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

cmdContacts :: Command
cmdContacts = do
    args <- words <$> asks ciLine
    ehead <- asks ciHead
    let contacts = contactView $ lookupSharedValue $ lsShared $ headObject ehead
        verbose = "-v" `elem` args
    forM_ (zip [1..] contacts) $ \(i :: Int, c) -> do
        liftIO $ putStrLn $ show i ++ ": " ++ T.unpack (displayIdentity $ contactIdentity c) ++
            (if verbose then " " ++ (unwords $ map (BC.unpack . showRef . storedRef) $ idDataF $ contactIdentity c) else "")

cmdContactAdd :: Command
cmdContactAdd = join $ contactRequest
    <$> asks ciPrint
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

cmdContactAccept :: Command
cmdContactAccept = join $ contactAccept
    <$> asks ciPrint
    <*> asks ciHead
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

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
    st <- asks (storedStorage . headStoredObject . ciHead)
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

module Main (main) where

import Control.Arrow (first)
import Control.Concurrent
import Control.Monad
import Control.Monad.Except
import Control.Monad.Fail
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

import System.Console.Haskeline
import System.Environment

import Attach
import Contact
import Identity
import Message
import Network
import PubKey
import Service
import State
import Storage
import Storage.Merge
import Sync

main :: IO ()
main = do
    st <- liftIO $ openStorage "test"
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

        ["update-identity"] -> updateSharedIdentity =<< loadLocalStateHead st

        ("update-identity" : srefs) -> do
            sequence <$> mapM (readRef st . BC.pack) srefs >>= \case
                Nothing -> error "ref does not exist"
                Just refs
                    | Just idt <- validateIdentityF $ map wrappedLoad refs -> do
                        BC.putStrLn . showRefDigest . refDigest . storedRef . idData =<< interactiveIdentityUpdate idt
                    | otherwise -> error "invalid identity"

        [bhost] -> interactiveLoop st bhost
        _       -> error "Expecting broadcast address"

interactiveLoop :: Storage -> String -> IO ()
interactiveLoop st bhost = runInputT defaultSettings $ do
    erebosHead <- liftIO $ loadLocalStateHead st
    outputStrLn $ T.unpack $ displayIdentity $ headLocalIdentity erebosHead

    haveTerminalUI >>= \case True -> return ()
                             False -> error "Requires terminal"
    extPrint <- getExternalPrint
    let extPrintLn str = extPrint $ str ++ "\n";
    server <- liftIO $ do
        startServer erebosHead extPrintLn bhost
            [ SomeService @AttachService Proxy
            , SomeService @SyncService Proxy
            , SomeService @ContactService Proxy
            , SomeService @DirectMessage Proxy
            ]

    peers <- liftIO $ newMVar []

    void $ liftIO $ forkIO $ void $ forever $ do
        peer <- readChan $ serverChanPeer server
        if | PeerIdentityFull pid <- peerIdentity peer -> do
                 let update [] = ([peer], Nothing)
                     update (p:ps) | PeerIdentityFull pid' <- peerIdentity p
                                   , pid' `sameIdentity` pid = (peer : ps, Just p)
                                   | otherwise               = first (p:) $ update ps
                 op <- modifyMVar peers (return . update)
                 let shown = showPeer peer
                 when (Just shown /= (showPeer <$> op)) $ extPrint shown
           | otherwise -> return ()

    let getInputLines prompt = do
            Just input <- lift $ getInputLine prompt
            case reverse input of
                 '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLines ">> "
                 _         -> return input

    let process :: CommandState -> MaybeT (InputT IO) CommandState
        process cstate = do
            let pname = case csPeer cstate of
                             Nothing -> ""
                             Just peer -> case peerIdentity peer of
                                 PeerIdentityFull pid -> maybe "<unnamed>" T.unpack $ idName $ finalOwner pid
                                 PeerIdentityRef wref -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                                 PeerIdentityUnknown  -> "<unknown>"
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
    loop $ Just $ CommandState { csPeer = Nothing }


data CommandInput = CommandInput
    { ciHead :: Head LocalState
    , ciServer :: Server
    , ciLine :: String
    , ciPrint :: String -> IO ()
    , ciPeers :: CommandM [Peer]
    }

data CommandState = CommandState
    { csPeer :: Maybe Peer
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
    ]

cmdUnknown :: String -> Command
cmdUnknown cmd = liftIO $ putStrLn $ "Unknown command: " ++ cmd

cmdPeers :: Command
cmdPeers = do
    peers <- join $ asks ciPeers
    forM_ (zip [1..] peers) $ \(i :: Int, p) -> do
        liftIO $ putStrLn $ show i ++ ": " ++ showPeer p

showPeer :: Peer -> String
showPeer peer =
    let name = case peerIdentity peer of
                    PeerIdentityUnknown  -> "<noid>"
                    PeerIdentityRef wref -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                    PeerIdentityFull pid -> T.unpack $ displayIdentity pid
        DatagramAddress addr = peerAddress peer
     in name ++ " [" ++ show addr ++ "]"

cmdSetPeer :: Int -> Command
cmdSetPeer n | n < 1     = liftIO $ putStrLn "Invalid peer index"
             | otherwise = do peers <- join $ asks ciPeers
                              modify $ \s -> s { csPeer = listToMaybe $ drop (n - 1) peers }

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
    PeerIdentityFull pid <- return $ peerIdentity peer
    let powner = finalOwner pid

    Just thread <- return $ find (sameIdentity powner . msgPeer) $
        messageThreadView $ lookupSharedValue $ lsShared $ headObject ehead
    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ mapM_ (putStrLn . formatMessage tzone) $ reverse $ take 50 $ threadToList thread

cmdUpdateIdentity :: Command
cmdUpdateIdentity = void $ do
    ehead <- asks ciHead
    liftIO $ updateSharedIdentity ehead

cmdAttach :: Command
cmdAttach = join $ attachToOwner
    <$> asks ciPrint
    <*> asks (headLocalIdentity . ciHead)
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

cmdAttachAccept :: Command
cmdAttachAccept = join $ attachAccept
    <$> asks ciPrint
    <*> asks ciHead
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

cmdContacts :: Command
cmdContacts = do
    ehead <- asks ciHead
    let contacts = contactView $ lookupSharedValue $ lsShared $ headObject ehead
    forM_ (zip [1..] contacts) $ \(i :: Int, c) -> do
        liftIO $ putStrLn $ show i ++ ": " ++ T.unpack (displayIdentity $ contactIdentity c)

cmdContactAdd :: Command
cmdContactAdd = join $ contactRequest
    <$> asks ciPrint
    <*> asks (headLocalIdentity . ciHead)
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

cmdContactAccept :: Command
cmdContactAccept = join $ contactAccept
    <$> asks ciPrint
    <*> asks ciHead
    <*> (maybe (throwError "no peer selected") return =<< gets csPeer)

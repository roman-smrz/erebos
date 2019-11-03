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
import Data.Time.LocalTime

import System.Console.Haskeline
import System.Environment

import Identity
import Message
import Message.Service
import Network
import PubKey
import Service
import State
import Storage

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
                    _ -> error $ "unknown object type '" ++ objtype ++ "'"

        ["update-identity"] -> updateIdentity st

        [bhost] -> interactiveLoop st bhost
        _       -> error "Expecting broadcast address"

interactiveLoop :: Storage -> String -> IO ()
interactiveLoop st bhost = runInputT defaultSettings $ do
    erebosHead <- liftIO $ loadLocalState st
    let serebos = wrappedLoad (headRef erebosHead) :: Stored LocalState
        Just self = verifyIdentity $ lsIdentity $ fromStored serebos
    outputStrLn $ T.unpack $ displayIdentity self

    haveTerminalUI >>= \case True -> return ()
                             False -> error "Requires terminal"
    extPrint <- getExternalPrint
    let extPrintLn str = extPrint $ str ++ "\n";
    chanPeer <- liftIO $
        startServer extPrintLn bhost self
            [ (T.pack "dmsg", SomeService (emptyServiceState :: DirectMessageService))
            ]

    peers <- liftIO $ newMVar []

    void $ liftIO $ forkIO $ void $ forever $ do
        peer <- readChan chanPeer
        let update [] = ([peer], Nothing)
            update (p:ps) | peerIdentityRef p == peerIdentityRef peer = (peer : ps, Just p)
                          | otherwise                                 = first (p:) $ update ps
        if | PeerIdentityUnknown <- peerIdentity peer -> return ()
           | otherwise -> do
                 op <- modifyMVar peers (return . update)
                 let shown = showPeer peer
                 when (Just shown /= (showPeer <$> op)) $ extPrint shown

    let getInputLines prompt = do
            Just input <- lift $ getInputLine prompt
            case reverse input of
                 '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLines ">> "
                 _         -> return input

    let process :: CommandState -> MaybeT (InputT IO) CommandState
        process cstate = do
            let pname = case csPeer cstate of
                             Nothing -> ""
                             Just peer -> case peerOwner peer of
                                 PeerIdentityFull pid -> maybe "<unnamed>" T.unpack $ idName pid
                                 PeerIdentityRef wref -> "<" ++ BC.unpack (showRefDigest $ wrDigest wref) ++ ">"
                                 PeerIdentityUnknown  -> "<unknown>"
            input <- getInputLines $ pname ++ "> "
            let (CommandM cmd, line) = case input of
                    '/':rest -> let (scmd, args) = dropWhile isSpace <$> span (\c -> isAlphaNum c || c == '-') rest
                                 in if all isDigit scmd
                                       then (cmdSetPeer $ read scmd, args)
                                       else (fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args)
                    _        -> (cmdSend, input)
            res <- liftIO $ runExceptT $ flip execStateT cstate $ runReaderT cmd CommandInput
                { ciSelf = self
                , ciLine = line
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
    { ciSelf :: UnifiedIdentity
    , ciLine :: String
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
    self <- asks ciSelf
    let st = storedStorage $ idData self
    Just peer <- gets csPeer
    PeerIdentityFull powner <- return $ peerOwner peer
    text <- asks ciLine
    smsg <- liftIO $ updateLocalState st $ \erb -> do
        (slist, smsg) <- case find ((== idData powner) . msgPeer . fromStored) (storedFromSList $ lsMessages $ fromStored erb) of
            Just thread -> do
                (smsg, thread') <- createDirectMessage self (fromStored thread) (T.pack text)
                (,smsg) <$> slistReplaceS thread thread' (lsMessages $ fromStored erb)
            Nothing -> do
                (smsg, thread') <- createDirectMessage self (emptyDirectThread powner) (T.pack text)
                (,smsg) <$> slistAddS thread' (lsMessages $ fromStored erb)
        erb' <- wrappedStore st (fromStored erb) { lsMessages = slist }
        return (erb', smsg)
    sendToPeer self peer (T.pack "dmsg") smsg

    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ putStrLn $ formatMessage tzone $ fromStored smsg

cmdHistory :: Command
cmdHistory = void $ do
    self <- asks ciSelf
    let st = storedStorage $ idData self
    Just peer <- gets csPeer
    PeerIdentityFull powner <- return $ peerOwner peer

    Just erebosHead <- liftIO $ loadHead st "erebos"
    let erebos = wrappedLoad (headRef erebosHead)
    Just thread <- return $ find ((== idData powner) . msgPeer) $ fromSList $ lsMessages $ fromStored erebos
    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ mapM_ (putStrLn . formatMessage tzone) $ reverse $ take 50 $ threadToList thread

cmdUpdateIdentity :: Command
cmdUpdateIdentity = void $ do
    st <- asks $ storedStorage . idData . ciSelf
    liftIO $ updateIdentity st

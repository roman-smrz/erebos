module Main (main) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Trans.Maybe

import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Char
import Data.List
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Time.Format
import Data.Time.LocalTime

import System.Console.Haskeline
import System.Environment
import System.IO

import Identity
import Message
import Network
import PubKey
import Storage


data Erebos = Erebos
    { erbIdentity :: Stored (Signed IdentityData)
    , erbMessages :: StoredList DirectMessageThread
    }

instance Storable Erebos where
    store' erb = storeRec $ do
        storeRef "id" $ erbIdentity erb
        storeZRef "dmsgs" $ erbMessages erb

    load' = loadRec $ Erebos
        <$> loadRef "id"
        <*> loadZRef "dmsgs"


loadErebosHead :: Storage -> IO Head
loadErebosHead st = loadHeadDef st "erebos" $ do
    putStr "Name: "
    hFlush stdout
    name <- T.getLine

    (secret, public) <- generateKeys st
    (_secretMsg, publicMsg) <- generateKeys st
    (devSecret, devPublic) <- generateKeys st
    (_devSecretMsg, devPublicMsg) <- generateKeys st

    owner <- wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
        { iddName = Just name, iddKeyMessage = Just publicMsg }
    identity <- wrappedStore st =<< signAdd devSecret =<< sign secret =<< wrappedStore st (emptyIdentityData devPublic)
        { iddOwner = Just owner, iddKeyMessage = Just devPublicMsg }

    msgs <- emptySList st
    return $ Erebos
        { erbIdentity = identity
        , erbMessages = msgs
        }

updateErebosHead_ :: Storage -> (Stored Erebos -> IO (Stored Erebos)) -> IO ()
updateErebosHead_ st f = updateErebosHead st (fmap (,()) . f)

updateErebosHead :: Storage -> (Stored Erebos -> IO (Stored Erebos, a)) -> IO a
updateErebosHead st f = do
    Just erebosHead <- loadHead st "erebos"
    (erebos, x) <- f $ wrappedLoad (headRef erebosHead)
    Right _ <- replaceHead erebos (Right erebosHead)
    return x

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

        [bhost] -> interactiveLoop st bhost
        _       -> error "Expecting broadcast address"

interactiveLoop :: Storage -> String -> IO ()
interactiveLoop st bhost = runInputT defaultSettings $ do
    erebosHead <- liftIO $ loadErebosHead st
    let serebos = wrappedLoad (headRef erebosHead) :: Stored Erebos
        Just self = verifyIdentity $ erbIdentity $ fromStored serebos
    outputStrLn $ T.unpack $ displayIdentity self

    haveTerminalUI >>= \case True -> return ()
                             False -> error "Requires terminal"
    extPrint <- getExternalPrint
    let extPrintLn str = extPrint $ str ++ "\n";
    (chanPeer, chanSvc) <- liftIO $
        startServer extPrintLn bhost self

    peers <- liftIO $ newMVar []

    void $ liftIO $ forkIO $ void $ forever $ do
        peer@Peer { peerAddress = DatagramAddress addr } <- readChan chanPeer
        extPrint $ show addr ++ "\n"
        extPrintLn $ maybe "<noid>" (T.unpack . displayIdentity) $ peerIdentity peer
        let update [] = [peer]
            update (p:ps) | peerIdentity p == peerIdentity peer = peer : ps
                          | otherwise                           = p : update ps
        when (isJust $ peerIdentity peer) $
            modifyMVar_ peers (return . update)

    tzone <- liftIO $ getCurrentTimeZone
    void $ liftIO $ forkIO $ forever $ readChan chanSvc >>= \case
        (peer, svc, ref)
            | svc == T.pack "dmsg" -> do
                let smsg = wrappedLoad ref
                    msg = fromStored smsg
                extPrintLn $ formatMessage tzone msg
                if | Just powner <- finalOwner <$> peerIdentity peer
                   , idData powner == msgFrom msg
                   -> updateErebosHead_ st $ \erb -> do
                          slist <- case find ((== idData powner) . msgPeer . fromStored) (storedFromSList $ erbMessages $ fromStored erb) of
                                        Just thread -> do thread' <- wrappedStore st (fromStored thread) { msgHead = smsg : msgHead (fromStored thread) }
                                                          slistReplaceS thread thread' $ erbMessages $ fromStored erb
                                        Nothing -> slistAdd (emptyDirectThread powner) { msgHead = [smsg] } $ erbMessages $ fromStored erb
                          wrappedStore st (fromStored erb) { erbMessages = slist }

                   | otherwise -> extPrint $ "Owner mismatch"
            | otherwise -> extPrint $ "Unknown service: " ++ T.unpack svc

    let getInputLines prompt = do
            Just input <- lift $ getInputLine prompt
            case reverse input of
                 '\\':rest -> (reverse ('\n':rest) ++) <$> getInputLines ">> "
                 _         -> return input

    let process cstate = do
            let pname = case csPeer cstate of
                             Nothing -> ""
                             Just peer -> maybe "<unnamed>" T.unpack $ idName . finalOwner <=< peerIdentity $ peer
            input <- getInputLines $ pname ++ "> "
            let (cmd, line) = case input of
                    '/':rest -> let (scmd, args) = dropWhile isSpace <$> span isAlphaNum rest
                                 in if all isDigit scmd
                                       then (cmdSetPeer $ read scmd, args)
                                       else (fromMaybe (cmdUnknown scmd) $ lookup scmd commands, args)
                    _        -> (cmdSend, input)
            liftIO $ flip execStateT cstate $ runReaderT cmd CommandInput
                { ciSelf = self
                , ciLine = line
                , ciPeers = liftIO $ readMVar peers
                }

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

type CommandM a = ReaderT CommandInput (StateT CommandState IO) a
type Command = CommandM ()

commands :: [(String, Command)]
commands =
    [ ("history", cmdHistory)
    , ("peers", cmdPeers)
    , ("send", cmdSend)
    ]

cmdUnknown :: String -> Command
cmdUnknown cmd = liftIO $ putStrLn $ "Unknown command: " ++ cmd

cmdPeers :: Command
cmdPeers = do
    peers <- join $ asks ciPeers
    forM_ (zip [1..] peers) $ \(i :: Int, p) -> do
        liftIO $ putStrLn $ show i ++ ": " ++ maybe "<noid>" (T.unpack . displayIdentity) (peerIdentity p)

cmdSetPeer :: Int -> Command
cmdSetPeer n | n < 1     = liftIO $ putStrLn "Invalid peer index"
             | otherwise = do peers <- join $ asks ciPeers
                              modify $ \s -> s { csPeer = listToMaybe $ drop (n - 1) peers }

cmdSend :: Command
cmdSend = void $ runMaybeT $ do
    self <- asks ciSelf
    let st = storedStorage $ idData self
    Just peer <- gets csPeer
    Just powner <- return $ finalOwner <$> peerIdentity peer
    _:_ <- return $ peerChannels peer
    text <- asks ciLine
    smsg <- liftIO $ updateErebosHead st $ \erb -> do
        (slist, smsg) <- case find ((== idData powner) . msgPeer . fromStored) (storedFromSList $ erbMessages $ fromStored erb) of
            Just thread -> do
                (smsg, thread') <- createDirectMessage self (fromStored thread) (T.pack text)
                (,smsg) <$> slistReplaceS thread thread' (erbMessages $ fromStored erb)
            Nothing -> do
                (smsg, thread') <- createDirectMessage self (emptyDirectThread powner) (T.pack text)
                (,smsg) <$> slistAddS thread' (erbMessages $ fromStored erb)
        erb' <- wrappedStore st (fromStored erb) { erbMessages = slist }
        return (erb', smsg)
    liftIO $ sendToPeer self peer (T.pack "dmsg") smsg

    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ putStrLn $ formatMessage tzone $ fromStored smsg

cmdHistory :: Command
cmdHistory = void $ runMaybeT $ do
    self <- asks ciSelf
    let st = storedStorage $ idData self
    Just peer <- gets csPeer
    Just powner <- return $ finalOwner <$> peerIdentity peer

    Just erebosHead <- liftIO $ loadHead st "erebos"
    let erebos = wrappedLoad (headRef erebosHead)
    Just thread <- return $ find ((== idData powner) . msgPeer) $ fromSList $ erbMessages $ fromStored erebos
    tzone <- liftIO $ getCurrentTimeZone
    liftIO $ mapM_ (putStrLn . formatMessage tzone) $ reverse $ take 50 $ threadToList thread


formatMessage :: TimeZone -> DirectMessage -> String
formatMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ iddName $ fromStored $ signedData $ fromStored $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

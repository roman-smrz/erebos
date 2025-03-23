module Terminal (
    Terminal,
    hasTerminalUI,
    withTerminal,
    setPrompt,
    getInputLine,
    InputHandling(..),

    TerminalLine,
    printLine,

    printBottomLines,
    clearBottomLines,

    CompletionFunc, Completion,
    noCompletion,
    simpleCompletion,
    completeWordWithPrev,
) where

import Control.Arrow
import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception
import Control.Monad

import Data.Char

import System.IO
import System.Console.ANSI


data Terminal = Terminal
    { termLock :: MVar ()
    , termAnsi :: Bool
    , termPrompt :: TVar String
    , termShowPrompt :: TVar Bool
    , termInput :: TVar ( String, String )
    , termBottomLines :: TVar [ String ]
    }

data TerminalLine = TerminalLine
    { tlTerminal :: Terminal
    }

data Input
    = InputChar Char
    | InputMoveRight
    | InputMoveLeft
    | InputMoveEnd
    | InputMoveStart
    | InputBackspace
    | InputClear
    | InputBackWord
    | InputEnd
    | InputEscape String
    deriving (Eq, Ord, Show)


data InputHandling a
    = KeepPrompt a
    | ErasePrompt a


hasTerminalUI :: Terminal -> Bool
hasTerminalUI = termAnsi

initTerminal :: IO Terminal
initTerminal = do
    termLock <- newMVar ()
    termAnsi <- hNowSupportsANSI stdout
    termPrompt <- newTVarIO ""
    termShowPrompt <- newTVarIO False
    termInput <- newTVarIO ( "", "" )
    termBottomLines <- newTVarIO []
    return Terminal {..}

bracketSet :: IO a -> (a -> IO b) -> a -> IO c -> IO c
bracketSet get set val = bracket (get <* set val) set . const

withTerminal :: CompletionFunc IO -> (Terminal -> IO a) -> IO a
withTerminal _ act = do
    term <- initTerminal

    bracketSet (hGetEcho stdin) (hSetEcho stdin) False $
        bracketSet (hGetBuffering stdin) (hSetBuffering stdin) NoBuffering $
        bracketSet (hGetBuffering stdout) (hSetBuffering stdout) (BlockBuffering Nothing) $
            act term


termPutStr :: Terminal -> String -> IO ()
termPutStr Terminal {..} str = do
    withMVar termLock $ \_ -> do
        putStr str
        hFlush stdout


getInput :: IO Input
getInput = do
    getChar >>= \case
        '\ESC' -> do
            esc <- readEsc
            case parseEsc esc of
                Just ( 'C' , [] ) -> return InputMoveRight
                Just ( 'D' , [] ) -> return InputMoveLeft
                _ -> return (InputEscape esc)
        '\b' -> return InputBackspace
        '\DEL' -> return InputBackspace
        '\NAK' -> return InputClear
        '\ETB' -> return InputBackWord
        '\SOH' -> return InputMoveStart
        '\ENQ' -> return InputMoveEnd
        '\EOT' -> return InputEnd
        c -> return (InputChar c)
  where
    readEsc = getChar >>= \case
        c | c == '\ESC' || isAlpha c -> return [ c ]
          | otherwise -> (c :) <$> readEsc

    parseEsc = \case
        '[' : c : [] -> do
            Just ( c, [] )
        _ -> Nothing


getInputLine :: Terminal -> (Maybe String -> InputHandling a) -> IO a
getInputLine term@Terminal {..} handleResult = do
    withMVar termLock $ \_ -> do
        prompt <- atomically $ do
            writeTVar termShowPrompt True
            readTVar termPrompt
        putStr $ prompt <> "\ESC[K"
        drawBottomLines term
        hFlush stdout
    (handleResult <$> go) >>= \case
        KeepPrompt x -> do
            termPutStr term "\n"
            return x
        ErasePrompt x -> do
            termPutStr term "\r\ESC[K"
            return x
  where
    go = getInput >>= \case
        InputChar '\n' -> do
            atomically $ do
                ( pre, post ) <- readTVar termInput
                writeTVar termInput ( "", "" )
                writeTVar termShowPrompt False
                return $ Just $ pre ++ post

        InputChar c | isPrint c -> withInput $ \case
            ( _, post ) -> do
                writeTVar termInput . first (++ [ c ]) =<< readTVar termInput
                return $ c : (if null post then "" else "\ESC[s" <> post <> "\ESC[u")

        InputChar _ -> go

        InputMoveRight -> withInput $ \case
            ( pre, c : post ) -> do
                writeTVar termInput ( pre ++ [ c ], post )
                return $ "\ESC[C"
            _ -> return ""

        InputMoveLeft -> withInput $ \case
            ( pre@(_ : _), post ) -> do
                writeTVar termInput ( init pre, last pre : post )
                return $ "\ESC[D"
            _ -> return ""

        InputBackspace -> withInput $ \case
            ( pre@(_ : _), post ) -> do
                writeTVar termInput ( init pre, post )
                return $ "\b\ESC[K" <> (if null post then "" else "\ESC[s" <> post <> "\ESC[u")
            _ -> return ""

        InputClear -> withInput $ \_ -> do
            writeTVar termInput ( "", "" )
            ("\r\ESC[K" <>) <$> getCurrentPromptLine term

        InputBackWord -> withInput $ \( pre, post ) -> do
            let pre' = reverse $ dropWhile (not . isSpace) $ dropWhile isSpace $ reverse pre
            writeTVar termInput ( pre', post )
            ("\r\ESC[K" <>) <$> getCurrentPromptLine term

        InputMoveStart -> withInput $ \( pre, post ) -> do
            writeTVar termInput ( "", pre <> post )
            return $ "\ESC[" <> show (length pre) <> "D"

        InputMoveEnd -> withInput $ \( pre, post ) -> do
            writeTVar termInput ( pre <> post, "" )
            return $ "\ESC[" <> show (length post) <> "C"

        InputEnd -> do
            atomically (readTVar termInput) >>= \case
                ( "", "" ) -> return Nothing
                _          -> go

        InputEscape _ -> go

    withInput f = do
        withMVar termLock $ const $ do
            str <- atomically $ f =<< readTVar termInput
            when (not $ null str) $ do
                putStr str
                hFlush stdout
        go


getCurrentPromptLine :: Terminal -> STM String
getCurrentPromptLine Terminal {..} = do
    prompt <- readTVar termPrompt
    ( pre, post ) <- readTVar termInput
    return $ prompt <> pre <> "\ESC[s" <> post <> "\ESC[u"

setPrompt :: Terminal -> String -> IO ()
setPrompt term@Terminal {..} prompt = do
    withMVar termLock $ \_ -> do
        join $ atomically $ do
            writeTVar termPrompt prompt
            readTVar termShowPrompt >>= \case
                True -> do
                    promptLine <- getCurrentPromptLine term
                    return $ do
                        putStr $ "\r\ESC[K" <> promptLine
                        hFlush stdout
                False -> return $ return ()

printLine :: Terminal -> String -> IO TerminalLine
printLine tlTerminal@Terminal {..} str = do
    withMVar termLock $ \_ -> do
        promptLine <- atomically $ do
            readTVar termShowPrompt >>= \case
                True -> getCurrentPromptLine tlTerminal
                False -> return ""
        putStr $ "\r\ESC[K" <> str <> "\n\ESC[K" <> promptLine
        drawBottomLines tlTerminal
        hFlush stdout
        return TerminalLine {..}


printBottomLines :: Terminal -> String -> IO ()
printBottomLines term@Terminal {..} str = do
    case lines str of
        [] -> clearBottomLines term
        blines -> do
            withMVar termLock $ \_ -> do
                atomically $ writeTVar termBottomLines blines
                drawBottomLines term
                hFlush stdout

clearBottomLines :: Terminal -> IO ()
clearBottomLines Terminal {..} = do
    withMVar termLock $ \_ -> do
        atomically (readTVar termBottomLines) >>= \case
            []  -> return ()
            _:_ -> do
                atomically $ writeTVar termBottomLines []
                putStr $ "\ESC[s\n\ESC[J\ESC[u"
                hFlush stdout

drawBottomLines :: Terminal -> IO ()
drawBottomLines Terminal {..} = do
    atomically (readTVar termBottomLines) >>= \case
        blines@( firstLine : otherLines ) -> do
            ( shift ) <- atomically $ do
                readTVar termShowPrompt >>= \case
                    True -> do
                        prompt <- readTVar termPrompt
                        ( pre, _ ) <- readTVar termInput
                        return (displayWidth (prompt <> pre) + 1)
                    False -> do
                        return 0
            putStr $ concat
                [ "\n\ESC[J", firstLine, concat (map ('\n' :) otherLines)
                , "\ESC[", show (length blines), "F"
                , "\ESC[", show shift, "G"
                ]
        [] -> return ()


displayWidth :: String -> Int
displayWidth = \case
    ('\ESC' : '[' : rest) -> displayWidth $ drop 1 $ dropWhile (not . isAlpha) rest
    ('\ESC' : _ : rest) -> displayWidth rest
    (_ : rest) -> 1 + displayWidth rest
    [] -> 0


type CompletionFunc m = ( String, String ) -> m ( String, [ Completion ] )

data Completion

noCompletion :: Monad m => CompletionFunc m
noCompletion ( l, _ ) = return ( l, [] )

completeWordWithPrev :: Maybe Char -> [ Char ] -> (String -> String -> m [ Completion ]) -> CompletionFunc m
completeWordWithPrev = error "TODO"

simpleCompletion :: String -> Completion
simpleCompletion = error "TODO"

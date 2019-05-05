module Main (main) where

import Control.Concurrent.Chan
import Control.Exception
import Control.Monad

import qualified Data.Text.IO as T

import System.Environment
import System.IO
import System.IO.Error

import Identity
import Network
import PubKey
import Storage


main :: IO ()
main = do
    [bhost] <- getArgs
    st <- openStorage "test"
    idhead <- catchJust (guard . isDoesNotExistError) (loadHead st "identity") $ \_ -> do
        putStr "Name: "
        hFlush stdout
        name <- T.getLine
        (secret, public) <- generateKeys st

        base <- sign secret =<< wrappedStore st (Identity name Nothing public)
        Right h <- replaceHead base (Left (st, "identity"))
        return h
    let sidentity = wrappedLoad (headRef idhead) :: Stored Identity
    print $ fromStored sidentity

    chan <- peerDiscovery bhost sidentity
    void $ forever $ print =<< readChan chan
    return ()

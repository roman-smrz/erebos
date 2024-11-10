module Erebos.Storage.Key (
    KeyPair(..),
    storeKey, loadKey, loadKeyMb,
    moveKeys,
) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class

import Data.ByteArray
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import System.Directory
import System.FilePath
import System.IO.Error

import Erebos.Storable
import Erebos.Storage.Internal

class Storable pub => KeyPair sec pub | sec -> pub, pub -> sec where
    generateKeys :: Storage -> IO (sec, Stored pub)
    keyGetPublic :: sec -> Stored pub
    keyGetData :: sec -> ScrubbedBytes
    keyFromData :: ScrubbedBytes -> Stored pub -> Maybe sec


keyFilePath :: KeyPair sec pub => FilePath -> Stored pub -> FilePath
keyFilePath sdir pkey = sdir </> "keys" </> (BC.unpack $ showRef $ storedRef pkey)

storeKey :: KeyPair sec pub => sec -> IO ()
storeKey key = do
    let spub = keyGetPublic key
    case stBacking $ storedStorage spub of
         StorageDir { dirPath = dir } -> writeFileOnce (keyFilePath dir spub) (BL.fromStrict $ convert $ keyGetData key)
         StorageMemory { memKeys = kstore } -> modifyMVar_ kstore $ return . M.insert (refDigest $ storedRef spub) (keyGetData key)

loadKey :: (KeyPair sec pub, MonadIO m, MonadError String m) => Stored pub -> m sec
loadKey pub = maybe (throwError $ "secret key not found for " <> show (storedRef pub)) return =<< loadKeyMb pub

loadKeyMb :: (KeyPair sec pub, MonadIO m) => Stored pub -> m (Maybe sec)
loadKeyMb spub = liftIO $ run $ storedStorage spub
  where
    run st = tryOneLevel (stBacking st) >>= \case
        key@Just {} -> return key
        Nothing | Just parent <- stParent st -> run parent
                | otherwise -> return Nothing
    tryOneLevel = \case
        StorageDir { dirPath = dir } -> tryIOError (BC.readFile (keyFilePath dir spub)) >>= \case
            Right kdata -> return $ keyFromData (convert kdata) spub
            Left _ -> return Nothing
        StorageMemory { memKeys = kstore } -> (flip keyFromData spub <=< M.lookup (refDigest $ storedRef spub)) <$> readMVar kstore

moveKeys :: MonadIO m => Storage -> Storage -> m ()
moveKeys from to = liftIO $ do
    case (stBacking from, stBacking to) of
        (StorageDir { dirPath = fromPath }, StorageDir { dirPath = toPath }) -> do
            files <- listDirectory (fromPath </> "keys")
            forM_ files $ \file -> do
                renameFile (fromPath </> "keys" </> file) (toPath </> "keys" </> file)

        (StorageDir { dirPath = fromPath }, StorageMemory { memKeys = toKeys }) -> do
            let move m file
                    | Just dgst <- readRefDigest (BC.pack file) = do
                        let path = fromPath </> "keys" </> file
                        key <- convert <$> BC.readFile path
                        removeFile path
                        return $ M.insert dgst key m
                    | otherwise = return m
            files <- listDirectory (fromPath </> "keys")
            modifyMVar_ toKeys $ \keys -> foldM move keys files

        (StorageMemory { memKeys = fromKeys }, StorageDir { dirPath = toPath }) -> do
            modifyMVar_ fromKeys $ \keys -> do
                forM_ (M.assocs keys) $ \(dgst, key) ->
                    writeFileOnce (toPath </> "keys" </> (BC.unpack $ showRefDigest dgst)) (BL.fromStrict $ convert key)
                return M.empty

        (StorageMemory { memKeys = fromKeys }, StorageMemory { memKeys = toKeys }) -> do
            when (fromKeys /= toKeys) $ do
                modifyMVar_ fromKeys $ \fkeys -> do
                    modifyMVar_ toKeys $ return . M.union fkeys
                    return M.empty

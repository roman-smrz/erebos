module Storage.Key (
    KeyPair(..),
    storeKey, loadKey, loadKeyMb,
) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Except

import Data.ByteArray
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.Map as M

import System.FilePath
import System.IO.Error

import Storage
import Storage.Internal

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
loadKey = maybe (throwError "secret key not found") return <=< loadKeyMb

loadKeyMb :: (KeyPair sec pub, MonadIO m) => Stored pub -> m (Maybe sec)
loadKeyMb spub = liftIO $ do
    case stBacking $ storedStorage spub of
         StorageDir { dirPath = dir } -> tryIOError (BC.readFile (keyFilePath dir spub)) >>= \case
             Right kdata -> return $ keyFromData (convert kdata) spub
             Left _ -> return Nothing
         StorageMemory { memKeys = kstore } -> (flip keyFromData spub <=< M.lookup (refDigest $ storedRef spub)) <$> readMVar kstore

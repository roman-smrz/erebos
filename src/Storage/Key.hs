module Storage.Key (
    KeyPair(..),
    storeKey, loadKey,
) where

import Data.ByteArray
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL

import System.FilePath
import System.IO.Error

import Storage
import Storage.Internal

class Storable pub => KeyPair sec pub | sec -> pub, pub -> sec where
    generateKeys :: Storage -> IO (sec, Stored pub)
    keyGetPublic :: sec -> Stored pub
    keyGetData :: sec -> ScrubbedBytes
    keyFromData :: ScrubbedBytes -> Stored pub -> Maybe sec


keyStorage :: Storage -> FilePath
keyStorage (Storage base) = base </> "keys"

keyFilePath :: KeyPair sec pub => Stored pub -> FilePath
keyFilePath pkey = keyStorage (storedStorage pkey) </> (BC.unpack $ showRef $ storedRef pkey)

storeKey :: KeyPair sec pub => sec -> IO ()
storeKey key = writeFileOnce (keyFilePath $ keyGetPublic key) (BL.fromStrict $ convert $ keyGetData key)

loadKey :: KeyPair sec pub => Stored pub -> IO (Maybe sec)
loadKey spub = do
    tryIOError (BC.readFile (keyFilePath spub)) >>= \case
        Right kdata -> return $ keyFromData (convert kdata) spub
        Left _ -> return Nothing

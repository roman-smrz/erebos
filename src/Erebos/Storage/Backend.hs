{-|
Description: Implement custom storage backend

Exports type class, which can be used to create custom 'Storage' backend.
-}

module Erebos.Storage.Backend (
    StorageBackend(..),
    Complete, Partial,
    Storage, PartialStorage,
    newStorage,
    withStorageBackend,

    refDigestBytes,
    allocateScrubbedBytes,
    unsafeUseScrubbedBytesAsCStringLen,

    WatchID, startWatchID, nextWatchID,
) where

import Control.Concurrent.MVar

import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.HashTable.IO qualified as HT

import Foreign.C.String
import Foreign.Ptr

import ConvertByteString
import Erebos.Object.Internal
import Erebos.Storage.Internal


newStorage :: StorageBackend bck => bck -> IO (Storage' (BackendCompleteness bck))
newStorage stBackend = do
    stRefGeneration <- newMVar =<< HT.new
    stRefRoots <- newMVar =<< HT.new
    return Storage {..}

withStorageBackend :: Storage' c -> (forall bck. (StorageBackend bck, BackendCompleteness bck ~ c) => bck -> IO a) -> IO a
withStorageBackend Storage {..} f = f stBackend


refDigestBytes :: RefDigest -> ByteString
refDigestBytes = toByteString

allocateScrubbedBytes :: Int -> (Ptr a -> IO ()) -> IO ScrubbedBytes
allocateScrubbedBytes len f = snd <$> BA.allocRet len f

unsafeUseScrubbedBytesAsCStringLen :: ScrubbedBytes -> (CStringLen -> IO a) -> IO a
unsafeUseScrubbedBytesAsCStringLen sb f = withByteArray sb (\ptr -> f ( ptr, BA.length sb ))

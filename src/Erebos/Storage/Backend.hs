{-|
Description: Implement custom storage backend

Exports type class, which can be used to create custom 'Storage' backend.
-}

module Erebos.Storage.Backend (
    StorageBackend(..),
    Complete, Partial,
    Storage, PartialStorage,
    newStorage,
    refDigestBytes,

    WatchID, startWatchID, nextWatchID,
) where

import Control.Concurrent.MVar

import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.HashTable.IO qualified as HT

import Erebos.Object.Internal
import Erebos.Storage.Internal


newStorage :: StorageBackend bck => bck -> IO (Storage' (BackendCompleteness bck))
newStorage stBackend = do
    stRefGeneration <- newMVar =<< HT.new
    stRefRoots <- newMVar =<< HT.new
    return Storage {..}


refDigestBytes :: RefDigest -> ByteString
refDigestBytes = BA.convert

{-|
Description: Encoding custom types into Erebos objects

Module provides the 'Storable' class for types that can be serialized to/from
Erebos objects, along with various helpers, mostly for encoding using records.

The 'Stored' wrapper for objects actually encoded and stored in some storage is
defined here as well.
-}

module Erebos.Storable (
    Storable(..), ZeroStorable(..),
    StorableText(..), StorableDate(..), StorableUUID(..),
    StorageCompleteness(..),

    Store, StoreRec,
    storeBlob, storeRec, storeZero,
    storeEmpty, storeInt, storeNum, storeText, storeBinary, storeDate, storeUUID, storeRef, storeRawRef, storeWeak, storeRawWeak,
    storeMbEmpty, storeMbInt, storeMbNum, storeMbText, storeMbBinary, storeMbDate, storeMbUUID, storeMbRef, storeMbRawRef, storeMbWeak, storeMbRawWeak,
    storeZRef, storeZWeak,
    storeRecItems,

    Load, LoadRec,
    loadCurrentRef, loadCurrentObject,
    loadRecCurrentRef, loadRecItems,

    loadBlob, loadRec, loadZero,
    loadEmpty, loadInt, loadNum, loadText, loadBinary, loadDate, loadUUID, loadRef, loadRawRef, loadRawWeak,
    loadMbEmpty, loadMbInt, loadMbNum, loadMbText, loadMbBinary, loadMbDate, loadMbUUID, loadMbRef, loadMbRawRef, loadMbRawWeak,
    loadTexts, loadBinaries, loadRefs, loadRawRefs, loadRawWeaks,
    loadZRef,

    Stored,
    fromStored, storedRef,
    wrappedStore, wrappedLoad,
    copyStored,
    unsafeMapStored,

    Storage, MonadStorage(..),

    module Erebos.Error,
) where

import Erebos.Error
import Erebos.Object.Internal

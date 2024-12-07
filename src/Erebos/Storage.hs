{-|
Description: Working with storage and heads

Provides functions for opening 'Storage' backed either by disk or memory. For
conveniance also function for working with 'Head's are reexported here.
-}

module Erebos.Storage (
    Storage, PartialStorage,
    openStorage, memoryStorage,
    deriveEphemeralStorage, derivePartialStorage,

    Head, HeadType,
    HeadID, HeadTypeID,
    headId, headStorage, headRef, headObject, headStoredObject,
    loadHeads, loadHead, reloadHead,
    storeHead, replaceHead, updateHead, updateHead_,

    WatchedHead,
    watchHead, watchHeadWith, unwatchHead,
    watchHeadRaw,

    MonadStorage(..),
) where

import Erebos.Object.Internal
import Erebos.Storage.Disk
import Erebos.Storage.Head
import Erebos.Storage.Memory

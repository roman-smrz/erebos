module Sync (
    SyncService,
    ServicePacket(..),
) where

import Control.Monad

import Data.List

import Service
import State
import Storage
import Storage.Merge

data SyncService

instance Service SyncService where
    serviceID _ = mkServiceID "a4f538d0-4e50-4082-8e10-7e3ec2af175d"

    data ServiceState SyncService = SyncService
    emptyServiceState = SyncService

    newtype ServicePacket SyncService = SyncPacket (Stored SharedState)

    serviceHandler packet = do
        let SyncPacket added = fromStored packet
        ls <- svcGetLocal
        let st = storedStorage ls
            current = sort $ lsShared $ fromStored ls
            updated = filterAncestors (added : current)
        when (current /= updated) $ do
            svcSetLocal =<< wrappedStore st (fromStored ls) { lsShared = updated }

instance Storable (ServicePacket SyncService) where
    store' (SyncPacket smsg) = store' smsg
    load' = SyncPacket <$> load'

module Sync (
    SyncService(..),
) where

import Control.Monad
import Control.Monad.Reader

import Data.List

import Identity
import Service
import State
import Storage
import Storage.Merge

data SyncService = SyncPacket (Stored SharedState)

instance Service SyncService where
    serviceID _ = mkServiceID "a4f538d0-4e50-4082-8e10-7e3ec2af175d"

    serviceHandler packet = do
        let SyncPacket added = fromStored packet
        pid <- asks svcPeerIdentity
        self <- svcSelf
        when (finalOwner pid `sameIdentity` finalOwner self) $ do
            updateLocalHead_ $ \ls -> do
                let current = sort $ lsShared $ fromStored ls
                    updated = filterAncestors (added : current)
                if current /= updated
                   then wrappedStore (storedStorage ls) (fromStored ls) { lsShared = updated }
                   else return ls

    serviceNewPeer = notifyPeer . lsShared . fromStored =<< svcGetLocal
    serviceStorageWatchers _ = (:[]) $ SomeStorageWatcher (lsShared . fromStored) notifyPeer

instance Storable SyncService where
    store' (SyncPacket smsg) = store' smsg
    load' = SyncPacket <$> load'

notifyPeer :: [Stored SharedState] -> ServiceHandler SyncService ()
notifyPeer shared = do
    pid <- asks svcPeerIdentity
    self <- svcSelf
    when (finalOwner pid `sameIdentity` finalOwner self) $ do
        forM_ shared $ \sh ->
            replyStoredRef =<< (wrappedStore (storedStorage sh) . SyncPacket) sh

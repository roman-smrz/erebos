module Erebos.Object.Deferred (
    Deferred,
    DeferredSize(..),
    DeferredResult(..),

    deferredRef,
    deferredLoad,
    deferredWait,
    deferredCheck,

    deferLoadWithServer,
) where

import Control.Concurrent.MVar
import Control.Monad.IO.Class

import Data.Word

import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Storable


-- | Deffered value, which can be loaded on request. Holds a reference (digest)
-- to an object and information about suitable network peers, from which the
-- data can be requested.
data Deferred a = Deferred
    { deferredRef_ :: RefDigest
    , deferredSize :: DeferredSize
    , deferredServer :: Server
    , deferredPeers :: [ RefDigest ]
    , deferredStatus :: MVar (Maybe (MVar (DeferredResult a)))
    }

-- | Size constraint for the deferred object.
data DeferredSize
    = DeferredExactSize Word64 -- ^ Component size of the referred data must be exactly the given value.
    | DeferredMaximumSize Word64 -- ^ Component size of the referred data must not exceed the given value.

-- | Result of the deferred load request.
data DeferredResult a
    = DeferredLoaded (Stored a) -- ^ Deferred object was sucessfully loaded.
    | DeferredInvalid -- ^ Deferred object was (partially) loaded, but failed to meet the size constraint or was an invalid object.
    | DeferredFailed -- ^ Failure to load the object, e.g. no suitable peer was found.

-- | Get the digest of the deferred object.
deferredRef :: Deferred a -> RefDigest
deferredRef = deferredRef_

-- | Request the deferred object to be loaded. Does nothing if that was already
-- requested before. The result can be received using `deferredWait` or
-- `deferredCheck` functions.
deferredLoad :: (Storable a, MonadIO m) => Deferred a -> m ()
deferredLoad Deferred {..} = liftIO $ do
    modifyMVar_ deferredStatus $ \case
        Nothing -> do
            mvar <- newEmptyMVar
            let matchPeer peer =
                    getPeerIdentity peer >>= \case
                        PeerIdentityFull pid -> do
                            return $ any (`elem` identityDigests pid) deferredPeers
                        _ -> return False

            liftIO (findPeer deferredServer matchPeer) >>= \case
                Just peer -> do
                    let bound = case deferredSize of
                            DeferredExactSize s -> s
                            DeferredMaximumSize s -> s

                        checkSize ref = case deferredSize of
                            DeferredExactSize s -> componentSize ref == s
                            DeferredMaximumSize s -> componentSize ref <= s

                    requestDataFromPeer peer deferredRef_ bound $ liftIO . \case
                        DataRequestFulfilled ref
                            | checkSize ref -> putMVar mvar $ DeferredLoaded $ wrappedLoad ref
                            | otherwise -> putMVar mvar DeferredInvalid
                        DataRequestRejected -> putMVar mvar DeferredFailed
                        DataRequestBrokenBound -> putMVar mvar DeferredInvalid

                Nothing -> putMVar mvar DeferredFailed
            return $ Just mvar
        cur@Just {} -> return cur

-- | Wait for a `Deferred` value to be loaded and return the result. Requests
-- the value to be loaded if that was not already done.
deferredWait :: (Storable a, MonadIO m) => Deferred a -> m (DeferredResult a)
deferredWait d@Deferred {..} = liftIO $ readMVar deferredStatus >>= \case
    Nothing -> deferredLoad d >> deferredWait d
    Just mvar -> readMVar mvar

-- | Check if a `Deferred` value has already been loaded and return it in
-- `Just` if so, otherwise return `Nothing`. Requests the value to be loaded if
-- that was not already done.
deferredCheck :: (Storable a, MonadIO m) => Deferred a -> m (Maybe (DeferredResult a))
deferredCheck d@Deferred {..} = liftIO $ readMVar deferredStatus >>= \case
    Nothing -> deferredLoad d >> deferredCheck d
    Just mvar -> tryReadMVar mvar

deferLoadWithServer :: (Storable a, MonadIO m) => RefDigest -> DeferredSize -> Server -> [ RefDigest ] -> m (Deferred a)
deferLoadWithServer deferredRef_ deferredSize deferredServer deferredPeers = do
    deferredStatus <- liftIO $ newMVar Nothing
    return Deferred {..}


identityDigests :: Foldable f => Identity f -> [ RefDigest ]
identityDigests pid = map (refDigest . storedRef) $ idDataF =<< unfoldOwners pid

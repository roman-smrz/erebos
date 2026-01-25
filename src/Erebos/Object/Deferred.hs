module Erebos.Object.Deferred (
    Deferred,
    DeferredResult(..),

    deferredRef,
    deferredLoad,

    deferLoadWithServer,
) where

import Control.Concurrent.MVar
import Control.Monad.IO.Class

import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Storable


data Deferred a = Deferred
    { deferredRef_ :: RefDigest
    , deferredServer :: Server
    , deferredPeers :: [ RefDigest ]
    }

data DeferredResult a
    = DeferredLoaded (Stored a)
    | DeferredInvalid
    | DeferredFailed

deferredRef :: Deferred a -> RefDigest
deferredRef = deferredRef_

deferredLoad :: MonadIO m => Storable a => Deferred a -> m (MVar (DeferredResult a))
deferredLoad Deferred {..} = liftIO $ do
    mvar <- newEmptyMVar
    let matchPeer peer =
            getPeerIdentity peer >>= \case
                PeerIdentityFull pid -> do
                    return $ any (`elem` identityDigests pid) deferredPeers
                _ -> return False

    liftIO (findPeer deferredServer matchPeer) >>= \case
        Just peer -> do
            requestDataFromPeer peer deferredRef_ $ liftIO . \case
                DataRequestFulfilled ref -> putMVar mvar $ DeferredLoaded $ wrappedLoad ref
                DataRequestRejected -> putMVar mvar DeferredFailed
                DataRequestInvalid -> putMVar mvar DeferredInvalid
        Nothing -> putMVar mvar DeferredFailed
    return mvar

deferLoadWithServer :: Storable a => RefDigest -> Server -> [ RefDigest ] -> IO (Deferred a)
deferLoadWithServer deferredRef_ deferredServer deferredPeers = return Deferred {..}


identityDigests :: Foldable f => Identity f -> [ RefDigest ]
identityDigests pid = map (refDigest . storedRef) $ idDataF =<< unfoldOwners pid

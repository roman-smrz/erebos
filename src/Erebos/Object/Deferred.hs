module Erebos.Object.Deferred (
    Deferred,
    DeferredSize(..),
    DeferredResult(..),

    deferredRef,
    deferredLoad,

    deferLoadWithServer,
) where

import Control.Concurrent.MVar
import Control.Monad.IO.Class

import Data.Word

import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Storable


data Deferred a = Deferred
    { deferredRef_ :: RefDigest
    , deferredSize :: DeferredSize
    , deferredServer :: Server
    , deferredPeers :: [ RefDigest ]
    }

data DeferredSize
    = DeferredExactSize Word64
    | DeferredMaximumSize Word64

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
    return mvar

deferLoadWithServer :: Storable a => RefDigest -> DeferredSize -> Server -> [ RefDigest ] -> IO (Deferred a)
deferLoadWithServer deferredRef_ deferredSize deferredServer deferredPeers = return Deferred {..}


identityDigests :: Foldable f => Identity f -> [ RefDigest ]
identityDigests pid = map (refDigest . storedRef) $ idDataF =<< unfoldOwners pid

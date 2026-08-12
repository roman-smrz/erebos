module Erebos.Storage.Monad (
    MonadStorage(..),
    mloadKey,
) where

import Control.Monad.Except
import Control.Monad.Reader

import Erebos.Error
import Erebos.Object.Internal
import Erebos.Storable.Internal
import Erebos.Storage.Key


class Monad m => MonadStorage m where
    getStorage :: m Storage
    mstore :: Storable a => a -> m (Stored a)

    default mstore :: MonadIO m => Storable a => a -> m (Stored a)
    mstore x = do
        st <- getStorage
        wrappedStore st x

    mstoreKey :: KeyPair sec pub => sec -> m ()
    default mstoreKey :: (KeyPair sec pub, MonadIO m) => sec -> m ()
    mstoreKey = liftIO . storeKey

    mloadKeyMb :: KeyPair sec pub => Stored pub -> m (Maybe sec)
    default mloadKeyMb :: (KeyPair sec pub, MonadIO m) => Stored pub -> m (Maybe sec)
    mloadKeyMb = loadKeyMb

mloadKey :: (KeyPair sec pub, MonadStorage m, MonadError e m, FromErebosError e) => Stored pub -> m sec
mloadKey pub = maybe (throwOtherError $ "secret key not found for " <> show (storedRef pub)) return =<< mloadKeyMb pub


instance MonadIO m => MonadStorage (ReaderT Storage m) where
    getStorage = ask

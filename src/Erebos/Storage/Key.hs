module Erebos.Storage.Key (
    KeyPair(..),
    storeKey, loadKey, loadKeyMb,
    moveKeys,
) where

import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class

import Data.ByteArray
import Data.Typeable

import Erebos.Storable
import Erebos.Storage.Internal

class Storable pub => KeyPair sec pub | sec -> pub, pub -> sec where
    generateKeys :: Storage -> IO (sec, Stored pub)
    keyGetPublic :: sec -> Stored pub
    keyGetData :: sec -> ScrubbedBytes
    keyFromData :: ScrubbedBytes -> Stored pub -> Maybe sec


storeKey :: KeyPair sec pub => sec -> IO ()
storeKey key = do
    let spub = keyGetPublic key
    case storedStorage spub of
        Storage {..} -> backendStoreKey stBackend (refDigest $ storedRef spub) (keyGetData key)

loadKey :: (KeyPair sec pub, MonadIO m, MonadError String m) => Stored pub -> m sec
loadKey pub = maybe (throwError $ "secret key not found for " <> show (storedRef pub)) return =<< loadKeyMb pub

loadKeyMb :: forall sec pub m. (KeyPair sec pub, MonadIO m) => Stored pub -> m (Maybe sec)
loadKeyMb spub = liftIO $ run $ storedStorage spub
  where
    run :: Storage' c -> IO (Maybe sec)
    run Storage {..} = backendLoadKey stBackend (refDigest $ storedRef spub) >>= \case
        Just bytes -> return $ keyFromData bytes spub
        Nothing
            | Just (parent :: Storage) <- cast (backendParent stBackend) -> run parent
            | Just (parent :: PartialStorage) <- cast (backendParent stBackend) -> run parent
            | otherwise -> return Nothing

moveKeys :: MonadIO m => Storage -> Storage -> m ()
moveKeys Storage { stBackend = from } Storage { stBackend = to } = liftIO $ do
    keys <- backendListKeys from
    forM_ keys $ \key -> do
        backendLoadKey from key >>= \case
            Just sec -> do
                backendStoreKey to key sec
                backendRemoveKey from key
            Nothing -> return ()

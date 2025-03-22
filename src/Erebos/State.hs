module Erebos.State (
    LocalState(..),
    SharedState(..), SharedType(..),
    SharedTypeID, mkSharedTypeID,

    MonadStorage(..),
    MonadHead(..),
    updateLocalHead_,

    updateSharedState, updateSharedState_,
    lookupSharedValue, makeSharedStateUpdate,

    localIdentity,
    headLocalIdentity,

    mergeSharedIdentity,
) where

import Control.Monad.Except
import Control.Monad.Reader

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BC
import Data.Typeable
import Data.UUID (UUID)
import Data.UUID qualified as U

import Erebos.Identity
import Erebos.Object
import Erebos.PubKey
import Erebos.Storable
import Erebos.Storage.Head
import Erebos.Storage.Merge

data LocalState = LocalState
    { lsIdentity :: Stored (Signed ExtendedIdentityData)
    , lsShared :: [Stored SharedState]
    , lsOther :: [ ( ByteString, RecItem ) ]
    }

data SharedState = SharedState
    { ssPrev :: [Stored SharedState]
    , ssType :: Maybe SharedTypeID
    , ssValue :: [Ref]
    }

newtype SharedTypeID = SharedTypeID UUID
    deriving (Eq, Ord, StorableUUID)

mkSharedTypeID :: String -> SharedTypeID
mkSharedTypeID = maybe (error "Invalid shared type ID") SharedTypeID . U.fromString

class Mergeable a => SharedType a where
    sharedTypeID :: proxy a -> SharedTypeID

instance Storable LocalState where
    store' LocalState {..} = storeRec $ do
        storeRef "id" lsIdentity
        mapM_ (storeRef "shared") lsShared
        storeRecItems lsOther

    load' = loadRec $ do
        lsIdentity <- loadRef "id"
        lsShared <- loadRefs "shared"
        lsOther <- filter ((`notElem` [ BC.pack "id", BC.pack "shared" ]) . fst) <$> loadRecItems
        return LocalState {..}

instance HeadType LocalState where
    headTypeID _ = mkHeadTypeID "1d7491a9-7bcb-4eaa-8f13-c8c4c4087e4e"

instance Storable SharedState where
    store' st = storeRec $ do
        mapM_ (storeRef "PREV") $ ssPrev st
        storeMbUUID "type" $ ssType st
        mapM_ (storeRawRef "value") $ ssValue st

    load' = loadRec $ SharedState
        <$> loadRefs "PREV"
        <*> loadMbUUID "type"
        <*> loadRawRefs "value"

instance SharedType (Maybe ComposedIdentity) where
    sharedTypeID _ = mkSharedTypeID "0c6c1fe0-f2d7-4891-926b-c332449f7871"


class (MonadIO m, MonadStorage m) => MonadHead a m where
    updateLocalHead :: (Stored a -> m (Stored a, b)) -> m b
    getLocalHead :: m (Stored a)
    getLocalHead = updateLocalHead $ \x -> return (x, x)

updateLocalHead_ :: MonadHead a m => (Stored a -> m (Stored a)) -> m ()
updateLocalHead_ f = updateLocalHead (fmap (,()) . f)

instance (HeadType a, MonadIO m) => MonadHead a (ReaderT (Head a) m) where
    updateLocalHead f = do
        h <- ask
        snd <$> updateHead h f


localIdentity :: LocalState -> UnifiedIdentity
localIdentity ls = maybe (error "failed to verify local identity")
    (updateOwners $ maybe [] idExtDataF $ lookupSharedValue $ lsShared ls)
    (validateExtendedIdentity $ lsIdentity ls)

headLocalIdentity :: Head LocalState -> UnifiedIdentity
headLocalIdentity = localIdentity . headObject


updateSharedState_ :: forall a m. (SharedType a, MonadHead LocalState m) => (a -> m a) -> Stored LocalState -> m (Stored LocalState)
updateSharedState_ f = fmap fst <$> updateSharedState (fmap (,()) . f)

updateSharedState :: forall a b m. (SharedType a, MonadHead LocalState m) => (a -> m (a, b)) -> Stored LocalState -> m (Stored LocalState, b)
updateSharedState f = \ls -> do
    let shared = lsShared $ fromStored ls
        val = lookupSharedValue shared
    st <- getStorage
    (val', x) <- f val
    (,x) <$> if toComponents val' == toComponents val
                then return ls
                else do shared' <- makeSharedStateUpdate st val' shared
                        wrappedStore st (fromStored ls) { lsShared = [shared'] }

lookupSharedValue :: forall a. SharedType a => [Stored SharedState] -> a
lookupSharedValue = mergeSorted . filterAncestors . map wrappedLoad . concatMap (ssValue . fromStored) . filterAncestors . helper
    where helper (x:xs) | Just sid <- ssType (fromStored x), sid == sharedTypeID @a Proxy = x : helper xs
                        | otherwise = helper $ ssPrev (fromStored x) ++ xs
          helper [] = []

makeSharedStateUpdate :: forall a m. MonadIO m => SharedType a => Storage -> a -> [Stored SharedState] -> m (Stored SharedState)
makeSharedStateUpdate st val prev = liftIO $ wrappedStore st SharedState
    { ssPrev = prev
    , ssType = Just $ sharedTypeID @a Proxy
    , ssValue = storedRef <$> toComponents val
    }


mergeSharedIdentity :: (MonadHead LocalState m, MonadError e m, FromErebosError e) => m UnifiedIdentity
mergeSharedIdentity = updateLocalHead $ updateSharedState $ \case
    Just cidentity -> do
        identity <- mergeIdentity cidentity
        return (Just $ toComposedIdentity identity, identity)
    Nothing -> throwOtherError "no existing shared identity"

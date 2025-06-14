{-|
Description: Define, use and watch heads

Provides data types and functions for reading, writing or watching `Head's.
Type class `HeadType' is used to define custom new `Head' types.
-}

module Erebos.Storage.Head (
    -- * Head type and accessors
    Head, HeadType(..),
    HeadID, HeadTypeID, mkHeadTypeID,
    headId, headStorage, headRef, headObject, headStoredObject,

    -- * Loading and storing heads
    loadHeads, loadHead, reloadHead,
    storeHead, replaceHead, updateHead, updateHead_,
    loadHeadRaw, storeHeadRaw, replaceHeadRaw,

    -- * Watching heads
    WatchedHead,
    watchHead, watchHeadWith, unwatchHead,
    watchHeadRaw,
) where

import Control.Concurrent
import Control.Monad
import Control.Monad.Reader

import Data.Bifunctor
import Data.Typeable

import Erebos.Object
import Erebos.Storable
import Erebos.Storage.Backend
import Erebos.Storage.Internal
import Erebos.UUID qualified as U


-- | Represents loaded Erebos storage head, along with the object it pointed to
-- at the time it was loaded.
--
-- Each possible head type has associated unique ID, represented as
-- `HeadTypeID'. For each type, there can be multiple individual heads in given
-- storage, each also identified by unique ID (`HeadID').
data Head a = Head HeadID (Stored a)
    deriving (Eq, Show)

-- | Instances of this class can be used as objects pointed to by heads in
-- Erebos storage. Each such type must be `Storable' and have a unique ID.
--
-- To create a custom head type, generate a new UUID and assign it to the type using
-- `mkHeadTypeID':
--
-- > instance HeadType MyType where
-- >     headTypeID _ = mkHeadTypeID "86e8033d-c476-4f81-9b7c-fd36b9144475"
class Storable a => HeadType a where
    headTypeID :: proxy a -> HeadTypeID
    -- ^ Get the ID of the given head type; must be unique for each `HeadType' instance.

instance MonadIO m => MonadStorage (ReaderT (Head a) m) where
    getStorage = asks $ headStorage


-- | Get `HeadID' associated with given `Head'.
headId :: Head a -> HeadID
headId (Head uuid _) = uuid

-- | Get storage from which the `Head' was loaded.
headStorage :: Head a -> Storage
headStorage = refStorage . headRef

-- | Get `Ref' of the `Head'\'s associated object.
headRef :: Head a -> Ref
headRef (Head _ sx) = storedRef sx

-- | Get the object the `Head' pointed to when it was loaded.
headObject :: Head a -> a
headObject (Head _ sx) = fromStored sx

-- | Get the object the `Head' pointed to when it was loaded as a `Stored' value.
headStoredObject :: Head a -> Stored a
headStoredObject (Head _ sx) = sx

-- | Create `HeadTypeID' from string representation of UUID.
mkHeadTypeID :: String -> HeadTypeID
mkHeadTypeID = maybe (error "Invalid head type ID") HeadTypeID . U.fromString


-- | Load all `Head's of type @a@ from storage.
loadHeads :: forall a m. MonadIO m => HeadType a => Storage -> m [Head a]
loadHeads st@Storage {..} =
    map (uncurry Head . fmap (wrappedLoad . Ref st))
        <$> liftIO (backendLoadHeads stBackend (headTypeID @a Proxy))

-- | Try to load a `Head' of type @a@ from storage.
loadHead
    :: forall a m. (HeadType a, MonadIO m)
    => Storage  -- ^ Storage from which to load the head
    -> HeadID  -- ^ ID of the particular head
    -> m (Maybe (Head a))  -- ^ Head object, or `Nothing' if not found
loadHead st hid = fmap (Head hid . wrappedLoad) <$> loadHeadRaw st (headTypeID @a Proxy) hid

-- | Try to load `Head' using a raw head and type IDs, getting `Ref' if found.
loadHeadRaw
    :: forall m. MonadIO m
    => Storage  -- ^ Storage from which to load the head
    -> HeadTypeID  -- ^ ID of the head type
    -> HeadID  -- ^ ID of the particular head
    -> m (Maybe Ref)  -- ^ `Ref' pointing to the head object, or `Nothing' if not found
loadHeadRaw st@Storage {..} tid hid = do
    fmap (Ref st) <$> liftIO (backendLoadHead stBackend tid hid)

-- | Reload the given head from storage, returning `Head' with updated object,
-- or `Nothing' if there is no longer head with the particular ID in storage.
reloadHead :: (HeadType a, MonadIO m) => Head a -> m (Maybe (Head a))
reloadHead (Head hid val) = loadHead (storedStorage val) hid

-- | Store a new `Head' of type 'a' in the storage.
storeHead :: forall a m. MonadIO m => HeadType a => Storage -> a -> m (Head a)
storeHead st obj = do
    let tid = headTypeID @a Proxy
    stored <- wrappedStore st obj
    hid <- storeHeadRaw st tid (storedRef stored)
    return $ Head hid stored

-- | Store a new `Head' in the storage, using the raw `HeadTypeID' and `Ref',
-- the function returns the assigned `HeadID' of the new head.
storeHeadRaw :: forall m. MonadIO m => Storage -> HeadTypeID -> Ref -> m HeadID
storeHeadRaw Storage {..} tid ref = liftIO $ do
    hid <- HeadID <$> U.nextRandom
    backendStoreHead stBackend tid hid (refDigest ref)
    return hid

-- | Try to replace existing `Head' of type @a@ in the storage. Function fails
-- if the head value in storage changed after being loaded here; for automatic
-- retry see `updateHead'.
replaceHead
    :: forall a m. (HeadType a, MonadIO m)
    => Head a  -- ^ Existing head, associated object is supposed to match the one in storage
    -> Stored a  -- ^ Intended new value
    -> m (Either (Maybe (Head a)) (Head a))
        -- ^
        -- [@`Left' `Nothing'@]:
        --     Nothing was stored – the head no longer exists in storage.
        -- [@`Left' (`Just' h)@]:
        --     Nothing was stored – the head value in storage does not match
        --     the first parameter, but is @h@ instead.
        -- [@`Right' h@]:
        --     Head value was updated in storage, the new head is @h@ (which is
        --     the same as first parameter with associated object replaced by
        --     the second parameter).
replaceHead prev@(Head hid pobj) stored' = liftIO $ do
    let st = headStorage prev
        tid = headTypeID @a Proxy
    stored <- copyStored st stored'
    bimap (fmap $ Head hid . wrappedLoad) (const $ Head hid stored) <$>
        replaceHeadRaw st tid hid (storedRef pobj) (storedRef stored)

-- | Try to replace existing head using raw IDs and `Ref's.
replaceHeadRaw
    :: forall m. MonadIO m
    => Storage  -- ^ Storage to use
    -> HeadTypeID  -- ^ ID of the head type
    -> HeadID  -- ^ ID of the particular head
    -> Ref  -- ^ Expected value in storage
    -> Ref  -- ^ Intended new value
    -> m (Either (Maybe Ref) Ref)
        -- ^
        -- [@`Left' `Nothing'@]:
        --     Nothing was stored – the head no longer exists in storage.
        -- [@`Left' (`Just' r)@]:
        --     Nothing was stored – the head value in storage does not match
        --     the expected value, but is @r@ instead.
        -- [@`Right' r@]:
        --     Head value was updated in storage, the new head value is @r@
        --     (which is the same as the indended value).
replaceHeadRaw st@Storage {..} tid hid prev new = liftIO $ do
    _ <- copyRef st new
    bimap (fmap $ Ref st) (Ref st) <$> backendReplaceHead stBackend tid hid (refDigest prev) (refDigest new)

-- | Update existing existing `Head' of type @a@ in the storage, using a given
-- function. The update function may be called multiple times in case the head
-- content changes concurrently during evaluation.
updateHead
    :: (HeadType a, MonadIO m)
    => Head a  -- ^ Existing head to be updated
    -> (Stored a -> m ( Stored a, b ))
        -- ^ Function that gets current value of the head and returns updated
        -- value, along with a custom extra value to be returned from
        -- `updateHead' call. The function may be called multiple times.
    -> m ( Maybe (Head a), b )
        -- ^ First element contains either the new head as @`Just' h@, or
        -- `Nothing' in case the head no longer exists in storage. Second
        -- element is the value from last call to the update function.
updateHead h f = do
    (o, x) <- f $ headStoredObject h
    replaceHead h o >>= \case
        Right h' -> return (Just h', x)
        Left Nothing -> return (Nothing, x)
        Left (Just h') -> updateHead h' f

-- | Update existing existing `Head' of type @a@ in the storage, using a given
-- function. The update function may be called multiple times in case the head
-- content changes concurrently during evaluation.
updateHead_
    :: (HeadType a, MonadIO m)
    => Head a  -- ^ Existing head to be updated
    -> (Stored a -> m (Stored a))
        -- ^ Function that gets current value of the head and returns updated
        -- value; may be called multiple times.
    -> m (Maybe (Head a))
        -- ^ The new head as @`Just' h@, or `Nothing' in case the head no
        -- longer exists in storage.
updateHead_ h = fmap fst . updateHead h . (fmap (,()) .)


-- | Represents a handle of a watched head, which can be used to cancel the
-- watching.
data WatchedHead = forall a. WatchedHead Storage WatchID (MVar a)

-- | Watch the given head. The callback will be called with the current head
-- value, and then again each time the head changes.
watchHead :: forall a. HeadType a => Head a -> (Head a -> IO ()) -> IO WatchedHead
watchHead h = watchHeadWith h id

-- | Watch the given head using custom selector function. The callback will be
-- called with the value derived from current head state, and then again each
-- time the selected value changes according to its `Eq' instance.
watchHeadWith
    :: forall a b. (HeadType a, Eq b)
    => Head a  -- ^ Head to watch
    -> (Head a -> b)  -- ^ Selector function
    -> (b -> IO ())  -- ^ Callback
    -> IO WatchedHead  -- ^ Watched head handle
watchHeadWith (Head hid val) sel cb = do
    watchHeadRaw (storedStorage val) (headTypeID @a Proxy) hid (sel . Head hid . wrappedLoad) cb

-- | Watch the given head using raw IDs and a selector from `Ref'.
watchHeadRaw :: forall b. Eq b => Storage -> HeadTypeID -> HeadID -> (Ref -> b) -> (b -> IO ()) -> IO WatchedHead
watchHeadRaw st@Storage {..} tid hid sel cb = do
    memo <- newEmptyMVar
    let cb' dgst = do
            let x = sel (Ref st dgst)
            modifyMVar_ memo $ \prev -> do
                when (Just x /= prev) $ cb x
                return $ Just x
    wid <- backendWatchHead stBackend tid hid cb'

    cur <- fmap sel <$> loadHeadRaw st tid hid
    maybe (return ()) cb cur
    putMVar memo cur

    return $ WatchedHead st wid memo

-- | Stop watching previously watched head.
unwatchHead :: WatchedHead -> IO ()
unwatchHead (WatchedHead Storage {..} wid _) = do
    backendUnwatchHead stBackend wid

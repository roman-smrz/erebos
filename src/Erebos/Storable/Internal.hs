module Erebos.Storable.Internal (
    Storable(..), ZeroStorable(..),
    StorableText(..), StorableDate(..), StorableUUID(..),

    Stored(..),
    fromStored, storedRef,
    storedStorage,
    wrappedStore, wrappedLoad,
    copyStored,
    unsafeMapStored,

    collectObjects, collectStoredObjects,

    MonadStorage(..),
) where

import Control.Monad.Reader

import Data.Function
import Data.Set (Set)
import Data.Set qualified as S

import Erebos.Storage.Internal

import Erebos.Object.Internal


data Stored a = Stored
    { storedRef' :: Ref
    , storedObject' :: a
    }
    deriving (Show)

instance Eq (Stored a) where
    (==)  =  (==) `on` (refDigest . storedRef')

instance Ord (Stored a) where
    compare  =  compare `on` (refDigest . storedRef')

instance Storable a => Storable (Stored a) where
    store st = copyRef st . storedRef
    store' (Stored _ x) = store' x
    load' = Stored <$> loadCurrentRef <*> load'

instance ZeroStorable a => ZeroStorable (Stored a) where
    fromZero st = Stored (zeroRef st) $ fromZero st

fromStored :: Stored a -> a
fromStored = storedObject'

storedRef :: Stored a -> Ref
storedRef = storedRef'

storedStorage :: Stored a -> Storage
storedStorage = refStorage . storedRef'

wrappedStore :: MonadIO m => Storable a => Storage -> a -> m (Stored a)
wrappedStore st x = do ref <- liftIO $ store st x
                       return $ Stored ref x

wrappedLoad :: Storable a => Ref -> Stored a
wrappedLoad ref = Stored ref (load ref)

copyStored :: forall m a. MonadIO m => Storage -> Stored a -> m (Stored a)
copyStored st (Stored ref' x) = liftIO $ returnLoadResult . fmap (\r -> Stored r x) <$> copyRef' st ref'

-- |Passed function needs to preserve the object representation to be safe
unsafeMapStored :: (a -> b) -> Stored a -> Stored b
unsafeMapStored f (Stored ref x) = Stored ref (f x)

collectStoredObjects :: Stored Object -> [ Stored Object ]
collectStoredObjects obj = obj : (fst $ collectOtherStored S.empty $ fromStored obj)


collectObjects :: Object -> [Object]
collectObjects obj = obj : map fromStored (fst $ collectOtherStored S.empty obj)

collectOtherStored :: Set RefDigest -> Object -> ( [ Stored Object ], Set RefDigest )
collectOtherStored seen (Rec items) = foldr helper ( [], seen ) $ map snd items
    where helper (RecRef ref) (xs, s)
              | r <- refDigest ref
              , r `S.notMember` s
              = let o = wrappedLoad ref
                    (xs', s') = collectOtherStored (S.insert r s) $ fromStored o
                 in ((o : xs') ++ xs, s')
          helper _ ( xs, s ) = ( xs, s )
collectOtherStored seen _ = ( [], seen )


class Monad m => MonadStorage m where
    getStorage :: m Storage
    mstore :: Storable a => a -> m (Stored a)

    default mstore :: MonadIO m => Storable a => a -> m (Stored a)
    mstore x = do
        st <- getStorage
        wrappedStore st x

instance MonadIO m => MonadStorage (ReaderT Storage m) where
    getStorage = ask

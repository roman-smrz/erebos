module Erebos.Storage.Internal where

import Control.Arrow
import Control.Concurrent
import Control.DeepSeq
import Control.Exception
import Control.Monad.Identity

import Crypto.Hash

import Data.Bits
import Data.ByteArray (ByteArrayAccess, ScrubbedBytes)
import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.HashTable.IO qualified as HT
import Data.Hashable
import Data.Kind
import Data.Typeable

import Foreign.Storable (peek)

import System.IO.Unsafe (unsafePerformIO)

import Erebos.UUID (UUID)
import Erebos.Util


data Storage' c = forall bck. (StorageBackend bck, BackendCompleteness bck ~ c) => Storage
    { stBackend :: bck
    , stRefGeneration :: MVar (HT.BasicHashTable RefDigest Generation)
    , stRefRoots :: MVar (HT.BasicHashTable RefDigest [RefDigest])
    }

type Storage = Storage' Complete
type PartialStorage = Storage' Partial

instance Eq (Storage' c) where
    Storage { stBackend = b } == Storage { stBackend = b' }
        | Just b'' <- cast b' =  b == b''
        | otherwise           =  False

instance Show (Storage' c) where
    show Storage { stBackend = b } = show b ++ showParentStorage b

showParentStorage :: StorageBackend bck => bck -> String
showParentStorage bck
    | Just (st :: Storage) <- cast (backendParent bck) = "@" ++ show st
    | Just (st :: PartialStorage) <- cast (backendParent bck) = "@" ++ show st
    | otherwise = ""


class (Eq bck, Show bck, Typeable bck, Typeable (BackendParent bck)) => StorageBackend bck where
    type BackendCompleteness bck :: Type -> Type
    type BackendCompleteness bck = Complete

    type BackendParent bck :: Type
    type BackendParent bck = ()
    backendParent :: bck -> BackendParent bck
    default backendParent :: BackendParent bck ~ () => bck -> BackendParent bck
    backendParent _ = ()


    backendLoadBytes :: bck -> RefDigest -> IO (Maybe BL.ByteString)
    default backendLoadBytes :: BackendParent bck ~ Storage => bck -> RefDigest -> IO (Maybe BL.ByteString)
    backendLoadBytes bck = case backendParent bck of Storage { stBackend = bck' } -> backendLoadBytes bck'

    backendStoreBytes :: bck -> RefDigest -> BL.ByteString -> IO ()
    default backendStoreBytes :: BackendParent bck ~ Storage => bck -> RefDigest -> BL.ByteString -> IO ()
    backendStoreBytes bck = case backendParent bck of Storage { stBackend = bck' } -> backendStoreBytes bck'


    backendLoadHeads :: bck -> HeadTypeID -> IO [ ( HeadID, RefDigest ) ]
    default backendLoadHeads :: BackendParent bck ~ Storage => bck -> HeadTypeID -> IO [ ( HeadID, RefDigest ) ]
    backendLoadHeads bck = case backendParent bck of Storage { stBackend = bck' } -> backendLoadHeads bck'

    backendLoadHead :: bck -> HeadTypeID -> HeadID -> IO (Maybe RefDigest)
    default backendLoadHead :: BackendParent bck ~ Storage => bck -> HeadTypeID -> HeadID -> IO (Maybe RefDigest)
    backendLoadHead bck = case backendParent bck of Storage { stBackend = bck' } -> backendLoadHead bck'

    backendStoreHead :: bck -> HeadTypeID -> HeadID -> RefDigest -> IO ()
    default backendStoreHead :: BackendParent bck ~ Storage => bck -> HeadTypeID -> HeadID -> RefDigest -> IO ()
    backendStoreHead bck = case backendParent bck of Storage { stBackend = bck' } -> backendStoreHead bck'

    backendReplaceHead :: bck -> HeadTypeID -> HeadID -> RefDigest -> RefDigest -> IO (Either (Maybe RefDigest) RefDigest)
    default backendReplaceHead :: BackendParent bck ~ Storage => bck -> HeadTypeID -> HeadID -> RefDigest -> RefDigest -> IO (Either (Maybe RefDigest) RefDigest)
    backendReplaceHead bck = case backendParent bck of Storage { stBackend = bck' } -> backendReplaceHead bck'

    backendWatchHead :: bck -> HeadTypeID -> HeadID -> (RefDigest -> IO ()) -> IO WatchID
    default backendWatchHead :: BackendParent bck ~ Storage => bck -> HeadTypeID -> HeadID -> (RefDigest -> IO ()) -> IO WatchID
    backendWatchHead bck = case backendParent bck of Storage { stBackend = bck' } -> backendWatchHead bck'

    backendUnwatchHead :: bck -> WatchID -> IO ()
    default backendUnwatchHead :: BackendParent bck ~ Storage => bck -> WatchID -> IO ()
    backendUnwatchHead bck = case backendParent bck of Storage { stBackend = bck' } -> backendUnwatchHead bck'


    backendListKeys :: bck -> IO [ RefDigest ]
    default backendListKeys :: BackendParent bck ~ Storage => bck -> IO [ RefDigest ]
    backendListKeys bck = case backendParent bck of Storage { stBackend = bck' } -> backendListKeys bck'

    backendLoadKey :: bck -> RefDigest -> IO (Maybe ScrubbedBytes)
    default backendLoadKey :: BackendParent bck ~ Storage => bck -> RefDigest -> IO (Maybe ScrubbedBytes)
    backendLoadKey bck = case backendParent bck of Storage { stBackend = bck' } -> backendLoadKey bck'

    backendStoreKey :: bck -> RefDigest -> ScrubbedBytes -> IO ()
    default backendStoreKey :: BackendParent bck ~ Storage => bck -> RefDigest -> ScrubbedBytes -> IO ()
    backendStoreKey bck = case backendParent bck of Storage { stBackend = bck' } -> backendStoreKey bck'

    backendRemoveKey :: bck -> RefDigest -> IO ()
    default backendRemoveKey :: BackendParent bck ~ Storage => bck -> RefDigest -> IO ()
    backendRemoveKey bck = case backendParent bck of Storage { stBackend = bck' } -> backendRemoveKey bck'



newtype WatchID = WatchID Int
    deriving (Eq, Ord)

startWatchID :: WatchID
startWatchID = WatchID 1

nextWatchID :: WatchID -> WatchID
nextWatchID (WatchID n) = WatchID (n + 1)

data WatchList = WatchList
    { wlNext :: WatchID
    , wlList :: [ WatchListItem ]
    }

data WatchListItem = WatchListItem
    { wlID :: WatchID
    , wlHead :: ( HeadTypeID, HeadID )
    , wlFun :: RefDigest -> IO ()
    }

watchListAdd :: HeadTypeID -> HeadID -> (RefDigest -> IO ()) -> WatchList -> ( WatchList, WatchID )
watchListAdd tid hid cb wl = ( wl', wlNext wl )
  where
    wl' = wl
        { wlNext = nextWatchID (wlNext wl)
        , wlList = WatchListItem
            { wlID = wlNext wl
            , wlHead = (tid, hid)
            , wlFun = cb
            } : wlList wl
        }

watchListDel :: WatchID -> WatchList -> WatchList
watchListDel wid wl = wl { wlList = filter ((/= wid) . wlID) $ wlList wl }


newtype RefDigest = RefDigest (Digest Blake2b_256)
    deriving (Eq, Ord, NFData, ByteArrayAccess)

instance Show RefDigest where
    show = BC.unpack . showRefDigest

data Ref' c = Ref (Storage' c) RefDigest

type Ref = Ref' Complete
type PartialRef = Ref' Partial

instance Eq (Ref' c) where
    Ref _ d1 == Ref _ d2  =  d1 == d2

instance Show (Ref' c) where
    show ref@(Ref st _) = show st ++ ":" ++ BC.unpack (showRef ref)

instance ByteArrayAccess (Ref' c) where
    length (Ref _ dgst) = BA.length dgst
    withByteArray (Ref _ dgst) = BA.withByteArray dgst

instance Hashable RefDigest where
    hashWithSalt salt ref = salt `xor` unsafePerformIO (BA.withByteArray ref peek)

instance Hashable (Ref' c) where
    hashWithSalt salt ref = salt `xor` unsafePerformIO (BA.withByteArray ref peek)

refStorage :: Ref' c -> Storage' c
refStorage (Ref st _) = st

refDigest :: Ref' c -> RefDigest
refDigest (Ref _ dgst) = dgst

showRef :: Ref' c -> ByteString
showRef = showRefDigest . refDigest

showRefDigestParts :: RefDigest -> (ByteString, ByteString)
showRefDigestParts x = (BC.pack "blake2", showHex x)

showRefDigest :: RefDigest -> ByteString
showRefDigest = showRefDigestParts >>> \(alg, hex) -> alg <> BC.pack "#" <> hex

readRefDigest :: ByteString -> Maybe RefDigest
readRefDigest x = case BC.split '#' x of
                       [alg, dgst] | BA.convert alg == BC.pack "blake2" ->
                           refDigestFromByteString =<< readHex dgst
                       _ -> Nothing

refDigestFromByteString :: ByteString -> Maybe RefDigest
refDigestFromByteString = fmap RefDigest . digestFromByteString

hashToRefDigest :: BL.ByteString -> RefDigest
hashToRefDigest = RefDigest . hashFinalize . hashUpdates hashInit . BL.toChunks


newtype Generation = Generation Int
    deriving (Eq, Show)

-- | UUID of individual Erebos storage head.
newtype HeadID = HeadID UUID
    deriving (Eq, Ord, Show)

-- | UUID of Erebos storage head type.
newtype HeadTypeID = HeadTypeID UUID
    deriving (Eq, Ord)

data Stored' c a = Stored (Ref' c) a
    deriving (Show)

instance Eq (Stored' c a) where
    Stored r1 _ == Stored r2 _  =  refDigest r1 == refDigest r2

instance Ord (Stored' c a) where
    compare (Stored r1 _) (Stored r2 _) = compare (refDigest r1) (refDigest r2)

storedStorage :: Stored' c a -> Storage' c
storedStorage (Stored (Ref st _) _) = st


type Complete = Identity
type Partial = Either RefDigest

class (Traversable compl, Monad compl, Typeable compl) => StorageCompleteness compl where
    type LoadResult compl a :: Type
    returnLoadResult :: compl a -> LoadResult compl a
    ioLoadBytes :: Ref' compl -> IO (compl BL.ByteString)

instance StorageCompleteness Complete where
    type LoadResult Complete a = a
    returnLoadResult = runIdentity
    ioLoadBytes ref@(Ref st dgst) = maybe (error $ "Ref not found in complete storage: "++show ref) Identity
        <$> ioLoadBytesFromStorage st dgst

instance StorageCompleteness Partial where
    type LoadResult Partial a = Either RefDigest a
    returnLoadResult = id
    ioLoadBytes (Ref st dgst) = maybe (Left dgst) Right <$> ioLoadBytesFromStorage st dgst

unsafeStoreRawBytes :: Storage' c -> BL.ByteString -> IO (Ref' c)
unsafeStoreRawBytes st@Storage {..} raw = do
    dgst <- evaluate $ force $ hashToRefDigest raw
    backendStoreBytes stBackend dgst raw
    return $ Ref st dgst

ioLoadBytesFromStorage :: Storage' c -> RefDigest -> IO (Maybe BL.ByteString)
ioLoadBytesFromStorage Storage {..} dgst =
    backendLoadBytes stBackend dgst >>= \case
        Just bytes -> return $ Just bytes
        Nothing
            | Just (parent :: Storage) <- cast (backendParent stBackend) -> ioLoadBytesFromStorage parent dgst
            | Just (parent :: PartialStorage) <- cast (backendParent stBackend) -> ioLoadBytesFromStorage parent dgst
            | otherwise -> return Nothing

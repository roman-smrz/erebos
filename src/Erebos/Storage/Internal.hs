{-# LANGUAGE CPP #-}

module Erebos.Storage.Internal where

import Codec.Compression.Zlib

import Control.Arrow
import Control.Concurrent
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Monad.Identity

import Crypto.Hash

import Data.Bits
import Data.ByteArray (ByteArray, ByteArrayAccess, ScrubbedBytes)
import qualified Data.ByteArray as BA
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Char
import Data.Function
import Data.Hashable
import qualified Data.HashTable.IO as HT
import Data.Kind
import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.UUID (UUID)

import Foreign.Storable (peek)

import System.Directory
import System.FilePath
import System.INotify (INotify)
import System.IO
import System.IO.Error
import System.IO.Unsafe (unsafePerformIO)
import System.Posix.Files
import System.Posix.IO


data Storage' c = Storage
    { stBacking :: StorageBacking c
    , stParent :: Maybe (Storage' Identity)
    , stRefGeneration :: MVar (HT.BasicHashTable RefDigest Generation)
    , stRefRoots :: MVar (HT.BasicHashTable RefDigest [RefDigest])
    }

instance Eq (Storage' c) where
    (==) = (==) `on` (stBacking &&& stParent)

instance Show (Storage' c) where
    show st@(Storage { stBacking = StorageDir { dirPath = path }}) = "dir" ++ showParentStorage st ++ ":" ++ path
    show st@(Storage { stBacking = StorageMemory {} }) = "mem" ++ showParentStorage st

showParentStorage :: Storage' c -> String
showParentStorage Storage { stParent = Nothing } = ""
showParentStorage Storage { stParent = Just st } = "@" ++ show st

data StorageBacking c
         = StorageDir { dirPath :: FilePath
                      , dirWatchers :: MVar ([(HeadTypeID, INotify)], WatchList c)
                      }
         | StorageMemory { memHeads :: MVar [((HeadTypeID, HeadID), Ref' c)]
                         , memObjs :: MVar (Map RefDigest BL.ByteString)
                         , memKeys :: MVar (Map RefDigest ScrubbedBytes)
                         , memWatchers :: MVar (WatchList c)
                         }
    deriving (Eq)

newtype WatchID = WatchID Int
    deriving (Eq, Ord, Num)

data WatchList c = WatchList
    { wlNext :: WatchID
    , wlList :: [WatchListItem c]
    }

data WatchListItem c = WatchListItem
    { wlID :: WatchID
    , wlHead :: (HeadTypeID, HeadID)
    , wlFun :: Ref' c -> IO ()
    }


newtype RefDigest = RefDigest (Digest Blake2b_256)
    deriving (Eq, Ord, NFData, ByteArrayAccess)

instance Show RefDigest where
    show = BC.unpack . showRefDigest

data Ref' c = Ref (Storage' c) RefDigest

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
                           refDigestFromByteString =<< readHex @ByteString dgst
                       _ -> Nothing

refDigestFromByteString :: ByteArrayAccess ba => ba -> Maybe RefDigest
refDigestFromByteString = fmap RefDigest . digestFromByteString

hashToRefDigest :: BL.ByteString -> RefDigest
hashToRefDigest = RefDigest . hashFinalize . hashUpdates hashInit . BL.toChunks

showHex :: ByteArrayAccess ba => ba -> ByteString
showHex = B.concat . map showHexByte . BA.unpack
    where showHexChar x | x < 10    = x + o '0'
                        | otherwise = x + o 'a' - 10
          showHexByte x = B.pack [ showHexChar (x `div` 16), showHexChar (x `mod` 16) ]
          o = fromIntegral . ord

readHex :: ByteArray ba => ByteString -> Maybe ba
readHex = return . BA.concat <=< readHex'
    where readHex' bs | B.null bs = Just []
          readHex' bs = do (bx, bs') <- B.uncons bs
                           (by, bs'') <- B.uncons bs'
                           x <- hexDigit bx
                           y <- hexDigit by
                           (B.singleton (x * 16 + y) :) <$> readHex' bs''
          hexDigit x | x >= o '0' && x <= o '9' = Just $ x - o '0'
                     | x >= o 'a' && x <= o 'z' = Just $ x - o 'a' + 10
                     | otherwise                = Nothing
          o = fromIntegral . ord


newtype Generation = Generation Int
    deriving (Eq, Show)

data Head' c a = Head HeadID (Stored' c a)
    deriving (Eq, Show)

newtype HeadID = HeadID UUID
    deriving (Eq, Ord, Show)

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

class (Traversable compl, Monad compl) => StorageCompleteness compl where
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
unsafeStoreRawBytes st raw = do
    let dgst = hashToRefDigest raw
    case stBacking st of
         StorageDir { dirPath = sdir } -> writeFileOnce (refPath sdir dgst) $ compress raw
         StorageMemory { memObjs = tobjs } ->
             dgst `deepseq` -- the TVar may be accessed when evaluating the data to be written
                 modifyMVar_ tobjs (return . M.insert dgst raw)
    return $ Ref st dgst

ioLoadBytesFromStorage :: Storage' c -> RefDigest -> IO (Maybe BL.ByteString)
ioLoadBytesFromStorage st dgst = loadCurrent st >>=
    \case Just bytes -> return $ Just bytes
          Nothing | Just parent <- stParent st -> ioLoadBytesFromStorage parent dgst
                  | otherwise                  -> return Nothing
    where loadCurrent Storage { stBacking = StorageDir { dirPath = spath } } = handleJust (guard . isDoesNotExistError) (const $ return Nothing) $
              Just . decompress . BL.fromChunks . (:[]) <$> (B.readFile $ refPath spath dgst)
          loadCurrent Storage { stBacking = StorageMemory { memObjs = tobjs } } = M.lookup dgst <$> readMVar tobjs

refPath :: FilePath -> RefDigest -> FilePath
refPath spath rdgst = intercalate "/" [spath, "objects", BC.unpack alg, pref, rest]
    where (alg, dgst) = showRefDigestParts rdgst
          (pref, rest) = splitAt 2 $ BC.unpack dgst


openLockFile :: FilePath -> IO Handle
openLockFile path = do
    createDirectoryIfMissing True (takeDirectory path)
    fd <- retry 10 $
#if MIN_VERSION_unix(2,8,0)
        openFd path WriteOnly defaultFileFlags
            { creat = Just $ unionFileModes ownerReadMode ownerWriteMode
            , exclusive = True
            }
#else
        openFd path WriteOnly (Just $ unionFileModes ownerReadMode ownerWriteMode) (defaultFileFlags { exclusive = True })
#endif
    fdToHandle fd
  where
    retry :: Int -> IO a -> IO a
    retry 0 act = act
    retry n act = catchJust (\e -> if isAlreadyExistsError e then Just () else Nothing)
                      act (\_ -> threadDelay (100 * 1000) >> retry (n - 1) act)

writeFileOnce :: FilePath -> BL.ByteString -> IO ()
writeFileOnce file content = bracket (openLockFile locked)
    hClose $ \h -> do
        fileExist file >>= \case
            True  -> removeLink locked
            False -> do BL.hPut h content
                        hFlush h
                        rename locked file
    where locked = file ++ ".lock"

writeFileChecked :: FilePath -> Maybe ByteString -> ByteString -> IO (Either (Maybe ByteString) ())
writeFileChecked file prev content = bracket (openLockFile locked)
    hClose $ \h -> do
        (prev,) <$> fileExist file >>= \case
            (Nothing, True) -> do
                current <- B.readFile file
                removeLink locked
                return $ Left $ Just current
            (Nothing, False) -> do B.hPut h content
                                   hFlush h
                                   rename locked file
                                   return $ Right ()
            (Just expected, True) -> do
                current <- B.readFile file
                if current == expected then do B.hPut h content
                                               hFlush h
                                               rename locked file
                                               return $ return ()
                                       else do removeLink locked
                                               return $ Left $ Just current
            (Just _, False) -> do
                removeLink locked
                return $ Left Nothing
    where locked = file ++ ".lock"

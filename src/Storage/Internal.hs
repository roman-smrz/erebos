module Storage.Internal where

import Codec.Compression.Zlib

import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Identity

import Crypto.Hash

import Data.ByteArray (ByteArrayAccess, ScrubbedBytes)
import qualified Data.ByteArray as BA
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.List
import Data.Map (Map)
import qualified Data.Map as M

import System.Directory
import System.FilePath
import System.INotify (INotify)
import System.IO
import System.IO.Error
import System.Posix.Files
import System.Posix.IO
import System.Posix.Types


data Storage' c = Storage
    { stBacking :: StorageBacking c
    , stParent :: Maybe (Storage' Identity)
    }
    deriving (Eq)

instance Show (Storage' c) where
    show st@(Storage { stBacking = StorageDir { dirPath = path }}) = "dir" ++ showParentStorage st ++ ":" ++ path
    show st@(Storage { stBacking = StorageMemory {} }) = "mem" ++ showParentStorage st

showParentStorage :: Storage' c -> String
showParentStorage Storage { stParent = Nothing } = ""
showParentStorage Storage { stParent = Just st } = "@" ++ show st

data StorageBacking c
         = StorageDir { dirPath :: FilePath
                      , dirWatchers :: MVar (Maybe INotify, [(String, Head' c -> IO ())])
                      }
         | StorageMemory { memHeads :: MVar [Head' c]
                         , memObjs :: MVar (Map RefDigest BL.ByteString)
                         , memKeys :: MVar (Map RefDigest ScrubbedBytes)
                         , memWatchers :: MVar [(String, Head' c -> IO ())]
                         }
    deriving (Eq)


type RefDigest = Digest Blake2b_256

data Ref' c = Ref (Storage' c) RefDigest
    deriving (Eq)

instance Show (Ref' c) where
    show ref@(Ref st _) = show st ++ ":" ++ BC.unpack (showRef ref)

instance ByteArrayAccess (Ref' c) where
    length (Ref _ dgst) = BA.length dgst
    withByteArray (Ref _ dgst) = BA.withByteArray dgst

refStorage :: Ref' c -> Storage' c
refStorage (Ref st _) = st

refDigest :: Ref' c -> RefDigest
refDigest (Ref _ dgst) = dgst

showRef :: Ref' c -> ByteString
showRef = showRefDigest . refDigest

showRefDigest :: RefDigest -> ByteString
showRefDigest = B.concat . map showHexByte . BA.unpack
    where showHex x | x < 10    = x + 48
                    | otherwise = x + 87
          showHexByte x = B.pack [ showHex (x `div` 16), showHex (x `mod` 16) ]


data Head' c = Head String (Ref' c)
    deriving (Show)


type Complete = Identity
type Partial = Either RefDigest

class (Traversable compl, Monad compl) => StorageCompleteness compl where
    type LoadResult compl a :: *
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

ioLoadBytesFromStorage :: Storage' c -> RefDigest -> IO (Maybe BL.ByteString)
ioLoadBytesFromStorage st dgst = loadCurrent st >>=
    \case Just bytes -> return $ Just bytes
          Nothing | Just parent <- stParent st -> ioLoadBytesFromStorage parent dgst
                  | otherwise                  -> return Nothing
    where loadCurrent Storage { stBacking = StorageDir { dirPath = spath } } = handleJust (guard . isDoesNotExistError) (const $ return Nothing) $
              Just . decompress <$> (BL.readFile $ refPath spath dgst)
          loadCurrent Storage { stBacking = StorageMemory { memObjs = tobjs } } = M.lookup dgst <$> readMVar tobjs

refPath :: FilePath -> RefDigest -> FilePath
refPath spath dgst = intercalate "/" [spath, "objects", pref, rest]
    where (pref, rest) = splitAt 2 $ BC.unpack $ showRefDigest dgst


openFdParents :: FilePath -> OpenMode -> Maybe FileMode -> OpenFileFlags -> IO Fd
openFdParents path omode fmode flags = do
    createDirectoryIfMissing True (takeDirectory path)
    openFd path omode fmode flags

writeFileOnce :: FilePath -> BL.ByteString -> IO ()
writeFileOnce file content = bracket
    (fdToHandle =<< openFdParents locked WriteOnly (Just $ unionFileModes ownerReadMode ownerWriteMode) (defaultFileFlags { exclusive = True }))
    hClose $ \h -> do
        fileExist file >>= \case
            True  -> removeLink locked
            False -> do BL.hPut h content
                        rename locked file
    where locked = file ++ ".lock"

writeFileChecked :: FilePath -> Maybe ByteString -> ByteString -> IO (Either (Maybe ByteString) ())
writeFileChecked file prev content = bracket
    (fdToHandle =<< openFdParents locked WriteOnly (Just $ unionFileModes ownerReadMode ownerWriteMode) (defaultFileFlags { exclusive = True }))
    hClose $ \h -> do
        (prev,) <$> fileExist file >>= \case
            (Nothing, True) -> do
                current <- B.readFile file
                removeLink locked
                return $ Left $ Just current
            (Nothing, False) -> do B.hPut h content
                                   rename locked file
                                   return $ Right ()
            (Just expected, True) -> do
                current <- B.readFile file
                if current == expected then do B.hPut h content
                                               rename locked file
                                               return $ return ()
                                       else do removeLink locked
                                               return $ Left $ Just current
            (Just _, False) -> do
                removeLink locked
                return $ Left Nothing
    where locked = file ++ ".lock"

module Storage.Internal where

import Control.Exception

import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as BL

import System.Directory
import System.FilePath
import System.IO
import System.Posix.Files
import System.Posix.IO
import System.Posix.Types

data Storage = Storage FilePath
    deriving (Eq, Ord)


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

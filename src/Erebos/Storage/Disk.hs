module Erebos.Storage.Disk (
    openStorage,
) where

import Codec.Compression.Zlib

import Control.Arrow
import Control.Concurrent
import Control.Exception
import Control.Monad

import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.ByteString.Char8 qualified as BC
import Data.ByteString.Lazy qualified as BL
import Data.ByteString.Lazy.Char8 qualified as BLC
import Data.Function
import Data.List
import Data.Maybe
import Data.UUID qualified as U

import System.Directory
import System.FSNotify
import System.FilePath
import System.IO
import System.IO.Error

import Erebos.Object
import Erebos.Storage.Backend
import Erebos.Storage.Head
import Erebos.Storage.Internal
import Erebos.Storage.Platform


data DiskStorage = StorageDir
    { dirPath :: FilePath
    , dirWatchers :: MVar ( Maybe WatchManager, [ HeadTypeID ], WatchList )
    }

instance Eq DiskStorage where
    (==) = (==) `on` dirPath

instance Show DiskStorage where
    show StorageDir { dirPath = path } = "dir:" ++ path

instance StorageBackend DiskStorage where
    backendLoadBytes StorageDir {..} dgst =
        handleJust (guard . isDoesNotExistError) (const $ return Nothing) $
              Just . decompress . BL.fromChunks . (:[]) <$> (B.readFile $ refPath dirPath dgst)
    backendStoreBytes StorageDir {..} dgst = writeFileOnce (refPath dirPath dgst) . compress


    backendLoadHeads StorageDir {..} tid = do
        let hpath = headTypePath dirPath tid

        files <- filterM (doesFileExist . (hpath </>)) =<<
            handleJust (\e -> guard (isDoesNotExistError e)) (const $ return [])
            (getDirectoryContents hpath)
        fmap catMaybes $ forM files $ \hname -> do
            case U.fromString hname of
                 Just hid -> do
                     content <- B.readFile (hpath </> hname)
                     return $ do
                         (h : _) <- Just (BC.lines content)
                         dgst <- readRefDigest h
                         Just $ ( HeadID hid, dgst )
                 Nothing -> return Nothing

    backendLoadHead StorageDir {..} tid hid = do
        handleJust (guard . isDoesNotExistError) (const $ return Nothing) $ do
            (h:_) <- BC.lines <$> B.readFile (headPath dirPath tid hid)
            return $ readRefDigest h

    backendStoreHead StorageDir {..} tid hid dgst = do
         Right () <- writeFileChecked (headPath dirPath tid hid) Nothing $
             showRefDigest dgst `B.append` BC.singleton '\n'
         return ()

    backendReplaceHead StorageDir {..} tid hid expected new = do
         let filename = headPath dirPath tid hid
             showDgstL r = showRefDigest r `B.append` BC.singleton '\n'

         writeFileChecked filename (Just $ showDgstL expected) (showDgstL new) >>= \case
             Left Nothing -> return $ Left Nothing
             Left (Just bs) -> do Just cur <- return $ readRefDigest $ BC.takeWhile (/='\n') bs
                                  return $ Left $ Just cur
             Right () -> return $ Right new

    backendWatchHead st@StorageDir {..} tid hid cb = do
        modifyMVar dirWatchers $ \( mbmanager, ilist, wl ) -> do
            manager <- maybe startManager return mbmanager
            ilist' <- case tid `elem` ilist of
                True -> return ilist
                False -> do
                    void $ watchDir manager (headTypePath dirPath tid) (const True) $ \case
                        Added { eventPath = fpath } | Just ihid <- HeadID <$> U.fromString (takeFileName fpath) -> do
                            backendLoadHead st tid ihid >>= \case
                                Just dgst -> do
                                    (_, _, iwl) <- readMVar dirWatchers
                                    mapM_ ($ dgst) . map wlFun . filter ((== (tid, ihid)) . wlHead) . wlList $ iwl
                                Nothing -> return ()
                        _ -> return ()
                    return $ tid : ilist
            return $ first ( Just manager, ilist', ) $ watchListAdd tid hid cb wl

    backendUnwatchHead StorageDir {..} wid = do
        modifyMVar_ dirWatchers $ \( mbmanager, ilist, wl ) -> do
            return ( mbmanager, ilist, watchListDel wid wl )


    backendListKeys StorageDir {..} = do
        catMaybes . map (readRefDigest . BC.pack) <$>
            listDirectory (keyDirPath dirPath)

    backendLoadKey StorageDir {..} dgst = do
        tryIOError (BC.readFile (keyFilePath dirPath dgst)) >>= \case
            Right kdata -> return $ Just $ BA.convert kdata
            Left _ -> return Nothing

    backendStoreKey StorageDir {..} dgst key = do
        writeFileOnce (keyFilePath dirPath dgst) (BL.fromStrict $ BA.convert key)

    backendRemoveKey StorageDir {..} dgst = do
        void $ tryIOError (removeFile $ keyFilePath dirPath dgst)


storageVersion :: String
storageVersion = "0.1"

openStorage :: FilePath -> IO Storage
openStorage path = modifyIOError annotate $ do
    let versionFileName = "erebos-storage"
    let versionPath = path </> versionFileName
    let writeVersionFile = writeFileOnce versionPath $ BLC.pack $ storageVersion <> "\n"

    maybeVersion <- handleJust (guard . isDoesNotExistError) (const $ return Nothing) $
        Just <$> readFile versionPath
    version <- case maybeVersion of
        Just versionContent -> do
            return $ takeWhile (/= '\n') versionContent

        Nothing -> do
            files <- handleJust (guard . isDoesNotExistError) (const $ return []) $
                listDirectory path
            when (not $ or
                    [ null files
                    , versionFileName `elem` files
                    , (versionFileName ++ ".lock") `elem` files
                    , "objects" `elem` files && "heads" `elem` files
                    ]) $ do
                fail "directory is neither empty, nor an existing erebos storage"

            createDirectoryIfMissing True $ path
            writeVersionFile
            takeWhile (/= '\n') <$> readFile versionPath

    when (version /= storageVersion) $ do
        fail $ "unsupported storage version " <> version

    createDirectoryIfMissing True $ path </> "objects"
    createDirectoryIfMissing True $ path </> "heads"
    watchers <- newMVar ( Nothing, [], WatchList startWatchID [] )
    newStorage $ StorageDir path watchers
  where
    annotate e = annotateIOError e "failed to open storage" Nothing (Just path)


refPath :: FilePath -> RefDigest -> FilePath
refPath spath rdgst = intercalate "/" [ spath, "objects", BC.unpack alg, pref, rest ]
    where (alg, dgst) = showRefDigestParts rdgst
          (pref, rest) = splitAt 2 $ BC.unpack dgst

headTypePath :: FilePath -> HeadTypeID -> FilePath
headTypePath spath (HeadTypeID tid) = spath </> "heads" </> U.toString tid

headPath :: FilePath -> HeadTypeID -> HeadID -> FilePath
headPath spath tid (HeadID hid) = headTypePath spath tid </> U.toString hid

keyDirPath :: FilePath -> FilePath
keyDirPath sdir = sdir </> "keys"

keyFilePath :: FilePath -> RefDigest -> FilePath
keyFilePath sdir dgst = keyDirPath sdir </> (BC.unpack $ showRefDigest dgst)


openLockFile :: FilePath -> IO Handle
openLockFile path = do
    createDirectoryIfMissing True (takeDirectory path)
    retry 10 $ createFileExclusive path
  where
    retry :: Int -> IO a -> IO a
    retry 0 act = act
    retry n act = catchJust (\e -> if isAlreadyExistsError e then Just () else Nothing)
                      act (\_ -> threadDelay (100 * 1000) >> retry (n - 1) act)

writeFileOnce :: FilePath -> BL.ByteString -> IO ()
writeFileOnce file content = bracket (openLockFile locked)
    hClose $ \h -> do
        doesFileExist file >>= \case
            True  -> removeFile locked
            False -> do BL.hPut h content
                        hClose h
                        renameFile locked file
    where locked = file ++ ".lock"

writeFileChecked :: FilePath -> Maybe ByteString -> ByteString -> IO (Either (Maybe ByteString) ())
writeFileChecked file prev content = bracket (openLockFile locked)
    hClose $ \h -> do
        (prev,) <$> doesFileExist file >>= \case
            (Nothing, True) -> do
                current <- B.readFile file
                removeFile locked
                return $ Left $ Just current
            (Nothing, False) -> do B.hPut h content
                                   hClose h
                                   renameFile locked file
                                   return $ Right ()
            (Just expected, True) -> do
                current <- B.readFile file
                if current == expected then do B.hPut h content
                                               hClose h
                                               renameFile locked file
                                               return $ return ()
                                       else do removeFile locked
                                               return $ Left $ Just current
            (Just _, False) -> do
                removeFile locked
                return $ Left Nothing
    where locked = file ++ ".lock"

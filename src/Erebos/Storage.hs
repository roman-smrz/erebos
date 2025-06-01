module Erebos.Storage (
    Storage, PartialStorage, StorageCompleteness,
    openStorage, memoryStorage,
    deriveEphemeralStorage, derivePartialStorage,

    Ref, PartialRef, RefDigest,
    refDigest, refFromDigest,
    readRef, showRef, showRefDigest,
    refDigestFromByteString, hashToRefDigest,
    copyRef, partialRef, partialRefFromDigest,

    Object, PartialObject, Object'(..), RecItem, RecItem'(..),
    serializeObject, deserializeObject, deserializeObjects,
    ioLoadObject, ioLoadBytes,
    storeRawBytes, lazyLoadBytes,
    storeObject,
    collectObjects, collectStoredObjects,

    Head, HeadType(..),
    HeadTypeID, mkHeadTypeID,
    headId, headStorage, headRef, headObject, headStoredObject,
    loadHeads, loadHead, reloadHead,
    storeHead, replaceHead, updateHead, updateHead_,
    loadHeadRaw, storeHeadRaw, replaceHeadRaw,

    WatchedHead,
    watchHead, watchHeadWith, unwatchHead,
    watchHeadRaw,

    MonadStorage(..),

    Storable(..), ZeroStorable(..),
    StorableText(..), StorableDate(..), StorableUUID(..),

    Store, StoreRec,
    evalStore, evalStoreObject,
    storeBlob, storeRec, storeZero,
    storeEmpty, storeInt, storeNum, storeText, storeBinary, storeDate, storeUUID, storeRef, storeRawRef,
    storeMbEmpty, storeMbInt, storeMbNum, storeMbText, storeMbBinary, storeMbDate, storeMbUUID, storeMbRef, storeMbRawRef,
    storeZRef,
    storeRecItems,

    Load, LoadRec,
    evalLoad,
    loadCurrentRef, loadCurrentObject,
    loadRecCurrentRef, loadRecItems,

    loadBlob, loadRec, loadZero,
    loadEmpty, loadInt, loadNum, loadText, loadBinary, loadDate, loadUUID, loadRef, loadRawRef,
    loadMbEmpty, loadMbInt, loadMbNum, loadMbText, loadMbBinary, loadMbDate, loadMbUUID, loadMbRef, loadMbRawRef,
    loadTexts, loadBinaries, loadRefs, loadRawRefs,
    loadZRef,

    Stored,
    fromStored, storedRef,
    wrappedStore, wrappedLoad,
    copyStored,
    unsafeMapStored,

    StoreInfo(..), makeStoreInfo,

    StoredHistory,
    fromHistory, fromHistoryAt, storedFromHistory, storedHistoryList,
    beginHistory, modifyHistory,
) where

import Control.Applicative
import Control.Concurrent
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Writer

import Crypto.Hash

import Data.Bifunctor
import Data.ByteString (ByteString)
import qualified Data.ByteArray as BA
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Char
import Data.Function
import qualified Data.HashTable.IO as HT
import Data.List
import qualified Data.Map as M
import Data.Maybe
import Data.Ratio
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.Text.Encoding
import Data.Text.Encoding.Error
import Data.Time.Calendar
import Data.Time.Clock
import Data.Time.Format
import Data.Time.LocalTime
import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as U
import qualified Data.UUID.V4 as U

import System.Directory
import System.FSNotify
import System.FilePath
import System.IO.Error
import System.IO.Unsafe

import Erebos.Storage.Internal


type Storage = Storage' Complete
type PartialStorage = Storage' Partial

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
    watchers <- newMVar (Nothing, [], WatchList 1 [])
    refgen <- newMVar =<< HT.new
    refroots <- newMVar =<< HT.new
    return $ Storage
        { stBacking = StorageDir path watchers
        , stParent = Nothing
        , stRefGeneration = refgen
        , stRefRoots = refroots
        }
  where
    annotate e = annotateIOError e "failed to open storage" Nothing (Just path)

memoryStorage' :: IO (Storage' c')
memoryStorage' = do
    backing <- StorageMemory <$> newMVar [] <*> newMVar M.empty <*> newMVar M.empty <*> newMVar (WatchList 1 [])
    refgen <- newMVar =<< HT.new
    refroots <- newMVar =<< HT.new
    return $ Storage
        { stBacking = backing
        , stParent = Nothing
        , stRefGeneration = refgen
        , stRefRoots = refroots
        }

memoryStorage :: IO Storage
memoryStorage = memoryStorage'

deriveEphemeralStorage :: Storage -> IO Storage
deriveEphemeralStorage parent = do
    st <- memoryStorage
    return $ st { stParent = Just parent }

derivePartialStorage :: Storage -> IO PartialStorage
derivePartialStorage parent = do
    st <- memoryStorage'
    return $ st { stParent = Just parent }

type Ref = Ref' Complete
type PartialRef = Ref' Partial

zeroRef :: Storage' c -> Ref' c
zeroRef s = Ref s (RefDigest h)
    where h = case digestFromByteString $ B.replicate (hashDigestSize $ digestAlgo h) 0 of
                   Nothing -> error $ "Failed to create zero hash"
                   Just h' -> h'
          digestAlgo :: Digest a -> a
          digestAlgo = undefined

isZeroRef :: Ref' c -> Bool
isZeroRef (Ref _ h) = all (==0) $ BA.unpack h


refFromDigest :: Storage' c -> RefDigest -> IO (Maybe (Ref' c))
refFromDigest st dgst = fmap (const $ Ref st dgst) <$> ioLoadBytesFromStorage st dgst

readRef :: Storage -> ByteString -> IO (Maybe Ref)
readRef s b =
    case readRefDigest b of
         Nothing -> return Nothing
         Just dgst -> refFromDigest s dgst

copyRef' :: forall c c'. (StorageCompleteness c, StorageCompleteness c') => Storage' c' -> Ref' c -> IO (c (Ref' c'))
copyRef' st ref'@(Ref _ dgst) = refFromDigest st dgst >>= \case Just ref -> return $ return ref
                                                                Nothing  -> doCopy
    where doCopy = do mbobj' <- ioLoadObject ref'
                      mbobj <- sequence $ copyObject' st <$> mbobj'
                      sequence $ unsafeStoreObject st <$> join mbobj

copyRecItem' :: forall c c'. (StorageCompleteness c, StorageCompleteness c') => Storage' c' -> RecItem' c -> IO (c (RecItem' c'))
copyRecItem' st = \case
    RecEmpty -> return $ return $ RecEmpty
    RecInt x -> return $ return $ RecInt x
    RecNum x -> return $ return $ RecNum x
    RecText x -> return $ return $ RecText x
    RecBinary x -> return $ return $ RecBinary x
    RecDate x -> return $ return $ RecDate x
    RecUUID x -> return $ return $ RecUUID x
    RecRef x -> fmap RecRef <$> copyRef' st x
    RecUnknown t x -> return $ return $ RecUnknown t x

copyObject' :: forall c c'. (StorageCompleteness c, StorageCompleteness c') => Storage' c' -> Object' c -> IO (c (Object' c'))
copyObject' _ (Blob bs) = return $ return $ Blob bs
copyObject' st (Rec rs) = fmap Rec . sequence <$> mapM (\( n, item ) -> fmap ( n, ) <$> copyRecItem' st item) rs
copyObject' _ ZeroObject = return $ return ZeroObject
copyObject' _ (UnknownObject otype content) = return $ return $ UnknownObject otype content

copyRef :: forall c c' m. (StorageCompleteness c, StorageCompleteness c', MonadIO m) => Storage' c' -> Ref' c -> m (LoadResult c (Ref' c'))
copyRef st ref' = liftIO $ returnLoadResult <$> copyRef' st ref'

copyRecItem :: forall c c' m. (StorageCompleteness c, StorageCompleteness c', MonadIO m) => Storage' c' -> RecItem' c -> m (LoadResult c (RecItem' c'))
copyRecItem st item' = liftIO $ returnLoadResult <$> copyRecItem' st item'

copyObject :: forall c c'. (StorageCompleteness c, StorageCompleteness c') => Storage' c' -> Object' c -> IO (LoadResult c (Object' c'))
copyObject st obj' = returnLoadResult <$> copyObject' st obj'

partialRef :: PartialStorage -> Ref -> PartialRef
partialRef st (Ref _ dgst) = Ref st dgst

partialRefFromDigest :: PartialStorage -> RefDigest -> PartialRef
partialRefFromDigest st dgst = Ref st dgst


data Object' c
    = Blob ByteString
    | Rec [(ByteString, RecItem' c)]
    | ZeroObject
    | UnknownObject ByteString ByteString
    deriving (Show)

type Object = Object' Complete
type PartialObject = Object' Partial

data RecItem' c
    = RecEmpty
    | RecInt Integer
    | RecNum Rational
    | RecText Text
    | RecBinary ByteString
    | RecDate ZonedTime
    | RecUUID UUID
    | RecRef (Ref' c)
    | RecUnknown ByteString ByteString
    deriving (Show)

type RecItem = RecItem' Complete

serializeObject :: Object' c -> BL.ByteString
serializeObject = \case
    Blob cnt -> BL.fromChunks [BC.pack "blob ", BC.pack (show $ B.length cnt), BC.singleton '\n', cnt]
    Rec rec -> let cnt = BL.fromChunks $ concatMap (uncurry serializeRecItem) rec
                in BL.fromChunks [BC.pack "rec ", BC.pack (show $ BL.length cnt), BC.singleton '\n'] `BL.append` cnt
    ZeroObject -> BL.empty
    UnknownObject otype cnt -> BL.fromChunks [ otype, BC.singleton ' ', BC.pack (show $ B.length cnt), BC.singleton '\n', cnt ]

-- |Serializes and stores object data without ony dependencies, so is safe only
-- if all the referenced objects are already stored or reference is partial.
unsafeStoreObject :: Storage' c -> Object' c -> IO (Ref' c)
unsafeStoreObject storage = \case
    ZeroObject -> return $ zeroRef storage
    obj -> unsafeStoreRawBytes storage $ serializeObject obj

storeObject :: PartialStorage -> PartialObject -> IO PartialRef
storeObject = unsafeStoreObject

storeRawBytes :: PartialStorage -> BL.ByteString -> IO PartialRef
storeRawBytes = unsafeStoreRawBytes

serializeRecItem :: ByteString -> RecItem' c -> [ByteString]
serializeRecItem name (RecEmpty) = [name, BC.pack ":e", BC.singleton ' ', BC.singleton '\n']
serializeRecItem name (RecInt x) = [name, BC.pack ":i", BC.singleton ' ', BC.pack (show x), BC.singleton '\n']
serializeRecItem name (RecNum x) = [name, BC.pack ":n", BC.singleton ' ', BC.pack (showRatio x), BC.singleton '\n']
serializeRecItem name (RecText x) = [name, BC.pack ":t", BC.singleton ' ', escaped, BC.singleton '\n']
    where escaped = BC.concatMap escape $ encodeUtf8 x
          escape '\n' = BC.pack "\n\t"
          escape c    = BC.singleton c
serializeRecItem name (RecBinary x) = [name, BC.pack ":b ", showHex x, BC.singleton '\n']
serializeRecItem name (RecDate x) = [name, BC.pack ":d", BC.singleton ' ', BC.pack (formatTime defaultTimeLocale "%s %z" x), BC.singleton '\n']
serializeRecItem name (RecUUID x) = [name, BC.pack ":u", BC.singleton ' ', U.toASCIIBytes x, BC.singleton '\n']
serializeRecItem name (RecRef x) = [name, BC.pack ":r ", showRef x, BC.singleton '\n']
serializeRecItem name (RecUnknown t x) = [ name, BC.singleton ':', t, BC.singleton ' ', x, BC.singleton '\n' ]

lazyLoadObject :: forall c. StorageCompleteness c => Ref' c -> LoadResult c (Object' c)
lazyLoadObject = returnLoadResult . unsafePerformIO . ioLoadObject

ioLoadObject :: forall c. StorageCompleteness c => Ref' c -> IO (c (Object' c))
ioLoadObject ref | isZeroRef ref = return $ return ZeroObject
ioLoadObject ref@(Ref st rhash) = do
    file' <- ioLoadBytes ref
    return $ do
        file <- file'
        let chash = hashToRefDigest file
        when (chash /= rhash) $ error $ "Hash mismatch on object " ++ BC.unpack (showRef ref) {- TODO throw -}
        return $ case runExcept $ unsafeDeserializeObject st file of
                      Left err -> error $ err ++ ", ref " ++ BC.unpack (showRef ref) {- TODO throw -}
                      Right (x, rest) | BL.null rest -> x
                                      | otherwise -> error $ "Superfluous content after " ++ BC.unpack (showRef ref) {- TODO throw -}

lazyLoadBytes :: forall c. StorageCompleteness c => Ref' c -> LoadResult c BL.ByteString
lazyLoadBytes ref | isZeroRef ref = returnLoadResult (return BL.empty :: c BL.ByteString)
lazyLoadBytes ref = returnLoadResult $ unsafePerformIO $ ioLoadBytes ref

unsafeDeserializeObject :: Storage' c -> BL.ByteString -> Except String (Object' c, BL.ByteString)
unsafeDeserializeObject _  bytes | BL.null bytes = return (ZeroObject, bytes)
unsafeDeserializeObject st bytes =
    case BLC.break (=='\n') bytes of
        (line, rest) | Just (otype, len) <- splitObjPrefix line -> do
            let (content, next) = first BL.toStrict $ BL.splitAt (fromIntegral len) $ BL.drop 1 rest
            guard $ B.length content == len
            (,next) <$> case otype of
                 _ | otype == BC.pack "blob" -> return $ Blob content
                   | otype == BC.pack "rec" -> maybe (throwError $ "Malformed record item ")
                                                   (return . Rec) $ sequence $ map parseRecLine $ mergeCont [] $ BC.lines content
                   | otherwise -> return $ UnknownObject otype content
        _ -> throwError $ "Malformed object"
    where splitObjPrefix line = do
              [otype, tlen] <- return $ BLC.words line
              (len, rest) <- BLC.readInt tlen
              guard $ BL.null rest
              return (BL.toStrict otype, len)

          mergeCont cs (a:b:rest) | Just ('\t', b') <- BC.uncons b = mergeCont (b':BC.pack "\n":cs) (a:rest)
          mergeCont cs (a:rest) = B.concat (a : reverse cs) : mergeCont [] rest
          mergeCont _ [] = []

          parseRecLine line = do
              colon <- BC.elemIndex ':' line
              space <- BC.elemIndex ' ' line
              guard $ colon < space
              let name = B.take colon line
                  itype = B.take (space-colon-1) $ B.drop (colon+1) line
                  content = B.drop (space+1) line

              let val = fromMaybe (RecUnknown itype content) $
                      case BC.unpack itype of
                          "e" -> do guard $ B.null content
                                    return RecEmpty
                          "i" -> do (num, rest) <- BC.readInteger content
                                    guard $ B.null rest
                                    return $ RecInt num
                          "n" -> RecNum <$> parseRatio content
                          "t" -> return $ RecText $ decodeUtf8With lenientDecode content
                          "b" -> RecBinary <$> readHex content
                          "d" -> RecDate <$> parseTimeM False defaultTimeLocale "%s %z" (BC.unpack content)
                          "u" -> RecUUID <$> U.fromASCIIBytes content
                          "r" -> RecRef . Ref st <$> readRefDigest content
                          _   -> Nothing
              return (name, val)

deserializeObject :: PartialStorage -> BL.ByteString -> Except String (PartialObject, BL.ByteString)
deserializeObject = unsafeDeserializeObject

deserializeObjects :: PartialStorage -> BL.ByteString -> Except String [PartialObject]
deserializeObjects _  bytes | BL.null bytes = return []
deserializeObjects st bytes = do (obj, rest) <- deserializeObject st bytes
                                 (obj:) <$> deserializeObjects st rest


collectObjects :: Object -> [Object]
collectObjects obj = obj : map fromStored (fst $ collectOtherStored S.empty obj)

collectStoredObjects :: Stored Object -> [Stored Object]
collectStoredObjects obj = obj : (fst $ collectOtherStored S.empty $ fromStored obj)

collectOtherStored :: Set RefDigest -> Object -> ([Stored Object], Set RefDigest)
collectOtherStored seen (Rec items) = foldr helper ([], seen) $ map snd items
    where helper (RecRef ref) (xs, s) | r <- refDigest ref
                                      , r `S.notMember` s
                                      = let o = wrappedLoad ref
                                            (xs', s') = collectOtherStored (S.insert r s) $ fromStored o
                                         in ((o : xs') ++ xs, s')
          helper _          (xs, s) = (xs, s)
collectOtherStored seen _ = ([], seen)


type Head = Head' Complete

headId :: Head a -> HeadID
headId (Head uuid _) = uuid

headStorage :: Head a -> Storage
headStorage = refStorage . headRef

headRef :: Head a -> Ref
headRef (Head _ sx) = storedRef sx

headObject :: Head a -> a
headObject (Head _ sx) = fromStored sx

headStoredObject :: Head a -> Stored a
headStoredObject (Head _ sx) = sx

deriving instance StorableUUID HeadID
deriving instance StorableUUID HeadTypeID

mkHeadTypeID :: String -> HeadTypeID
mkHeadTypeID = maybe (error "Invalid head type ID") HeadTypeID . U.fromString

class Storable a => HeadType a where
    headTypeID :: proxy a -> HeadTypeID


headTypePath :: FilePath -> HeadTypeID -> FilePath
headTypePath spath (HeadTypeID tid) = spath </> "heads" </> U.toString tid

headPath :: FilePath -> HeadTypeID -> HeadID -> FilePath
headPath spath tid (HeadID hid) = headTypePath spath tid </> U.toString hid

loadHeads :: forall a m. MonadIO m => HeadType a => Storage -> m [Head a]
loadHeads s@(Storage { stBacking = StorageDir { dirPath = spath }}) = liftIO $ do
    let hpath = headTypePath spath $ headTypeID @a Proxy

    files <- filterM (doesFileExist . (hpath </>)) =<<
        handleJust (\e -> guard (isDoesNotExistError e)) (const $ return [])
        (getDirectoryContents hpath)
    fmap catMaybes $ forM files $ \hname -> do
        case U.fromString hname of
             Just hid -> do
                 (h:_) <- BC.lines <$> B.readFile (hpath </> hname)
                 Just ref <- readRef s h
                 return $ Just $ Head (HeadID hid) $ wrappedLoad ref
             Nothing -> return Nothing
loadHeads Storage { stBacking = StorageMemory { memHeads = theads } } = liftIO $ do
    let toHead ((tid, hid), ref) | tid == headTypeID @a Proxy = Just $ Head hid $ wrappedLoad ref
                                 | otherwise                  = Nothing
    catMaybes . map toHead <$> readMVar theads

loadHead :: forall a m. (HeadType a, MonadIO m) => Storage -> HeadID -> m (Maybe (Head a))
loadHead st hid = fmap (Head hid . wrappedLoad) <$> loadHeadRaw st (headTypeID @a Proxy) hid

loadHeadRaw :: forall m. MonadIO m => Storage -> HeadTypeID -> HeadID -> m (Maybe Ref)
loadHeadRaw s@(Storage { stBacking = StorageDir { dirPath = spath }}) tid hid = liftIO $ do
    handleJust (guard . isDoesNotExistError) (const $ return Nothing) $ do
        (h:_) <- BC.lines <$> B.readFile (headPath spath tid hid)
        Just ref <- readRef s h
        return $ Just ref
loadHeadRaw Storage { stBacking = StorageMemory { memHeads = theads } } tid hid = liftIO $ do
    lookup (tid, hid) <$> readMVar theads

reloadHead :: (HeadType a, MonadIO m) => Head a -> m (Maybe (Head a))
reloadHead (Head hid (Stored (Ref st _) _)) = loadHead st hid

storeHead :: forall a m. MonadIO m => HeadType a => Storage -> a -> m (Head a)
storeHead st obj = do
    let tid = headTypeID @a Proxy
    stored <- wrappedStore st obj
    hid <- storeHeadRaw st tid (storedRef stored)
    return $ Head hid stored

storeHeadRaw :: forall m. MonadIO m => Storage -> HeadTypeID -> Ref -> m HeadID
storeHeadRaw st tid ref = liftIO $ do
    hid <- HeadID <$> U.nextRandom
    case stBacking st of
         StorageDir { dirPath = spath } -> do
             Right () <- writeFileChecked (headPath spath tid hid) Nothing $
                 showRef ref `B.append` BC.singleton '\n'
             return ()
         StorageMemory { memHeads = theads } -> do
             modifyMVar_ theads $ return . (((tid, hid), ref) :)
    return hid

replaceHead :: forall a m. (HeadType a, MonadIO m) => Head a -> Stored a -> m (Either (Maybe (Head a)) (Head a))
replaceHead prev@(Head hid pobj) stored' = liftIO $ do
    let st = headStorage prev
        tid = headTypeID @a Proxy
    stored <- copyStored st stored'
    bimap (fmap $ Head hid . wrappedLoad) (const $ Head hid stored) <$>
        replaceHeadRaw st tid hid (storedRef pobj) (storedRef stored)

replaceHeadRaw :: forall m. MonadIO m => Storage -> HeadTypeID -> HeadID -> Ref -> Ref -> m (Either (Maybe Ref) Ref)
replaceHeadRaw st tid hid prev new = liftIO $ do
    case stBacking st of
         StorageDir { dirPath = spath } -> do
             let filename = headPath spath tid hid
                 showRefL r = showRef r `B.append` BC.singleton '\n'

             writeFileChecked filename (Just $ showRefL prev) (showRefL new) >>= \case
                 Left Nothing -> return $ Left Nothing
                 Left (Just bs) -> do Just oref <- readRef st $ BC.takeWhile (/='\n') bs
                                      return $ Left $ Just oref
                 Right () -> return $ Right new

         StorageMemory { memHeads = theads, memWatchers = twatch } -> do
             res <- modifyMVar theads $ \hs -> do
                 ws <- map wlFun . filter ((==(tid, hid)) . wlHead) . wlList <$> readMVar twatch
                 return $ case partition ((==(tid, hid)) . fst) hs of
                     ([] , _  ) -> (hs, Left Nothing)
                     ((_, r):_, hs') | r == prev -> (((tid, hid), new) : hs',
                                                                  Right (new, ws))
                                     | otherwise -> (hs, Left $ Just r)
             case res of
                  Right (r, ws) -> mapM_ ($ r) ws >> return (Right r)
                  Left x -> return $ Left x

updateHead :: (HeadType a, MonadIO m) => Head a -> (Stored a -> m (Stored a, b)) -> m (Maybe (Head a), b)
updateHead h f = do
    (o, x) <- f $ headStoredObject h
    replaceHead h o >>= \case
        Right h' -> return (Just h', x)
        Left Nothing -> return (Nothing, x)
        Left (Just h') -> updateHead h' f

updateHead_ :: (HeadType a, MonadIO m) => Head a -> (Stored a -> m (Stored a)) -> m (Maybe (Head a))
updateHead_ h = fmap fst . updateHead h . (fmap (,()) .)


data WatchedHead = forall a. WatchedHead Storage WatchID (MVar a)

watchHead :: forall a. HeadType a => Head a -> (Head a -> IO ()) -> IO WatchedHead
watchHead h = watchHeadWith h id

watchHeadWith :: forall a b. (HeadType a, Eq b) => Head a -> (Head a -> b) -> (b -> IO ()) -> IO WatchedHead
watchHeadWith (Head hid (Stored (Ref st _) _)) sel cb = do
    watchHeadRaw st (headTypeID @a Proxy) hid (sel . Head hid . wrappedLoad) cb

watchHeadRaw :: forall b. Eq b => Storage -> HeadTypeID -> HeadID -> (Ref -> b) -> (b -> IO ()) -> IO WatchedHead
watchHeadRaw st tid hid sel cb = do
    memo <- newEmptyMVar
    let addWatcher wl = (wl', WatchedHead st (wlNext wl) memo)
            where wl' = wl { wlNext = wlNext wl + 1
                           , wlList = WatchListItem
                               { wlID = wlNext wl
                               , wlHead = (tid, hid)
                               , wlFun = \r -> do
                                   let x = sel r
                                   modifyMVar_ memo $ \prev -> do
                                       when (Just x /= prev) $ cb x
                                       return $ Just x
                               } : wlList wl
                           }

    watched <- case stBacking st of
         StorageDir { dirPath = spath, dirWatchers = mvar } -> modifyMVar mvar $ \(mbmanager, ilist, wl) -> do
             manager <- maybe startManager return mbmanager
             ilist' <- case tid `elem` ilist of
                 True -> return ilist
                 False -> do
                     void $ watchDir manager (headTypePath spath tid) (const True) $ \case
                         ev@Added {} | Just ihid <- HeadID <$> U.fromString (takeFileName (eventPath ev)) -> do
                             loadHeadRaw st tid ihid >>= \case
                                 Just ref -> do
                                     (_, _, iwl) <- readMVar mvar
                                     mapM_ ($ ref) . map wlFun . filter ((== (tid, ihid)) . wlHead) . wlList $ iwl
                                 Nothing -> return ()
                         _ -> return ()
                     return $ tid : ilist
             return $ first ( Just manager, ilist', ) $ addWatcher wl

         StorageMemory { memWatchers = mvar } -> modifyMVar mvar $ return . addWatcher

    cur <- fmap sel <$> loadHeadRaw st tid hid
    maybe (return ()) cb cur
    putMVar memo cur

    return watched

unwatchHead :: WatchedHead -> IO ()
unwatchHead (WatchedHead st wid _) = do
    let delWatcher wl = wl { wlList = filter ((/=wid) . wlID) $ wlList wl }
    case stBacking st of
        StorageDir { dirWatchers = mvar } -> modifyMVar_ mvar $ return . second delWatcher
        StorageMemory { memWatchers = mvar } -> modifyMVar_ mvar $ return . delWatcher


class Monad m => MonadStorage m where
    getStorage :: m Storage
    mstore :: Storable a => a -> m (Stored a)

    default mstore :: MonadIO m => Storable a => a -> m (Stored a)
    mstore x = do
        st <- getStorage
        wrappedStore st x

instance MonadIO m => MonadStorage (ReaderT Storage m) where
    getStorage = ask

instance MonadIO m => MonadStorage (ReaderT (Head a) m) where
    getStorage = asks $ headStorage


class Storable a where
    store' :: a -> Store
    load' :: Load a

    store :: StorageCompleteness c => Storage' c -> a -> IO (Ref' c)
    store st = evalStore st . store'
    load :: Ref -> a
    load = evalLoad load'

class Storable a => ZeroStorable a where
    fromZero :: Storage -> a

data Store = StoreBlob ByteString
           | StoreRec (forall c. StorageCompleteness c => Storage' c -> [IO [(ByteString, RecItem' c)]])
           | StoreZero
           | StoreUnknown ByteString ByteString

evalStore :: StorageCompleteness c => Storage' c -> Store -> IO (Ref' c)
evalStore st = unsafeStoreObject st <=< evalStoreObject st

evalStoreObject :: StorageCompleteness c => Storage' c -> Store -> IO (Object' c)
evalStoreObject _ (StoreBlob x) = return $ Blob x
evalStoreObject s (StoreRec f) = Rec . concat <$> sequence (f s)
evalStoreObject _ StoreZero = return ZeroObject
evalStoreObject _ (StoreUnknown otype content) = return $ UnknownObject otype content

newtype StoreRecM c a = StoreRecM (ReaderT (Storage' c) (Writer [IO [(ByteString, RecItem' c)]]) a)
    deriving (Functor, Applicative, Monad)

type StoreRec c = StoreRecM c ()

newtype Load a = Load (ReaderT (Ref, Object) (Except String) a)
    deriving (Functor, Applicative, Alternative, Monad, MonadPlus, MonadError String)

evalLoad :: Load a -> Ref -> a
evalLoad (Load f) ref = either (error {- TODO throw -} . ((BC.unpack (showRef ref) ++ ": ")++)) id $ runExcept $ runReaderT f (ref, lazyLoadObject ref)

loadCurrentRef :: Load Ref
loadCurrentRef = Load $ asks fst

loadCurrentObject :: Load Object
loadCurrentObject = Load $ asks snd

newtype LoadRec a = LoadRec (ReaderT (Ref, [(ByteString, RecItem)]) (Except String) a)
    deriving (Functor, Applicative, Alternative, Monad, MonadPlus, MonadError String)

loadRecCurrentRef :: LoadRec Ref
loadRecCurrentRef = LoadRec $ asks fst

loadRecItems :: LoadRec [(ByteString, RecItem)]
loadRecItems = LoadRec $ asks snd


instance Storable Object where
    store' (Blob bs) = StoreBlob bs
    store' (Rec xs) = StoreRec $ \st -> return $ do
        Rec xs' <- copyObject st (Rec xs)
        return xs'
    store' ZeroObject = StoreZero
    store' (UnknownObject otype content) = StoreUnknown otype content

    load' = loadCurrentObject

    store st = unsafeStoreObject st <=< copyObject st
    load = lazyLoadObject

instance Storable ByteString where
    store' = storeBlob
    load' = loadBlob id

instance Storable a => Storable [a] where
    store' []     = storeZero
    store' (x:xs) = storeRec $ do
        storeRef "i" x
        storeRef "n" xs

    load' = loadCurrentObject >>= \case
                ZeroObject -> return []
                _          -> loadRec $ (:)
                                  <$> loadRef "i"
                                  <*> loadRef "n"

instance Storable a => ZeroStorable [a] where
    fromZero _ = []


storeBlob :: ByteString -> Store
storeBlob = StoreBlob

storeRec :: (forall c. StorageCompleteness c => StoreRec c) -> Store
storeRec sr = StoreRec $ do
    let StoreRecM r = sr
    execWriter . runReaderT r

storeZero :: Store
storeZero = StoreZero


class StorableText a where
    toText :: a -> Text
    fromText :: MonadError String m => Text -> m a

instance StorableText Text where
    toText = id; fromText = return

instance StorableText [Char] where
    toText = T.pack; fromText = return . T.unpack


class StorableDate a where
    toDate :: a -> ZonedTime
    fromDate :: ZonedTime -> a

instance StorableDate ZonedTime where
    toDate = id; fromDate = id

instance StorableDate UTCTime where
    toDate = utcToZonedTime utc
    fromDate = zonedTimeToUTC

instance StorableDate Day where
    toDate day = toDate $ UTCTime day 0
    fromDate = utctDay . fromDate


class StorableUUID a where
    toUUID :: a -> UUID
    fromUUID :: UUID -> a

instance StorableUUID UUID where
    toUUID = id; fromUUID = id


storeEmpty :: String -> StoreRec c
storeEmpty name = StoreRecM $ tell [return [(BC.pack name, RecEmpty)]]

storeMbEmpty :: String -> Maybe () -> StoreRec c
storeMbEmpty name = maybe (return ()) (const $ storeEmpty name)

storeInt :: Integral a => String -> a -> StoreRec c
storeInt name x = StoreRecM $ tell [return [(BC.pack name, RecInt $ toInteger x)]]

storeMbInt :: Integral a => String -> Maybe a -> StoreRec c
storeMbInt name = maybe (return ()) (storeInt name)

storeNum :: (Real a, Fractional a) => String -> a -> StoreRec c
storeNum name x = StoreRecM $ tell [return [(BC.pack name, RecNum $ toRational x)]]

storeMbNum :: (Real a, Fractional a) => String -> Maybe a -> StoreRec c
storeMbNum name = maybe (return ()) (storeNum name)

storeText :: StorableText a => String -> a -> StoreRec c
storeText name x = StoreRecM $ tell [return [(BC.pack name, RecText $ toText x)]]

storeMbText :: StorableText a => String -> Maybe a -> StoreRec c
storeMbText name = maybe (return ()) (storeText name)

storeBinary :: BA.ByteArrayAccess a => String -> a -> StoreRec c
storeBinary name x = StoreRecM $ tell [return [(BC.pack name, RecBinary $ BA.convert x)]]

storeMbBinary :: BA.ByteArrayAccess a => String -> Maybe a -> StoreRec c
storeMbBinary name = maybe (return ()) (storeBinary name)

storeDate :: StorableDate a => String -> a -> StoreRec c
storeDate name x = StoreRecM $ tell [return [(BC.pack name, RecDate $ toDate x)]]

storeMbDate :: StorableDate a => String -> Maybe a -> StoreRec c
storeMbDate name = maybe (return ()) (storeDate name)

storeUUID :: StorableUUID a => String -> a -> StoreRec c
storeUUID name x = StoreRecM $ tell [return [(BC.pack name, RecUUID $ toUUID x)]]

storeMbUUID :: StorableUUID a => String -> Maybe a -> StoreRec c
storeMbUUID name = maybe (return ()) (storeUUID name)

storeRef :: Storable a => StorageCompleteness c => String -> a -> StoreRec c
storeRef name x = StoreRecM $ do
    s <- ask
    tell $ (:[]) $ do
        ref <- store s x
        return [(BC.pack name, RecRef ref)]

storeMbRef :: Storable a => StorageCompleteness c => String -> Maybe a -> StoreRec c
storeMbRef name = maybe (return ()) (storeRef name)

storeRawRef :: StorageCompleteness c => String -> Ref -> StoreRec c
storeRawRef name ref = StoreRecM $ do
    st <- ask
    tell $ (:[]) $ do
        ref' <- copyRef st ref
        return [(BC.pack name, RecRef ref')]

storeMbRawRef :: StorageCompleteness c => String -> Maybe Ref -> StoreRec c
storeMbRawRef name = maybe (return ()) (storeRawRef name)

storeZRef :: (ZeroStorable a, StorageCompleteness c) => String -> a -> StoreRec c
storeZRef name x = StoreRecM $ do
    s <- ask
    tell $ (:[]) $ do
        ref <- store s x
        return $ if isZeroRef ref then []
                                  else [(BC.pack name, RecRef ref)]

storeRecItems :: StorageCompleteness c => [ ( ByteString, RecItem ) ] -> StoreRec c
storeRecItems items = StoreRecM $ do
    st <- ask
    tell $ flip map items $ \( name, value ) -> do
        value' <- copyRecItem st value
        return [ ( name, value' ) ]

loadBlob :: (ByteString -> a) -> Load a
loadBlob f = loadCurrentObject >>= \case
    Blob x -> return $ f x
    _      -> throwError "Expecting blob"

loadRec :: LoadRec a -> Load a
loadRec (LoadRec lrec) = loadCurrentObject >>= \case
    Rec rs -> do
        ref <- loadCurrentRef
        either throwError return $ runExcept $ runReaderT lrec (ref, rs)
    _ -> throwError "Expecting record"

loadZero :: a -> Load a
loadZero x = loadCurrentObject >>= \case
    ZeroObject -> return x
    _          -> throwError "Expecting zero"


loadEmpty :: String -> LoadRec ()
loadEmpty name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbEmpty name

loadMbEmpty :: String -> LoadRec (Maybe ())
loadMbEmpty name = listToMaybe . mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecEmpty ) | name' == bname
        = Just ()
    p _ = Nothing

loadInt :: Num a => String -> LoadRec a
loadInt name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbInt name

loadMbInt :: Num a => String -> LoadRec (Maybe a)
loadMbInt name = listToMaybe . mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecInt x ) | name' == bname
        = Just (fromInteger x)
    p _ = Nothing

loadNum :: (Real a, Fractional a) => String -> LoadRec a
loadNum name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbNum name

loadMbNum :: (Real a, Fractional a) => String -> LoadRec (Maybe a)
loadMbNum name = listToMaybe . mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecNum x ) | name' == bname
        = Just (fromRational x)
    p _ = Nothing

loadText :: StorableText a => String -> LoadRec a
loadText name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbText name

loadMbText :: StorableText a => String -> LoadRec (Maybe a)
loadMbText name = listToMaybe <$> loadTexts name

loadTexts :: StorableText a => String -> LoadRec [a]
loadTexts name = sequence . mapMaybe p =<< loadRecItems
  where
    bname = BC.pack name
    p ( name', RecText x ) | name' == bname
        = Just (fromText x)
    p _ = Nothing

loadBinary :: BA.ByteArray a => String -> LoadRec a
loadBinary name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbBinary name

loadMbBinary :: BA.ByteArray a => String -> LoadRec (Maybe a)
loadMbBinary name = listToMaybe <$> loadBinaries name

loadBinaries :: BA.ByteArray a => String -> LoadRec [a]
loadBinaries name = mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecBinary x ) | name' == bname
        = Just (BA.convert x)
    p _ = Nothing

loadDate :: StorableDate a => String -> LoadRec a
loadDate name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbDate name

loadMbDate :: StorableDate a => String -> LoadRec (Maybe a)
loadMbDate name = listToMaybe . mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecDate x ) | name' == bname
        = Just (fromDate x)
    p _ = Nothing

loadUUID :: StorableUUID a => String -> LoadRec a
loadUUID name = maybe (throwError $ "Missing record iteem '"++name++"'") return =<< loadMbUUID name

loadMbUUID :: StorableUUID a => String -> LoadRec (Maybe a)
loadMbUUID name = listToMaybe . mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecUUID x ) | name' == bname
        = Just (fromUUID x)
    p _ = Nothing

loadRawRef :: String -> LoadRec Ref
loadRawRef name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbRawRef name

loadMbRawRef :: String -> LoadRec (Maybe Ref)
loadMbRawRef name = listToMaybe <$> loadRawRefs name

loadRawRefs :: String -> LoadRec [Ref]
loadRawRefs name = mapMaybe p <$> loadRecItems
  where
    bname = BC.pack name
    p ( name', RecRef x ) | name' == bname = Just x
    p _                                    = Nothing

loadRef :: Storable a => String -> LoadRec a
loadRef name = load <$> loadRawRef name

loadMbRef :: Storable a => String -> LoadRec (Maybe a)
loadMbRef name = fmap load <$> loadMbRawRef name

loadRefs :: Storable a => String -> LoadRec [a]
loadRefs name = map load <$> loadRawRefs name

loadZRef :: ZeroStorable a => String -> LoadRec a
loadZRef name = loadMbRef name >>= \case
                    Nothing -> do Ref st _ <- loadRecCurrentRef
                                  return $ fromZero st
                    Just x  -> return x


type Stored a = Stored' Complete a

instance Storable a => Storable (Stored a) where
    store st = copyRef st . storedRef
    store' (Stored _ x) = store' x
    load' = Stored <$> loadCurrentRef <*> load'

instance ZeroStorable a => ZeroStorable (Stored a) where
    fromZero st = Stored (zeroRef st) $ fromZero st

fromStored :: Stored a -> a
fromStored (Stored _ x) = x

storedRef :: Stored a -> Ref
storedRef (Stored ref _) = ref

wrappedStore :: MonadIO m => Storable a => Storage -> a -> m (Stored a)
wrappedStore st x = do ref <- liftIO $ store st x
                       return $ Stored ref x

wrappedLoad :: Storable a => Ref -> Stored a
wrappedLoad ref = Stored ref (load ref)

copyStored :: forall c c' m a. (StorageCompleteness c, StorageCompleteness c', MonadIO m) =>
    Storage' c' -> Stored' c a -> m (LoadResult c (Stored' c' a))
copyStored st (Stored ref' x) = liftIO $ returnLoadResult . fmap (flip Stored x) <$> copyRef' st ref'

-- |Passed function needs to preserve the object representation to be safe
unsafeMapStored :: (a -> b) -> Stored a -> Stored b
unsafeMapStored f (Stored ref x) = Stored ref (f x)


data StoreInfo = StoreInfo
    { infoDate :: ZonedTime
    , infoNote :: Maybe Text
    }
    deriving (Show)

makeStoreInfo :: IO StoreInfo
makeStoreInfo = StoreInfo
    <$> getZonedTime
    <*> pure Nothing

storeInfoRec :: StoreInfo -> StoreRec c
storeInfoRec info = do
    storeDate "date" $ infoDate info
    storeMbText "note" $ infoNote info

loadInfoRec :: LoadRec StoreInfo
loadInfoRec = StoreInfo
    <$> loadDate "date"
    <*> loadMbText "note"


data History a = History StoreInfo (Stored a) (Maybe (StoredHistory a))
    deriving (Show)

type StoredHistory a = Stored (History a)

instance Storable a => Storable (History a) where
    store' (History si x prev) = storeRec $ do
        storeInfoRec si
        storeMbRef "prev" prev
        storeRef "item" x

    load' = loadRec $ History
        <$> loadInfoRec
        <*> loadRef "item"
        <*> loadMbRef "prev"

fromHistory :: StoredHistory a -> a
fromHistory = fromStored . storedFromHistory

fromHistoryAt :: ZonedTime -> StoredHistory a -> Maybe a
fromHistoryAt zat = fmap (fromStored . snd) . listToMaybe . dropWhile ((at<) . zonedTimeToUTC . fst) . storedHistoryTimedList
    where at = zonedTimeToUTC zat

storedFromHistory :: StoredHistory a -> Stored a
storedFromHistory sh = let History _ item _ = fromStored sh
                        in item

storedHistoryList :: StoredHistory a -> [Stored a]
storedHistoryList = map snd . storedHistoryTimedList

storedHistoryTimedList :: StoredHistory a -> [(ZonedTime, Stored a)]
storedHistoryTimedList sh = let History hinfo item prev = fromStored sh
                             in (infoDate hinfo, item) : maybe [] storedHistoryTimedList prev

beginHistory :: Storable a => Storage -> StoreInfo -> a -> IO (StoredHistory a)
beginHistory st si x = do sx <- wrappedStore st x
                          wrappedStore st $ History si sx Nothing

modifyHistory :: Storable a => StoreInfo -> (a -> a) -> StoredHistory a -> IO (StoredHistory a)
modifyHistory si f prev@(Stored (Ref st _) _) = do
    sx <- wrappedStore st $ f $ fromHistory prev
    wrappedStore st $ History si sx (Just prev)


showRatio :: Rational -> String
showRatio r = case decimalRatio r of
                   Just (n, 1) -> show n
                   Just (n', d) -> let n = abs n'
                                    in (if n' < 0 then "-" else "") ++ show (n `div` d) ++ "." ++
                                       (concatMap (show.(`mod` 10).snd) $ reverse $ takeWhile ((>1).fst) $ zip (iterate (`div` 10) d) (iterate (`div` 10) (n `mod` d)))
                   Nothing -> show (numerator r) ++ "/" ++ show (denominator r)

decimalRatio :: Rational -> Maybe (Integer, Integer)
decimalRatio r = do
    let n = numerator r
        d = denominator r
        (c2, d') = takeFactors 2 d
        (c5, d'') = takeFactors 5 d'
    guard $ d'' == 1
    let m = if c2 > c5 then 5 ^ (c2 - c5)
                       else 2 ^ (c5 - c2)
    return (n * m, d * m)

takeFactors :: Integer -> Integer -> (Integer, Integer)
takeFactors f n | n `mod` f == 0 = let (c, n') = takeFactors f (n `div` f)
                                    in (c+1, n')
                | otherwise = (0, n)

parseRatio :: ByteString -> Maybe Rational
parseRatio bs = case BC.groupBy ((==) `on` isNumber) bs of
                     (m:xs) | m == BC.pack "-" -> negate <$> positive xs
                     xs                        -> positive xs
    where positive = \case
              [bx] -> fromInteger . fst <$> BC.readInteger bx
              [bx, op, by] -> do
                  (x, _) <- BC.readInteger bx
                  (y, _) <- BC.readInteger by
                  case BC.unpack op of
                       "." -> return $ (x % 1) + (y % (10 ^ BC.length by))
                       "/" -> return $ x % y
                       _   -> Nothing
              _ -> Nothing

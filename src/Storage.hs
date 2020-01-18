module Storage (
    Storage, PartialStorage,
    openStorage, memoryStorage,
    deriveEphemeralStorage, derivePartialStorage,

    Ref, PartialRef, RefDigest,
    refStorage, refDigest,
    readRef, showRef, showRefDigest,
    copyRef, partialRef, partialRefFromDigest,

    Object, PartialObject, Object'(..), RecItem, RecItem'(..),
    serializeObject, deserializeObject, deserializeObjects,
    ioLoadObject, ioLoadBytes,
    storeRawBytes, lazyLoadBytes,
    storeObject,
    collectObjects, collectStoredObjects,

    Head,
    headName, headRef, headObject,
    loadHeads, loadHead, loadHeadDef, replaceHead,
    watchHead,

    Storable(..), ZeroStorable(..),
    StorableText(..), StorableDate(..), StorableUUID(..),

    storeBlob, storeRec, storeZero,
    storeInt, storeNum, storeText, storeBinary, storeDate, storeUUID, storeJson, storeRef, storeRawRef,
    storeMbInt, storeMbNum, storeMbText, storeMbBinary, storeMbDate, storeMbUUID, storeMbJson, storeMbRef, storeMbRawRef,
    storeZRef,

    LoadRec,
    loadBlob, loadRec, loadZero,
    loadInt, loadNum, loadText, loadBinary, loadDate, loadUUID, loadJson, loadRef, loadRawRef,
    loadMbInt, loadMbNum, loadMbText, loadMbBinary, loadMbDate, loadMbUUID, loadMbJson, loadMbRef, loadMbRawRef,
    loadBinaries, loadRefs, loadRawRefs,
    loadZRef,

    Stored,
    fromStored, storedRef, storedStorage,
    wrappedStore, wrappedLoad,
    copyStored,

    StoreInfo(..), makeStoreInfo,

    StoredHistory,
    fromHistory, fromHistoryAt, storedFromHistory, storedHistoryList,
    beginHistory, modifyHistory,
) where

import Codec.Compression.Zlib
import qualified Codec.MIME.Type as MIME
import qualified Codec.MIME.Parse as MIME

import Control.Arrow
import Control.Concurrent
import Control.DeepSeq
import Control.Exception
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.Writer

import Crypto.Hash

import qualified Data.Aeson as J
import Data.ByteString (ByteString, singleton)
import qualified Data.ByteArray as BA
import Data.ByteArray.Encoding
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.Char
import Data.Function
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
import Data.UUID (UUID)
import qualified Data.UUID as U

import System.Directory
import System.INotify
import System.IO.Error
import System.IO.Unsafe

import Storage.Internal


type Storage = Storage' Complete
type PartialStorage = Storage' Partial

openStorage :: FilePath -> IO Storage
openStorage path = do
    createDirectoryIfMissing True $ path ++ "/objects"
    createDirectoryIfMissing True $ path ++ "/heads"
    watchers <- newMVar (Nothing, [])
    return $ Storage { stBacking = StorageDir path watchers, stParent = Nothing }

memoryStorage' :: IO (Storage' c')
memoryStorage' = do
    backing <- StorageMemory <$> newMVar [] <*> newMVar M.empty <*> newMVar M.empty <*> newMVar []
    return $ Storage { stBacking = backing, stParent = Nothing }

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
zeroRef s = Ref s h
    where h = case digestFromByteString $ B.replicate (hashDigestSize $ digestAlgo h) 0 of
                   Nothing -> error $ "Failed to create zero hash"
                   Just h' -> h'
          digestAlgo :: Digest a -> a
          digestAlgo = undefined

isZeroRef :: Ref' c -> Bool
isZeroRef (Ref _ h) = all (==0) $ BA.unpack h


readRefDigest :: ByteString -> Maybe RefDigest
readRefDigest = digestFromByteString . B.concat <=< readHex
    where readHex bs | B.null bs = Just []
          readHex bs = do (bx, bs') <- B.uncons bs
                          (by, bs'') <- B.uncons bs'
                          x <- hexDigit bx
                          y <- hexDigit by
                          (singleton (x * 16 + y) :) <$> readHex bs''
          hexDigit x | x >= o '0' && x <= o '9' = Just $ x - o '0'
                     | x >= o 'a' && x <= o 'z' = Just $ x - o 'a' + 10
                     | otherwise                = Nothing
          o = fromIntegral . ord

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

copyObject' :: forall c c'. (StorageCompleteness c, StorageCompleteness c') => Storage' c' -> Object' c -> IO (c (Object' c'))
copyObject' _ (Blob bs) = return $ return $ Blob bs
copyObject' st (Rec rs) = fmap Rec . sequence <$> mapM copyItem rs
    where copyItem :: (ByteString, RecItem' c) -> IO (c (ByteString, RecItem' c'))
          copyItem (n, item) = fmap (n,) <$> case item of
              RecInt x -> return $ return $ RecInt x
              RecNum x -> return $ return $ RecNum x
              RecText x -> return $ return $ RecText x
              RecBinary x -> return $ return $ RecBinary x
              RecDate x -> return $ return $ RecDate x
              RecUUID x -> return $ return $ RecUUID x
              RecJson x -> return $ return $ RecJson x
              RecRef x -> fmap RecRef <$> copyRef' st x
copyObject' _ ZeroObject = return $ return ZeroObject

copyRef :: forall c c' m. (StorageCompleteness c, StorageCompleteness c', MonadIO m) => Storage' c' -> Ref' c -> m (LoadResult c (Ref' c'))
copyRef st ref' = liftIO $ returnLoadResult <$> copyRef' st ref'

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
    deriving (Show)

type Object = Object' Complete
type PartialObject = Object' Partial

data RecItem' c
    = RecInt Integer
    | RecNum Rational
    | RecText Text
    | RecBinary ByteString
    | RecDate ZonedTime
    | RecUUID UUID
    | RecJson J.Value
    | RecRef (Ref' c)
    deriving (Show)

type RecItem = RecItem' Complete

serializeObject :: Object' c -> BL.ByteString
serializeObject = \case
    Blob cnt -> BL.fromChunks [BC.pack "blob ", BC.pack (show $ B.length cnt), BC.singleton '\n', cnt]
    Rec rec -> let cnt = BL.fromChunks $ concatMap (uncurry serializeRecItem) rec
                in BL.fromChunks [BC.pack "rec ", BC.pack (show $ BL.length cnt), BC.singleton '\n'] `BL.append` cnt
    ZeroObject -> BL.empty

unsafeStoreObject :: Storage' c -> Object' c -> IO (Ref' c)
unsafeStoreObject storage = \case
    ZeroObject -> return $ zeroRef storage
    obj -> unsafeStoreRawBytes storage $ serializeObject obj

storeObject :: PartialStorage -> PartialObject -> IO PartialRef
storeObject = unsafeStoreObject

storeRawBytes :: PartialStorage -> BL.ByteString -> IO PartialRef
storeRawBytes = unsafeStoreRawBytes

unsafeStoreRawBytes :: Storage' c -> BL.ByteString -> IO (Ref' c)
unsafeStoreRawBytes st raw = do
    let dgst = hashFinalize $ hashUpdates hashInit $ BL.toChunks raw
    case stBacking st of
         StorageDir { dirPath = sdir } -> writeFileOnce (refPath sdir dgst) $ compress raw
         StorageMemory { memObjs = tobjs } ->
             dgst `deepseq` -- the TVar may be accessed when evaluating the data to be written
                 modifyMVar_ tobjs (return . M.insert dgst raw)
    return $ Ref st dgst

serializeRecItem :: ByteString -> RecItem' c -> [ByteString]
serializeRecItem name (RecInt x) = [name, BC.pack ":i", BC.singleton ' ', BC.pack (show x), BC.singleton '\n']
serializeRecItem name (RecNum x) = [name, BC.pack ":n", BC.singleton ' ', BC.pack (showRatio x), BC.singleton '\n']
serializeRecItem name (RecText x) = [name, BC.pack ":t", BC.singleton ' ', escaped, BC.singleton '\n']
    where escaped = BC.concatMap escape $ encodeUtf8 x
          escape '\n' = BC.pack "\n\t"
          escape c    = BC.singleton c
serializeRecItem name (RecBinary x) = [name, BC.pack ":b ", convertToBase Base64 x, BC.singleton '\n']
serializeRecItem name (RecDate x) = [name, BC.pack ":d", BC.singleton ' ', BC.pack (formatTime defaultTimeLocale "%s %z" x), BC.singleton '\n']
serializeRecItem name (RecUUID x) = [name, BC.pack ":u", BC.singleton ' ', U.toASCIIBytes x, BC.singleton '\n']
serializeRecItem name (RecJson x) = [name, BC.pack ":j", BC.singleton ' '] ++ BL.toChunks (J.encode x) ++ [BC.singleton '\n']
serializeRecItem name (RecRef x) = [name, BC.pack ":r.b2 ", showRef x, BC.singleton '\n']

lazyLoadObject :: forall c. StorageCompleteness c => Ref' c -> LoadResult c (Object' c)
lazyLoadObject = returnLoadResult . unsafePerformIO . ioLoadObject

ioLoadObject :: forall c. StorageCompleteness c => Ref' c -> IO (c (Object' c))
ioLoadObject ref | isZeroRef ref = return $ return ZeroObject
ioLoadObject ref@(Ref st rhash) = do
    file' <- ioLoadBytes ref
    return $ do
        file <- file'
        let chash = hashFinalize $ hashUpdates hashInit $ BL.toChunks file
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
                   | otherwise -> throwError $ "Unknown object type"
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

              val <- case BC.unpack itype of
                          "i" -> do (num, rest) <- BC.readInteger content
                                    guard $ B.null rest
                                    return $ RecInt num
                          "n" -> RecNum <$> parseRatio content
                          "t" -> return $ RecText $ decodeUtf8With lenientDecode content
                          "b" -> either (const Nothing) (Just . RecBinary) $ convertFromBase Base64 content
                          "d" -> RecDate <$> parseTimeM False defaultTimeLocale "%s %z" (BC.unpack content)
                          "u" -> RecUUID <$> U.fromASCIIBytes content
                          "j" -> RecJson <$> J.decode (BL.fromStrict content)
                          "r.b2" -> RecRef . Ref st <$> readRefDigest content
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

headName :: Head -> String
headName (Head name _) = name

headRef :: Head -> Ref
headRef (Head _ ref) = ref

headObject :: Storable a => Head -> a
headObject = load . headRef


loadHeads :: Storage -> IO [Head]
loadHeads s@(Storage { stBacking = StorageDir { dirPath = spath }}) = do
    let hpath = spath ++ "/heads/"
    files <- filterM (doesFileExist . (hpath++)) =<< getDirectoryContents hpath
    forM files $ \hname -> do
        (h:_) <- BC.lines <$> B.readFile (hpath ++ "/" ++ hname)
        Just ref <- readRef s h
        return $ Head hname ref
loadHeads Storage { stBacking = StorageMemory { memHeads = theads } } = readMVar theads

loadHead :: Storage -> String -> IO (Maybe Head)
loadHead s@(Storage { stBacking = StorageDir { dirPath = spath }}) hname = do
    handleJust (guard . isDoesNotExistError) (const $ return Nothing) $ do
        let hpath = spath ++ "/heads/"
        (h:_) <- BC.lines <$> B.readFile (hpath ++ hname)
        Just ref <- readRef s h
        return $ Just $ Head hname ref
loadHead Storage { stBacking = StorageMemory { memHeads = theads } } hname =
    find ((==hname) . headName) <$> readMVar theads

loadHeadDef :: Storable a => Storage -> String -> IO a -> IO Head
loadHeadDef s hname gen = loadHead s hname >>= \case
        Just h  -> return h
        Nothing -> do obj <- gen
                      Right h <- replaceHead obj (Left (s, hname))
                      return h

replaceHead :: Storable a => a -> Either (Storage, String) Head -> IO (Either (Maybe Head) Head)
replaceHead obj prev = do
    let (st, name) = either id (\(Head n (Ref s _)) -> (s, n)) prev
    ref <- store st obj
    case stBacking st of
         StorageDir { dirPath = spath } -> do
             let filename = spath ++ "/heads/" ++ name
                 showRefL r = showRef r `B.append` BC.singleton '\n'

             writeFileChecked filename (either (const Nothing) (Just . showRefL . headRef) prev) (showRefL ref) >>= \case
                 Left Nothing -> return $ Left Nothing
                 Left (Just bs) -> do Just oref <- readRef st $ BC.takeWhile (/='\n') bs
                                      return $ Left $ Just $ Head name oref
                 Right () -> return $ Right $ Head name ref

         StorageMemory { memHeads = theads, memWatchers = twatch } -> do
             res <- modifyMVar theads $ \hs -> do
                 ws <- map snd . filter ((==name) . fst) <$> readMVar twatch
                 case (partition ((== name) . headName) hs, prev) of
                      (([], _), Left _)  -> let h = Head name ref
                                             in return (h:hs, Right (h, ws))
                      (([], _), Right _) -> return (hs, Left Nothing)
                      ((h:_, _), Left _) -> return (hs, Left (Just h))
                      ((h:_, hs'), Right h') | headRef h == headRef h' -> let nh = Head name ref
                                                                           in return (nh:hs', Right (nh, ws))
                                             | otherwise -> return (hs, Left (Just h))
             case res of
                  Right (h, ws) -> mapM_ ($h) ws >> return (Right h)
                  Left x -> return $ Left x

watchHead :: Head -> (Head -> IO ()) -> IO ()
watchHead (Head name (Ref st _)) cb = do
    case stBacking st of
         StorageDir { dirPath = spath, dirWatchers = mvar } -> modifyMVar_ mvar $ \(mbi, watchers) -> do
             inotify <- (\f -> maybe f return mbi) $ do
                 inotify <- initINotify
                 void $ addWatch inotify [Move] (BC.pack $ spath ++ "/heads") $ \case
                     MovedIn { filePath = fpath } -> do
                         let cname = BC.unpack fpath
                         loadHead st cname >>= \case
                             Just h -> mapM_ ($h) . map snd . filter ((== cname) . fst) . snd =<< readMVar mvar
                             Nothing -> return ()
                     _ -> return ()
                 return inotify
             return (Just inotify, (name, cb) : watchers)

         StorageMemory { memWatchers = mvar } -> modifyMVar_ mvar $ return . ((name, cb) :)


class Storable a where
    store' :: a -> Store
    load' :: Load a

    store :: StorageCompleteness c => Storage' c -> a -> IO (Ref' c)
    store st = unsafeStoreObject st <=< evalStore st . store'
    load :: Ref -> a
    load ref = let Load f = load'
                in either (error {- TODO throw -} . ((BC.unpack (showRef ref) ++ ": ")++)) id $ runReaderT f (ref, lazyLoadObject ref)

class Storable a => ZeroStorable a where
    fromZero :: Storage -> a

data Store = StoreBlob ByteString
           | StoreRec (forall c. StorageCompleteness c => Storage' c -> [IO [(ByteString, RecItem' c)]])
           | StoreZero

evalStore :: StorageCompleteness c => Storage' c -> Store -> IO (Object' c)
evalStore _ (StoreBlob x) = return $ Blob x
evalStore s (StoreRec f) = Rec . concat <$> sequence (f s)
evalStore _ StoreZero = return ZeroObject

type StoreRec c = ReaderT (Storage' c) (Writer [IO [(ByteString, RecItem' c)]]) ()

newtype Load a = Load (ReaderT (Ref, Object) (Either String) a)
    deriving (Functor, Applicative, Monad, MonadReader (Ref, Object), MonadError String)

type LoadRec a = ReaderT (Ref, [(ByteString, RecItem)]) (Either String) a


instance Storable Object where
    store' (Blob bs) = StoreBlob bs
    store' (Rec xs) = StoreRec $ \st -> return $ do
        Rec xs' <- copyObject st (Rec xs)
        return xs'
    store' ZeroObject = StoreZero

    load' = asks snd

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

    load' = asks snd >>= \case
                ZeroObject -> return []
                _          -> loadRec $ (:)
                                  <$> loadRef "i"
                                  <*> loadRef "n"

instance Storable a => ZeroStorable [a] where
    fromZero _ = []


storeBlob :: ByteString -> Store
storeBlob = StoreBlob

storeRec :: (forall c. StorageCompleteness c => StoreRec c) -> Store
storeRec r = StoreRec $ execWriter . runReaderT r

storeZero :: Store
storeZero = StoreZero


class StorableText a where
    toText :: a -> Text
    fromText :: MonadError String m => Text -> m a

instance StorableText Text where
    toText = id; fromText = return

instance StorableText [Char] where
    toText = T.pack; fromText = return . T.unpack

instance StorableText MIME.Type where
    toText = MIME.showType
    fromText = maybe (throwError "Malformed MIME type") return . MIME.parseMIMEType


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


storeInt :: Integral a => String -> a -> StoreRec c
storeInt name x = tell [return [(BC.pack name, RecInt $ toInteger x)]]

storeMbInt :: Integral a => String -> Maybe a -> StoreRec c
storeMbInt name = maybe (return ()) (storeInt name)

storeNum :: (Real a, Fractional a) => String -> a -> StoreRec c
storeNum name x = tell [return [(BC.pack name, RecNum $ toRational x)]]

storeMbNum :: (Real a, Fractional a) => String -> Maybe a -> StoreRec c
storeMbNum name = maybe (return ()) (storeNum name)

storeText :: StorableText a => String -> a -> StoreRec c
storeText name x = tell [return [(BC.pack name, RecText $ toText x)]]

storeMbText :: StorableText a => String -> Maybe a -> StoreRec c
storeMbText name = maybe (return ()) (storeText name)

storeBinary :: BA.ByteArrayAccess a => String -> a -> StoreRec c
storeBinary name x = tell [return [(BC.pack name, RecBinary $ BA.convert x)]]

storeMbBinary :: BA.ByteArrayAccess a => String -> Maybe a -> StoreRec c
storeMbBinary name = maybe (return ()) (storeBinary name)

storeDate :: StorableDate a => String -> a -> StoreRec c
storeDate name x = tell [return [(BC.pack name, RecDate $ toDate x)]]

storeMbDate :: StorableDate a => String -> Maybe a -> StoreRec c
storeMbDate name = maybe (return ()) (storeDate name)

storeUUID :: StorableUUID a => String -> a -> StoreRec c
storeUUID name x = tell [return [(BC.pack name, RecUUID $ toUUID x)]]

storeMbUUID :: StorableUUID a => String -> Maybe a -> StoreRec c
storeMbUUID name = maybe (return ()) (storeUUID name)

storeJson :: J.ToJSON a => String -> a -> StoreRec c
storeJson name x = tell [return [(BC.pack name, RecJson $ J.toJSON x)]]

storeMbJson :: J.ToJSON a => String -> Maybe a -> StoreRec c
storeMbJson name = maybe (return ()) (storeJson name)

storeRef :: Storable a => StorageCompleteness c => String -> a -> StoreRec c
storeRef name x = do
    s <- ask
    tell $ (:[]) $ do
        ref <- store s x
        return [(BC.pack name, RecRef ref)]

storeMbRef :: Storable a => StorageCompleteness c => String -> Maybe a -> StoreRec c
storeMbRef name = maybe (return ()) (storeRef name)

storeRawRef :: StorageCompleteness c => String -> Ref -> StoreRec c
storeRawRef name ref = do
    st <- ask
    tell $ (:[]) $ do
        ref' <- copyRef st ref
        return [(BC.pack name, RecRef ref')]

storeMbRawRef :: StorageCompleteness c => String -> Maybe Ref -> StoreRec c
storeMbRawRef name = maybe (return ()) (storeRawRef name)

storeZRef :: (ZeroStorable a, StorageCompleteness c) => String -> a -> StoreRec c
storeZRef name x = do
    s <- ask
    tell $ (:[]) $ do
        ref <- store s x
        return $ if isZeroRef ref then []
                                  else [(BC.pack name, RecRef ref)]


loadBlob :: (ByteString -> a) -> Load a
loadBlob f = asks snd >>= \case
    Blob x -> return $ f x
    _      -> throwError "Expecting blob"

loadRec :: LoadRec a -> Load a
loadRec lrec = ask >>= \case
    (ref, Rec rs) -> either throwError return $ runReaderT lrec (ref, rs)
    _             -> throwError "Expecting record"

loadZero :: a -> Load a
loadZero x = asks snd >>= \case
    ZeroObject -> return x
    _          -> throwError "Expecting zero"


loadInt :: Num a => String -> LoadRec a
loadInt name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbInt name

loadMbInt :: Num a => String -> LoadRec (Maybe a)
loadMbInt name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecInt x) -> return (Just $ fromInteger x)
    Just _ -> throwError $ "Expecting type int of record item '"++name++"'"

loadNum :: (Real a, Fractional a) => String -> LoadRec a
loadNum name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbNum name

loadMbNum :: (Real a, Fractional a) => String -> LoadRec (Maybe a)
loadMbNum name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecNum x) -> return (Just $ fromRational x)
    Just _ -> throwError $ "Expecting type number of record item '"++name++"'"

loadText :: StorableText a => String -> LoadRec a
loadText name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbText name

loadMbText :: StorableText a => String -> LoadRec (Maybe a)
loadMbText name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecText x) -> Just <$> fromText x
    Just _ -> throwError $ "Expecting type text of record item '"++name++"'"

loadBinary :: BA.ByteArray a => String -> LoadRec a
loadBinary name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbBinary name

loadMbBinary :: BA.ByteArray a => String -> LoadRec (Maybe a)
loadMbBinary name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecBinary x) -> return $ Just $ BA.convert x
    Just _ -> throwError $ "Expecting type binary of record item '"++name++"'"

loadBinaries :: BA.ByteArray a => String -> LoadRec [a]
loadBinaries name = do
    items <- map snd . filter ((BC.pack name ==) . fst) <$> asks snd
    forM items $ \case RecBinary x -> return $ BA.convert x
                       _ -> throwError $ "Expecting type binary of record item '"++name++"'"

loadDate :: StorableDate a => String -> LoadRec a
loadDate name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbDate name

loadMbDate :: StorableDate a => String -> LoadRec (Maybe a)
loadMbDate name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecDate x) -> return $ Just $ fromDate x
    Just _ -> throwError $ "Expecting type date of record item '"++name++"'"

loadUUID :: StorableUUID a => String -> LoadRec a
loadUUID name = maybe (throwError $ "Missing record iteem '"++name++"'") return =<< loadMbUUID name

loadMbUUID :: StorableUUID a => String -> LoadRec (Maybe a)
loadMbUUID name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecUUID x) -> return $ Just $ fromUUID x
    Just _ -> throwError $ "Expecting type UUID of record item '"++name++"'"

loadJson :: J.FromJSON a => String -> LoadRec a
loadJson name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbJson name

loadMbJson :: J.FromJSON a => String -> LoadRec (Maybe a)
loadMbJson name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecJson v) -> case J.fromJSON v of
                             J.Error err -> throwError err
                             J.Success x -> return (Just x)
    Just _ -> throwError $ "Expecting type JSON of record item '"++name++"'"

loadRawRef :: String -> LoadRec Ref
loadRawRef name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbRawRef name

loadMbRawRef :: String -> LoadRec (Maybe Ref)
loadMbRawRef name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecRef x) -> return (Just x)
    Just _ -> throwError $ "Expecting type ref of record item '"++name++"'"

loadRawRefs :: String -> LoadRec [Ref]
loadRawRefs name = do
    items <- map snd . filter ((BC.pack name ==) . fst) <$> asks snd
    forM items $ \case RecRef x -> return x
                       _ -> throwError $ "Expecting type ref of record item '"++name++"'"

loadRef :: Storable a => String -> LoadRec a
loadRef name = load <$> loadRawRef name

loadMbRef :: Storable a => String -> LoadRec (Maybe a)
loadMbRef name = fmap load <$> loadMbRawRef name

loadRefs :: Storable a => String -> LoadRec [a]
loadRefs name = map load <$> loadRawRefs name

loadZRef :: ZeroStorable a => String -> LoadRec a
loadZRef name = loadMbRef name >>= \case
                    Nothing -> do Ref st _ <- asks fst
                                  return $ fromZero st
                    Just x  -> return x


type Stored a = Stored' Complete a

instance Storable a => Storable (Stored a) where
    store st = copyRef st . storedRef
    store' (Stored _ x) = store' x
    load' = Stored <$> asks fst <*> load'

instance ZeroStorable a => ZeroStorable (Stored a) where
    fromZero st = Stored (zeroRef st) $ fromZero st

fromStored :: Stored a -> a
fromStored (Stored _ x) = x

storedRef :: Stored a -> Ref
storedRef (Stored ref _) = ref

storedStorage :: Stored a -> Storage
storedStorage (Stored (Ref st _) _) = st

wrappedStore :: Storable a => Storage -> a -> IO (Stored a)
wrappedStore st x = do ref <- store st x
                       return $ Stored ref x

wrappedLoad :: Storable a => Ref -> Stored a
wrappedLoad ref = Stored ref (load ref)

copyStored :: forall c c' m a. (StorageCompleteness c, StorageCompleteness c', MonadIO m) =>
    Storage' c' -> Stored' c a -> m (LoadResult c (Stored' c' a))
copyStored st (Stored ref' x) = liftIO $ returnLoadResult . fmap (flip Stored x) <$> copyRef' st ref'


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

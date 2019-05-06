module Storage (
    Storage,
    openStorage,

    Ref,
    readRef, showRef,

    Object(..), RecItem(..),
    serializeObject, deserializeObject, deserializeObjects,
    storeRawBytes, lazyLoadBytes,
    collectObjects, collectStoredObjects,

    Head,
    headName, headRef, headObject,
    loadHeads, loadHead, replaceHead,

    Storable(..),
    StorableText(..), StorableDate(..),

    storeBlob, storeRec, storeZero,
    storeInt, storeNum, storeText, storeBinary, storeDate, storeJson, storeRef,
    storeMbInt, storeMbNum, storeMbText, storeMbBinary, storeMbDate, storeMbJson, storeMbRef,
    storeZRef,

    loadBlob, loadRec, loadZero,
    loadInt, loadNum, loadText, loadBinary, loadDate, loadJson, loadRef,
    loadMbInt, loadMbNum, loadMbText, loadMbBinary, loadMbDate, loadMbJson, loadMbRef,
    loadZRef,

    Stored,
    fromStored, storedRef, storedStorage,
    wrappedStore, wrappedLoad,

    StoreInfo(..), makeStoreInfo,

    StoredHistory,
    fromHistory, fromHistoryAt, storedFromHistory, storedHistoryList,
    beginHistory, modifyHistory,

    StoredList,
    emptySList, fromSList, storedFromSList,
    slistAdd, slistAddS, slistInsert, slistInsertS, slistRemove, slistReplace, slistReplaceS,
    mapFromSList, updateOld,

    StoreUpdate(..),
    withStoredListItem, withStoredListItemS,
) where

import Codec.Compression.Zlib
import qualified Codec.MIME.Type as MIME
import qualified Codec.MIME.Parse as MIME

import Control.Arrow
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
import Data.Map (Map)
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

import System.Directory
import System.FilePath
import System.IO
import System.IO.Unsafe
import System.Posix.Files
import System.Posix.IO
import System.Posix.Types


data Storage = Storage FilePath
    deriving (Eq, Ord)

openStorage :: FilePath -> IO Storage
openStorage path = do
    createDirectoryIfMissing True $ path ++ "/objects"
    createDirectoryIfMissing True $ path ++ "/heads"
    return $ Storage path


data Ref = Ref Storage (Digest Blake2b_256)
    deriving (Eq, Ord)

instance Show Ref where
    show ref@(Ref (Storage path) _) = path ++ ":" ++ BC.unpack (showRef ref)

instance BA.ByteArrayAccess Ref where
    length (Ref _ dgst) = BA.length dgst
    withByteArray (Ref _ dgst) = BA.withByteArray dgst

zeroRef :: Storage -> Ref
zeroRef s = Ref s h
    where h = case digestFromByteString $ B.replicate (BA.length h) 0 of
                   Nothing -> error $ "Failed to create zero hash"
                   Just h' -> h'

isZeroRef :: Ref -> Bool
isZeroRef (Ref _ h) = all (==0) $ BA.unpack h


unsafeReadRef :: Storage -> ByteString -> Maybe Ref
unsafeReadRef s = Just . Ref s <=< digestFromByteString . B.concat <=< readHex
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

readRef :: Storage -> ByteString -> IO (Maybe Ref)
readRef s b =
    case unsafeReadRef s b of
         Nothing -> return Nothing
         Just ref -> do
             doesFileExist (refPath ref) >>= \case
                 True -> return $ Just ref
                 False -> return Nothing

showRef :: Ref -> ByteString
showRef (Ref _ h) = B.concat $ map showHexByte $ BA.unpack h
    where showHex x | x < 10    = x + 48
                    | otherwise = x + 87
          showHexByte x = B.pack [ showHex (x `div` 16), showHex (x `mod` 16) ]

refPath :: Ref -> FilePath
refPath ref@(Ref (Storage spath) _) = intercalate "/" [spath, "objects", pref, rest]
    where (pref, rest) = splitAt 2 $ BC.unpack $ showRef ref


data Object = Blob ByteString
            | Rec [(ByteString, RecItem)]
            | ZeroObject
    deriving (Show)

data RecItem = RecInt Integer
             | RecNum Rational
             | RecText Text
             | RecBinary ByteString
             | RecDate ZonedTime
             | RecJson J.Value
             | RecRef Ref
    deriving (Show)

serializeObject :: Object -> BL.ByteString
serializeObject = \case
    Blob cnt -> BL.fromChunks [BC.pack "blob ", BC.pack (show $ B.length cnt), BC.singleton '\n', cnt]
    Rec rec -> let cnt = BL.fromChunks $ concatMap (uncurry serializeRecItem) rec
                in BL.fromChunks [BC.pack "rec ", BC.pack (show $ BL.length cnt), BC.singleton '\n'] `BL.append` cnt
    ZeroObject -> BL.empty

storeObject :: Storage -> Object -> IO Ref
storeObject storage = \case
    ZeroObject -> return $ zeroRef storage
    obj -> storeRawBytes storage $ serializeObject obj

storeRawBytes :: Storage -> BL.ByteString -> IO Ref
storeRawBytes st raw = do
  let ref = Ref st $ hashFinalize $ hashUpdates hashInit $ BL.toChunks raw
  writeFileOnce (refPath ref) $ compress raw
  return ref

serializeRecItem :: ByteString -> RecItem -> [ByteString]
serializeRecItem name (RecInt x) = [name, BC.pack ":i", BC.singleton ' ', BC.pack (show x), BC.singleton '\n']
serializeRecItem name (RecNum x) = [name, BC.pack ":n", BC.singleton ' ', BC.pack (showRatio x), BC.singleton '\n']
serializeRecItem name (RecText x) = [name, BC.pack ":t", BC.singleton ' ', escaped, BC.singleton '\n']
    where escaped = BC.concatMap escape $ encodeUtf8 x
          escape '\\' = BC.pack "\\\\"
          escape '\n' = BC.pack "\\n"
          escape c    = BC.singleton c
serializeRecItem name (RecBinary x) = [name, BC.pack ":b ", convertToBase Base64 x, BC.singleton '\n']
serializeRecItem name (RecDate x) = [name, BC.pack ":d", BC.singleton ' ', BC.pack (formatTime defaultTimeLocale "%s %z" x), BC.singleton '\n']
serializeRecItem name (RecJson x) = [name, BC.pack ":j", BC.singleton ' '] ++ BL.toChunks (J.encode x) ++ [BC.singleton '\n']
serializeRecItem name (RecRef x) = [name, BC.pack ":r.b2 ", showRef x, BC.singleton '\n']

lazyLoadObject :: Ref -> Object
lazyLoadObject = fst . lazyLoadObject'

lazyLoadBytes :: Ref -> BL.ByteString
lazyLoadBytes = snd . lazyLoadObject'

lazyLoadObject' :: Ref -> (Object, BL.ByteString)
lazyLoadObject' ref | isZeroRef ref = (ZeroObject, BL.empty)
lazyLoadObject' ref@(Ref st rhash) = unsafePerformIO $ do
    file <- decompress <$> (BL.readFile $ refPath ref)
    let Ref _ chash = Ref st $ hashFinalize $ hashUpdates hashInit $ BL.toChunks file
    when (chash /= rhash) $ error $ "Hash mismatch on object " ++ BC.unpack (showRef ref) {- TODO throw -}
    let obj = case runExcept $ deserializeObject st file of
                   Left err -> error $ err ++ ", ref " ++ BC.unpack (showRef ref) {- TODO throw -}
                   Right (x, rest) | BL.null rest -> x
                                   | otherwise -> error $ "Superfluous content after " ++ BC.unpack (showRef ref) {- TODO throw -}
    return (obj, file)

deserializeObject :: Storage -> BL.ByteString -> Except String (Object, BL.ByteString)
deserializeObject _  bytes | BL.null bytes = return (ZeroObject, bytes)
deserializeObject st bytes =
    case BLC.break (=='\n') bytes of
        (line, rest) | Just (otype, len) <- splitObjPrefix line -> do
            let (content, next) = first BL.toStrict $ BL.splitAt (fromIntegral len) $ BL.drop 1 rest
            guard $ B.length content == len
            (,next) <$> case otype of
                 _ | otype == BC.pack "blob" -> return $ Blob content
                   | otype == BC.pack "rec" -> maybe (throwError $ "Malformed record item ")
                                                   (return . Rec) $ sequence $ map parseRecLine $ BC.lines content
                   | otherwise -> throwError $ "Unknown object type"
        _ -> throwError $ "Malformed object"
    where splitObjPrefix line = do
              [otype, tlen] <- return $ BLC.words line
              (len, rest) <- BLC.readInt tlen
              guard $ BL.null rest
              return (BL.toStrict otype, len)

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
                          "j" -> RecJson <$> J.decode (BL.fromStrict content)
                          "r.b2" -> RecRef <$> unsafeReadRef st content
                          _   -> Nothing
              return (name, val)

deserializeObjects :: Storage -> BL.ByteString -> Except String [Object]
deserializeObjects _  bytes | BL.null bytes = return []
deserializeObjects st bytes = do (obj, rest) <- deserializeObject st bytes
                                 (obj:) <$> deserializeObjects st rest


collectObjects :: Object -> [Object]
collectObjects obj = obj : map fromStored (fst $ collectOtherStored S.empty obj)

collectStoredObjects :: Stored Object -> [Stored Object]
collectStoredObjects obj = obj : (fst $ collectOtherStored S.empty $ fromStored obj)

collectOtherStored :: Set Ref -> Object -> ([Stored Object], Set Ref)
collectOtherStored seen (Rec items) = foldr helper ([], seen) $ map snd items
    where helper (RecRef r) (xs, s) | r `S.notMember` s = let o = wrappedLoad r
                                                              (xs', s') = collectOtherStored (S.insert r s) $ fromStored o
                                                           in ((o : xs') ++ xs, s')
          helper _          (xs, s) = (xs, s)
collectOtherStored seen _ = ([], seen)


data Head = Head String Ref
    deriving (Show)

headName :: Head -> String
headName (Head name _) = name

headRef :: Head -> Ref
headRef (Head _ ref) = ref

headObject :: Storable a => Head -> a
headObject = load . headRef


loadHeads :: Storage -> IO [Head]
loadHeads s@(Storage spath) = do
    let hpath = spath ++ "/heads/"
    files <- filterM (doesFileExist . (hpath++)) =<< getDirectoryContents hpath
    forM files $ \hname -> do
        (h:_) <- BC.lines <$> B.readFile (hpath ++ "/" ++ hname)
        Just ref <- readRef s h
        return $ Head hname ref

loadHead :: Storage -> String -> IO Head
loadHead s@(Storage spath) hname = do
    let hpath = spath ++ "/heads/"
    (h:_) <- BC.lines <$> B.readFile (hpath ++ hname)
    Just ref <- readRef s h
    return $ Head hname ref

replaceHead :: Storable a => a -> Either (Storage, String) Head -> IO (Either (Maybe Head) Head)
replaceHead obj prev = do
    ref <- store st obj
    writeFileChecked filename (either (const Nothing) (Just . showRefL . headRef) prev) (showRefL ref) >>= \case
        Left Nothing -> return $ Left Nothing
        Left (Just bs) -> do Just oref <- readRef st $ BC.takeWhile (/='\n') bs
                             return $ Left $ Just $ Head name oref
        Right () -> return $ Right $ Head name ref
    where (st@(Storage spath), name) = either id (\(Head n (Ref s _)) -> (s, n)) prev
          filename = spath ++ "/heads/" ++ name
          showRefL ref = showRef ref `B.append` BC.singleton '\n'


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


class Storable a where
    store' :: a -> Store
    load' :: Load a

    store :: Storage -> a -> IO Ref
    store st = storeObject st <=< evalStore st . store'
    load :: Ref -> a
    load ref = let Load f = load'
                in either (error {- TODO throw -} . ((BC.unpack (showRef ref) ++ ": ")++)) id $ f ref $ lazyLoadObject ref

class Storable a => ZeroStorable a where
    fromZero :: Storage -> a

data Store = StoreBlob ByteString
           | StoreRec (Storage -> [IO [(ByteString, RecItem)]])
           | StoreZero

evalStore :: Storage -> Store -> IO Object
evalStore _ (StoreBlob x) = return $ Blob x
evalStore s (StoreRec f) = Rec . concat <$> sequence (f s)
evalStore _ StoreZero = return ZeroObject

type StoreRec = ReaderT Storage (Writer [IO [(ByteString, RecItem)]]) ()

data Load a = Load (Ref -> Object -> Either String a)

type LoadRec a = ReaderT (Ref, [(ByteString, RecItem)]) (Either String) a


instance Storable Object where
    store' (Blob bs) = StoreBlob bs
    store' (Rec xs) = StoreRec $ const $ map (return.return) xs
    store' ZeroObject = StoreZero

    load' = Load $ const return

    store = storeObject
    load = lazyLoadObject

instance Storable ByteString where
    store' = storeBlob
    load' = loadBlob id

instance Storable a => Storable [a] where
    store' []     = storeZero
    store' (x:xs) = storeRec $ do
        storeRef "i" x
        storeRef "n" xs

    load' = Load $ \ref -> \case
              ZeroObject -> return []
              obj        ->
                let Load fres = loadRec $ (:)
                        <$> loadRef "i"
                        <*> loadRef "n"
                 in fres ref obj

instance Storable a => ZeroStorable [a] where
    fromZero _ = []


storeBlob :: ByteString -> Store
storeBlob = StoreBlob

storeRec :: StoreRec -> Store
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


storeInt :: Integral a => String -> a -> StoreRec
storeInt name x = tell [return [(BC.pack name, RecInt $ toInteger x)]]

storeMbInt :: Integral a => String -> Maybe a -> StoreRec
storeMbInt name = maybe (return ()) (storeInt name)

storeNum :: (Real a, Fractional a) => String -> a -> StoreRec
storeNum name x = tell [return [(BC.pack name, RecNum $ toRational x)]]

storeMbNum :: (Real a, Fractional a) => String -> Maybe a -> StoreRec
storeMbNum name = maybe (return ()) (storeNum name)

storeText :: StorableText a => String -> a -> StoreRec
storeText name x = tell [return [(BC.pack name, RecText $ toText x)]]

storeMbText :: StorableText a => String -> Maybe a -> StoreRec
storeMbText name = maybe (return ()) (storeText name)

storeBinary :: BA.ByteArrayAccess a => String -> a -> StoreRec
storeBinary name x = tell [return [(BC.pack name, RecBinary $ BA.convert x)]]

storeMbBinary :: BA.ByteArrayAccess a => String -> Maybe a -> StoreRec
storeMbBinary name = maybe (return ()) (storeBinary name)

storeDate :: StorableDate a => String -> a -> StoreRec
storeDate name x = tell [return [(BC.pack name, RecDate $ toDate x)]]

storeMbDate :: StorableDate a => String -> Maybe a -> StoreRec
storeMbDate name = maybe (return ()) (storeDate name)

storeJson :: J.ToJSON a => String -> a -> StoreRec
storeJson name x = tell [return [(BC.pack name, RecJson $ J.toJSON x)]]

storeMbJson :: J.ToJSON a => String -> Maybe a -> StoreRec
storeMbJson name = maybe (return ()) (storeJson name)

storeRef :: Storable a => String -> a -> StoreRec
storeRef name x = do
    s <- ask
    tell $ (:[]) $ do
        ref <- store s x
        return [(BC.pack name, RecRef ref)]

storeMbRef :: Storable a => String -> Maybe a -> StoreRec
storeMbRef name = maybe (return ()) (storeRef name)

storeRawRef :: String -> Ref -> StoreRec
storeRawRef name ref = tell [return [(BC.pack name, RecRef ref)]]

storeMbRawRef :: String -> Maybe Ref -> StoreRec
storeMbRawRef name = maybe (return ()) (storeRawRef name)

storeZRef :: ZeroStorable a => String -> a -> StoreRec
storeZRef name x = do
    s <- ask
    tell $ (:[]) $ do
        ref <- store s x
        return $ if isZeroRef ref then []
                                  else [(BC.pack name, RecRef ref)]


loadBlob :: (ByteString -> a) -> Load a
loadBlob f = Load $ const $ \case
    Blob x -> return $ f x
    _      -> throwError "Expecting blob"

loadRec :: LoadRec a -> Load a
loadRec lrec = Load $ \ref -> \case
    Rec rs -> runReaderT lrec (ref, rs)
    _      -> throwError "Expecting record"

loadZero :: a -> Load a
loadZero x = Load $ const $ \case
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

loadDate :: StorableDate a => String -> LoadRec a
loadDate name = maybe (throwError $ "Missing record item '"++name++"'") return =<< loadMbDate name

loadMbDate :: StorableDate a => String -> LoadRec (Maybe a)
loadMbDate name = asks (lookup (BC.pack name) . snd) >>= \case
    Nothing -> return Nothing
    Just (RecDate x) -> return $ Just $ fromDate x
    Just _ -> throwError $ "Expecting type date of record item '"++name++"'"

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

loadRef :: Storable a => String -> LoadRec a
loadRef name = load <$> loadRawRef name

loadMbRef :: Storable a => String -> LoadRec (Maybe a)
loadMbRef name = fmap load <$> loadMbRawRef name

loadZRef :: ZeroStorable a => String -> LoadRec a
loadZRef name = loadMbRef name >>= \case
                    Nothing -> do Ref st _ <- asks fst
                                  return $ fromZero st
                    Just x  -> return x


data Stored a = Stored Ref a
    deriving (Show)

instance Eq (Stored a) where
    Stored r1 _ == Stored r2 _  =  r1 == r2

instance Ord (Stored a) where
    compare (Stored r1 _) (Stored r2 _) = compare r1 r2

instance Storable a => Storable (Stored a) where
    store st (Stored ref@(Ref st' _) x) | st' == st = return ref
                                        | otherwise = store st x
    store' (Stored _ x) = store' x
    load' = Load $ \ref obj ->
                let Load fres = load'
                 in Stored ref <$> fres ref obj

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


data StoreInfo = StoreInfo
    { infoDate :: ZonedTime
    , infoNote :: Maybe Text
    }
    deriving (Show)

makeStoreInfo :: IO StoreInfo
makeStoreInfo = StoreInfo
    <$> getZonedTime
    <*> pure Nothing

storeInfoRec :: StoreInfo -> StoreRec
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


data List a = ListNil
            | ListItem (Maybe Ref) (Maybe Ref) (Maybe (Stored a)) (StoredList a)
    deriving (Show)

type StoredList a = Stored (List a)

instance Storable a => Storable (List a) where
    store' ListNil = storeZero
    store' (ListItem remove after item next) = storeRec $ do
        storeMbRawRef "r" remove
        storeMbRawRef "a" after
        storeMbRef "i" item
        storeRef "n" next

    load' = Load $ \ref -> \case
              ZeroObject -> return ListNil
              obj        ->
                let Load fres = loadRec $ ListItem
                        <$> loadMbRawRef "r"
                        <*> loadMbRawRef "a"
                        <*> loadMbRef "i"
                        <*> loadRef "n"
                 in fres ref obj

instance Storable a => ZeroStorable (List a) where
    fromZero _ = ListNil


emptySList :: Storable a => Storage -> IO (StoredList a)
emptySList st = wrappedStore st ListNil

fromSList :: StoredList a -> [a]
fromSList = map fromStored . storedFromSList

storedFromSList :: StoredList a -> [Stored a]
storedFromSList = fromSList' []
    where fromSList' :: [(Ref, Bool, [Stored a])] -> StoredList a -> [Stored a]
          fromSList' _ (Stored _ ListNil) = []
          fromSList' repl (Stored cref (ListItem rref aref x rest)) =
              case (rref, aref) of
                   (Nothing, Nothing) -> let (rx, repl') = findRepl cref x repl
                                          in rx ++ fromSList' repl' rest
                   (Just r , Nothing) -> fromSList' (addReplace cref r x repl) rest
                   (Nothing, Just a ) -> fromSList' (addInsert  cref a x repl) rest
                   (Just r , Just a ) -> fromSList' (addReplace cref r x $ addInsert cref a x repl) rest

          addReplace = findAddRepl False
          addInsert = findAddRepl True

          findAddRepl :: Bool -> Ref -> Ref -> Maybe (Stored a) -> [(Ref, Bool, [Stored a])] -> [(Ref, Bool, [Stored a])]
          findAddRepl keep c t x rs = let (x', rs') = findRepl c x rs
                                       in addRepl keep c t x' rs'

          addRepl :: Bool -> Ref -> Ref -> [Stored a] -> [(Ref, Bool, [Stored a])] -> [(Ref, Bool, [Stored a])]
          addRepl keep _ t x [] = [(t, keep, x)]
          addRepl keep c t x ((pr, pk, px) : rs)
              | pr == c   = (t , keep, x ++ px) : rs
              | pr == t   = (t , pk, px ++ x) : rs
              | otherwise = (pr, pk, px) : addRepl keep c t x rs

          findRepl :: Ref -> Maybe (Stored a) -> [(Ref, Bool, [Stored a])] -> ([Stored a], [(Ref, Bool, [Stored a])])
          findRepl _ x [] = (maybeToList x, [])
          findRepl c x ((pr, pk, px) : rs)
              | pr == c   = (if pk then maybe id (:) x px else px, rs)
              | otherwise = ((pr, pk, px):) <$> findRepl c x rs

slistAdd :: Storable a => a -> StoredList a -> IO (StoredList a)
slistAdd x next@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistAddS sx next

slistAddS :: Storable a => Stored a -> StoredList a -> IO (StoredList a)
slistAddS sx next@(Stored (Ref st _) _) = wrappedStore st (ListItem Nothing Nothing (Just sx) next)

slistInsert :: Storable a => Stored a -> a -> StoredList a -> IO (StoredList a)
slistInsert after x next@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistInsertS after sx next

slistInsertS :: Storable a => Stored a -> Stored a -> StoredList a -> IO (StoredList a)
slistInsertS after sx next@(Stored (Ref st _) _) = wrappedStore st $ ListItem Nothing (findSListRef after next) (Just sx) next

slistRemove :: Storable a => Stored a -> StoredList a -> IO (StoredList a)
slistRemove rm next@(Stored (Ref st _) _) = wrappedStore st $ ListItem (findSListRef rm next) Nothing Nothing next

slistReplace :: Storable a => Stored a -> a -> StoredList a -> IO (StoredList a)
slistReplace rm x next@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistReplaceS rm sx next

slistReplaceS :: Storable a => Stored a -> Stored a -> StoredList a -> IO (StoredList a)
slistReplaceS rm sx next@(Stored (Ref st _) _) = wrappedStore st $ ListItem (findSListRef rm next) Nothing (Just sx) next

findSListRef :: Stored a -> StoredList a -> Maybe Ref
findSListRef _ (Stored _ ListNil) = Nothing
findSListRef x (Stored ref (ListItem _ _ y next)) | y == Just x = Just ref
                                                  | otherwise   = findSListRef x next

mapFromSList :: Storable a => StoredList a -> Map Ref (Stored a)
mapFromSList list = helper list M.empty
    where helper :: Storable a => StoredList a -> Map Ref (Stored a) -> Map Ref (Stored a)
          helper (Stored _ ListNil) cur = cur
          helper (Stored _ (ListItem (Just rref) _ (Just x) rest)) cur =
              let rxref = case load rref of
                               ListItem _ _ (Just rx) _  -> sameType rx x $ storedRef rx
                               _ -> error "mapFromSList: malformed list"
               in helper rest $ case M.lookup (storedRef x) cur of
                                     Nothing -> M.insert rxref x cur
                                     Just x' -> M.insert rxref x' cur
          helper (Stored _ (ListItem _ _ _ rest)) cur = helper rest cur
          sameType :: a -> a -> b -> b
          sameType _ _ x = x

updateOld :: Map Ref (Stored a) -> Stored a -> Stored a
updateOld m x = fromMaybe x $ M.lookup (storedRef x) m


data StoreUpdate a = StoreKeep
                   | StoreReplace a
                   | StoreRemove

withStoredListItem :: (Storable a) => (a -> Bool) -> StoredList a -> (a -> IO (StoreUpdate a)) -> IO (StoredList a)
withStoredListItem p list f = withStoredListItemS (p . fromStored) list (suMap (wrappedStore $ storedStorage list) <=< f . fromStored)
    where suMap :: Monad m => (a -> m b) -> StoreUpdate a -> m (StoreUpdate b)
          suMap _ StoreKeep = return StoreKeep
          suMap g (StoreReplace x) = return . StoreReplace =<< g x
          suMap _ StoreRemove = return StoreRemove

withStoredListItemS :: (Storable a) => (Stored a -> Bool) -> StoredList a -> (Stored a -> IO (StoreUpdate (Stored a))) -> IO (StoredList a)
withStoredListItemS p list f = do
    case find p $ storedFromSList list of
         Just sx -> f sx >>= \case StoreKeep -> return list
                                   StoreReplace nx -> slistReplaceS sx nx list
                                   StoreRemove -> slistRemove sx list
         Nothing -> return list


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

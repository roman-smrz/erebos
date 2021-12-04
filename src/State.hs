module State (
    LocalState(..),
    SharedState, SharedType(..),
    SharedTypeID, mkSharedTypeID,

    loadLocalStateHead,
    updateLocalState, updateLocalState_,

    updateSharedState, updateSharedState_,
    lookupSharedValue, makeSharedStateUpdate,

    headLocalIdentity,

    mergeSharedIdentity,
    updateSharedIdentity,
    interactiveIdentityUpdate,
) where

import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as U

import System.IO

import Identity
import PubKey
import Storage
import Storage.Merge

data LocalState = LocalState
    { lsIdentity :: Stored (Signed IdentityData)
    , lsShared :: [Stored SharedState]
    }

data SharedState = SharedState
    { ssPrev :: [Stored SharedState]
    , ssType :: Maybe SharedTypeID
    , ssValue :: [Ref]
    }

newtype SharedTypeID = SharedTypeID UUID
    deriving (Eq, Ord, StorableUUID)

mkSharedTypeID :: String -> SharedTypeID
mkSharedTypeID = maybe (error "Invalid shared type ID") SharedTypeID . U.fromString

class Storable a => SharedType a where
    sharedTypeID :: proxy a -> SharedTypeID

instance Storable LocalState where
    store' st = storeRec $ do
        storeRef "id" $ lsIdentity st
        mapM_ (storeRef "shared") $ lsShared st

    load' = loadRec $ LocalState
        <$> loadRef "id"
        <*> loadRefs "shared"

instance HeadType LocalState where
    headTypeID _ = mkHeadTypeID "1d7491a9-7bcb-4eaa-8f13-c8c4c4087e4e"

instance Storable SharedState where
    store' st = storeRec $ do
        mapM_ (storeRef "PREV") $ ssPrev st
        storeMbUUID "type" $ ssType st
        mapM_ (storeRawRef "value") $ ssValue st

    load' = loadRec $ SharedState
        <$> loadRefs "PREV"
        <*> loadMbUUID "type"
        <*> loadRawRefs "value"

instance SharedType (Signed IdentityData) where
    sharedTypeID _ = mkSharedTypeID "0c6c1fe0-f2d7-4891-926b-c332449f7871"


loadLocalStateHead :: Storage -> IO (Head LocalState)
loadLocalStateHead st = loadHeads st >>= \case
    (h:_) -> return h
    [] -> do
        putStr "Name: "
        hFlush stdout
        name <- T.getLine

        putStr "Device: "
        hFlush stdout
        devName <- T.getLine

        owner <- if
            | T.null name -> return Nothing
            | otherwise -> Just <$> createIdentity st (Just name) Nothing

        identity <- createIdentity st (if T.null devName then Nothing else Just devName) owner

        shared <- wrappedStore st $ SharedState
            { ssPrev = []
            , ssType = Just $ sharedTypeID @(Signed IdentityData) Proxy
            , ssValue = [storedRef $ idData $ fromMaybe identity owner]
            }
        storeHead st $ LocalState
            { lsIdentity = idData identity
            , lsShared = [shared]
            }

headLocalIdentity :: Head LocalState -> UnifiedIdentity
headLocalIdentity h =
    let ls = headObject h
     in maybe (error "failed to verify local identity")
            (updateOwners (lookupSharedValue $ lsShared ls))
            (validateIdentity $ lsIdentity ls)


updateLocalState_ :: Head LocalState -> (Stored LocalState -> IO (Stored LocalState)) -> IO ()
updateLocalState_ h f = updateLocalState h (fmap (,()) . f)

updateLocalState :: Head LocalState -> (Stored LocalState -> IO (Stored LocalState, a)) -> IO a
updateLocalState h f = snd <$> updateHead h f

updateSharedState_ :: SharedType a => Head LocalState -> ([Stored a] -> IO ([Stored a])) -> IO ()
updateSharedState_ h f = updateSharedState h (fmap (,()) . f)

updateSharedState :: forall a b. SharedType a => Head LocalState -> ([Stored a] -> IO ([Stored a], b)) -> IO b
updateSharedState h f = updateLocalState h $ \ls -> do
    let shared = lsShared $ fromStored ls
        val = lookupSharedValue shared
        st = refStorage $ headRef h
    (val', x) <- f val
    (,x) <$> if val' == val
                then return ls
                else do shared' <- makeSharedStateUpdate st val' shared
                        wrappedStore st (fromStored ls) { lsShared = [shared'] }

lookupSharedValue :: forall a. SharedType a => [Stored SharedState] -> [Stored a]
lookupSharedValue = map wrappedLoad . concatMap (ssValue . fromStored) . filterAncestors . helper
    where helper (x:xs) | Just sid <- ssType (fromStored x), sid == sharedTypeID @a Proxy = x : helper xs
                        | otherwise = helper $ ssPrev (fromStored x) ++ xs
          helper [] = []

makeSharedStateUpdate :: forall a. SharedType a => Storage -> [Stored a] -> [Stored SharedState] -> IO (Stored SharedState)
makeSharedStateUpdate st val prev = wrappedStore st SharedState
    { ssPrev = prev
    , ssType = Just $ sharedTypeID @a Proxy
    , ssValue = storedRef <$> val
    }


mergeSharedIdentity :: Head LocalState -> IO UnifiedIdentity
mergeSharedIdentity = flip updateSharedState $ \sdata -> do
    let Just cidentity = validateIdentityF sdata
    identity <- mergeIdentity cidentity
    return ([idData identity], identity)

updateSharedIdentity :: Head LocalState -> IO ()
updateSharedIdentity = flip updateSharedState_ $ \sdata -> do
    let Just identity = validateIdentityF sdata
    (:[]) . idData <$> interactiveIdentityUpdate identity

interactiveIdentityUpdate :: Foldable m => Identity m -> IO UnifiedIdentity
interactiveIdentityUpdate identity = do
    let st = storedStorage $ head $ toList $ idDataF $ identity
        public = idKeyIdentity identity

    T.putStr $ T.concat $ concat
        [ [ T.pack "Name" ]
        , case idName identity of
               Just name -> [T.pack " [", name, T.pack "]"]
               Nothing -> []
        , [ T.pack ": " ]
        ]
    hFlush stdout
    name <- T.getLine

    if  | T.null name -> mergeIdentity identity
        | otherwise -> do
            Just secret <- loadKey public
            maybe (error "created invalid identity") return . validateIdentity =<<
                wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
                { iddPrev = toList $ idDataF identity
                , iddName = Just name
                }

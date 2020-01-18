module State (
    LocalState(..),
    SharedState, SharedType(..),
    SharedTypeID, mkSharedTypeID,

    loadLocalState, loadLocalStateHead,
    updateLocalState, updateLocalState_,

    updateSharedState, updateSharedState_,
    lookupSharedValue, makeSharedStateUpdate,

    loadLocalIdentity, headLocalIdentity,

    mergeSharedIdentity,
    updateSharedIdentity,
    interactiveIdentityUpdate,
) where

import Control.Monad

import Data.Foldable
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as U

import System.IO

import Identity
import Message
import PubKey
import Storage
import Storage.List
import Storage.Merge

data LocalState = LocalState
    { lsIdentity :: Stored (Signed IdentityData)
    , lsShared :: [Stored SharedState]
    , lsMessages :: StoredList DirectMessageThread -- TODO: move to shared
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
        storeRef "dmsg" $ lsMessages st

    load' = loadRec $ LocalState
        <$> loadRef "id"
        <*> loadRefs "shared"
        <*> loadRef "dmsg"

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


loadLocalState :: Storage -> IO (Stored LocalState)
loadLocalState = return . wrappedLoad . headRef <=< loadLocalStateHead

loadLocalStateHead :: Storage -> IO Head
loadLocalStateHead st = loadHeadDef st "erebos" $ do
    putStr "Name: "
    hFlush stdout
    name <- T.getLine

    putStr "Device: "
    hFlush stdout
    devName <- T.getLine

    (owner, secret) <- if
        | T.null name -> return (Nothing, Nothing)
        | otherwise -> do
            (secret, public) <- generateKeys st
            (_secretMsg, publicMsg) <- generateKeys st

            return . (, Just secret) . Just =<< wrappedStore st =<< sign secret =<<
                wrappedStore st (emptyIdentityData public)
                { iddName = Just name, iddKeyMessage = Just publicMsg }

    (devSecret, devPublic) <- generateKeys st
    (_devSecretMsg, devPublicMsg) <- generateKeys st

    identity <- wrappedStore st =<< maybe return signAdd secret =<< sign devSecret =<< wrappedStore st (emptyIdentityData devPublic)
        { iddName = if T.null devName then Nothing else Just devName
        , iddOwner = owner
        , iddKeyMessage = Just devPublicMsg
        }

    msgs <- emptySList st

    shared <- wrappedStore st $ SharedState
        { ssPrev = []
        , ssType = Just $ sharedTypeID @(Signed IdentityData) Proxy
        , ssValue = [storedRef $ fromMaybe identity owner]
        }
    return $ LocalState
        { lsIdentity = identity
        , lsShared = [shared]
        , lsMessages = msgs
        }

loadLocalIdentity :: Storage -> IO UnifiedIdentity
loadLocalIdentity = return . headLocalIdentity <=< loadLocalStateHead

headLocalIdentity :: Head -> UnifiedIdentity
headLocalIdentity h =
    let ls = load $ headRef h
     in maybe (error "failed to verify local identity")
            (updateOwners (lookupSharedValue $ lsShared ls))
            (validateIdentity $ lsIdentity ls)


updateLocalState_ :: Storage -> (Stored LocalState -> IO (Stored LocalState)) -> IO ()
updateLocalState_ st f = updateLocalState st (fmap (,()) . f)

updateLocalState :: Storage -> (Stored LocalState -> IO (Stored LocalState, a)) -> IO a
updateLocalState st f = do
    Just erebosHead <- loadHead st "erebos"
    let ls = wrappedLoad (headRef erebosHead)
    (ls', x) <- f ls
    when (ls' /= ls) $ do
        Right _ <- replaceHead ls' (Right erebosHead)
        return ()
    return x


updateSharedState_ :: SharedType a => Storage -> ([Stored a] -> IO ([Stored a])) -> IO ()
updateSharedState_ st f = updateSharedState st (fmap (,()) . f)

updateSharedState :: forall a b. SharedType a => Storage -> ([Stored a] -> IO ([Stored a], b)) -> IO b
updateSharedState st f = updateLocalState st $ \ls -> do
    let shared = lsShared $ fromStored ls
        val = lookupSharedValue shared
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


mergeSharedIdentity :: Storage -> IO UnifiedIdentity
mergeSharedIdentity st = updateSharedState st $ \sdata -> do
    let Just cidentity = validateIdentityF sdata
    identity <- mergeIdentity cidentity
    return ([idData identity], identity)

updateSharedIdentity :: Storage -> IO ()
updateSharedIdentity st = updateSharedState_ st $ \sdata -> do
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

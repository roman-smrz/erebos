{-# LANGUAGE UndecidableInstances #-}

module Erebos.Identity (
    Identity, ComposedIdentity, UnifiedIdentity,
    IdentityData(..), ExtendedIdentityData(..), IdentityExtension(..),
    idData, idDataF, idExtData, idExtDataF,
    idName, idOwner, idUpdates, idKeyIdentity, idKeyMessage,
    eiddBase, eiddStoredBase,
    eiddName, eiddOwner, eiddKeyIdentity, eiddKeyMessage,

    emptyIdentityData,
    emptyIdentityExtension,
    createIdentity,
    validateIdentity, validateIdentityF, validateIdentityFE,
    validateExtendedIdentity, validateExtendedIdentityF, validateExtendedIdentityFE,
    loadIdentity, loadMbIdentity, loadUnifiedIdentity, loadMbUnifiedIdentity,

    mergeIdentity, toUnifiedIdentity, toComposedIdentity,
    updateIdentity, updateOwners,
    sameIdentity,

    unfoldOwners,
    finalOwner,
    displayIdentity,
) where

import Control.Arrow
import Control.Monad
import Control.Monad.Except
import Control.Monad.Identity qualified as I
import Control.Monad.Reader

import Data.Either
import Data.Foldable
import Data.Function
import Data.List
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T

import Erebos.PubKey
import Erebos.Storable
import Erebos.Storage.Merge
import Erebos.Util

data Identity m = IdentityKind m => Identity
    { idData_ :: m (Stored (Signed ExtendedIdentityData))
    , idName_ :: Maybe Text
    , idOwner_ :: Maybe ComposedIdentity
    , idUpdates_ :: [Stored (Signed ExtendedIdentityData)]
    , idKeyIdentity_ :: Stored PublicKey
    , idKeyMessage_ :: Stored PublicKey
    }

deriving instance Show (m (Stored (Signed ExtendedIdentityData))) => Show (Identity m)

class (Functor f, Foldable f) => IdentityKind f where
    ikFilterAncestors :: Storable a => f (Stored a) -> f (Stored a)

instance IdentityKind I.Identity where
    ikFilterAncestors = id

instance IdentityKind [] where
    ikFilterAncestors = filterAncestors

type ComposedIdentity = Identity []
type UnifiedIdentity = Identity I.Identity

instance Eq (m (Stored (Signed ExtendedIdentityData))) => Eq (Identity m) where
    (==) = (==) `on` (idData_ &&& idUpdates_)

data IdentityData = IdentityData
    { iddPrev :: [Stored (Signed IdentityData)]
    , iddName :: Maybe Text
    , iddOwner :: Maybe (Stored (Signed IdentityData))
    , iddKeyIdentity :: Stored PublicKey
    , iddKeyMessage :: Maybe (Stored PublicKey)
    }
    deriving (Show)

data IdentityExtension = IdentityExtension
    { idePrev :: [Stored (Signed ExtendedIdentityData)]
    , ideBase :: Stored (Signed IdentityData)
    , ideName :: Maybe Text
    , ideOwner :: Maybe (Stored (Signed ExtendedIdentityData))
    }
    deriving (Show)

data ExtendedIdentityData = BaseIdentityData IdentityData
                          | ExtendedIdentityData IdentityExtension
    deriving (Show)

baseToExtended :: Stored (Signed IdentityData) -> Stored (Signed ExtendedIdentityData)
baseToExtended = unsafeMapStored (unsafeMapSigned BaseIdentityData)

instance Storable IdentityData where
    store' idt = storeRec $ do
        mapM_ (storeRef "SPREV") $ iddPrev idt
        storeMbText "name" $ iddName idt
        storeMbRef "owner" $ iddOwner idt
        storeRef "key-id" $ iddKeyIdentity idt
        storeMbRef "key-msg" $ iddKeyMessage idt

    load' = loadRec $ IdentityData
        <$> loadRefs "SPREV"
        <*> loadMbText "name"
        <*> loadMbRef "owner"
        <*> loadRef "key-id"
        <*> loadMbRef "key-msg"

instance Storable IdentityExtension where
    store' IdentityExtension {..} = storeRec $ do
        mapM_ (storeRef "SPREV") idePrev
        storeRef "SBASE" ideBase
        storeMbText "name" ideName
        storeMbRef "owner" ideOwner

    load' = loadRec $ IdentityExtension
        <$> loadRefs "SPREV"
        <*> loadRef "SBASE"
        <*> loadMbText "name"
        <*> loadMbRef "owner"

instance Storable ExtendedIdentityData where
    store' (BaseIdentityData idata) = store' idata
    store' (ExtendedIdentityData idata) = store' idata

    load' = msum
        [ BaseIdentityData <$> load'
        , ExtendedIdentityData <$> load'
        ]

instance Mergeable (Maybe ComposedIdentity) where
    type Component (Maybe ComposedIdentity) = Signed ExtendedIdentityData
    mergeSorted = validateExtendedIdentityF
    toComponents = maybe [] idExtDataF

idData :: UnifiedIdentity -> Stored (Signed IdentityData)
idData = I.runIdentity . idDataF

idDataF :: Identity m -> m (Stored (Signed IdentityData))
idDataF idt@Identity {} = ikFilterAncestors . fmap eiddStoredBase . idData_ $ idt

idExtData :: UnifiedIdentity -> Stored (Signed ExtendedIdentityData)
idExtData = I.runIdentity . idExtDataF

idExtDataF :: Identity m -> m (Stored (Signed ExtendedIdentityData))
idExtDataF = idData_

idName :: Identity m -> Maybe Text
idName = idName_

idOwner :: Identity m -> Maybe ComposedIdentity
idOwner = idOwner_

idUpdates :: Identity m -> [Stored (Signed ExtendedIdentityData)]
idUpdates = idUpdates_

idKeyIdentity :: Identity m -> Stored PublicKey
idKeyIdentity = idKeyIdentity_

idKeyMessage :: Identity m -> Stored PublicKey
idKeyMessage = idKeyMessage_

eiddPrev :: ExtendedIdentityData -> [Stored (Signed ExtendedIdentityData)]
eiddPrev (BaseIdentityData idata) = baseToExtended <$> iddPrev idata
eiddPrev (ExtendedIdentityData IdentityExtension {..}) = baseToExtended ideBase : idePrev

eiddBase :: ExtendedIdentityData -> IdentityData
eiddBase (BaseIdentityData idata) = idata
eiddBase (ExtendedIdentityData IdentityExtension {..}) = fromSigned ideBase

eiddStoredBase :: Stored (Signed ExtendedIdentityData) -> Stored (Signed IdentityData)
eiddStoredBase ext = case fromSigned ext of
                          (BaseIdentityData idata) -> unsafeMapStored (unsafeMapSigned (const idata)) ext
                          (ExtendedIdentityData IdentityExtension {..}) -> ideBase

eiddName :: ExtendedIdentityData -> Maybe Text
eiddName (BaseIdentityData idata) = iddName idata
eiddName (ExtendedIdentityData IdentityExtension {..}) = ideName

eiddOwner :: ExtendedIdentityData -> Maybe (Stored (Signed ExtendedIdentityData))
eiddOwner (BaseIdentityData idata) = baseToExtended <$> iddOwner idata
eiddOwner (ExtendedIdentityData IdentityExtension {..}) = ideOwner

eiddKeyIdentity :: ExtendedIdentityData -> Stored PublicKey
eiddKeyIdentity = iddKeyIdentity . eiddBase

eiddKeyMessage :: ExtendedIdentityData -> Maybe (Stored PublicKey)
eiddKeyMessage = iddKeyMessage . eiddBase


emptyIdentityData :: Stored PublicKey -> IdentityData
emptyIdentityData key = IdentityData
    { iddName = Nothing
    , iddPrev = []
    , iddOwner = Nothing
    , iddKeyIdentity = key
    , iddKeyMessage = Nothing
    }

emptyIdentityExtension :: Stored (Signed IdentityData) -> IdentityExtension
emptyIdentityExtension base = IdentityExtension
    { idePrev = []
    , ideBase = base
    , ideName = Nothing
    , ideOwner = Nothing
    }

isExtension :: Stored (Signed ExtendedIdentityData) -> Bool
isExtension x = case fromSigned x of BaseIdentityData {} -> False
                                     _ -> True


createIdentity
    :: forall m e. (MonadStorage m, MonadError e m, FromErebosError e, MonadIO m)
    => Maybe Text -> Maybe UnifiedIdentity -> m UnifiedIdentity
createIdentity name owner = do
    st <- getStorage
    ( secret, public ) <- liftIO $ generateKeys st
    ( _secretMsg, publicMsg ) <- liftIO $ generateKeys st

    let signOwner :: Signed a -> m (Signed a)
        signOwner idd
            | Just o <- owner = do
                ownerSecret <- maybe (throwOtherError "failed to load private key") return =<<
                    loadKeyMb (iddKeyIdentity $ fromSigned $ idData o)
                signAdd ownerSecret idd
            | otherwise = return idd

    baseData <- mstore =<< signOwner =<< sign secret =<<
        mstore (emptyIdentityData public)
            { iddOwner = idData <$> owner
            , iddKeyMessage = Just publicMsg
            }
    let extOwner = do
            odata <- idExtData <$> owner
            guard $ isExtension odata
            return odata

    maybe (throwOtherError "created invalid identity") return =<< do
        validateExtendedIdentityF . I.Identity <$>
            if isJust name || isJust extOwner
               then mstore =<< signOwner =<< sign secret =<<
                       mstore . ExtendedIdentityData =<< return (emptyIdentityExtension baseData)
                       { ideName = name
                       , ideOwner = extOwner
                       }
               else return $ baseToExtended baseData

validateIdentity :: Stored (Signed IdentityData) -> Maybe UnifiedIdentity
validateIdentity = validateIdentityF . I.Identity

validateIdentityF :: IdentityKind m => m (Stored (Signed IdentityData)) -> Maybe (Identity m)
validateIdentityF = either (const Nothing) Just . runExcept . validateIdentityFE

validateIdentityFE :: IdentityKind m => m (Stored (Signed IdentityData)) -> Except String (Identity m)
validateIdentityFE = validateExtendedIdentityFE . fmap baseToExtended

validateExtendedIdentity :: Stored (Signed ExtendedIdentityData) -> Maybe UnifiedIdentity
validateExtendedIdentity = validateExtendedIdentityF . I.Identity

validateExtendedIdentityF :: IdentityKind m => m (Stored (Signed ExtendedIdentityData)) -> Maybe (Identity m)
validateExtendedIdentityF = either (const Nothing) Just . runExcept . validateExtendedIdentityFE

validateExtendedIdentityFE :: IdentityKind m => m (Stored (Signed ExtendedIdentityData)) -> Except String (Identity m)
validateExtendedIdentityFE mdata = do
    let idata = ikFilterAncestors mdata
    when (null idata) $ throwError "null data"
    mapM_ verifySignatures $ gatherPrevious S.empty $ toList idata
    Identity
        <$> pure idata
        <*> pure (lookupProperty eiddName idata)
        <*> case lookupProperty eiddOwner idata of
                 Nothing    -> return Nothing
                 Just owner -> return <$> validateExtendedIdentityFE [owner]
        <*> pure []
        <*> pure (eiddKeyIdentity $ fromSigned $ minimum idata)
        <*> case lookupProperty eiddKeyMessage idata of
                 Nothing -> throwError "no message key"
                 Just mk -> return mk

loadIdentity :: String -> LoadRec ComposedIdentity
loadIdentity name = maybe (throwOtherError "identity validation failed") return . validateExtendedIdentityF =<< loadRefs name

loadMbIdentity :: String -> LoadRec (Maybe ComposedIdentity)
loadMbIdentity name = return . validateExtendedIdentityF =<< loadRefs name

loadUnifiedIdentity :: String -> LoadRec UnifiedIdentity
loadUnifiedIdentity name = maybe (throwOtherError "identity validation failed") return . validateExtendedIdentity =<< loadRef name

loadMbUnifiedIdentity :: String -> LoadRec (Maybe UnifiedIdentity)
loadMbUnifiedIdentity name = return . (validateExtendedIdentity =<<) =<< loadMbRef name


gatherPrevious :: Set (Stored (Signed ExtendedIdentityData)) -> [Stored (Signed ExtendedIdentityData)] -> Set (Stored (Signed ExtendedIdentityData))
gatherPrevious res (n:ns) | n `S.member` res = gatherPrevious res ns
                          | otherwise        = gatherPrevious (S.insert n res) $ (eiddPrev $ fromSigned n) ++ ns
gatherPrevious res [] = res

verifySignatures :: Stored (Signed ExtendedIdentityData) -> Except String ()
verifySignatures sidd = do
    let idd = fromSigned sidd
        required = concat
            [ [ eiddKeyIdentity idd ]
            , map (eiddKeyIdentity . fromSigned) $ eiddPrev idd
            , map (eiddKeyIdentity . fromSigned) $ toList $ eiddOwner idd
            ]
    unless (all (fromStored sidd `isSignedBy`) required) $ do
        throwError "signature verification failed"

lookupProperty :: forall a m. Foldable m => (ExtendedIdentityData -> Maybe a) -> m (Stored (Signed ExtendedIdentityData)) -> Maybe a
lookupProperty sel topHeads = findResult propHeads
  where
    findPropHeads :: Stored (Signed ExtendedIdentityData) -> [ Stored (Signed ExtendedIdentityData) ]
    findPropHeads sobj | Just _ <- sel $ fromSigned sobj = [ sobj ]
                       | otherwise = findPropHeads =<< (eiddPrev $ fromSigned sobj)

    propHeads :: [ Stored (Signed ExtendedIdentityData) ]
    propHeads = filterAncestors $ findPropHeads =<< toList topHeads

    findResult :: [ Stored (Signed ExtendedIdentityData) ] -> Maybe a
    findResult [] = Nothing
    findResult xs = sel $ fromSigned $ minimum xs

mergeIdentity :: (MonadStorage m, MonadError e m, FromErebosError e, MonadIO m) => Identity f -> m UnifiedIdentity
mergeIdentity idt | Just idt' <- toUnifiedIdentity idt = return idt'
mergeIdentity idt@Identity {..} = do
    (owner, ownerData) <- case idOwner_ of
        Nothing -> return (Nothing, Nothing)
        Just cowner | Just owner <- toUnifiedIdentity cowner -> return (Just owner, Nothing)
                    | otherwise -> do owner <- mergeIdentity cowner
                                      return (Just owner, Just $ idData owner)

    let public = idKeyIdentity idt
    secret <- loadKey public

    unifiedBaseData <-
        case toList $ idDataF idt of
            [idata] -> return idata
            idatas -> mstore =<< sign secret =<< mstore (emptyIdentityData public)
                { iddPrev = idatas, iddOwner = ownerData }

    case filter isExtension $ toList $ idExtDataF idt of
        [] -> return Identity { idData_ = I.Identity (baseToExtended unifiedBaseData), idOwner_ = toComposedIdentity <$> owner, .. }
        extdata -> do
            unifiedExtendedData <- mstore =<< sign secret =<<
                (mstore . ExtendedIdentityData) (emptyIdentityExtension unifiedBaseData)
                    { idePrev = extdata }
            return Identity { idData_ = I.Identity unifiedExtendedData, idOwner_ = toComposedIdentity <$> owner, .. }


toUnifiedIdentity :: Identity m -> Maybe UnifiedIdentity
toUnifiedIdentity Identity {..}
    | [sdata] <- toList idData_ = Just Identity { idData_ = I.Identity sdata, .. }
    | otherwise = Nothing

toComposedIdentity :: Identity m -> ComposedIdentity
toComposedIdentity Identity {..} = Identity { idData_ = toList idData_
                                            , idOwner_ = toComposedIdentity <$> idOwner_
                                            , ..
                                            }

updateIdentity :: [Stored (Signed ExtendedIdentityData)] -> Identity m -> ComposedIdentity
updateIdentity [] orig = toComposedIdentity orig
updateIdentity updates orig@Identity {} =
    case validateExtendedIdentityF $ ourUpdates ++ idata of
         Just updated -> updated
             { idOwner_ = updateIdentity ownerUpdates <$> idOwner_ updated
             , idUpdates_ = ownerUpdates
             }
         Nothing -> toComposedIdentity orig
    where idata = toList $ idData_ orig
          idataRoots = foldl' mergeUniq [] $ map storedRoots idata
          (ourUpdates, ownerUpdates) = partitionEithers $ flip map (filterAncestors $ updates ++ idUpdates_ orig) $
              -- if an update is related to anything in idData_, use it here, otherwise push to owners
              \u -> if storedRoots u `intersectsSorted` idataRoots
                       then Left u
                       else Right u

updateOwners :: [Stored (Signed ExtendedIdentityData)] -> Identity m -> Identity m
updateOwners updates orig@Identity { idOwner_ = Just owner, idUpdates_ = cupdates } =
    orig { idOwner_ = Just $ updateIdentity updates owner, idUpdates_ = filterAncestors (updates ++ cupdates) }
updateOwners _ orig@Identity { idOwner_ = Nothing } = orig

sameIdentity :: (Foldable m, Foldable m') => Identity m -> Identity m' -> Bool
sameIdentity x y = intersectsSorted (roots x) (roots y)
  where
    roots idt = uniq $ sort $ concatMap storedRoots $ toList $ idDataF idt


unfoldOwners :: Foldable m => Identity m -> [ComposedIdentity]
unfoldOwners = unfoldr (fmap (\i -> (i, idOwner i))) . Just . toComposedIdentity

finalOwner :: Foldable m => Identity m -> ComposedIdentity
finalOwner = last . unfoldOwners

displayIdentity :: Foldable m => Identity m -> Text
displayIdentity identity = T.concat
    [ T.intercalate (T.pack " / ") $ map (fromMaybe (T.pack "<unnamed>") . idName) owners
    ]
    where owners = reverse $ unfoldOwners identity

{-# LANGUAGE UndecidableInstances #-}

module Identity (
    Identity, ComposedIdentity, UnifiedIdentity, IdentityData(..),
    idData, idDataF, idName, idOwner, idUpdates, idKeyIdentity, idKeyMessage,

    emptyIdentityData,
    createIdentity,
    validateIdentity, validateIdentityF, validateIdentityFE,
    loadIdentity, loadUnifiedIdentity,

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
import Data.Ord
import Data.Set (Set)
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T

import PubKey
import Storage
import Storage.Merge

data Identity m = Identity
    { idData_ :: m (Stored (Signed IdentityData))
    , idName_ :: Maybe Text
    , idOwner_ :: Maybe ComposedIdentity
    , idUpdates_ :: [Stored (Signed IdentityData)]
    , idKeyIdentity_ :: Stored PublicKey
    , idKeyMessage_ :: Stored PublicKey
    }

deriving instance Show (m (Stored (Signed IdentityData))) => Show (Identity m)

type ComposedIdentity = Identity []
type UnifiedIdentity = Identity I.Identity

instance Eq (m (Stored (Signed IdentityData))) => Eq (Identity m) where
    (==) = (==) `on` (idData_ &&& idUpdates_)

data IdentityData = IdentityData
    { iddPrev :: [Stored (Signed IdentityData)]
    , iddName :: Maybe Text
    , iddOwner :: Maybe (Stored (Signed IdentityData))
    , iddKeyIdentity :: Stored PublicKey
    , iddKeyMessage :: Maybe (Stored PublicKey)
    }
    deriving (Show)

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

instance Mergeable (Maybe ComposedIdentity) where
    type Component (Maybe ComposedIdentity) = Signed IdentityData
    mergeSorted = validateIdentityF
    toComponents = maybe [] idDataF

idData :: UnifiedIdentity -> Stored (Signed IdentityData)
idData = I.runIdentity . idDataF

idDataF :: Identity m -> m (Stored (Signed IdentityData))
idDataF = idData_

idName :: Identity m -> Maybe Text
idName = idName_

idOwner :: Identity m -> Maybe ComposedIdentity
idOwner = idOwner_

idUpdates :: Identity m -> [Stored (Signed IdentityData)]
idUpdates = idUpdates_

idKeyIdentity :: Identity m -> Stored PublicKey
idKeyIdentity = idKeyIdentity_

idKeyMessage :: Identity m -> Stored PublicKey
idKeyMessage = idKeyMessage_


emptyIdentityData :: Stored PublicKey -> IdentityData
emptyIdentityData key = IdentityData
    { iddName = Nothing
    , iddPrev = []
    , iddOwner = Nothing
    , iddKeyIdentity = key
    , iddKeyMessage = Nothing
    }

createIdentity :: Storage -> Maybe Text -> Maybe UnifiedIdentity -> IO UnifiedIdentity
createIdentity st name owner = do
    (secret, public) <- generateKeys st
    (_secretMsg, publicMsg) <- generateKeys st

    let signOwner idd
            | Just o <- owner = do
                Just ownerSecret <- loadKeyMb (iddKeyIdentity $ fromStored $ signedData $ fromStored $ idData o)
                signAdd ownerSecret idd
            | otherwise = return idd

    Just identity <- flip runReaderT st $ do
        return . validateIdentity =<< mstore =<< signOwner =<< sign secret =<<
            mstore (emptyIdentityData public)
                { iddName = name
                , iddOwner = idData <$> owner
                , iddKeyMessage = Just publicMsg
                }
    return identity

validateIdentity :: Stored (Signed IdentityData) -> Maybe UnifiedIdentity
validateIdentity = validateIdentityF . I.Identity

validateIdentityF :: Foldable m => m (Stored (Signed IdentityData)) -> Maybe (Identity m)
validateIdentityF = either (const Nothing) Just . runExcept . validateIdentityFE

validateIdentityFE :: Foldable m => m (Stored (Signed IdentityData)) -> Except String (Identity m)
validateIdentityFE mdata = do
    let idata = filterAncestors $ toList mdata
    when (null idata) $ throwError "null data"
    mapM_ verifySignatures $ gatherPrevious S.empty idata
    Identity
        <$> pure mdata
        <*> pure (lookupProperty iddName idata)
        <*> case lookupProperty iddOwner idata of
                 Nothing    -> return Nothing
                 Just owner -> return <$> validateIdentityFE [owner]
        <*> pure []
        <*> pure (iddKeyIdentity $ fromStored $ signedData $ fromStored $ minimum idata)
        <*> case lookupProperty iddKeyMessage idata of
                 Nothing -> throwError "no message key"
                 Just mk -> return mk

loadIdentity :: String -> LoadRec ComposedIdentity
loadIdentity name = maybe (throwError "identity validation failed") return . validateIdentityF =<< loadRefs name

loadUnifiedIdentity :: String -> LoadRec UnifiedIdentity
loadUnifiedIdentity name = maybe (throwError "identity validation failed") return . validateIdentity =<< loadRef name


gatherPrevious :: Set (Stored (Signed IdentityData)) -> [Stored (Signed IdentityData)] -> Set (Stored (Signed IdentityData))
gatherPrevious res (n:ns) | n `S.member` res = gatherPrevious res ns
                          | otherwise        = gatherPrevious (S.insert n res) $ (iddPrev $ fromStored $ signedData $ fromStored n) ++ ns
gatherPrevious res [] = res

verifySignatures :: Stored (Signed IdentityData) -> Except String ()
verifySignatures sidd = do
    let idd = fromStored $ signedData $ fromStored sidd
        required = concat
            [ [ iddKeyIdentity idd ]
            , map (iddKeyIdentity . fromStored . signedData . fromStored) $ iddPrev idd
            , map (iddKeyIdentity . fromStored . signedData . fromStored) $ toList $ iddOwner idd
            ]
    unless (all (fromStored sidd `isSignedBy`) required) $ do
        throwError "signature verification failed"

lookupProperty :: forall a. (IdentityData -> Maybe a) -> [Stored (Signed IdentityData)] -> Maybe a
lookupProperty sel topHeads = findResult filteredLayers
    where findPropHeads :: Stored (Signed IdentityData) -> [(Stored (Signed IdentityData), a)]
          findPropHeads sobj | Just x <- sel $ fromStored $ signedData $ fromStored sobj = [(sobj, x)]
                             | otherwise = findPropHeads =<< (iddPrev $ fromStored $ signedData $ fromStored sobj)

          propHeads :: [(Stored (Signed IdentityData), a)]
          propHeads = findPropHeads =<< topHeads

          historyLayers :: [Set (Stored (Signed IdentityData))]
          historyLayers = generations $ map fst propHeads

          filteredLayers :: [[(Stored (Signed IdentityData), a)]]
          filteredLayers = scanl (\cur obsolete -> filter ((`S.notMember` obsolete) . fst) cur) propHeads historyLayers

          findResult ([(_, x)] : _) = Just x
          findResult ([] : _) = Nothing
          findResult [] = Nothing
          findResult [xs] = Just $ snd $ minimumBy (comparing fst) xs
          findResult (_:rest) = findResult rest

mergeIdentity :: (Foldable f, MonadStorage m, MonadError String m, MonadIO m) => Identity f -> m UnifiedIdentity
mergeIdentity idt | Just idt' <- toUnifiedIdentity idt = return idt'
mergeIdentity idt = do
    (owner, ownerData) <- case idOwner_ idt of
        Nothing -> return (Nothing, Nothing)
        Just cowner | Just owner <- toUnifiedIdentity cowner -> return (Just owner, Nothing)
                    | otherwise -> do owner <- mergeIdentity cowner
                                      return (Just owner, Just $ idData owner)

    let public = idKeyIdentity idt
    secret <- loadKey public
    sdata <- mstore =<< sign secret =<< mstore (emptyIdentityData public)
        { iddPrev = toList $ idDataF idt, iddOwner = ownerData }
    return $ idt { idData_ = I.Identity sdata, idOwner_ = toComposedIdentity <$> owner }

toUnifiedIdentity :: Foldable m => Identity m -> Maybe UnifiedIdentity
toUnifiedIdentity idt
    | [sdata] <- toList $ idDataF idt = Just idt { idData_ = I.Identity sdata }
    | otherwise = Nothing

toComposedIdentity :: Foldable m => Identity m -> ComposedIdentity
toComposedIdentity idt = idt { idData_ = toList $ idDataF idt
                             , idOwner_ = toComposedIdentity <$> idOwner_ idt
                             }


updateIdentity :: Foldable m => [Stored (Signed IdentityData)] -> Identity m -> ComposedIdentity
updateIdentity [] orig = toComposedIdentity orig
updateIdentity updates orig =
    case validateIdentityF $ filterAncestors (ourUpdates ++ idata) of
         -- need to filter ancestors here as validateIdentityF currently stores the whole list in idData_
         Just updated -> updated
             { idOwner_ = updateIdentity ownerUpdates <$> idOwner_ updated
             , idUpdates_ = ownerUpdates
             }
         Nothing -> toComposedIdentity orig
    where idata = toList $ idData_ orig
          ilen = length idata
          (ourUpdates, ownerUpdates) = partitionEithers $ flip map (filterAncestors $ updates ++ idUpdates_ orig) $
              -- if an update is related to anything in idData_, use it here, otherwise push to owners
              \u -> if length (filterAncestors (u : idata)) < ilen + 1
                       then Left u
                       else Right u

updateOwners :: [Stored (Signed IdentityData)] -> Identity m -> Identity m
updateOwners updates orig@Identity { idOwner_ = Just owner, idUpdates_ = cupdates } =
    orig { idOwner_ = Just $ updateIdentity updates owner, idUpdates_ = filterAncestors (updates ++ cupdates) }
updateOwners _ orig@Identity { idOwner_ = Nothing } = orig

sameIdentity :: (Foldable m, Foldable m') => Identity m -> Identity m' -> Bool
sameIdentity x y = not $ S.null $ S.intersection (refset x) (refset y)
    where refset idt = foldr S.insert (ancestors $ toList $ idDataF idt) (idDataF idt)


unfoldOwners :: (Foldable m) => Identity m -> [ComposedIdentity]
unfoldOwners = unfoldr (fmap (\i -> (i, idOwner i))) . Just . toComposedIdentity

finalOwner :: (Foldable m, Applicative m) => Identity m -> ComposedIdentity
finalOwner = last . unfoldOwners

displayIdentity :: (Foldable m, Applicative m) => Identity m -> Text
displayIdentity identity = T.concat
    [ T.intercalate (T.pack " / ") $ map (fromMaybe (T.pack "<unnamed>") . idName) owners
    ]
    where owners = reverse $ unfoldOwners identity

{-# LANGUAGE UndecidableInstances #-}

module Identity (
    Identity, ComposedIdentity, UnifiedIdentity, IdentityData(..),
    idData, idDataF, idName, idOwner, idKeyIdentity, idKeyMessage,

    emptyIdentityData,
    verifyIdentity, verifyIdentityF,
    mergeIdentity, toComposedIdentity,

    finalOwner,
    displayIdentity,
) where

import Control.Monad
import qualified Control.Monad.Identity as I

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

data Identity m = Identity
    { idData_ :: m (Stored (Signed IdentityData))
    , idName_ :: Maybe Text
    , idOwner_ :: Maybe UnifiedIdentity
    , idKeyIdentity_ :: Stored PublicKey
    , idKeyMessage_ :: Stored PublicKey
    }

deriving instance Show (m (Stored (Signed IdentityData))) => Show (Identity m)

type ComposedIdentity = Identity []
type UnifiedIdentity = Identity I.Identity

instance Eq UnifiedIdentity where
    (==) = (==) `on` idData

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
        mapM_ (storeRef "PREV") $ iddPrev idt
        storeMbText "name" $ iddName idt
        storeMbRef "owner" $ iddOwner idt
        storeRef "key-id" $ iddKeyIdentity idt
        storeMbRef "key-msg" $ iddKeyMessage idt

    load' = loadRec $ IdentityData
        <$> loadRefs "PREV"
        <*> loadMbText "name"
        <*> loadMbRef "owner"
        <*> loadRef "key-id"
        <*> loadMbRef "key-msg"

idData :: UnifiedIdentity -> Stored (Signed IdentityData)
idData = I.runIdentity . idDataF

idDataF :: Identity m -> m (Stored (Signed IdentityData))
idDataF = idData_

idName :: Identity m -> Maybe Text
idName = idName_

idOwner :: Identity m -> Maybe UnifiedIdentity
idOwner = idOwner_

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

verifyIdentity :: Stored (Signed IdentityData) -> Maybe UnifiedIdentity
verifyIdentity = verifyIdentityF . I.Identity

verifyIdentityF :: Foldable m => m (Stored (Signed IdentityData)) -> Maybe (Identity m)
verifyIdentityF mdata = do
    let idata = toList mdata -- TODO: eliminate ancestors
    guard $ not $ null idata
    mapM_ verifySignatures $ gatherPrevious S.empty idata
    Identity
        <$> pure mdata
        <*> pure (lookupProperty iddName idata)
        <*> case lookupProperty iddOwner idata of
                 Nothing    -> return Nothing
                 Just owner -> Just <$> verifyIdentity owner
        <*> pure (iddKeyIdentity $ fromStored $ signedData $ fromStored $ minimum idata)
        <*> lookupProperty iddKeyMessage idata

gatherPrevious :: Set (Stored (Signed IdentityData)) -> [Stored (Signed IdentityData)] -> Set (Stored (Signed IdentityData))
gatherPrevious res (n:ns) | n `S.member` res = gatherPrevious res ns
                          | otherwise        = gatherPrevious (S.insert n res) $ (iddPrev $ fromStored $ signedData $ fromStored n) ++ ns
gatherPrevious res [] = res

verifySignatures :: Stored (Signed IdentityData) -> Maybe ()
verifySignatures sidd = do
    let idd = fromStored $ signedData $ fromStored sidd
        required = concat
            [ [ iddKeyIdentity idd ]
            , map (iddKeyIdentity . fromStored . signedData . fromStored) $ iddPrev idd
            , map (iddKeyIdentity . fromStored . signedData . fromStored) $ toList $ iddOwner idd
            ]
    guard $ all (fromStored sidd `isSignedBy`) required

lookupProperty :: forall a. (IdentityData -> Maybe a) -> [Stored (Signed IdentityData)] -> Maybe a
lookupProperty sel topHeads = findResult filteredLayers
    where findPropHeads :: Stored (Signed IdentityData) -> [(Stored (Signed IdentityData), a)]
          findPropHeads sobj | Just x <- sel $ fromStored $ signedData $ fromStored sobj = [(sobj, x)]
                             | otherwise = findPropHeads =<< (iddPrev $ fromStored $ signedData $ fromStored sobj)

          propHeads :: [(Stored (Signed IdentityData), a)]
          propHeads = findPropHeads =<< topHeads

          historyLayers :: [Set (Stored (Signed IdentityData))]
          historyLayers = flip unfoldr (map fst propHeads, S.empty) $ \(hs, cur) ->
              case filter (`S.notMember` cur) $ (iddPrev . fromStored . signedData . fromStored) =<< hs of
                   []    -> Nothing
                   added -> let next = foldr S.insert cur added
                             in Just (next, (added, next))

          filteredLayers :: [[(Stored (Signed IdentityData), a)]]
          filteredLayers = scanl (\cur obsolete -> filter ((`S.notMember` obsolete) . fst) cur) propHeads historyLayers

          findResult ([(_, x)] : _) = Just x
          findResult ([] : _) = Nothing
          findResult [] = Nothing
          findResult [xs] = Just $ snd $ minimumBy (comparing fst) xs
          findResult (_:rest) = findResult rest

mergeIdentity :: Foldable m => Identity m -> IO UnifiedIdentity
mergeIdentity idt | [sdata] <- toList $ idDataF idt = return $ idt { idData_ = I.Identity sdata }
mergeIdentity idt = do
    (sid:_) <- return $ toList $ idDataF idt
    let st = storedStorage sid
        public = idKeyIdentity idt
    Just secret <- loadKey public
    sdata <- wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
        { iddPrev = toList $ idDataF idt }
    return $ idt { idData_ = I.Identity sdata }


toComposedIdentity :: Foldable m => Identity m -> ComposedIdentity
toComposedIdentity idt = idt { idData_ = toList $ idDataF idt }


unfoldOwners :: (Foldable m, Applicative m) => Identity m -> [Identity m]
unfoldOwners cur = cur : case idOwner cur of
                              Nothing   -> []
                              Just owner@Identity { idData_ = I.Identity pid } ->
                                  unfoldOwners owner { idData_ = pure pid }

finalOwner :: (Foldable m, Applicative m) => Identity m -> Identity m
finalOwner = last . unfoldOwners

displayIdentity :: (Foldable m, Applicative m) => Identity m -> Text
displayIdentity identity = T.concat
    [ T.intercalate (T.pack " / ") $ map (fromMaybe (T.pack "<unnamed>") . idName) owners
    ]
    where owners = reverse $ unfoldOwners identity

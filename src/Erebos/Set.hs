module Erebos.Set (
    Set,

    emptySet,
    loadSet,
    storeSetAdd,
    storeSetAddComponent,

    fromSetBy,
) where

import Control.Arrow

import Data.Function
import Data.List
import Data.Map (Map)
import Data.Map qualified as M
import Data.Maybe
import Data.Ord

import Erebos.Object
import Erebos.Storable
import Erebos.Storage.Merge
import Erebos.Util

data Set a = Set [Stored (SetItem (Component a))]
    deriving (Eq)

data SetItem a = SetItem
    { siPrev :: [Stored (SetItem a)]
    , siItem :: [Stored a]
    }

instance Storable a => Storable (SetItem a) where
    store' x = storeRec $ do
        mapM_ (storeRef "PREV") $ siPrev x
        mapM_ (storeRef "item") $ siItem x

    load' = loadRec $ SetItem
        <$> loadRefs "PREV"
        <*> loadRefs "item"

instance Mergeable a => Mergeable (Set a) where
    type Component (Set a) = SetItem (Component a)
    mergeSorted = Set
    toComponents (Set items) = items


emptySet :: Set a
emptySet = Set []

loadSet :: Mergeable a => Ref -> Set a
loadSet = mergeSorted . (:[]) . wrappedLoad

storeSetAdd :: (Mergeable a, MonadStorage m) => a -> Set a -> m (Set a)
storeSetAdd x (Set prev) = Set . (: []) <$> mstore SetItem
    { siPrev = prev
    , siItem = toComponents x
    }

storeSetAddComponent :: (Mergeable a, MonadStorage m) => Stored (Component a) -> Set a -> m (Set a)
storeSetAddComponent component (Set prev) = Set . (: []) <$> mstore SetItem
    { siPrev = prev
    , siItem = [ component ]
    }


fromSetBy :: forall a. Mergeable a => (a -> a -> Ordering) -> Set a -> [a]
fromSetBy cmp (Set heads) = sortBy cmp $ map merge $ groupRelated items
  where
    -- gather all item components in the set history
    items :: [Stored (Component a)]
    items = walkAncestors (siItem . fromStored) heads

    -- map individual roots to full root set as joined in history of individual items
    rootToRootSet :: Map RefDigest [RefDigest]
    rootToRootSet = foldl' (\m rs -> foldl' (\m' r -> M.insertWith (\a b -> uniq $ sort $ a++b) r rs m') m rs) M.empty $
        map (map (refDigest . storedRef) . storedRoots) items

    -- get full root set for given item component
    storedRootSet :: Stored (Component a) -> [RefDigest]
    storedRootSet = fromJust . flip M.lookup rootToRootSet . refDigest . storedRef . head . storedRoots

    -- group components of single item, i.e. components sharing some root
    groupRelated :: [Stored (Component a)] -> [[Stored (Component a)]]
    groupRelated = map (map fst) . groupBy ((==) `on` snd) . sortBy (comparing snd) . map (id &&& storedRootSet)

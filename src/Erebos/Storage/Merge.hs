module Erebos.Storage.Merge (
    Mergeable(..),
    merge, storeMerge,

    Generation,
    showGeneration,
    compareGeneration, generationMax,
    storedGeneration,

    generations, generationsBy,
    ancestors,
    precedes,
    precedesOrEquals,
    filterAncestors,
    storedRoots,
    walkAncestors,

    findProperty,
    findPropertyFirst,

    storedDifference,
) where

import Data.Kind

import Erebos.Object
import Erebos.Storable.Internal
import Erebos.Storage.Graph


class Storable (Component a) => Mergeable a where
    type Component a :: Type
    mergeSorted :: [Stored (Component a)] -> a
    toComponents :: a -> [Stored (Component a)]

instance Mergeable [Stored Object] where
    type Component [Stored Object] = Object
    mergeSorted = id
    toComponents = id

merge :: Mergeable a => [Stored (Component a)] -> a
merge [] = error "merge: empty list"
merge xs = mergeSorted $ filterAncestors xs

storeMerge :: (Mergeable a, Storable a) => [Stored (Component a)] -> IO (Stored a)
storeMerge [] = error "merge: empty list"
storeMerge xs@(x : _) = wrappedStore (storedStorage x) $ mergeSorted $ filterAncestors xs

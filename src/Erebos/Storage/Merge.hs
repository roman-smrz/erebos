module Erebos.Storage.Merge (
    Mergeable(..),
    merge, storeMerge,

    Generation,
    showGeneration,
    compareGeneration, generationMax,
    storedGeneration,

    generations,
    ancestors,
    precedes,
    precedesOrEquals,
    filterAncestors,
    storedRoots,
    walkAncestors,

    findProperty,
    findPropertyFirst,
) where

import Control.Concurrent.MVar

import Data.ByteString.Char8 qualified as BC
import Data.HashTable.IO qualified as HT
import Data.Kind
import Data.List
import Data.Maybe
import Data.Set (Set)
import Data.Set qualified as S

import System.IO.Unsafe (unsafePerformIO)

import Erebos.Storage
import Erebos.Storage.Internal
import Erebos.Util

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
storeMerge xs@(Stored ref _ : _) = wrappedStore (refStorage ref) $ mergeSorted $ filterAncestors xs

previous :: Storable a => Stored a -> [Stored a]
previous (Stored ref _) = case load ref of
    Rec items | Just (RecRef dref) <- lookup (BC.pack "SDATA") items
              , Rec ditems <- load dref ->
                    map wrappedLoad $ catMaybes $ map (\case RecRef r -> Just r; _ -> Nothing) $
                        map snd $ filter ((`elem` [ BC.pack "SPREV", BC.pack "SBASE" ]) . fst) ditems

              | otherwise ->
                    map wrappedLoad $ catMaybes $ map (\case RecRef r -> Just r; _ -> Nothing) $
                        map snd $ filter ((`elem` [ BC.pack "PREV", BC.pack "BASE" ]) . fst) items
    _ -> []


nextGeneration :: [Generation] -> Generation
nextGeneration = foldl' helper (Generation 0)
    where helper (Generation c) (Generation n) | c <= n    = Generation (n + 1)
                                               | otherwise = Generation c

showGeneration :: Generation -> String
showGeneration (Generation x) = show x

compareGeneration :: Generation -> Generation -> Maybe Ordering
compareGeneration (Generation x) (Generation y) = Just $ compare x y

generationMax :: Storable a => [Stored a] -> Maybe (Stored a)
generationMax (x : xs) = Just $ snd $ foldl' helper (storedGeneration x, x) xs
    where helper (mg, mx) y = let yg = storedGeneration y
                               in case compareGeneration mg yg of
                                       Just LT -> (yg, y)
                                       _       -> (mg, mx)
generationMax [] = Nothing

storedGeneration :: Storable a => Stored a -> Generation
storedGeneration x =
    unsafePerformIO $ withMVar (stRefGeneration $ refStorage $ storedRef x) $ \ht -> do
        let doLookup y = HT.lookup ht (refDigest $ storedRef y) >>= \case
                Just gen -> return gen
                Nothing -> do
                    gen <- nextGeneration <$> mapM doLookup (previous y)
                    HT.insert ht (refDigest $ storedRef y) gen
                    return gen
        doLookup x


-- |Returns list of sets starting with the set of given objects and
-- intcrementally adding parents.
generations :: Storable a => [Stored a] -> [Set (Stored a)]
generations = unfoldr gen . (,S.empty)
    where gen (hs, cur) = case filter (`S.notMember` cur) hs of
              []    -> Nothing
              added -> let next = foldr S.insert cur added
                        in Just (next, (previous =<< added, next))

-- |Returns set containing all given objects and their ancestors
ancestors :: Storable a => [Stored a] -> Set (Stored a)
ancestors = last . (S.empty:) . generations

precedes :: Storable a => Stored a -> Stored a -> Bool
precedes x y = not $ x `elem` filterAncestors [x, y]

precedesOrEquals :: Storable a => Stored a -> Stored a -> Bool
precedesOrEquals x y = filterAncestors [ x, y ] == [ y ]

filterAncestors :: Storable a => [Stored a] -> [Stored a]
filterAncestors [x] = [x]
filterAncestors xs = let xs' = uniq $ sort xs
                      in helper xs' xs'
    where helper remains walk = case generationMax walk of
                                     Just x -> let px = previous x
                                                   remains' = filter (\r -> all (/=r) px) remains
                                                in helper remains' $ uniq $ sort (px ++ filter (/=x) walk)
                                     Nothing -> remains

storedRoots :: Storable a => Stored a -> [Stored a]
storedRoots x = do
    let st = refStorage $ storedRef x
    unsafePerformIO $ withMVar (stRefRoots st) $ \ht -> do
        let doLookup y = HT.lookup ht (refDigest $ storedRef y) >>= \case
                Just roots -> return roots
                Nothing -> do
                    roots <- case previous y of
                        [] -> return [refDigest $ storedRef y]
                        ps -> map (refDigest . storedRef) . filterAncestors . map (wrappedLoad @Object . Ref st) . concat <$> mapM doLookup ps
                    HT.insert ht (refDigest $ storedRef y) roots
                    return roots
        map (wrappedLoad . Ref st) <$> doLookup x

walkAncestors :: (Storable a, Monoid m) => (Stored a -> m) -> [Stored a] -> m
walkAncestors f = helper . sortBy cmp
  where
    helper (x : y : xs) | x == y = helper (x : xs)
    helper (x : xs) = f x <> helper (mergeBy cmp (sortBy cmp (previous x)) xs)
    helper [] = mempty

    cmp x y = case compareGeneration (storedGeneration x) (storedGeneration y) of
                   Just LT -> GT
                   Just GT -> LT
                   _ -> compare x y

findProperty :: forall a b. Storable a => (a -> Maybe b) -> [Stored a] -> [b]
findProperty sel = map (fromJust . sel . fromStored) . filterAncestors . (findPropHeads sel =<<)

findPropertyFirst :: forall a b. Storable a => (a -> Maybe b) -> [Stored a] -> Maybe b
findPropertyFirst sel = fmap (fromJust . sel . fromStored) . listToMaybe . filterAncestors . (findPropHeads sel =<<)

findPropHeads :: forall a b. Storable a => (a -> Maybe b) -> Stored a -> [Stored a]
findPropHeads sel sobj | Just _ <- sel $ fromStored sobj = [sobj]
                       | otherwise = findPropHeads sel =<< previous sobj

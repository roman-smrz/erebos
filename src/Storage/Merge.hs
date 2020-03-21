module Storage.Merge (
    Mergeable(..),
    merge, storeMerge,

    Generation,
    compareGeneration, generationMax,
    storedGeneration,

    generations,
    ancestors,
    precedes,
    filterAncestors,

    findProperty,
) where

import Control.Concurrent.MVar

import qualified Data.ByteString.Char8 as BC
import qualified Data.HashTable.IO as HT
import Data.List
import Data.Maybe
import Data.Set (Set)
import qualified Data.Set as S

import System.IO.Unsafe (unsafePerformIO)

import Storage
import Storage.Internal
import Util

class Storable a => Mergeable a where
    mergeSorted :: [Stored a] -> a

merge :: Mergeable a => [Stored a] -> a
merge [] = error "merge: empty list"
merge [x] = fromStored x
merge xs = mergeSorted $ filterAncestors xs

storeMerge :: Mergeable a => [Stored a] -> IO (Stored a)
storeMerge [] = error "merge: empty list"
storeMerge [x] = return x
storeMerge xs@(Stored ref _ : _) = wrappedStore (refStorage ref) $ mergeSorted $ filterAncestors xs

previous :: Storable a => Stored a -> [Stored a]
previous (Stored ref _) = case load ref of
    Rec items | Just (RecRef dref) <- lookup (BC.pack "SDATA") items
              , Rec ditems <- load dref ->
                    map wrappedLoad $ catMaybes $ map (\case RecRef r -> Just r; _ -> Nothing) $
                        map snd $ filter ((== BC.pack "SPREV") . fst) ditems

              | otherwise ->
                    map wrappedLoad $ catMaybes $ map (\case RecRef r -> Just r; _ -> Nothing) $
                        map snd $ filter ((== BC.pack "PREV") . fst) items
    _ -> []


nextGeneration :: [Generation] -> Generation
nextGeneration = foldl' helper (Generation 0)
    where helper (Generation c) (Generation n) | c <= n    = Generation (n + 1)
                                               | otherwise = Generation c

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


generations :: Storable a => [Stored a] -> [Set (Stored a)]
generations = unfoldr gen . (,S.empty)
    where gen (hs, cur) = case filter (`S.notMember` cur) $ previous =<< hs of
              []    -> Nothing
              added -> let next = foldr S.insert cur added
                        in Just (next, (added, next))

ancestors :: Storable a => [Stored a] -> Set (Stored a)
ancestors = last . (S.empty:) . generations

precedes :: Storable a => Stored a -> Stored a -> Bool
precedes x y = x `S.member` ancestors [y]

filterAncestors :: Storable a => [Stored a] -> [Stored a]
filterAncestors [x] = [x]
filterAncestors xs = uniq $ sort $ filter (`S.notMember` ancestors xs) xs


findProperty :: forall a b. Storable a => (a -> Maybe b) -> [Stored a] -> [b]
findProperty sel = map (fromJust . sel . fromStored) . filterAncestors . (findPropHeads =<<)
    where findPropHeads :: Stored a -> [Stored a]
          findPropHeads sobj | Just _ <- sel $ fromStored sobj = [sobj]
                             | otherwise = findPropHeads =<< previous sobj

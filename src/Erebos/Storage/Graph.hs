module Erebos.Storage.Graph (
    Generation,
    showGeneration,
    compareGeneration, generationMax,
    storedGeneration,

    generations, generationsBy,
    ancestors,
    precedes,
    precedesOrEquals,
    filterAncestors,
    commonAncestors,
    storedRoots,
    walkAncestors,

    findProperty,
    findPropertyFirst,

    storedDifference,

    Graph,
    graphFromTips, graphRemoveTips,
    graphSize,
    graphToList,
) where

import Control.Arrow
import Control.Concurrent.MVar

import Data.ByteString.Char8 qualified as BC
import Data.HashTable.IO qualified as HT
import Data.List
import Data.List.NonEmpty (NonEmpty)
import Data.List.NonEmpty qualified as NE
import Data.Maybe
import Data.Ord
import Data.Set (Set)
import Data.Set qualified as S

import System.IO.Unsafe (unsafePerformIO)

import Erebos.Object
import Erebos.Storable.Internal
import Erebos.Storage.Internal
import Erebos.Util


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
generations :: Storable a => [ Stored a ] -> NonEmpty (Set (Stored a))
generations = generationsBy previous

-- |Returns list of sets starting with the set of given objects and
-- intcrementally adding parents, with the first parameter being
-- a function to get all the parents of given object.
generationsBy :: Ord a => (a -> [ a ]) -> [ a ] -> NonEmpty (Set a)
generationsBy parents xs = NE.unfoldr gen ( xs, S.fromList xs )
  where
    gen ( hs, cur ) = ( cur, ) $
        case filter (`S.notMember` cur) (parents =<< hs) of
            []    -> Nothing
            added -> let next = foldr S.insert cur added
                      in Just ( added, next )


type StoredTips a = [ Stored a ]

-- |Returns set containing all given objects and their ancestors
ancestors :: Storable a => [Stored a] -> Set (Stored a)
ancestors = NE.last . generations

precedes :: Storable a => Stored a -> Stored a -> Bool
precedes x y = not $ x `elem` filterAncestors [x, y]

precedesOrEquals :: Storable a => Stored a -> Stored a -> Bool
precedesOrEquals x y = filterAncestors [ x, y ] == [ y ]

filterAncestors :: Storable a => [ Stored a ] -> StoredTips a
filterAncestors [ x ] = [ x ]
filterAncestors xs = let xs' = uniq $ sort xs
                      in helper xs' xs'
    where helper remains walk = case generationMax walk of
                                     Just x -> let px = previous x
                                                   remains' = filter (\r -> all (/=r) px) remains
                                                in helper remains' $ uniq $ sort (px ++ filter (/=x) walk)
                                     Nothing -> remains

commonAncestors :: Storable a => [ Stored a ] -> [ Stored a ] -> StoredTips a
commonAncestors [] _ = []
commonAncestors _ [] = []
commonAncestors oxs oys = sort $ gom oxs' oys'
  where
    maximumGen = maximumBy (comparing (\(Generation n) ->  n))
    oxs' = map (storedGeneration &&& id) oxs
    oys' = map (storedGeneration &&& id) oys

    gom [] _ = []
    gom _ [] = []
    gom xs ys = go (maximumGen (map fst xs ++ map fst ys)) xs ys

    go g xs ys =
        let ( cxs, nxs ) = partition ((g ==) . fst) xs
            ( cys, nys ) = partition ((g ==) . fst) ys
            ( common, ( cxs', cys' ) ) = takeCommon (uniq $ sort $ map snd cxs) (uniq $ sort $ map snd cys)
            pxs = map (storedGeneration &&& id) $ concatMap previous cxs'
            pys = map (storedGeneration &&& id) $ concatMap previous cys'
         in case ( pxs, pys ) of
                ( [], [] ) -> common ++ gom nxs nys
                ( _ , _  ) -> common ++ go (maximumGen (map fst pxs ++ map fst pys)) (pxs ++ nxs) (pys ++ nys)

    takeCommon (x : xs) (y : ys)
        | x < y = second (first  (x :)) $ takeCommon xs (y : ys)
        | y < x = second (second (y :)) $ takeCommon (x : xs) ys
        | otherwise = first (x :) $ takeCommon xs ys
    takeCommon [] ys = ( [], ( [], ys ))
    takeCommon xs [] = ( [], ( xs, [] ))


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


-- | Compute symmetrict difference between two stored histories. In other
-- words, return all 'Stored a' objects reachable (via 'previous') from first
-- given set, but not from the second; and vice versa.
storedDifference :: Storable a => [ Stored a ] -> [ Stored a ] -> [ Stored a ]
storedDifference xs' ys' =
    let xs = filterAncestors xs'
        ys = filterAncestors ys'

        filteredPrevious blocked zs = filterAncestors (previous zs ++ blocked) `diffSorted` blocked
        xg = S.toAscList $ NE.last $ generationsBy (filteredPrevious ys) $ filterAncestors (xs ++ ys) `diffSorted` ys
        yg = S.toAscList $ NE.last $ generationsBy (filteredPrevious xs) $ filterAncestors (ys ++ xs) `diffSorted` xs

     in xg `mergeUniq` yg


data Graph a = Graph
    { graphHead :: StoredTips a
    , graphTail :: StoredTips a
    }

graphFromTips :: StoredTips a -> Graph a
graphFromTips h = Graph h []

graphRemoveTips :: Storable a => StoredTips a -> Graph a -> Graph a
graphRemoveTips remove g =
    let gheads = filter (\h -> not $ any (h `precedesOrEquals`) remove) (graphHead g)
        gtails = commonAncestors gheads $ graphTail g ++ remove
     in Graph { graphHead = gheads, graphTail = gtails }

graphSize :: Storable a => Graph a -> Int
graphSize = length . graphToList (\_ _ -> EQ)

graphToList :: Storable a => (Stored a -> Stored a -> Ordering) -> Graph a -> [ Stored a ]
graphToList cmp Graph {..} = go S.empty graphHead
  where
    go _ [] = []
    go used (x : xs)
        | ( x', xs' ) <- selectMax x xs
        = x' : go (S.insert x used) (xs' ++ filter (\(p :: Stored a) -> not $ p `S.member` used || any (p `precedesOrEquals`) graphTail) (previous x))

    cmp' x y = case cmp x y of EQ -> compare x y
                               o  -> o

    selectMax y (x : xs)
        = case cmp' y x of
              LT -> (y :) <$> selectMax x xs
              EQ -> selectMax y xs
              GT -> (x :) <$> selectMax y xs
    selectMax y [] = ( y, [] )

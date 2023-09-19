module Util where

uniq :: Eq a => [a] -> [a]
uniq (x:y:xs) | x == y    = uniq (x:xs)
              | otherwise = x : uniq (y:xs)
uniq xs = xs

mergeBy :: (a -> a -> Ordering) -> [a] -> [a] -> [a]
mergeBy cmp (x : xs) (y : ys) = case cmp x y of
                                     LT -> x : mergeBy cmp xs (y : ys)
                                     EQ -> x : y : mergeBy cmp xs ys
                                     GT -> y : mergeBy cmp (x : xs) ys
mergeBy _ xs [] = xs
mergeBy _ [] ys = ys

mergeUniqBy :: (a -> a -> Ordering) -> [a] -> [a] -> [a]
mergeUniqBy cmp (x : xs) (y : ys) = case cmp x y of
                                         LT -> x : mergeBy cmp xs (y : ys)
                                         EQ -> x : mergeBy cmp xs ys
                                         GT -> y : mergeBy cmp (x : xs) ys
mergeUniqBy _ xs [] = xs
mergeUniqBy _ [] ys = ys

mergeUniq :: Ord a => [a] -> [a] -> [a]
mergeUniq = mergeUniqBy compare

diffSorted :: Ord a => [a] -> [a] -> [a]
diffSorted (x:xs) (y:ys) | x < y     = x : diffSorted xs (y:ys)
                         | x > y     = diffSorted (x:xs) ys
                         | otherwise = diffSorted xs (y:ys)
diffSorted xs _ = xs

intersectsSorted :: Ord a => [a] -> [a] -> Bool
intersectsSorted (x:xs) (y:ys) | x < y     = intersectsSorted xs (y:ys)
                               | x > y     = intersectsSorted (x:xs) ys
                               | otherwise = True
intersectsSorted _ _ = False

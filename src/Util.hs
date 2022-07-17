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

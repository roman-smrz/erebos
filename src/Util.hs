module Util where

uniq :: Eq a => [a] -> [a]
uniq (x:y:xs) | x == y    = uniq (x:xs)
              | otherwise = x : uniq (y:xs)
uniq xs = xs

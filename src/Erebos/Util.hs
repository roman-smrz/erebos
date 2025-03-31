module Erebos.Util where

import Control.Monad

import Data.ByteArray (ByteArray, ByteArrayAccess)
import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as B
import Data.Char


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


showHex :: ByteArrayAccess ba => ba -> ByteString
showHex = B.concat . map showHexByte . BA.unpack
    where showHexChar x | x < 10    = x + o '0'
                        | otherwise = x + o 'a' - 10
          showHexByte x = B.pack [ showHexChar (x `div` 16), showHexChar (x `mod` 16) ]
          o = fromIntegral . ord

readHex :: ByteArray ba => ByteString -> Maybe ba
readHex = return . BA.concat <=< readHex'
    where readHex' bs | B.null bs = Just []
          readHex' bs = do (bx, bs') <- B.uncons bs
                           (by, bs'') <- B.uncons bs'
                           x <- hexDigit bx
                           y <- hexDigit by
                           (B.singleton (x * 16 + y) :) <$> readHex' bs''
          hexDigit x | x >= o '0' && x <= o '9' = Just $ x - o '0'
                     | x >= o 'a' && x <= o 'z' = Just $ x - o 'a' + 10
                     | otherwise                = Nothing
          o = fromIntegral . ord

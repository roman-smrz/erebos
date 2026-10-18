module Crypto.Random (
    MonadRandom(..),
) where

import Crypto.Random.Entropy

import Data.ByteArray

class Monad m => MonadRandom m where
    getRandomBytes :: ByteArray byteArray => Int -> m byteArray

instance MonadRandom IO where
    getRandomBytes = fmap convert . getEntropy

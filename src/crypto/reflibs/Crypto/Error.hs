module Crypto.Error (
    CryptoError(..),
    CryptoFailable(..),

    throwCryptoErrorIO,
    maybeCryptoError,
) where

import Control.Exception
import Control.Monad


data CryptoError = CryptoError
    deriving (Show)

instance Exception CryptoError

data CryptoFailable a
    = CryptoPassed a
    | CryptoFailed CryptoError
    deriving (Show)

instance Functor CryptoFailable where
    fmap f (CryptoPassed x) = CryptoPassed (f x)
    fmap _ (CryptoFailed e) = CryptoFailed e

instance Applicative CryptoFailable where
    pure = CryptoPassed
    (<*>) = ap

instance Monad CryptoFailable where
    CryptoPassed x >>= f = f x
    CryptoFailed e >>= _ = CryptoFailed e

throwCryptoErrorIO :: CryptoFailable a -> IO a
throwCryptoErrorIO (CryptoPassed r) = return r
throwCryptoErrorIO (CryptoFailed e) = throwIO e

maybeCryptoError :: CryptoFailable a -> Maybe a
maybeCryptoError (CryptoPassed x) = Just x
maybeCryptoError (CryptoFailed _) = Nothing

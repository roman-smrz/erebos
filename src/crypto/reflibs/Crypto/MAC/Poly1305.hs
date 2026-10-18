module Crypto.MAC.Poly1305 (
    Auth(..),
    authTag,
) where

import Crypto.Error

import Data.ByteArray as BA
import Data.ByteString (ByteString)


newtype Auth = Auth ByteString
    deriving (Eq, ByteArrayAccess)

authTag :: ByteArrayAccess b => b -> CryptoFailable Auth
authTag ba
    | BA.length bs /= 16 = CryptoFailed CryptoError
    | otherwise = CryptoPassed (Auth bs)
  where
    bs = convert ba

module Crypto.PubKey.Ed25519 (
    EdSecretKey,
    EdPublicKey,
    EdSignature,

    edSecretKey,
    edPublicKey,
    edSignature,
    edToPublic,
    edGenerateSecretKey,
    edSign,
    edVerify,
) where

import Crypto.Error
import Crypto.Random
import Crypto.Sign.Ed25519 qualified as Ed

import Data.ByteArray as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS


newtype EdSecretKey = EdSecretKey { fromSecretKey :: Ed.SecretKey }
    deriving (Eq)

newtype EdPublicKey = EdPublicKey { fromPublicKey :: Ed.PublicKey }
    deriving (Eq)

newtype EdSignature = EdSignature { fromSignature :: Ed.Signature }
    deriving (Eq)

instance Show EdSecretKey where show = show . fromSecretKey
instance Show EdPublicKey where show = show . fromPublicKey
instance Show EdSignature where show = show . fromSignature

instance ByteArrayAccess EdSecretKey where
    length = BS.length . Ed.openSecretKey . fromSecretKey
    withByteArray = withByteArray . Ed.openSecretKey . fromSecretKey

instance ByteArrayAccess EdPublicKey where
    length = BS.length . Ed.openPublicKey . fromPublicKey
    withByteArray = withByteArray . Ed.openPublicKey . fromPublicKey

instance ByteArrayAccess EdSignature where
    length = BS.length . Ed.unSignature . fromSignature
    withByteArray = withByteArray . Ed.unSignature . fromSignature

signatureSize :: Int
signatureSize = 64

edSignature :: ByteString -> CryptoFailable EdSignature
edSignature bs
    | BS.length bs == signatureSize = CryptoPassed (EdSignature (Ed.Signature bs))
    | otherwise = CryptoFailed CryptoError

edPublicKey :: ByteString -> CryptoFailable EdPublicKey
edPublicKey = maybe (CryptoFailed CryptoError) (CryptoPassed . EdPublicKey) . Ed.makePublicKey . convert

edSecretKey :: ByteArrayAccess ba => ba -> CryptoFailable EdSecretKey
edSecretKey = maybe (CryptoFailed CryptoError) (CryptoPassed . EdSecretKey) . Ed.makeSecretKey . convert

edToPublic :: EdSecretKey -> EdPublicKey
edToPublic = EdPublicKey . Ed.secretToPublicKey . fromSecretKey

edSign :: ByteArrayAccess ba => EdSecretKey -> EdPublicKey -> ba -> EdSignature
edSign (EdSecretKey key) _ = EdSignature . Ed.dsign key . convert

edVerify :: ByteArrayAccess ba => EdPublicKey -> ba -> EdSignature -> Bool
edVerify (EdPublicKey pub) msg (EdSignature sig) = Ed.dverify pub (convert msg) sig

edGenerateSecretKey :: IO EdSecretKey
edGenerateSecretKey = maybe (error "createKeypairFromSeed_ failed") (return . EdSecretKey . snd) . Ed.createKeypairFromSeed_ =<< getRandomBytes 32

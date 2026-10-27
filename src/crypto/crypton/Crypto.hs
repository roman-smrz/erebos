{-# OPTIONS_GHC -fno-warn-orphans #-}

module Crypto (
    getEntropy,
    getRandomBytes,

    Digest, Blake2b_256,
    hashInit, hashUpdates, hashFinalize,
    hashDigestSize,
    digestFromByteString,

    AeadState,
    Nonce, nonce8, nonce12,
    Auth(..), authTag,
    aeadInitialize, aeadAppendAAD,
    aeadFinalize, aeadFinalizeAAD,
    encrypt, decrypt,

    EdPublicKey, EdSecretKey, EdSignature,
    edPublicKey, edSecretKey, edGenerateSecretKey, edToPublic,
    edSignature, edSign, edVerify,
    CxPublicKey, CxSecretKey, DhSecret,
    cxPublicKey, cxSecretKey, cxGenerateSecretKey, cxToPublic,
    diffieHellman,

    CryptoFailable(..),
    maybeCryptoError,
    throwCryptoErrorIO,
) where

import Control.Monad

import Crypto.Cipher.ChaChaPoly1305 qualified as C
import Crypto.Error
import Crypto.Hash
import Crypto.MAC.Poly1305 (Auth(..), authTag)
import Crypto.PubKey.Curve25519 qualified as Cx
import Crypto.PubKey.Ed25519 qualified as Ed
import Crypto.Random
import Crypto.Random.Entropy

import Data.ByteArray (Bytes, ByteArray)
import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)

import ConvertByteString


instance ToByteString (Digest a) where
    toByteString = BA.convert
    withByteArray = BA.withByteArray


type AeadState = C.State

type Nonce = C.Nonce

nonce8 :: Bytes -> Bytes -> CryptoFailable Nonce
nonce8 = C.nonce8

nonce12 :: Bytes -> CryptoFailable Nonce
nonce12 = C.nonce12

aeadInitialize :: ScrubbedBytes -> Nonce -> CryptoFailable AeadState
aeadInitialize = C.initialize

aeadAppendAAD :: ByteString -> AeadState -> AeadState
aeadAppendAAD = C.appendAAD

aeadFinalizeAAD :: AeadState -> AeadState
aeadFinalizeAAD = C.finalizeAAD

encrypt :: ByteArray ba => ba -> AeadState -> ( ba, AeadState )
encrypt = C.encrypt

decrypt :: ByteArray ba => ( ba, Auth ) -> AeadState -> Maybe ba
decrypt ( ctext, auth ) st = do
    let ( plain, st' ) = C.decrypt ctext st
    guard (C.finalize st' `BA.constEq` auth)
    return plain

aeadFinalize :: AeadState -> Auth
aeadFinalize = C.finalize


type EdPublicKey = Ed.PublicKey
type EdSecretKey = Ed.SecretKey
type EdSignature = Ed.Signature

instance ToByteString EdSecretKey where
    toByteString = BA.convert
    withByteArray = BA.withByteArray

edPublicKey :: ByteString -> CryptoFailable EdPublicKey
edPublicKey = Ed.publicKey

edSecretKey :: ScrubbedBytes -> CryptoFailable EdSecretKey
edSecretKey = Ed.secretKey

edGenerateSecretKey :: IO EdSecretKey
edGenerateSecretKey = Ed.generateSecretKey

edToPublic :: EdSecretKey -> EdPublicKey
edToPublic = Ed.toPublic

edSignature :: ByteString -> CryptoFailable EdSignature
edSignature = Ed.signature

edSign :: ToByteString ba => EdSecretKey -> EdPublicKey -> ba -> EdSignature
edSign skey pkey content = Ed.sign skey pkey (toByteString content)

edVerify :: ToByteString ba => EdPublicKey -> ba -> EdSignature -> Bool
edVerify pkey content sig = Ed.verify pkey (toByteString content) sig

type CxPublicKey = Cx.PublicKey
type CxSecretKey = Cx.SecretKey
type DhSecret = Cx.DhSecret

instance ToByteString CxSecretKey where
    toByteString = BA.convert
    withByteArray = BA.withByteArray

instance ToByteString DhSecret where
    toByteString = BA.convert
    withByteArray = BA.withByteArray

cxPublicKey :: ByteString -> CryptoFailable CxPublicKey
cxPublicKey = Cx.publicKey

cxSecretKey :: ScrubbedBytes -> CryptoFailable CxSecretKey
cxSecretKey = Cx.secretKey

cxGenerateSecretKey :: IO CxSecretKey
cxGenerateSecretKey = Cx.generateSecretKey

cxToPublic :: CxSecretKey -> CxPublicKey
cxToPublic = Cx.toPublic

diffieHellman :: CxPublicKey -> CxSecretKey -> DhSecret
diffieHellman = Cx.dh

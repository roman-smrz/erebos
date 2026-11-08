module Crypto.Cipher.ChaChaPoly1305 (
    AeadState, Nonce, Auth(..),
    authTag,
    nonce8, nonce12,
    aeadInitialize,
    aeadAppendAAD, aeadFinalizeAAD,
    encrypt, decrypt,
    aeadFinalize,
) where

import Crypto.AEAD.ChaCha20Poly1305 qualified as P
import Crypto.Error
import Crypto.MAC.Poly1305

import Data.ByteArray as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as B


data AeadState = AeadState
    { stKey :: ByteString
    , stNonce :: Nonce
    , stAAD :: [ ByteString ]
    , stAuth :: Maybe Auth
    }

newtype Nonce = Nonce ByteString
    deriving (Eq, ByteArrayAccess)

nonce8 :: ByteString -> ByteString -> CryptoFailable Nonce
nonce8 c iv
    | B.length c /= 4 = CryptoFailed CryptoError
    | B.length iv /= 8 = CryptoFailed CryptoError
    | otherwise = CryptoPassed $ Nonce $ B.concat [ c, iv ]

nonce12 :: ByteString -> CryptoFailable Nonce
nonce12 iv
    | B.length iv /= 12 = CryptoFailed CryptoError
    | otherwise = CryptoPassed $ Nonce iv

aeadInitialize :: ScrubbedBytes -> Nonce -> CryptoFailable AeadState
aeadInitialize key nonce
    | BA.length key /= 32 = CryptoFailed CryptoError
    | otherwise = CryptoPassed AeadState
        { stKey = BA.convert key
        , stNonce = nonce
        , stAAD = []
        , stAuth = Nothing
        }

aeadAppendAAD :: ByteString -> AeadState -> AeadState
aeadAppendAAD aad st = st { stAAD = stAAD st ++ [ aad ] }

aeadFinalizeAAD :: AeadState -> AeadState
aeadFinalizeAAD = id

encrypt :: ByteString -> AeadState -> ( ByteString, AeadState )
encrypt ptext st =
    let Nonce nonce = stNonce st
        ( ctext, mac ) = P.encrypt (B.concat $ stAAD st) (stKey st) nonce ptext
     in ( ctext, st { stAuth = Just $ Auth mac } )

decrypt :: ( ByteString, Auth ) -> AeadState -> Maybe ByteString
decrypt ( ctext, auth ) st = do
    let Nonce nonce = stNonce st
        Auth mac = auth
    P.decrypt (B.concat $ stAAD st) (stKey st) nonce ( ctext, mac )

aeadFinalize :: AeadState -> Auth
aeadFinalize st = case stAuth st of
    Just auth -> auth
    Nothing -> aeadFinalize $ snd $ encrypt B.empty st

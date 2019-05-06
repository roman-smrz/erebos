module PubKey (
    PublicKey, SecretKey,
    Signature(sigKey), Signed(..),
    generateKeys,
    sign, signAdd,
) where

import Control.Monad
import Control.Monad.Except

import Crypto.Error
import qualified Crypto.PubKey.Ed25519 as ED

import Data.ByteString (ByteString)
import qualified Data.Text as T

import Storage

data PublicKey = PublicKey ED.PublicKey
    deriving (Show)

data SecretKey = SecretKey ED.SecretKey (Stored PublicKey)

data Signature = Signature
    { sigKey :: Stored PublicKey
    , sigSignature :: ED.Signature
    }
    deriving (Show)

data Signed a = Signed
    { signedData :: Stored a
    , signedSignature :: [Stored Signature]
    }
    deriving (Show)

instance Storable PublicKey where
    store' (PublicKey pk) = storeRec $ do
        storeText "type" $ T.pack "ed25519"
        storeBinary "pubkey" pk

    load' = loadRec $ do
        ktype <- loadText "type"
        guard $ ktype == "ed25519"
        maybe (throwError "Public key decoding failed") (return . PublicKey) .
            maybeCryptoError . (ED.publicKey :: ByteString -> CryptoFailable ED.PublicKey) =<<
                loadBinary "pubkey"

instance Storable Signature where
    store' sig = storeRec $ do
        storeRef "key" $ sigKey sig
        storeBinary "sig" $ sigSignature sig

    load' = loadRec $ Signature
        <$> loadRef "key"
        <*> loadSignature "sig"
        where loadSignature = maybe (throwError "Signature decoding failed") return .
                  maybeCryptoError . (ED.signature :: ByteString -> CryptoFailable ED.Signature) <=< loadBinary

instance Storable a => Storable (Signed a) where
    store' sig = storeRec $ do
        storeRef "data" $ signedData sig
        mapM_ (storeRef "sig") $ signedSignature sig

    load' = loadRec $ Signed
        <$> loadRef "data"
        <*> loadRefs "sig"


generateKeys :: Storage -> IO (SecretKey, Stored PublicKey)
generateKeys st = do
    secret <- ED.generateSecretKey
    public <- wrappedStore st $ PublicKey $ ED.toPublic secret
    return (SecretKey secret public, public)

sign :: SecretKey -> Stored a -> IO (Signed a)
sign secret val = signAdd secret $ Signed val []

signAdd :: SecretKey -> Signed a -> IO (Signed a)
signAdd (SecretKey secret spublic) (Signed val sigs) = do
    let PublicKey public = fromStored spublic
        sig = ED.sign secret public $ storedRef val
    ssig <- wrappedStore (storedStorage val) $ Signature spublic sig
    return $ Signed val (ssig : sigs)

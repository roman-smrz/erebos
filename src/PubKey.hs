module PubKey (
    PublicKey, SecretKey,
    KeyPair(generateKeys), loadKey,
    Signature(sigKey), Signed, signedData, signedSignature,
    sign, signAdd,
) where

import Control.Monad
import Control.Monad.Except

import Crypto.Error
import qualified Crypto.PubKey.Ed25519 as ED

import Data.ByteArray
import Data.ByteString (ByteString)
import qualified Data.Text as T

import Storage
import Storage.Key

data PublicKey = PublicKey ED.PublicKey
    deriving (Show)

data SecretKey = SecretKey ED.SecretKey (Stored PublicKey)

data Signature = Signature
    { sigKey :: Stored PublicKey
    , sigSignature :: ED.Signature
    }
    deriving (Show)

data Signed a = Signed
    { signedData_ :: Stored a
    , signedSignature_ :: [Stored Signature]
    }
    deriving (Show)

signedData :: Signed a -> Stored a
signedData = signedData_

signedSignature :: Signed a -> [Stored Signature]
signedSignature = signedSignature_

instance KeyPair SecretKey PublicKey where
    keyGetPublic (SecretKey _ pub) = pub
    keyGetData (SecretKey sec _) = convert sec
    keyFromData kdata spub = SecretKey <$> maybeCryptoError (ED.secretKey kdata) <*> pure spub
    generateKeys st = do
        secret <- ED.generateSecretKey
        public <- wrappedStore st $ PublicKey $ ED.toPublic secret
        let pair = SecretKey secret public
        storeKey pair
        return (pair, public)

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

    load' = loadRec $ do
        sdata <- loadRef "data"
        sigs <- loadRefs "sig"
        forM_ sigs $ \sig -> do
            let PublicKey pubkey = fromStored $ sigKey $ fromStored sig
            when (not $ ED.verify pubkey (storedRef sdata) $ sigSignature $ fromStored sig) $
                throwError "signature verification failed"
        return $ Signed sdata sigs

sign :: SecretKey -> Stored a -> IO (Signed a)
sign secret val = signAdd secret $ Signed val []

signAdd :: SecretKey -> Signed a -> IO (Signed a)
signAdd (SecretKey secret spublic) (Signed val sigs) = do
    let PublicKey public = fromStored spublic
        sig = ED.sign secret public $ storedRef val
    ssig <- wrappedStore (storedStorage val) $ Signature spublic sig
    return $ Signed val (ssig : sigs)

module Erebos.PubKey (
    PublicKey, SecretKey,
    KeyPair(generateKeys), loadKey, loadKeyMb,
    Signature(sigKey), Signed, signedData, signedSignature,
    sign, signAdd, isSignedBy,
    fromSigned,
    unsafeMapSigned,

    PublicKexKey, SecretKexKey,
    dhSecret,
) where

import Control.Monad
import Control.Monad.Except

import Crypto.Error
import qualified Crypto.PubKey.Ed25519 as ED
import qualified Crypto.PubKey.Curve25519 as CX

import Data.ByteArray
import Data.ByteString (ByteString)
import qualified Data.Text as T

import Erebos.Storage
import Erebos.Storage.Key

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
    keyFromData kdata spub = do
        skey <- maybeCryptoError $ ED.secretKey kdata
        let PublicKey pkey = fromStored spub
        guard $ ED.toPublic skey == pkey
        return $ SecretKey skey spub
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
        storeRef "SDATA" $ signedData sig
        mapM_ (storeRef "sig") $ signedSignature sig

    load' = loadRec $ do
        sdata <- loadRef "SDATA"
        sigs <- loadRefs "sig"
        forM_ sigs $ \sig -> do
            let PublicKey pubkey = fromStored $ sigKey $ fromStored sig
            when (not $ ED.verify pubkey (storedRef sdata) $ sigSignature $ fromStored sig) $
                throwError "signature verification failed"
        return $ Signed sdata sigs

sign :: MonadStorage m => SecretKey -> Stored a -> m (Signed a)
sign secret val = signAdd secret $ Signed val []

signAdd :: MonadStorage m => SecretKey -> Signed a -> m (Signed a)
signAdd (SecretKey secret spublic) (Signed val sigs) = do
    let PublicKey public = fromStored spublic
        sig = ED.sign secret public $ storedRef val
    ssig <- mstore $ Signature spublic sig
    return $ Signed val (ssig : sigs)

isSignedBy :: Signed a -> Stored PublicKey -> Bool
isSignedBy sig key = key `elem` map (sigKey . fromStored) (signedSignature sig)

fromSigned :: Stored (Signed a) -> a
fromSigned = fromStored . signedData . fromStored

-- |Passed function needs to preserve the object representation to be safe
unsafeMapSigned :: (a -> b) -> Signed a -> Signed b
unsafeMapSigned f signed = signed { signedData_ = unsafeMapStored f (signedData_ signed) }


data PublicKexKey = PublicKexKey CX.PublicKey
    deriving (Show)

data SecretKexKey = SecretKexKey CX.SecretKey (Stored PublicKexKey)

instance KeyPair SecretKexKey PublicKexKey where
    keyGetPublic (SecretKexKey _ pub) = pub
    keyGetData (SecretKexKey sec _) = convert sec
    keyFromData kdata spub = do
        skey <- maybeCryptoError $ CX.secretKey kdata
        let PublicKexKey pkey = fromStored spub
        guard $ CX.toPublic skey == pkey
        return $ SecretKexKey skey spub
    generateKeys st = do
        secret <- CX.generateSecretKey
        public <- wrappedStore st $ PublicKexKey $ CX.toPublic secret
        let pair = SecretKexKey secret public
        storeKey pair
        return (pair, public)

instance Storable PublicKexKey where
    store' (PublicKexKey pk) = storeRec $ do
        storeText "type" $ T.pack "x25519"
        storeBinary "pubkey" pk

    load' = loadRec $ do
        ktype <- loadText "type"
        guard $ ktype == "x25519"
        maybe (throwError "public key decoding failed") (return . PublicKexKey) .
            maybeCryptoError . (CX.publicKey :: ScrubbedBytes -> CryptoFailable CX.PublicKey) =<<
                loadBinary "pubkey"

dhSecret :: SecretKexKey -> PublicKexKey -> ScrubbedBytes
dhSecret (SecretKexKey secret _) (PublicKexKey public) = convert $ CX.dh public secret

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

import Data.ByteArray
import Data.Text qualified as T

import Crypto
import Erebos.Storable
import Erebos.Storage.Key

data PublicKey = PublicKey EdPublicKey
    deriving (Show)

data SecretKey = SecretKey EdSecretKey (Stored PublicKey)

data Signature = Signature
    { sigKey :: Stored PublicKey
    , sigSignature :: EdSignature
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
        skey <- maybeCryptoError $ edSecretKey kdata
        let PublicKey pkey = fromStored spub
        guard $ edToPublic skey == pkey
        return $ SecretKey skey spub
    generateKeys st = do
        secret <- edGenerateSecretKey
        public <- wrappedStore st $ PublicKey $ edToPublic secret
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
        maybe (throwOtherError "public key decoding failed") (return . PublicKey) .
            maybeCryptoError . edPublicKey =<< loadBinary "pubkey"

instance Storable Signature where
    store' sig = storeRec $ do
        storeRef "key" $ sigKey sig
        storeBinary "sig" $ sigSignature sig

    load' = loadRec $ Signature
        <$> loadRef "key"
        <*> loadSignature "sig"
        where loadSignature = maybe (throwOtherError "signature decoding failed") return .
                  maybeCryptoError . edSignature <=< loadBinary

instance Storable a => Storable (Signed a) where
    store' sig = storeRec $ do
        storeRef "SDATA" $ signedData sig
        mapM_ (storeRef "sig") $ signedSignature sig

    load' = loadRec $ do
        sdata <- loadRef "SDATA"
        sigs <- loadRefs "sig"
        forM_ sigs $ \sig -> do
            let PublicKey pubkey = fromStored $ sigKey $ fromStored sig
            when (not $ edVerify pubkey (storedRef sdata) $ sigSignature $ fromStored sig) $
                throwOtherError "signature verification failed"
        return $ Signed sdata sigs

sign :: MonadStorage m => SecretKey -> Stored a -> m (Signed a)
sign secret val = signAdd secret $ Signed val []

signAdd :: MonadStorage m => SecretKey -> Signed a -> m (Signed a)
signAdd (SecretKey secret spublic) (Signed val sigs) = do
    let PublicKey public = fromStored spublic
        sig = edSign secret public $ storedRef val
    ssig <- mstore $ Signature spublic sig
    return $ Signed val (ssig : sigs)

isSignedBy :: Signed a -> Stored PublicKey -> Bool
isSignedBy sig key = key `elem` Prelude.map (sigKey . fromStored) (signedSignature sig)

fromSigned :: Stored (Signed a) -> a
fromSigned = fromStored . signedData . fromStored

-- |Passed function needs to preserve the object representation to be safe
unsafeMapSigned :: (a -> b) -> Signed a -> Signed b
unsafeMapSigned f signed = signed { signedData_ = unsafeMapStored f (signedData_ signed) }


data PublicKexKey = PublicKexKey CxPublicKey
    deriving (Show)

data SecretKexKey = SecretKexKey CxSecretKey (Stored PublicKexKey)

instance KeyPair SecretKexKey PublicKexKey where
    keyGetPublic (SecretKexKey _ pub) = pub
    keyGetData (SecretKexKey sec _) = convert sec
    keyFromData kdata spub = do
        skey <- maybeCryptoError $ cxSecretKey kdata
        let PublicKexKey pkey = fromStored spub
        guard $ cxToPublic skey == pkey
        return $ SecretKexKey skey spub
    generateKeys st = do
        secret <- cxGenerateSecretKey
        public <- wrappedStore st $ PublicKexKey $ cxToPublic secret
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
        maybe (throwOtherError "public key decoding failed") (return . PublicKexKey) .
            maybeCryptoError . cxPublicKey =<< loadBinary "pubkey"

dhSecret :: SecretKexKey -> PublicKexKey -> ScrubbedBytes
dhSecret (SecretKexKey secret _) (PublicKexKey public) = convert $ diffieHellman public secret

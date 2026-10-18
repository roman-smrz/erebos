module Crypto.PubKey.Curve25519 (
    CxSecretKey,
    CxPublicKey,
    DhSecret,

    cxSecretKey,
    cxPublicKey,

    cxGenerateSecretKey,
    cxToPublic,
    diffieHellman,
) where

import Control.Monad

import Crypto.Error
import Crypto.Random

import Data.Bits
import Data.ByteArray
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Unsafe qualified as BS
import Data.Word

import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr
import Foreign.Storable

import System.IO.Unsafe


newtype CxSecretKey = CxSecretKey ByteString
    deriving (Eq, ByteArrayAccess)

newtype CxPublicKey = CxPublicKey ByteString
    deriving (Eq, Show, ByteArrayAccess)

newtype DhSecret = DhSecret ByteString
    deriving (Eq, Show, ByteArrayAccess)

cxSecretKey :: ByteArrayAccess bs => bs -> CryptoFailable CxSecretKey
cxSecretKey ba
    | BS.length bs == 32 = CryptoPassed $ CxSecretKey bs
    | otherwise = CryptoFailed CryptoError
  where
    bs = convert ba

cxPublicKey :: ByteString -> CryptoFailable CxPublicKey
cxPublicKey bs
    | BS.length bs == 32 = CryptoPassed $ CxPublicKey bs
    | otherwise = CryptoFailed CryptoError

cxGenerateSecretKey :: IO CxSecretKey
cxGenerateSecretKey = do
    bs <- getRandomBytes 32
    BS.unsafeUseAsCString bs $ \ptr -> do
        pokeElemOff ptr 0 . (.&. 248) =<< peekElemOff ptr 0
        pokeElemOff ptr 31 . (.|. 64) . (.&. 127) =<< peekElemOff ptr 31
    return $ CxSecretKey bs

cxToPublic :: CxSecretKey -> CxPublicKey
cxToPublic (CxSecretKey bs) = unsafePerformIO $ do
    let len = 32
    ptr <- mallocBytes len
    BS.unsafeUseAsCString bs $ \secret -> do
        allocaBytes 32 $ \basepoint -> do
            fillBytes basepoint 0 len
            pokeElemOff basepoint 0 9
            void $ curve25519_donna ptr (castPtr secret) basepoint
    CxPublicKey <$> BS.unsafePackCStringFinalizer ptr len (free ptr)

diffieHellman :: CxPublicKey -> CxSecretKey -> DhSecret
diffieHellman (CxPublicKey bpub) (CxSecretKey bsec) = unsafePerformIO $ do
    let len = 32
    ptr <- mallocBytes len
    BS.unsafeUseAsCString bpub $ \pub -> do
        BS.unsafeUseAsCString bsec $ \sec -> do
            void $ curve25519_donna ptr (castPtr sec) (castPtr pub)
    DhSecret <$> BS.unsafePackCStringFinalizer ptr len (free ptr)

foreign import ccall unsafe "curve25519_donna" curve25519_donna :: Ptr Word8 -> Ptr Word8 -> Ptr Word8 -> IO CInt

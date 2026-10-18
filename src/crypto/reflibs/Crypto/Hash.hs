module Crypto.Hash (
    Digest,
    Blake2b_256,

    digestFromByteString,
    hashInit,
    hashUpdates,
    hashFinalize,
    hashDigestSize,
) where

import Control.DeepSeq
import Control.Monad

import Data.ByteArray
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Unsafe

import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr
import Foreign.Storable

import System.IO.Unsafe


newtype Digest a = Digest ByteString
    deriving (Eq, Ord, NFData, ByteArrayAccess)

data Context a = Context (ForeignPtr Blake2bContext)

data Blake2bContext

data Blake2b_256


digestFromByteString :: ByteString -> Maybe (Digest Blake2b_256)
digestFromByteString bs
    | BS.length bs == digestSize = Just (Digest bs)
    | otherwise                  = Nothing

hashInit :: Context Blake2b_256
hashInit = unsafePerformIO $ do
    fctx <- mallocForeignPtrBytes stateSize
    withForeignPtr fctx $ \ctx -> void $ cInit ctx (fromIntegral digestSize)
    return $ Context fctx

hashUpdates :: Context Blake2b_256 -> [ ByteString ] -> Context Blake2b_256
hashUpdates (Context fbefore) updates = unsafePerformIO $ do
    fafter <- mallocForeignPtrBytes stateSize
    withForeignPtr fbefore $ \before ->
        withForeignPtr fafter $ \after -> do
            copyBytes after before stateSize
            forM_ updates $ \update -> unsafeUseAsCStringLen update $ \( ptr, len ) -> do
                void $ cUpdate after ptr (fromIntegral len)
    return $ Context fafter

hashFinalize :: Context Blake2b_256 -> Digest Blake2b_256
hashFinalize (Context fctx) = unsafePerformIO $ do
    bytes <- mallocBytes digestSize
    withForeignPtr fctx $ \ctx ->
        void $ cFinal ctx bytes (fromIntegral digestSize)
    Digest <$> unsafePackMallocCStringLen ( bytes, digestSize )

hashDigestSize :: Blake2b_256 -> Int
hashDigestSize _ = fromIntegral digestSize

digestSize :: Int
digestSize = 32

stateSize :: Int
stateSize = fromIntegral $ unsafeDupablePerformIO $ peek cStateSize

foreign import ccall unsafe "&blake2b_state_size" cStateSize :: Ptr CSize
foreign import ccall unsafe "blake2b_init" cInit :: Ptr Blake2bContext -> CSize -> IO CInt
foreign import ccall unsafe "blake2b_update" cUpdate :: Ptr Blake2bContext -> Ptr CChar -> CSize -> IO CInt
foreign import ccall unsafe "blake2b_final" cFinal :: Ptr Blake2bContext -> Ptr CChar -> CSize -> IO CInt

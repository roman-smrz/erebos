module Crypto.Random.Entropy (
    getEntropy,
) where

import Foreign.C.Error
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Ptr

import Data.ByteString (ByteString)
import Data.ByteString.Unsafe

getEntropy :: Int -> IO ByteString
getEntropy n = do
    bytes <- mallocBytes n
    throwErrnoIfMinus1_ "getentropy" $ c_getentropy bytes (fromIntegral n)
    unsafePackMallocCStringLen ( bytes, n )

foreign import ccall "unistd.h getentropy" c_getentropy :: Ptr CChar -> CSize -> IO CInt

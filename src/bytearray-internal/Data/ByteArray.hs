module Data.ByteArray (
    ByteArray(..),
    ByteArrayAccess(..),

    ScrubbedBytes,

    take,
    convert,
) where

import Prelude hiding (length, take)

import Control.Exception

import Data.Bits
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Internal qualified as BSI
import Data.ByteString.Unsafe qualified as BS
import Data.List (foldl')

import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Utils
import Foreign.Ptr

import System.IO.Unsafe


class ByteArrayAccess ba where
    length :: ba -> Int
    withByteArray :: ba -> (Ptr p -> IO a) -> IO a

class (Eq ba, Ord ba, Monoid ba, ByteArrayAccess ba) => ByteArray ba where
    allocRet :: Int -> (Ptr p -> IO a) -> IO ( a, ba )


instance ByteArrayAccess BS.ByteString where
    length = BS.length
    withByteArray bs f = BS.unsafeUseAsCString bs (f . castPtr)

instance ByteArray BS.ByteString where
    allocRet len f = do
        fptr <- BSI.mallocByteString len
        x <- withForeignPtr fptr $ f . castPtr
        return ( x, BSI.fromForeignPtr fptr 0 len )


newtype ScrubbedBytes = ScrubbedBytes ByteString
    deriving (Ord, ByteArrayAccess)

instance Eq ScrubbedBytes where
    ScrubbedBytes a == ScrubbedBytes b =
        BS.length a == BS.length b && foldl' (.|.) 0 (BS.zipWith xor a b) == 0

instance Semigroup ScrubbedBytes where
    a <> b = unsafePerformIO $ fmap snd $ allocRet (la + lb) $ \dst -> do
        withByteArray a $ \src -> copyBytes dst src la
        withByteArray b $ \src -> copyBytes (dst `plusPtr` la) src lb
      where
        la = length a
        lb = length b

instance Monoid ScrubbedBytes where
    mempty = ScrubbedBytes BS.empty

instance ByteArray ScrubbedBytes where
    allocRet len f = mask $ \restore -> do
        ptr <- mallocBytes len
        let scrubAndFree = fillBytes ptr 0 len >> free ptr
        x <- restore (f $ castPtr ptr) `onException` scrubAndFree
        ( x, ) . ScrubbedBytes <$> BS.unsafePackCStringFinalizer ptr len scrubAndFree


take :: Int -> ScrubbedBytes -> ScrubbedBytes
take n (ScrubbedBytes bs) = ScrubbedBytes (BS.take n bs)

convert :: (ByteArrayAccess bin, ByteArray bout) => bin -> bout
convert ba = snd $ unsafePerformIO $ allocRet len $ \dst -> do
    withByteArray ba $ \src -> copyBytes dst src len
  where
    len = length ba

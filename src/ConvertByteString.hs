module ConvertByteString (
    ToByteString(..),
    FromByteString(..),
    ConvertByteString(..),
    BA.ScrubbedBytes,
) where

import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString.Unsafe qualified as BS

import Foreign.Ptr


class ToByteString a where
    toByteString :: a -> ByteString
    withByteArray :: a -> (Ptr p -> IO b) -> IO b

class FromByteString a where
    fromByteString :: ByteString -> a

class ConvertByteString a b where
    convert :: a -> b


instance ToByteString ByteString where
    toByteString = id
    withByteArray bs f = BS.unsafeUseAsCString bs (f . castPtr)

instance FromByteString ByteString where
    fromByteString = id

instance ToByteString a => ConvertByteString a ByteString where
    convert = toByteString


instance ToByteString BA.ScrubbedBytes where
    toByteString = BA.convert
    withByteArray = BA.withByteArray

instance FromByteString BA.ScrubbedBytes where
    fromByteString = BA.convert

instance BA.ByteArrayAccess a => ConvertByteString a BA.ScrubbedBytes where
    convert = BA.convert

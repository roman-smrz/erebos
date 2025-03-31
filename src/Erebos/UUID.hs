module Erebos.UUID (
    UUID,
    toString, fromString,
    toText, fromText,
    toASCIIBytes, fromASCIIBytes,
    nextRandom,
) where

import Control.Monad

import Crypto.Random.Entropy

import Data.Bits
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BSC
import Data.Text (Text)
import Data.Text.Encoding

import Erebos.Util


newtype UUID = UUID ByteString
    deriving (Eq, Ord)

instance Show UUID where
    show = toString


toString :: UUID -> String
toString = BSC.unpack . toASCIIBytes

fromString :: String -> Maybe UUID
fromString = fromASCIIBytes . BSC.pack

toText :: UUID -> Text
toText = decodeUtf8 . toASCIIBytes

fromText :: Text -> Maybe UUID
fromText = fromASCIIBytes . encodeUtf8

toASCIIBytes :: UUID -> ByteString
toASCIIBytes (UUID uuid) = BS.concat
    [ showHex $ BS.take 4 $            uuid
    , BSC.singleton '-'
    , showHex $ BS.take 2 $ BS.drop  4 uuid
    , BSC.singleton '-'
    , showHex $ BS.take 2 $ BS.drop  6 uuid
    , BSC.singleton '-'
    , showHex $ BS.take 2 $ BS.drop  8 uuid
    , BSC.singleton '-'
    , showHex             $ BS.drop 10 uuid
    ]

fromASCIIBytes :: ByteString -> Maybe UUID
fromASCIIBytes bs = do
    guard $ BS.length bs == 36
    guard $ BSC.index bs 8 == '-'
    guard $ BSC.index bs 13 == '-'
    guard $ BSC.index bs 18 == '-'
    guard $ BSC.index bs 23 == '-'
    UUID . BS.concat <$> sequence
        [ readHex $ BS.take 8 $            bs
        , readHex $ BS.take 4 $ BS.drop 9  bs
        , readHex $ BS.take 4 $ BS.drop 14 bs
        , readHex $ BS.take 4 $ BS.drop 19 bs
        , readHex             $ BS.drop 24 bs
        ]


nextRandom :: IO UUID
nextRandom = do
    [ b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, ba, bb, bc, bd, be, bf ]
        <- BS.unpack <$> getEntropy 16
    let version = 4
        b6' = b6 .&. 0x0f .|. (version `shiftL` 4)
        b8' = b8 .&. 0x3f .|. 0x80
    return $ UUID $ BS.pack [ b0, b1, b2, b3, b4, b5, b6', b7, b8', b9, ba, bb, bc, bd, be, bf ]

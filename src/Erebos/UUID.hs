module Erebos.UUID (
    UUID,
    toString, fromString,
    toText, fromText,
    toASCIIBytes, fromASCIIBytes,
    nextRandom,
) where

import Crypto.Random.Entropy

import Data.Bits
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BSL
import Data.Maybe
import Data.UUID.Types

nextRandom :: IO UUID
nextRandom = do
    [ b0, b1, b2, b3, b4, b5, b6, b7, b8, b9, ba, bb, bc, bd, be, bf ]
        <- BS.unpack <$> getEntropy 16
    let version = 4
        b6' = b6 .&. 0x0f .|. (version `shiftL` 4)
        b8' = b8 .&. 0x3f .|. 0x80
    return $ fromJust $ fromByteString $ BSL.pack [ b0, b1, b2, b3, b4, b5, b6', b7, b8', b9, ba, bb, bc, bd, be, bf ]

{-|
Description: Core Erebos objects and references

Data types and functions for working with "raw" Erebos objects and references.
-}

module Erebos.Object (
    Object, PartialObject, Object'(..),
    serializeObject, deserializeObject, deserializeObjects,
    ioLoadObject, ioLoadBytes,
    storeRawBytes, lazyLoadBytes,

    RecItem, RecItem'(..),

    Ref, PartialRef, RefDigest,
    refDigest, refFromDigest,
    readRef, showRef,
    readRefDigest, showRefDigest,
    refDigestFromByteString, hashToRefDigest,
    copyRef, partialRef, partialRefFromDigest,

    componentSize,
    partialComponentSize,
) where

import Data.ByteString.Lazy qualified as BL
import Data.Maybe
import Data.Set qualified as S
import Data.Word

import Erebos.Object.Internal


componentSize :: Ref -> Word64
componentSize ref = go S.empty [ ref ]
  where
    go seen (r : rs)
        | refDigest r `S.member` seen = go seen rs
        | otherwise = objectSize r + go (S.insert (refDigest r) seen) (referredFrom r ++ rs)
    go _ [] = 0

    objectSize = fromIntegral . BL.length . lazyLoadBytes
    referredFrom r = case load r of
        Rec items -> mapMaybe ((\case RecRef r' -> Just r'; _ -> Nothing) . snd) items
        _ -> []

partialComponentSize :: PartialRef -> IO Word64
partialComponentSize ref = go S.empty [ ref ]
  where
    go seen (r : rs)
        | refDigest r `S.member` seen = go seen rs
        | otherwise = do
            size <- objectSize r
            referred <- referredFrom r
            (size +) <$> go (S.insert (refDigest r) seen) (referred ++ rs)
    go _ [] = return 0

    objectSize r = either (const 0) (fromIntegral . BL.length) <$> ioLoadBytes r
    referredFrom r = ioLoadObject r >>= return . \case
        Right (Rec items) -> mapMaybe ((\case RecRef r' -> Just r'; _ -> Nothing) . snd) items
        _ -> []

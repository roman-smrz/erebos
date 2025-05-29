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
) where

import Erebos.Object.Internal

module Test.State (
    CustomSharedState(..),
) where

import Data.Proxy

import GHC.TypeLits

import Erebos.Object
import Erebos.State
import Erebos.Storage.Merge


data CustomSharedState (tid :: Symbol) = CustomSharedState
    { customStateComponents :: StoredTips Object
    }

instance Mergeable (CustomSharedState tid) where
    type Component (CustomSharedState tid) = Object
    toComponents = customStateComponents
    mergeSorted = CustomSharedState

instance KnownSymbol tid => SharedType (CustomSharedState tid) where
    sharedTypeID _ = mkSharedTypeID (symbolVal @tid Proxy)

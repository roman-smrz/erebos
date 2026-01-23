module Erebos.Error (
    ErebosError(..),
    showErebosError,

    FromErebosError(..),
    throwErebosError,
    throwOtherError,
) where

import Control.Monad.Except

import {-# SOURCE #-} Erebos.Network.Protocol


data ErebosError
    = ManyErrors [ ErebosError ]
    | OtherError String
    | UnhandledService ServiceID

showErebosError :: ErebosError -> String
showErebosError (ManyErrors errs) = unlines $ map showErebosError errs
showErebosError (OtherError str) = str
showErebosError (UnhandledService svc) = "unhandled service ‘" ++ show svc ++ "’"

instance Semigroup ErebosError where
    ManyErrors [] <> b = b
    a <> ManyErrors [] = a
    ManyErrors a <> ManyErrors b = ManyErrors (a ++ b)
    ManyErrors a <> b = ManyErrors (a ++ [ b ])
    a <> ManyErrors b = ManyErrors (a : b)
    a <> b = ManyErrors [ a, b ]

instance Monoid ErebosError where
    mempty = ManyErrors []


class FromErebosError e where
    fromErebosError :: ErebosError -> e
    toErebosError :: e -> Maybe ErebosError

instance FromErebosError ErebosError where
    fromErebosError = id
    toErebosError = Just

throwErebosError :: (MonadError e m, FromErebosError e) => ErebosError -> m a
throwErebosError = throwError . fromErebosError

throwOtherError :: (MonadError e m, FromErebosError e) => String -> m a
throwOtherError = throwErebosError . OtherError

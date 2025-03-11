module Erebos.Error (
    ErebosError(..),
    showErebosError,

    FromErebosError(..),
    throwOtherError,
) where

import Control.Monad.Except


data ErebosError
    = ManyErrors [ ErebosError ]
    | OtherError String

showErebosError :: ErebosError -> String
showErebosError (ManyErrors errs) = unlines $ map showErebosError errs
showErebosError (OtherError str) = str

instance Semigroup ErebosError where
    ManyErrors [] <> b = b
    a <> ManyErrors [] = a
    ManyErrors a <> ManyErrors b = ManyErrors (a ++ b)
    ManyErrors a <> b = ManyErrors (a ++ [ b ])
    a <> ManyErrors b = ManyErrors (a : b)
    a@OtherError {} <> b@OtherError {} = ManyErrors [ a, b ]

instance Monoid ErebosError where
    mempty = ManyErrors []


class FromErebosError e where
    fromErebosError :: ErebosError -> e

instance FromErebosError ErebosError where
    fromErebosError = id

throwOtherError :: (MonadError e m, FromErebosError e) => String -> m a
throwOtherError = throwError . fromErebosError . OtherError

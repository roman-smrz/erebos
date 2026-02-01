{-# LANGUAGE OverloadedStrings #-}

module Erebos.TextFormat.Ansi (
    FormattedText,

    AnsiText(..),
    renderAnsiText,
) where

import Control.Applicative

import Data.String
import Data.Text (Text)
import Data.Text qualified as T

import Erebos.TextFormat.Types


newtype AnsiText = AnsiText { fromAnsiText :: Text }
    deriving (Eq, Ord, Semigroup, Monoid, IsString)


renderAnsiText :: FormattedText -> AnsiText
renderAnsiText = AnsiText . go ( Nothing, Nothing )
  where
    go cur@( cfg, cbg ) = \case
        PlainText text -> text
        ConcatenatedText ftexts -> mconcat $ map (go cur) ftexts
        FormattedText (CustomTextColor fg bg) ftext -> mconcat
            [ ansiColor fg bg
            , go ( fg <|> cfg, bg <|> cbg ) ftext
            , ansiColor
                (if fg /= cfg then cfg <|> Just DefaultColor else Nothing)
                (if bg /= cbg then cbg <|> Just DefaultColor else Nothing)
            ]


ansiColor :: Maybe Color -> Maybe Color -> Text
ansiColor Nothing Nothing = ""
ansiColor (Just fg) Nothing = "\ESC[" <> T.pack (show (colorNum fg)) <> "m"
ansiColor Nothing (Just bg) = "\ESC[" <> T.pack (show (colorNum bg + 10)) <> "m"
ansiColor (Just fg) (Just bg) = "\ESC[" <> T.pack (show (colorNum fg)) <> ";" <> T.pack (show (colorNum bg + 10)) <> "m"

colorNum :: Color -> Int
colorNum = \case
    DefaultColor -> 39
    Black -> 30
    Red -> 31
    Green -> 32
    Yellow -> 33
    Blue -> 34
    Magenta -> 35
    Cyan -> 36
    White -> 37
    BrightBlack -> 90
    BrightRed -> 91
    BrightGreen -> 92
    BrightYellow -> 93
    BrightBlue -> 94
    BrightMagenta -> 95
    BrightCyan -> 96
    BrightWhite -> 97

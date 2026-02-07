module Erebos.TextFormat.Types (
    FormattedText(..),
    TextStyle(..),
    Color(..),
) where

import Data.String
import Data.Text (Text)


data FormattedText
    = PlainText Text
    | ConcatenatedText [ FormattedText ]
    | FormattedText TextStyle FormattedText
    | EndWithNewline FormattedText

instance IsString FormattedText where
    fromString = PlainText . fromString

instance Semigroup FormattedText where
    ConcatenatedText xs <> ConcatenatedText ys = ConcatenatedText (xs ++ ys)
    x <> ConcatenatedText ys = ConcatenatedText (x : ys)
    ConcatenatedText xs <> y = ConcatenatedText (xs ++ [ y ])
    x <> y = ConcatenatedText [ x, y ]

instance Monoid FormattedText where
    mempty = ConcatenatedText []
    mconcat [] = ConcatenatedText []
    mconcat [ x ] = x
    mconcat xs = ConcatenatedText $ concatMap flatten xs
      where
        flatten (ConcatenatedText ys) = ys
        flatten y = [ y ]


data TextStyle
    = CustomTextColor (Maybe Color) (Maybe Color)


data Color
    = DefaultColor
    | Black
    | Red
    | Green
    | Yellow
    | Blue
    | Magenta
    | Cyan
    | White
    | BrightBlack
    | BrightRed
    | BrightGreen
    | BrightYellow
    | BrightBlue
    | BrightMagenta
    | BrightCyan
    | BrightWhite
    deriving (Eq)

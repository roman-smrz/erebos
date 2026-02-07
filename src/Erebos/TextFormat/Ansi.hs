{-# LANGUAGE OverloadedStrings #-}

module Erebos.TextFormat.Ansi (
    FormattedText,

    AnsiText(..),
    renderAnsiText,
) where

import Control.Applicative
import Control.Monad.State
import Control.Monad.Writer

import Data.String
import Data.Text (Text)
import Data.Text qualified as T

import Erebos.TextFormat.Types


newtype AnsiText = AnsiText { fromAnsiText :: Text }
    deriving (Eq, Ord, Semigroup, Monoid, IsString)


data RenderState = RenderState
    { rsEndedWithNewline :: Bool
    }

initialRenderState :: RenderState
initialRenderState = RenderState
    { rsEndedWithNewline = True
    }

renderAnsiText :: FormattedText -> AnsiText
renderAnsiText ft = AnsiText $ T.concat $ execWriter $ flip evalStateT initialRenderState $ go ( Nothing, Nothing ) ft
  where
    go :: ( Maybe Color, Maybe Color ) -> FormattedText -> StateT RenderState (Writer [ Text ]) ()
    go cur@( cfg, cbg ) = \case
        PlainText text -> do
            tell [ text ]
            case T.unsnoc text of
                Just ( _, c ) -> modify (\s -> s { rsEndedWithNewline = c == '\n' })
                Nothing -> return ()
        ConcatenatedText ftexts -> mconcat <$> mapM (go cur) ftexts
        FormattedText (CustomTextColor fg bg) ftext -> do
            tell [ ansiColor fg bg ]
            go ( fg <|> cfg, bg <|> cbg ) ftext
            tell [ ansiColor
                (if fg /= cfg then cfg <|> Just DefaultColor else Nothing)
                (if bg /= cbg then cbg <|> Just DefaultColor else Nothing)
                ]
        EndWithNewline ftext -> do
            go cur ftext
            gets rsEndedWithNewline >>= \case
                True -> return ()
                False -> tell [ "\n" ] >> modify (\s -> s { rsEndedWithNewline = True })


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

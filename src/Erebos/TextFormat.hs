{-# LANGUAGE OverloadedStrings #-}

module Erebos.TextFormat (
    FormattedText,
    plainText,

    TextStyle,
    withStyle, noStyle,

    Color(..),
    setForegroundColor, setBackgroundColor,

    endWithNewline,

    renderPlainText,
    formattedTextLength,
    formattedTextHeight,
) where

import Data.Text (Text)
import Data.Text qualified as T

import Erebos.TextFormat.Types


plainText :: Text -> FormattedText
plainText = PlainText


withStyle :: TextStyle -> FormattedText -> FormattedText
withStyle = FormattedText

noStyle :: TextStyle
noStyle = CustomTextColor Nothing Nothing

setForegroundColor :: Color -> TextStyle -> TextStyle
setForegroundColor color (CustomTextColor _ bg) = CustomTextColor (Just color) bg

setBackgroundColor :: Color -> TextStyle -> TextStyle
setBackgroundColor color (CustomTextColor fg _) = CustomTextColor fg (Just color)


endWithNewline :: FormattedText -> FormattedText
endWithNewline = EndWithNewline


renderPlainText :: FormattedText -> Text
renderPlainText = \case
    PlainText text -> text
    ConcatenatedText ftexts -> mconcat $ map renderPlainText ftexts
    FormattedText _ ftext -> renderPlainText ftext
    EndWithNewline ftext -> let res = renderPlainText ftext
                             in case T.unsnoc res of
                                    Just ( _, '\n') -> res
                                    _               -> res <> "\n"

formattedTextLength :: FormattedText -> Int
formattedTextLength = \case
    PlainText text -> T.length text
    ConcatenatedText ftexts -> sum $ map formattedTextLength ftexts
    FormattedText _ ftext -> formattedTextLength ftext
    EndWithNewline ftext -> formattedTextLength ftext

formattedTextHeight :: FormattedText -> Int
formattedTextHeight = countLines . collectParts
  where
    collectParts = \case
        PlainText text -> [ text ]
        ConcatenatedText ftexts -> concatMap collectParts ftexts
        FormattedText _ ftext -> collectParts ftext
        EndWithNewline ftext -> collectParts ftext
    countLines (t : ts)
        | T.null t = countLines ts
        | otherwise = 1 + countLines (dropLine (t : ts))
    countLines [] = 0
    dropLine (t : ts)
        | Just ( '\n', t' ) <- T.uncons (T.dropWhile (/= '\n') t) = t' : ts
        | otherwise = dropLine ts
    dropLine [] = []

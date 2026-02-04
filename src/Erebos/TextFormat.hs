module Erebos.TextFormat (
    FormattedText,
    plainText,

    TextStyle,
    withStyle, noStyle,

    Color(..),
    setForegroundColor, setBackgroundColor,

    renderPlainText,
    formattedTextLength,
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


renderPlainText :: FormattedText -> Text
renderPlainText = \case
    PlainText text -> text
    ConcatenatedText ftexts -> mconcat $ map renderPlainText ftexts
    FormattedText _ ftext -> renderPlainText ftext

formattedTextLength :: FormattedText -> Int
formattedTextLength = \case
    PlainText text -> T.length text
    ConcatenatedText ftexts -> sum $ map formattedTextLength ftexts
    FormattedText _ ftext -> formattedTextLength ftext

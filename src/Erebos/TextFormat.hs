module Erebos.TextFormat (
    FormattedText,

    renderPlainText,
) where

import Data.Text (Text)

import Erebos.TextFormat.Types


renderPlainText :: FormattedText -> Text
renderPlainText = \case
    PlainText text -> text
    ConcatenatedText ftexts -> mconcat $ map renderPlainText ftexts
    FormattedText _ ftext -> renderPlainText ftext

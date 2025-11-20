module Erebos.Conversation.Class (
    ConversationType(..),
) where

import Data.Text (Text)
import Data.Time.LocalTime
import Data.Typeable

import Erebos.Identity


class (Typeable conv, Typeable msg) => ConversationType conv msg | conv -> msg, msg -> conv where
    convMessageFrom :: msg -> ComposedIdentity
    convMessageTime :: msg -> ZonedTime
    convMessageText :: msg -> Maybe Text
    convMessageListSince :: Maybe conv -> conv -> [ ( msg, Bool ) ]

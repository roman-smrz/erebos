module Erebos.Conversation.Class (
    ConversationType(..),
    MessageExtra(..),
    RefDigest,
) where

import Control.Monad.Except

import Data.Text (Text)
import Data.Time.LocalTime
import Data.Typeable

import Erebos.Error
import Erebos.Identity
import Erebos.Object
import Erebos.State


data MessageExtra
    = UserJoined
    | UserLeft


class (Typeable conv, Typeable msg) => ConversationType conv msg | conv -> msg, msg -> conv where
    convMessageFrom :: msg -> ComposedIdentity
    convMessageTime :: msg -> ZonedTime
    convMessageText :: msg -> Maybe Text
    convMessageExtra :: msg -> [ MessageExtra ]
    convMessageExtra _ = []

    convReference :: conv -> RefDigest
    convMessageListSince
        :: Maybe conv -- ^ Original state to diff from
        -> conv       -- ^ Current state
        -> ( Int, [ ( msg, Bool ) ] ) -- ^ Number of removed, list of added messages

    convMarkAllSeen :: (MonadHead LocalState m, MonadError e m, FromErebosError e) => conv -> m ()

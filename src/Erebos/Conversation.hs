module Erebos.Conversation (
    Message,
    messageFrom,
    messageText,
    messageUnread,
    formatMessage,

    Conversation,
    directMessageConversation,
    reloadConversation,
    lookupConversations,

    conversationName,
    conversationPeer,
    conversationHistory,

    sendMessage,
) where

import Control.Monad.Except

import Data.List
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.LocalTime

import Erebos.Identity
import Erebos.Message hiding (formatMessage)
import Erebos.State
import Erebos.Storage


data Message = DirectMessageMessage DirectMessage Bool

messageFrom :: Message -> ComposedIdentity
messageFrom (DirectMessageMessage msg _) = msgFrom msg

messageText :: Message -> Maybe Text
messageText (DirectMessageMessage msg _) = Just $ msgText msg

messageUnread :: Message -> Bool
messageUnread (DirectMessageMessage _ unread) = unread

formatMessage :: TimeZone -> Message -> String
formatMessage tzone (DirectMessageMessage msg _) = formatDirectMessage tzone msg


data Conversation = DirectMessageConversation DirectMessageThread

directMessageConversation :: MonadHead LocalState m => ComposedIdentity -> m Conversation
directMessageConversation peer = do
    (find (sameIdentity peer . msgPeer) . toThreadList . lookupSharedValue . lsShared . fromStored <$> getLocalHead) >>= \case
        Just thread -> return $ DirectMessageConversation thread
        Nothing -> return $ DirectMessageConversation $ DirectMessageThread peer [] [] []

reloadConversation :: MonadHead LocalState m => Conversation -> m Conversation
reloadConversation (DirectMessageConversation thread) = directMessageConversation (msgPeer thread)

lookupConversations :: MonadHead LocalState m => m [Conversation]
lookupConversations = map DirectMessageConversation . toThreadList . lookupSharedValue . lsShared . fromStored <$> getLocalHead


conversationName :: Conversation -> Text
conversationName (DirectMessageConversation thread) = fromMaybe (T.pack "<unnamed>") $ idName $ msgPeer thread

conversationPeer :: Conversation -> Maybe ComposedIdentity
conversationPeer (DirectMessageConversation thread) = Just $ msgPeer thread

conversationHistory :: Conversation -> [Message]
conversationHistory (DirectMessageConversation thread) = map (\msg -> DirectMessageMessage msg False) $ threadToList thread


sendMessage :: (MonadHead LocalState m, MonadError String m) => Conversation -> Text -> m Message
sendMessage (DirectMessageConversation thread) text = DirectMessageMessage <$> (fromStored <$> sendDirectMessage (msgPeer thread) text) <*> pure False

module Erebos.Conversation (
    Message,
    messageFrom,
    messageTime,
    messageText,
    messageUnread,
    formatMessage,

    Conversation,
    directMessageConversation,
    chatroomConversation,
    chatroomConversationByStateData,
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
import Data.Time.Format
import Data.Time.LocalTime

import Erebos.Chatroom
import Erebos.Identity
import Erebos.Message hiding (formatMessage)
import Erebos.State
import Erebos.Storable


data Message = DirectMessageMessage DirectMessage Bool
             | ChatroomMessage ChatMessage Bool

messageFrom :: Message -> ComposedIdentity
messageFrom (DirectMessageMessage msg _) = msgFrom msg
messageFrom (ChatroomMessage msg _) = cmsgFrom msg

messageTime :: Message -> ZonedTime
messageTime (DirectMessageMessage msg _) = msgTime msg
messageTime (ChatroomMessage msg _) = cmsgTime msg

messageText :: Message -> Maybe Text
messageText (DirectMessageMessage msg _) = Just $ msgText msg
messageText (ChatroomMessage msg _) = cmsgText msg

messageUnread :: Message -> Bool
messageUnread (DirectMessageMessage _ unread) = unread
messageUnread (ChatroomMessage _ unread) = unread

formatMessage :: TimeZone -> Message -> String
formatMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ messageTime msg
    , maybe "<unnamed>" T.unpack $ idName $ messageFrom msg
    , maybe "" ((": "<>) . T.unpack) $ messageText msg
    ]


data Conversation = DirectMessageConversation DirectMessageThread
                  | ChatroomConversation ChatroomState

directMessageConversation :: MonadHead LocalState m => ComposedIdentity -> m Conversation
directMessageConversation peer = do
    (find (sameIdentity peer . msgPeer) . toThreadList . lookupSharedValue . lsShared . fromStored <$> getLocalHead) >>= \case
        Just thread -> return $ DirectMessageConversation thread
        Nothing -> return $ DirectMessageConversation $ DirectMessageThread peer [] [] []

chatroomConversation :: MonadHead LocalState m => ChatroomState -> m (Maybe Conversation)
chatroomConversation rstate = chatroomConversationByStateData (head $ roomStateData rstate)

chatroomConversationByStateData :: MonadHead LocalState m => Stored ChatroomStateData -> m (Maybe Conversation)
chatroomConversationByStateData sdata = fmap ChatroomConversation <$> findChatroomByStateData sdata

reloadConversation :: MonadHead LocalState m => Conversation -> m Conversation
reloadConversation (DirectMessageConversation thread) = directMessageConversation (msgPeer thread)
reloadConversation cur@(ChatroomConversation rstate) =
    fromMaybe cur <$> chatroomConversation rstate

lookupConversations :: MonadHead LocalState m => m [Conversation]
lookupConversations = map DirectMessageConversation . toThreadList . lookupSharedValue . lsShared . fromStored <$> getLocalHead


conversationName :: Conversation -> Text
conversationName (DirectMessageConversation thread) = fromMaybe (T.pack "<unnamed>") $ idName $ msgPeer thread
conversationName (ChatroomConversation rstate) = fromMaybe (T.pack "<unnamed>") $ roomName =<< roomStateRoom rstate

conversationPeer :: Conversation -> Maybe ComposedIdentity
conversationPeer (DirectMessageConversation thread) = Just $ msgPeer thread
conversationPeer (ChatroomConversation _) = Nothing

conversationHistory :: Conversation -> [Message]
conversationHistory (DirectMessageConversation thread) = map (\msg -> DirectMessageMessage msg False) $ threadToList thread
conversationHistory (ChatroomConversation rstate) = map (\msg -> ChatroomMessage msg False) $ roomStateMessages rstate


sendMessage :: (MonadHead LocalState m, MonadError String m) => Conversation -> Text -> m (Maybe Message)
sendMessage (DirectMessageConversation thread) text = fmap Just $ DirectMessageMessage <$> (fromStored <$> sendDirectMessage (msgPeer thread) text) <*> pure False
sendMessage (ChatroomConversation rstate) text = sendChatroomMessage rstate text >> return Nothing

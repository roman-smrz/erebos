{-# LANGUAGE OverloadedStrings #-}

module Erebos.Conversation (
    Message,
    messageFrom,
    messageTime,
    messageText,
    messageUnread,
    formatMessage,
    formatMessageFT,

    Conversation,
    isSameConversation,
    directMessageConversation,
    chatroomConversation,
    chatroomConversationByStateData,
    isChatroomStateConversation,
    reloadConversation,
    lookupConversations,

    conversationName,
    conversationPeer,
    conversationHistory,

    sendMessage,
    deleteConversation,
) where

import Control.Monad.Except

import Data.List
import Data.Maybe
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Format
import Data.Time.LocalTime

import Erebos.Chatroom
import Erebos.Conversation.Class
import Erebos.DirectMessage
import Erebos.Identity
import Erebos.State
import Erebos.Storable
import Erebos.TextFormat
import Erebos.TextFormat.Types


data Message = forall conv msg. ConversationType conv msg => Message msg Bool

withMessage :: (forall conv msg. ConversationType conv msg => msg -> a) -> Message -> a
withMessage f (Message msg _) = f msg

messageFrom :: Message -> ComposedIdentity
messageFrom = withMessage convMessageFrom

messageTime :: Message -> ZonedTime
messageTime = withMessage convMessageTime

messageText :: Message -> Maybe Text
messageText = withMessage convMessageText

messageUnread :: Message -> Bool
messageUnread (Message _ unread) = unread

formatMessage :: TimeZone -> Message -> String
formatMessage tzone = T.unpack . renderPlainText . formatMessageFT tzone

formatMessageFT :: TimeZone -> Message -> FormattedText
formatMessageFT tzone msg =
    (if messageUnread msg then FormattedText (CustomTextColor (Just BrightYellow) Nothing) else id) $ mconcat
        [ PlainText $ T.pack $ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ messageTime msg
        , maybe "<unnamed>" PlainText $ idName $ messageFrom msg
        , maybe "" ((": " <>) . PlainText) $ messageText msg
        ]


data Conversation
    = DirectMessageConversation DirectMessageThread
    | ChatroomConversation ChatroomState

withConversation :: (forall conv msg. ConversationType conv msg => conv -> a) -> Conversation -> a
withConversation f (DirectMessageConversation conv) = f conv
withConversation f (ChatroomConversation conv) = f conv

isSameConversation :: Conversation -> Conversation -> Bool
isSameConversation (DirectMessageConversation t) (DirectMessageConversation t')
    = sameIdentity (msgPeer t) (msgPeer t')
isSameConversation (ChatroomConversation rstate) (ChatroomConversation rstate') = isSameChatroom rstate rstate'
isSameConversation _ _ = False

directMessageConversation :: MonadHead LocalState m => ComposedIdentity -> m Conversation
directMessageConversation peer = do
    createOrUpdateDirectMessagePeer peer
    (find (sameIdentity peer . msgPeer) . dmThreadList . lookupSharedValue . lsShared . fromStored <$> getLocalHead) >>= \case
        Just thread -> return $ DirectMessageConversation thread
        Nothing -> return $ DirectMessageConversation $ DirectMessageThread peer [] [] [] []

chatroomConversation :: MonadHead LocalState m => ChatroomState -> m (Maybe Conversation)
chatroomConversation rstate = chatroomConversationByStateData (head $ roomStateData rstate)

chatroomConversationByStateData :: MonadHead LocalState m => Stored ChatroomStateData -> m (Maybe Conversation)
chatroomConversationByStateData sdata = fmap ChatroomConversation <$> findChatroomByStateData sdata

isChatroomStateConversation :: ChatroomState -> Conversation -> Bool
isChatroomStateConversation rstate (ChatroomConversation rstate') = isSameChatroom rstate rstate'
isChatroomStateConversation _ _ = False

reloadConversation :: MonadHead LocalState m => Conversation -> m Conversation
reloadConversation (DirectMessageConversation thread) = directMessageConversation (msgPeer thread)
reloadConversation cur@(ChatroomConversation rstate) =
    fromMaybe cur <$> chatroomConversation rstate

lookupConversations :: MonadHead LocalState m => m [ Conversation ]
lookupConversations = map DirectMessageConversation . dmThreadList . lookupSharedValue . lsShared . fromStored <$> getLocalHead


conversationName :: Conversation -> Text
conversationName (DirectMessageConversation thread) = fromMaybe (T.pack "<unnamed>") $ idName $ msgPeer thread
conversationName (ChatroomConversation rstate) = fromMaybe (T.pack "<unnamed>") $ roomName =<< roomStateRoom rstate

conversationPeer :: Conversation -> Maybe ComposedIdentity
conversationPeer (DirectMessageConversation thread) = Just $ msgPeer thread
conversationPeer (ChatroomConversation _) = Nothing

conversationHistory :: Conversation -> [ Message ]
conversationHistory = withConversation $ map (uncurry Message) . convMessageListSince Nothing


sendMessage :: (MonadHead LocalState m, MonadError e m, FromErebosError e) => Conversation -> Text -> m ()
sendMessage (DirectMessageConversation thread) text = sendDirectMessage (msgPeer thread) text
sendMessage (ChatroomConversation rstate) text = sendChatroomMessage rstate text

deleteConversation :: (MonadHead LocalState m, MonadError e m, FromErebosError e) => Conversation -> m ()
deleteConversation (DirectMessageConversation _) = throwOtherError "deleting direct message conversation is not supported"
deleteConversation (ChatroomConversation rstate) = deleteChatroomByStateData (head $ roomStateData rstate)

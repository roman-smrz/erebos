module Erebos.Message (
    DirectMessage(..),
    sendDirectMessage,

    DirectMessageAttributes(..),
    defaultDirectMessageAttributes,

    DirectMessageThreads,
    toThreadList,

    DirectMessageThread(..),
    threadToList,
    messageThreadView,

    watchReceivedMessages,
    formatMessage,
) where

import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.List
import Data.Ord
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Format
import Data.Time.LocalTime

import Erebos.Identity
import Erebos.Network
import Erebos.Service
import Erebos.State
import Erebos.Storage
import Erebos.Storage.Merge

data DirectMessage = DirectMessage
    { msgFrom :: ComposedIdentity
    , msgPrev :: [Stored DirectMessage]
    , msgTime :: ZonedTime
    , msgText :: Text
    }

instance Storable DirectMessage where
    store' msg = storeRec $ do
        mapM_ (storeRef "from") $ idExtDataF $ msgFrom msg
        mapM_ (storeRef "PREV") $ msgPrev msg
        storeDate "time" $ msgTime msg
        storeText "text" $ msgText msg

    load' = loadRec $ DirectMessage
        <$> loadIdentity "from"
        <*> loadRefs "PREV"
        <*> loadDate "time"
        <*> loadText "text"

data DirectMessageAttributes = DirectMessageAttributes
    { dmOwnerMismatch :: ServiceHandler DirectMessage ()
    }

defaultDirectMessageAttributes :: DirectMessageAttributes
defaultDirectMessageAttributes = DirectMessageAttributes
    { dmOwnerMismatch = svcPrint "Owner mismatch"
    }

instance Service DirectMessage where
    serviceID _ = mkServiceID "c702076c-4928-4415-8b6b-3e839eafcb0d"

    type ServiceAttributes DirectMessage = DirectMessageAttributes
    defaultServiceAttributes _ = defaultDirectMessageAttributes

    serviceHandler smsg = do
        let msg = fromStored smsg
        powner <- asks $ finalOwner . svcPeerIdentity
        erb <- svcGetLocal
        st <- getStorage
        let DirectMessageThreads prev _ = lookupSharedValue $ lsShared $ fromStored erb
            sent = findMsgProperty powner msSent prev
            received = findMsgProperty powner msReceived prev
            received' = filterAncestors $ smsg : received
        if powner `sameIdentity` msgFrom msg ||
               filterAncestors sent == filterAncestors (smsg : sent)
           then do
               when (received' /= received) $ do
                   next <- wrappedStore st $ MessageState
                       { msPrev = prev
                       , msPeer = powner
                       , msReady = []
                       , msSent = []
                       , msReceived = received'
                       , msSeen = []
                       }
                   let threads = DirectMessageThreads [next] (messageThreadView [next])
                   shared <- makeSharedStateUpdate st threads (lsShared $ fromStored erb)
                   svcSetLocal =<< wrappedStore st (fromStored erb) { lsShared = [shared] }

               when (powner `sameIdentity` msgFrom msg) $ do
                   replyStoredRef smsg

           else join $ asks $ dmOwnerMismatch . svcAttributes

    serviceNewPeer = syncDirectMessageToPeer . lookupSharedValue . lsShared . fromStored =<< svcGetLocal

    serviceStorageWatchers _ = (:[]) $
        SomeStorageWatcher (lookupSharedValue . lsShared . fromStored) syncDirectMessageToPeer


data MessageState = MessageState
    { msPrev :: [Stored MessageState]
    , msPeer :: ComposedIdentity
    , msReady :: [Stored DirectMessage]
    , msSent :: [Stored DirectMessage]
    , msReceived :: [Stored DirectMessage]
    , msSeen :: [Stored DirectMessage]
    }

data DirectMessageThreads = DirectMessageThreads [Stored MessageState] [DirectMessageThread]

instance Eq DirectMessageThreads where
    DirectMessageThreads mss _ == DirectMessageThreads mss' _ = mss == mss'

toThreadList :: DirectMessageThreads -> [DirectMessageThread]
toThreadList (DirectMessageThreads _ threads) = threads

instance Storable MessageState where
    store' MessageState {..} = storeRec $ do
        mapM_ (storeRef "PREV") msPrev
        mapM_ (storeRef "peer") $ idDataF msPeer
        mapM_ (storeRef "ready") msReady
        mapM_ (storeRef "sent") msSent
        mapM_ (storeRef "received") msReceived
        mapM_ (storeRef "seen") msSeen

    load' = loadRec $ do
        msPrev <- loadRefs "PREV"
        msPeer <- loadIdentity "peer"
        msReady <- loadRefs "ready"
        msSent <- loadRefs "sent"
        msReceived <- loadRefs "received"
        msSeen <- loadRefs "seen"
        return MessageState {..}

instance Mergeable DirectMessageThreads where
    type Component DirectMessageThreads = MessageState
    mergeSorted mss = DirectMessageThreads mss (messageThreadView mss)
    toComponents (DirectMessageThreads mss _) = mss

instance SharedType DirectMessageThreads where
    sharedTypeID _ = mkSharedTypeID "ee793681-5976-466a-b0f0-4e1907d3fade"

findMsgProperty :: Foldable m => Identity m -> (MessageState -> [a]) -> [Stored MessageState] -> [a]
findMsgProperty pid sel mss = concat $ flip findProperty mss $ \x -> do
    guard $ msPeer x `sameIdentity` pid
    guard $ not $ null $ sel x
    return $ sel x


sendDirectMessage :: (Foldable f, Applicative f, MonadHead LocalState m, MonadError String m)
                  => Identity f -> Text -> m (Stored DirectMessage)
sendDirectMessage pid text = updateLocalHead $ \ls -> do
    let self = localIdentity $ fromStored ls
        powner = finalOwner pid
    flip updateSharedState ls $ \(DirectMessageThreads prev _) -> do
        let ready = findMsgProperty powner msReady prev
            received = findMsgProperty powner msReceived prev

        time <- liftIO getZonedTime
        smsg <- mstore DirectMessage
            { msgFrom = toComposedIdentity $ finalOwner self
            , msgPrev = filterAncestors $ ready ++ received
            , msgTime = time
            , msgText = text
            }
        next <- mstore MessageState
            { msPrev = prev
            , msPeer = powner
            , msReady = [smsg]
            , msSent = []
            , msReceived = []
            , msSeen = []
            }
        return (DirectMessageThreads [next] (messageThreadView [next]), smsg)

syncDirectMessageToPeer :: DirectMessageThreads -> ServiceHandler DirectMessage ()
syncDirectMessageToPeer (DirectMessageThreads mss _) = do
    pid <- finalOwner <$> asks svcPeerIdentity
    peer <- asks svcPeer
    let thread = messageThreadFor pid mss
    mapM_ (sendToPeerStored peer) $ msgHead thread
    updateLocalHead_ $ \ls -> do
        let powner = finalOwner pid
        flip updateSharedState_ ls $ \unchanged@(DirectMessageThreads prev _) -> do
            let ready = findMsgProperty powner msReady prev
                sent = findMsgProperty powner msSent prev
                sent' = filterAncestors (ready ++ sent)

            if sent' /= sent
              then do
                next <- mstore MessageState
                    { msPrev = prev
                    , msPeer = powner
                    , msReady = []
                    , msSent = sent'
                    , msReceived = []
                    , msSeen = []
                    }
                return $ DirectMessageThreads [next] (messageThreadView [next])
              else do
                return unchanged


data DirectMessageThread = DirectMessageThread
    { msgPeer :: ComposedIdentity
    , msgHead :: [Stored DirectMessage]
    , msgSent :: [Stored DirectMessage]
    , msgSeen :: [Stored DirectMessage]
    }

threadToList :: DirectMessageThread -> [DirectMessage]
threadToList thread = helper S.empty $ msgHead thread
    where helper seen msgs
              | msg : msgs' <- filter (`S.notMember` seen) $ reverse $ sortBy (comparing cmpView) msgs =
                  fromStored msg : helper (S.insert msg seen) (msgs' ++ msgPrev (fromStored msg))
              | otherwise = []
          cmpView msg = (zonedTimeToUTC $ msgTime $ fromStored msg, msg)

messageThreadView :: [Stored MessageState] -> [DirectMessageThread]
messageThreadView = helper []
    where helper used ms' = case filterAncestors ms' of
              mss@(sms : rest)
                  | any (sameIdentity $ msPeer $ fromStored sms) used ->
                      helper used $ msPrev (fromStored sms) ++ rest
                  | otherwise ->
                      let peer = msPeer $ fromStored sms
                       in messageThreadFor peer mss : helper (peer : used) (msPrev (fromStored sms) ++ rest)
              _ -> []

messageThreadFor :: ComposedIdentity -> [Stored MessageState] -> DirectMessageThread
messageThreadFor peer mss =
    let ready = findMsgProperty peer msReady mss
        sent = findMsgProperty peer msSent mss
        received = findMsgProperty peer msReceived mss
        seen = findMsgProperty peer msSeen mss

     in DirectMessageThread
         { msgPeer = peer
         , msgHead = filterAncestors $ ready ++ received
         , msgSent = filterAncestors $ sent ++ received
         , msgSeen = filterAncestors $ ready ++ seen
         }


watchReceivedMessages :: Head LocalState -> (Stored DirectMessage -> IO ()) -> IO WatchedHead
watchReceivedMessages h f = do
    let self = finalOwner $ localIdentity $ headObject h
    watchHeadWith h (lookupSharedValue . lsShared . headObject) $ \(DirectMessageThreads sms _) -> do
        forM_ (map fromStored sms) $ \ms -> do
            mapM_ f $ filter (not . sameIdentity self . msgFrom . fromStored) $ msReceived ms

formatMessage :: TimeZone -> DirectMessage -> String
formatMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ idName $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

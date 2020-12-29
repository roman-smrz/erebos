module Message (
    DirectMessage(..),
    sendDirectMessage,

    DirectMessageThread(..),
    threadToList,
    messageThreadView,

    formatMessage,
) where

import Control.Monad.Except
import Control.Monad.Reader

import Data.List
import Data.Ord
import qualified Data.Set as S
import Data.Text (Text)
import qualified Data.Text as T
import Data.Time.Format
import Data.Time.LocalTime

import Identity
import Network
import Service
import State
import Storage
import Storage.Merge

data DirectMessage = DirectMessage
    { msgFrom :: ComposedIdentity
    , msgPrev :: [Stored DirectMessage]
    , msgTime :: ZonedTime
    , msgText :: Text
    }

instance Storable DirectMessage where
    store' msg = storeRec $ do
        mapM_ (storeRef "from") $ idDataF $ msgFrom msg
        mapM_ (storeRef "PREV") $ msgPrev msg
        storeDate "time" $ msgTime msg
        storeText "text" $ msgText msg

    load' = loadRec $ DirectMessage
        <$> loadIdentity "from"
        <*> loadRefs "PREV"
        <*> loadDate "time"
        <*> loadText "text"

instance Service DirectMessage where
    serviceID _ = mkServiceID "c702076c-4928-4415-8b6b-3e839eafcb0d"

    serviceHandler smsg = do
        let msg = fromStored smsg
        powner <- asks $ finalOwner . svcPeerIdentity
        tzone <- liftIO $ getCurrentTimeZone
        erb <- svcGetLocal
        let st = storedStorage erb
            prev = lookupSharedValue $ lsShared $ fromStored erb
            sent = findMsgProperty powner msSent prev
            received = findMsgProperty powner msReceived prev
        if powner `sameIdentity` msgFrom msg ||
               filterAncestors sent == filterAncestors (smsg : sent)
           then do
               erb' <- liftIO $ do
                   next <- wrappedStore st $ MessageState
                       { msPrev = prev
                       , msPeer = powner
                       , msSent = []
                       , msReceived = filterAncestors $ smsg : received
                       , msSeen = []
                       }
                   shared <- makeSharedStateUpdate st [next] (lsShared $ fromStored erb)
                   wrappedStore st (fromStored erb) { lsShared = [shared] }
               svcSetLocal erb'
               when (powner `sameIdentity` msgFrom msg) $ do
                   svcPrint $ formatMessage tzone msg
                   replyStoredRef smsg

           else svcPrint "Owner mismatch"


data MessageState = MessageState
    { msPrev :: [Stored MessageState]
    , msPeer :: ComposedIdentity
    , msSent :: [Stored DirectMessage]
    , msReceived :: [Stored DirectMessage]
    , msSeen :: [Stored DirectMessage]
    }

instance Storable MessageState where
    store' ms = storeRec $ do
        mapM_ (storeRef "PREV") $ msPrev ms
        mapM_ (storeRef "peer") $ idDataF $ msPeer ms
        mapM_ (storeRef "sent") $ msSent ms
        mapM_ (storeRef "received") $ msReceived ms
        mapM_ (storeRef "seen") $ msSeen ms

    load' = loadRec $ MessageState
        <$> loadRefs "PREV"
        <*> loadIdentity "peer"
        <*> loadRefs "sent"
        <*> loadRefs "received"
        <*> loadRefs "seen"

instance SharedType MessageState where
    sharedTypeID _ = mkSharedTypeID "ee793681-5976-466a-b0f0-4e1907d3fade"

findMsgProperty :: Foldable m => Identity m -> (MessageState -> [a]) -> [Stored MessageState] -> [a]
findMsgProperty pid sel mss = concat $ flip findProperty mss $ \x -> do
    guard $ msPeer x `sameIdentity` pid
    guard $ not $ null $ sel x
    return $ sel x


sendDirectMessage :: (MonadIO m, MonadError String m) => Head LocalState -> Peer -> Text -> m (Stored DirectMessage)
sendDirectMessage h peer text = do
    pid <- peerIdentity peer >>= \case PeerIdentityFull pid -> return pid
                                       _ -> throwError "incomplete peer identity"
    let st = refStorage $ headRef h
        self = headLocalIdentity h
        powner = finalOwner pid

    smsg <- liftIO $ updateSharedState h $ \prev -> do
        let sent = findMsgProperty powner msSent prev
            received = findMsgProperty powner msReceived prev

        time <- getZonedTime
        smsg <- wrappedStore st DirectMessage
            { msgFrom = toComposedIdentity $ finalOwner self
            , msgPrev = filterAncestors $ sent ++ received
            , msgTime = time
            , msgText = text
            }
        next <- wrappedStore st $ MessageState
            { msPrev = prev
            , msPeer = powner
            , msSent = [smsg]
            , msReceived = []
            , msSeen = []
            }
        return ([next], smsg)

    sendToPeerStored self peer smsg
    return smsg


data DirectMessageThread = DirectMessageThread
    { msgPeer :: ComposedIdentity
    , msgHead :: [Stored DirectMessage]
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
                          sent = findMsgProperty peer msSent mss
                          received = findMsgProperty peer msReceived mss
                          seen = findMsgProperty peer msSeen mss

                       in DirectMessageThread
                              { msgPeer = peer
                              , msgHead = filterAncestors $ sent ++ received
                              , msgSeen = filterAncestors $ sent ++ seen
                              } : helper (peer : used) (msPrev (fromStored sms) ++ rest)
              _ -> []


formatMessage :: TimeZone -> DirectMessage -> String
formatMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ idName $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

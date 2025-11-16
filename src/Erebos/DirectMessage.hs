module Erebos.DirectMessage (
    DirectMessage(..),
    sendDirectMessage,
    dmMarkAsSeen,
    updateDirectMessagePeer,
    createOrUpdateDirectMessagePeer,

    DirectMessageAttributes(..),
    defaultDirectMessageAttributes,

    DirectMessageThreads,
    dmThreadList,

    DirectMessageThread(..),
    dmThreadToList, dmThreadToListSince, dmThreadToListUnread, dmThreadToListSinceUnread,
    dmThreadView,

    watchDirectMessageThreads,
    formatDirectMessage,
) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.List
import Data.Ord
import Data.Set (Set)
import Data.Set qualified as S
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Format
import Data.Time.LocalTime

import Erebos.Conversation.Class
import Erebos.Discovery
import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.Service
import Erebos.State
import Erebos.Storable
import Erebos.Storage.Head
import Erebos.Storage.Merge


instance ConversationType DirectMessageThread DirectMessage where
    convMessageFrom = msgFrom
    convMessageTime = msgTime
    convMessageText = Just . msgText


data DirectMessage = DirectMessage
    { msgFrom :: ComposedIdentity
    , msgPrev :: [ Stored DirectMessage ]
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
        let DirectMessageThreads prev _ = lookupSharedValue $ lsShared $ fromStored erb
            sent = findMsgProperty powner msSent prev
            received = findMsgProperty powner msReceived prev
            received' = filterAncestors $ smsg : received
        if powner `sameIdentity` msgFrom msg ||
               filterAncestors sent == filterAncestors (smsg : sent)
           then do
               when (received' /= received) $ do
                   next <- mstore MessageState
                       { msPrev = prev
                       , msPeer = powner
                       , msReady = []
                       , msSent = []
                       , msReceived = received'
                       , msSeen = []
                       }
                   let threads = DirectMessageThreads [ next ] (dmThreadView [ next ])
                   shared <- makeSharedStateUpdate threads (lsShared $ fromStored erb)
                   svcSetLocal =<< mstore (fromStored erb) { lsShared = [ shared ] }

               when (powner `sameIdentity` msgFrom msg) $ do
                   replyStoredRef smsg

           else join $ asks $ dmOwnerMismatch . svcAttributes

    serviceNewPeer = do
        syncDirectMessageToPeer . lookupSharedValue . lsShared . fromStored =<< svcGetLocal

    serviceUpdatedPeer = do
        updateDirectMessagePeer . finalOwner =<< asks svcPeerIdentity

    serviceStorageWatchers _ =
        [ SomeStorageWatcher (lookupSharedValue . lsShared . fromStored) syncDirectMessageToPeer
        , GlobalStorageWatcher (lookupSharedValue . lsShared . fromStored) findMissingPeers
        ]


data MessageState = MessageState
    { msPrev :: [ Stored MessageState ]
    , msPeer :: ComposedIdentity
    , msReady :: [ Stored DirectMessage ]
    , msSent :: [ Stored DirectMessage ]
    , msReceived :: [ Stored DirectMessage ]
    , msSeen :: [ Stored DirectMessage ]
    }

data DirectMessageThreads = DirectMessageThreads [ Stored MessageState ] [ DirectMessageThread ]

instance Eq DirectMessageThreads where
    DirectMessageThreads mss _ == DirectMessageThreads mss' _ = mss == mss'

dmThreadList :: DirectMessageThreads -> [ DirectMessageThread ]
dmThreadList (DirectMessageThreads _ threads) = threads

instance Storable MessageState where
    store' MessageState {..} = storeRec $ do
        mapM_ (storeRef "PREV") msPrev
        mapM_ (storeRef "peer") $ idExtDataF msPeer
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
    mergeSorted mss = DirectMessageThreads mss (dmThreadView mss)
    toComponents (DirectMessageThreads mss _) = mss

instance SharedType DirectMessageThreads where
    sharedTypeID _ = mkSharedTypeID "ee793681-5976-466a-b0f0-4e1907d3fade"

findMsgProperty :: Foldable m => Identity m -> (MessageState -> [ a ]) -> [ Stored MessageState ] -> [ a ]
findMsgProperty pid sel mss = concat $ flip findProperty mss $ \x -> do
    guard $ msPeer x `sameIdentity` pid
    guard $ not $ null $ sel x
    return $ sel x


sendDirectMessage :: (Foldable f, Applicative f, MonadHead LocalState m)
                  => Identity f -> Text -> m ()
sendDirectMessage pid text = updateLocalState_ $ \ls -> do
    let self = localIdentity $ fromStored ls
        powner = finalOwner pid
    flip updateSharedState_ ls $ \(DirectMessageThreads prev _) -> do
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
            , msReady = [ smsg ]
            , msSent = []
            , msReceived = []
            , msSeen = []
            }
        return $ DirectMessageThreads [ next ] (dmThreadView [ next ])

dmMarkAsSeen
    :: (Foldable f, Applicative f, MonadHead LocalState m)
    => Identity f -> m ()
dmMarkAsSeen pid = do
    updateLocalState_ $ updateSharedState_ $ \(DirectMessageThreads prev _) -> do
        let powner = finalOwner pid
            received = findMsgProperty powner msReceived prev
        next <- mstore MessageState
            { msPrev = prev
            , msPeer = powner
            , msReady = []
            , msSent = []
            , msReceived = []
            , msSeen = received
            }
        return $ DirectMessageThreads [ next ] (dmThreadView [ next ])

updateDirectMessagePeer
    :: (Foldable f, Applicative f, MonadHead LocalState m)
    => Identity f -> m ()
updateDirectMessagePeer = createOrUpdateDirectMessagePeer' False

createOrUpdateDirectMessagePeer
    :: (Foldable f, Applicative f, MonadHead LocalState m)
    => Identity f -> m ()
createOrUpdateDirectMessagePeer = createOrUpdateDirectMessagePeer' True

createOrUpdateDirectMessagePeer'
    :: (Foldable f, Applicative f, MonadHead LocalState m)
    => Bool -> Identity f -> m ()
createOrUpdateDirectMessagePeer' create pid = do
    let powner = finalOwner pid
    updateLocalState_ $ updateSharedState_ $ \old@(DirectMessageThreads prev threads) -> do
        let updatePeerThread = do
                next <- mstore MessageState
                    { msPrev = prev
                    , msPeer = powner
                    , msReady = []
                    , msSent = []
                    , msReceived = []
                    , msSeen = []
                    }
                return $ DirectMessageThreads [ next ] (dmThreadView [ next ])
        case find (sameIdentity powner . msgPeer) threads of
            Nothing
                | create
                -> updatePeerThread

            Just thread
                | oldPeer <- msgPeer thread
                , newPeer <- updateIdentity (idExtDataF powner) oldPeer
                , oldPeer /= newPeer
                -> updatePeerThread

            _ -> return old


syncDirectMessageToPeer :: DirectMessageThreads -> ServiceHandler DirectMessage ()
syncDirectMessageToPeer (DirectMessageThreads mss _) = do
    pid <- finalOwner <$> asks svcPeerIdentity
    peer <- asks svcPeer
    let thread = messageThreadFor pid mss
    mapM_ (sendToPeerStored peer) $ msgHead thread
    updateLocalState_ $ \ls -> do
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
                return $ DirectMessageThreads [ next ] (dmThreadView [ next ])
              else do
                return unchanged

findMissingPeers :: Server -> DirectMessageThreads -> ExceptT ErebosError IO ()
findMissingPeers server threads = do
    forM_ (dmThreadList threads) $ \thread -> do
        when (msgHead thread /= msgReceived thread) $ do
            mapM_ (discoverySearch server) $ map (refDigest . storedRef) $ idDataF $ msgPeer thread


data DirectMessageThread = DirectMessageThread
    { msgPeer :: ComposedIdentity
    , msgHead :: [ Stored DirectMessage ]
    , msgSent :: [ Stored DirectMessage ]
    , msgSeen :: [ Stored DirectMessage ]
    , msgReceived :: [ Stored DirectMessage ]
    }

dmThreadToList :: DirectMessageThread -> [ DirectMessage ]
dmThreadToList thread = map fst $ threadToListHelper (msgSeen thread) S.empty $ msgHead thread

dmThreadToListSince :: DirectMessageThread -> DirectMessageThread -> [ DirectMessage ]
dmThreadToListSince since thread = map fst $ threadToListHelper (msgSeen thread) (S.fromAscList $ msgHead since) (msgHead thread)

dmThreadToListUnread :: DirectMessageThread -> [ ( DirectMessage, Bool ) ]
dmThreadToListUnread thread = threadToListHelper (msgSeen thread) S.empty $ msgHead thread

dmThreadToListSinceUnread :: DirectMessageThread -> DirectMessageThread -> [ ( DirectMessage, Bool ) ]
dmThreadToListSinceUnread since thread = threadToListHelper (msgSeen thread) (S.fromAscList $ msgHead since) (msgHead thread)

threadToListHelper :: [ Stored DirectMessage ] -> Set (Stored DirectMessage) -> [ Stored DirectMessage ] -> [ ( DirectMessage, Bool ) ]
threadToListHelper seen used msgs
    | msg : msgs' <- filter (`S.notMember` used) $ reverse $ sortBy (comparing cmpView) msgs =
        ( fromStored msg, not $ any (msg `precedesOrEquals`) seen ) : threadToListHelper seen (S.insert msg used) (msgs' ++ msgPrev (fromStored msg))
    | otherwise = []
  where
    cmpView msg = (zonedTimeToUTC $ msgTime $ fromStored msg, msg)

dmThreadView :: [ Stored MessageState ] -> [ DirectMessageThread ]
dmThreadView = helper []
    where helper used ms' = case filterAncestors ms' of
              mss@(sms : rest)
                  | any (sameIdentity $ msPeer $ fromStored sms) used ->
                      helper used $ msPrev (fromStored sms) ++ rest
                  | otherwise ->
                      let peer = msPeer $ fromStored sms
                       in messageThreadFor peer mss : helper (peer : used) (msPrev (fromStored sms) ++ rest)
              _ -> []

messageThreadFor :: ComposedIdentity -> [ Stored MessageState ] -> DirectMessageThread
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
         , msgReceived = filterAncestors $ received
         }


watchDirectMessageThreads :: Head LocalState -> (DirectMessageThread -> DirectMessageThread -> IO ()) -> IO WatchedHead
watchDirectMessageThreads h f = do
    prevVar <- newMVar Nothing
    watchHeadWith h (lookupSharedValue . lsShared . headObject) $ \(DirectMessageThreads sms _) -> do
        modifyMVar_ prevVar $ \case
            Just prev -> do
                let addPeer (p : ps) p'
                        | p `sameIdentity` p' = p : ps
                        | otherwise = p : addPeer ps p'
                    addPeer [] p' = [ p' ]

                let peers = foldl' addPeer [] $ map (msPeer . fromStored) $ storedDifference prev sms
                forM_ peers $ \peer -> do
                    f (messageThreadFor peer prev) (messageThreadFor peer sms)
                return (Just sms)

            Nothing -> do
                return (Just sms)

formatDirectMessage :: TimeZone -> DirectMessage -> String
formatDirectMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ idName $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

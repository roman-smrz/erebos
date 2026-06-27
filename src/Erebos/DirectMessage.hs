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

    DirectMessageThread(msgPeer, msgHead, msgSent, msgSeen, msgReceived),
    dmEmptyThread,
    dmThreadToList, dmThreadToListSince, dmThreadToListUnread, dmThreadToListSinceUnread,
    dmThreadToListChange,
    dmThreadView,

    watchDirectMessageThreads,
    formatDirectMessage,
) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.List
import Data.Maybe
import Data.Ord
import Data.Proxy
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
import Erebos.Storage.Graph
import Erebos.Storage.Head
import Erebos.Storage.Merge


instance ConversationType DirectMessageThread DirectMessage where
    convMessageFrom = msgFrom
    convMessageTime = msgTime
    convMessageText = Just . msgText

    convReference = refDigest . storedRef . head . idDataF . msgPeer

    convMessageListSince Nothing      thread = ( 0, ) $ dmThreadToListUnread thread
    convMessageListSince (Just since) thread = dmThreadToListChange since thread

    convMarkAllSeen DirectMessageThread {..} = dmMarkAsSeen msgPeer


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

data DirectMessagePeerState = DirectMessagePeerState
    { dmpsLastThread :: Maybe DirectMessageThread
    }

data DirectMessageGlobalState = DirectMessageGlobalState
    { dmgsLastState :: Maybe [ Stored MessageState ]
    }

instance Service DirectMessage where
    serviceID _ = mkServiceID "c702076c-4928-4415-8b6b-3e839eafcb0d"

    type ServiceAttributes DirectMessage = DirectMessageAttributes
    defaultServiceAttributes _ = defaultDirectMessageAttributes

    type ServiceState DirectMessage = DirectMessagePeerState
    emptyServiceState _ = DirectMessagePeerState
        { dmpsLastThread = Nothing
        }

    type ServiceGlobalState DirectMessage = DirectMessageGlobalState
    emptyServiceGlobalState _ = DirectMessageGlobalState
        { dmgsLastState = Nothing
        }

    serviceHandler smsg = do
        let msg = fromStored smsg
        powner <- asks $ finalOwner . svcPeerIdentity
        erb <- svcGetLocal
        let DirectMessageThreads prev _ = lookupSharedValue $ lsShared $ fromStored erb
            sent = concat $ propertyValue $ findMsgProperty powner msSent prev
            received = concat $ propertyValue $ findMsgProperty powner msReceived prev
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

msgPropertySelector :: Foldable m => Identity m -> (MessageState -> [ a ]) -> MessageState -> Maybe [ a ]
msgPropertySelector pid sel x = do
    guard $ msPeer x `sameIdentity` pid
    guard $ not $ null $ sel x
    return $ sel x

findMsgProperty :: Foldable m => Identity m -> (MessageState -> [ a ]) -> [ Stored MessageState ] -> Property MessageState [ a ]
findMsgProperty pid sel mss = flip findProperty' mss $ msgPropertySelector pid sel

findMsgPropertyUpdate :: Property MessageState [ a ] -> [ Stored MessageState ] -> Property MessageState [ a ]
findMsgPropertyUpdate prev mss = findPropertyUpdate prev mss


sendDirectMessage :: (Foldable f, Applicative f, MonadHead LocalState m)
                  => Identity f -> Text -> m ()
sendDirectMessage pid text = updateLocalState_ $ \ls -> do
    let self = localIdentity $ fromStored ls
        powner = finalOwner pid
    flip updateSharedState_ ls $ \(DirectMessageThreads prev _) -> do
        let ready = concat $ propertyValue $ findMsgProperty powner msReady prev
            received = concat $ propertyValue $ findMsgProperty powner msReceived prev

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
            received = concat $ propertyValue $ findMsgProperty powner msReceived prev
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
    pthread <- fromMaybe (dmEmptyThread pid) . dmpsLastThread <$> svcGet
    let thread = messageThreadFor pthread mss
    mapM_ (sendToPeerStored peer) $ msgHead thread
    when (msgHead thread /= msgSent thread) $ do
        updateLocalState_ $ \ls -> do
            let powner = finalOwner pid
            flip updateSharedState_ ls $ \_ -> do
                next <- mstore MessageState
                    { msPrev = mss
                    , msPeer = powner
                    , msReady = []
                    , msSent = msgHead thread
                    , msReceived = []
                    , msSeen = []
                    }
                return $ DirectMessageThreads [ next ] (dmThreadView [ next ])
    svcModify $ \s -> s
        { dmpsLastThread = Just thread
        }

findMissingPeers :: Server -> DirectMessageThreads -> ExceptT ErebosError IO ()
findMissingPeers server (DirectMessageThreads states threads) = do
    prev <- modifyServiceGlobalState server (Proxy @DirectMessage) $ \gs ->
        ( gs { dmgsLastState = Just states }, dmgsLastState gs )
    let diffPeers = map (msPeer . fromStored) . storedDifference states <$> prev

    forM_ (takeWhile (\t -> maybe True (any (sameIdentity $ msgPeer t)) diffPeers) threads) $ \thread -> do
        when (msgHead thread /= msgReceived thread) $ do
            mapM_ (discoverySearch server) $ map (refDigest . storedRef) $ idDataF $ msgPeer thread


data DirectMessageThread = DirectMessageThread
    { msgPeer :: ComposedIdentity
    , msgHead :: [ Stored DirectMessage ]
    , msgSent :: [ Stored DirectMessage ]
    , msgSeen :: [ Stored DirectMessage ]
    , msgReceived :: [ Stored DirectMessage ]
    , msgPropReady :: Property MessageState [ Stored DirectMessage ]
    , msgPropSent :: Property MessageState [ Stored DirectMessage ]
    , msgPropReceived :: Property MessageState [ Stored DirectMessage ]
    , msgPropSeen :: Property MessageState [ Stored DirectMessage ]
    }

dmEmptyThread :: ComposedIdentity -> DirectMessageThread
dmEmptyThread peer = DirectMessageThread
    { msgPeer = peer
    , msgHead = []
    , msgSent = []
    , msgSeen = []
    , msgReceived = []
    , msgPropReady = emptyProperty $ msgPropertySelector peer msReady
    , msgPropSent = emptyProperty $ msgPropertySelector peer msSent
    , msgPropReceived = emptyProperty $ msgPropertySelector peer msReceived
    , msgPropSeen = emptyProperty $ msgPropertySelector peer msSeen
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
threadToListHelper seen used msgs = map (\msg -> ( fromStored msg, isNew msg )) $ graphToList cmp $ graphRemoveTips (S.toList used) $ graphFromTips msgs
  where
    cmp = comparing $ zonedTimeToUTC . msgTime . fromStored
    isNew msg = not $ any (msg `precedesOrEquals`) seen

dmThreadToListChange :: DirectMessageThread -> DirectMessageThread -> ( Int, [ ( DirectMessage, Bool ) ] )
dmThreadToListChange since thread =
    ( graphSize $ graphRemoveTips bottom $ graphFromTips (msgHead since)
    , map (\msg -> ( fromStored msg, isNew msg )) $ graphToList cmp $ graphRemoveTips bottom $ graphFromTips (msgHead thread)
    )
  where
    cmp = comparing $ zonedTimeToUTC . msgTime . fromStored
    isNew msg = not $ any (msg `precedesOrEquals`) (msgSeen thread)
    bottom
        | msgSeen since == msgSeen thread = msgHead since
        | otherwise = commonAncestors (msgHead since) (msgSeen since)

dmThreadView :: [ Stored MessageState ] -> [ DirectMessageThread ]
dmThreadView = helper []
    where helper used ms' = case filterAncestors ms' of
              mss@(sms : rest)
                  | any (sameIdentity $ msPeer $ fromStored sms) used ->
                      helper used $ msPrev (fromStored sms) ++ rest
                  | otherwise ->
                      let peer = msPeer $ fromStored sms
                       in messageThreadFor (dmEmptyThread peer) mss : helper (peer : used) (msPrev (fromStored sms) ++ rest)
              _ -> []

messageThreadFor :: DirectMessageThread -> [ Stored MessageState ] -> DirectMessageThread
messageThreadFor pthread mss =
    let readyProp = findMsgPropertyUpdate (msgPropReady pthread) mss
        ready = concat $ propertyValue readyProp
        sentProp = findMsgPropertyUpdate (msgPropSent pthread) mss
        sent = concat $ propertyValue sentProp
        receivedProp = findMsgPropertyUpdate (msgPropReceived pthread) mss
        received = concat $ propertyValue receivedProp
        seenProp = findMsgPropertyUpdate (msgPropSeen pthread) mss
        seen = concat $ propertyValue seenProp

     in DirectMessageThread
         { msgPeer = msgPeer pthread
         , msgHead = filterAncestors $ ready ++ received
         , msgSent = filterAncestors $ sent ++ received
         , msgSeen = filterAncestors $ ready ++ seen
         , msgReceived = filterAncestors $ received
         , msgPropReady = readyProp
         , msgPropSent = sentProp
         , msgPropReceived = receivedProp
         , msgPropSeen = seenProp
         }


watchDirectMessageThreads :: Head LocalState -> (DirectMessageThread -> DirectMessageThread -> IO ()) -> IO WatchedHead
watchDirectMessageThreads h callback = do
    prevVar <- newMVar Nothing
    watchHeadWith h (lookupSharedValue . lsShared . headObject) $ \(DirectMessageThreads sms _) -> do
        modifyMVar_ prevVar $ \case
            Just ( prev, prevPeers ) -> do
                let addPeer (p : ps) p'
                        | p `sameIdentity` p' = p : ps
                        | otherwise = p : addPeer ps p'
                    addPeer [] p' = [ p' ]
                let changedPeers = foldl' addPeer [] $ map (msPeer . fromStored) $ storedDifference prev sms

                let updatePeer (px@( p, x ) : ps) p' f
                        | p `sameIdentity` p' = let x' = f x in ( ( x, x' ), ( p', x' ) : ps )
                        | otherwise = (px :) <$> updatePeer ps p' f
                    updatePeer [] p' f =
                        let x = messageThreadFor (dmEmptyThread p') prev; x' = f x
                         in ( ( x, x' ), [ ( p', x' ) ] )

                peers <- (\f -> foldM f prevPeers changedPeers) $ \peers peer -> do
                    let ( ( t, t' ), peers' ) = updatePeer peers peer $ \pt ->
                            messageThreadFor pt sms
                    callback t t'
                    return peers'
                return (Just ( sms, peers ))

            Nothing -> do
                return (Just ( sms, [] ))


formatDirectMessage :: TimeZone -> DirectMessage -> String
formatDirectMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ idName $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

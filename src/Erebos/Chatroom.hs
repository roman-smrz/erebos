module Erebos.Chatroom (
    Chatroom(..),
    ChatroomData(..),
    validateChatroom,

    ChatroomState(..),
    ChatroomStateData(..),
    createChatroom,
    updateChatroomByStateData,
    listChatrooms,
    findChatroomByRoomData,
    findChatroomByStateData,
    chatroomSetSubscribe,
    chatroomMembers,
    joinChatroom, joinChatroomByStateData,
    joinChatroomAs, joinChatroomAsByStateData,
    leaveChatroom, leaveChatroomByStateData,
    getMessagesSinceState,

    ChatroomSetChange(..),
    watchChatrooms,

    ChatMessage,
    cmsgFrom, cmsgReplyTo, cmsgTime, cmsgText, cmsgLeave,
    cmsgRoom, cmsgRoomData,
    ChatMessageData(..),
    sendChatroomMessage,
    sendChatroomMessageByStateData,

    ChatroomService(..),
) where

import Control.Arrow
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class

import Data.Bool
import Data.Either
import Data.Foldable
import Data.Function
import Data.IORef
import Data.List
import Data.Maybe
import Data.Monoid
import Data.Ord
import Data.Set qualified as S
import Data.Text (Text)
import Data.Time

import Erebos.Identity
import Erebos.Object.Internal
import Erebos.PubKey
import Erebos.Service
import Erebos.Set
import Erebos.State
import Erebos.Storage.Merge
import Erebos.Util


data ChatroomData = ChatroomData
    { rdPrev :: [Stored (Signed ChatroomData)]
    , rdName :: Maybe Text
    , rdDescription :: Maybe Text
    , rdKey :: Stored PublicKey
    }

data Chatroom = Chatroom
    { roomData :: [Stored (Signed ChatroomData)]
    , roomName :: Maybe Text
    , roomDescription :: Maybe Text
    , roomKey :: Stored PublicKey
    }

instance Storable ChatroomData where
    store' ChatroomData {..} = storeRec $ do
        mapM_ (storeRef "SPREV") rdPrev
        storeMbText "name" rdName
        storeMbText "description" rdDescription
        storeRef "key" rdKey

    load' = loadRec $ do
        rdPrev <- loadRefs "SPREV"
        rdName <- loadMbText "name"
        rdDescription <- loadMbText "description"
        rdKey <- loadRef "key"
        return ChatroomData {..}

validateChatroom :: [Stored (Signed ChatroomData)] -> Except String Chatroom
validateChatroom roomData = do
    when (null roomData) $ throwError "null data"
    when (not $ getAll $ walkAncestors verifySignatures roomData) $ do
        throwError "signature verification failed"

    let roomName = findPropertyFirst (rdName . fromStored . signedData) roomData
        roomDescription = findPropertyFirst (rdDescription . fromStored . signedData) roomData
    roomKey <- maybe (throwError "missing key") return $
        findPropertyFirst (Just . rdKey . fromStored . signedData) roomData
    return Chatroom {..}
  where
    verifySignatures sdata =
        let rdata = fromSigned sdata
            required = concat
                [ [ rdKey rdata ]
                , map (rdKey . fromSigned) $ rdPrev rdata
                ]
         in All $ all (fromStored sdata `isSignedBy`) required


data ChatMessageData = ChatMessageData
    { mdPrev :: [Stored (Signed ChatMessageData)]
    , mdRoom :: [Stored (Signed ChatroomData)]
    , mdFrom :: ComposedIdentity
    , mdReplyTo :: Maybe (Stored (Signed ChatMessageData))
    , mdTime :: ZonedTime
    , mdText :: Maybe Text
    , mdLeave :: Bool
    }

data ChatMessage = ChatMessage
    { cmsgData :: Stored (Signed ChatMessageData)
    }

validateSingleMessage :: Stored (Signed ChatMessageData) -> Maybe ChatMessage
validateSingleMessage sdata = do
    guard $ fromStored sdata `isSignedBy` idKeyMessage (mdFrom (fromSigned sdata))
    return $ ChatMessage sdata

cmsgFrom :: ChatMessage -> ComposedIdentity
cmsgFrom = mdFrom . fromSigned . cmsgData

cmsgReplyTo :: ChatMessage -> Maybe ChatMessage
cmsgReplyTo = fmap ChatMessage . mdReplyTo . fromSigned . cmsgData

cmsgTime :: ChatMessage -> ZonedTime
cmsgTime = mdTime . fromSigned . cmsgData

cmsgText :: ChatMessage -> Maybe Text
cmsgText = mdText . fromSigned . cmsgData

cmsgLeave :: ChatMessage -> Bool
cmsgLeave = mdLeave . fromSigned . cmsgData

cmsgRoom :: ChatMessage -> Maybe Chatroom
cmsgRoom = either (const Nothing) Just . runExcept . validateChatroom . cmsgRoomData

cmsgRoomData :: ChatMessage -> [ Stored (Signed ChatroomData) ]
cmsgRoomData = concat . findProperty ((\case [] -> Nothing; xs -> Just xs) . mdRoom . fromStored . signedData) . (: []) . cmsgData

instance Storable ChatMessageData where
    store' ChatMessageData {..} = storeRec $ do
        mapM_ (storeRef "SPREV") mdPrev
        mapM_ (storeRef "room") mdRoom
        mapM_ (storeRef "from") $ idExtDataF mdFrom
        storeMbRef "reply-to" mdReplyTo
        storeDate "time" mdTime
        storeMbText "text" mdText
        when mdLeave $ storeEmpty "leave"

    load' = loadRec $ do
        mdPrev <- loadRefs "SPREV"
        mdRoom <- loadRefs "room"
        mdFrom <- loadIdentity "from"
        mdReplyTo <- loadMbRef "reply-to"
        mdTime <- loadDate "time"
        mdText <- loadMbText "text"
        mdLeave <- isJust <$> loadMbEmpty "leave"
        return ChatMessageData {..}

threadToListSince :: [ Stored (Signed ChatMessageData) ] -> [ Stored (Signed ChatMessageData) ] -> [ ChatMessage ]
threadToListSince since thread = helper (S.fromList since) thread
  where
    helper :: S.Set (Stored (Signed ChatMessageData)) -> [Stored (Signed ChatMessageData)] -> [ChatMessage]
    helper seen msgs
        | msg : msgs' <- filter (`S.notMember` seen) $ reverse $ sortBy (comparing cmpView) msgs =
            maybe id (:) (validateSingleMessage msg) $
               helper (S.insert msg seen) (msgs' ++ mdPrev (fromSigned msg))
        | otherwise = []
    cmpView msg = (zonedTimeToUTC $ mdTime $ fromSigned msg, msg)

sendChatroomMessage
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => ChatroomState -> Text -> m ()
sendChatroomMessage rstate msg = sendChatroomMessageByStateData (head $ roomStateData rstate) msg

sendChatroomMessageByStateData
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => Stored ChatroomStateData -> Text -> m ()
sendChatroomMessageByStateData lookupData msg = sendRawChatroomMessageByStateData lookupData Nothing Nothing (Just msg) False

sendRawChatroomMessageByStateData
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => Stored ChatroomStateData -> Maybe UnifiedIdentity -> Maybe (Stored (Signed ChatMessageData)) -> Maybe Text -> Bool -> m ()
sendRawChatroomMessageByStateData lookupData mbIdentity mdReplyTo mdText mdLeave = void $ findAndUpdateChatroomState $ \cstate -> do
    guard $ any (lookupData `precedesOrEquals`) $ roomStateData cstate
    Just $ do
        mdFrom <- finalOwner <$> if
            | Just identity <- mbIdentity -> return identity
            | Just identity <- roomStateIdentity cstate -> return identity
            | otherwise -> localIdentity . fromStored <$> getLocalHead
        secret <- loadKey $ idKeyMessage mdFrom
        mdTime <- liftIO getZonedTime
        let mdPrev = roomStateMessageData cstate
            mdRoom = if null (roomStateMessageData cstate)
                        then maybe [] roomData (roomStateRoom cstate)
                        else []

        mdata <- mstore =<< sign secret =<< mstore ChatMessageData {..}
        mergeSorted . (:[]) <$> mstore ChatroomStateData
            { rsdPrev = roomStateData cstate
            , rsdRoom = []
            , rsdSubscribe = Just (not mdLeave)
            , rsdIdentity = mbIdentity
            , rsdMessages = [ mdata ]
            }


data ChatroomStateData = ChatroomStateData
    { rsdPrev :: [Stored ChatroomStateData]
    , rsdRoom :: [Stored (Signed ChatroomData)]
    , rsdSubscribe :: Maybe Bool
    , rsdIdentity :: Maybe UnifiedIdentity
    , rsdMessages :: [Stored (Signed ChatMessageData)]
    }

data ChatroomState = ChatroomState
    { roomStateData :: [Stored ChatroomStateData]
    , roomStateRoom :: Maybe Chatroom
    , roomStateMessageData :: [Stored (Signed ChatMessageData)]
    , roomStateSubscribe :: Bool
    , roomStateIdentity :: Maybe UnifiedIdentity
    , roomStateMessages :: [ChatMessage]
    }

instance Storable ChatroomStateData where
    store' ChatroomStateData {..} = storeRec $ do
        forM_ rsdPrev $ storeRef "PREV"
        forM_ rsdRoom $ storeRef "room"
        forM_ rsdSubscribe $ storeInt "subscribe" . bool @Int 0 1
        forM_ rsdIdentity $ storeRef "id" . idExtData
        forM_ rsdMessages $ storeRef "msg"

    load' = loadRec $ do
        rsdPrev <- loadRefs "PREV"
        rsdRoom <- loadRefs "room"
        rsdSubscribe <- fmap ((/=) @Int 0) <$> loadMbInt "subscribe"
        rsdIdentity <- loadMbUnifiedIdentity "id"
        rsdMessages <- loadRefs "msg"
        return ChatroomStateData {..}

instance Mergeable ChatroomState where
    type Component ChatroomState = ChatroomStateData

    mergeSorted roomStateData =
        let roomStateRoom = either (const Nothing) Just $ runExcept $
                validateChatroom $ concat $ findProperty ((\case [] -> Nothing; xs -> Just xs) . rsdRoom) roomStateData
            roomStateMessageData = filterAncestors $ concat $ flip findProperty roomStateData $ \case
                ChatroomStateData {..} | null rsdMessages -> Nothing
                                       | otherwise        -> Just rsdMessages
            roomStateSubscribe = fromMaybe False $ findPropertyFirst rsdSubscribe roomStateData
            roomStateIdentity = findPropertyFirst rsdIdentity roomStateData
            roomStateMessages = threadToListSince [] $ concatMap (rsdMessages . fromStored) roomStateData
         in ChatroomState {..}

    toComponents = roomStateData

instance SharedType (Set ChatroomState) where
    sharedTypeID _ = mkSharedTypeID "7bc71cbf-bc43-42b1-b413-d3a2c9a2aae0"

createChatroom :: (MonadStorage m, MonadHead LocalState m, MonadIO m, MonadError String m) => Maybe Text -> Maybe Text -> m ChatroomState
createChatroom rdName rdDescription = do
    (secret, rdKey) <- liftIO . generateKeys =<< getStorage
    let rdPrev = []
    rdata <- mstore =<< sign secret =<< mstore ChatroomData {..}
    cstate <- mergeSorted . (:[]) <$> mstore ChatroomStateData
        { rsdPrev = []
        , rsdRoom = [ rdata ]
        , rsdSubscribe = Just True
        , rsdIdentity = Nothing
        , rsdMessages = []
        }

    updateLocalHead $ updateSharedState $ \rooms -> do
        st <- getStorage
        (, cstate) <$> storeSetAdd st cstate rooms

findAndUpdateChatroomState
    :: (MonadStorage m, MonadHead LocalState m)
    => (ChatroomState -> Maybe (m ChatroomState))
    -> m (Maybe ChatroomState)
findAndUpdateChatroomState f = do
    updateLocalHead $ updateSharedState $ \roomSet -> do
        let roomList = fromSetBy (comparing $ roomName <=< roomStateRoom) roomSet
        case catMaybes $ map (\x -> (x,) <$> f x) roomList of
            ((orig, act) : _) -> do
                upd <- act
                if roomStateData orig /= roomStateData upd
                  then do
                    st <- getStorage
                    roomSet' <- storeSetAdd st upd roomSet
                    return (roomSet', Just upd)
                  else do
                    return (roomSet, Just upd)
            [] -> return (roomSet, Nothing)

updateChatroomByStateData
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => Stored ChatroomStateData
    -> Maybe Text
    -> Maybe Text
    -> m (Maybe ChatroomState)
updateChatroomByStateData lookupData newName newDesc = findAndUpdateChatroomState $ \cstate -> do
    guard $ any (lookupData `precedesOrEquals`) $ roomStateData cstate
    room <- roomStateRoom cstate
    Just $ do
        secret <- loadKey $ roomKey room
        rdata <- mstore =<< sign secret =<< mstore ChatroomData
            { rdPrev = roomData room
            , rdName = newName
            , rdDescription = newDesc
            , rdKey = roomKey room
            }
        mergeSorted . (:[]) <$> mstore ChatroomStateData
            { rsdPrev = roomStateData cstate
            , rsdRoom = [ rdata ]
            , rsdSubscribe = Just True
            , rsdIdentity = Nothing
            , rsdMessages = []
            }


listChatrooms :: MonadHead LocalState m => m [ChatroomState]
listChatrooms = fromSetBy (comparing $ roomName <=< roomStateRoom) .
    lookupSharedValue . lsShared . fromStored <$> getLocalHead

findChatroom :: MonadHead LocalState m => (ChatroomState -> Bool) -> m (Maybe ChatroomState)
findChatroom p = do
    list <- map snd . chatroomSetToList . lookupSharedValue . lsShared . fromStored <$> getLocalHead
    return $ find p list

findChatroomByRoomData :: MonadHead LocalState m => Stored (Signed ChatroomData) -> m (Maybe ChatroomState)
findChatroomByRoomData cdata = findChatroom $
    maybe False (any (cdata `precedesOrEquals`) . roomData) . roomStateRoom

findChatroomByStateData :: MonadHead LocalState m => Stored ChatroomStateData -> m (Maybe ChatroomState)
findChatroomByStateData cdata = findChatroom $ any (cdata `precedesOrEquals`) . roomStateData

chatroomSetSubscribe
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => Stored ChatroomStateData -> Bool -> m ()
chatroomSetSubscribe lookupData subscribe = void $ findAndUpdateChatroomState $ \cstate -> do
    guard $ any (lookupData `precedesOrEquals`) $ roomStateData cstate
    Just $ do
        mergeSorted . (:[]) <$> mstore ChatroomStateData
            { rsdPrev = roomStateData cstate
            , rsdRoom = []
            , rsdSubscribe = Just subscribe
            , rsdIdentity = Nothing
            , rsdMessages = []
            }

chatroomMembers :: ChatroomState -> [ ComposedIdentity ]
chatroomMembers ChatroomState {..} =
    map (mdFrom . fromSigned . head) $
    filter (any $ not . mdLeave . fromSigned) $ -- keep only users that hasn't left
    map (filterAncestors . map snd) $ -- gather message data per each identity and filter ancestors
    groupBy ((==) `on` fst) $ -- group on identity root
    sortBy (comparing fst) $ -- sort by first root of identity data
    map (\x -> ( head . filterAncestors . concatMap storedRoots . idDataF . mdFrom . fromSigned $ x, x )) $
    toList $ ancestors $ roomStateMessageData

joinChatroom
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => ChatroomState -> m ()
joinChatroom rstate = joinChatroomByStateData (head $ roomStateData rstate)

joinChatroomByStateData
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => Stored ChatroomStateData -> m ()
joinChatroomByStateData lookupData = sendRawChatroomMessageByStateData lookupData Nothing Nothing Nothing False

joinChatroomAs
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => UnifiedIdentity -> ChatroomState -> m ()
joinChatroomAs identity rstate = joinChatroomAsByStateData identity (head $ roomStateData rstate)

joinChatroomAsByStateData
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => UnifiedIdentity -> Stored ChatroomStateData -> m ()
joinChatroomAsByStateData identity lookupData = sendRawChatroomMessageByStateData lookupData (Just identity) Nothing Nothing False

leaveChatroom
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => ChatroomState -> m ()
leaveChatroom rstate = leaveChatroomByStateData (head $ roomStateData rstate)

leaveChatroomByStateData
    :: (MonadStorage m, MonadHead LocalState m, MonadError String m)
    => Stored ChatroomStateData -> m ()
leaveChatroomByStateData lookupData = sendRawChatroomMessageByStateData lookupData Nothing Nothing Nothing True

getMessagesSinceState :: ChatroomState -> ChatroomState -> [ChatMessage]
getMessagesSinceState cur old = threadToListSince (roomStateMessageData old) (roomStateMessageData cur)


data ChatroomSetChange = AddedChatroom ChatroomState
                       | RemovedChatroom ChatroomState
                       | UpdatedChatroom ChatroomState ChatroomState

watchChatrooms :: MonadIO m => Head LocalState -> (Set ChatroomState -> Maybe [ChatroomSetChange] -> IO ()) -> m WatchedHead
watchChatrooms h f = liftIO $ do
    lastVar <- newIORef Nothing
    watchHeadWith h (lookupSharedValue . lsShared . headObject) $ \cur -> do
        let curList = chatroomSetToList cur
        mbLast <- readIORef lastVar
        writeIORef lastVar $ Just curList
        f cur $ do
            lastList <- mbLast
            return $ makeChatroomDiff lastList curList

chatroomSetToList :: Set ChatroomState -> [(Stored ChatroomStateData, ChatroomState)]
chatroomSetToList = map (cmp &&& id) . fromSetBy (comparing cmp)
  where
    cmp :: ChatroomState -> Stored ChatroomStateData
    cmp = head . filterAncestors . concatMap storedRoots . toComponents

makeChatroomDiff
    :: [(Stored ChatroomStateData, ChatroomState)]
    -> [(Stored ChatroomStateData, ChatroomState)]
    -> [ChatroomSetChange]
makeChatroomDiff (x@(cx, vx) : xs) (y@(cy, vy) : ys)
    | cx < cy = RemovedChatroom vx : makeChatroomDiff xs (y : ys)
    | cx > cy = AddedChatroom vy : makeChatroomDiff (x : xs) ys
    | roomStateData vx /= roomStateData vy = UpdatedChatroom vx vy : makeChatroomDiff xs ys
    | otherwise = makeChatroomDiff xs ys
makeChatroomDiff xs [] = map (RemovedChatroom . snd) xs
makeChatroomDiff [] ys = map (AddedChatroom . snd) ys


data ChatroomService = ChatroomService
    { chatRoomQuery :: Bool
    , chatRoomInfo :: [Stored (Signed ChatroomData)]
    , chatRoomSubscribe :: [Stored (Signed ChatroomData)]
    , chatRoomUnsubscribe :: [Stored (Signed ChatroomData)]
    , chatRoomMessage :: [Stored (Signed ChatMessageData)]
    }
    deriving (Eq)

emptyPacket :: ChatroomService
emptyPacket = ChatroomService
    { chatRoomQuery = False
    , chatRoomInfo = []
    , chatRoomSubscribe = []
    , chatRoomUnsubscribe = []
    , chatRoomMessage = []
    }

instance Storable ChatroomService where
    store' ChatroomService {..} = storeRec $ do
        when  chatRoomQuery $ storeEmpty "room-query"
        forM_ chatRoomInfo $ storeRef "room-info"
        forM_ chatRoomSubscribe $ storeRef "room-subscribe"
        forM_ chatRoomUnsubscribe $ storeRef "room-unsubscribe"
        forM_ chatRoomMessage $ storeRef "room-message"

    load' = loadRec $ do
        chatRoomQuery <- isJust <$> loadMbEmpty "room-query"
        chatRoomInfo <- loadRefs "room-info"
        chatRoomSubscribe <- loadRefs "room-subscribe"
        chatRoomUnsubscribe <- loadRefs "room-unsubscribe"
        chatRoomMessage <- loadRefs "room-message"
        return ChatroomService {..}

data PeerState = PeerState
    { psSendRoomUpdates :: Bool
    , psLastList :: [(Stored ChatroomStateData, ChatroomState)]
    , psSubscribedTo :: [ Stored (Signed ChatroomData) ] -- least root for each room
    }

instance Service ChatroomService where
    serviceID _ = mkServiceID "627657ae-3e39-468a-8381-353395ef4386"

    type ServiceState ChatroomService = PeerState
    emptyServiceState _ = PeerState
        { psSendRoomUpdates = False
        , psLastList = []
        , psSubscribedTo = []
        }

    serviceHandler spacket = do
        let ChatroomService {..} = fromStored spacket

        previouslyUpdated <- psSendRoomUpdates <$> svcGet
        svcModify $ \s -> s { psSendRoomUpdates = True }

        when (not previouslyUpdated) $ do
            syncChatroomsToPeer . lookupSharedValue . lsShared . fromStored =<< getLocalHead

        when chatRoomQuery $ do
            rooms <- listChatrooms
            replyPacket emptyPacket
                { chatRoomInfo = concatMap roomData $ catMaybes $ map roomStateRoom rooms
                }

        when (not $ null chatRoomInfo) $ do
            updateLocalHead_ $ updateSharedState_ $ \roomSet -> do
                let rooms = fromSetBy (comparing $ roomName <=< roomStateRoom) roomSet
                    upd set (roomInfo :: Stored (Signed ChatroomData)) = do
                        let currentRoots = storedRoots roomInfo
                            isCurrentRoom = any ((`intersectsSorted` currentRoots) . storedRoots) .
                                maybe [] roomData . roomStateRoom

                        let prev = concatMap roomStateData $ filter isCurrentRoom rooms
                            prevRoom = filterAncestors $ concat $ findProperty ((\case [] -> Nothing; xs -> Just xs) . rsdRoom) prev
                            room = filterAncestors $ (roomInfo : ) prevRoom

                        -- update local state only if we got roomInfo not present there
                        if roomInfo `notElem` prevRoom && roomInfo `elem` room
                          then do
                            sdata <- mstore ChatroomStateData
                                { rsdPrev = prev
                                , rsdRoom = room
                                , rsdSubscribe = Nothing
                                , rsdIdentity = Nothing
                                , rsdMessages = []
                                }
                            storeSetAddComponent sdata set
                          else return set
                foldM upd roomSet chatRoomInfo

        forM_ chatRoomSubscribe $ \subscribeData -> do
            mbRoomState <- findChatroomByRoomData subscribeData
            forM_ mbRoomState $ \roomState ->
                forM (roomStateRoom roomState) $ \room -> do
                    let leastRoot = head . filterAncestors . concatMap storedRoots . roomData $ room
                    svcModify $ \ps -> ps { psSubscribedTo = leastRoot : psSubscribedTo ps }
                    replyPacket emptyPacket
                        { chatRoomMessage = roomStateMessageData roomState
                        }

        forM_ chatRoomUnsubscribe $ \unsubscribeData -> do
            mbRoomState <- findChatroomByRoomData unsubscribeData
            forM_ (mbRoomState >>= roomStateRoom) $ \room -> do
                let leastRoot = head . filterAncestors . concatMap storedRoots . roomData $ room
                svcModify $ \ps -> ps { psSubscribedTo = filter (/= leastRoot) (psSubscribedTo ps) }

        when (not (null chatRoomMessage)) $ do
            updateLocalHead_ $ updateSharedState_ $ \roomSet -> do
                let rooms = fromSetBy (comparing $ roomName <=< roomStateRoom) roomSet
                    upd set (msgData :: Stored (Signed ChatMessageData))
                        | Just msg <- validateSingleMessage msgData = do
                            let roomInfo = cmsgRoomData msg
                                currentRoots = filterAncestors $ concatMap storedRoots roomInfo
                                isCurrentRoom = any ((`intersectsSorted` currentRoots) . storedRoots) .
                                    maybe [] roomData . roomStateRoom

                            let prevData = concatMap roomStateData $ filter isCurrentRoom rooms
                                prev = mergeSorted prevData
                                prevMessages = roomStateMessageData prev
                                messages = filterAncestors $ msgData : prevMessages

                            -- update local state only if subscribed and we got some new messages
                            if roomStateSubscribe prev && messages /= prevMessages
                              then do
                                sdata <- mstore ChatroomStateData
                                    { rsdPrev = prevData
                                    , rsdRoom = []
                                    , rsdSubscribe = Nothing
                                    , rsdIdentity = Nothing
                                    , rsdMessages = messages
                                    }
                                storeSetAddComponent sdata set
                              else return set
                        | otherwise = return set
                foldM upd roomSet chatRoomMessage

    serviceNewPeer = do
        replyPacket emptyPacket { chatRoomQuery = True }

    serviceStorageWatchers _ = (:[]) $
        SomeStorageWatcher (lookupSharedValue . lsShared . fromStored) syncChatroomsToPeer

syncChatroomsToPeer :: Set ChatroomState -> ServiceHandler ChatroomService ()
syncChatroomsToPeer set = do
    ps@PeerState {..} <- svcGet
    when psSendRoomUpdates $ do
        let curList = chatroomSetToList set
            diff = makeChatroomDiff psLastList curList

        roomUpdates <- fmap (concat . catMaybes) $
            forM diff $ return . \case
                AddedChatroom room -> roomData <$> roomStateRoom room
                RemovedChatroom {} -> Nothing
                UpdatedChatroom oldroom room
                    | roomStateData oldroom /= roomStateData room -> roomData <$> roomStateRoom room
                    | otherwise -> Nothing

        (subscribe, unsubscribe) <- fmap (partitionEithers . concat . catMaybes) $
            forM diff $ return . \case
                AddedChatroom room
                    | roomStateSubscribe room
                    -> map Left . roomData <$> roomStateRoom room
                RemovedChatroom oldroom
                    | roomStateSubscribe oldroom
                    -> map Right . roomData <$> roomStateRoom oldroom
                UpdatedChatroom oldroom room
                    | roomStateSubscribe oldroom /= roomStateSubscribe room
                    -> map (if roomStateSubscribe room then Left else Right) . roomData <$> roomStateRoom room
                _ -> Nothing

        messages <- fmap concat $ do
            let leastRootFor = head . filterAncestors . concatMap storedRoots . roomData
            forM diff $ return . \case
                AddedChatroom rstate
                    | Just room <- roomStateRoom rstate
                    , leastRootFor room `elem` psSubscribedTo
                    -> roomStateMessageData rstate
                UpdatedChatroom oldstate rstate
                    | Just room <- roomStateRoom rstate
                    , leastRootFor room `elem` psSubscribedTo
                    , roomStateMessageData oldstate /= roomStateMessageData rstate
                    -> roomStateMessageData rstate
                _ -> []

        let packet = emptyPacket
                { chatRoomInfo = roomUpdates
                , chatRoomSubscribe = subscribe
                , chatRoomUnsubscribe = unsubscribe
                , chatRoomMessage = messages
                }

        when (packet /= emptyPacket) $ do
            replyPacket packet
        svcSet $ ps { psLastList = curList }

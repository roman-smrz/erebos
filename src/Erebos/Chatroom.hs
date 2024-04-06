module Erebos.Chatroom (
    Chatroom(..),
    ChatroomData(..),
    validateChatroom,

    ChatroomState(..),
    createChatroom,
    listChatrooms,

    ChatroomSetChange(..),
    watchChatrooms,

    ChatroomService(..),
) where

import Control.Arrow
import Control.Monad
import Control.Monad.Except

import Data.IORef
import Data.Maybe
import Data.Monoid
import Data.Ord
import Data.Text (Text)

import Erebos.PubKey
import Erebos.Service
import Erebos.Set
import Erebos.State
import Erebos.Storage
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


data ChatroomStateData = ChatroomStateData
    { rsdPrev :: [Stored ChatroomStateData]
    , rsdRoom :: [Stored (Signed ChatroomData)]
    }

data ChatroomState = ChatroomState
    { roomStateData :: [Stored ChatroomStateData]
    , roomStateRoom :: Maybe Chatroom
    }

instance Storable ChatroomStateData where
    store' ChatroomStateData {..} = storeRec $ do
        forM_ rsdPrev $ storeRef "PREV"
        forM_ rsdRoom $ storeRef "room"

    load' = loadRec $ do
        rsdPrev <- loadRefs "PREV"
        rsdRoom <- loadRefs "room"
        return ChatroomStateData {..}

instance Mergeable ChatroomState where
    type Component ChatroomState = ChatroomStateData

    mergeSorted cdata = ChatroomState
        { roomStateData = cdata
        , roomStateRoom = either (const Nothing) Just $ runExcept $
            validateChatroom $ concat $ findProperty ((\case [] -> Nothing; xs -> Just xs) . rsdRoom) cdata
        }

    toComponents = roomStateData

instance SharedType (Set ChatroomState) where
    sharedTypeID _ = mkSharedTypeID "7bc71cbf-bc43-42b1-b413-d3a2c9a2aae0"

createChatroom :: (MonadStorage m, MonadHead LocalState m, MonadIO m, MonadError String m) => Maybe Text -> Maybe Text -> m Chatroom
createChatroom rdName rdDescription = do
    st <- getStorage
    (secret, rdKey) <- liftIO $ generateKeys st
    let rdPrev = []
    rdata <- wrappedStore st =<< sign secret =<< wrappedStore st ChatroomData {..}
    room <- liftEither $ runExcept $ validateChatroom [ rdata ]

    updateLocalHead_ $ updateSharedState_ $ \rooms -> do
        sdata <- wrappedStore st ChatroomStateData
            { rsdPrev = []
            , rsdRoom = [ rdata ]
            }
        storeSetAdd st (mergeSorted @ChatroomState [ sdata ]) rooms
    return room

listChatrooms :: MonadHead LocalState m => m [ChatroomState]
listChatrooms = fromSetBy (comparing $ roomName <=< roomStateRoom) .
    lookupSharedValue . lsShared . fromStored <$> getLocalHead


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
    }

emptyPacket :: ChatroomService
emptyPacket = ChatroomService
    { chatRoomQuery = False
    , chatRoomInfo = []
    }

instance Storable ChatroomService where
    store' ChatroomService {..} = storeRec $ do
        when  chatRoomQuery $ storeEmpty "room-query"
        forM_ chatRoomInfo $ storeRef "room-info"

    load' = loadRec $ do
        chatRoomQuery <- isJust <$> loadMbEmpty "room-query"
        chatRoomInfo <- loadRefs "room-info"
        return ChatroomService {..}

data PeerState = PeerState
    { psSendRoomUpdates :: Bool
    , psLastList :: [(Stored ChatroomStateData, ChatroomState)]
    }

instance Service ChatroomService where
    serviceID _ = mkServiceID "627657ae-3e39-468a-8381-353395ef4386"

    type ServiceState ChatroomService = PeerState
    emptyServiceState _ = PeerState
        { psSendRoomUpdates = False
        , psLastList = []
        }

    serviceHandler spacket = do
        let ChatroomService {..} = fromStored spacket
        svcModify $ \s -> s { psSendRoomUpdates = True }

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
                                concatMap (rsdRoom . fromStored) . roomStateData

                        let prev = concatMap roomStateData $ filter isCurrentRoom rooms
                            prevRoom = concatMap (rsdRoom . fromStored) prev
                            room = filterAncestors $ (roomInfo : ) prevRoom

                        -- update local state only if we got roomInfo not present there
                        if roomInfo `notElem` prevRoom && roomInfo `elem` room
                          then do
                            sdata <- mstore ChatroomStateData { rsdPrev = prev, rsdRoom = room }
                            storeSetAddComponent sdata set
                          else return set
                foldM upd roomSet chatRoomInfo

    serviceNewPeer = do
        replyPacket emptyPacket { chatRoomQuery = True }

    serviceStorageWatchers _ = (:[]) $
        SomeStorageWatcher (lookupSharedValue . lsShared . fromStored) syncChatroomsToPeer

syncChatroomsToPeer :: Set ChatroomState -> ServiceHandler ChatroomService ()
syncChatroomsToPeer set = do
    ps@PeerState {..} <- svcGet
    when psSendRoomUpdates $ do
        let curList = chatroomSetToList set
        updates <- fmap (concat . catMaybes) $
            forM (makeChatroomDiff psLastList curList) $ return . \case
                AddedChatroom room -> roomData <$> roomStateRoom room
                RemovedChatroom {} -> Nothing
                UpdatedChatroom _ room -> roomData <$> roomStateRoom room
        when (not $ null updates) $ do
            replyPacket $ emptyPacket { chatRoomInfo = updates }
        svcSet $ ps { psLastList = curList }

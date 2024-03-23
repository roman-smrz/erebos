module Erebos.Chatroom (
    Chatroom(..),
    ChatroomData(..),
    validateChatroom,

    ChatroomState(..),
    createChatroom,
) where

import Control.Monad
import Control.Monad.Except

import Data.Monoid
import Data.Text (Text)

import Erebos.PubKey
import Erebos.Set
import Erebos.State
import Erebos.Storage
import Erebos.Storage.Merge


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

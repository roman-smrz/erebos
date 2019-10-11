module Message (
    DirectMessage(..), DirectMessageThread(..),
    emptyDirectThread, createDirectMessage,
    threadToList,
) where

import Data.List
import Data.Ord
import qualified Data.Set as S
import Data.Text (Text)
import Data.Time.LocalTime

import Identity
import PubKey
import Storage

data DirectMessage = DirectMessage
    { msgFrom :: Stored (Signed IdentityData)
    , msgPrev :: [Stored DirectMessage]
    , msgTime :: ZonedTime
    , msgText :: Text
    }

data DirectMessageThread = DirectMessageThread
    { msgPeer :: Stored (Signed IdentityData)
    , msgHead :: [Stored DirectMessage]
    , msgSeen :: [Stored DirectMessage]
    }

instance Storable DirectMessage where
    store' msg = storeRec $ do
        storeRef "from" $ msgFrom msg
        mapM_ (storeRef "prev") $ msgPrev msg
        storeDate "time" $ msgTime msg
        storeText "text" $ msgText msg

    load' = loadRec $ DirectMessage
        <$> loadRef "from"
        <*> loadRefs "prev"
        <*> loadDate "time"
        <*> loadText "text"

instance Storable DirectMessageThread where
    store' msg = storeRec $ do
        storeRef "peer" $ msgPeer msg
        mapM_ (storeRef "head") $ msgHead msg
        mapM_ (storeRef "seen") $ msgSeen msg

    load' = loadRec $ DirectMessageThread
        <$> loadRef "peer"
        <*> loadRefs "head"
        <*> loadRefs "seen"


emptyDirectThread :: UnifiedIdentity -> DirectMessageThread
emptyDirectThread peer = DirectMessageThread (idData peer) [] []

createDirectMessage :: UnifiedIdentity -> DirectMessageThread -> Text -> IO (Stored DirectMessage, Stored DirectMessageThread)
createDirectMessage self thread msg = do
    let st = storedStorage $ idData self
    time <- getZonedTime
    smsg <- wrappedStore st DirectMessage
        { msgFrom = idData $ finalOwner self
        , msgPrev = msgHead thread
        , msgTime = time
        , msgText = msg
        }
    sthread <- wrappedStore st thread
        { msgHead = [smsg]
        , msgSeen = [smsg]
        }
    return (smsg, sthread)

threadToList :: DirectMessageThread -> [DirectMessage]
threadToList thread = helper S.empty $ msgHead thread
    where helper seen msgs
              | msg : msgs' <- filter (`S.notMember` seen) $ reverse $ sortBy (comparing cmpView) msgs =
                  fromStored msg : helper (S.insert msg seen) (msgs' ++ msgPrev (fromStored msg))
              | otherwise = []
          cmpView msg = (zonedTimeToUTC $ msgTime $ fromStored msg, msg)

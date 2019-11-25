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
import Storage

data DirectMessage = DirectMessage
    { msgFrom :: ComposedIdentity
    , msgPrev :: [Stored DirectMessage]
    , msgTime :: ZonedTime
    , msgText :: Text
    }

data DirectMessageThread = DirectMessageThread
    { msgPeer :: ComposedIdentity
    , msgHead :: [Stored DirectMessage]
    , msgSeen :: [Stored DirectMessage]
    }

instance Storable DirectMessage where
    store' msg = storeRec $ do
        mapM_ (storeRef "from") $ idDataF $ msgFrom msg
        mapM_ (storeRef "prev") $ msgPrev msg
        storeDate "time" $ msgTime msg
        storeText "text" $ msgText msg

    load' = loadRec $ DirectMessage
        <$> loadIdentity "from"
        <*> loadRefs "prev"
        <*> loadDate "time"
        <*> loadText "text"

instance Storable DirectMessageThread where
    store' msg = storeRec $ do
        mapM_ (storeRef "peer") $ idDataF $ msgPeer msg
        mapM_ (storeRef "head") $ msgHead msg
        mapM_ (storeRef "seen") $ msgSeen msg

    load' = loadRec $ DirectMessageThread
        <$> loadIdentity "peer"
        <*> loadRefs "head"
        <*> loadRefs "seen"


emptyDirectThread :: ComposedIdentity -> DirectMessageThread
emptyDirectThread peer = DirectMessageThread peer [] []

createDirectMessage :: UnifiedIdentity -> DirectMessageThread -> Text -> IO (Stored DirectMessage, Stored DirectMessageThread)
createDirectMessage self thread msg = do
    let st = storedStorage $ idData self
    time <- getZonedTime
    smsg <- wrappedStore st DirectMessage
        { msgFrom = toComposedIdentity $ finalOwner self
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

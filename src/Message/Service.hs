module Message.Service (
    DirectMessageService,
    ServicePacket(DirectMessagePacket),
    formatMessage,
) where

import Control.Monad.Reader

import Data.List
import qualified Data.Text as T
import Data.Time.Format
import Data.Time.LocalTime

import Identity
import Message
import Service
import State
import Storage
import Storage.List

data DirectMessageService

instance Service DirectMessageService where
    serviceID _ = mkServiceID "c702076c-4928-4415-8b6b-3e839eafcb0d"

    data ServiceState DirectMessageService = DirectMessageService
    emptyServiceState = DirectMessageService

    newtype ServicePacket DirectMessageService = DirectMessagePacket (Stored DirectMessage)

    serviceHandler packet = do
        let DirectMessagePacket smsg = fromStored packet
            msg = fromStored smsg
        powner <- asks $ finalOwner . svcPeer
        tzone <- liftIO $ getCurrentTimeZone
        svcPrint $ formatMessage tzone msg
        if | powner `sameIdentity` msgFrom msg
           -> do erb <- svcGetLocal
                 let st = storedStorage erb
                 erb' <- liftIO $ do
                     threads <- storedFromSList $ lsMessages $ fromStored erb
                     slist <- case find (sameIdentity powner . msgPeer . fromStored) threads of
                                   Just thread -> do thread' <- wrappedStore st (fromStored thread) { msgHead = smsg : msgHead (fromStored thread) }
                                                     slistReplaceS thread thread' $ lsMessages $ fromStored erb
                                   Nothing -> slistAdd (emptyDirectThread powner) { msgHead = [smsg] } $ lsMessages $ fromStored erb
                     wrappedStore st (fromStored erb) { lsMessages = slist }
                 svcSetLocal erb'
                 return Nothing

           | otherwise -> do svcPrint "Owner mismatch"
                             return Nothing

instance Storable (ServicePacket DirectMessageService) where
    store' (DirectMessagePacket smsg) = store' smsg
    load' = DirectMessagePacket <$> load'

formatMessage :: TimeZone -> DirectMessage -> String
formatMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ idName $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

module Message.Service (
    DirectMessageService,
    formatMessage,
) where

import Control.Monad.Reader
import Control.Monad.State

import Data.List
import qualified Data.Text as T
import Data.Time.Format
import Data.Time.LocalTime

import Identity
import Message
import PubKey
import Service
import State
import Storage

data DirectMessageService = DirectMessageService

instance Service DirectMessageService where
    type ServicePacket DirectMessageService = DirectMessage
    emptyServiceState = DirectMessageService
    serviceHandler smsg = do
        let msg = fromStored smsg
        powner <- asks svcPeerOwner
        tzone <- liftIO $ getCurrentTimeZone
        svcPrint $ formatMessage tzone msg
        if | idData powner == msgFrom msg
           -> do erb <- gets svcLocal
                 let st = storedStorage erb
                 erb' <- liftIO $ do
                     slist <- case find ((== idData powner) . msgPeer . fromStored) (storedFromSList $ lsMessages $ fromStored erb) of
                                   Just thread -> do thread' <- wrappedStore st (fromStored thread) { msgHead = smsg : msgHead (fromStored thread) }
                                                     slistReplaceS thread thread' $ lsMessages $ fromStored erb
                                   Nothing -> slistAdd (emptyDirectThread powner) { msgHead = [smsg] } $ lsMessages $ fromStored erb
                     wrappedStore st (fromStored erb) { lsMessages = slist }
                 modify $ \s -> s { svcLocal = erb' }
                 return Nothing

           | otherwise -> do svcPrint "Owner mismatch"
                             return Nothing

formatMessage :: TimeZone -> DirectMessage -> String
formatMessage tzone msg = concat
    [ formatTime defaultTimeLocale "[%H:%M] " $ utcToLocalTime tzone $ zonedTimeToUTC $ msgTime msg
    , maybe "<unnamed>" T.unpack $ iddName $ fromStored $ signedData $ fromStored $ msgFrom msg
    , ": "
    , T.unpack $ msgText msg
    ]

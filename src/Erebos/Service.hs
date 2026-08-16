module Erebos.Service (
    Service(..),
    SomeService(..), someService, someServiceAttr, someServiceID,
    SomeServiceState(..), fromServiceState, someServiceEmptyState,
    SomeServiceGlobalState(..), fromServiceGlobalState, someServiceEmptyGlobalState,
    SomeStorageWatcher(SomeStorageWatcher, GlobalStorageWatcher),
    someStorageWatcher, someStorageWatcherHC, globalStorageWatcher, globalStorageWatcherH,
    ServiceID, mkServiceID,

    ServiceHandler,
    ServiceInput(..),
    ServiceReply(..),
    runServiceHandler,

    svcGet, svcSet, svcModify,
    svcGetGlobal, svcSetGlobal, svcModifyGlobal,
    svcGetLocal, svcSetLocal,

    svcSelf,
    svcPrint,

    replyPacket, replyStored, replyStoredRef,
    afterCommit,
) where

import Control.Monad.Except

import {-# SOURCE #-} Erebos.Network
import Erebos.Network.Protocol
import Erebos.State
import Erebos.Storable
import Erebos.Storage.Head

import Service


someStorageWatcher :: forall s a. (Service s, Eq a) => (Stored LocalState -> a) -> (a -> ServiceHandler s ()) -> SomeStorageWatcher s
someStorageWatcher = SomeStorageWatcher

someStorageWatcherHC :: forall s a. (Service s, Eq a) => (Stored LocalState -> HeadCacheType LocalState -> a) -> (a -> ServiceHandler s ()) -> SomeStorageWatcher s
someStorageWatcherHC = SomeStorageWatcherHC

globalStorageWatcher :: forall s a. (Service s, Eq a) => (Stored LocalState -> a) -> (Server -> a -> ExceptT ErebosError IO ()) -> SomeStorageWatcher s
globalStorageWatcher = GlobalStorageWatcher

globalStorageWatcherH :: forall s a. (Service s, Eq a) => (Head LocalState -> a) -> (Server -> a -> ExceptT ErebosError IO ()) -> SomeStorageWatcher s
globalStorageWatcherH = GlobalStorageWatcherH

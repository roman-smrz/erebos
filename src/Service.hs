module Service (
    Service(..),
    SomeService(..), someService, someServiceAttr, someServiceID,
    SomeServiceState(..), fromServiceState, someServiceEmptyState,
    SomeServiceGlobalState(..), fromServiceGlobalState, someServiceEmptyGlobalState,
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
) where

import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Writer

import Data.Kind
import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as U

import Identity
import {-# SOURCE #-} Network
import State
import Storage

class (Typeable s, Storable s, Typeable (ServiceState s), Typeable (ServiceGlobalState s)) => Service s where
    serviceID :: proxy s -> ServiceID
    serviceHandler :: Stored s -> ServiceHandler s ()

    type ServiceAttributes s = attr | attr -> s
    type ServiceAttributes s = Proxy s
    defaultServiceAttributes :: proxy s -> ServiceAttributes s
    default defaultServiceAttributes :: ServiceAttributes s ~ Proxy s => proxy s -> ServiceAttributes s
    defaultServiceAttributes _ = Proxy

    type ServiceState s :: Type
    type ServiceState s = ()
    emptyServiceState :: proxy s -> ServiceState s
    default emptyServiceState :: ServiceState s ~ () => proxy s -> ServiceState s
    emptyServiceState _ = ()

    type ServiceGlobalState s :: Type
    type ServiceGlobalState s = ()
    emptyServiceGlobalState :: proxy s -> ServiceGlobalState s
    default emptyServiceGlobalState :: ServiceGlobalState s ~ () => proxy s -> ServiceGlobalState s
    emptyServiceGlobalState _ = ()


data SomeService = forall s. Service s => SomeService (Proxy s) (ServiceAttributes s)

someService :: forall s proxy. Service s => proxy s -> SomeService
someService _ = SomeService @s Proxy (defaultServiceAttributes @s Proxy)

someServiceAttr :: forall s. Service s => ServiceAttributes s -> SomeService
someServiceAttr attr = SomeService @s Proxy attr

someServiceID :: SomeService -> ServiceID
someServiceID (SomeService s _) = serviceID s

data SomeServiceState = forall s. Service s => SomeServiceState (Proxy s) (ServiceState s)

fromServiceState :: Service s => proxy s -> SomeServiceState -> Maybe (ServiceState s)
fromServiceState _ (SomeServiceState _ s) = cast s

someServiceEmptyState :: SomeService -> SomeServiceState
someServiceEmptyState (SomeService p _) = SomeServiceState p (emptyServiceState p)

data SomeServiceGlobalState = forall s. Service s => SomeServiceGlobalState (Proxy s) (ServiceGlobalState s)

fromServiceGlobalState :: Service s => proxy s -> SomeServiceGlobalState -> Maybe (ServiceGlobalState s)
fromServiceGlobalState _ (SomeServiceGlobalState _ s) = cast s

someServiceEmptyGlobalState :: SomeService -> SomeServiceGlobalState
someServiceEmptyGlobalState (SomeService p _) = SomeServiceGlobalState p (emptyServiceGlobalState p)


newtype ServiceID = ServiceID UUID
    deriving (Eq, Ord, StorableUUID)

mkServiceID :: String -> ServiceID
mkServiceID = maybe (error "Invalid service ID") ServiceID . U.fromString

data ServiceInput s = ServiceInput
    { svcAttributes :: ServiceAttributes s
    , svcPeer :: Peer
    , svcPeerIdentity :: UnifiedIdentity
    , svcServer :: Server
    , svcPrintOp :: String -> IO ()
    }

data ServiceReply s = ServiceReply (Either s (Stored s)) Bool

data ServiceHandlerState s = ServiceHandlerState
    { svcValue :: ServiceState s
    , svcGlobal :: ServiceGlobalState s
    , svcLocal :: Stored LocalState
    }

newtype ServiceHandler s a = ServiceHandler (ReaderT (ServiceInput s) (WriterT [ServiceReply s] (StateT (ServiceHandlerState s) (ExceptT String IO))) a)
    deriving (Functor, Applicative, Monad, MonadReader (ServiceInput s), MonadWriter [ServiceReply s], MonadState (ServiceHandlerState s), MonadError String, MonadIO)

instance MonadStorage (ServiceHandler s) where
    getStorage = asks $ peerStorage . svcPeer

instance MonadHead LocalState (ServiceHandler s) where
    updateLocalHead f = do
        (ls, x) <- f =<< gets svcLocal
        modify $ \s -> s { svcLocal = ls }
        return x

runServiceHandler :: Service s => Head LocalState -> ServiceInput s -> ServiceState s -> ServiceGlobalState s -> ServiceHandler s () -> IO ([ServiceReply s], (ServiceState s, ServiceGlobalState s))
runServiceHandler h input svc global shandler = do
    let sstate = ServiceHandlerState { svcValue = svc, svcGlobal = global, svcLocal = headStoredObject h }
        ServiceHandler handler = shandler
    (runExceptT $ flip runStateT sstate $ execWriterT $ flip runReaderT input $ handler) >>= \case
        Left err -> do
            svcPrintOp input $ "service failed: " ++ err
            return ([], (svc, global))
        Right (rsp, sstate')
            | svcLocal sstate' == svcLocal sstate -> return (rsp, (svcValue sstate', svcGlobal sstate'))
            | otherwise -> replaceHead h (svcLocal sstate') >>= \case
                Left (Just h') -> runServiceHandler h' input svc global shandler
                _              -> return (rsp, (svcValue sstate', svcGlobal sstate'))

svcGet :: ServiceHandler s (ServiceState s)
svcGet = gets svcValue

svcSet :: ServiceState s -> ServiceHandler s ()
svcSet x = modify $ \st -> st { svcValue = x }

svcModify :: (ServiceState s -> ServiceState s) -> ServiceHandler s ()
svcModify f = modify $ \st -> st { svcValue = f (svcValue st) }

svcGetGlobal :: ServiceHandler s (ServiceGlobalState s)
svcGetGlobal = gets svcGlobal

svcSetGlobal :: ServiceGlobalState s -> ServiceHandler s ()
svcSetGlobal x = modify $ \st -> st { svcGlobal = x }

svcModifyGlobal :: (ServiceGlobalState s -> ServiceGlobalState s) -> ServiceHandler s ()
svcModifyGlobal f = modify $ \st -> st { svcGlobal = f (svcGlobal st) }

svcGetLocal :: ServiceHandler s (Stored LocalState)
svcGetLocal = gets svcLocal

svcSetLocal :: Stored LocalState -> ServiceHandler s ()
svcSetLocal x = modify $ \st -> st { svcLocal = x }

svcSelf :: ServiceHandler s UnifiedIdentity
svcSelf = maybe (throwError "failed to validate own identity") return .
        validateIdentity . lsIdentity . fromStored =<< svcGetLocal

svcPrint :: String -> ServiceHandler s ()
svcPrint str = liftIO . ($str) =<< asks svcPrintOp

replyPacket :: Service s => s -> ServiceHandler s ()
replyPacket x = tell [ServiceReply (Left x) True]

replyStored :: Service s => Stored s -> ServiceHandler s ()
replyStored x = tell [ServiceReply (Right x) True]

replyStoredRef :: Service s => Stored s -> ServiceHandler s ()
replyStoredRef x = tell [ServiceReply (Right x) False]

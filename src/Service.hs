module Service (
    Service(..),
    SomeService(..), someServiceID,
    SomeServiceState(..), fromServiceState, someServiceEmptyState,
    SomeServiceGlobalState(..), fromServiceGlobalState, someServiceEmptyGlobalState,
    ServiceID, mkServiceID,

    ServiceHandler,
    ServiceInput(..),
    ServiceReply(..),
    handleServicePacket,

    svcGet, svcSet, svcModify,
    svcGetGlobal, svcSetGlobal, svcModifyGlobal,
    svcGetLocal, svcSetLocal,
    svcPrint,
    replyPacket, replyStored, replyStoredRef,
) where

import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State
import Control.Monad.Writer

import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as U

import Identity
import State
import Storage

class (Typeable s, Storable s, Typeable (ServiceState s), Typeable (ServiceGlobalState s)) => Service s where
    serviceID :: proxy s -> ServiceID
    serviceHandler :: Stored s -> ServiceHandler s ()

    type ServiceState s :: *
    type ServiceState s = ()
    emptyServiceState :: proxy s -> ServiceState s
    default emptyServiceState :: ServiceState s ~ () => proxy s -> ServiceState s
    emptyServiceState _ = ()

    type ServiceGlobalState s :: *
    type ServiceGlobalState s = ()
    emptyServiceGlobalState :: proxy s -> ServiceGlobalState s
    default emptyServiceGlobalState :: ServiceGlobalState s ~ () => proxy s -> ServiceGlobalState s
    emptyServiceGlobalState _ = ()


data SomeService = forall s. Service s => SomeService (Proxy s)

someServiceID :: SomeService -> ServiceID
someServiceID (SomeService s) = serviceID s

data SomeServiceState = forall s. Service s => SomeServiceState (Proxy s) (ServiceState s)

fromServiceState :: Service s => proxy s -> SomeServiceState -> Maybe (ServiceState s)
fromServiceState _ (SomeServiceState _ s) = cast s

someServiceEmptyState :: SomeService -> SomeServiceState
someServiceEmptyState (SomeService p) = SomeServiceState p (emptyServiceState p)

data SomeServiceGlobalState = forall s. Service s => SomeServiceGlobalState (Proxy s) (ServiceGlobalState s)

fromServiceGlobalState :: Service s => proxy s -> SomeServiceGlobalState -> Maybe (ServiceGlobalState s)
fromServiceGlobalState _ (SomeServiceGlobalState _ s) = cast s

someServiceEmptyGlobalState :: SomeService -> SomeServiceGlobalState
someServiceEmptyGlobalState (SomeService p) = SomeServiceGlobalState p (emptyServiceGlobalState p)


newtype ServiceID = ServiceID UUID
    deriving (Eq, Ord, StorableUUID)

mkServiceID :: String -> ServiceID
mkServiceID = maybe (error "Invalid service ID") ServiceID . U.fromString

data ServiceInput = ServiceInput
    { svcPeer :: UnifiedIdentity
    , svcPrintOp :: String -> IO ()
    }

data ServiceReply s = ServiceReply (Either s (Stored s)) Bool

data ServiceHandlerState s = ServiceHandlerState
    { svcValue :: ServiceState s
    , svcGlobal :: ServiceGlobalState s
    , svcLocal :: Stored LocalState
    }

newtype ServiceHandler s a = ServiceHandler (ReaderT ServiceInput (WriterT [ServiceReply s] (StateT (ServiceHandlerState s) (ExceptT String IO))) a)
    deriving (Functor, Applicative, Monad, MonadReader ServiceInput, MonadWriter [ServiceReply s], MonadState (ServiceHandlerState s), MonadError String, MonadIO)

handleServicePacket :: Service s => Head LocalState -> ServiceInput -> ServiceState s -> ServiceGlobalState s -> Stored s -> IO ([ServiceReply s], (ServiceState s, ServiceGlobalState s))
handleServicePacket h input svc global packet = do
    let sstate = ServiceHandlerState { svcValue = svc, svcGlobal = global, svcLocal = headStoredObject h }
        ServiceHandler handler = serviceHandler packet
    (runExceptT $ flip runStateT sstate $ execWriterT $ flip runReaderT input $ handler) >>= \case
        Left err -> do
            svcPrintOp input $ "service failed: " ++ err
            return ([], (svc, global))
        Right (rsp, sstate')
            | svcLocal sstate' == svcLocal sstate -> return (rsp, (svcValue sstate', svcGlobal sstate'))
            | otherwise -> replaceHead h (svcLocal sstate') >>= \case
                Left (Just h') -> handleServicePacket h' input svc global packet
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

svcPrint :: String -> ServiceHandler s ()
svcPrint str = liftIO . ($str) =<< asks svcPrintOp

replyPacket :: Service s => s -> ServiceHandler s ()
replyPacket x = tell [ServiceReply (Left x) True]

replyStored :: Service s => Stored s -> ServiceHandler s ()
replyStored x = tell [ServiceReply (Right x) True]

replyStoredRef :: Service s => Stored s -> ServiceHandler s ()
replyStoredRef x = tell [ServiceReply (Right x) False]

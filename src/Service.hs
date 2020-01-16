module Service (
    Service(..),
    SomeService(..), SomeServiceState(..),
    someServiceID, fromServiceState, someServiceEmptyState,
    ServiceID, mkServiceID,

    ServiceHandler,
    ServiceInput(..),
    handleServicePacket,

    svcGet, svcSet,
    svcGetLocal, svcSetLocal,
    svcPrint,
) where

import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State

import Data.Typeable
import Data.UUID (UUID)
import qualified Data.UUID as U

import Identity
import State
import Storage

class (Typeable s, Storable (ServicePacket s)) => Service s where
    serviceID :: proxy s -> ServiceID

    data ServiceState s :: *
    emptyServiceState :: ServiceState s

    data ServicePacket s :: *
    serviceHandler :: Stored (ServicePacket s) -> ServiceHandler s (Maybe (ServicePacket s))

data SomeService = forall s. Service s => SomeService (Proxy s)

data SomeServiceState = forall s. Service s => SomeServiceState (ServiceState s)

someServiceID :: SomeService -> ServiceID
someServiceID (SomeService s) = serviceID s

fromServiceState :: Service s => SomeServiceState -> Maybe (ServiceState s)
fromServiceState (SomeServiceState s) = cast s

someServiceEmptyState :: SomeService -> SomeServiceState
someServiceEmptyState (SomeService (Proxy :: Proxy s)) = SomeServiceState (emptyServiceState :: ServiceState s)

newtype ServiceID = ServiceID UUID
    deriving (Eq, Ord, StorableUUID)

mkServiceID :: String -> ServiceID
mkServiceID = maybe (error "Invalid service ID") ServiceID . U.fromString

data ServiceInput = ServiceInput
    { svcPeer :: UnifiedIdentity
    , svcPrintOp :: String -> IO ()
    }

data ServiceHandlerState s = ServiceHandlerState
    { svcValue :: ServiceState s
    , svcLocal :: Stored LocalState
    }

newtype ServiceHandler s a = ServiceHandler (ReaderT ServiceInput (StateT (ServiceHandlerState s) (ExceptT String IO)) a)
    deriving (Functor, Applicative, Monad, MonadReader ServiceInput, MonadState (ServiceHandlerState s), MonadError String, MonadIO)

handleServicePacket :: Service s => Storage -> ServiceInput -> ServiceState s -> Stored (ServicePacket s) -> IO (Maybe (ServicePacket s), ServiceState s)
handleServicePacket st input svc packet = do
    herb <- loadLocalStateHead st
    let erb = wrappedLoad $ headRef herb
        sstate = ServiceHandlerState { svcValue = svc, svcLocal = erb }
        ServiceHandler handler = serviceHandler packet
    (runExceptT $ flip runStateT sstate $ flip runReaderT input $ handler) >>= \case
        Left err -> do
            svcPrintOp input $ "service failed: " ++ err
            return (Nothing, svc)
        Right (rsp, sstate')
            | svcLocal sstate' == svcLocal sstate -> return (rsp, svcValue sstate')
            | otherwise -> replaceHead (svcLocal sstate') (Right herb) >>= \case
                Left  _ -> handleServicePacket st input svc packet
                Right _ -> return (rsp, svcValue sstate')

svcGet :: ServiceHandler s (ServiceState s)
svcGet = gets svcValue

svcSet :: ServiceState s -> ServiceHandler s ()
svcSet x = modify $ \st -> st { svcValue = x }

svcGetLocal :: ServiceHandler s (Stored LocalState)
svcGetLocal = gets svcLocal

svcSetLocal :: Stored LocalState -> ServiceHandler s ()
svcSetLocal x = modify $ \st -> st { svcLocal = x }

svcPrint :: String -> ServiceHandler s ()
svcPrint str = liftIO . ($str) =<< asks svcPrintOp

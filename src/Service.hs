module Service (
    Service(..),
    SomeService(..),

    ServiceHandler,
    ServiceInput(..), ServiceState(..),
    handleServicePacket,

    svcPrint,
) where

import Control.Monad.Except
import Control.Monad.Reader
import Control.Monad.State

import Identity
import State
import Storage

class Storable (ServicePacket s) => Service s where
    type ServicePacket s :: *
    emptyServiceState :: s
    serviceHandler :: Stored (ServicePacket s) -> ServiceHandler s (Maybe (ServicePacket s))

data SomeService = forall s. Service s => SomeService s

data ServiceInput = ServiceInput
    { svcPeer :: UnifiedIdentity
    , svcPeerOwner :: UnifiedIdentity
    , svcPrintOp :: String -> IO ()
    }

data ServiceState s = ServiceState
    { svcValue :: s
    , svcLocal :: Stored LocalState
    }

newtype ServiceHandler s a = ServiceHandler (ReaderT ServiceInput (StateT (ServiceState s) (ExceptT String IO)) a)
    deriving (Functor, Applicative, Monad, MonadReader ServiceInput, MonadState (ServiceState s), MonadIO)

handleServicePacket :: Service s => Storage -> ServiceInput -> s -> Stored (ServicePacket s) -> IO (Maybe (ServicePacket s), s)
handleServicePacket st input svc packet = do
    herb <- loadLocalState st
    let erb = wrappedLoad $ headRef herb
        sstate = ServiceState { svcValue = svc, svcLocal = erb }
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

svcPrint :: String -> ServiceHandler s ()
svcPrint str = liftIO . ($str) =<< asks svcPrintOp

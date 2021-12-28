module Pairing (
    PairingService(..),
    PairingState(..),
    PairingAttributes(..),
    PairingResult(..),

    pairingRequest,
    pairingAccept,
    pairingReject,
) where

import Control.Monad.Except
import Control.Monad.Reader

import Crypto.Random

import Data.Bits
import Data.ByteArray (Bytes, convert)
import qualified Data.ByteArray as BA
import qualified Data.ByteString.Char8 as BC
import Data.Maybe
import qualified Data.Text as T
import Data.Typeable
import Data.Word

import Identity
import Network
import Service
import State
import Storage

data PairingService a = PairingRequest RefDigest
                      | PairingResponse Bytes
                      | PairingRequestNonce Bytes
                      | PairingAccept a
                      | PairingDecline

data PairingState a = NoPairing
                    | OurRequest Bytes
                    | OurRequestConfirm (Maybe (PairingVerifiedResult a))
                    | OurRequestReady
                    | PeerRequest Bytes RefDigest
                    | PeerRequestConfirm
                    | PairingDone

data PairingAttributes a = PairingAttributes
    { pairingHookRequest :: ServiceHandler (PairingService a) ()
    , pairingHookResponse :: String -> ServiceHandler (PairingService a) ()
    , pairingHookRequestNonce :: String -> ServiceHandler (PairingService a) ()
    , pairingHookRequestNonceFailed :: ServiceHandler (PairingService a) ()
    , pairingHookConfirmedResponse :: ServiceHandler (PairingService a) ()
    , pairingHookConfirmedRequest :: ServiceHandler (PairingService a) ()
    , pairingHookAcceptedResponse :: ServiceHandler (PairingService a) ()
    , pairingHookAcceptedRequest :: ServiceHandler (PairingService a) ()
    , pairingHookVerifyFailed :: ServiceHandler (PairingService a) ()
    , pairingHookRejected :: ServiceHandler (PairingService a) ()
    , pairingHookFailed :: ServiceHandler (PairingService a) ()
    }

class (Typeable a, Storable a) => PairingResult a where
    type PairingVerifiedResult a :: *
    type PairingVerifiedResult a = a

    pairingServiceID :: proxy a -> ServiceID
    pairingVerifyResult :: a -> ServiceHandler (PairingService a) (Maybe (PairingVerifiedResult a))
    pairingFinalizeRequest :: PairingVerifiedResult a -> ServiceHandler (PairingService a) ()
    pairingFinalizeResponse :: ServiceHandler (PairingService a) a
    defaultPairingAttributes :: proxy (PairingService a) -> PairingAttributes a


instance Storable a => Storable (PairingService a) where
    store' (PairingRequest x) = storeRec $ storeBinary "request" x
    store' (PairingResponse x) = storeRec $ storeBinary "response" x
    store' (PairingRequestNonce x) = storeRec $ storeBinary "reqnonce" x
    store' (PairingAccept x) = store' x
    store' (PairingDecline) = storeRec $ storeText "decline" ""

    load' = do
        res <- loadRec $ do
            (req :: Maybe Bytes) <- loadMbBinary "request"
            rsp <- loadMbBinary "response"
            rnonce <- loadMbBinary "reqnonce"
            (decline :: Maybe T.Text) <- loadMbText "decline"
            return $ catMaybes
                    [ PairingRequest <$> (refDigestFromByteString =<< req)
                    , PairingResponse <$> rsp
                    , PairingRequestNonce <$> rnonce
                    , const PairingDecline <$> decline
                    ]
        case res of
             x:_ -> return x
             [] -> PairingAccept <$> load'


instance PairingResult a => Service (PairingService a) where
    serviceID _ = pairingServiceID @a Proxy

    type ServiceAttributes (PairingService a) = PairingAttributes a
    defaultServiceAttributes = defaultPairingAttributes

    type ServiceState (PairingService a) = PairingState a
    emptyServiceState _ = NoPairing

    serviceHandler spacket = ((,fromStored spacket) <$> svcGet) >>= \case
        (NoPairing, PairingRequest confirm) -> do
            join $ asks $ pairingHookRequest . svcAttributes
            nonce <- liftIO $ getRandomBytes 32
            svcSet $ PeerRequest nonce confirm
            replyPacket $ PairingResponse nonce
        (NoPairing, _) -> return ()

        (PairingDone, _) -> return ()
        (_, PairingDecline) -> do
            join $ asks $ pairingHookRejected . svcAttributes
            svcSet NoPairing

        (OurRequest nonce, PairingResponse pnonce) -> do
            peer <- asks $ svcPeerIdentity
            self <- maybe (throwError "failed to validate own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            hook <- asks $ pairingHookResponse . svcAttributes
            hook $ confirmationNumber $ nonceDigest self peer nonce pnonce
            svcSet $ OurRequestConfirm Nothing
            replyPacket $ PairingRequestNonce nonce
        (OurRequest _, _) -> reject

        (OurRequestConfirm _, PairingAccept x) -> do
            flip catchError (const reject) $ do
                pairingVerifyResult x >>= \case
                    Just x' -> do
                        join $ asks $ pairingHookConfirmedRequest . svcAttributes
                        svcSet $ OurRequestConfirm (Just x')
                    Nothing -> do
                        join $ asks $ pairingHookVerifyFailed . svcAttributes
                        svcSet NoPairing
                        replyPacket PairingDecline

        (OurRequestConfirm _, _) -> reject

        (OurRequestReady, PairingAccept x) -> do
            flip catchError (const reject) $ do
                pairingVerifyResult x >>= \case
                    Just x' -> do
                        pairingFinalizeRequest x'
                        join $ asks $ pairingHookAcceptedResponse . svcAttributes
                        svcSet $ PairingDone
                    Nothing -> do
                        join $ asks $ pairingHookVerifyFailed . svcAttributes
                        throwError ""
        (OurRequestReady, _) -> reject

        (PeerRequest nonce dgst, PairingRequestNonce pnonce) -> do
            peer <- asks $ svcPeerIdentity
            self <- maybe (throwError "failed to verify own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            if dgst == nonceDigest peer self pnonce BA.empty
               then do hook <- asks $ pairingHookRequestNonce . svcAttributes
                       hook $ confirmationNumber $ nonceDigest peer self pnonce nonce
                       svcSet PeerRequestConfirm
               else do join $ asks $ pairingHookRequestNonceFailed . svcAttributes
                       svcSet NoPairing
                       replyPacket PairingDecline
        (PeerRequest _ _, _) -> reject
        (PeerRequestConfirm, _) -> reject

reject :: PairingResult a => ServiceHandler (PairingService a) ()
reject = do
    join $ asks $ pairingHookFailed . svcAttributes
    svcSet NoPairing
    replyPacket PairingDecline


nonceDigest :: UnifiedIdentity -> UnifiedIdentity -> Bytes -> Bytes -> RefDigest
nonceDigest id1 id2 nonce1 nonce2 = hashToRefDigest $ serializeObject $ Rec
        [ (BC.pack "id", RecRef $ storedRef $ idData id1)
        , (BC.pack "id", RecRef $ storedRef $ idData id2)
        , (BC.pack "nonce", RecBinary $ convert nonce1)
        , (BC.pack "nonce", RecBinary $ convert nonce2)
        ]

confirmationNumber :: RefDigest -> String
confirmationNumber dgst = let (a:b:c:d:_) = map fromIntegral $ BA.unpack dgst :: [Word32]
                              str = show $ ((a `shift` 24) .|. (b `shift` 16) .|. (c `shift` 8) .|. d) `mod` (10 ^ len)
                           in replicate (len - length str) '0' ++ str
    where len = 6

pairingRequest :: forall a m proxy. (PairingResult a, MonadIO m, MonadError String m) => proxy a -> Peer -> m ()
pairingRequest _ peer = do
    self <- liftIO $ serverIdentity $ peerServer peer
    nonce <- liftIO $ getRandomBytes 32
    pid <- peerIdentity peer >>= \case
        PeerIdentityFull pid -> return pid
        _ -> throwError "incomplete peer identity"
    sendToPeerWith @(PairingService a) peer $ \case
        NoPairing -> return (Just $ PairingRequest (nonceDigest self pid nonce BA.empty), OurRequest nonce)
        _ -> throwError "already in progress"

pairingAccept :: forall a m proxy. (PairingResult a, MonadIO m, MonadError String m) => proxy a -> Peer -> m ()
pairingAccept _ peer = runPeerService @(PairingService a) peer $ do
    svcGet >>= \case
        NoPairing -> throwError $ "none in progress"
        OurRequest {} -> throwError $ "waiting for peer"
        OurRequestConfirm Nothing -> do
            join $ asks $ pairingHookConfirmedResponse . svcAttributes
            svcSet OurRequestReady
        OurRequestConfirm (Just verified) -> do
            join $ asks $ pairingHookAcceptedResponse . svcAttributes
            pairingFinalizeRequest verified
            svcSet PairingDone
        OurRequestReady -> throwError $ "already accepted, waiting for peer"
        PeerRequest {} -> throwError $ "waiting for peer"
        PeerRequestConfirm -> do
            join $ asks $ pairingHookAcceptedRequest . svcAttributes
            replyPacket . PairingAccept =<< pairingFinalizeResponse
            svcSet PairingDone
        PairingDone -> throwError $ "already done"

pairingReject :: forall a m proxy. (PairingResult a, MonadIO m, MonadError String m) => proxy a -> Peer -> m ()
pairingReject _ peer = runPeerService @(PairingService a) peer $ do
    svcGet >>= \case
        NoPairing -> throwError $ "none in progress"
        PairingDone -> throwError $ "already done"
        _ -> reject

module Pairing (
    PairingService(..),
    PairingState(..),
    PairingAttributes(..),
    PairingResult(..),
    PairingFailureReason(..),

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
import Data.Kind
import Data.Maybe
import Data.Typeable
import Data.Word

import Identity
import Network
import PubKey
import Service
import State
import Storage

data PairingService a = PairingRequest (Stored (Signed IdentityData)) (Stored (Signed IdentityData)) RefDigest
                      | PairingResponse Bytes
                      | PairingRequestNonce Bytes
                      | PairingAccept a
                      | PairingReject

data PairingState a = NoPairing
                    | OurRequest UnifiedIdentity UnifiedIdentity Bytes
                    | OurRequestConfirm (Maybe (PairingVerifiedResult a))
                    | OurRequestReady
                    | PeerRequest UnifiedIdentity UnifiedIdentity Bytes RefDigest
                    | PeerRequestConfirm
                    | PairingDone

data PairingFailureReason a = PairingUserRejected
                            | PairingUnexpectedMessage (PairingState a) (PairingService a)
                            | PairingFailedOther String

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
    , pairingHookFailed :: PairingFailureReason a -> ServiceHandler (PairingService a) ()
    }

class (Typeable a, Storable a) => PairingResult a where
    type PairingVerifiedResult a :: Type
    type PairingVerifiedResult a = a

    pairingServiceID :: proxy a -> ServiceID
    pairingVerifyResult :: a -> ServiceHandler (PairingService a) (Maybe (PairingVerifiedResult a))
    pairingFinalizeRequest :: PairingVerifiedResult a -> ServiceHandler (PairingService a) ()
    pairingFinalizeResponse :: ServiceHandler (PairingService a) a
    defaultPairingAttributes :: proxy (PairingService a) -> PairingAttributes a


instance Storable a => Storable (PairingService a) where
    store' (PairingRequest idReq idRsp x) = storeRec $ do
        storeRef "id-req" idReq
        storeRef "id-rsp" idRsp
        storeBinary "request" x
    store' (PairingResponse x) = storeRec $ storeBinary "response" x
    store' (PairingRequestNonce x) = storeRec $ storeBinary "reqnonce" x
    store' (PairingAccept x) = store' x
    store' (PairingReject) = storeRec $ storeEmpty "reject"

    load' = do
        res <- loadRec $ do
            (req :: Maybe Bytes) <- loadMbBinary "request"
            idReq <- loadMbRef "id-req"
            idRsp <- loadMbRef "id-rsp"
            rsp <- loadMbBinary "response"
            rnonce <- loadMbBinary "reqnonce"
            rej <- loadMbEmpty "reject"
            return $ catMaybes
                    [ PairingRequest <$> idReq <*> idRsp <*> (refDigestFromByteString =<< req)
                    , PairingResponse <$> rsp
                    , PairingRequestNonce <$> rnonce
                    , const PairingReject <$> rej
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
        (NoPairing, PairingRequest pdata sdata confirm) -> do
            self <- maybe (throwError "failed to validate received identity") return $ validateIdentity sdata
            self' <- maybe (throwError "failed to validate own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            when (not $ self `sameIdentity` self') $ do
                throwError "pairing request to different identity"

            peer <- maybe (throwError "failed to validate received peer identity") return $ validateIdentity pdata
            peer' <- asks $ svcPeerIdentity
            when (not $ peer `sameIdentity` peer') $ do
                throwError "pairing request from different identity"

            join $ asks $ pairingHookRequest . svcAttributes
            nonce <- liftIO $ getRandomBytes 32
            svcSet $ PeerRequest peer self nonce confirm
            replyPacket $ PairingResponse nonce
        (NoPairing, _) -> return ()

        (PairingDone, _) -> return ()
        (_, PairingReject) -> do
            join $ asks $ pairingHookRejected . svcAttributes
            svcSet NoPairing

        (OurRequest self peer nonce, PairingResponse pnonce) -> do
            hook <- asks $ pairingHookResponse . svcAttributes
            hook $ confirmationNumber $ nonceDigest self peer nonce pnonce
            svcSet $ OurRequestConfirm Nothing
            replyPacket $ PairingRequestNonce nonce
        x@(OurRequest {}, _) -> reject $ uncurry PairingUnexpectedMessage x

        (OurRequestConfirm _, PairingAccept x) -> do
            flip catchError (reject . PairingFailedOther) $ do
                pairingVerifyResult x >>= \case
                    Just x' -> do
                        join $ asks $ pairingHookConfirmedRequest . svcAttributes
                        svcSet $ OurRequestConfirm (Just x')
                    Nothing -> do
                        join $ asks $ pairingHookVerifyFailed . svcAttributes
                        svcSet NoPairing
                        replyPacket PairingReject

        x@(OurRequestConfirm _, _) -> reject $ uncurry PairingUnexpectedMessage x

        (OurRequestReady, PairingAccept x) -> do
            flip catchError (reject . PairingFailedOther) $ do
                pairingVerifyResult x >>= \case
                    Just x' -> do
                        pairingFinalizeRequest x'
                        join $ asks $ pairingHookAcceptedResponse . svcAttributes
                        svcSet $ PairingDone
                    Nothing -> do
                        join $ asks $ pairingHookVerifyFailed . svcAttributes
                        throwError ""
        x@(OurRequestReady, _) -> reject $ uncurry PairingUnexpectedMessage x

        (PeerRequest peer self nonce dgst, PairingRequestNonce pnonce) -> do
            if dgst == nonceDigest peer self pnonce BA.empty
               then do hook <- asks $ pairingHookRequestNonce . svcAttributes
                       hook $ confirmationNumber $ nonceDigest peer self pnonce nonce
                       svcSet PeerRequestConfirm
               else do join $ asks $ pairingHookRequestNonceFailed . svcAttributes
                       svcSet NoPairing
                       replyPacket PairingReject
        x@(PeerRequest {}, _) -> reject $ uncurry PairingUnexpectedMessage x
        x@(PeerRequestConfirm, _) -> reject $ uncurry PairingUnexpectedMessage x

reject :: PairingResult a => PairingFailureReason a -> ServiceHandler (PairingService a) ()
reject reason = do
    join $ asks $ flip pairingHookFailed reason . svcAttributes
    svcSet NoPairing
    replyPacket PairingReject


nonceDigest :: UnifiedIdentity -> UnifiedIdentity -> Bytes -> Bytes -> RefDigest
nonceDigest idReq idRsp nonceReq nonceRsp = hashToRefDigest $ serializeObject $ Rec
        [ (BC.pack "id-req", RecRef $ storedRef $ idData idReq)
        , (BC.pack "id-rsp", RecRef $ storedRef $ idData idRsp)
        , (BC.pack "nonce-req", RecBinary $ convert nonceReq)
        , (BC.pack "nonce-rsp", RecBinary $ convert nonceRsp)
        ]

confirmationNumber :: RefDigest -> String
confirmationNumber dgst =
    case map fromIntegral $ BA.unpack dgst :: [Word32] of
         (a:b:c:d:_) -> let str = show $ ((a `shift` 24) .|. (b `shift` 16) .|. (c `shift` 8) .|. d) `mod` (10 ^ len)
                         in replicate (len - length str) '0' ++ str
         _ -> ""
    where len = 6

pairingRequest :: forall a m proxy. (PairingResult a, MonadIO m, MonadError String m) => proxy a -> Peer -> m ()
pairingRequest _ peer = do
    self <- liftIO $ serverIdentity $ peerServer peer
    nonce <- liftIO $ getRandomBytes 32
    pid <- peerIdentity peer >>= \case
        PeerIdentityFull pid -> return pid
        _ -> throwError "incomplete peer identity"
    sendToPeerWith @(PairingService a) peer $ \case
        NoPairing -> return (Just $ PairingRequest (idData self) (idData pid) (nonceDigest self pid nonce BA.empty), OurRequest self pid nonce)
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
        _ -> reject PairingUserRejected

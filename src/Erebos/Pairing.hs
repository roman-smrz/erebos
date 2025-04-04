module Erebos.Pairing (
    PairingService(..),
    PairingState(..),
    PairingAttributes(..),
    PairingResult(..),
    PairingFailureReason(..),

    pairingRequest,
    pairingAccept,
    pairingReject,
) where

import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Crypto.Random

import Data.Bits
import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BC
import Data.Kind
import Data.Maybe
import Data.Typeable
import Data.Word

import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.PubKey
import Erebos.Service
import Erebos.State
import Erebos.Storable

data PairingService a = PairingRequest (Stored (Signed IdentityData)) (Stored (Signed IdentityData)) RefDigest
                      | PairingResponse ByteString
                      | PairingRequestNonce ByteString
                      | PairingAccept a
                      | PairingReject

data PairingState a = NoPairing
                    | OurRequest UnifiedIdentity UnifiedIdentity ByteString
                    | OurRequestConfirm (Maybe (PairingVerifiedResult a))
                    | OurRequestReady
                    | PeerRequest UnifiedIdentity UnifiedIdentity ByteString RefDigest
                    | PeerRequestConfirm
                    | PairingDone

data PairingFailureReason a = PairingUserRejected
                            | PairingUnexpectedMessage (PairingState a) (PairingService a)
                            | PairingFailedOther ErebosError

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
            (req :: Maybe ByteString) <- loadMbBinary "request"
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
            self <- maybe (throwOtherError "failed to validate received identity") return $ validateIdentity sdata
            self' <- maybe (throwOtherError "failed to validate own identity") return .
                validateExtendedIdentity . lsIdentity . fromStored =<< svcGetLocal
            when (not $ self `sameIdentity` self') $ do
                throwOtherError "pairing request to different identity"

            peer <- maybe (throwOtherError "failed to validate received peer identity") return $ validateIdentity pdata
            peer' <- asks $ svcPeerIdentity
            when (not $ peer `sameIdentity` peer') $ do
                throwOtherError "pairing request from different identity"

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
                        throwOtherError ""
        x@(OurRequestReady, _) -> reject $ uncurry PairingUnexpectedMessage x

        (PeerRequest peer self nonce dgst, PairingRequestNonce pnonce) -> do
            if dgst == nonceDigest peer self pnonce BS.empty
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


nonceDigest :: UnifiedIdentity -> UnifiedIdentity -> ByteString -> ByteString -> RefDigest
nonceDigest idReq idRsp nonceReq nonceRsp = hashToRefDigest $ serializeObject $ Rec
        [ (BC.pack "id-req", RecRef $ storedRef $ idData idReq)
        , (BC.pack "id-rsp", RecRef $ storedRef $ idData idRsp)
        , (BC.pack "nonce-req", RecBinary nonceReq)
        , (BC.pack "nonce-rsp", RecBinary nonceRsp)
        ]

confirmationNumber :: RefDigest -> String
confirmationNumber dgst =
    case map fromIntegral $ BA.unpack dgst :: [Word32] of
         (a:b:c:d:_) -> let str = show $ ((a `shift` 24) .|. (b `shift` 16) .|. (c `shift` 8) .|. d) `mod` (10 ^ len)
                         in replicate (len - length str) '0' ++ str
         _ -> ""
    where len = 6

pairingRequest :: forall a m e proxy. (PairingResult a, MonadIO m, MonadError e m, FromErebosError e) => proxy a -> Peer -> m ()
pairingRequest _ peer = do
    self <- liftIO $ serverIdentity $ peerServer peer
    nonce <- liftIO $ getRandomBytes 32
    pid <- peerIdentity peer >>= \case
        PeerIdentityFull pid -> return pid
        _ -> throwOtherError "incomplete peer identity"
    sendToPeerWith @(PairingService a) peer $ \case
        NoPairing -> return (Just $ PairingRequest (idData self) (idData pid) (nonceDigest self pid nonce BS.empty), OurRequest self pid nonce)
        _ -> throwOtherError "already in progress"

pairingAccept :: forall a m e proxy. (PairingResult a, MonadIO m, MonadError e m, FromErebosError e) => proxy a -> Peer -> m ()
pairingAccept _ peer = runPeerService @(PairingService a) peer $ do
    svcGet >>= \case
        NoPairing -> throwOtherError $ "none in progress"
        OurRequest {} -> throwOtherError $ "waiting for peer"
        OurRequestConfirm Nothing -> do
            join $ asks $ pairingHookConfirmedResponse . svcAttributes
            svcSet OurRequestReady
        OurRequestConfirm (Just verified) -> do
            join $ asks $ pairingHookAcceptedResponse . svcAttributes
            pairingFinalizeRequest verified
            svcSet PairingDone
        OurRequestReady -> throwOtherError $ "already accepted, waiting for peer"
        PeerRequest {} -> throwOtherError $ "waiting for peer"
        PeerRequestConfirm -> do
            join $ asks $ pairingHookAcceptedRequest . svcAttributes
            replyPacket . PairingAccept =<< pairingFinalizeResponse
            svcSet PairingDone
        PairingDone -> throwOtherError $ "already done"

pairingReject :: forall a m e proxy. (PairingResult a, MonadIO m, MonadError e m, FromErebosError e) => proxy a -> Peer -> m ()
pairingReject _ peer = runPeerService @(PairingService a) peer $ do
    svcGet >>= \case
        NoPairing -> throwOtherError $ "none in progress"
        PairingDone -> throwOtherError $ "already done"
        _ -> reject PairingUserRejected

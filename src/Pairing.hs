module Pairing (
    PairingService(..),
    PairingState(..),
    PairingResult(..),

    pairingRequest,
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
                    | OurRequestConfirm (Maybe a)
                    | OurRequestReady
                    | PeerRequest Bytes RefDigest
                    | PeerRequestConfirm
                    | PairingDone
                    | PairingFailed

class (Typeable a, Storable a) => PairingResult a where
    pairingServiceID :: proxy a -> ServiceID
    pairingHookRequest :: ServiceHandler (PairingService a) ()
    pairingHookResponse :: String -> ServiceHandler (PairingService a) ()
    pairingHookRequestNonce :: String -> ServiceHandler (PairingService a) ()
    pairingHookRequestNonceFailed :: ServiceHandler (PairingService a) ()
    pairingHookConfirm :: a -> ServiceHandler (PairingService a) (Maybe a)
    pairingHookAccept :: a -> ServiceHandler (PairingService a) ()


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

    type ServiceState (PairingService a) = PairingState a
    emptyServiceState _ = NoPairing

    serviceHandler spacket = ((,fromStored spacket) <$> svcGet) >>= \case
        (NoPairing, PairingRequest confirm) -> do
            pairingHookRequest
            nonce <- liftIO $ getRandomBytes 32
            svcSet $ PeerRequest nonce confirm
            replyPacket $ PairingResponse nonce
        (NoPairing, _) -> return ()

        (OurRequest nonce, PairingResponse pnonce) -> do
            peer <- asks $ svcPeer
            self <- maybe (throwError "failed to validate own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            pairingHookResponse $ confirmationNumber $ nonceDigest self peer nonce pnonce
            svcSet $ OurRequestConfirm Nothing
            replyPacket $ PairingRequestNonce nonce
        (OurRequest _, _) -> do
            svcSet $ PairingFailed
            replyPacket PairingDecline

        (OurRequestConfirm _, PairingAccept x) -> do
            (svcSet . OurRequestConfirm =<< pairingHookConfirm x) `catchError` \_ -> do
                svcSet $ PairingFailed
                replyPacket PairingDecline

        (OurRequestConfirm _, _) -> do
            svcSet $ PairingFailed
            replyPacket PairingDecline

        (OurRequestReady, PairingAccept x) -> do
            pairingHookAccept x `catchError` \_ -> do
                svcSet $ PairingFailed
                replyPacket PairingDecline
        (OurRequestReady, _) -> do
            svcSet $ PairingFailed
            replyPacket PairingDecline

        (PeerRequest nonce dgst, PairingRequestNonce pnonce) -> do
            peer <- asks $ svcPeer
            self <- maybe (throwError "failed to verify own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            if dgst == nonceDigest peer self pnonce BA.empty
               then do pairingHookRequestNonce $ confirmationNumber $ nonceDigest peer self pnonce nonce
                       svcSet PeerRequestConfirm
               else do pairingHookRequestNonceFailed
                       svcSet PairingFailed
                       replyPacket PairingDecline
        (PeerRequest _ _, _) -> do
            svcSet $ PairingFailed
            replyPacket PairingDecline
        (PeerRequestConfirm, _) -> do
            svcSet $ PairingFailed
            replyPacket PairingDecline

        (PairingDone, _) -> return ()
        (PairingFailed, _) -> return ()


nonceDigest :: UnifiedIdentity -> UnifiedIdentity -> Bytes -> Bytes -> RefDigest
nonceDigest id1 id2 nonce1 nonce2 = hashToRefDigest $ serializeObject $ Rec
        [ (BC.pack "id", RecRef $ storedRef $ idData id1)
        , (BC.pack "id", RecRef $ storedRef $ idData id2)
        , (BC.pack "nonce", RecBinary $ convert nonce1)
        , (BC.pack "nonce", RecBinary $ convert nonce2)
        ]

confirmationNumber :: RefDigest -> String
confirmationNumber dgst = let (a:b:c:d:_) = map fromIntegral $ BA.unpack dgst :: [Word32]
                              str = show $ (a .|. (b `shift` 8) .|. (c `shift` 16) .|. (d `shift` 24)) `mod` (10 ^ len)
                           in replicate (len - length str) '0' ++ str
    where len = 6

pairingRequest :: forall a m proxy. (PairingResult a, MonadIO m, MonadError String m) => proxy a -> UnifiedIdentity -> Peer -> m ()
pairingRequest _ self peer = do
    nonce <- liftIO $ getRandomBytes 32
    pid <- case peerIdentity peer of
                PeerIdentityFull pid -> return pid
                _ -> throwError "incomplete peer identity"
    sendToPeerWith @(PairingService a) self peer $ \case
        NoPairing -> return (Just $ PairingRequest (nonceDigest self pid nonce BA.empty), OurRequest nonce)
        _ -> throwError "alredy in progress"

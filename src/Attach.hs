module Attach (
    AttachService,
    attachToOwner,
    attachAccept,
    attachReject,
) where

import Control.Monad.Except
import Control.Monad.Reader

import Data.ByteArray (ScrubbedBytes)
import Data.Maybe
import Data.Proxy
import qualified Data.Text as T

import Identity
import Network
import Pairing
import PubKey
import Service
import State
import Storage
import Storage.Key

type AttachService = PairingService AttachIdentity

data AttachIdentity = AttachIdentity (Stored (Signed IdentityData)) [ScrubbedBytes]

instance Storable AttachIdentity where
    store' (AttachIdentity x keys) = storeRec $ do
         storeRef "identity" x
         mapM_ (storeBinary "skey") keys

    load' = loadRec $ AttachIdentity
        <$> loadRef "identity"
        <*> loadBinaries "skey"

instance PairingResult AttachIdentity where
    pairingServiceID _ = mkServiceID "4995a5f9-2d4d-48e9-ad3b-0bf1c2a1be7f"

    type PairingVerifiedResult AttachIdentity = (UnifiedIdentity, [ScrubbedBytes])

    pairingVerifyResult (AttachIdentity sdata keys) = do
        curid <- lsIdentity . fromStored <$> svcGetLocal
        secret <- loadKey $ eiddKeyIdentity $ fromSigned curid
        sdata' <- mstore =<< signAdd secret (fromStored sdata)
        return $ do
            guard $ iddKeyIdentity (fromSigned sdata) ==
                eiddKeyIdentity (fromSigned curid)
            identity <- validateIdentity sdata'
            guard $ iddPrev (fromSigned $ idData identity) == [eiddStoredBase curid]
            return (identity, keys)

    pairingFinalizeRequest (identity, keys) = updateLocalHead_ $ \slocal -> do
        let owner = finalOwner identity
        st <- getStorage
        pkeys <- mapM (copyStored st) [ idKeyIdentity owner, idKeyMessage owner ]
        liftIO $ mapM_ storeKey $ catMaybes [ keyFromData sec pub | sec <- keys, pub <- pkeys ]

        identity' <- mergeIdentity $ updateIdentity [ lsIdentity $ fromStored slocal ] identity
        shared <- makeSharedStateUpdate st (Just owner) (lsShared $ fromStored slocal)
        mstore (fromStored slocal)
            { lsIdentity = idExtData identity'
            , lsShared = [ shared ]
            }

    pairingFinalizeResponse = do
        owner <- mergeSharedIdentity
        pid <- asks svcPeerIdentity
        secret <- loadKey $ idKeyIdentity owner
        identity <- mstore =<< sign secret =<< mstore (emptyIdentityData $ idKeyIdentity pid)
            { iddPrev = [idData pid], iddOwner = Just (idData owner) }
        skeys <- map keyGetData . catMaybes <$> mapM loadKeyMb [ idKeyIdentity owner, idKeyMessage owner ]
        return $ AttachIdentity identity skeys

    defaultPairingAttributes _ = PairingAttributes
        { pairingHookRequest = do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Attach from " ++ T.unpack (displayIdentity peer) ++ " initiated"

        , pairingHookResponse = \confirm -> do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Attach to " ++ T.unpack (displayIdentity peer) ++ ": " ++ confirm

        , pairingHookRequestNonce = \confirm -> do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Attach from " ++ T.unpack (displayIdentity peer) ++ ": " ++ confirm

        , pairingHookRequestNonceFailed = do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Failed attach from " ++ T.unpack (displayIdentity peer)

        , pairingHookConfirmedResponse = do
            svcPrint $ "Confirmed peer, waiting for updated identity"

        , pairingHookConfirmedRequest = do
            svcPrint $ "Attachment confirmed by peer"

        , pairingHookAcceptedResponse = do
            svcPrint $ "Accepted updated identity"

        , pairingHookAcceptedRequest = do
            svcPrint $ "Accepted new attached device, seding updated identity"

        , pairingHookVerifyFailed = do
            svcPrint $ "Failed to verify new identity"

        , pairingHookRejected = do
            svcPrint $ "Attachment rejected by peer"

        , pairingHookFailed = \_ -> do
            svcPrint $ "Attachement failed"
        }

attachToOwner :: (MonadIO m, MonadError String m) => Peer -> m ()
attachToOwner = pairingRequest @AttachIdentity Proxy

attachAccept :: (MonadIO m, MonadError String m) => Peer -> m ()
attachAccept = pairingAccept @AttachIdentity Proxy

attachReject :: (MonadIO m, MonadError String m) => Peer -> m ()
attachReject = pairingReject @AttachIdentity Proxy

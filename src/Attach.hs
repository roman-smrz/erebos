module Attach (
    AttachService,
    attachToOwner, attachAccept,
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

data AttachIdentity = AttachIdentity (Stored (Signed IdentityData)) [ScrubbedBytes] (Maybe UnifiedIdentity)

instance Storable AttachIdentity where
    store' (AttachIdentity x keys _) = storeRec $ do
         storeRef "identity" x
         mapM_ (storeBinary "skey") keys

    load' = loadRec $ AttachIdentity
        <$> loadRef "identity"
        <*> loadBinaries "skey"
        <*> pure Nothing

instance PairingResult AttachIdentity where
    pairingServiceID _ = mkServiceID "4995a5f9-2d4d-48e9-ad3b-0bf1c2a1be7f"

    pairingHookRequest = do
        peer <- asks $ svcPeer
        svcPrint $ "Attach from " ++ T.unpack (displayIdentity peer) ++ " initiated"

    pairingHookResponse confirm = do
        peer <- asks $ svcPeer
        svcPrint $ "Attach to " ++ T.unpack (displayIdentity peer) ++ ": " ++ confirm

    pairingHookRequestNonce confirm = do
        peer <- asks $ svcPeer
        svcPrint $ "Attach from " ++ T.unpack (displayIdentity peer) ++ ": " ++ confirm

    pairingHookRequestNonceFailed = do
        peer <- asks $ svcPeer
        svcPrint $ "Failed attach from " ++ T.unpack (displayIdentity peer)

    pairingHookConfirm (AttachIdentity sdata keys _) = do
        verifyAttachedIdentity sdata >>= \case
            Just identity -> do
                svcPrint $ "Attachment confirmed by peer"
                return $ Just $ AttachIdentity sdata keys (Just identity)
            Nothing -> do
                svcPrint $ "Failed to verify new identity"
                throwError "Failed to verify new identity"

    pairingHookAccept (AttachIdentity sdata keys _) = do
        verifyAttachedIdentity sdata >>= \case
            Just identity -> do
                svcPrint $ "Accepted updated identity"
                svcSetLocal =<< finalizeAttach identity keys =<< svcGetLocal
            Nothing -> do
                svcPrint $ "Failed to verify new identity"
                throwError "Failed to verify new identity"

attachToOwner :: (MonadIO m, MonadError String m) => (String -> IO ()) -> UnifiedIdentity -> Peer -> m ()
attachToOwner _ = pairingRequest @AttachIdentity Proxy

attachAccept :: (MonadIO m, MonadError String m) => (String -> IO ()) -> Head LocalState -> Peer -> m ()
attachAccept printMsg h peer = do
    let st = refStorage $ headRef h
        self = headLocalIdentity h
    sendToPeerWith self peer $ \case
        NoPairing -> throwError $ "none in progress"
        OurRequest {} -> throwError $ "waiting for peer"
        OurRequestConfirm Nothing -> do
            liftIO $ printMsg $ "Confirmed peer, waiting for updated identity"
            return (Nothing, OurRequestReady)
        OurRequestConfirm (Just (AttachIdentity _ _ Nothing)) -> do
            liftIO $ printMsg $ "Confirmed peer, but verification of received identity failed"
            return (Nothing, NoPairing)
        OurRequestConfirm (Just (AttachIdentity _ keys (Just identity))) -> do
            liftIO $ do
                printMsg $ "Accepted updated identity"
                updateLocalState_ h $ finalizeAttach identity keys
            return (Nothing, PairingDone)
        OurRequestReady -> throwError $ "alredy accepted, waiting for peer"
        PeerRequest {} -> throwError $ "waiting for peer"
        PeerRequestConfirm -> do
            liftIO $ printMsg $ "Accepted new attached device, seding updated identity"
            owner <- liftIO $ mergeSharedIdentity h
            PeerIdentityFull pid <- peerIdentity peer
            Just secret <- liftIO $ loadKey $ idKeyIdentity owner
            liftIO $ do
                identity <- wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData $ idKeyIdentity pid)
                    { iddPrev = [idData pid], iddOwner = Just (idData owner) }
                skeys <- map keyGetData . catMaybes <$> mapM loadKey [ idKeyIdentity owner, idKeyMessage owner ]
                return (Just $ PairingAccept $ AttachIdentity identity skeys Nothing, PairingDone)
        PairingDone -> throwError $ "alredy done"
        PairingFailed -> throwError $ "alredy failed"

verifyAttachedIdentity :: Stored (Signed IdentityData) -> ServiceHandler s (Maybe UnifiedIdentity)
verifyAttachedIdentity sdata = do
    curid <- lsIdentity . fromStored <$> svcGetLocal
    secret <- maybe (throwError "failed to load own secret key") return =<<
        liftIO (loadKey $ iddKeyIdentity $ fromStored $ signedData $ fromStored curid)
    sdata' <- liftIO $ wrappedStore (storedStorage sdata) =<< signAdd secret (fromStored sdata)
    return $ do
        guard $ iddKeyIdentity (fromStored $ signedData $ fromStored sdata) ==
            iddKeyIdentity (fromStored $ signedData $ fromStored curid)
        identity <- validateIdentity sdata'
        guard $ iddPrev (fromStored $ signedData $ fromStored $ idData identity) == [curid]
        return identity

finalizeAttach :: MonadIO m => UnifiedIdentity -> [ScrubbedBytes] -> Stored LocalState -> m (Stored LocalState)
finalizeAttach identity skeys slocal = liftIO $ do
    let owner = finalOwner identity
        st = storedStorage slocal
    pkeys <- mapM (copyStored st) [ idKeyIdentity owner, idKeyMessage owner ]
    mapM_ storeKey $ catMaybes [ keyFromData sec pub | sec <- skeys, pub <- pkeys ]

    shared <- makeSharedStateUpdate st (idDataF owner) (lsShared $ fromStored slocal)
    wrappedStore st (fromStored slocal)
        { lsIdentity = idData identity
        , lsShared = [ shared ]
        }

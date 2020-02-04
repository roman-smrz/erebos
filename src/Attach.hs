module Attach (
    AttachService,
    attachToOwner, attachAccept,
) where

import Control.Monad.Except
import Control.Monad.Reader

import Crypto.Hash
import Crypto.Random

import Data.Bits
import Data.ByteArray (Bytes, ScrubbedBytes, convert)
import qualified Data.ByteArray as BA
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as BL
import Data.Maybe
import qualified Data.Text as T
import Data.Word

import Identity
import Network
import PubKey
import Service
import State
import Storage
import Storage.Key

data AttachService = AttachRequest RefDigest
                   | AttachResponse Bytes
                   | AttachRequestNonce Bytes
                   | AttachIdentity (Stored (Signed IdentityData)) [ScrubbedBytes]
                   | AttachDecline

data AttachState = NoAttach
                 | OurRequest Bytes
                 | OurRequestConfirm (Maybe (UnifiedIdentity, [ScrubbedBytes]))
                 | OurRequestReady
                 | PeerRequest Bytes RefDigest
                 | PeerRequestConfirm
                 | AttachDone
                 | AttachFailed

instance Storable AttachService where
    store' at = storeRec $ do
        case at of
             AttachRequest x -> storeBinary "request" x
             AttachResponse x -> storeBinary "response" x
             AttachRequestNonce x -> storeBinary "reqnonce" x
             AttachIdentity x keys -> do
                 storeRef "identity" x
                 mapM_ (storeBinary "skey") keys
             AttachDecline -> storeText "decline" ""

    load' = loadRec $ do
        (req :: Maybe Bytes) <- loadMbBinary "request"
        rsp <- loadMbBinary "response"
        rnonce <- loadMbBinary "reqnonce"
        aid <- loadMbRef "identity"
        skeys <- loadBinaries "skey"
        (decline :: Maybe T.Text) <- loadMbText "decline"
        let res = catMaybes
                [ AttachRequest <$> (digestFromByteString =<< req)
                , AttachResponse <$> rsp
                , AttachRequestNonce <$> rnonce
                , AttachIdentity <$> aid <*> pure skeys
                , const AttachDecline <$> decline
                ]
        case res of
             x:_ -> return x
             [] -> throwError "invalid attach stange"

instance Service AttachService where
    serviceID _ = mkServiceID "4995a5f9-2d4d-48e9-ad3b-0bf1c2a1be7f"

    type ServiceState AttachService = AttachState
    emptyServiceState _ = NoAttach

    serviceHandler spacket = ((,fromStored spacket) <$> svcGet) >>= \case
        (NoAttach, AttachRequest confirm) -> do
            peer <- asks $ svcPeer
            svcPrint $ "Attach from " ++ T.unpack (displayIdentity peer) ++ " initiated"
            nonce <- liftIO $ getRandomBytes 32
            svcSet $ PeerRequest nonce confirm
            replyPacket $ AttachResponse nonce
        (NoAttach, _) -> return ()

        (OurRequest nonce, AttachResponse pnonce) -> do
            peer <- asks $ svcPeer
            self <- maybe (throwError "failed to verify own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            svcPrint $ "Attach to " ++ T.unpack (displayIdentity peer) ++ ": " ++ confirmationNumber (nonceDigest self peer nonce pnonce)
            svcSet $ OurRequestConfirm Nothing
            replyPacket $ AttachRequestNonce nonce
        (OurRequest _, _) -> do
            svcSet $ AttachFailed
            replyPacket AttachDecline

        (OurRequestConfirm _, AttachIdentity sdata keys) -> do
            verifyAttachedIdentity sdata >>= \case
                Just owner -> do
                    svcPrint $ "Attachment confirmed by peer"
                    svcSet $ OurRequestConfirm $ Just (owner, keys)
                Nothing -> do
                    svcPrint $ "Failed to verify new identity"
                    svcSet $ AttachFailed
                    replyPacket AttachDecline
        (OurRequestConfirm _, _) -> do
            svcSet $ AttachFailed
            replyPacket AttachDecline

        (OurRequestReady, AttachIdentity sdata keys) -> do
            verifyAttachedIdentity sdata >>= \case
                Just identity -> do
                    svcPrint $ "Accepted updated identity"
                    st <- storedStorage <$> svcGetLocal
                    finalizeAttach st identity keys
                Nothing -> do
                    svcPrint $ "Failed to verify new identity"
                    svcSet $ AttachFailed
                    replyPacket AttachDecline
        (OurRequestReady, _) -> do
            svcSet $ AttachFailed
            replyPacket AttachDecline

        (PeerRequest nonce dgst, AttachRequestNonce pnonce) -> do
            peer <- asks $ svcPeer
            self <- maybe (throwError "failed to verify own identity") return .
                validateIdentity . lsIdentity . fromStored =<< svcGetLocal
            if dgst == nonceDigest peer self pnonce BA.empty
               then do svcPrint $ "Attach from " ++ T.unpack (displayIdentity peer) ++ ": " ++ confirmationNumber (nonceDigest peer self pnonce nonce)
                       svcSet PeerRequestConfirm
               else do svcPrint $ "Failed attach from " ++ T.unpack (displayIdentity peer)
                       svcSet AttachFailed
                       replyPacket AttachDecline
        (PeerRequest _ _, _) -> do
            svcSet $ AttachFailed
            replyPacket AttachDecline
        (PeerRequestConfirm, _) -> do
            svcSet $ AttachFailed
            replyPacket AttachDecline

        (AttachDone, _) -> return ()
        (AttachFailed, _) -> return ()

attachToOwner :: (MonadIO m, MonadError String m) => (String -> IO ()) -> UnifiedIdentity -> Peer -> m ()
attachToOwner _ self peer = do
    nonce <- liftIO $ getRandomBytes 32
    pid <- case peerIdentity peer of
                PeerIdentityFull pid -> return pid
                _ -> throwError "incomplete peer identity"
    sendToPeerWith self peer $ \case
        NoAttach -> return (Just $ AttachRequest (nonceDigest self pid nonce BA.empty), OurRequest nonce)
        _ -> throwError "alredy in progress"

attachAccept :: (MonadIO m, MonadError String m) => (String -> IO ()) -> UnifiedIdentity -> Peer -> m ()
attachAccept printMsg self peer = do
    let st = storedStorage $ idData self
    sendToPeerWith self peer $ \case
        NoAttach -> throwError $ "none in progress"
        OurRequest {} -> throwError $ "waiting for peer"
        OurRequestConfirm Nothing -> do
            liftIO $ printMsg $ "Confirmed peer, waiting for updated identity"
            return (Nothing, OurRequestReady)
        OurRequestConfirm (Just (identity, keys)) -> do
            liftIO $ printMsg $ "Accepted updated identity"
            finalizeAttach st identity keys
            return (Nothing, AttachDone)
        OurRequestReady -> throwError $ "alredy accepted, waiting for peer"
        PeerRequest {} -> throwError $ "waiting for peer"
        PeerRequestConfirm -> do
            liftIO $ printMsg $ "Accepted new attached device, seding updated identity"
            owner <- liftIO $ mergeSharedIdentity st
            PeerIdentityFull pid <- return $ peerIdentity peer
            Just secret <- liftIO $ loadKey $ idKeyIdentity owner
            liftIO $ do
                identity <- wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData $ idKeyIdentity pid)
                    { iddPrev = [idData pid], iddOwner = Just (idData owner) }
                skeys <- map keyGetData . catMaybes <$> mapM loadKey [ idKeyIdentity owner, idKeyMessage owner ]
                return (Just $ AttachIdentity identity skeys, NoAttach)
        AttachDone -> throwError $ "alredy done"
        AttachFailed -> throwError $ "alredy failed"


nonceDigest :: UnifiedIdentity -> UnifiedIdentity -> Bytes -> Bytes -> RefDigest
nonceDigest id1 id2 nonce1 nonce2 = hashFinalize $ hashUpdates hashInit $
    BL.toChunks $ serializeObject $ Rec
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


finalizeAttach :: MonadIO m => Storage -> UnifiedIdentity -> [ScrubbedBytes] -> m ()
finalizeAttach st identity skeys = do
    liftIO $ updateLocalState_ st $ \slocal -> do
        let owner = finalOwner identity
        pkeys <- mapM (copyStored st) [ idKeyIdentity owner, idKeyMessage owner ]
        mapM_ storeKey $ catMaybes [ keyFromData sec pub | sec <- skeys, pub <- pkeys ]

        shared <- makeSharedStateUpdate st (idDataF owner) (lsShared $ fromStored slocal)
        wrappedStore st (fromStored slocal)
            { lsIdentity = idData identity
            , lsShared = [ shared ]
            }

module Contact (
    Contact(..),
    contactView,

    ContactService,
    contactRequest,
    contactAccept,
) where

import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.Maybe
import Data.Proxy
import Data.Text (Text)
import qualified Data.Text as T

import Identity
import Network
import Pairing
import PubKey
import Service
import State
import Storage
import Storage.Merge

data Contact = Contact
    { contactIdentity :: ComposedIdentity
    , contactName :: Maybe Text
    }

data ContactData = ContactData
    { cdPrev :: [Stored ContactData]
    , cdIdentity :: [Stored (Signed IdentityData)]
    , cdName :: Maybe Text
    }

instance Storable ContactData where
    store' x = storeRec $ do
        mapM_ (storeRef "PREV") $ cdPrev x
        mapM_ (storeRef "identity") $ cdIdentity x
        storeMbText "name" $ cdName x

    load' = loadRec $ ContactData
        <$> loadRefs "PREV"
        <*> loadRefs "identity"
        <*> loadMbText "name"

instance SharedType ContactData where
    sharedTypeID _ = mkSharedTypeID "34fbb61e-6022-405f-b1b3-a5a1abecd25e"

contactView :: [Stored ContactData] -> [Contact]
contactView = helper [] . filterAncestors
    where helper used (x:xs) | Just cid <- validateIdentityF (cdIdentity (fromStored x))
                             , not $ any (sameIdentity cid) used
                             = Contact { contactIdentity = cid
                                       , contactName = lookupProperty cid cdName (x:xs)
                                       } : helper (cid:used) xs
                             | otherwise = helper used xs
          helper _ [] = []

lookupProperty :: forall a. ComposedIdentity -> (ContactData -> Maybe a) -> [Stored ContactData] -> Maybe a
lookupProperty idt sel = join . fmap (sel . fromStored) . listToMaybe . filterAncestors . helper
    where helper (x:xs) | Just cid <- validateIdentityF (cdIdentity (fromStored x))
                        , cid `sameIdentity` idt
                        , Just _ <- sel $ fromStored x
                        = x : helper xs
                        | otherwise = helper $ cdPrev (fromStored x) ++ xs
          helper [] = []


type ContactService = PairingService ContactAccepted

data ContactAccepted = ContactAccepted

instance Storable ContactAccepted where
    store' ContactAccepted = storeRec $ do
        storeText "accept" ""
    load' = loadRec $ do
        (_ :: T.Text) <- loadText "accept"
        return ContactAccepted

instance PairingResult ContactAccepted where
    pairingServiceID _ = mkServiceID "d9c37368-0da1-4280-93e9-d9bd9a198084"

    pairingHookRequest = do
        peer <- asks $ svcPeer
        svcPrint $ "Contact pairing from " ++ T.unpack (displayIdentity peer) ++ " initiated"

    pairingHookResponse confirm = do
        peer <- asks $ svcPeer
        svcPrint $ "Confirm contact " ++ T.unpack (displayIdentity $ finalOwner peer) ++ ": " ++ confirm

    pairingHookRequestNonce confirm = do
        peer <- asks $ svcPeer
        svcPrint $ "Contact request from " ++ T.unpack (displayIdentity $ finalOwner peer) ++ ": " ++ confirm

    pairingHookRequestNonceFailed = do
        peer <- asks $ svcPeer
        svcPrint $ "Failed contact request from " ++ T.unpack (displayIdentity peer)

    pairingHookConfirm ContactAccepted = do
        svcPrint $ "Contact confirmed by peer"
        return $ Just ContactAccepted

    pairingHookAccept ContactAccepted = return ()

contactRequest :: (MonadIO m, MonadError String m) => (String -> IO ()) -> UnifiedIdentity -> Peer -> m ()
contactRequest _ = pairingRequest @ContactAccepted Proxy

contactAccept :: (MonadIO m, MonadError String m) => (String -> IO ()) -> Head LocalState -> Peer -> m ()
contactAccept printMsg h peer = do
    let self = headLocalIdentity h
    sendToPeerWith self peer $ \case
        NoPairing -> throwError $ "none in progress"
        OurRequest {} -> throwError $ "waiting for peer"
        OurRequestConfirm Nothing -> do
            liftIO $ printMsg $ "Contact accepted, waiting for peer confirmation"
            return (Nothing, OurRequestReady)
        OurRequestConfirm (Just ContactAccepted) -> do
            PeerIdentityFull pid <- return $ peerIdentity peer
            liftIO $ do
                printMsg $ "Contact accepted"
                updateLocalState_ h $ finalizeContact pid
            return (Nothing, PairingDone)
        OurRequestReady -> throwError $ "alredy accepted, waiting for peer"
        PeerRequest {} -> throwError $ "waiting for peer"
        PeerRequestConfirm -> do
            PeerIdentityFull pid <- return $ peerIdentity peer
            liftIO $ do
                printMsg $ "Contact accepted"
                updateLocalState_ h $ finalizeContact pid
            return (Just $ PairingAccept ContactAccepted, PairingDone)
        PairingDone -> throwError $ "alredy done"
        PairingFailed -> throwError $ "alredy failed"

finalizeContact :: MonadIO m => UnifiedIdentity -> Stored LocalState -> m (Stored LocalState)
finalizeContact identity slocal = liftIO $ do
    let st = storedStorage slocal
    contact <- wrappedStore st ContactData
            { cdPrev = lookupSharedValue $ lsShared $ fromStored slocal
            , cdIdentity = idDataF $ finalOwner identity
            , cdName = Nothing
            }
    shared <- makeSharedStateUpdate st [contact] (lsShared $ fromStored slocal)
    wrappedStore st (fromStored slocal) { lsShared = [shared] }

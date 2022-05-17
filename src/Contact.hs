module Contact (
    Contact(..),
    contactView,

    Contacts,
    toContactList,

    ContactService,
    contactRequest,
    contactAccept,
    contactReject,
) where

import Control.Arrow
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

data Contacts = Contacts [Stored ContactData] [Contact]

toContactList :: Contacts -> [Contact]
toContactList (Contacts _ list) = list

instance Storable ContactData where
    store' x = storeRec $ do
        mapM_ (storeRef "PREV") $ cdPrev x
        mapM_ (storeRef "identity") $ cdIdentity x
        storeMbText "name" $ cdName x

    load' = loadRec $ ContactData
        <$> loadRefs "PREV"
        <*> loadRefs "identity"
        <*> loadMbText "name"

instance Mergeable Contacts where
    type Component Contacts = ContactData
    mergeSorted cdata = Contacts cdata $ contactView cdata
    toComponents (Contacts cdata _) = cdata

instance SharedType Contacts where
    sharedTypeID _ = mkSharedTypeID "34fbb61e-6022-405f-b1b3-a5a1abecd25e"

contactView :: [Stored ContactData] -> [Contact]
contactView = helper []
    where helper used = filterAncestors >>> \case
              x:xs | Just cid <- validateIdentityF (cdIdentity (fromStored x))
                   , not $ any (sameIdentity cid) used
                   -> Contact { contactIdentity = cid
                              , contactName = lookupProperty cid cdName (x:xs)
                              } : helper (cid:used) (cdPrev (fromStored x) ++ xs)
                   | otherwise -> helper used (cdPrev (fromStored x) ++ xs)
              [] -> []

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

    pairingVerifyResult = return . Just

    pairingFinalizeRequest ContactAccepted = do
        pid <- asks svcPeerIdentity
        finalizeContact pid

    pairingFinalizeResponse = do
        pid <- asks svcPeerIdentity
        finalizeContact pid
        return ContactAccepted

    defaultPairingAttributes _ = PairingAttributes
        { pairingHookRequest = do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Contact pairing from " ++ T.unpack (displayIdentity peer) ++ " initiated"

        , pairingHookResponse = \confirm -> do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Confirm contact " ++ T.unpack (displayIdentity $ finalOwner peer) ++ ": " ++ confirm

        , pairingHookRequestNonce = \confirm -> do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Contact request from " ++ T.unpack (displayIdentity $ finalOwner peer) ++ ": " ++ confirm

        , pairingHookRequestNonceFailed = do
            peer <- asks $ svcPeerIdentity
            svcPrint $ "Failed contact request from " ++ T.unpack (displayIdentity peer)

        , pairingHookConfirmedResponse = do
            svcPrint $ "Contact accepted, waiting for peer confirmation"

        , pairingHookConfirmedRequest = do
            svcPrint $ "Contact confirmed by peer"

        , pairingHookAcceptedResponse = do
            svcPrint $ "Contact accepted"

        , pairingHookAcceptedRequest = do
            svcPrint $ "Contact accepted"

        , pairingHookVerifyFailed = return ()

        , pairingHookRejected = do
            svcPrint $ "Contact rejected by peer"

        , pairingHookFailed = do
            svcPrint $ "Contact failed"
        }

contactRequest :: (MonadIO m, MonadError String m) => Peer -> m ()
contactRequest = pairingRequest @ContactAccepted Proxy

contactAccept :: (MonadIO m, MonadError String m) => Peer -> m ()
contactAccept = pairingAccept @ContactAccepted Proxy

contactReject :: (MonadIO m, MonadError String m) => Peer -> m ()
contactReject = pairingReject @ContactAccepted Proxy

finalizeContact :: MonadHead LocalState m => UnifiedIdentity -> m ()
finalizeContact identity = updateSharedState_ $ \(Contacts prev _) -> do
    let st = storedStorage $ idData identity
    contact <- wrappedStore st ContactData
            { cdPrev = prev
            , cdIdentity = idDataF $ finalOwner identity
            , cdName = Nothing
            }
    return $ Contacts [contact] (contactView [contact])

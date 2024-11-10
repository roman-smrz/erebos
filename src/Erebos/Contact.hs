module Erebos.Contact (
    Contact,
    contactIdentity,
    contactCustomName,
    contactName,

    contactSetName,

    ContactService,
    contactRequest,
    contactAccept,
    contactReject,
) where

import Control.Monad
import Control.Monad.Except
import Control.Monad.Reader

import Data.Maybe
import Data.Proxy
import Data.Text (Text)
import qualified Data.Text as T

import Erebos.Identity
import Erebos.Network
import Erebos.Pairing
import Erebos.PubKey
import Erebos.Service
import Erebos.Set
import Erebos.State
import Erebos.Storable
import Erebos.Storage.Merge

data Contact = Contact
    { contactData :: [Stored ContactData]
    , contactIdentity_ :: Maybe ComposedIdentity
    , contactCustomName_ :: Maybe Text
    }

data ContactData = ContactData
    { cdPrev :: [Stored ContactData]
    , cdIdentity :: [Stored (Signed ExtendedIdentityData)]
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

instance Mergeable Contact where
    type Component Contact = ContactData

    mergeSorted cdata = Contact
        { contactData = cdata
        , contactIdentity_ = validateExtendedIdentityF $ concat $ findProperty ((\case [] -> Nothing; xs -> Just xs) . cdIdentity) cdata
        , contactCustomName_ = findPropertyFirst cdName cdata
        }

    toComponents = contactData

instance SharedType (Set Contact) where
    sharedTypeID _ = mkSharedTypeID "34fbb61e-6022-405f-b1b3-a5a1abecd25e"

contactIdentity :: Contact -> Maybe ComposedIdentity
contactIdentity = contactIdentity_

contactCustomName :: Contact -> Maybe Text
contactCustomName = contactCustomName_

contactName :: Contact -> Text
contactName c = fromJust $ msum
    [ contactCustomName c
    , idName =<< contactIdentity c
    , Just T.empty
    ]

contactSetName :: MonadHead LocalState m => Contact -> Text -> Set Contact -> m (Set Contact)
contactSetName contact name set = do
    st <- getStorage
    cdata <- wrappedStore st ContactData
        { cdPrev = toComponents contact
        , cdIdentity = []
        , cdName = Just name
        }
    storeSetAdd st (mergeSorted @Contact [cdata]) set


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

        , pairingHookFailed = \_ -> do
            svcPrint $ "Contact failed"
        }

contactRequest :: (MonadIO m, MonadError String m) => Peer -> m ()
contactRequest = pairingRequest @ContactAccepted Proxy

contactAccept :: (MonadIO m, MonadError String m) => Peer -> m ()
contactAccept = pairingAccept @ContactAccepted Proxy

contactReject :: (MonadIO m, MonadError String m) => Peer -> m ()
contactReject = pairingReject @ContactAccepted Proxy

finalizeContact :: MonadHead LocalState m => UnifiedIdentity -> m ()
finalizeContact identity = updateLocalHead_ $ updateSharedState_ $ \contacts -> do
    st <- getStorage
    cdata <- wrappedStore st ContactData
        { cdPrev = []
        , cdIdentity = idExtDataF $ finalOwner identity
        , cdName = Nothing
        }
    storeSetAdd st (mergeSorted @Contact [cdata]) contacts

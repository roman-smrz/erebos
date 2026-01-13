{-# LANGUAGE OverloadedStrings #-}

module Erebos.Invite (
    Invite(..),
    InviteData(..),
    InviteToken, showInviteToken, textInviteToken, parseInviteToken,
    InviteService,
    InviteServiceAttributes(..),

    createSingleContactInvite,
    acceptInvite,
) where

import Control.Arrow
import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class
import Control.Monad.Reader

import Crypto.Random

import Data.ByteArray (ByteArray, ByteArrayAccess)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BC
import Data.Foldable
import Data.Ord
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding

import Erebos.Contact
import Erebos.Identity
import Erebos.Network
import Erebos.Object
import Erebos.PubKey
import Erebos.Service
import Erebos.Set
import Erebos.State
import Erebos.Storable
import Erebos.Storage.Merge
import Erebos.Util


data Invite = Invite
    { inviteData :: [ Stored InviteData ]
    , inviteToken :: Maybe InviteToken
    , inviteAccepted :: [ Stored (Signed ExtendedIdentityData) ]
    , inviteContact :: Maybe Text
    }

data InviteData = InviteData
    { invdPrev :: [ Stored InviteData ]
    , invdToken :: Maybe InviteToken
    , invdAccepted :: Maybe (Stored (Signed ExtendedIdentityData))
    , invdContact :: Maybe Text
    }

instance Storable InviteData where
    store' x = storeRec $ do
        mapM_ (storeRef "PREV") $ invdPrev x
        mapM_ (storeBinary "token") $ invdToken x
        mapM_ (storeRef "accepted") $ invdAccepted x
        mapM_ (storeText "contact") $ invdContact x

    load' = loadRec $ InviteData
        <$> loadRefs "PREV"
        <*> loadMbBinary "token"
        <*> loadMbRef "accepted"
        <*> loadMbText "contact"


newtype InviteToken = InviteToken ByteString
    deriving (Eq, Ord, Semigroup, Monoid, ByteArray, ByteArrayAccess)

showInviteToken :: InviteToken -> String
showInviteToken (InviteToken token) = BC.unpack (showHex token)

textInviteToken :: InviteToken -> Text
textInviteToken (InviteToken token) = decodeUtf8 (showHex token)

parseInviteToken :: Text -> Maybe InviteToken
parseInviteToken text = InviteToken <$> (readHex $ encodeUtf8 text)


instance Mergeable Invite where
    type Component Invite = InviteData

    mergeSorted invdata = Invite
        { inviteData = invdata
        , inviteToken = findPropertyFirst invdToken invdata
        , inviteAccepted = findProperty invdAccepted invdata
        , inviteContact = findPropertyFirst invdContact invdata
        }

    toComponents = inviteData

instance SharedType (Set Invite) where
    sharedTypeID _ = mkSharedTypeID "78da787a-9380-432e-a51d-532a30d27b3d"


createSingleContactInvite :: MonadHead LocalState m => Text -> m Invite
createSingleContactInvite name = do
    token <- liftIO $ getRandomBytes 32
    invite <- mergeSorted @Invite . (: []) <$> mstore InviteData
        { invdPrev = []
        , invdToken = Just token
        , invdAccepted = Nothing
        , invdContact = Just name
        }
    updateLocalState_ $ updateSharedState_ $ \invites -> do
        storeSetAdd invite invites
    return invite

identityOwnerDigests :: Foldable f => Identity f -> [ RefDigest ]
identityOwnerDigests pid = map (refDigest . storedRef) $ concatMap toList $ toList $ generations $ idExtDataF $ finalOwner pid

acceptInvite :: (MonadIO m, MonadError e m, FromErebosError e) => Server -> RefDigest -> InviteToken -> m ()
acceptInvite server from token = do
    let matchPeer peer = do
            getPeerIdentity peer >>= \case
                PeerIdentityFull pid -> do
                    return $ from `elem` identityOwnerDigests pid
                _ -> return False
    liftIO (findPeer server matchPeer) >>= \case
        Just peer -> runPeerService @InviteService peer $ do
            svcModify (token :)
            replyPacket $ AcceptInvite token
        Nothing -> do
            throwOtherError "peer not found"


data InviteService
    = AcceptInvite InviteToken
    | InvalidInvite InviteToken
    | ContactInvite InviteToken (Maybe Text)
    | UnknownInvitePacket

data InviteServiceAttributes = InviteServiceAttributes
    { inviteHookAccepted :: InviteToken -> ServiceHandler InviteService ()
    , inviteHookReplyContact :: InviteToken -> Maybe Text -> ServiceHandler InviteService ()
    , inviteHookReplyInvalid :: InviteToken -> ServiceHandler InviteService ()
    }

defaultInviteServiceAttributes :: InviteServiceAttributes
defaultInviteServiceAttributes = InviteServiceAttributes
    { inviteHookAccepted = \token -> do
        pid <- asks $ svcPeerIdentity
        svcPrint $ T.unpack $ "Invite accepted by " <> displayIdentity pid
            <> " (token: " <> textInviteToken token <> ")"
    , inviteHookReplyContact = \token mbName -> do
        pid <- asks $ svcPeerIdentity
        svcPrint $ T.unpack $ "Invite confirmed by " <> displayIdentity pid
            <> (maybe "" (" with name " <>) mbName)
            <> " (token: " <> textInviteToken token <> ")"
    , inviteHookReplyInvalid = \token -> do
        pid <- asks $ svcPeerIdentity
        svcPrint $ T.unpack $ "Invite rejected as invalid by " <> displayIdentity pid
            <> " (token: " <> textInviteToken token <> ")"
    }

instance Storable InviteService where
    store' x = storeRec $ case x of
        AcceptInvite token -> storeBinary "accept" token
        InvalidInvite token -> storeBinary "invalid" token
        ContactInvite token mbName -> do
            storeBinary "valid" token
            maybe (storeEmpty "contact") (storeText "contact") mbName
        UnknownInvitePacket -> return ()

    load' = loadRec $ msum
        [ AcceptInvite <$> loadBinary "accept"
        , InvalidInvite <$> loadBinary "invalid"
        , ContactInvite <$> loadBinary "valid" <*> msum
            [ return Nothing <* loadEmpty "contact"
            , Just <$> loadText "contact"
            ]
        , return UnknownInvitePacket
        ]

instance Service InviteService where
    serviceID _ = mkServiceID "70bff715-6856-43a0-8c58-007a06a26eb1"

    type ServiceState InviteService = [ InviteToken ] -- accepted invites, waiting for reply
    emptyServiceState _ = []

    type ServiceAttributes InviteService = InviteServiceAttributes
    defaultServiceAttributes _ = defaultInviteServiceAttributes

    serviceHandler = fromStored >>> \case
        AcceptInvite token -> do
            invites <- fromSetBy (comparing inviteToken) . lookupSharedValue . lsShared . fromStored <$> getLocalHead
            case find ((Just token ==) . inviteToken) invites of
                Just invite
                    | Just name <- inviteContact invite
                    , [] <- inviteAccepted invite
                    -> do
                        asks (inviteHookAccepted . svcAttributes) >>= ($ token)
                        identity <- asks svcPeerIdentity
                        cdata <- mstore ContactData
                            { cdPrev = []
                            , cdIdentity = idExtDataF $ finalOwner identity
                            , cdName = Just name
                            }
                        invdata <- mstore InviteData
                            { invdPrev = inviteData invite
                            , invdToken = Nothing
                            , invdAccepted = Just (idExtData identity)
                            , invdContact = Nothing
                            }
                        updateLocalState_ $ updateSharedState_ $ storeSetAdd (mergeSorted @Contact [ cdata ])
                        updateLocalState_ $ updateSharedState_ $ storeSetAdd (mergeSorted @Invite [ invdata ])
                        replyPacket $ ContactInvite token Nothing

                    | otherwise -> do
                        replyPacket $ InvalidInvite token

                Nothing -> do
                    replyPacket $ InvalidInvite token

        InvalidInvite token -> do
            asks (inviteHookReplyInvalid . svcAttributes) >>= ($ token)
            svcModify $ filter (/= token)

        ContactInvite token mbName -> do
            asks (inviteHookReplyContact . svcAttributes) >>= ($ mbName) . ($ token)
            waitingTokens <- svcGet
            if token `elem` waitingTokens
              then do
                svcSet $ filter (/= token) waitingTokens
                identity <- asks svcPeerIdentity
                cdata <- mstore ContactData
                    { cdPrev = []
                    , cdIdentity = idExtDataF $ finalOwner identity
                    , cdName = Nothing
                    }
                updateLocalState_ $ updateSharedState_ $ storeSetAdd (mergeSorted @Contact [ cdata ])
              else do
                svcPrint $ "Received unexpected invite response for " <> BC.unpack (showHex token)

        UnknownInvitePacket -> do
            svcPrint $ "Received unknown invite packet"

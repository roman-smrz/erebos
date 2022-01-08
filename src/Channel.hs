module Channel (
    Channel,
    ChannelRequest, ChannelRequestData(..),
    ChannelAccept, ChannelAcceptData(..),

    createChannelRequest,
    acceptChannelRequest,
    acceptedChannel,

    channelEncrypt,
    channelDecrypt,
) where

import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Except

import Crypto.Cipher.AES
import Crypto.Cipher.Types
import Crypto.Error

import Data.Binary
import Data.ByteArray
import Data.ByteArray qualified as BA
import Data.ByteString qualified as B
import Data.ByteString.Lazy qualified as BL
import Data.List
import Data.Text qualified as T

import Identity
import PubKey
import Storage

data Channel = Channel
    { chPeers :: [Stored (Signed IdentityData)]
    , chKey :: ScrubbedBytes
    , chNonceFixedOur :: Bytes
    , chNonceFixedPeer :: Bytes
    , chNonceCounter :: MVar Word64
    }

type ChannelRequest = Signed ChannelRequestData

data ChannelRequestData = ChannelRequest
    { crPeers :: [Stored (Signed IdentityData)]
    , crKey :: Stored PublicKexKey
    }
    deriving (Show)

type ChannelAccept = Signed ChannelAcceptData

data ChannelAcceptData = ChannelAccept
    { caRequest :: Stored ChannelRequest
    , caKey :: Stored PublicKexKey
    }


instance Storable ChannelRequestData where
    store' cr = storeRec $ do
        mapM_ (storeRef "peer") $ crPeers cr
        storeText "enc" $ T.pack "aes-128-gcm"
        storeRef "key" $ crKey cr

    load' = loadRec $ do
        enc <- loadText "enc"
        guard $ enc == "aes-128-gcm"
        ChannelRequest
            <$> loadRefs "peer"
            <*> loadRef "key"

instance Storable ChannelAcceptData where
    store' ca = storeRec $ do
        storeRef "req" $ caRequest ca
        storeText "enc" $ T.pack "aes-128-gcm"
        storeRef "key" $ caKey ca

    load' = loadRec $ do
        enc <- loadText "enc"
        guard $ enc == "aes-128-gcm"
        ChannelAccept
            <$> loadRef "req"
            <*> loadRef "key"


createChannelRequest :: (MonadIO m) => Storage -> UnifiedIdentity -> UnifiedIdentity -> m (Stored ChannelRequest)
createChannelRequest st self peer = liftIO $ do
    (_, xpublic) <- generateKeys st
    Just skey <- loadKey $ idKeyMessage self
    wrappedStore st =<< sign skey =<< wrappedStore st ChannelRequest { crPeers = sort [idData self, idData peer], crKey = xpublic }

acceptChannelRequest :: (MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelRequest -> m (Stored ChannelAccept, Channel)
acceptChannelRequest self peer req = do
    when ((crPeers $ fromStored $ signedData $ fromStored req) /= sort (map idData [self, peer])) $
        throwError $ "mismatched peers in channel request"
    when (idKeyMessage peer `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)) $
        throwError $ "channel requent not signed by peer"

    let st = storedStorage req
        KeySizeFixed ksize = cipherKeySize (undefined :: AES128)
    liftIO $ do
        (xsecret, xpublic) <- generateKeys st
        Just skey <- loadKey $ idKeyMessage self
        acc <- wrappedStore st =<< sign skey =<< wrappedStore st ChannelAccept { caRequest = req, caKey = xpublic }
        counter <- newMVar 0
        return $ (acc,) $ Channel
            { chPeers = crPeers $ fromStored $ signedData $ fromStored req
            , chKey = BA.take ksize $ dhSecret xsecret $
                fromStored $ crKey $ fromStored $ signedData $ fromStored req
            , chNonceFixedOur  = BA.pack [ 2, 0, 0, 0, 0, 0 ]
            , chNonceFixedPeer = BA.pack [ 1, 0, 0, 0, 0, 0 ]
            , chNonceCounter = counter
            }

acceptedChannel :: (MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelAccept -> m Channel
acceptedChannel self peer acc = do
    let req = caRequest $ fromStored $ signedData $ fromStored acc
        KeySizeFixed ksize = cipherKeySize (undefined :: AES128)

    when ((crPeers $ fromStored $ signedData $ fromStored req) /= sort (map idData [self, peer])) $
        throwError $ "mismatched peers in channel accept"
    when (idKeyMessage peer `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored acc)) $
        throwError $ "channel accept not signed by peer"
    when (idKeyMessage self `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)) $
        throwError $ "original channel request not signed by us"

    xsecret <- liftIO (loadKey $ crKey $ fromStored $ signedData $ fromStored req) >>= \case
        Just key -> return key
        Nothing  -> throwError $ "secret key not found"
    counter <- liftIO $ newMVar 0
    return $ Channel
        { chPeers = crPeers $ fromStored $ signedData $ fromStored req
        , chKey = BA.take ksize $ dhSecret xsecret $
            fromStored $ caKey $ fromStored $ signedData $ fromStored acc
        , chNonceFixedOur  = BA.pack [ 1, 0, 0, 0, 0, 0 ]
        , chNonceFixedPeer = BA.pack [ 2, 0, 0, 0, 0, 0 ]
        , chNonceCounter = counter
        }


channelEncrypt :: (ByteArray ba, MonadIO m, MonadError String m) => Channel -> ba -> m ba
channelEncrypt ch plain = do
    cipher <- case cipherInit $ chKey ch of
                   CryptoPassed (cipher :: AES128) -> return cipher
                   _ -> throwError "failed to init AES128 cipher"
    let bsize = blockSize cipher
    count <- liftIO $ modifyMVar (chNonceCounter ch) $ \c -> return (c + 1, c)
    let cbytes = convert $ BL.toStrict $ BL.drop 2 $ encode count
        iv = chNonceFixedOur ch `append` cbytes
    aead <- case aeadInit AEAD_GCM cipher iv of
                 CryptoPassed aead -> return aead
                 _ -> throwError "failed to init AEAD_GCM"
    let (tag, ctext) = aeadSimpleEncrypt aead B.empty plain bsize
    return $ BA.concat [ BA.pack [ 0, 0 ], convert cbytes, ctext, convert tag ]

channelDecrypt :: (ByteArray ba, MonadError String m) => Channel -> ba -> m ba
channelDecrypt ch body = do
    cipher <- case cipherInit $ chKey ch of
                   CryptoPassed (cipher :: AES128) -> return cipher
                   _ -> throwError "failed to init AES128 cipher"
    let bsize = blockSize cipher
        (cbytes, body') = BA.splitAt 8 body
        iv = chNonceFixedPeer ch `append` convert (BA.drop 2 cbytes)
        (ctext, tag) = BA.splitAt (BA.length body' - bsize) body'
    aead <- case aeadInit AEAD_GCM cipher iv of
                 CryptoPassed aead -> return aead
                 _ -> throwError "failed to init AEAD_GCM"
    case aeadSimpleDecrypt aead B.empty ctext (AuthTag $ convert tag) of
         Just plain -> return plain
         Nothing -> throwError "failed to decrypt data"

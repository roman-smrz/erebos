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

import Control.Monad
import Control.Monad.Except

import Crypto.Cipher.AES
import Crypto.Cipher.Types
import Crypto.Error
import Crypto.Random

import Data.ByteArray
import qualified Data.ByteArray as BA
import Data.ByteString (ByteString)
import qualified Data.ByteString as B
import Data.List
import qualified Data.Text as T

import Identity
import PubKey
import Storage

data Channel = Channel
    { chPeers :: [Stored (Signed IdentityData)]
    , chKey :: ScrubbedBytes
    }
    deriving (Show)

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


instance Storable Channel where
    store' ch = storeRec $ do
        mapM_ (storeRef "peer") $ chPeers ch
        storeText "enc" $ T.pack "aes-128-gcm"
        storeBinary "key" $ chKey ch

    load' = loadRec $ do
        enc <- loadText "enc"
        guard $ enc == "aes-128-gcm"
        Channel
            <$> loadRefs "peer"
            <*> loadBinary "key"

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

acceptChannelRequest :: (MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelRequest -> m (Stored ChannelAccept, Stored Channel)
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
        ch <- wrappedStore st Channel
            { chPeers = crPeers $ fromStored $ signedData $ fromStored req
            , chKey = BA.take ksize $ dhSecret xsecret $
                fromStored $ crKey $ fromStored $ signedData $ fromStored req
            }
        return (acc, ch)

acceptedChannel :: (MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelAccept -> m (Stored Channel)
acceptedChannel self peer acc = do
    let st = storedStorage acc
        req = caRequest $ fromStored $ signedData $ fromStored acc
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
    liftIO $ wrappedStore st Channel
        { chPeers = crPeers $ fromStored $ signedData $ fromStored req
        , chKey = BA.take ksize $ dhSecret xsecret $
            fromStored $ caKey $ fromStored $ signedData $ fromStored acc
        }


channelEncrypt :: (ByteArray ba, MonadIO m, MonadError String m) => Channel -> ba -> m ba
channelEncrypt ch plain = do
    cipher <- case cipherInit $ chKey ch of
                   CryptoPassed (cipher :: AES128) -> return cipher
                   _ -> throwError "failed to init AES128 cipher"
    let bsize = blockSize cipher
    (iv :: ByteString) <- liftIO $ getRandomBytes 12
    aead <- case aeadInit AEAD_GCM cipher iv of
                 CryptoPassed aead -> return aead
                 _ -> throwError "failed to init AEAD_GCM"
    let (tag, ctext) = aeadSimpleEncrypt aead B.empty plain bsize
    return $ BA.concat [ convert iv, ctext, convert tag ]

channelDecrypt :: (ByteArray ba, MonadError String m) => Channel -> ba -> m ba
channelDecrypt ch body = do
    cipher <- case cipherInit $ chKey ch of
                   CryptoPassed (cipher :: AES128) -> return cipher
                   _ -> throwError "failed to init AES128 cipher"
    let bsize = blockSize cipher
        (iv, body') = BA.splitAt 12 body
        (ctext, tag) = BA.splitAt (BA.length body' - bsize) body'
    aead <- case aeadInit AEAD_GCM cipher iv of
                 CryptoPassed aead -> return aead
                 _ -> throwError "failed to init AEAD_GCM"
    case aeadSimpleDecrypt aead B.empty ctext (AuthTag $ convert tag) of
         Just plain -> return plain
         Nothing -> throwError "failed to decrypt data"

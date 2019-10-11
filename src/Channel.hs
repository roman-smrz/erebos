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
import Control.Monad.Fail

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
        storeRef "key" $ crKey cr

    load' = loadRec $ ChannelRequest
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


createChannelRequest :: Storage -> UnifiedIdentity -> UnifiedIdentity -> IO (Stored ChannelRequest)
createChannelRequest st self peer = do
    (_, xpublic) <- generateKeys st
    Just skey <- loadKey $ idKeyMessage self
    wrappedStore st =<< sign skey =<< wrappedStore st ChannelRequest { crPeers = sort [idData self, idData peer], crKey = xpublic }

acceptChannelRequest :: UnifiedIdentity -> UnifiedIdentity -> Stored ChannelRequest -> ExceptT [String] IO (Stored ChannelAccept, Stored Channel)
acceptChannelRequest self peer req = do
    guard $ (crPeers $ fromStored $ signedData $ fromStored req) == sort (map idData [self, peer])
    guard $ (idKeyMessage peer) `elem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)

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

acceptedChannel :: UnifiedIdentity -> UnifiedIdentity -> Stored ChannelAccept -> ExceptT [String] IO (Stored Channel)
acceptedChannel self peer acc = do
    let st = storedStorage acc
        req = caRequest $ fromStored $ signedData $ fromStored acc
        KeySizeFixed ksize = cipherKeySize (undefined :: AES128)

    guard $ (crPeers $ fromStored $ signedData $ fromStored req) == sort (map idData [self, peer])
    guard $ idKeyMessage peer `elem` (map (sigKey . fromStored) $ signedSignature $ fromStored acc)
    guard $ idKeyMessage self `elem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)

    Just xsecret <- liftIO $ loadKey $ crKey $ fromStored $ signedData $ fromStored req
    liftIO $ wrappedStore st Channel
        { chPeers = crPeers $ fromStored $ signedData $ fromStored req
        , chKey = BA.take ksize $ dhSecret xsecret $
            fromStored $ caKey $ fromStored $ signedData $ fromStored acc
        }


channelEncrypt :: (ByteArray ba, MonadRandom m, MonadFail m) => Channel -> ba -> m ba
channelEncrypt ch plain = do
    CryptoPassed (cipher :: AES128) <- return $ cipherInit $ chKey ch
    let bsize = blockSize cipher
    (iv :: ByteString) <- getRandomBytes 12
    CryptoPassed aead <- return $ aeadInit AEAD_GCM cipher iv
    let (tag, ctext) = aeadSimpleEncrypt aead B.empty plain bsize
    return $ BA.concat [ convert iv, ctext, convert tag ]

channelDecrypt :: (ByteArray ba, MonadFail m) => Channel -> ba -> m ba
channelDecrypt ch body = do
    CryptoPassed (cipher :: AES128) <- return $ cipherInit $ chKey ch
    let bsize = blockSize cipher
        (iv, body') = BA.splitAt 12 body
        (ctext, tag) = BA.splitAt (BA.length body' - bsize) body'
    CryptoPassed aead <- return $ aeadInit AEAD_GCM cipher iv
    Just plain <- return $ aeadSimpleDecrypt aead B.empty ctext (AuthTag $ convert tag)
    return plain

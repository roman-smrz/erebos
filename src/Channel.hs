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
import Crypto.Data.Padding
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
    { chPeers :: [Stored Identity]
    , chKey :: ScrubbedBytes
    }
    deriving (Show)

type ChannelRequest = Signed ChannelRequestData

data ChannelRequestData = ChannelRequest
    { crPeers :: [Stored Identity]
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


createChannelRequest :: Stored Identity -> Stored Identity -> IO (Stored ChannelRequest)
createChannelRequest self peer = do
    let st = storedStorage self
    (_, xpublic) <- generateKeys st
    Just skey <- loadKey $ idKeyMessage $ fromStored $ signedData $ fromStored self
    wrappedStore st =<< sign skey =<< wrappedStore st ChannelRequest { crPeers = sort [self, peer], crKey = xpublic }

acceptChannelRequest :: Stored Identity -> Stored Identity -> Stored ChannelRequest -> ExceptT [String] IO (Stored ChannelAccept, Stored Channel)
acceptChannelRequest self peer req = do
    guard $ (crPeers $ fromStored $ signedData $ fromStored req) == sort [self, peer]
    guard $ (idKeyMessage $ fromStored $ signedData $ fromStored peer) `elem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)

    let st = storedStorage self
        KeySizeFixed ksize = cipherKeySize (undefined :: AES128)
    liftIO $ do
        (xsecret, xpublic) <- generateKeys st
        Just skey <- loadKey $ idKeyMessage $ fromStored $ signedData $ fromStored self
        acc <- wrappedStore st =<< sign skey =<< wrappedStore st ChannelAccept { caRequest = req, caKey = xpublic }
        ch <- wrappedStore st Channel
            { chPeers = crPeers $ fromStored $ signedData $ fromStored req
            , chKey = BA.take ksize $ dhSecret xsecret $
                fromStored $ crKey $ fromStored $ signedData $ fromStored req
            }
        return (acc, ch)

acceptedChannel :: Stored Identity -> Stored Identity -> Stored ChannelAccept -> ExceptT [String] IO (Stored Channel)
acceptedChannel self peer acc = do
    let st = storedStorage self
        req = caRequest $ fromStored $ signedData $ fromStored acc
        KeySizeFixed ksize = cipherKeySize (undefined :: AES128)

    guard $ (crPeers $ fromStored $ signedData $ fromStored req) == sort [self, peer]
    guard $ (idKeyMessage $ fromStored $ signedData $ fromStored peer) `elem` (map (sigKey . fromStored) $ signedSignature $ fromStored acc)
    guard $ (idKeyMessage $ fromStored $ signedData $ fromStored self) `elem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)

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
    (iv :: ByteString) <- getRandomBytes bsize
    CryptoPassed aead <- return $ aeadInit AEAD_GCM cipher iv
    let (tag, ctext) = aeadSimpleEncrypt aead B.empty (pad (PKCS7 bsize) plain) bsize
    return $ BA.concat [ convert iv, convert tag, ctext ]

channelDecrypt :: (ByteArray ba, MonadFail m) => Channel -> ba -> m ba
channelDecrypt ch body = do
    CryptoPassed (cipher :: AES128) <- return $ cipherInit $ chKey ch
    let bsize = blockSize cipher
        (iv, body') = BA.splitAt bsize body
        (tag, ctext) = BA.splitAt bsize body'
    CryptoPassed aead <- return $ aeadInit AEAD_GCM cipher iv
    Just plain <- return $ unpad (PKCS7 bsize) =<< aeadSimpleDecrypt aead B.empty ctext (AuthTag $ convert tag)
    return plain

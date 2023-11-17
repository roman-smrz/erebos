module Erebos.Channel (
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

import Crypto.Cipher.ChaChaPoly1305
import Crypto.Error

import Data.Binary
import Data.ByteArray (ByteArray, Bytes, ScrubbedBytes, convert)
import Data.ByteArray qualified as BA
import Data.ByteString.Lazy qualified as BL
import Data.List

import Erebos.Identity
import Erebos.PubKey
import Erebos.Storage

data Channel = Channel
    { chPeers :: [Stored (Signed IdentityData)]
    , chKey :: ScrubbedBytes
    , chNonceFixedOur :: Bytes
    , chNonceFixedPeer :: Bytes
    , chCounterNextOut :: MVar Word64
    , chCounterNextIn :: MVar Word64
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
        storeRef "key" $ crKey cr

    load' = loadRec $ do
        ChannelRequest
            <$> loadRefs "peer"
            <*> loadRef "key"

instance Storable ChannelAcceptData where
    store' ca = storeRec $ do
        storeRef "req" $ caRequest ca
        storeRef "key" $ caKey ca

    load' = loadRec $ do
        ChannelAccept
            <$> loadRef "req"
            <*> loadRef "key"


keySize :: Int
keySize = 32

createChannelRequest :: (MonadStorage m, MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> m (Stored ChannelRequest)
createChannelRequest self peer = do
    (_, xpublic) <- liftIO . generateKeys =<< getStorage
    skey <- loadKey $ idKeyMessage self
    mstore =<< sign skey =<< mstore ChannelRequest { crPeers = sort [idData self, idData peer], crKey = xpublic }

acceptChannelRequest :: (MonadStorage m, MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelRequest -> m (Stored ChannelAccept, Channel)
acceptChannelRequest self peer req = do
    case sequence $ map validateIdentity $ crPeers $ fromStored $ signedData $ fromStored req of
        Nothing -> throwError $ "invalid peers in channel request"
        Just peers -> do
            when (not $ any (self `sameIdentity`) peers) $
                throwError $ "self identity missing in channel request peers"
            when (not $ any (peer `sameIdentity`) peers) $
                throwError $ "peer identity missing in channel request peers"
    when (idKeyMessage peer `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)) $
        throwError $ "channel requent not signed by peer"

    (xsecret, xpublic) <- liftIO . generateKeys =<< getStorage
    skey <- loadKey $ idKeyMessage self
    acc <- mstore =<< sign skey =<< mstore ChannelAccept { caRequest = req, caKey = xpublic }
    liftIO $ do
        let chPeers = crPeers $ fromStored $ signedData $ fromStored req
            chKey = BA.take keySize $ dhSecret xsecret $
                fromStored $ crKey $ fromStored $ signedData $ fromStored req
            chNonceFixedOur  = BA.pack [ 2, 0, 0, 0 ]
            chNonceFixedPeer = BA.pack [ 1, 0, 0, 0 ]
        chCounterNextOut <- newMVar 0
        chCounterNextIn <- newMVar 0

        return (acc, Channel {..})

acceptedChannel :: (MonadIO m, MonadError String m) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelAccept -> m Channel
acceptedChannel self peer acc = do
    let req = caRequest $ fromStored $ signedData $ fromStored acc
    case sequence $ map validateIdentity $ crPeers $ fromStored $ signedData $ fromStored req of
        Nothing -> throwError $ "invalid peers in channel accept"
        Just peers -> do
            when (not $ any (self `sameIdentity`) peers) $
                throwError $ "self identity missing in channel accept peers"
            when (not $ any (peer `sameIdentity`) peers) $
                throwError $ "peer identity missing in channel accept peers"
    when (idKeyMessage peer `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored acc)) $
        throwError $ "channel accept not signed by peer"
    when (idKeyMessage self `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)) $
        throwError $ "original channel request not signed by us"

    xsecret <- loadKey $ crKey $ fromStored $ signedData $ fromStored req
    let chPeers = crPeers $ fromStored $ signedData $ fromStored req
        chKey = BA.take keySize $ dhSecret xsecret $
            fromStored $ caKey $ fromStored $ signedData $ fromStored acc
        chNonceFixedOur  = BA.pack [ 1, 0, 0, 0 ]
        chNonceFixedPeer = BA.pack [ 2, 0, 0, 0 ]
    chCounterNextOut <- liftIO $ newMVar 0
    chCounterNextIn <- liftIO $ newMVar 0

    return Channel {..}


channelEncrypt :: (ByteArray ba, MonadIO m, MonadError String m) => Channel -> ba -> m (ba, Word64)
channelEncrypt Channel {..} plain = do
    count <- liftIO $ modifyMVar chCounterNextOut $ \c -> return (c + 1, c)
    let cbytes = convert $ BL.toStrict $ encode count
        nonce = nonce8 chNonceFixedOur cbytes
    state <- case initialize chKey =<< nonce of
        CryptoPassed state -> return state
        CryptoFailed err -> throwError $ "failed to init chacha-poly1305 cipher: " <> show err

    let (ctext, state') = encrypt plain state
        tag = finalize state'
    return (BA.concat [ convert $ BA.drop 7 cbytes, ctext, convert tag ], count)

channelDecrypt :: (ByteArray ba, MonadIO m, MonadError String m) => Channel -> ba -> m (ba, Word64)
channelDecrypt Channel {..} body = do
    when (BA.length body < 17) $ do
        throwError $ "invalid encrypted data length"

    expectedCount <- liftIO $ readMVar chCounterNextIn
    let countByte = body `BA.index` 0
        body' = BA.dropView body 1
        guessedCount = expectedCount - 128 + fromIntegral (countByte - fromIntegral expectedCount + 128 :: Word8)
        nonce = nonce8 chNonceFixedPeer $ convert $ BL.toStrict $ encode guessedCount
        blen = BA.length body' - 16
        ctext = BA.takeView body' blen
        tag = BA.dropView body' blen
    state <- case initialize chKey =<< nonce of
        CryptoPassed state -> return state
        CryptoFailed err -> throwError $ "failed to init chacha-poly1305 cipher: " <> show err

    let (plain, state') = decrypt (convert ctext) state
    when (not $ tag `BA.constEq` finalize state') $ do
        throwError $ "tag validation falied"

    liftIO $ modifyMVar_ chCounterNextIn $ return . max (guessedCount + 1)
    return (plain, guessedCount)

module Erebos.Network.Channel (
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
import Control.Monad.IO.Class

import Data.Binary
import Data.ByteArray (ScrubbedBytes, convert)
import Data.ByteArray qualified as BA
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as BL
import Data.List

import Crypto
import Erebos.Identity
import Erebos.PubKey
import Erebos.Storable

data Channel = Channel
    { chPeers :: [Stored (Signed IdentityData)]
    , chKey :: ScrubbedBytes
    , chNonceFixedOur :: ByteString
    , chNonceFixedPeer :: ByteString
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

createChannelRequest :: (MonadStorage m, MonadIO m, MonadError e m, FromErebosError e) => UnifiedIdentity -> UnifiedIdentity -> m (Stored ChannelRequest)
createChannelRequest self peer = do
    (_, xpublic) <- liftIO . generateKeys =<< getStorage
    skey <- loadKey $ idKeyMessage self
    mstore =<< sign skey =<< mstore ChannelRequest { crPeers = sort [idData self, idData peer], crKey = xpublic }

acceptChannelRequest :: (MonadStorage m, MonadIO m, MonadError e m, FromErebosError e) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelRequest -> m (Stored ChannelAccept, Channel)
acceptChannelRequest self peer req = do
    case sequence $ map validateIdentity $ crPeers $ fromStored $ signedData $ fromStored req of
        Nothing -> throwOtherError $ "invalid peers in channel request"
        Just peers -> do
            when (not $ any (self `sameIdentity`) peers) $
                throwOtherError $ "self identity missing in channel request peers"
            when (not $ any (peer `sameIdentity`) peers) $
                throwOtherError $ "peer identity missing in channel request peers"
    when (idKeyMessage peer `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)) $
        throwOtherError $ "channel requent not signed by peer"

    (xsecret, xpublic) <- liftIO . generateKeys =<< getStorage
    skey <- loadKey $ idKeyMessage self
    acc <- mstore =<< sign skey =<< mstore ChannelAccept { caRequest = req, caKey = xpublic }
    liftIO $ do
        let chPeers = crPeers $ fromStored $ signedData $ fromStored req
            chKey = BA.take keySize $ dhSecret xsecret $
                fromStored $ crKey $ fromStored $ signedData $ fromStored req
            chNonceFixedOur  = BS.pack [ 2, 0, 0, 0 ]
            chNonceFixedPeer = BS.pack [ 1, 0, 0, 0 ]
        chCounterNextOut <- newMVar 0
        chCounterNextIn <- newMVar 0

        return (acc, Channel {..})

acceptedChannel :: (MonadIO m, MonadError e m, FromErebosError e) => UnifiedIdentity -> UnifiedIdentity -> Stored ChannelAccept -> m Channel
acceptedChannel self peer acc = do
    let req = caRequest $ fromStored $ signedData $ fromStored acc
    case sequence $ map validateIdentity $ crPeers $ fromStored $ signedData $ fromStored req of
        Nothing -> throwOtherError $ "invalid peers in channel accept"
        Just peers -> do
            when (not $ any (self `sameIdentity`) peers) $
                throwOtherError $ "self identity missing in channel accept peers"
            when (not $ any (peer `sameIdentity`) peers) $
                throwOtherError $ "peer identity missing in channel accept peers"
    when (idKeyMessage peer `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored acc)) $
        throwOtherError $ "channel accept not signed by peer"
    when (idKeyMessage self `notElem` (map (sigKey . fromStored) $ signedSignature $ fromStored req)) $
        throwOtherError $ "original channel request not signed by us"

    xsecret <- loadKey $ crKey $ fromStored $ signedData $ fromStored req
    let chPeers = crPeers $ fromStored $ signedData $ fromStored req
        chKey = BA.take keySize $ dhSecret xsecret $
            fromStored $ caKey $ fromStored $ signedData $ fromStored acc
        chNonceFixedOur  = BS.pack [ 1, 0, 0, 0 ]
        chNonceFixedPeer = BS.pack [ 2, 0, 0, 0 ]
    chCounterNextOut <- liftIO $ newMVar 0
    chCounterNextIn <- liftIO $ newMVar 0

    return Channel {..}


channelEncrypt :: (MonadIO m, MonadError e m, FromErebosError e) => Channel -> ByteString -> m ( ByteString, Word64 )
channelEncrypt Channel {..} plain = do
    count <- liftIO $ modifyMVar chCounterNextOut $ \c -> return ( c + 1, c )
    let cbytes = BL.toStrict $ encode count
        nonce = nonce8 chNonceFixedOur cbytes
    state <- case aeadInitialize chKey =<< nonce of
        CryptoPassed state -> return state
        CryptoFailed err -> throwOtherError $ "failed to init chacha-poly1305 cipher: " <> show err

    let ( ctext, state' ) = encrypt plain state
        tag = aeadFinalize state'
    return ( BS.concat [ BS.drop 7 cbytes, ctext, convert tag ], count )

channelDecrypt :: (MonadIO m, MonadError e m, FromErebosError e) => Channel -> ByteString -> m ( ByteString, Word64 )
channelDecrypt Channel {..} body = do
    when (BS.length body < 17) $ do
        throwOtherError $ "invalid encrypted data length"

    expectedCount <- liftIO $ readMVar chCounterNextIn
    let countByte = body `BS.index` 0
        body' = BS.drop 1 body
        guessedCount = expectedCount - 128 + fromIntegral (countByte - fromIntegral expectedCount + 128 :: Word8)
        nonce = nonce8 chNonceFixedPeer $ BL.toStrict $ encode guessedCount
        blen = BS.length body' - 16
        ctext = BS.take blen body'
        tag = BS.drop blen body'
    state <- case aeadInitialize chKey =<< nonce of
        CryptoPassed state -> return state
        CryptoFailed err -> throwOtherError $ "failed to init chacha-poly1305 cipher: " <> show err

    auth <- case authTag tag of
        CryptoPassed auth -> return auth
        CryptoFailed err -> throwOtherError $ "failed to decode auth tag: " <> show err
    plain <- maybe (throwOtherError "tag validation failed") return $
        decrypt ( convert ctext, auth ) state

    liftIO $ modifyMVar_ chCounterNextIn $ return . max (guessedCount + 1)
    return (plain, guessedCount)

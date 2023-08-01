{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecursiveDo #-}

module ICE (
    IceSession,
    IceSessionRole(..),
    IceRemoteInfo,

    iceCreate,
    iceDestroy,
    iceRemoteInfo,
    iceShow,
    iceConnect,
    iceSend,

    iceSetChan,
) where

import Control.Arrow
import Control.Concurrent.MVar
import Control.Monad
import Control.Monad.Except
import Control.Monad.Identity

import Data.ByteString (ByteString, packCStringLen, useAsCString)
import qualified Data.ByteString.Lazy.Char8 as BLC
import Data.ByteString.Unsafe
import Data.Function
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.Text.Read as T
import Data.Void

import Foreign.C.String
import Foreign.C.Types
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.StablePtr

import Flow
import Storage

#include "pjproject.h"

data IceSession = IceSession
    { isStrans :: PjIceStrans
    , isChan :: MVar (Either [ByteString] (Flow Void ByteString))
    }

instance Eq IceSession where
    (==) = (==) `on` isStrans

instance Ord IceSession where
    compare = compare `on` isStrans

instance Show IceSession where
    show _ = "<ICE>"


data IceRemoteInfo = IceRemoteInfo
    { iriUsernameFrament :: Text
    , iriPassword :: Text
    , iriDefaultCandidate :: Text
    , iriCandidates :: [Text]
    }

data IceCandidate = IceCandidate
    { icandFoundation :: Text
    , icandPriority :: Int
    , icandAddr :: Text
    , icandPort :: Int
    , icandType :: Text
    }

instance Storable IceRemoteInfo where
    store' x = storeRec $ do
        storeText "ice-ufrag" $ iriUsernameFrament x
        storeText "ice-pass" $ iriPassword x
        storeText "ice-default" $ iriDefaultCandidate x
        mapM_ (storeText "ice-candidate") $ iriCandidates x

    load' = loadRec $ IceRemoteInfo
        <$> loadText "ice-ufrag"
        <*> loadText "ice-pass"
        <*> loadText "ice-default"
        <*> loadTexts "ice-candidate"

instance StorableText IceCandidate where
    toText x = T.concat $
        [ icandFoundation x
        , T.singleton ' '
        , T.pack $ show $ icandPriority x
        , T.singleton ' '
        , icandAddr x
        , T.singleton ' '
        , T.pack $ show $ icandPort x
        , T.singleton ' '
        , icandType x
        ]

    fromText t = case T.words t of
        [found, tprio, addr, tport, ctype]
            | Right (prio, _) <- T.decimal tprio
            , Right (port, _) <- T.decimal tport
            -> return $ IceCandidate
                { icandFoundation = found
                , icandPriority = prio
                , icandAddr = addr
                , icandPort = port
                , icandType = ctype
                }
        _ -> throwError "failed to parse candidate"


{#enum pj_ice_sess_role as IceSessionRole {underscoreToCase} deriving (Show, Eq) #}

{#pointer *pj_ice_strans as ^ #}

iceCreate :: IceSessionRole -> (IceSession -> IO ()) -> IO IceSession
iceCreate role cb = do
    rec sptr <- newStablePtr sess
        cbptr <- newStablePtr $ cb sess
        sess <- IceSession
            <$> {#call ice_create #} (fromIntegral $ fromEnum role) (castStablePtrToPtr sptr) (castStablePtrToPtr cbptr)
            <*> (newMVar $ Left [])
    return $ sess

{#fun ice_destroy as ^ { isStrans `IceSession' } -> `()' #}

iceRemoteInfo :: IceSession -> IO IceRemoteInfo
iceRemoteInfo sess = do
    let maxlen = 128
        maxcand = 29

    allocaBytes maxlen $ \ufrag ->
        allocaBytes maxlen $ \pass ->
        allocaBytes maxlen $ \def ->
        allocaBytes (maxcand*maxlen) $ \bytes ->
        allocaArray maxcand $ \carr -> do
        let cptrs = take maxcand $ iterate (`plusPtr` maxlen) bytes
        pokeArray carr $ take maxcand cptrs

        ncand <- {#call ice_encode_session #} (isStrans sess) ufrag pass def carr (fromIntegral maxlen) (fromIntegral maxcand)
        if ncand < 0 then fail "failed to generate ICE remote info"
                     else IceRemoteInfo
                              <$> (T.pack <$> peekCString ufrag)
                              <*> (T.pack <$> peekCString pass)
                              <*> (T.pack <$> peekCString def)
                              <*> (mapM (return . T.pack <=< peekCString) $ take (fromIntegral ncand) cptrs)

iceShow :: IceSession -> IO String
iceShow sess = do
    st <- memoryStorage
    return . drop 1 . dropWhile (/='\n') . BLC.unpack . runIdentity =<<
        ioLoadBytes =<< store st =<< iceRemoteInfo sess

iceConnect :: IceSession -> IceRemoteInfo -> (IO ()) -> IO ()
iceConnect sess remote cb = do
    cbptr <- newStablePtr $ cb
    ice_connect sess cbptr
        (iriUsernameFrament remote)
        (iriPassword remote)
        (iriDefaultCandidate remote)
        (iriCandidates remote)

{#fun ice_connect { isStrans `IceSession', castStablePtrToPtr `StablePtr (IO ())',
    withText* `Text',  withText* `Text', withText* `Text', withTextArray* `[Text]'& } -> `()' #}

withText :: Text -> (Ptr CChar -> IO a) -> IO a
withText t f = useAsCString (T.encodeUtf8 t) f

withTextArray :: Num n => [Text] -> ((Ptr (Ptr CChar), n) -> IO ()) -> IO ()
withTextArray tsAll f = helper tsAll []
    where helper (t:ts) bs = withText t $ \b -> helper ts (b:bs)
          helper [] bs = allocaArray (length bs) $ \ptr -> do
              pokeArray ptr $ reverse bs
              f (ptr, fromIntegral $ length bs)

withByteStringLen :: Num n => ByteString -> ((Ptr CChar, n) -> IO a) -> IO a
withByteStringLen t f = unsafeUseAsCStringLen t (f . (id *** fromIntegral))

{#fun ice_send as ^ { isStrans `IceSession', withByteStringLen* `ByteString'& } -> `()' #}

foreign export ccall ice_call_cb :: StablePtr (IO ()) -> IO ()
ice_call_cb :: StablePtr (IO ()) -> IO ()
ice_call_cb = join . deRefStablePtr

iceSetChan :: IceSession -> Flow Void ByteString -> IO ()
iceSetChan sess chan = do
    modifyMVar_ (isChan sess) $ \orig -> do
        case orig of
             Left buf -> mapM_ (writeFlowIO chan) $ reverse buf
             Right _ -> return ()
        return $ Right chan

foreign export ccall ice_rx_data :: StablePtr IceSession -> Ptr CChar -> Int -> IO ()
ice_rx_data :: StablePtr IceSession -> Ptr CChar -> Int -> IO ()
ice_rx_data sptr buf len = do
    sess <- deRefStablePtr sptr
    bs <- packCStringLen (buf, len)
    modifyMVar_ (isChan sess) $ \case
            mc@(Right chan) -> writeFlowIO chan bs >> return mc
            Left bss -> return $ Left (bs:bss)

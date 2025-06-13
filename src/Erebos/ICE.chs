{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE RecursiveDo #-}

module Erebos.ICE (
    IceSession,
    IceSessionRole(..),
    IceConfig,
    IceRemoteInfo,

    iceCreateConfig,
    iceStopThread,
    iceCreateSession,
    iceDestroy,
    iceRemoteInfo,
    iceShow,
    iceConnect,
    iceSend,

    iceSetChan,
) where

import Control.Arrow
import Control.Concurrent
import Control.Monad
import Control.Monad.Identity

import Data.ByteString (ByteString, packCStringLen, useAsCString)
import Data.ByteString.Lazy.Char8 qualified as BLC
import Data.ByteString.Unsafe
import Data.Function
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as T
import Data.Text.Read qualified as T
import Data.Void
import Data.Word

import Foreign.C.String
import Foreign.C.Types
import Foreign.ForeignPtr
import Foreign.Marshal.Alloc
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.StablePtr

import Erebos.Flow
import Erebos.Object
import Erebos.Storable
import Erebos.Storage

#include "pjproject.h"

data IceSession = IceSession
    { isStrans :: PjIceStrans
    , _isConfig :: IceConfig
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
        _ -> throwOtherError "failed to parse candidate"


{#enum pj_ice_sess_role as IceSessionRole {underscoreToCase} deriving (Show, Eq) #}

data PjIceStransCfg
newtype IceConfig = IceConfig (ForeignPtr PjIceStransCfg)

foreign import ccall unsafe "pjproject.h &ice_cfg_free"
    ice_cfg_free :: FunPtr (Ptr PjIceStransCfg -> IO ())
foreign import ccall unsafe "pjproject.h ice_cfg_create"
    ice_cfg_create :: CString -> Word16 -> CString -> Word16 -> IO (Ptr PjIceStransCfg)

iceCreateConfig :: Maybe ( Text, Word16 ) -> Maybe ( Text, Word16 ) -> IO (Maybe IceConfig)
iceCreateConfig stun turn =
    maybe ($ nullPtr) (withText . fst) stun $ \cstun ->
    maybe ($ nullPtr) (withText . fst) turn $ \cturn -> do
        cfg <- ice_cfg_create cstun (maybe 0 snd stun) cturn (maybe 0 snd turn)
        if cfg == nullPtr
          then return Nothing
          else Just . IceConfig <$> newForeignPtr ice_cfg_free cfg

foreign import ccall unsafe "pjproject.h ice_cfg_stop_thread"
    ice_cfg_stop_thread :: Ptr PjIceStransCfg -> IO ()

iceStopThread :: IceConfig -> IO ()
iceStopThread (IceConfig fcfg) = withForeignPtr fcfg ice_cfg_stop_thread

{#pointer *pj_ice_strans as ^ #}

iceCreateSession :: IceConfig -> IceSessionRole -> (IceSession -> IO ()) -> IO IceSession
iceCreateSession icfg@(IceConfig fcfg) role cb = do
    rec sptr <- newStablePtr sess
        cbptr <- newStablePtr $ do
            -- The callback may be called directly from pj_ice_strans_create or later
            -- from a different thread; make sure we use a different thread here 
            -- to avoid deadlock on accessing 'sess'.
            forkIO $ cb sess
        sess <- IceSession
            <$> (withForeignPtr fcfg $ \cfg ->
                    {#call ice_create #} (castPtr cfg) (fromIntegral $ fromEnum role) (castStablePtrToPtr sptr) (castStablePtrToPtr cbptr)
                )
            <*> pure icfg
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

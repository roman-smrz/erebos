module Erebos.Storage.Memory (
    memoryStorage,
    deriveEphemeralStorage,
    derivePartialStorage,
) where

import Control.Concurrent.MVar
import Control.DeepSeq

import Data.ByteArray (ScrubbedBytes)
import Data.ByteString.Lazy qualified as BL
import Data.Function
import Data.Kind
import Data.List
import Data.Map (Map)
import Data.Map qualified as M
import Data.Maybe
import Data.Typeable

import Erebos.Object
import Erebos.Storage.Backend
import Erebos.Storage.Head
import Erebos.Storage.Internal


data MemoryStorage p (c :: Type -> Type) = StorageMemory
    { memParent :: p
    , memHeads :: MVar [ (( HeadTypeID, HeadID ), RefDigest ) ]
    , memObjs :: MVar (Map RefDigest BL.ByteString)
    , memKeys :: MVar (Map RefDigest ScrubbedBytes)
    , memWatchers :: MVar WatchList
    }

instance Eq (MemoryStorage p c) where
    (==) = (==) `on` memObjs

instance Show (MemoryStorage p c) where
    show StorageMemory {} = "mem"

instance (StorageCompleteness c, Typeable p) => StorageBackend (MemoryStorage p c) where
    type BackendCompleteness (MemoryStorage p c) = c
    type BackendParent (MemoryStorage p c) = p
    backendParent = memParent

    backendLoadBytes StorageMemory {..} dgst =
        M.lookup dgst <$> readMVar memObjs

    backendStoreBytes StorageMemory {..} dgst raw =
        dgst `deepseq` -- the TVar may be accessed when evaluating the data to be written
            modifyMVar_ memObjs (return . M.insert dgst raw)


    backendLoadHeads StorageMemory {..} tid = do
        let toRes ( ( tid', hid ), dgst )
                | tid' == tid = Just ( hid, dgst )
                | otherwise   = Nothing
        catMaybes . map toRes <$> readMVar memHeads

    backendLoadHead StorageMemory {..} tid hid =
        lookup (tid, hid) <$> readMVar memHeads

    backendStoreHead StorageMemory {..} tid hid dgst =
        modifyMVar_ memHeads $ return . (( ( tid, hid ), dgst ) :)

    backendReplaceHead StorageMemory {..} tid hid expected new = do
        res <- modifyMVar memHeads $ \hs -> do
            ws <- map wlFun . filter ((==(tid, hid)) . wlHead) . wlList <$> readMVar memWatchers
            return $ case partition ((==(tid, hid)) . fst) hs of
                ( [] , _ ) -> ( hs, Left Nothing )
                (( _, dgst ) : _, hs' )
                    | dgst == expected -> ((( tid, hid ), new ) : hs', Right ( new, ws ))
                    | otherwise -> ( hs, Left $ Just dgst )
        case res of
            Right ( dgst, ws ) -> mapM_ ($ dgst) ws >> return (Right dgst)
            Left x -> return $ Left x

    backendWatchHead StorageMemory {..} tid hid cb = modifyMVar memWatchers $ return . watchListAdd tid hid cb

    backendUnwatchHead StorageMemory {..} wid = modifyMVar_ memWatchers $ return . watchListDel wid


    backendListKeys StorageMemory {..} = M.keys <$> readMVar memKeys
    backendLoadKey StorageMemory {..} dgst = M.lookup dgst <$> readMVar memKeys
    backendStoreKey StorageMemory {..} dgst key = modifyMVar_ memKeys $ return . M.insert dgst key
    backendRemoveKey StorageMemory {..} dgst = modifyMVar_ memKeys $ return . M.delete dgst


memoryStorage' :: (StorageCompleteness c, Typeable p) => p -> IO (Storage' c)
memoryStorage' memParent = do
    memHeads <- newMVar []
    memObjs <- newMVar M.empty
    memKeys <- newMVar M.empty
    memWatchers <- newMVar (WatchList startWatchID [])
    newStorage $ StorageMemory {..}

memoryStorage :: IO Storage
memoryStorage = memoryStorage' ()

deriveEphemeralStorage :: Storage -> IO Storage
deriveEphemeralStorage parent = memoryStorage' parent

derivePartialStorage :: Storage -> IO PartialStorage
derivePartialStorage parent = memoryStorage' parent

module State (
    LocalState(..),
    SharedState(..),

    loadLocalState,
    updateLocalState, updateLocalState_,
    updateIdentity,
) where

import Data.List
import qualified Data.Text as T
import qualified Data.Text.IO as T

import System.IO

import Identity
import Message
import PubKey
import Storage
import Util

data LocalState = LocalState
    { lsIdentity :: Stored (Signed IdentityData)
    , lsShared :: [Stored SharedState]
    , lsMessages :: StoredList DirectMessageThread -- TODO: move to shared
    }

data SharedState = SharedState
    { ssPrev :: [Stored SharedState]
    , ssIdentity :: [Stored (Signed IdentityData)]
    }

instance Storable LocalState where
    store' st = storeRec $ do
        storeRef "id" $ lsIdentity st
        mapM_ (storeRef "shared") $ lsShared st
        storeRef "dmsg" $ lsMessages st

    load' = loadRec $ LocalState
        <$> loadRef "id"
        <*> loadRefs "shared"
        <*> loadRef "dmsg"

instance Storable SharedState where
    store' st = storeRec $ do
        mapM_ (storeRef "PREV") $ ssPrev st
        mapM_ (storeRef "id") $ ssIdentity st

    load' = loadRec $ SharedState
        <$> loadRefs "PREV"
        <*> loadRefs "id"


loadLocalState :: Storage -> IO Head
loadLocalState st = loadHeadDef st "erebos" $ do
    putStr "Name: "
    hFlush stdout
    name <- T.getLine

    (secret, public) <- generateKeys st
    (_secretMsg, publicMsg) <- generateKeys st
    (devSecret, devPublic) <- generateKeys st
    (_devSecretMsg, devPublicMsg) <- generateKeys st

    owner <- wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
        { iddName = Just name, iddKeyMessage = Just publicMsg }
    identity <- wrappedStore st =<< signAdd devSecret =<< sign secret =<< wrappedStore st (emptyIdentityData devPublic)
        { iddOwner = Just owner, iddKeyMessage = Just devPublicMsg }

    msgs <- emptySList st

    shared <- wrappedStore st $ SharedState
        { ssPrev = []
        , ssIdentity = [owner]
        }
    return $ LocalState
        { lsIdentity = identity
        , lsShared = [shared]
        , lsMessages = msgs
        }

updateLocalState_ :: Storage -> (Stored LocalState -> IO (Stored LocalState)) -> IO ()
updateLocalState_ st f = updateLocalState st (fmap (,()) . f)

updateLocalState :: Storage -> (Stored LocalState -> IO (Stored LocalState, a)) -> IO a
updateLocalState ls f = do
    Just erebosHead <- loadHead ls "erebos"
    (st, x) <- f $ wrappedLoad (headRef erebosHead)
    Right _ <- replaceHead st (Right erebosHead)
    return x

updateSharedState_ :: Storage -> (Stored SharedState -> IO (Stored SharedState)) -> IO ()
updateSharedState_ st f = updateSharedState st (fmap (,()) . f)

updateSharedState :: Storage -> (Stored SharedState -> IO (Stored SharedState, a)) -> IO a
updateSharedState st f = updateLocalState st $ \ls -> do
    (shared, x) <- f =<< mergeSharedStates (lsShared $ fromStored ls)
    (,x) <$> wrappedStore st (fromStored ls) { lsShared = [shared] }

mergeSharedStates :: [(Stored SharedState)] -> IO (Stored SharedState)
mergeSharedStates [s] = return s
mergeSharedStates ss@(s:_) = wrappedStore (storedStorage s) $ SharedState
        { ssPrev = ss
        , ssIdentity = uniq $ sort $ concatMap (ssIdentity . fromStored) $ ss -- TODO: ancestor elimination
        }
mergeSharedStates [] = error "mergeSharedStates: empty list"

updateIdentity :: Storage -> IO ()
updateIdentity st = updateSharedState_ st $ \sshared -> do
    let shared = fromStored sshared
        Just identity = verifyIdentityF $ ssIdentity shared
        public = idKeyIdentity identity

    T.putStr $ T.concat $ concat
        [ [ T.pack "Name" ]
        , case idName identity of
               Just name -> [T.pack " [", name, T.pack "]"]
               Nothing -> []
        , [ T.pack ": " ]
        ]
    hFlush stdout
    name <- T.getLine

    identity' <- if
        | T.null name -> idData <$> mergeIdentity identity
        | otherwise -> do
            Just secret <- loadKey public
            wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentityData public)
                { iddPrev = ssIdentity shared
                , iddName = Just name
                }

    wrappedStore st shared { ssIdentity = [identity'] }

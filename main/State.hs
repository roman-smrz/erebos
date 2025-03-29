module State (
    loadLocalStateHead,
    updateSharedIdentity,
    interactiveIdentityUpdate,
) where

import Control.Monad.Except
import Control.Monad.IO.Class

import Data.Foldable
import Data.Maybe
import Data.Proxy
import Data.Text qualified as T

import Erebos.Error
import Erebos.Identity
import Erebos.PubKey
import Erebos.State
import Erebos.Storable
import Erebos.Storage

import Terminal


loadLocalStateHead :: MonadIO m => Terminal -> Storage -> m (Head LocalState)
loadLocalStateHead term st = loadHeads st >>= \case
    (h:_) -> return h
    [] -> liftIO $ do
        setPrompt term "Name: "
        name <- getInputLine term $ KeepPrompt . maybe T.empty T.pack

        setPrompt term "Device: "
        devName <- getInputLine term $ KeepPrompt . maybe T.empty T.pack

        owner <- if
            | T.null name -> return Nothing
            | otherwise -> Just <$> createIdentity st (Just name) Nothing

        identity <- createIdentity st (if T.null devName then Nothing else Just devName) owner

        shared <- wrappedStore st $ SharedState
            { ssPrev = []
            , ssType = Just $ sharedTypeID @(Maybe ComposedIdentity) Proxy
            , ssValue = [ storedRef $ idExtData $ fromMaybe identity owner ]
            }
        storeHead st $ LocalState
            { lsPrev = Nothing
            , lsIdentity = idExtData identity
            , lsShared = [ shared ]
            , lsOther = []
            }


updateSharedIdentity :: (MonadHead LocalState m, MonadError e m, FromErebosError e) => Terminal -> m ()
updateSharedIdentity term = updateLocalState_ $ updateSharedState_ $ \case
    Just identity -> do
        Just . toComposedIdentity <$> interactiveIdentityUpdate term identity
    Nothing -> throwOtherError "no existing shared identity"

interactiveIdentityUpdate :: (Foldable f, MonadStorage m, MonadIO m, MonadError e m, FromErebosError e) => Terminal -> Identity f -> m UnifiedIdentity
interactiveIdentityUpdate term identity = do
    let public = idKeyIdentity identity

    name <- liftIO $ do
        setPrompt term $ T.unpack $ T.concat $ concat
            [ [ T.pack "Name" ]
            , case idName identity of
                   Just name -> [T.pack " [", name, T.pack "]"]
                   Nothing -> []
            , [ T.pack ": " ]
            ]
        getInputLine term $ KeepPrompt . maybe T.empty T.pack

    if  | T.null name -> mergeIdentity identity
        | otherwise -> do
            secret <- loadKey public
            maybe (throwOtherError "created invalid identity") return . validateIdentity =<<
                mstore =<< sign secret =<< mstore (emptyIdentityData public)
                { iddPrev = toList $ idDataF identity
                , iddName = Just name
                }

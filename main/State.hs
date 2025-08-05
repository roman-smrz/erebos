module State (
    loadLocalStateHead,
    createLocalStateHead,
    updateSharedIdentity,
    interactiveIdentityUpdate,
) where

import Control.Monad
import Control.Monad.Except
import Control.Monad.IO.Class

import Data.Foldable
import Data.Proxy
import Data.Text (Text)
import Data.Text qualified as T

import Erebos.Error
import Erebos.Identity
import Erebos.PubKey
import Erebos.State
import Erebos.Storable
import Erebos.Storage

import Terminal


loadLocalStateHead
    :: (MonadStorage m, MonadError e m, FromErebosError e, MonadIO m)
    => Terminal -> m (Head LocalState)
loadLocalStateHead term = getStorage >>= loadHeads >>= \case
    (h : _) -> return h
    [] -> do
        name <- liftIO $ do
            setPrompt term "Name: "
            getInputLine term $ KeepPrompt . maybe T.empty T.pack

        devName <- liftIO $ do
            setPrompt term "Device: "
            getInputLine term $ KeepPrompt . maybe T.empty T.pack

        ( owner, shared ) <- if
            | T.null name -> do
                return ( Nothing, [] )
            | otherwise -> do
                owner <- createIdentity (Just name) Nothing
                shared <- mstore SharedState
                    { ssPrev = []
                    , ssType = Just $ sharedTypeID @(Maybe ComposedIdentity) Proxy
                    , ssValue = [ storedRef $ idExtData owner ]
                    }
                return ( Just owner, [ shared ] )

        identity <- createIdentity (if T.null devName then Nothing else Just devName) owner

        st <- getStorage
        storeHead st $ LocalState
            { lsPrev = Nothing
            , lsIdentity = idExtData identity
            , lsShared = shared
            , lsOther = []
            }

createLocalStateHead
    :: (MonadStorage m, MonadError e m, FromErebosError e, MonadIO m)
    => [ Maybe Text ] -> m (Head LocalState)
createLocalStateHead [] = throwOtherError "createLocalStateHead: empty name list"
createLocalStateHead ( ownerName : names ) = do
    owner <- createIdentity ownerName Nothing
    identity <- foldM createSingleIdentity owner names
    shared <- case names of
        [] -> return []
        _ : _ -> do
            fmap (: []) $ mstore SharedState
                { ssPrev = []
                , ssType = Just $ sharedTypeID @(Maybe ComposedIdentity) Proxy
                , ssValue = [ storedRef $ idExtData owner ]
                }
    st <- getStorage
    storeHead st $ LocalState
        { lsPrev = Nothing
        , lsIdentity = idExtData identity
        , lsShared = shared
        , lsOther = []
        }
  where
    createSingleIdentity owner name = createIdentity name (Just owner)


updateSharedIdentity :: (MonadHead LocalState m, MonadError e m, FromErebosError e) => Terminal -> m ()
updateSharedIdentity term = updateLocalState_ $ updateSharedState_ $ \case
    Just identity -> do
        Just . toComposedIdentity <$> interactiveIdentityUpdate term identity
    Nothing -> throwOtherError "no existing shared identity"

interactiveIdentityUpdate :: (Foldable f, MonadStorage m, MonadIO m, MonadError e m, FromErebosError e) => Terminal -> Identity f -> m UnifiedIdentity
interactiveIdentityUpdate term fidentity = do
    identity <- mergeIdentity fidentity
    name <- liftIO $ do
        setPrompt term $ T.unpack $ T.concat $ concat
            [ [ T.pack "Name" ]
            , case idName identity of
                   Just name -> [T.pack " [", name, T.pack "]"]
                   Nothing -> []
            , [ T.pack ": " ]
            ]
        getInputLine term $ KeepPrompt . maybe T.empty T.pack

    if  | T.null name -> return identity
        | otherwise -> do
            secret <- loadKey $ idKeyIdentity identity
            maybe (throwOtherError "created invalid identity") return . validateExtendedIdentity =<<
                mstore =<< sign secret =<< mstore . ExtendedIdentityData =<< return (emptyIdentityExtension $ idData identity)
                { idePrev = toList $ idExtDataF identity
                , ideName = Just name
                }

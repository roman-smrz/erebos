module Identity (
    Identity(..),
) where

import Data.Text (Text)

import Storage

data Identity = Identity
    { idName :: Text
    , idPrev :: Maybe (Stored Identity)
    }
    deriving (Show)

instance Storable Identity where
    store' idt = storeRec $ do
        storeText "name" $ idName idt
        storeMbRef "prev" $ idPrev idt

    load' = loadRec $ Identity
        <$> loadText "name"
        <*> loadMbRef "prev"

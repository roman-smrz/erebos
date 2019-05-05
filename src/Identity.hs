module Identity (
    Identity, IdentityData(..),
) where

import Data.Text (Text)

import PubKey
import Storage

type Identity = Signed IdentityData

data IdentityData = Identity
    { idName :: Text
    , idPrev :: Maybe (Stored Identity)
    , idKeyIdentity :: Stored PublicKey
    }
    deriving (Show)

instance Storable IdentityData where
    store' idt = storeRec $ do
        storeText "name" $ idName idt
        storeMbRef "prev" $ idPrev idt
        storeRef "key-id" $ idKeyIdentity idt

    load' = loadRec $ Identity
        <$> loadText "name"
        <*> loadMbRef "prev"
        <*> loadRef "key-id"

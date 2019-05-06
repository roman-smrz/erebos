module Identity (
    Identity, IdentityData(..),
    emptyIdentity,
) where

import Data.Text (Text)

import PubKey
import Storage

type Identity = Signed IdentityData

data IdentityData = Identity
    { idName :: Maybe Text
    , idPrev :: Maybe (Stored Identity)
    , idOwner :: Maybe (Stored Identity)
    , idKeyIdentity :: Stored PublicKey
    }
    deriving (Show)

emptyIdentity :: Stored PublicKey -> IdentityData
emptyIdentity key = Identity
    { idName = Nothing
    , idPrev = Nothing
    , idOwner = Nothing
    , idKeyIdentity = key
    }

instance Storable IdentityData where
    store' idt = storeRec $ do
        storeMbText "name" $ idName idt
        storeMbRef "prev" $ idPrev idt
        storeMbRef "owner" $ idOwner idt
        storeRef "key-id" $ idKeyIdentity idt

    load' = loadRec $ Identity
        <$> loadMbText "name"
        <*> loadMbRef "prev"
        <*> loadMbRef "owner"
        <*> loadRef "key-id"

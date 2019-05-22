module Identity (
    Identity, IdentityData(..),
    emptyIdentity,
    finalOwner,
    displayIdentity,
) where

import Data.Maybe
import Data.Text (Text)
import qualified Data.Text as T

import PubKey
import Storage

type Identity = Signed IdentityData

data IdentityData = Identity
    { idName :: Maybe Text
    , idPrev :: Maybe (Stored Identity)
    , idOwner :: Maybe (Stored Identity)
    , idKeyIdentity :: Stored PublicKey
    , idKeyMessage :: Stored PublicKey
    }
    deriving (Show)

emptyIdentity :: Stored PublicKey -> Stored PublicKey -> IdentityData
emptyIdentity key kmsg = Identity
    { idName = Nothing
    , idPrev = Nothing
    , idOwner = Nothing
    , idKeyIdentity = key
    , idKeyMessage = kmsg
    }

instance Storable IdentityData where
    store' idt = storeRec $ do
        storeMbText "name" $ idName idt
        storeMbRef "prev" $ idPrev idt
        storeMbRef "owner" $ idOwner idt
        storeRef "key-id" $ idKeyIdentity idt
        storeRef "key-msg" $ idKeyMessage idt

    load' = loadRec $ Identity
        <$> loadMbText "name"
        <*> loadMbRef "prev"
        <*> loadMbRef "owner"
        <*> loadRef "key-id"
        <*> loadRef "key-msg"

unfoldOwners :: Stored Identity -> [Stored Identity]
unfoldOwners cur = cur : case idOwner $ fromStored $ signedData $ fromStored cur of
                              Nothing   -> []
                              Just prev -> unfoldOwners prev

finalOwner :: Stored Identity -> Stored Identity
finalOwner = last . unfoldOwners

displayIdentity :: Stored Identity -> Text
displayIdentity sidentity = T.concat
    [ T.intercalate (T.pack " / ") $ map (fromMaybe (T.pack "<unnamed>") . idName . fromStored . signedData . fromStored) owners
    ]
    where owners = reverse $ unfoldOwners sidentity

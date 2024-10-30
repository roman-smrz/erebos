module Test.Service (
    TestMessage(..),
    TestMessageAttributes(..),
) where

import Control.Monad.Reader

import Data.ByteString.Lazy.Char8 qualified as BL

import Erebos.Network
import Erebos.Object
import Erebos.Service
import Erebos.Storable

data TestMessage = TestMessage (Stored Object)

data TestMessageAttributes = TestMessageAttributes
    { testMessageReceived :: Object -> String -> String -> String -> ServiceHandler TestMessage ()
    }

instance Storable TestMessage where
    store' (TestMessage msg) = store' msg
    load' = TestMessage <$> load'

instance Service TestMessage where
    serviceID _ = mkServiceID "cb46b92c-9203-4694-8370-8742d8ac9dc8"

    type ServiceAttributes TestMessage = TestMessageAttributes
    defaultServiceAttributes _ = TestMessageAttributes (\_ _ _ _ -> return ())

    serviceHandler smsg = do
        let TestMessage sobj = fromStored smsg
            obj = fromStored sobj
        case map BL.unpack $ BL.words $ BL.takeWhile (/='\n') $ serializeObject obj of
            [otype, len] -> do
                cb <- asks $ testMessageReceived . svcAttributes
                cb obj otype len (show $ refDigest $ storedRef sobj)
            _ -> return ()

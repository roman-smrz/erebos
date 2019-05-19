module Main (main) where

import Control.Concurrent
import Control.Exception
import Control.Monad

import Data.List
import Data.Maybe
import qualified Data.Text as T
import qualified Data.Text.IO as T

import System.Environment
import System.IO
import System.IO.Error

import Identity
import Message
import Network
import PubKey
import Storage


data Erebos = Erebos
    { erbIdentity :: Stored Identity
    , erbMessages :: StoredList DirectMessageThread
    }

instance Storable Erebos where
    store' erb = storeRec $ do
        storeRef "id" $ erbIdentity erb
        storeZRef "dmsgs" $ erbMessages erb

    load' = loadRec $ Erebos
        <$> loadRef "id"
        <*> loadZRef "dmsgs"


loadErebosHead :: Storage -> IO Head
loadErebosHead st = do
    catchJust (guard . isDoesNotExistError) (loadHead st "erebos") $ \_ -> do
        putStr "Name: "
        hFlush stdout
        name <- T.getLine

        (secret, public) <- generateKeys st
        (_secretMsg, publicMsg) <- generateKeys st
        (devSecret, devPublic) <- generateKeys st
        (_devSecretMsg, devPublicMsg) <- generateKeys st

        owner <- wrappedStore st =<< sign secret =<< wrappedStore st (emptyIdentity public publicMsg) { idName = Just name }
        identity <- wrappedStore st =<< signAdd devSecret =<< sign secret =<<
            wrappedStore st (emptyIdentity devPublic devPublicMsg) { idOwner = Just owner }

        msgs <- emptySList st
        let erebos = Erebos
                { erbIdentity = identity
                , erbMessages = msgs
                }

        Right h <- replaceHead erebos (Left (st, "erebos"))
        return h

updateErebosHead_ :: Storage -> (Stored Erebos -> IO (Stored Erebos)) -> IO ()
updateErebosHead_ st f = updateErebosHead st (fmap (,()) . f)

updateErebosHead :: Storage -> (Stored Erebos -> IO (Stored Erebos, a)) -> IO a
updateErebosHead st f = do
    erebosHead <- loadHead st "erebos"
    (erebos, x) <- f $ wrappedLoad (headRef erebosHead)
    Right _ <- replaceHead erebos (Right erebosHead)
    return x

main :: IO ()
main = do
    [bhost] <- getArgs
    st <- openStorage "test"
    erebosHead <- loadErebosHead st
    let serebos = wrappedLoad (headRef erebosHead) :: Stored Erebos
        self = erbIdentity $ fromStored serebos
    print $ fromStored self

    (chanPeer, chanSvc) <- startServer bhost $ erbIdentity $ fromStored serebos

    void $ forkIO $ void $ forever $ do
        peer@Peer { peerAddress = DatagramAddress addr } <- readChan chanPeer
        print addr
        putStrLn $ maybe "<noid>" show $ peerIdentity peer
        if | Just powner <- finalOwner <$> peerIdentity peer
           , _:_ <- peerChannels peer
           -> do
               msg <- updateErebosHead st $ \erb -> do
                   (slist, msg) <- case find ((== powner) . msgPeer . fromStored) (storedFromSList $ erbMessages $ fromStored erb) of
                       Just thread -> do
                           (msg, thread') <- createDirectMessage self (fromStored thread) (T.pack "Hello")
                           (,msg) <$> slistReplaceS thread thread' (erbMessages $ fromStored erb)
                       Nothing -> do
                           (msg, thread') <- createDirectMessage self (emptyDirectThread powner) (T.pack "Hello")
                           (,msg) <$> slistAddS thread' (erbMessages $ fromStored erb)
                   erb' <- wrappedStore st (fromStored erb) { erbMessages = slist }
                   return (erb', msg)
               sendToPeer self peer (T.pack "dmsg") msg

           | otherwise -> return ()

    void $ forever $ readChan chanSvc >>= \case
        (peer, svc, ref)
            | svc == T.pack "dmsg" -> do
                let msg = wrappedLoad ref
                putStr "Direct message from: "
                T.putStrLn $ fromMaybe (T.pack "<unnamed>") $ idName $ fromStored $ signedData $ fromStored $ msgFrom $ fromStored msg
                if | Just powner <- finalOwner <$> peerIdentity peer
                   , powner == msgFrom (fromStored msg)
                   -> updateErebosHead_ st $ \erb -> do
                          slist <- case find ((== powner) . msgPeer . fromStored) (storedFromSList $ erbMessages $ fromStored erb) of
                                        Just thread -> do thread' <- wrappedStore st (fromStored thread) { msgHead = msg : msgHead (fromStored thread) }
                                                          slistReplaceS thread thread' $ erbMessages $ fromStored erb
                                        Nothing -> slistAdd (emptyDirectThread powner) { msgHead = [msg] } $ erbMessages $ fromStored erb
                          wrappedStore st (fromStored erb) { erbMessages = slist }

                   | otherwise -> putStrLn $ "Owner mismatch"

            | otherwise -> T.putStrLn $ T.pack "Unknown service: " `T.append` svc

    return ()

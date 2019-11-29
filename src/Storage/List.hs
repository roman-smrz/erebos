module Storage.List (
    StoredList,
    emptySList, fromSList, storedFromSList,
    slistAdd, slistAddS, slistInsert, slistInsertS, slistRemove, slistReplace, slistReplaceS,
    mapFromSList, updateOld,

    StoreUpdate(..),
    withStoredListItem, withStoredListItemS,
) where

import Control.Monad
import Control.Monad.Reader

import Data.List
import Data.Map (Map)
import qualified Data.Map as M
import Data.Maybe

import Storage
import Storage.Internal

data List a = ListNil
            | ListItem (Maybe Ref) (Maybe Ref) (Maybe (Stored a)) (StoredList a)
    deriving (Show)

type StoredList a = Stored (List a)

instance Storable a => Storable (List a) where
    store' ListNil = storeZero
    store' (ListItem remove after item next) = storeRec $ do
        storeMbRawRef "r" remove
        storeMbRawRef "a" after
        storeMbRef "i" item
        storeRef "n" next

    load' = asks snd >>= \case
                ZeroObject -> return ListNil
                _          ->
                    loadRec $ ListItem
                        <$> loadMbRawRef "r"
                        <*> loadMbRawRef "a"
                        <*> loadMbRef "i"
                        <*> loadRef "n"

instance Storable a => ZeroStorable (List a) where
    fromZero _ = ListNil


emptySList :: Storable a => Storage -> IO (StoredList a)
emptySList st = wrappedStore st ListNil

fromSList :: StoredList a -> [a]
fromSList = map fromStored . storedFromSList

storedFromSList :: StoredList a -> [Stored a]
storedFromSList = fromSList' []
    where fromSList' :: [(Ref, Bool, [Stored a])] -> StoredList a -> [Stored a]
          fromSList' _ (Stored _ ListNil) = []
          fromSList' repl (Stored cref (ListItem rref aref x rest)) =
              case (rref, aref) of
                   (Nothing, Nothing) -> let (rx, repl') = findRepl cref x repl
                                          in rx ++ fromSList' repl' rest
                   (Just r , Nothing) -> fromSList' (addReplace cref r x repl) rest
                   (Nothing, Just a ) -> fromSList' (addInsert  cref a x repl) rest
                   (Just r , Just a ) -> fromSList' (addReplace cref r x $ addInsert cref a x repl) rest

          addReplace = findAddRepl False
          addInsert = findAddRepl True

          findAddRepl :: Bool -> Ref -> Ref -> Maybe (Stored a) -> [(Ref, Bool, [Stored a])] -> [(Ref, Bool, [Stored a])]
          findAddRepl keep c t x rs = let (x', rs') = findRepl c x rs
                                       in addRepl keep c t x' rs'

          addRepl :: Bool -> Ref -> Ref -> [Stored a] -> [(Ref, Bool, [Stored a])] -> [(Ref, Bool, [Stored a])]
          addRepl keep _ t x [] = [(t, keep, x)]
          addRepl keep c t x ((pr, pk, px) : rs)
              | pr == c   = (t , keep, x ++ px) : rs
              | pr == t   = (t , pk, px ++ x) : rs
              | otherwise = (pr, pk, px) : addRepl keep c t x rs

          findRepl :: Ref -> Maybe (Stored a) -> [(Ref, Bool, [Stored a])] -> ([Stored a], [(Ref, Bool, [Stored a])])
          findRepl _ x [] = (maybeToList x, [])
          findRepl c x ((pr, pk, px) : rs)
              | pr == c   = (if pk then maybe id (:) x px else px, rs)
              | otherwise = ((pr, pk, px):) <$> findRepl c x rs

slistAdd :: Storable a => a -> StoredList a -> IO (StoredList a)
slistAdd x next@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistAddS sx next

slistAddS :: Storable a => Stored a -> StoredList a -> IO (StoredList a)
slistAddS sx next@(Stored (Ref st _) _) = wrappedStore st (ListItem Nothing Nothing (Just sx) next)

slistInsert :: Storable a => Stored a -> a -> StoredList a -> IO (StoredList a)
slistInsert after x next@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistInsertS after sx next

slistInsertS :: Storable a => Stored a -> Stored a -> StoredList a -> IO (StoredList a)
slistInsertS after sx next@(Stored (Ref st _) _) = wrappedStore st $ ListItem Nothing (findSListRef after next) (Just sx) next

slistRemove :: Storable a => Stored a -> StoredList a -> IO (StoredList a)
slistRemove rm next@(Stored (Ref st _) _) = wrappedStore st $ ListItem (findSListRef rm next) Nothing Nothing next

slistReplace :: Storable a => Stored a -> a -> StoredList a -> IO (StoredList a)
slistReplace rm x next@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistReplaceS rm sx next

slistReplaceS :: Storable a => Stored a -> Stored a -> StoredList a -> IO (StoredList a)
slistReplaceS rm sx next@(Stored (Ref st _) _) = wrappedStore st $ ListItem (findSListRef rm next) Nothing (Just sx) next

findSListRef :: Stored a -> StoredList a -> Maybe Ref
findSListRef _ (Stored _ ListNil) = Nothing
findSListRef x (Stored ref (ListItem _ _ y next)) | y == Just x = Just ref
                                                  | otherwise   = findSListRef x next

mapFromSList :: Storable a => StoredList a -> Map RefDigest (Stored a)
mapFromSList list = helper list M.empty
    where helper :: Storable a => StoredList a -> Map RefDigest (Stored a) -> Map RefDigest (Stored a)
          helper (Stored _ ListNil) cur = cur
          helper (Stored _ (ListItem (Just rref) _ (Just x) rest)) cur =
              let rxref = case load rref of
                               ListItem _ _ (Just rx) _  -> sameType rx x $ storedRef rx
                               _ -> error "mapFromSList: malformed list"
               in helper rest $ case M.lookup (refDigest $ storedRef x) cur of
                                     Nothing -> M.insert (refDigest rxref) x cur
                                     Just x' -> M.insert (refDigest rxref) x' cur
          helper (Stored _ (ListItem _ _ _ rest)) cur = helper rest cur
          sameType :: a -> a -> b -> b
          sameType _ _ x = x

updateOld :: Map RefDigest (Stored a) -> Stored a -> Stored a
updateOld m x = fromMaybe x $ M.lookup (refDigest $ storedRef x) m


data StoreUpdate a = StoreKeep
                   | StoreReplace a
                   | StoreRemove

withStoredListItem :: (Storable a) => (a -> Bool) -> StoredList a -> (a -> IO (StoreUpdate a)) -> IO (StoredList a)
withStoredListItem p list f = withStoredListItemS (p . fromStored) list (suMap (wrappedStore $ storedStorage list) <=< f . fromStored)
    where suMap :: Monad m => (a -> m b) -> StoreUpdate a -> m (StoreUpdate b)
          suMap _ StoreKeep = return StoreKeep
          suMap g (StoreReplace x) = return . StoreReplace =<< g x
          suMap _ StoreRemove = return StoreRemove

withStoredListItemS :: (Storable a) => (Stored a -> Bool) -> StoredList a -> (Stored a -> IO (StoreUpdate (Stored a))) -> IO (StoredList a)
withStoredListItemS p list f = do
    case find p $ storedFromSList list of
         Just sx -> f sx >>= \case StoreKeep -> return list
                                   StoreReplace nx -> slistReplaceS sx nx list
                                   StoreRemove -> slistRemove sx list
         Nothing -> return list

module Storage.List (
    StoredList,
    emptySList, fromSList, storedFromSList,
    slistAdd, slistAddS,
    -- TODO slistInsert, slistInsertS,
    slistRemove, slistReplace, slistReplaceS,
    -- TODO mapFromSList, updateOld,

    -- TODO StoreUpdate(..),
    -- TODO withStoredListItem, withStoredListItemS,
) where

import Control.Monad.Reader

import Data.List
import Data.Maybe
import qualified Data.Set as S

import Storage
import Storage.Internal
import Storage.Merge

data List a = ListNil
            | ListItem { listPrev :: [StoredList a]
                       , listItem :: Maybe (Stored a)
                       , listRemove :: Maybe (Stored (List a))
                       }

type StoredList a = Stored (List a)

instance Storable a => Storable (List a) where
    store' ListNil = storeZero
    store' x@ListItem {} = storeRec $ do
        mapM_ (storeRef "PREV") $ listPrev x
        mapM_ (storeRef "item") $ listItem x
        mapM_ (storeRef "remove") $ listRemove x

    load' = asks snd >>= \case
        ZeroObject -> return ListNil
        _ -> loadRec $ ListItem <$> loadRefs "PREV"
                                <*> loadMbRef "item"
                                <*> loadMbRef "remove"

instance Storable a => ZeroStorable (List a) where
    fromZero _ = ListNil


emptySList :: Storable a => Storage -> IO (StoredList a)
emptySList st = wrappedStore st ListNil

groupsFromSLists :: forall a. Storable a => StoredList a -> [[Stored a]]
groupsFromSLists = helperSelect S.empty . (:[])
  where
    helperSelect :: S.Set (StoredList a) -> [StoredList a] -> [[Stored a]]
    helperSelect rs xxs | x:xs <- sort $ filterRemoved rs xxs = helper rs x xs
                        | otherwise = []

    helper :: S.Set (StoredList a) -> StoredList a -> [StoredList a] -> [[Stored a]]
    helper rs x xs
        | ListNil <- fromStored x
        = []

        | Just rm <- listRemove (fromStored x)
        , ans <- ancestors [x]
        , (other, collision) <- partition (S.null . S.intersection ans . ancestors . (:[])) xs
        , cont <- helperSelect (rs `S.union` ancestors [rm]) $ concatMap (listPrev . fromStored) (x : collision) ++ other
        = case catMaybes $ map (listItem . fromStored) (x : collision) of
               [] -> cont
               xis -> xis : cont

        | otherwise = case listItem (fromStored x) of
                           Nothing -> helperSelect rs $ listPrev (fromStored x) ++ xs
                           Just xi -> [xi] : (helperSelect rs $ listPrev (fromStored x) ++ xs)

    filterRemoved :: S.Set (StoredList a) -> [StoredList a] -> [StoredList a]
    filterRemoved rs = filter (S.null . S.intersection rs . ancestors . (:[]))

fromSList :: Mergeable a => StoredList (Component a) -> [a]
fromSList = map merge . groupsFromSLists

storedFromSList :: (Mergeable a, Storable a) => StoredList (Component a) -> IO [Stored a]
storedFromSList = mapM storeMerge . groupsFromSLists

slistAdd :: Storable a => a -> StoredList a -> IO (StoredList a)
slistAdd x prev@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistAddS sx prev

slistAddS :: Storable a => Stored a -> StoredList a -> IO (StoredList a)
slistAddS sx prev@(Stored (Ref st _) _) = wrappedStore st (ListItem [prev] (Just sx) Nothing)

{- TODO
slistInsert :: Storable a => Stored a -> a -> StoredList a -> IO (StoredList a)
slistInsert after x prev@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistInsertS after sx prev

slistInsertS :: Storable a => Stored a -> Stored a -> StoredList a -> IO (StoredList a)
slistInsertS after sx prev@(Stored (Ref st _) _) = wrappedStore st $ ListItem Nothing (findSListRef after prev) (Just sx) prev
-}

slistRemove :: Storable a => Stored a -> StoredList a -> IO (StoredList a)
slistRemove rm prev@(Stored (Ref st _) _) = wrappedStore st $ ListItem [prev] Nothing (findSListRef rm prev)

slistReplace :: Storable a => Stored a -> a -> StoredList a -> IO (StoredList a)
slistReplace rm x prev@(Stored (Ref st _) _) = do
    sx <- wrappedStore st x
    slistReplaceS rm sx prev

slistReplaceS :: Storable a => Stored a -> Stored a -> StoredList a -> IO (StoredList a)
slistReplaceS rm sx prev@(Stored (Ref st _) _) = wrappedStore st $ ListItem [prev] (Just sx) (findSListRef rm prev)

findSListRef :: Stored a -> StoredList a -> Maybe (StoredList a)
findSListRef _ (Stored _ ListNil) = Nothing
findSListRef x cur | listItem (fromStored cur) == Just x = Just cur
                   | otherwise                           = listToMaybe $ catMaybes $ map (findSListRef x) $ listPrev $ fromStored cur

{- TODO
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
-}

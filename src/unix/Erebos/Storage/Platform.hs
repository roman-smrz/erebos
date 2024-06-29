{-# LANGUAGE CPP #-}

module Erebos.Storage.Platform (
    createFileExclusive,
) where

import System.IO
import System.Posix.Files
import System.Posix.IO

createFileExclusive :: FilePath -> IO Handle
createFileExclusive path = fdToHandle =<< do
#if MIN_VERSION_unix(2,8,0)
    openFd path WriteOnly defaultFileFlags
        { creat = Just $ unionFileModes ownerReadMode ownerWriteMode
        , exclusive = True
        }
#else
    openFd path WriteOnly (Just $ unionFileModes ownerReadMode ownerWriteMode) (defaultFileFlags { exclusive = True })
#endif

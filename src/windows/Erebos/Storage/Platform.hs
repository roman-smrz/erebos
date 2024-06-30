module Erebos.Storage.Platform (
    createFileExclusive,
) where

import Data.Bits

import System.IO
import System.Win32.File
import System.Win32.Types

createFileExclusive :: FilePath -> IO Handle
createFileExclusive path = do
    hANDLEToHandle =<< createFile path gENERIC_WRITE (fILE_SHARE_READ .|. fILE_SHARE_DELETE) Nothing cREATE_NEW fILE_ATTRIBUTE_NORMAL Nothing

module Erebos.Network.Protocol where

import Erebos.UUID (UUID)

newtype ServiceID = ServiceID UUID
instance Show ServiceID

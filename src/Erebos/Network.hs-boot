module Erebos.Network where

import Erebos.Storage

data Server
data Peer

peerStorage :: Peer -> Storage

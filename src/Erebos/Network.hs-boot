module Erebos.Network where

import Erebos.Object.Internal

data Server
data Peer
data PeerAddress

peerStorage :: Peer -> Storage

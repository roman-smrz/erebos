module Network where

import Storage

data Server
data Peer

peerStorage :: Peer -> Storage

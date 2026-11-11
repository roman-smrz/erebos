{-# LANGUAGE CPP #-}

module Erebos.Network.Address (
    InetAddress(..),
    inetFromSockAddr,
    inetToSockAddr,
    makeLocalhostAddress,
    getLocalhostPort,

    SockAddr, PortNumber,
) where

import Data.Bifunctor
#ifdef ENABLE_SOCKETS
import Data.IP qualified as IP
#endif
import Data.Word

import Foreign.C.Types
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.Storable as F

#ifdef ENABLE_SOCKETS
import Network.Socket
#endif

import Text.Read


#ifdef ENABLE_SOCKETS

newtype InetAddress = InetAddress { fromInetAddress :: IP.IP }
    deriving (Eq, Ord)

instance Show InetAddress where
    show (InetAddress ipaddr)
        | IP.IPv6 ipv6 <- ipaddr
        , ( 0, 0, 0xffff, ipv4 ) <- IP.fromIPv6w ipv6
        = show (IP.toIPv4w ipv4)

        | otherwise
        = show ipaddr

instance Read InetAddress where
    readPrec = do
        readPrec >>= return . InetAddress . \case
            IP.IPv4 ipv4 -> IP.IPv6 $ IP.toIPv6w ( 0, 0, 0xffff, IP.fromIPv4w ipv4 )
            ipaddr       -> ipaddr

    readListPrec = readListPrecDefault

instance F.Storable InetAddress where
    sizeOf _ = sizeOf (undefined :: CInt) + 16
    alignment _ = 8

    peek ptr = (unpackFamily <$> peekByteOff ptr 0) >>= \case
        AF_INET -> InetAddress . IP.IPv4 . IP.fromHostAddress <$> peekByteOff ptr (sizeOf (undefined :: CInt))
        AF_INET6 -> InetAddress . IP.IPv6 . IP.toIPv6b . map fromIntegral <$> peekArray 16 (ptr `plusPtr` sizeOf (undefined :: CInt) :: Ptr Word8)
        _ -> fail "InetAddress: unknown family"

    poke ptr (InetAddress addr) = case addr of
        IP.IPv4 ip -> do
            pokeByteOff ptr 0 (packFamily AF_INET)
            pokeByteOff ptr (sizeOf (undefined :: CInt)) (IP.toHostAddress ip)
        IP.IPv6 ip -> do
            pokeByteOff ptr 0 (packFamily AF_INET6)
            pokeArray (ptr `plusPtr` sizeOf (undefined :: CInt) :: Ptr Word8) (map fromIntegral $ IP.fromIPv6b ip)
#else

data SockAddr
    deriving (Eq, Ord, Show, Read)

type PortNumber = Word16

data InetAddress
    deriving (Eq, Ord, Show, Read)

#endif


inetFromSockAddr :: SockAddr -> Maybe ( InetAddress, PortNumber )
#ifdef ENABLE_SOCKETS
inetFromSockAddr saddr = first InetAddress <$> IP.fromSockAddr saddr
#else
inetFromSockAddr = \case {}
#endif

inetToSockAddr :: ( InetAddress, PortNumber ) -> SockAddr
#ifdef ENABLE_SOCKETS
inetToSockAddr = IP.toSockAddr . first fromInetAddress
#else
inetToSockAddr ( addr, _ ) = case addr of {}
#endif


makeLocalhostAddress :: PortNumber -> SockAddr
#ifdef ENABLE_SOCKETS
makeLocalhostAddress port = SockAddrInet6 port 0 ( 0, 0, 0, 1 ) 0
#else
makeLocalhostAddress _ = undefined
#endif

getLocalhostPort :: SockAddr -> Maybe PortNumber
#ifdef ENABLE_SOCKETS
getLocalhostPort (SockAddrInet6 port _ ( 0, 0, 0, 1 ) _) = Just port
#endif
getLocalhostPort _                                       = Nothing

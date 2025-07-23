module Erebos.Network.Address (
    InetAddress(..),
    inetFromSockAddr,
    inetToSockAddr,

    SockAddr, PortNumber,
) where

import Data.Bifunctor
import Data.IP qualified as IP
import Data.Word

import Foreign.C.Types
import Foreign.Marshal.Array
import Foreign.Ptr
import Foreign.Storable as F

import Network.Socket

import Text.Read


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


inetFromSockAddr :: SockAddr -> Maybe ( InetAddress, PortNumber )
inetFromSockAddr saddr = first InetAddress <$> IP.fromSockAddr saddr

inetToSockAddr :: ( InetAddress, PortNumber ) -> SockAddr
inetToSockAddr = IP.toSockAddr . first fromInetAddress

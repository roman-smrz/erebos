module Erebos.Service.Stream (
    StreamPacket(..),
    StreamReader, getStreamReaderNumber,
    StreamWriter, getStreamWriterNumber,
    openStream, receivedStreams,
    readStreamPacket, writeStreamPacket,
    writeStream,
    closeStream, 
) where

import Control.Concurrent.MVar
import Control.Monad.Reader
import Control.Monad.Writer

import Data.ByteString (ByteString)
import Data.Word

import Erebos.Flow
import Erebos.Network
import Erebos.Network.Protocol
import Erebos.Service


data StreamReader = StreamReader RawStreamReader

getStreamReaderNumber :: StreamReader -> IO Int
getStreamReaderNumber (StreamReader stream) = return $ rsrNum stream

data StreamWriter = StreamWriter (MVar StreamWriterData)

data StreamWriterData = StreamWriterData
    { swdStream :: RawStreamWriter
    , swdSequence :: Maybe Word64
    }

getStreamWriterNumber :: StreamWriter -> IO Int
getStreamWriterNumber (StreamWriter stream) = rswNum . swdStream <$> readMVar stream


openStream :: Service s => ServiceHandler s StreamWriter
openStream = do
    mvar <- liftIO newEmptyMVar
    tell [ ServiceOpenStream $ \stream -> putMVar mvar $ StreamWriterData stream (Just 0) ]
    return $ StreamWriter mvar

receivedStreams :: Service s => ServiceHandler s [ StreamReader ]
receivedStreams = do
    map StreamReader <$> asks svcNewStreams

readStreamPacket :: StreamReader -> IO StreamPacket
readStreamPacket (StreamReader stream) = do
    readFlowIO (rsrFlow stream)

writeStreamPacket :: StreamWriter -> StreamPacket -> IO ()
writeStreamPacket (StreamWriter mvar) packet = do
    withMVar mvar $ \swd -> do
        writeFlowIO (rswFlow $ swdStream swd) packet

writeStream :: StreamWriter -> ByteString -> IO ()
writeStream (StreamWriter mvar) bytes = do
    modifyMVar_ mvar $ \swd -> do
        case swdSequence swd of
            Just seqNum -> do
                writeFlowIO (rswFlow $ swdStream swd) $ StreamData seqNum bytes
                return swd { swdSequence = Just (seqNum + 1) }
            Nothing -> do
                fail "writeStream: stream closed"

closeStream :: StreamWriter -> IO ()
closeStream (StreamWriter mvar) = do
    withMVar mvar $ \swd -> do
        case swdSequence swd of
            Just seqNum -> writeFlowIO (rswFlow $ swdStream swd) $ StreamClosed seqNum
            Nothing -> fail "closeStream: stream already closed"

module Erebos.Flow (
    Flow, SymFlow,
    newFlow, newFlowIO,
    readFlow, tryReadFlow, canReadFlow,
    writeFlow, writeFlowBulk, tryWriteFlow, canWriteFlow,
    readFlowIO, writeFlowIO,

    mapFlow,
) where

import Control.Concurrent.STM


data Flow r w
    = Flow (TBQueue r) (TBQueue w)
    | forall r' w'. MappedFlow (r' -> r) (w -> w') (Flow r' w')

type SymFlow a = Flow a a

newFlow :: STM (Flow a b, Flow b a)
newFlow = do
    x <- newTBQueue 16
    y <- newTBQueue 16
    return (Flow x y, Flow y x)

newFlowIO :: IO (Flow a b, Flow b a)
newFlowIO = atomically newFlow

readFlow :: Flow r w -> STM r
readFlow (Flow rvar _) = readTBQueue rvar
readFlow (MappedFlow f _ up) = f <$> readFlow up

tryReadFlow :: Flow r w -> STM (Maybe r)
tryReadFlow (Flow rvar _) = tryReadTBQueue rvar
tryReadFlow (MappedFlow f _ up) = fmap f <$> tryReadFlow up

canReadFlow :: Flow r w -> STM Bool
canReadFlow (Flow rvar _) = not <$> isEmptyTBQueue rvar
canReadFlow (MappedFlow _ _ up) = canReadFlow up

writeFlow :: Flow r w -> w -> STM ()
writeFlow (Flow _ wvar) = writeTBQueue wvar
writeFlow (MappedFlow _ f up) = writeFlow up . f

writeFlowBulk :: Flow r w -> [w] -> STM ()
writeFlowBulk _ [] = return ()
writeFlowBulk (Flow _ wvar) xs = mapM_ (writeTBQueue wvar) xs
writeFlowBulk (MappedFlow _ f up) xs = writeFlowBulk up $ map f xs

tryWriteFlow :: Flow r w -> w -> STM Bool
tryWriteFlow (Flow _ wvar) x = do
    isFullTBQueue wvar >>= \case
        True -> return False
        False -> do
            writeTBQueue wvar x
            return True
tryWriteFlow (MappedFlow _ f up) x = tryWriteFlow up $ f x

canWriteFlow :: Flow r w -> STM Bool
canWriteFlow (Flow _ wvar) = not <$> isFullTBQueue wvar
canWriteFlow (MappedFlow _ _ up) = canWriteFlow up

readFlowIO :: Flow r w -> IO r
readFlowIO path = atomically $ readFlow path

writeFlowIO :: Flow r w -> w -> IO ()
writeFlowIO path = atomically . writeFlow path


mapFlow :: (r -> r') -> (w' -> w) -> Flow r w -> Flow r' w'
mapFlow rf wf (MappedFlow rf' wf' up) = MappedFlow (rf . rf') (wf' . wf) up
mapFlow rf wf up = MappedFlow rf wf up

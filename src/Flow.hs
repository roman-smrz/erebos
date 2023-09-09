module Flow (
    Flow, SymFlow,
    newFlow, newFlowIO,
    readFlow, writeFlow, writeFlowBulk,
    readFlowIO, writeFlowIO,

    mapFlow,
) where

import Control.Concurrent.STM


data Flow r w = Flow (TMVar [r]) (TMVar [w])
              | forall r' w'. MappedFlow (r' -> r) (w -> w') (Flow r' w')

type SymFlow a = Flow a a

newFlow :: STM (Flow a b, Flow b a)
newFlow = do
    x <- newEmptyTMVar
    y <- newEmptyTMVar
    return (Flow x y, Flow y x)

newFlowIO :: IO (Flow a b, Flow b a)
newFlowIO = atomically newFlow

readFlow :: Flow r w -> STM r
readFlow (Flow rvar _) = takeTMVar rvar >>= \case
    (x:[]) -> return x
    (x:xs) -> putTMVar rvar xs >> return x
    [] -> error "Flow: empty list"
readFlow (MappedFlow f _ up) = f <$> readFlow up

writeFlow :: Flow r w -> w -> STM ()
writeFlow (Flow _ wvar) = putTMVar wvar . (:[])
writeFlow (MappedFlow _ f up) = writeFlow up . f

writeFlowBulk :: Flow r w -> [w] -> STM ()
writeFlowBulk _ [] = return ()
writeFlowBulk (Flow _ wvar) xs = putTMVar wvar xs
writeFlowBulk (MappedFlow _ f up) xs = writeFlowBulk up $ map f xs

readFlowIO :: Flow r w -> IO r
readFlowIO path = atomically $ readFlow path

writeFlowIO :: Flow r w -> w -> IO ()
writeFlowIO path = atomically . writeFlow path


mapFlow :: (r -> r') -> (w' -> w) -> Flow r w -> Flow r' w'
mapFlow rf wf (MappedFlow rf' wf' up) = MappedFlow (rf . rf') (wf' . wf) up
mapFlow rf wf up = MappedFlow rf wf up

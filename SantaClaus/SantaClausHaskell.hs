import Control.Monad (when, liftM)
import System.Random (randomRIO)
import Control.Concurrent (threadDelay, forkIO)
import Control.Concurrent.STM
    (atomically, STM, retry, TVar, newTVarIO, readTVar, readTVarIO, writeTVar)

-- Helper Functions
-------------------
gotoSanta :: TVar [Int] -> Int -> STM ()
gotoSanta santasDoor wId = do
    queueAtDoor <- readTVar santasDoor
    {- Forces a reindeer to go back to sleep if it was
    awoken by a the change of sleepTVar by Santa
    helping a set of Elfs, simplifies the program
    as separate sleepTVar's else would be needed -}
    if wId `elem` queueAtDoor
       then retry
       else writeTVar santasDoor (wId : queueAtDoor)

lengthOfTheQueue :: TVar [Int] -> STM Int
lengthOfTheQueue santasDoor = liftM length $ readTVar santasDoor

sleepRandomInterval :: Int -> IO ()
sleepRandomInterval maxSleep = randomRIO (1 * 1000000, maxSleep * 1000000) >>=
    threadDelay

-- Reindeer and Elf
-------------------
worker :: TVar [Int] -> Int -> Int -> Int -> TVar Bool -> IO ()
worker queue maxQueue wId maxSleep sleepTVar = do
    sleepRandomInterval maxSleep
    atomically (do
       queueLength <- lengthOfTheQueue queue
       if queueLength < maxQueue then gotoSanta queue wId else retry
       when (queueLength + 1 == maxQueue) $ wakeSanta sleepTVar)
    worker queue maxQueue wId maxSleep sleepTVar

-- Santa
--------
sleepSanta :: TVar Bool -> STM ()
sleepSanta sleepTVar = do
    {- Forces Santa to block until "sleepTVar" is set to
    "True" by another thread calling "wakeSanta", when he is
    awoken does he set "sleepTVar" to "False" so he wille go
    back to sleep when calling the "sleepSanta" function -}
    sleepBool <- readTVar sleepTVar
    if sleepBool then writeTVar sleepTVar False else retry

wakeSanta :: TVar Bool -> STM ()
wakeSanta sleepTVar = do
    {- Checks is Santa is already awoken and wakes him if he
    is not, if he is awake and currently helping another
    group of workers are the thread forced to block until
    "sleepTVar" changed by Santa when he goes back to sleep,
    this is nessesary to prevent starvation of the Elfs -}
    sleepBool <- readTVar sleepTVar
    if sleepBool then retry else writeTVar sleepTVar True

workSanta :: [Int] -> TVar [Int] -> String -> IO ()
workSanta workers queue phrase = do
    putStrLn $ "Santa: " ++ phrase ++ show workers
    sleepRandomInterval 5
    atomically $ writeTVar queue []

santa :: TVar [Int] -> TVar [Int] -> TVar Bool -> IO ()
santa elfQueue reindeerQueue sleepTVar = do
    atomically $ sleepSanta sleepTVar
    elfs <- readTVarIO elfQueue
    reindeers <- readTVarIO reindeerQueue
    if length reindeers == 9
    then workSanta reindeers reindeerQueue
        "ho ho delivering presents with reindeers: "
    else workSanta elfs elfQueue
        "ho ho helping elfs: "
    santa elfQueue reindeerQueue sleepTVar

-- Main
-------
main :: IO ()
main = do
    sleepTVar <- newTVarIO False
    elfQueue <-  newTVarIO []
    reindeerQueue <- newTVarIO []
    mapM_ (\ wId -> forkIO $ worker elfQueue 3 wId 15 sleepTVar) [1 .. 30]
    mapM_ (\ wId -> forkIO $ worker reindeerQueue 9 wId 30 sleepTVar) [1 .. 9]
    santa elfQueue reindeerQueue sleepTVar

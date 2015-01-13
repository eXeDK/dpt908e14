-- An STM Based Parallel K-Means Implementation Written in Haskell
-- ============================================================================
-- [DONE] Linted Using
-- https://github.com/ndmitchell/hlint
-- http://projects.haskell.org/style-scanner/
--
-- Sources of inspiration
-- ============================================================================
-- http://en.wikibooks.org/wiki/Data_Mining_Algorithms_In_R/Clustering/K-Means
--
-- https://www.haskell.org/haskellwiki/Performance
-- https://www.haskell.org/platform/doc/2014.2.0.0/platform/doc/frames.html
-- https://github.com/tibbe/haskell-style-guide/blob/master/haskell-style.md
-- http://stackoverflow.com/questions/9611904/haskell-lists-arrays-vectors-sequences

import Data.Char (isAlpha)
import Data.Time.Clock.POSIX (getPOSIXTime)
import Data.Time.LocalTime (getZonedTime)
import Data.List.Split (splitOn)
import Control.DeepSeq (deepseq, NFData, rnf)
import Control.Concurrent (forkIO, getNumCapabilities)
import Control.Concurrent.STM
import Control.Monad (replicateM_, void, liftM)
import System.Exit (exitFailure)
import System.Environment (getArgs)

import Prelude as P
import Data.Sequence as S (Seq, fromList, mapWithIndex, foldlWithIndex, index, update)
import Data.Vector as V (Vector, fromList, map, head, tail, foldl, length, null, zipWith, sum, splitAt, mapM_, concat, toList, take)


---------------------
-- Data Structures --
---------------------
data Point = Point { pFeatures :: Vector Double
                   , pCluster :: Int
                   , pLabel :: Int -- -1 If non exist
                   } deriving (Show)

data Centroid = Centroid { cFeatures :: Vector Double
                         , cCluster :: Int
                         } deriving (Show)


{- Point and Centroid is made an instance of NFData to support the use of `deepseq`,
   forcing the results to be strictly computed instead of lazely, simplifying bencmark
-}
instance NFData Point where
    rnf (Point features cluster label) = features `seq` cluster `seq` label `seq` ()

instance NFData Centroid where
    rnf (Centroid features cluster) = features `seq` cluster `seq` ()


----------
-- Main --
----------
main :: IO ()
main = do
    -- Prints the number of system threads the used by the runtime for scheduling
    putStrLn $ "-------------------------------------------------"
    systemThreads <- getNumCapabilities
    putStrLn $ "Number of System Threads Used: " ++ show systemThreads
    putStrLn $ "-------------------------------------------------"
    -- Configuration Parameters
    (nThreads, numberOfCentroids, datasetFilepath) <- getArgs >>= parseArgs
    getZonedTime >>= (\stamp -> putStrLn $ "Parsing Dataset:   " ++ show stamp)
    let maxIterations = 300
    dataset <- liftM V.fromList (readDataset datasetFilepath "," True)
    let centroids = takeNPointsAsCentroids numberOfCentroids dataset
    -- Run KMeans and compute execution time
    getZonedTime >>= (\stamp -> putStrLn $ "Starting K-Means:  " ++ show stamp)
    startTime <- getPOSIXTime
    clusteredPoints <- kMeans dataset centroids maxIterations nThreads
    endTime <- clusteredPoints `deepseq` getPOSIXTime -- deepseq force strict execution
    getZonedTime >>= (\stamp -> putStrLn $ "Finnished K-Means: " ++ show stamp)
    let percentageScore = computeCorrectClassefiedPercentage clusteredPoints
    -- Print Output
    putStrLn $ "\nDataset: " ++ datasetFilepath
    putStrLn $ "Maximum Iterations: " ++ show maxIterations
    putStrLn $ "Execution Time: " ++ show (endTime - startTime)
    putStrLn $ "Percentage Score: " ++ show percentageScore
    putStrLn "\nCentroids: " >> printNElements centroids (-1)
    return ()

takeNPointsAsCentroids :: Int -> Vector Point -> Vector Centroid
takeNPointsAsCentroids n dataset = V.fromList $ P.zipWith (Centroid . pFeatures)
                                            (V.toList $ V.take n dataset) [0..]

irisCentroids :: Vector Centroid
irisCentroids =  V.fromList $ mapListWithIndex Centroid centroids
  where centroids = P.map V.fromList
            [[5.006, 3.418, 1.464, 0.244],
            [5.9016129, 2.7483871, 4.39354839, 1.43387097],
            [6.85, 3.07368421, 5.74210526, 2.07105263]]

----------------------
-- KMeans Interface --
----------------------
kMeans :: Vector Point -> Vector Centroid  -> Int -> Int -> IO (Vector Point)
kMeans points centroids maxIterations nThreads = do
    -- Extracts and creates all configuration parameters and data structures
    let nPoints = V.length points
    let nClusters = V.length centroids
    let partitionSize = floor $ fromIntegral nPoints / fromIntegral nThreads
    let leftoverCount = nPoints `mod` nThreads
    let partitionedDataset = partitionData points partitionSize leftoverCount 0 []
    -- Setup of barriers and communication queue
    barrier <- newBarrier nThreads
    let awaitOnConcreteBarrier = awaitOnBarrier barrier
    recieveUpdatedCentroidQueue <- atomically newTQueue
    sendCentroidProposalQueue <- atomically newTQueue
    sendClusteredPointsQueue <- atomically newTQueue
    -- Starts the asked number of threads with a part of the points and the centroids
    V.mapM_ (startThreads maxIterations nPoints nClusters
            recieveUpdatedCentroidQueue sendCentroidProposalQueue
            sendClusteredPointsQueue awaitOnConcreteBarrier centroids) partitionedDataset
    computeCentroidsFromProposals False nThreads
        sendCentroidProposalQueue recieveUpdatedCentroidQueue
    recieceClusteredPoints nThreads sendClusteredPointsQueue []

partitionData :: Vector Point
              -> Int
              -> Int
              -> Int
              -> [(Int, Vector Point)]
              -> Vector (Int, Vector Point)
partitionData points partitionSize leftoverCount threadId acc
    | V.null points = V.fromList $ P.reverse acc
    | leftoverCount == 0 =
                let (part, rest) = V.splitAt partitionSize points in
                    recur rest part leftoverCount
    | otherwise =
                let (part, rest) = V.splitAt (partitionSize + 1) points in
                    recur rest part (leftoverCount - 1)
    where recur rest part count = partitionData rest partitionSize count
                                        (threadId + 1) ((threadId, part) : acc)

startThreads :: Int
             -> Int
             -> Int
             -> TQueue (Vector Centroid, Bool)
             -> TQueue (Vector (Int, Vector Double), Bool)
             -> TQueue (Vector Point)
             -> (Int -> IO ())
             -> Vector Centroid
             -> (Int, Vector Point)
             -> IO ()
startThreads iterations nPoints nClusters recieveUpdatedCentroidQueue
        sendCentroidProposalQueue sendClusteredPointsQueue awaitOnConcreteBarrier
        centroids (threadId, points) =
    void $ forkIO (runKMeans points centroids iterations nPoints nClusters
                  recieveUpdatedCentroidQueue sendCentroidProposalQueue
                  sendClusteredPointsQueue awaitOnConcreteBarrier threadId)

----------------------------
-- KMeans Parallel Runner --
----------------------------
runKMeans :: Vector Point
          -> Vector Centroid
          -> Int
          -> Int
          -> Int
          -> TQueue (Vector Centroid, Bool)
          -> TQueue (Vector (Int, Vector Double), Bool)
          -> TQueue (Vector Point)
          -> (Int -> IO())
          -> Int
          -> IO ()
runKMeans points _ 0 _ _ _ sendCentroidProposalQueue sendClusteredPointsQueue _ _ =
    -- The algorithm have run the maximum number of iterations and send back the result
    writeToQueue sendCentroidProposalQueue (emptyVector, False) >>
    writeToQueue sendClusteredPointsQueue points
runKMeans points centroids iterations nPoints nClusters recieveUpdatedCentroidQueue
    sendCentroidProposalQueue sendClusteredPointsQueue awaitOnConcreteBarrier threadId = do
        -- Computes new points and centroids
        let !updatedPoints = V.map (`updateCluster` centroids) points
        let !centroidProposals = updateCentroids updatedPoints nClusters
        let !isPointsChanged = pointsChanged points updatedPoints False
        -- Send centroid proposals to the summation function
        writeToQueue sendCentroidProposalQueue (centroidProposals, isPointsChanged)
        -- A barrier that prevent a single thread from running multiple iterations
        _ <- awaitOnConcreteBarrier threadId
        -- Retrieves the updated set of centroids and termination flag from the sumnation function
        (updatedCentroids, allFinished) <-
           atomically $ readTQueue recieveUpdatedCentroidQueue
        -- If all threads reported that no points changed cluster are KMeans terminated
        if allFinished
           then writeToQueue sendClusteredPointsQueue points
           else runKMeans updatedPoints updatedCentroids (iterations - 1) nPoints
               nClusters recieveUpdatedCentroidQueue sendCentroidProposalQueue
               sendClusteredPointsQueue awaitOnConcreteBarrier threadId

computeCentroidsFromProposals :: Bool
                              -> Int
                              -> TQueue (Vector (Int, Vector Double), Bool)
                              -> TQueue (Vector Centroid, Bool)
                              -> IO ()
-- There are no more threads in need of new centroids
computeCentroidsFromProposals True nThreads _ recieveUpdatedCentroidQueue =
    writeNToQueue recieveUpdatedCentroidQueue (emptyVector, True) nThreads
computeCentroidsFromProposals _ nThreads sendCentroidProposalQueue recieveUpdatedCentroidQueue = do
    -- A set of centroids proposals are collected from each thread and the average is used
    (centroids, allFinished) <- recieveCentroids nThreads
        sendCentroidProposalQueue emptyVector True
    writeNToQueue recieveUpdatedCentroidQueue (centroids, allFinished) nThreads
    computeCentroidsFromProposals allFinished nThreads
        sendCentroidProposalQueue recieveUpdatedCentroidQueue

recieveCentroids :: Int
                 -> TQueue (Vector (Int, Vector Double), Bool)
                 -> Vector (Int, Vector Double)
                 -> Bool
                 -> IO (Vector Centroid, Bool)
recieveCentroids 0 _ accCentroidProposals allFinished =
    return (buildCentroidFromProposals accCentroidProposals, allFinished)
recieveCentroids threadsRunning sendCentroidProposalQueue accCentroidProposals allFinished = do
    (centroidProposal, isPointsChanged) <- atomically $ readTQueue sendCentroidProposalQueue
    let cp = if V.null accCentroidProposals
                then centroidProposal
                else V.zipWith addCentroidProposal centroidProposal accCentroidProposals
    recieveCentroids (threadsRunning-1) sendCentroidProposalQueue cp
        (not isPointsChanged && allFinished)

buildCentroidFromProposals :: Vector (Int, Vector Double) -> Vector Centroid
buildCentroidFromProposals centroidProposals = V.fromList $
    mapListWithIndex buildCentroid (V.toList centroidProposals)
    where buildCentroid (count, features) = Centroid (divideByCount features
                                                (fromIntegral count))
          divideByCount fv count = V.map (/ count) fv

pointsChanged :: Vector Point -> Vector Point -> Bool -> Bool
pointsChanged points updatedPoints isChanged
    | isChanged = True
    | V.null points = False
    | otherwise = pointsChanged (V.tail points) (V.tail updatedPoints) isPointChanged
        where isPointChanged = pCluster (V.head points) /= pCluster (V.head updatedPoints)

recieceClusteredPoints :: Int
                       -> TQueue (Vector Point)
                       -> [Vector Point]
                       -> IO (Vector Point)
recieceClusteredPoints 0 _ accPoints = return $ V.concat accPoints
recieceClusteredPoints nThreads sendClusteredPointsQueue accPoints = do
                     clusteredPoints <- atomically $ readTQueue sendClusteredPointsQueue
                     recieceClusteredPoints (nThreads-1) sendClusteredPointsQueue
                        (clusteredPoints : accPoints)


----------------------------------------------------------------
-- Phase 1: Reassign all points to the best possible centroid --
----------------------------------------------------------------
updateCluster :: Point -> Vector Centroid -> Point
updateCluster point centroids = changeCluster point bestCluster
    --HACK: (0/0) produces NaN as it alywas fails comparisons with <, > or =
    where bestCluster = computeBestCluster (pFeatures point) centroids 0 (0/0)

changeCluster :: Point -> Int -> Point
changeCluster point newCluster = Point (pFeatures point) newCluster (pLabel point)

computeBestCluster :: Vector Double -> Vector Centroid -> Int -> Double -> Int
computeBestCluster features centroids bestCluster bestDist
    | V.null centroids = bestCluster
    | otherwise = if newDist > bestDist
                  then computeBestCluster features (V.tail centroids)
                    bestCluster bestDist
                  else computeBestCluster features (V.tail centroids)
                    (cCluster $ V.head centroids) newDist
    where newDist = computeEucledianDistance features (cFeatures $ V.head centroids)


----------------------------------------------------------------------
-- Phase 2: Recompute the position of the centroid for each cluster --
----------------------------------------------------------------------
updateCentroids :: Vector Point -> Int -> Vector (Int, Vector Double)
updateCentroids points numberOfClusters = V.fromList $ sumCentroids points accumulator
          where accumulator = S.fromList $ replicate numberOfClusters
                    (0, V.map (const 0.0) (pFeatures $ V.head points))

sumCentroids :: Vector Point -> Seq (Int, Vector Double) -> [(Int, Vector Double)]
sumCentroids points acc
    | V.null points = P.reverse $ foldlWithIndex (\cAcc _ cp -> cp : cAcc) [] acc
    | otherwise = sumCentroids (V.tail points) (accPointToCentroid (V.head points) acc)

accPointToCentroid :: Point -> Seq (Int, Vector Double) -> Seq (Int, Vector Double)
accPointToCentroid point acc = update (pCluster point) (fst oldTuple + 1, newValue) acc
    where oldTuple = index acc (pCluster point)
          newValue = V.zipWith (+) (pFeatures point) (snd $ index acc $ pCluster point)


------------------------
-- Distance Functions --
------------------------
computeEucledianDistance :: Vector Double -> Vector Double -> Double
computeEucledianDistance v1 v2 = V.sum $ V.zipWith diffsq v1 v2
    where diffsq a b = (a-b)^(2::Int)


-----------------
-- IO Function --
-----------------
readDataset :: String -> String -> Bool -> IO [Point]
readDataset filePath delimiter containsLabels = do
                            csvFile <- liftM (removeHeader . lines) (readFile filePath)
                            let csvData = P.map (P.map read . splitOn delimiter) csvFile
                            let dataset  =  if containsLabels
                                            then P.foldl pointWithLabel [] csvData
                                            else P.foldl pointWithoutLabel [] csvData
                            return $ P.reverse dataset

removeHeader :: [String] -> [String]
removeHeader = dropWhile $ any isAlpha

pointWithoutLabel :: [Point] -> [Double]-> [Point]
pointWithoutLabel points sample = Point (V.fromList sample) (-1) (-1) : points

pointWithLabel :: [Point] -> [Double]-> [Point]
pointWithLabel points sample = Point (V.fromList $ P.init sample) (-1)
    (safeTruncate $ last sample) : points

-----------------------
-- STM Based Barrier --
-----------------------
data Barrier = Barrier (TVar Int) (TVar Int) (TVar (Seq Bool))

newBarrier :: Int -> IO Barrier
newBarrier nThreads = atomically (do
                                 bCount <- newTVar 0
                                 bNThreads <- newTVar nThreads
                                 bFlags <- newTVar $ S.fromList $ replicate nThreads True
                                 return $ Barrier bCount bNThreads bFlags)

awaitOnBarrier :: Barrier -> Int -> IO ()
awaitOnBarrier (Barrier bCount bNThreads bBFlags) threadId = do
    -- Decrements the barriers thread counter atomically
    atomically (do
               count <- readTVar bCount
               nThreads <- readTVar bNThreads
               let updatedCount = count + 1
               if updatedCount /= nThreads
                  then writeTVar bCount updatedCount
                  else do
                      {-- The expected number of threads have arrived at the
                          barrier so the flags blocking each thread are set to
                          false and the barriers thread count is reset --}
                      flagSeq <- readTVar bBFlags
                      let newFlags = mapWithIndex (\_ _ -> False) flagSeq
                      writeTVar bBFlags newFlags
                      writeTVar bCount 0)
    -- Until the barriers counter reaches zero are
    atomically (do
               flagSeq <- readTVar bBFlags
               if index flagSeq threadId
                  then retry
                  else writeTVar bBFlags (update threadId True flagSeq))

----------------------
-- Helper Functions --
----------------------
parseArgs :: [String] -> IO (Int, Int, String)
parseArgs [x,y,z] = return (read x, read y, z)
parseArgs _ = putStrLn "usage: KMeansHaskell number-of-threads\
    \ number-of-centroids dataset-filepath" >> exitFailure

emptyVector :: Vector a
emptyVector = V.fromList []

mapListWithIndex :: (a -> Int -> b) -> [a] -> [b]
mapListWithIndex func list = P.zipWith func list [0 ..]

writeToQueue :: TQueue a -> a -> IO ()
writeToQueue queue element = writeNToQueue queue element 1

writeNToQueue :: TQueue a
            -> a
            -> Int
            -> IO ()
writeNToQueue queue element copies =
    replicateM_ copies (atomically $ writeTQueue queue element)


computeCorrectClassefiedPercentage :: Vector Point -> Double
computeCorrectClassefiedPercentage points =
    if pLabel (V.head points) == -1
       then 0
       else correctClassifications / fromIntegral (V.length points) * 100.0
    where correctClassifications = V.foldl isCorrect 0 points
          isCorrect acc point = if pCluster point == pLabel point
                                   then acc + 1
                                   else acc

safeTruncate :: Double -> Int
safeTruncate integerValue = if integerValue == fromIntegral (round integerValue)
                            then round integerValue
                            else error "ERROR: labels must be encoded is integer values"

addCentroidProposal :: (Int, Vector Double)
                    -> (Int, Vector Double)
                    -> (Int, Vector Double)
addCentroidProposal (cp1Count, cp1Featurs) (cp2Count, cp2Features) =
    (cp1Count + cp2Count, V.zipWith (+) cp1Featurs cp2Features)

printNElements :: Show a => Vector a -> Int -> IO ()
printNElements xs n
    | V.null xs = return ()
    | n == 0 = return ()
    | n == (-1) = print (V.head xs) >> printNElements (V.tail xs) (-1)
    | otherwise = print (V.head xs) >> printNElements (V.tail xs) (n - 1)


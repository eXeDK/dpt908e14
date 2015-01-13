namespace dpt908e14Kmeans

module Kmeans =
    type Centroid = 
        { Features: List<double> ;
          Name: int
        }

    type CentroidSuggestion = 
        { Centroid: int; 
        Features: List<double>;
        Count: int
        }

    type DataPoint = 
        { Features: List<double>;
        Centroid: int;
        Label: int 
        }

    type msg = 
        | GetNewCentroids of AsyncReplyChannel<array<CentroidSuggestion>>
        | GetDataPoints of AsyncReplyChannel<array<DataPoint>>
        | Cluster of AsyncReplyChannel<int>
        | UpdateCentroids of List<Centroid>
        | Start

    let rec dataPartition(data: array<DataPoint>, output: array<array<DataPoint>>, defaultSize: int, remainingRest: int) =
        if (not (Array.isEmpty data)) then
            let thisPartitionSize = if (remainingRest > 0) then defaultSize + 1 else defaultSize
            let nextRemainingRest = if (remainingRest > 0) then remainingRest - 1 else remainingRest
            let thisPartition = Array.sub data 0 thisPartitionSize
            let remainingData = Array.sub data thisPartitionSize (data.Length - thisPartitionSize)

            dataPartition(remainingData, Array.append [| thisPartition |] output, defaultSize, nextRemainingRest)
        else
            output

    let createFeatures countFeatures = 
        [ for _ in 1 .. countFeatures do 
            yield (double) 0.0
        ]

    let calculateStatistics (segments: array<MailboxProcessor<msg>>) =
        let finalData =
            (segments |> Array.map(fun x -> 
                x.PostAndAsyncReply(fun replyChannel -> GetDataPoints replyChannel)))
            |> Async.Parallel
            |> Async.RunSynchronously
            |> Array.concat

        let percentage = ((double)(finalData |> Array.map(fun x -> if (x.Centroid.Equals(x.Label)) then 1 else 0) |> Array.sum) / (double)finalData.Length) * (double)100.0
        printfn "Correct percentage: %f%%" percentage

    let rec sumFeatures(suggestions: List<CentroidSuggestion>, sum: List<double>, count: int) =
        if (suggestions.IsEmpty) then
            // calculate mean and return
            sum |> List.map(fun x -> if (count = 0) then 0.0 else x / (double)count)
        else                          
            // Recursive step      
            sumFeatures(suggestions.Tail, 
                List.map2 (fun f1 f2 ->
                    f1 + f2
                ) suggestions.Head.Features sum, count + suggestions.Head.Count)

    let rec kmeans (maxIterations: int, actors: array<MailboxProcessor<msg>>) =
        if (maxIterations = 0) then
            // printfn "No more iterations"
            printfn ""
        else
            let sumChanged = 
                (actors |> Array.map(fun actor -> 
                    actor.PostAndAsyncReply(fun replyChannel -> 
                        Cluster replyChannel)))
                |> Async.Parallel
                |> Async.RunSynchronously
                |> Array.sum

            printfn "Changes: %s" ((string)sumChanged)

            // Get new centroid positions from segments
            let centroidsSuggestions =
                (actors |> Array.map(fun x -> 
                    x.PostAndAsyncReply(fun replyChannel -> GetNewCentroids replyChannel)))
                |> Async.Parallel
                |> Async.RunSynchronously

            // Calculate mean from segments
            let newCentroids = 
                centroidsSuggestions 
                    |> Array.concat
                    |> Seq.groupBy(fun x -> x.Centroid)
                    |> Seq.map(fun (index, x) ->
                        let newFeatures = x |> Seq.toList
                        { Centroid.Name = index; 
                        Features = sumFeatures(newFeatures, (createFeatures newFeatures.Head.Features.Length), 0) }
                    )
                    |> Seq.toList
            // printfn "Calculated new centroids"

            if (sumChanged = 0) then
                // printfn "No more changes"
                // Calculate statistics
                calculateStatistics(actors)
            else
                // Deliver new centroids to actors
                for segment in actors do
                    segment.Post(UpdateCentroids newCentroids)

                // Start next iteration
                kmeans(maxIterations - 1, actors)

    [<EntryPoint>]
    let main argv =
        let numbInterations = 300
        let numbSegments = (int)argv.[0]
        let numbCentroids = (int)argv.[1]
        let dataFilePath = argv.[2]
        printfn "Number of actors: %d" numbSegments
        printfn "Number of centroids: %d" numbCentroids
        printfn "Maximum Iterations: %d" numbInterations
        printfn "Dataset: %s" dataFilePath 

        let dataFile = System.IO.File.ReadAllLines(dataFilePath)
                         |> Seq.map(fun line -> line.Split(','))

        let dataPoints = 
            [| for row in dataFile do
                let listData = Array.toList(Array.rev(row))
                let features = List.rev(listData.Tail)
                yield { DataPoint.Features = List.map(fun x -> (double) x) features; Centroid = 0; Label = (int)listData.Head }
            |]

        let centroids = 
            [| for i in 0 .. (numbCentroids - 1) do
                yield { Centroid.Features = dataPoints.[i].Features; Name = i };
            |]

        let dataSegmentSize = (int)(floor((float)dataPoints.Length / (float)numbSegments))
        let dataPartitionRest = dataPoints.Length - (numbSegments * dataSegmentSize)
        let dataSegments = dataPartition(dataPoints, [| |], dataSegmentSize, dataPartitionRest)
        
        let segments = 
            [| for dataSegment in dataSegments do
                yield MailboxProcessor.Start(fun inbox ->
                    let hash = dataSegment.GetHashCode()

                    let updateCentroid (dataPoint : DataPoint, newCentroid: Centroid) =
                        { DataPoint.Features = dataPoint.Features; Centroid = newCentroid.Name; Label = dataPoint.Label }

                    let updateCentroidSuggestion (centroid: int, newFeatures: List<double>, newCount: int) =
                        { CentroidSuggestion.Centroid = centroid; Features = newFeatures; Count = newCount}

                    let rec loop(dataPoints: array<DataPoint>, currentCentroids: seq<Centroid>, msg) = 
                        async { 
                                let! msg = inbox.Receive()
                                match msg with
                                | UpdateCentroids(newCentroids) ->
                                    // printfn "Updated centroids %d" hash
                                    return! loop(dataPoints, newCentroids, msg)

                                | GetDataPoints(replyChannel) ->
                                    // printfn "Get datapoints %d" hash
                                    replyChannel.Reply(dataPoints)
                                    return! loop(dataPoints, currentCentroids, msg)

                                | GetNewCentroids(replyChannel) ->
                                    // printfn "Getting new centroids %d" hash
                                    let newCentroids = 
                                        [| for centroid in currentCentroids do
                                            yield { CentroidSuggestion.Centroid = centroid.Name; Features = (createFeatures centroid.Features.Length); Count = 0}
                                        |]

                                    let centroidsMap(dataSet: array<CentroidSuggestion>, item: DataPoint) =
                                        dataSet |> Array.map(fun x ->
                                            if (not (x.Centroid = item.Centroid)) then
                                                x
                                            else
                                                updateCentroidSuggestion(x.Centroid, (List.map2( fun f1 f2 -> f1 + f2 ) x.Features item.Features), x.Count + 1)
                                        )

                                    let rec generateNewCentroids(newCentroids: array<CentroidSuggestion>, data: List<DataPoint>) =
                                        if (data.Tail.Length > 0) then
                                            generateNewCentroids(centroidsMap(newCentroids, data.Head), data.Tail)
                                        else
                                            centroidsMap(newCentroids, data.Head)

                                    // Reply and return
                                    replyChannel.Reply(generateNewCentroids(newCentroids, Array.toList dataPoints))
                                    return! loop(dataPoints, currentCentroids, msg)
                                | Cluster(replyChannel) ->
                                    // printfn "Doing clustering %d" hash
                                    let oldCentroids = dataPoints 
                                                    |> Array.map(fun dataPoint -> dataPoint.Centroid)
                                    
                                    // Calculate dataPoint segmentation                                   
                                    let newData = dataPoints |> Array.map(fun dataPoint -> 
                                        updateCentroid(dataPoint, 
                                            currentCentroids |> Seq.minBy(fun centroid ->
                                                Seq.sum (Seq.map2(fun f1 f2 ->
                                                    (f1 - f2)**2.0
                                                ) dataPoint.Features centroid.Features)
                                            )
                                        )
                                    )

                                    let newCentroids = newData |> Array.map(fun x -> x.Centroid)
                                    let changedCount = Array.sum (Array.map2(fun c1 c2 -> if (c1.Equals(c2)) then 0 else 1) oldCentroids newCentroids)

                                    // Reply and return
                                    replyChannel.Reply(changedCount)
                                    return! loop(newData, currentCentroids, msg) 
                                | Start ->
                                    // printfn "Start segment %d" hash
                                    return! loop(dataPoints, currentCentroids, msg) 
                                }
                    loop(dataSegment, centroids, Start)
                )
            |]

        let stopWatch = System.Diagnostics.Stopwatch.StartNew()
        kmeans(numbInterations, segments)
        stopWatch.Stop()
        printfn "Execution Time: %f" stopWatch.Elapsed.TotalSeconds
        System.Console.ReadKey() |> ignore

        0 // return an integer exit code

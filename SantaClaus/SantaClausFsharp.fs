open System

type WorkerType =
    | Elf
    | Reindeer

//Helper Functions
let SleepRandomInterval maxTime =
    let sleepTime = System.Random().Next(maxTime)
    Threading.Thread.Sleep(sleepTime * 1000)

//Santa
let Santa = MailboxProcessor.Start(fun inbox ->
            async { while true do
                    let! (workerType, ids, replyChannels) = inbox.Receive()
                    match workerType with
                    | Reindeer ->
                        printfn
                            "Santa: ho ho delivering presents with reindeers: %A" ids
                    | Elf -> printfn "Santa: ho ho helping elfs: %A" ids
                    SleepRandomInterval(5)
                    for (replyChannel : AsyncReplyChannel<_>) in replyChannels
                        do replyChannel.Reply()
                }
            )

//Santa's House
let SantasHouse = MailboxProcessor.Start(fun inbox ->
        let rec loop (reindeers : List<_ * _>) (elfs : List<_ * _>) =
            async { if reindeers.Length = 9 then
                                            let (ids, replyChannels) =
                                                List.unzip(reindeers)
                                            Santa.Post(Reindeer, ids,
                                                replyChannels)
                                            return! loop [] elfs
                    elif elfs.Length = 3 then
                                            let (ids, replyChannels) =
                                                List.unzip(elfs)
                                            Santa.Post(Elf, ids, replyChannels)
                                            return! loop reindeers []
                    let! msg = inbox.Receive()
                    match msg with
                    | (Reindeer, id, replyChannel) ->
                            return! loop ((id, replyChannel) :: reindeers) elfs
                    | (Elf, id, replyChannel) ->
                            return! loop reindeers ((id, replyChannel) :: elfs)}
        loop [] [])

//Worker
let Worker workerType id maxSleepTime = MailboxProcessor.Start(fun inbox ->
        async {
            while true do
                SleepRandomInterval(maxSleepTime)
                SantasHouse.PostAndReply(fun replyChannel ->
                    (workerType, id, replyChannel))
            }
        )

[<EntryPoint>]
let main argv =
    let elfs = [for id in 1 .. 30 -> Worker Elf id 15]
    let reindeers = [for id in 1 .. 9 -> Worker Reindeer id 30]
    Console.ReadLine() |> ignore
    0


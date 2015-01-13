-module(santa_clause_erlang).
-export([main/0, worker/4, santa/0, santas_house/3]).

% Helper Functions
sleep_random_interval(MaxTime) ->
    SleepTime = random:uniform(MaxTime),
    timer:sleep(SleepTime * 1000).


% Worker
worker(Type, ID, SleepTime, SantasHouse) ->
    sleep_random_interval(SleepTime),
    SantasHouse ! {Type, ID, self()},
    receive
        leave ->
            worker(Type, ID, SleepTime, SantasHouse)
    end.

% Santas House
% The interpretation of the house as an actor is to allow aggregation of the
% Reindeers and Elfs, without resorting to waking Santa or implementing a P2P
% consensus algorithm for archiving agreement between each individual actor
santas_house(Santa, Elfs, Reindeers) ->
    if
        length(Reindeers) == 9 ->
            {IDS, PIDS} = lists:unzip(Reindeers),
            Santa ! {reindeers, IDS, PIDS},
            santas_house(Santa, Elfs, []);
        length(Elfs) == 3 ->
            {IDS, PIDS} = lists:unzip(Elfs),
            Santa ! {elfs, IDS, PIDS},
            santas_house(Santa, [], Reindeers);
        true ->
            receive
                {elf, ID, PID} ->
                    santas_house(Santa, [{ID, PID} | Elfs], Reindeers);
                {reindeer, ID, PID} ->
                    santas_house(Santa, Elfs, [{ID, PID} | Reindeers])
            end
    end.

% Santa
santa() ->
    % Additional preference for Reindeers could be obtained by extracting all
    % messages and checking for any Reindeers before processing them, if Santa
    % spends more time in each meeting then the Elfs and Reindeers sleep
    receive
        {reindeers, IDS, PIDS} ->
            io:format("Santa: ho ho delivering presents with reindeers: ~w\n",
                      [IDS]),
            sleep_random_interval(5),
            lists:foreach(fun(PID) -> PID ! leave end, PIDS);
        {elfs, IDS, PIDS} ->
            io:format("Santa: ho ho helping elfs: ~w\n", [IDS]),
            sleep_random_interval(5),
            lists:foreach(fun(PID) -> PID ! leave end, PIDS)
    end,
    santa().

main() ->
    Santa = spawn(santa_clause_erlang, santa, []),
    SantasHouse = spawn(santa_clause_erlang, santas_house, [Santa, [], []]),

    lists:foreach(
      fun(ID) -> spawn(santa_clause_erlang, worker,
                       [elf, ID, 15, SantasHouse]) end,
      lists:seq(1,30)),

    lists:foreach(
      fun(ID) -> spawn(santa_clause_erlang, worker,
                       [reindeer, ID, 30, SantasHouse]) end,
      lists:seq(1,9)).

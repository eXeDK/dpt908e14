-module(main).
-author("ThomasStig").
-import(string, [tokens/2, substr/3]).

%% API
-export([start/0, kmeans_actor/2, main_actor/7]).

% Records
-record(dataPoint, {features = [], centroid = 0, label = 0}).
-record(centroid, {name = 0, features = []}).
-record(centroidSuggestion, {centroid = 0, features = [], count = 0}).

update_datapoint(DataPoint, NewCentroid) ->
  #dataPoint{features = DataPoint#dataPoint.features, label = DataPoint#dataPoint.label, centroid = NewCentroid}.

update_centroid_suggestion(Centroid, NewFeatures, NewCount) ->
  #centroidSuggestion{centroid = Centroid, features = NewFeatures, count = NewCount}.

%%% Group-by function
%%% Author: Jean-Baptiste Potonnier
%%% URL: https://gist.github.com/jbpotonnier/1310406
groupBy(F, L) ->
  lists:foldr(fun({K,V}, D) ->
    dict:append(K, V, D) end , dict:new(), [ {F(X), X} || X <- L ]).

%%% Floor function
%%% Author: BrentAFulgham
%%% URL: http://schemecookbook.org/Erlang/NumberRounding
floor(X) ->
  T = erlang:trunc(X),
  case (X - T) of
    Neg when Neg < 0 -> T - 1;
    Pos when Pos > 0 -> T;
    _ -> T
  end.

%%% Read file and parse every line
file(Fname) ->
  case file:open(Fname, [read]) of
    {ok, Fd} ->
      parse_line(Fd, []);
    {error, Reason} ->
      {error, Reason}
  end.

%%% Parse a single line to a DataPoint and add it recursivly to Results
parse_line(Fd, Results) ->
  case io:get_line(Fd, "") of
    eof ->
      file:close(Fd),
      Results;
    Line ->
      MyTokens = lists:reverse(tokens(substr(Line, 1, string:len(Line) - 1), ",")),
      DataPoint = #dataPoint{features = lists:map(fun(X) -> list_to_float(X) end, lists:reverse(tl(MyTokens))), label = list_to_integer(hd(MyTokens))},
      parse_line(Fd, [DataPoint | Results])
  end.

%%% Generate a set of empty features for summing up on
empty_features(NumbFeatures) ->
  [0.0 || _ <- lists:seq(0, NumbFeatures - 1)].

%%% Add the datapoint's features to the correct centroid suggestion
%%% Returns a set of centroid suggestions with the Item's features added to the correct centroid suggestion
centroid_suggestion_sum(CentroidSuggestions, Item) ->
  lists:map(fun(CentroidSuggestion) ->
    case CentroidSuggestion#centroidSuggestion.centroid /= Item#dataPoint.centroid of
      true ->
        CentroidSuggestion;
      false ->
        update_centroid_suggestion(
          CentroidSuggestion#centroidSuggestion.centroid,
          lists:zipwith(fun(X, Y) -> X + Y end, CentroidSuggestion#centroidSuggestion.features, Item#dataPoint.features),
          CentroidSuggestion#centroidSuggestion.count + 1
        )
    end
  end
  , CentroidSuggestions).

%%% Generate a new set of centroid suggestions
%%% Returns the centroid suggestions
generate_new_centroids_suggestions(NewCentroids, DataPoints) ->
  case length(tl(DataPoints)) > 0 of
    true ->
      generate_new_centroids_suggestions(centroid_suggestion_sum(NewCentroids, hd(DataPoints)), tl(DataPoints));
    false ->
      centroid_suggestion_sum(NewCentroids, hd(DataPoints))
  end.

%%% Sums the features of all the suggestions recursively in SumFeatures and returns the set of features divided by the final Count
sum_features(Suggestions, SumFeatures, Count) ->
  case length(Suggestions) > 0 of
    true ->
      sum_features(tl(Suggestions), lists:zipwith(fun(X, Y) -> X + Y end, (hd(Suggestions))#centroidSuggestion.features, SumFeatures), Count + (hd(Suggestions))#centroidSuggestion.count);
    false ->
      lists:map(fun(X) ->
        case (Count == 0) of
          true ->
            0.0;
          false ->
            X / Count
        end end, SumFeatures)
  end.

%%% The K-means actor
kmeans_actor(DataPartition, Centroids) ->
  receive
    %%% Tell the actor to cluster its data points
    {MainActor, cluster} ->
      % io:format("kmeans_actor (~s): cluster ~n", [pid_to_list(self())]),
      % io:format("Centroids: ~s ~n", [io_lib:write(Centroids)]),

      OldCentroids = lists:map(fun(DataPoint) -> DataPoint#dataPoint.centroid end, DataPartition),

      NewData = lists:map(fun(DataPoint) ->
        % Returns list of list of distance from DataPoint to Centroid, Centroid.Name
        Distances = lists:map(fun(Centroid) ->
          [lists:sum(lists:zipwith(fun(X,Y) -> math:pow(X - Y, 2) end, DataPoint#dataPoint.features, Centroid#centroid.features)), Centroid#centroid.name] end
          , Centroids),

        % Returns the name of the centroid with the minimum distance to the DataPoint
        MinCentroidName = lists:nth(2, lists:min(Distances)),

        % Return new DataPoint to the map function
        update_datapoint(DataPoint, MinCentroidName)
        end
        , DataPartition),

      NewCentroids = lists:map(fun(DataPoint) -> DataPoint#dataPoint.centroid end, NewData),
      Changed = lists:sum(lists:zipwith(fun(X, Y) ->
        case X == Y of
          true ->
            0;
          false ->
            1
        end
      end,
        OldCentroids, NewCentroids)),

      MainActor ! {cluster_result, Changed},
      kmeans_actor(NewData, Centroids);

    %%% Tell the actor to calculate its new centroid suggestions
    {MainActor, get_new_centroids} ->
      % io:format("kmeans_actor (~s): get_new_centroids ~n", [pid_to_list(self())]),
      CentroidSuggestions = lists:map(fun(X) -> #centroidSuggestion{centroid = X#centroid.name, features = empty_features(length(X#centroid.features)), count = 0} end, Centroids),

      MainActor ! {new_centroids_result, generate_new_centroids_suggestions(CentroidSuggestions, DataPartition)},
      kmeans_actor(DataPartition, Centroids);

    %%% Update the centroids
    {MainActor, update_centroids, NewCentroids} ->
      % io:format("kmeans_actor (~s): update_centroids ~n", [pid_to_list(self())]),
      kmeans_actor(DataPartition, NewCentroids);

    %%% Get the data points of the actor
    {MainActor, get_datapoints} ->
      % io:format("kmeans_actor (~s): get_datapoints ~n", [pid_to_list(self())]),
      MainActor ! {datapoints_result, DataPartition},
      kmeans_actor(DataPartition, Centroids)
  end.

%%% Main actor
main_actor(Actors, Iteration, ChangesList, SuggestionsList, DataPointsList, ClusteringDone, StartingTimestamp) ->
  receive
    %%% Start clustering of points
    cluster ->
      % io:format("main_actor: cluster~n"),
      case Iteration > 0 of
        true ->
          % io:format("~s Start work by main actor(Iteration: ~s) ~n", [pid_to_list(self()), integer_to_list(Iteration)]),

          % Message all actors to cluster their data
          lists:map(fun(Actor) -> Actor ! {self(), cluster} end, Actors),

          main_actor(Actors, Iteration, ChangesList, SuggestionsList, DataPointsList, ClusteringDone, StartingTimestamp);

        false ->
          % No more iterations allowed, stop clustering
          % Ask actors for data points
          lists:map(fun(Actor) -> Actor ! {self(), get_datapoints} end, Actors),
          main_actor(Actors, Iteration, ChangesList, SuggestionsList, DataPointsList, true, StartingTimestamp)
      end;

    %%% Get clustering result
    {cluster_result, Changes} ->
      % io:format("main_actor: cluster_result~n"),
      % Add cluster result
      NewChangesList = [Changes | ChangesList],

      % If all results are received proceed
      case length(NewChangesList) == length(Actors) of
        true ->
          % Ask actors for centroid suggestions
          lists:map(fun(A) -> A ! {self(), get_new_centroids} end, Actors),

          % If there are changes
          case lists:sum(NewChangesList) > 0 of
            true ->
              % io:format("Changes in clustering: ~s ~n", [integer_to_list(lists:sum(NewChangesList))]),

              % Reset list of changes
              main_actor(Actors, Iteration, [], SuggestionsList, DataPointsList, ClusteringDone, StartingTimestamp);
            false ->
              % io:format("0 changes happened - done ~n"),
              % Ask actors for data points
              lists:map(fun(A) -> A ! {self(), get_datapoints} end, Actors),
              main_actor(Actors, Iteration, [], SuggestionsList, DataPointsList, true, StartingTimestamp)
          end;
        false ->
          % All results are not received, wait for more results
          main_actor(Actors, Iteration, NewChangesList, SuggestionsList, DataPointsList, ClusteringDone, StartingTimestamp)
      end;

    %%% Get new centroid suggestions
    {new_centroids_result, CentroidSuggestions} ->
      % io:format("main_actor: new_centroids_result~n"),
      % Add centroids suggestions
      NewCentroidSuggestsList = [CentroidSuggestions | SuggestionsList],

      % Is all suggestions received?
      case length(NewCentroidSuggestsList) == length(Actors) of
        true ->
          % Calculate the final centroids
          FlatSuggestions = lists:flatten(NewCentroidSuggestsList),
          Grouped = dict:to_list(groupBy(fun(X) -> X#centroidSuggestion.centroid end, FlatSuggestions)),
          MappedGroupedList = lists:map(fun({Index, Suggestions}) -> #centroid{name = Index, features = sum_features(Suggestions, empty_features(length((hd(Suggestions))#centroidSuggestion.features)), 0)} end, Grouped),

          % io:format("Centroids: ~s ~n", [io_lib:write(MappedGroupedList)]),

          % Send new centroids to all actors
          lists:map(fun(A) -> A ! {self(), update_centroids, MappedGroupedList} end, Actors),
          self() ! cluster,
          main_actor(Actors, Iteration - 1, ChangesList, [], DataPointsList, ClusteringDone, StartingTimestamp);
        false ->
          % All results are not received, wait for more results
          main_actor(Actors, Iteration, ChangesList, NewCentroidSuggestsList, DataPointsList, ClusteringDone, StartingTimestamp)
      end;

    %%% Get datapoints for correct percentage calculation
    {datapoints_result, DataPoints} ->
      % io:format("main_actor: datapoints_result~n"),
      NewDataPointsList = [DataPoints | DataPointsList],

      case length(NewDataPointsList) == length(Actors) of
        true ->
          FlatDataPoints = lists:flatten(NewDataPointsList),
          Percentage = lists:sum(lists:map(fun(X) ->
            case X#dataPoint.centroid == X#dataPoint.label of
              true ->
                1;
              false ->
                0
            end
          end, FlatDataPoints)),
          io:format("Percentage correct clustered: ~.2f% ~n", [(Percentage / length(FlatDataPoints)) * 100.0]),

          case ClusteringDone of
            true ->
              %io:format("Clustering done"),
              output_timediff(erlang:now(), StartingTimestamp),
              init:stop();
            false ->
              main_actor(Actors, Iteration, ChangesList, SuggestionsList, [], ClusteringDone, StartingTimestamp)
          end;
        false ->
          main_actor(Actors, Iteration, ChangesList, SuggestionsList, NewDataPointsList, ClusteringDone, StartingTimestamp)
      end
  end.

%%% Partition data
data_partition(Data, Output, DefaultSize, RemainingRest) ->
  case (length(Data) > 0) of
    true ->
      ThisPartitionSize = case (RemainingRest > 0) of true -> DefaultSize + 1; false -> DefaultSize end,
      NextRemainingRest = case (RemainingRest > 0) of true -> RemainingRest - 1; false -> RemainingRest end,
      ThisPartition = lists:sublist(Data, ThisPartitionSize),
      RemainingData = lists:sublist(Data, ThisPartitionSize + 1, length(Data) - ThisPartitionSize),

      data_partition(RemainingData, [ThisPartition | Output], DefaultSize, NextRemainingRest);
    false ->
      Output
  end.

output_timediff(T2, T1) ->
  Diff = timer:now_diff(T2, T1),
  io:format("Execution Time: ~.3f~n", [Diff / 1000000.0]).

%%% kmeans starting function
kmeans(MaxIterations, CountDataPartitions, DataPoints, Centroids) ->
  % Partition data
  DataPartitionSize = floor(length(DataPoints) / CountDataPartitions),
  DataPartitionRest = length(DataPoints) - (CountDataPartitions * DataPartitionSize),
  DataPartitions = data_partition(DataPoints, [], DataPartitionSize, DataPartitionRest),

  % Setup actor system
  Actors = lists:map(fun(V) -> spawn(main, kmeans_actor, [V, Centroids]) end, DataPartitions),
  MainActor = spawn(main, main_actor, [Actors, MaxIterations, [], [], [], false, erlang:now()]),

  % Start main actor clustering
  MainActor ! cluster.

%%% Starting function
start() ->
  Arguments = init:get_plain_arguments(),
  NumbSegments = list_to_integer(lists:nth(1, Arguments)),
  NumbCentroids = list_to_integer(lists:nth(2, Arguments)),
  DataFilePath = lists:nth(3, Arguments),
  NumbIterations = 300,

  io:format("Number of Actors: ~s ~n", [integer_to_list(NumbSegments)]),
  io:format("Number of centroids: ~s ~n", [integer_to_list(NumbCentroids)]),
  io:format("Maximum Iterations: ~s ~n", [integer_to_list(NumbIterations)]),
  io:format("Dataset: ~s ~n", [DataFilePath]),

  % Load DataPoints
  DataPoints = lists:reverse(file(DataFilePath)),

  % Create Centroids
  Centroids = [#centroid{name = I, features = (lists:nth(I + 1, DataPoints))#dataPoint.features} || I <- lists:seq(0, NumbCentroids - 1)],

  % Start kmeans
  kmeans(NumbIterations, NumbSegments, DataPoints, Centroids).
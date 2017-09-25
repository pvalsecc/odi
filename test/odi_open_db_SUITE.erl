%%%-------------------------------------------------------------------
%%% @author patrick
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Jul 2017 14:22
%%%-------------------------------------------------------------------
-module(odi_open_db_SUITE).
-include_lib("common_test/include/ct.hrl").

%% API
-export([all/0, init_per_testcase/2, end_per_testcase/2, init_lager/0]).

-export([stats/1, record/1, query/1, command/1, script/1, tx/1]).

all() ->
    [stats, record, query, command, script, tx].

init_lager() ->
    _ = application:stop(lager),
    application:set_env(lager, suppress_application_start_stop, true),
    application:set_env(lager, handlers,
        [
            {lager_common_test_backend, [debug, {lager_default_formatter, [
                severity, " ",
                {module, [" [", module, ":", function, "] "], [" [] "]},
                message
            ]}]}
        ]),
    ok = lager:start(),
    %% we care more about getting all of our messages here than being
    %% careful with the amount of memory that we're using.
    error_logger_lager_h:set_high_water(100000).

init_per_testcase(_TestCase, Config) ->
    init_lager(),
    {ok, Admin} = odi:connect("localhost", "root", "root", []),
    ok = odi:db_create(Admin, "test", "graph", "plocal", null),
    ok = odi:close(Admin),
    {{Clusters, null}, Con} = odi:db_open("localhost", "test", "root", "root", []),
    [{con, Con}, {clusters, Clusters} | Config].

stats(Config) ->
    Con = ?config(con, Config),
    Clusters = odi:db_reload(Con),
    true = is_list(Clusters),
    true = length(Clusters) > 0,
    Size = odi:db_size(Con),
    true = Size > 0,
    NbRecords = odi:db_countrecords(Con),
    true = NbRecords > 0,
    NewClusterNum = odi:datacluster_add(Con, "toto", -1),
    true = odi:datacluster_remove(Con, NewClusterNum).

record(Config) ->
    Con = ?config(con, Config),
    Clusters = ?config(clusters, Config),
    {_, VClusterId} = lists:keyfind(<<"v">>, 1, Clusters),
    {_, EClusterId} = lists:keyfind(<<"e">>, 1, Clusters),

    Data1 = #{"toto" => {integer, 42}, "tutu" => {string, "tutu"}},
    {VClusterId, RecordPos1, 1, []} = odi:record_create(Con, VClusterId, {"V", Data1}, document, sync),

    Data2 = #{"x" => {double, 4.5}},
    {VClusterId, RecordPos2, 1, []} = odi:record_create(Con, VClusterId, {"V", Data2}, document, sync),

    Data3 = #{"in" => {link, {VClusterId, RecordPos1}}, "out" => {link, {VClusterId, RecordPos2}}},
    {EClusterId, RecordPos3, 1, []} = odi:record_create(Con, EClusterId, {"E", Data3}, document, sync),

    Data1b = #{"out_" => {linkbag, [{EClusterId, RecordPos3}]}},
    Data1final = maps:merge(Data1, Data1b),
    {2, []} = odi:record_update(Con, {VClusterId, RecordPos1}, false, {"V", Data1final}, 1, sync),

    Data2b = #{"in_" => {linkbag, [{EClusterId, RecordPos3}]}},
    Data2final = maps:merge(Data2, Data2b),
    {2, []} = odi:record_update(Con, {VClusterId, RecordPos2}, false, {"V", Data2final}, 1, sync),

    Result = odi:record_load(Con, {VClusterId, RecordPos1}, "*:2", false),
    io:format("Read record: ~p~n", [Result]),
    {true, document, 2, "V", Data1final} =
        lists:keyfind(true, 1, Result),
    {{VClusterId, RecordPos2}, document, 2, "V", Data2final} =
        lists:keyfind({VClusterId, RecordPos2}, 1, Result),
    {{EClusterId, RecordPos3}, document, 1, "E", Data3} =
        lists:keyfind({EClusterId, RecordPos3}, 1, Result),
    3 = length(Result),

    true = odi:record_delete(Con, {VClusterId, RecordPos2}, 2, sync).

query(Config) ->
    Con = ?config(con, Config),

    {Results0, []} = odi:query(Con, "select from V", -1, default),
    [] = Results0,

    Data1 = #{"toto" => {integer, 42}, "tutu" => {string, "tutu"}},
    {VClusterId1, RecordPos1, 1, []} = odi:record_create(Con, -1, {"V", Data1}, document, sync),

    {Results1, []} = odi:query(Con, "select from V", -1, default),
    {{VClusterId1, RecordPos1}, document, 1, "V", Data1} = lists:keyfind({VClusterId1, RecordPos1}, 1, Results1),
    1 = length(Results1),

    Data2 = #{"x" => {double, 4.5}},
    {VClusterId2, RecordPos2, 1, []} = odi:record_create(Con, -1, {"V", Data2}, document, sync),

    {Results2, []} = odi:query(Con, "select from V", -1, default),
    {{VClusterId1, RecordPos1}, document, 1, "V", Data1} = lists:keyfind({VClusterId1, RecordPos1}, 1, Results2),
    {{VClusterId2, RecordPos2}, document, 1, "V", Data2} = lists:keyfind({VClusterId2, RecordPos2}, 1, Results2),
    2 = length(Results2),

    {error, Error} = odi:query(Con, "notselect from v", -1 , default),
    io:format("Error: ~p~n", [Error]),

    {[{{VClusterId1, RecordPos1}, document, 1, "V", Data1}], []} = odi:query(Con, "select from V where toto = :toto", -1, default, #{"toto" => {integer, 42}}).

command(Config) ->
    Con = ?config(con, Config),

    Data = #{"x" => {float, 4.5}},
    Result = odi:command(Con, "INSERT INTO V (x) VALUES (:x)", #{"x" => {float, 4.5}}, null),
    [{{ClusterId, RecordPos}, document, 1, "V", Data}] = Result,

    {Result2, []} = odi:query(Con, "select from V", -1, default),
    [{{ClusterId, RecordPos}, document, 1, "V", Data}] = Result2.

script(Config) ->
    Con = ?config(con, Config),

    {[{{-1, -1}, document, 0, [], #{"result" := {integer, 4}}}], []} =
        odi:script(Con, "Javascript", "print('hello world'); 4").

tx(Config) ->
    Con = ?config(con, Config),

    % Create two vertices linked with one edge (for some reason, for the RID to be remapped, the linkbags must have UUIDs.
    Data1 = #{"toto" => {integer, 42}, "tutu" => {string, "tutu"}, "out_" => {linkbag, {randUuid(), [{-1, -4}]}}},
    Data2 = #{"x" => {double, 4.5}, "in_" => {linkbag, {randUuid(), [{-1, -4}]}}},
    DataE = #{"in" => {link, {-1, -2}}, "out" => {link, {-1, -3}}},
    ResultT1 = odi:tx_commit(Con, 1, true, [
        {create, {-1, -2}, document, {"V", Data1}},
        {create, {-1, -3}, document, {"V", Data2}},
        {create, {-1, -4}, document, {"E", DataE}}
    ]),
    {Ids, _Update, _Changes} = ResultT1,
    {{-1, -2}, {VClusterId1, RecordPos1}} = lists:keyfind({-1, -2}, 1, Ids),
    {{-1, -3}, {VClusterId2, RecordPos2}} = lists:keyfind({-1, -3}, 1, Ids),
    {{-1, -4}, {EClusterId, ERecordPos}} = lists:keyfind({-1, -4}, 1, Ids),

    Data1fixed = Data1#{"out_" => {linkbag, [{EClusterId, ERecordPos}]}},
    Data2fixed = Data2#{"in_" => {linkbag, [{EClusterId, ERecordPos}]}},
    DataEfixed = #{"in" => {link, {VClusterId1, RecordPos1}}, "out" => {link, {VClusterId2, RecordPos2}}},

    {ResultsReadBack, ResultsReadBackCache} = odi:query(Con, "select from V", -1, default),
    {{VClusterId1, RecordPos1}, document, 1, "V", Data1fixed} = lists:keyfind({VClusterId1, RecordPos1}, 1, ResultsReadBack),
    {{VClusterId2, RecordPos2}, document, 1, "V", Data2fixed} = lists:keyfind({VClusterId2, RecordPos2}, 1, ResultsReadBack),
    2 = length(ResultsReadBack),
    [{{EClusterId, ERecordPos}, document, 1, "E", DataEfixed}] = ResultsReadBackCache,

    Data1b = Data1fixed#{"toto" => {integer, 43}},
    ResultT2 = odi:tx_commit(Con, 2, true, [
        {update, {VClusterId1, RecordPos1}, document, 1, true, {"V", Data1b}},
        {delete, {VClusterId2, RecordPos2}, document, 1}
    ]),
    {[], [{{VClusterId1, RecordPos1}, 2}], []} = ResultT2,

    {ResultsReadBack2, ResultsReadBackCache2} = odi:query(Con, "select from V", -1, default),
    [{{VClusterId1, RecordPos1}, document, 2, "V", Data1b}] = ResultsReadBack2,
    [{{EClusterId, ERecordPos}, document, 1, "E", DataEfixed}] = ResultsReadBackCache2.  %% TODO: is it normal E keeps the out link to the deleted V?

end_per_testcase(_TestCase, Config) ->
    Con = ?config(con, Config),
    case process_info(Con) of
        undefined ->
            ok;
        _ ->
            {stop, closed} = odi:db_close(Con)
    end,
    {ok, Admin} = odi:connect("localhost", "root", "root", []),
    ok = odi:db_delete(Admin, "test", "plocal"),
    ok = odi:close(Admin).


%% private ----------------------

randUuid() ->
    rand:uniform((1 bsl 128)) - 1.

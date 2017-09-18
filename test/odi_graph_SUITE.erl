-module(odi_graph_SUITE).
-include_lib("common_test/include/ct.hrl").

%% API
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([simple/1, two_steps/1, two_steps_simple/1, reading/1]).

all() ->
    [simple, two_steps, two_steps_simple, reading].

init_per_testcase(TestCase, Config) ->
    NewConfig = odi_open_db_SUITE:init_per_testcase(TestCase, Config),
    Con = ?config(con, NewConfig),
    [_Result1] = odi:command(Con, "CREATE CLASS Test EXTENDS V"),
    [_Result2] = odi:command(Con, "CREATE PROPERTY Test.field1 STRING"),
    [_Result3] = odi:command(Con, "CREATE PROPERTY Test.field2 LONG"),
    [_Result4] = odi:command(Con, "CREATE CLASS TestSub EXTENDS Test"),
    [_Result5] = odi:command(Con, "CREATE PROPERTY TestSub.field3 BOOLEAN"),
    [_Result6] = odi:command(Con, "CREATE CLASS TestEdge EXTENDS E"),
    [_Result7] = odi:command(Con, "CREATE PROPERTY TestEdge.in LINK Test"),
    [_Result8] = odi:command(Con, "CREATE PROPERTY TestEdge.out LINK Test"),

    NewConfig.

simple(Config) ->
    Con = ?config(con, Config),
    {ok, Transaction} = odi_graph:begin_transaction(Con),
    ok = odi_graph:create_vertex(Transaction, -2, {"Test", #{field1 => "hello", field2 => 12}}),
    ok = odi_graph:create_vertex(Transaction, -3, {"TestSub", #{field1 => "world", field2 => 44, field3 => true}}),
    ok = odi_graph:create_edge(Transaction, -4, -2, -3, {"TestEdge", #{}}),
    ok = odi_graph:create_edge(Transaction, -5, -2, -3, {"TestEdge", #{}}),
    ok = odi_graph:update(Transaction, -2, #{field2 => 42}),
    IdRemaps = odi_graph:commit(Transaction, 1),

    check_results(IdRemaps, Con, 1).

two_steps(Config) ->
    Con = ?config(con, Config),
    {ok, Transaction1} = odi_graph:begin_transaction(Con),
    ok = odi_graph:create_vertex(Transaction1, -2, {"Test", #{field1 => "hello", field2 => 12}}),
    ok = odi_graph:create_vertex(Transaction1, -3, {"TestSub", #{field1 => "world", field2 => 44, field3 => true}}),
    ok = odi_graph:create_vertex(Transaction1, -6, {"Test", #{field1 => "to be deleted", field2 => 46, other => true}}),
    IdRemaps1 = odi_graph:commit(Transaction1, 1),

    {ok, Transaction2} = odi_graph:begin_transaction(Con),
    ok = odi_graph:update(Transaction2, maps:get(-2, IdRemaps1), #{field2 => 42}),
    ok = odi_graph:create_edge(Transaction2, -4, maps:get(-2, IdRemaps1), maps:get(-3, IdRemaps1),
        {"TestEdge", #{}}),
    ok = odi_graph:create_edge(Transaction2, -5, maps:get(-2, IdRemaps1), maps:get(-3, IdRemaps1),
        {"TestEdge", #{}}),
    ok = odi_graph:delete(Transaction2, maps:get(-6, IdRemaps1), 1),
    IdRemaps2 = odi_graph:commit(Transaction2, 2),

    check_results(maps:merge(IdRemaps1, IdRemaps2), Con, 2).

two_steps_simple(Config) ->
    Con = ?config(con, Config),
    {ok, Transaction1} = odi_graph:begin_transaction(Con),
    ok = odi_graph:create_vertex(Transaction1, -2, {"Test", #{field1 => "hello", field2 => 12}}),
    ok = odi_graph:create_vertex(Transaction1, -3, {"TestSub", #{field1 => "world", field2 => 44, field3 => true}}),
    IdRemaps1 = odi_graph:commit(Transaction1, 1),
    ct:log("RIDs=~p", [IdRemaps1]),

    {ok, Transaction2} = odi_graph:begin_transaction(Con),
    ok = odi_graph:update(Transaction2, maps:get(-2, IdRemaps1), #{field2 => 42}),
    ok = odi_graph:create_edge(Transaction2, -4, maps:get(-2, IdRemaps1), maps:get(-3, IdRemaps1),
        {"TestEdge", #{}}),
    ok = odi_graph:create_edge(Transaction2, -5, maps:get(-2, IdRemaps1), maps:get(-3, IdRemaps1),
        {"TestEdge", #{}}),
    IdRemaps2 = odi_graph:commit(Transaction2, 2),
    ct:log("RIDs2=~p", [IdRemaps2]),

    check_results(maps:merge(IdRemaps1, IdRemaps2), Con, 2).

reading(Config) ->
    simple(Config),
    Con = ?config(con, Config),
    {ok, Transaction} = odi_graph:begin_transaction(Con),
    try
        [{HelloRid, document, 1, "Test", HelloData}] =
            odi_graph:query(Transaction, "SELECT FROM Test WHERE field1='hello'", -1, default),

        {HelloRid, document, 1, "Test", HelloData} = odi_graph:record_load(Transaction, HelloRid, default),
        #{"field1" := "hello", "out_TestEdge" := [Edge1Rid, _Edge2Rid]} = HelloData,

        {Edge1Rid, document, 1, "TestEdge", Edge1Data} = odi_graph:record_load(Transaction, Edge1Rid, default),
        #{"out" := HelloRid, "in" := WorldRid} = Edge1Data,

        {WorldRid, document, 1, "TestSub", WorldData} = odi_graph:record_load(Transaction, WorldRid, default),
        #{"field1" := "world"} = WorldData,

        ParamResult = odi_graph:query(Transaction, "SELECT FROM Test WHERE field1 = :field1", -1, default,
                                      #{"field1" => "hello"}),
        [{HelloRid, document, 1, "Test", HelloData}] = ParamResult
    after
        odi_graph:rollback(Transaction)
    end.


check_results(IdRemaps, Con, VertexVersion) ->
    #{
        -2 := {TestClusterId, TestClusterPosition},
        -3 := {TestSubClusterId, TestSubClusterPosition},
        -4 := {TestEdgeClusterId1, TestEdgeClusterPosition1},
        -5 := {TestEdgeClusterId2, TestEdgeClusterPosition2}
    } = IdRemaps,
    {ResultsReadBack, ResultsReadBackCache} = odi:query(Con, "SELECT FROM Test ORDER BY field1", -1, default),
    [
        {{TestClusterId, TestClusterPosition}, document, VertexVersion, "Test", DataTest},
        {{TestSubClusterId, TestSubClusterPosition}, document, VertexVersion, "TestSub", DataTestSub}
    ] = ResultsReadBack,
    [
        {{TestEdgeClusterId2, TestEdgeClusterPosition2}, document, 1, "TestEdge", DataEdge2},
        {{TestEdgeClusterId1, TestEdgeClusterPosition1}, document, 1, "TestEdge", DataEdge1}
    ] = ResultsReadBackCache,
    #{
        "field1" := {string, "hello"},
        "field2" := {long, 42},
        "out_TestEdge" := {linkbag, [
            {TestEdgeClusterId1, TestEdgeClusterPosition1},
            {TestEdgeClusterId2, TestEdgeClusterPosition2}
        ]}
    } = DataTest,
    #{
        "in" := {link, {TestSubClusterId, TestSubClusterPosition}},
        "out" := {link, {TestClusterId, TestClusterPosition}}
    } = DataEdge1,
    #{
        "in" := {link, {TestSubClusterId, TestSubClusterPosition}},
        "out" := {link, {TestClusterId, TestClusterPosition}}
    } = DataEdge2,
    #{
        "field1" := {string, "world"},
        "field2" := {long, 44},
        "field3" := {bool, true},
        "in_TestEdge" := {linkbag, [
            {TestEdgeClusterId1, TestEdgeClusterPosition1},
            {TestEdgeClusterId2, TestEdgeClusterPosition2}
        ]}
    } = DataTestSub.

end_per_testcase(TestCase, Config) ->
    odi_open_db_SUITE:end_per_testcase(TestCase, Config).

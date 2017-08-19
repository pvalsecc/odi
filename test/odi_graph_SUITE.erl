%%%-------------------------------------------------------------------
%%% @author patrick
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 31. Jul 2017 14:22
%%%-------------------------------------------------------------------
-module(odi_graph_SUITE).
-include_lib("common_test/include/ct.hrl").

%% API
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([simple/1]).

all() ->
    [simple].

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
    odi_graph:create_vertex(Transaction, -2, {"Test", #{field1 => "hello", field2 => 42}}),
    odi_graph:create_vertex(Transaction, -3, {"TestSub", #{field1 => "world", field2 => 44, field3 => true}}),
    odi_graph:create_edge(Transaction, -4, -2, -3, {"TestEdge", #{}}),
    IdRemaps = odi_graph:commit(Transaction, 1),
    #{
        -2 := {TestClusterId, TestClusterPosition},
        -3 := {TestSubClusterId, TestSubClusterPosition},
        -4 := {TestEdgeClusterId, TestEdgeClusterPosition}
    } = IdRemaps,

    {ResultsReadBack, ResultsReadBackCache} = odi:query(Con, "SELECT FROM Test ORDER BY field1", -1, default),
    [
        {{TestClusterId, TestClusterPosition}, document, 1, "Test", DataTest},
        {{TestSubClusterId, TestSubClusterPosition}, document, 1, "TestSub", DataTestSub}
    ] = ResultsReadBack,
    [{{TestEdgeClusterId, TestEdgeClusterPosition}, document, 1, "TestEdge", DataEdge}] = ResultsReadBackCache,
    #{
        "field1" := {string, "hello"},
        "field2" := {long, 42},
        "out_TestEdge" := {linkbag, [{TestEdgeClusterId, TestEdgeClusterPosition}]}
    } = DataTest,
    #{
        "in" := {link, {TestSubClusterId, TestSubClusterPosition}},
        "out" := {link, {TestClusterId, TestClusterPosition}}
    } = DataEdge,
    #{
        "field1" := {string, "world"},
        "field2" := {long, 44},
        "field3" := {bool, true},
        "in_TestEdge" := {linkbag, [{TestEdgeClusterId, TestEdgeClusterPosition}]}
    } = DataTestSub.

end_per_testcase(TestCase, Config) ->
    odi_open_db_SUITE:end_per_testcase(TestCase, Config).

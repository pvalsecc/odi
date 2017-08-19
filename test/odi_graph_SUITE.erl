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
    [_Result2] = odi:command(Con, "CREATE PROPERTY Test.field1 string"),
    [_Result3] = odi:command(Con, "CREATE PROPERTY Test.field2 long"),
    [_Result4] = odi:command(Con, "CREATE CLASS TestSub EXTENDS Test"),
    [_Result5] = odi:command(Con, "CREATE PROPERTY TestSub.field3 boolean"),
    NewConfig.

simple(Config) ->
    Con = ?config(con, Config),
    {ok, Transaction} = odi_graph:begin_transaction(Con),
    odi_graph:create_vectice(Transaction, -1, {"Test", #{field1 => "hello", field2 => 42}}),
    odi_graph:create_vectice(Transaction, -1, {"TestSub", #{field1 => "hello", field2 => 42, field3 => true}}),
    odi_graph:commit(Transaction, 1).

end_per_testcase(TestCase, Config) ->
    odi_open_db_SUITE:end_per_testcase(TestCase, Config).

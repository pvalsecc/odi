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
    odi_open_db_SUITE:init_per_testcase(TestCase, Config).

simple(Config) ->
    Con = ?config(con, Config),
    odi_graph:begin_transaction(Con).

end_per_testcase(TestCase, Config) ->
    odi_open_db_SUITE:end_per_testcase(TestCase, Config).

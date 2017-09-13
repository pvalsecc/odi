-module(odi_live_SUITE).
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
    [_Result2] = odi:command(Con, "CREATE PROPERTY Test.x STRING"),
    [{true, document, _Version, _Class, _RawSchemas}] = odi:record_load(Con, {0, 1}, "*:-1 index:0", true),
    NewConfig.

simple(Config) ->
    Con = ?config(con, Config),
    TestPid = self(),
    io:format("XXX Before subcribing with LIVE SELECT~n"),
    {[{{-1, -1}, document, 0, "", #{"token" := {integer, Token}}}],[]} = odi:live_query(Con, "LIVE SELECT FROM Test", fun() -> TestPid ! message, ok end),
    io:format("XXX After subcribing with LIVE SELECT: ~p~n", [Token]),
    {_Clusters, Con2} = odi:db_open("localhost", "test", "root", "root", []),
    [{true, document, _Version, _Class, _RawSchemas}] = odi:record_load(Con2, {0, 1}, "*:-1 index:0", true),
    io:format("XXX Before INSERT~n"),
    Result = odi:command(Con2, "INSERT INTO Test (x) VALUES ('X')"),
    io:format("XXX After INSERT~n"),
    [{{_ClusterId, _RecordPos}, document, 1, "Test", Data}] = Result,
    odi:close(Con2),

    receive
        {message} ->
            ok
    after
        5000 ->
            io:format("Timeout~n"),
            ct:fail("Didn't receive the notification")
    end.

end_per_testcase(TestCase, Config) ->
    odi_open_db_SUITE:end_per_testcase(TestCase, Config).

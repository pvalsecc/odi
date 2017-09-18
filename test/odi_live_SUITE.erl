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
    {ok, Token} = odi:live_query(Con, "LIVE SELECT FROM Test", fun(What, Message) ->
        TestPid ! {What, Message}
    end),
    io:format("XXX After subcribing with LIVE SELECT: ~p~n", [Token]),

    {_Clusters, Con2} = odi:db_open("localhost", "test", "root", "root", []),
    [{true, document, _Version, _Class, _RawSchemas}] = odi:record_load(Con2, {0, 1}, "*:-1 index:0", true),

    io:format("XXX Before INSERT~n"),
    Result = odi:command(Con2, "INSERT INTO Test (x) VALUES ('X')"),
    io:format("XXX After INSERT~n"),
    [{Rid, document, 1, "Test", Data}] = Result,

    Data2 = #{"x" => {string, "Y"}},
    {2, []} = odi:record_update(Con2, Rid, false, {"V", Data2}, 1, sync),
    io:format("XXX After UPDATE~n"),

    true = odi:record_delete(Con2, Rid, 2, sync),
    io:format("XXX After DELETE~n"),
    odi:close(Con2),

    receive
        Message ->
            {live, {created, {Rid, document, 1, "Test", Data}}} = Message
    after
        5000 -> ct:fail("Didn't receive the created notification")
    end,

    receive
        Message2 ->
            {live, {updated, {Rid, document, 2, "Test", Data2}}} = Message2
    after
        5000 -> ct:fail("Didn't receive the updated notification")
    end,

    receive
        Message3 ->
            {live, {deleted, {Rid, document, 2, "Test", Data2}}} = Message3
    after
        5000 -> ct:fail("Didn't receive the deleted notification")
    end,

    io:format("XXX Before UNSUBSCRIBE~n"),
    [{{-1,-1}, document,0,[], #{"unsubscribe" := {bool,true}}}] =
        odi:command(Con, io_lib:format("LIVE UNSUBSCRIBE ~p", [Token])),

    receive
        Message4 ->
            {live_unsubscribe, {}} = Message4
    after
        5000 -> ct:fail("Didn't receive the unsubscription notification")
    end,

    receive
        Message5 ->
            ct:fail("Unexpected message: ~p", [Message5])
    after
        200 -> ok
    end.


end_per_testcase(TestCase, Config) ->
    odi_open_db_SUITE:end_per_testcase(TestCase, Config).

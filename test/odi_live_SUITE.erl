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
    ct:log("XXX Before subcribing with LIVE SELECT"),
    {ok, Token} = odi:live_query(Con, "LIVE SELECT FROM Test", fun(What, Message) ->
        TestPid ! {What, Message}
    end),
    ct:log("XXX After subcribing with LIVE SELECT: ~p", [Token]),

    {_Clusters, Con2} = odi:db_open("localhost", "test", "root", "root", []),
    [{true, document, _Version, _Class, _RawSchemas}] = odi:record_load(Con2, {0, 1}, "*:-1 index:0", true),

    ct:log("XXX Before INSERT"),
    Result = odi:command(Con2, "INSERT INTO Test (x) VALUES ('X')"),
    ct:log("XXX After INSERT"),
    [{Rid, document, 1, "Test", Data}] = Result,

    Data2 = #{"x" => {string, "Y"}},
    {2, []} = odi:record_update(Con2, Rid, false, {"V", Data2}, 1, sync),
    ct:log("XXX After UPDATE"),

    true = odi:record_delete(Con2, Rid, 2, sync),
    ct:log("XXX After DELETE"),
    odi:close(Con2),  %% TODO: why does the usage of db_close breaks LIVE UNSUBSCRIBE on Con?

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

    ct:log("XXX Before UNSUBSCRIBE"),
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

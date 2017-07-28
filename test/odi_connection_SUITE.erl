-module(odi_connection_SUITE).
-include_lib("common_test/include/ct.hrl").

%% API
-export([all/0, init_per_testcase/2, end_per_testcase/2]).

-export([create/1]).

all() ->
    [create].

init_per_testcase(_TestCase, Config) ->
    {ok, Admin} = odi:connect("localhost", "root", "root", []),
    [{admin, Admin} | Config].

create(Config) ->
    Admin = ?config(admin, Config),
    ok = odi:db_create(Admin, "test", "graph", "plocal", null),
    true = odi:db_exist(Admin, "test", "plocal"),
    ok = odi:db_delete(Admin, "test", "plocal").

end_per_testcase(_TestCase, Config) ->
    Admin = ?config(admin, Config),
    ok = odi:close(Admin).

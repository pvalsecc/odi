%%% Copyright (C) 2013 - Aleksandr Mescheryakov.  All rights reserved.
%% @doc Low level interface to access OrientDB
%%
%% @see odi_graph. odi_graph, if you want higher level access for graphs
%% @end

-module(odi).

-export([connect/4,
         close/1,
         db_open/5,
         db_create/5,
         db_close/1,
         db_exist/3,
         db_reload/1,
         db_delete/3,
         db_size/1,
         db_countrecords/1,
         datacluster_add/3,
         datacluster_remove/2,
         record_create/5,
         record_load/4,
         record_update/6,
         record_update/7,
         record_delete/4,
         query/4,
         query/5,
         command/2,
         command/4,
         script/3,
         script/5,
         live_query/3,
         live_query/4,
         tx_commit/4]).

-include("../include/odi.hrl").

-define(DEFAULT_TIMEOUT, infinity).

%% -- client interface --

-type rid()::{ClusterId::integer(), ClusterPosition::integer()}.
%% A record ID


-type record()::{Class::string(), Data::#{string() => any()}}.  %% TODO: more details
%% A record


-type record_type()::raw|flat|document.


-type mode()::sync|async|no_response.


-type fetched_record()::
    {Key::true|rid(), document, Version::integer(), Class::string(), Data::map()} |
    {Key::true|rid(), raw, Version::integer(), Class::raw, Data::binary()}.
%% The result of a record fetch


-type collection_change()::
    {Uuid::integer(), UpdatedFileId::integer(), UpdatePageIndex::integer(), UpdatedPageOffset::integer()}.

-type tx_operation()::
    {update, Rid::rid(), RecordType::record_type(), Version::integer(), UpdateContent::boolean(), Record::record()} |
    {delete, Rid::rid(), RecordType::record_type(), Version::integer()} |
    {create, Rid::rid(), RecordType::record_type(), Record::record()}.
%% A transaction operation


-type error()::{error, [{ExceptionClass::binary(), ExceptionMessage::binary()}]}.
%% Result type for an error


-export_type([tx_operation/0]).


-spec connect(Host::string(), Username::string(), Password::string(),
              Opts::[{timeout, Timeout::integer()} | {port, Port::integer()}]) -> {ok, Con::pid()} | error().
%% @doc Connect to a server.
%%
%% This is the first operation requested by the client when it needs to work with the server instance without
%% opening a database.
%%
%% `{ok, Con} = odi:connect("127.0.0.1", "server_user", "server_password", []).'
connect(Host, Username, Password, Opts) ->
    {ok, C} = odi_sock:start_link(),
    {call(C, {connect, Host, Username, Password, Opts}), C}.


-spec db_create(C::pid(), DatabaseName::string(), DatabaseType::string(), StorageType::string(),
                BackupPath::string()) -> ok | error().
%% @doc Creates a database in the remote OrientDB server instance.
%%
%% Works in connect-mode.
%%
%% `ok = db_create(Con, "test", "graph", "plocal", null).'
db_create(C, DatabaseName, DatabaseType, StorageType, BackupPath) ->
    call(C, {db_create, DatabaseName, DatabaseType, StorageType, BackupPath}).


-spec db_exist(C::pid(), DatabaseName::string(), StorageType::string()) -> boolean() | error().
%% @doc Asks if a database exists in the OrientDB Server instance.
%%
%% It returns true (non-zero) or false (zero). Works in connect-mode.
%%
%% `db_exist(Con, "test", "plocal").'
db_exist(C, DatabaseName, StorageType) ->
    call(C, {db_exist, DatabaseName, StorageType}).


-spec db_reload(C::pid()) -> [{ClusterName::string(), ClusterId::integer()}] | error().
%% @doc Reloads database information.
db_reload(C) ->
    call(C, {db_reload}).


-spec db_delete(C::pid(), DatabaseName::string(), StorageType::string()) -> ok | error().
%% @doc Removes a database from the OrientDB Server instance.
%%
%% Works in connect-mode.
%%
%% `db_delete(Con, "test", "plocal").'
db_delete(C, DatabaseName, ServerStorageType) ->
    call(C, {db_delete, DatabaseName, ServerStorageType}).


-spec close(C::pid()) -> ok.
%% @doc Close the connection.
close(C) ->
    odi_sock:close(C).


-spec db_open(Host::string(), Dbname::string(), Username::string(), Password::string(),
    Opts::[{timeout, Timeout::integer()} | {port, Port::integer()}]) ->
    {Clusters::[{ClusterName::string(), ClusterId::integer()}], Con::pid()} | error().
%% @doc Open a database
%%
%% This is the first operation the client should call. It opens a database on the remote OrientDB Server.
%%
%% `{Clusters, Con} = odi:db_open("127.0.0.1", "demo",  "admin", "admin", []).'
db_open(Host, DBName, Username, Password, Opts) ->
    {ok, C} = odi_sock:start_link(),
    {call(C, {db_open, Host, DBName, Username, Password, Opts}), C}.


-spec db_close(C::pid()) -> {stop, closed}.
%% @doc Closes the database and the network connection to the OrientDB Server instance.
db_close(C) ->
    call(C, {db_close}).


-spec db_size(C::pid()) -> integer() | error().
%% @doc Returns size of the opened database.
db_size(C) ->
    call(C, {db_size}).


-spec db_countrecords(C::pid()) -> integer() | error().
%% @doc Asks for the number of records in a database in the OrientDB Server instance.
db_countrecords(C) ->
    call(C, {db_countrecords}).


-spec datacluster_add(C::pid(), Name::string(), ClusterId::integer()) -> integer() | error().
%% @doc Add a new data cluster.
%%
%% If you pass -1 as ClusterId, the server will assign one.
%%
%% `NewClusterId = odi:datacluster_add(Con, "toto", -1).'
datacluster_add(C, Name, ClusterId) ->
    call(C, {datacluster_add, Name, ClusterId}).


-spec datacluster_remove(C::pid(), ClusterId::integer()) -> boolean() | error().
%% @doc Remove a cluster.
datacluster_remove(C, ClusterId) ->
    call(C, {datacluster_remove, ClusterId}).


-spec record_create(C::pid(), ClusterId::integer(), RecordContent::binary(), RecordType::record_type(), Mode::mode()) ->
    {Rid::rid(), RecordVersion::integer(), CollectionChanges::[collection_change()]} | error().
%% @doc Create a new record.
%%
%% You can set the VClusterId parameter to -1 to have OrientDB assign one for you (recommended).
%%
%% ```
%% Data1 = #{"toto" => {integer, 42}, "tutu" => {string, "tutu"}},
%% {Rid, 1, CollectionChanges} = odi:record_create(Con, -1, {"V", Data1}, document, sync).
%% '''
record_create(C, ClusterId, {Class, Fields}, document, Mode) ->
    {RecordBin, _} = odi_record_binary:encode_record(Class, Fields, 0),
    call(C, {record_create, ClusterId, RecordBin, document, Mode});
record_create(C, ClusterId, RecordContent, RecordType, Mode) ->
    call(C, {record_create, ClusterId, RecordContent, RecordType, Mode}).


-spec record_load(C::pid(), {ClusterId::integer(), RecordPosition::integer()}, FetchPlan::string()|default,
    IgnoreCache::boolean()) -> [fetched_record()] | error().
%% @doc Load a record by RecordID, according to a fetch plan.
%%
%% ```
%% [{true, document, Version, Class, Data} | CachedRecords] =
%%     odi:record_load(Con, {ClusterId, RecordPos}, "*:2", false).
%% '''
record_load(C, {ClusterId, RecordPosition}, FetchPlan, IgnoreCache) ->
    FetchPlan2 = case FetchPlan of default -> "*:1"; _ -> FetchPlan end,
    call(C, {record_load, ClusterId, RecordPosition, FetchPlan2, IgnoreCache}).


-spec record_update(C::pid(), RID::rid(), UpdateContent::boolean(), Record::record(),
                    OldRecordVersion::pos_integer(), Mode::mode()) ->
    {RecordVersion::pos_integer(), CollectionChanges::[collection_change()]} | error().
%% @doc Update a record. Returns the new record's version.
%%
%% ```
%% Data2 = #{"x" => {string, "Y"}},
%% {2, CollectionChange} = odi:record_update(Con2, Rid, false, {"V", Data2}, 1, sync).
%% '''
record_update(C, RID, UpdateContent, {Class, Fields}, OldRecordVersion, Mode) ->
    {RecordBin, _} = odi_record_binary:encode_record(Class, Fields, 0),
    record_update(C, RID, UpdateContent, RecordBin, OldRecordVersion, document, Mode).


-spec record_update(C::pid(), RID::rid(), UpdateContent::boolean(), Record::binary(),
                    OldRecordVersion ::pos_integer(), RecordType::record_type(), Mode::mode()) ->
    {NewRecordVersion ::pos_integer(), CollectionChanges::[collection_change()]} | error().
%% @doc Update a record. Returns the new record's version.
record_update(C, {ClusterId, ClusterPosition}, UpdateContent, RecordContent, OldRecordVersion, RecordType,
              Mode) when is_binary(RecordContent) ->
    call(C, {record_update, ClusterId, ClusterPosition, UpdateContent, RecordContent, OldRecordVersion, RecordType, Mode}).


%% @doc Delete a record by its RecordID.
-spec record_delete(C::pid(), RID::rid(), RecordVersion::pos_integer(), Mode::mode()) -> boolean() | error().
%%
%% During the optimistic transaction the record will be deleted only if the versions match.
%% Returns true if has been deleted otherwise false.
%%
%% `true = odi:record_delete(Con, {ClusterId, RecordPos}, 2, sync).'
record_delete(C, {ClusterId, ClusterPosition}, RecordVersion, Mode) ->
    call(C, {record_delete, ClusterId, ClusterPosition, RecordVersion, Mode}).


-spec query(C::pid(), SQL::string(), Limit::integer(), FetchPlan::string()|default) ->
    {Results::[fetched_record()], Cached::[fetched_record()]} | error().
%% @doc SQL query (SELECT or TRAVERSE).
%%
%% `{ResultsReadBack, ResultsReadBackCache} = odi:query(Con, "select from V", -1, default).'
query(C, SQL, Limit, FetchPlan) ->
    query(C, SQL, Limit, FetchPlan, null).


-spec query(C::pid(), SQL::string(), Limit::integer(), FetchPlan::string()|default,
            Params::#{string()=>any()} | null) ->
    {Results::[fetched_record()], Cached::[fetched_record()]} | error().
%% @doc SQL query with parameters (SELECT or TRAVERSE).
%%
%% ```
%% {ResultsReadBack, ResultsReadBackCache} =
%%     odi:query(Con, "select from V where x=:value", -1, default, #{"value" => 42}).
%% '''
query(C, SQL, Limit, FetchPlan, Params) ->
    FetchPlan2 = case FetchPlan of default -> "*:1"; _ -> FetchPlan end,
    call(C, {command, {select, SQL, Limit, FetchPlan2, Params}, sync}).


-spec command(C::pid(), SQL::string()) -> [fetched_record()] | error().
%% @doc Synchronous SQL command.
%%
%% `[{Rid, document, 1, "Test", Data}] = odi:command(Con2, "INSERT INTO Test (x) VALUES ('X')").'
command(C, SQL) ->
    command(C, SQL, null, null).


-spec command(C::pid(), SQL::string(), SimpleParams::#{string() => any()} | null,
              ComplexParams::#{string() => {embedded_list, []}} | null) -> [fetched_record()] | error().
%% @doc Synchronous SQL command with parameters.
%%
%% ```
%% [{Rid, document, 1, "Test", Data}] =
%%     odi:command(Con2, "INSERT INTO Test (x) VALUES (:x)", #{"x" => "X"}).
%% '''
command(C, SQL, SimpleParams, ComplexParams) ->
    {Results, []} = call(C, {command, {command, SQL, SimpleParams, ComplexParams}, sync}),
    Results.


-spec script(C::pid(), Language::string(), Code::string()) ->
    {[fetched_record()], [fetched_record()]} | error().
%% @doc Synchronous script.
%%
%% ```
%% {[{{-1, -1}, document, 0, [], #{"result" := {integer, 4}}}], []} =
%%     odi:script(Con, "Javascript", "print('hello world'); 4").
%% '''
script(C, Language, Code) ->
    script(C, Language, Code, null, null).


-spec script(C::pid(), Language::string(), Code::string(), SimpleParams::#{string() => any()} | null,
             ComplexParams::#{string() => {embedded_list, []}} | null) ->
    {[fetched_record()], [fetched_record()]} | error().
%% @doc Synchronous script with parameters.
%%
%% @see script/3
script(C, Language, Code, SimpleParams, ComplexParams) ->
    call(C, {command, {script, Language, Code, SimpleParams, ComplexParams}, sync}).


-spec live_query(C::pid(), SQL::string(),
                 CallBack::fun((live | live_unsubscription,
                                {loaded|updated|deleted|created, fetched_record()} | {}) -> any())) ->
    {ok, Token::integer()} | error().
%% @doc Live SELECT query.
%%
%% ```
%% TestPid = self(),
%% {ok, Token} = odi:live_query(Con, "LIVE SELECT FROM Test", fun(What, Message) ->
%%     TestPid ! {What, Message}
%% end).
%% '''
live_query(C, SQL, CallBack) ->
    live_query(C, SQL, null, CallBack).


-spec live_query(C::pid(), SQL::string(), Params::#{string() => any()} | null,
    CallBack::fun((live, {loaded|updated|deleted|created, fetched_record()}) -> any())) ->
    {ok, Token::integer()} | error().
%% @doc Live SELECT query with parameters.
%%
%% @see live_query/3
live_query(C, SQL, Params, CallBack) ->
    case call(C, {command, {live, SQL, -1, "", Params, CallBack}, live}) of
        {[{_Rid, document, 0, _Class, #{"token" := {integer, Token}}}],[]} -> {ok, Token};
        Other -> Other
    end.


-spec tx_commit(C::pid(), TxId::integer(), UsingLog::boolean(), Operations::[tx_operation()]) ->
  {CreatedRecords::[{ClientSpecifiedRid::rid(), ActualRid::rid()}],
   UpdatedRecords::[{UpdatedRid::rid(), NewRecordVersion::integer()}],
   CollectionChanges::[collection_change()]} | error().
%% @doc Commits a transaction.
%%
%% This operation flushes all the given changes to the server side in a single transaction.
%%
%% ```
%% {IdRemaps, _Updates, _Changes} = = odi:tx_commit(Con, 1, true, [
%%     {create, {-1, -2}, document, {"V", Data1}},
%%     {update, {VClusterId1, RecordPos1}, document, 1, true, {"V", Data1b}},
%%     {delete, {VClusterId2, RecordPos2}, document, 1}
%% ]).
%% '''
%%
%% Create operations can use -1 for the cluster IDs and other negative values for the record IDs. IdRemaps
%% allows to know what was actually used.
tx_commit(C, TxId, UsingTxLog, Operations) ->
    call(C, {tx_commit, TxId, UsingTxLog, lists:map(fun encode_operation_record/1, Operations)}).



%% -- internal functions --

call(C, Command) ->
    case gen_server:call(C, Command, infinity) of
        Error = {error, _} -> Error;
        {R} -> R;
        R -> R
    end.


encode_operation_record({update, Rid, RecordType, Version, UpdateContent,
                         {Class, Fields}}) ->
    {RecordBin, _} = odi_record_binary:encode_record(Class, Fields, 0),
    {update, Rid, RecordType, Version, UpdateContent,
        RecordBin};
encode_operation_record({delete, _Rid, _RecordType, _Version} = Record) ->
  Record;
encode_operation_record({create, Rid, RecordType, {Class, Fields}}) ->
    {RecordBin, _} = odi_record_binary:encode_record(Class, Fields, 0),
    {create, Rid, RecordType, RecordBin}.

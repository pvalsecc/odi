%%% Copyright (C) 2013 - Aleksandr Mescheryakov.  All rights reserved.

-module(odi).

-export([start_link/0,
         connect/4,
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
         command/2,
         script/3,
         tx_commit/4]).

-include("../include/odi.hrl").

-define(DEFAULT_TIMEOUT, infinity).

%% -- client interface --

-type rid()::{ClusterId::integer(), ClusterPosition::integer()}.
-type record()::{Class::string(), Data::#{string() => any()}}.  %% TODO: more details
-type record_type()::raw|flat|document.
-type mode()::sync|async|no_response.
-type fetched_record()::
    {Key::true|rid(), document, Version::integer(), Class::string(), Data::map()} |
    {Key::true|rid(), raw, Version::integer(), Class::raw, Data::binary()}.
-type tx_operation()::
    {update, Rid::rid(), RecordType::record_type(), Version::integer(), UpdateContent::boolean(), Record::record()} |
    {delete, Rid::rid(), RecordType::record_type(), Version::integer()} |
    {create, Rid::rid(), RecordType::record_type(), Record::record()}.
-type error()::{error, [{ExceptionClass::binary(), ExceptionMessage::binary()}]}.

-export_type([tx_operation/0]).

start_link() ->
    odi_sock:start_link().

%This is the first operation requested by the client when it needs to work with the server instance without openning a database.
%Returns the session_id:integer of the client.
%Opts (proplists) = port (default 2424), timeout (default 5000 ms).
-spec connect(Host::string(), Username::string(), Password::string(),
              Opts::[{timeout, Timeout::integer()} | {port, Port::integer()}]) -> {ok, Con::pid()} | error().
connect(Host, Username, Password, Opts) ->
    {ok, C} = odi_sock:start_link(),
    {call(C, {connect, Host, Username, Password, Opts}), C}.

%This is the first operation the client should call. It opens a database on the remote OrientDB Server.
%Returns the Session-Id to being reused for all the next calls and the list of configured clusters.
-spec db_open(Host::string(), Dbname::string(), Username::string(), Password::string(),
              Opts::[{timeout, Timeout::integer()} | {port, Port::integer()}]) ->
      {Clusters::[{ClusterName::string(), ClusterId::integer()}], Con::pid()} | error().
db_open(Host, DBName, Username, Password, Opts) ->
    {ok, C} = odi_sock:start_link(),
    {call(C, {db_open, Host, DBName, Username, Password, Opts}), C}.

%Creates a database in the remote OrientDB server instance.
%Works in connect-mode.
-spec db_create(C::pid(), DatabaseName::string(), DatabaseType::string(), StorageType::string(),
                BackupPath::string()) -> ok | error().
db_create(C, DatabaseName, DatabaseType, StorageType, BackupPath) ->
    call(C, {db_create, DatabaseName, DatabaseType, StorageType, BackupPath}).

%Closes the database and the network connection to the OrientDB Server instance.
%No return is expected. The socket is also closed.
-spec db_close(C::pid()) -> {stop, closed}.
db_close(C) ->
    call(C, {db_close}).

%Asks if a database exists in the OrientDB Server instance. It returns true (non-zero) or false (zero).
%Works in connect-mode.
-spec db_exist(C::pid(), DatabaseName::string(), StorageType::string()) -> boolean() | error().
db_exist(C, DatabaseName, StorageType) ->
    call(C, {db_exist, DatabaseName, StorageType}).

%Reloads database information.
-spec db_reload(C::pid()) -> [{ClusterName::string(), ClusterId::integer()}] | error().
db_reload(C) ->
    call(C, {db_reload}).

%Removes a database from the OrientDB Server instance.
%Works in connect-mode.
-spec db_delete(C::pid(), DatabaseName::string(), StorageType::string()) -> ok | error().
db_delete(C, DatabaseName, ServerStorageType) ->
    call(C, {db_delete, DatabaseName, ServerStorageType}).

%Returns size of the opened database.
-spec db_size(C::pid()) -> integer() | error().
db_size(C) ->
    call(C, {db_size}).

%Asks for the number of records in a database in the OrientDB Server instance.
-spec db_countrecords(C::pid()) -> integer() | error().
db_countrecords(C) ->
    call(C, {db_countrecords}).

%Add a new data cluster.
-spec datacluster_add(C::pid(), Name::string(), ClusterId::integer()) -> integer() | error().
datacluster_add(C, Name, ClusterId) ->
    call(C, {datacluster_add, Name, ClusterId}).

%Remove a cluster.
-spec datacluster_remove(C::pid(), ClusterId::integer()) -> boolean() | error().
datacluster_remove(C, ClusterId) ->
    call(C, {datacluster_remove, ClusterId}).

%Create a new record. Returns the position in the cluster of the new record.
%New records can have version > 0 (since v1.0) in case the RID has been recycled.
-spec record_create(C::pid(), ClusterId::integer(), RecordContent::binary(), RecordType::record_type(), Mode::mode()) ->
    {ClusterId::integer(), ClusterPosition::integer(), RecordVersion::integer(),
     [{Uuid::integer(), UpdatedFileId::integer(), UpdatePageIndex::integer(), UpdatedPageOffset::integer()}]} |
    error().
record_create(C, ClusterId, {Class, Fields}, document, Mode) ->
    {RecordBin, _} = odi_record_binary:encode_record(Class, Fields, 0),
    call(C, {record_create, ClusterId, RecordBin, document, Mode});
record_create(C, ClusterId, RecordContent, RecordType, Mode) ->
    call(C, {record_create, ClusterId, RecordContent, RecordType, Mode}).

%Load a record by RecordID, according to a fetch plan
-spec record_load(C::pid(), {ClusterId::integer(), RecordPosition::integer()}, FetchPlan::string()|default,
    IgnoreCache::boolean()) -> [fetched_record()] | error().
record_load(C, {ClusterId, RecordPosition}, FetchPlan, IgnoreCache) ->
    FetchPlan2 = case FetchPlan of default -> "*:1"; _ -> FetchPlan end,
    call(C, {record_load, ClusterId, RecordPosition, FetchPlan2, IgnoreCache}).

%Update a record. Returns the new record's version.
%   RecordVersion: current record version
%   RecordType: raw, flat, document
%   Mode: sync, async
-spec record_update(C::pid(), RID::rid(), UpdateContent::boolean(), Record::record(),
                    OldRecordVersion::integer(), Mode::mode()) ->
    {RecordVersion::integer(),
     [{Uuid::integer(), UpdatedFileId::integer(), UpdatePageIndex::integer(), UpdatedPageOffset::integer()}]} | error().
record_update(C, RID, UpdateContent, {Class, Fields}, OldRecordVersion, Mode) ->
    {RecordBin, _} = odi_record_binary:encode_record(Class, Fields, 0),
    record_update(C, RID, UpdateContent, RecordBin, OldRecordVersion, document, Mode).

-spec record_update(C::pid(), RID::rid(), UpdateContent::boolean(), Record::binary(),
                    OldRecordVersion ::integer(), RecordType::record_type(), Mode::mode()) ->
    {OldRecordVersion ::integer(),
     [{Uuid::integer(), UpdatedFileId::integer(), UpdatePageIndex::integer(), UpdatedPageOffset::integer()}]} | error().
record_update(C, {ClusterId, ClusterPosition}, UpdateContent, RecordContent, OldRecordVersion, RecordType,
              Mode) when is_binary(RecordContent) ->
    call(C, {record_update, ClusterId, ClusterPosition, UpdateContent, RecordContent, OldRecordVersion, RecordType, Mode}).

%Delete a record by its RecordID. During the optimistic transaction the record will be deleted only if the versions match.
%Returns true if has been deleted otherwise false.
-spec record_delete(C::pid(), RID::rid(), RecordVersion::integer(), Mode::mode()) -> boolean() | error().
record_delete(C, {ClusterId, ClusterPosition}, RecordVersion, Mode) ->
    call(C, {record_delete, ClusterId, ClusterPosition, RecordVersion, Mode}).

%SQL query (SELECT or TRAVERSE).
-spec query(C::pid(), SQL::string(), Limit::integer(), FetchPlan::string()|default) ->
    {Results::[fetched_record()], Cached::[fetched_record()]} | error().
query(C, SQL, Limit, FetchPlan) ->
    FetchPlan2 = case FetchPlan of default -> "*:1"; _ -> FetchPlan end,
    call(C, {command, {select, SQL, Limit, FetchPlan2}, sync}).

%Syncronous SQL command.
-spec command(C::pid(), SQL::string()) -> [fetched_record()] | error().
command(C, SQL) ->
    {Results, []} = call(C, {command, {command, SQL}, sync}),
    Results.

%Syncronous SQL script.
-spec script(C::pid(), Language::string(), Code::string()) -> {[fetched_record()], [fetched_record()]} | error().
script(C, Language, Code) ->
    call(C, {command, {script, Language, Code}, sync}).

%Commits a transaction. This operation flushes all the pending changes to the server side.
%   Operations: [{OperationType, ClusterId, ClusterPosition, RecordType}]
-spec tx_commit(C::pid(), TxId::integer(), UsingLog::boolean(), Operations::[tx_operation()]) ->
  {CreatedRecords::[{ClientSpecifiedRid::rid(), ActualRid::rid()}],
   UpdatedRecords::[{UpdatedRid::rid(), NewRecordVersion::integer()}],
   CollectionChanges::[{Uuid::number(), UpdatedFileId::number(), UpdatedPageIndex::number(),
                        UpdatedPageOffset ::number()}]} | error().
tx_commit(C, TxId, UsingTxLog, Operations) ->
    call(C, {tx_commit, TxId, UsingTxLog, lists:map(fun encode_operation_record/1, Operations)}).

-spec close(C::pid()) -> ok.
close(C) ->
    odi_sock:close(C).

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


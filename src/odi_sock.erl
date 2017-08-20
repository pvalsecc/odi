%%% Copyright (C) 2013 - Aleksandr Mescheryakov.  All rights reserved.

-module(odi_sock).

-behavior(gen_server).

-export([start_link/0,
         close/1,
         get_parameter/2]).

-export([handle_call/3, handle_cast/2, handle_info/2]).
-export([init/1, code_change/3, terminate/2]).

-export([on_response/3, command/2]).

-include("../include/odi.hrl").
-include("odi_debug.hrl").

-record(state, {mod,    %socket module: gen_tcp or ssl(unsupported)
                sock,   %opened socket
                session_id = -1, %OrientDB session Id
                open_mode = wait_version, %connection opened with: connect() | db_open()
                data = <<>>, %received data from socket
                queue = queue:new(), %commands queue
                timeout = 5000, %network timeout
                global_properties = #{}
                }).

%% -- client interface --

start_link() ->
    gen_server:start_link(?MODULE, [], []).

close(C) when is_pid(C) ->
    catch gen_server:cast(C, stop),
    ok.

get_parameter(C, Name) ->
    gen_server:call(C, {get_parameter, to_binary(Name)}, infinity).

% --- support functions ---

to_binary(B) when is_binary(B) -> B;
to_binary(L) when is_list(L)   -> list_to_binary(L).

%% -- gen_server implementation --

init([]) ->
    {ok, #state{}}.

terminate(_Reason, State) ->
    handle_cast(stop, State),
    ok.

handle_call(Command, From, #state{queue=Q, timeout=Timeout} = State) ->
    Req = {{call, From}, Command},
    case command(Command, State#state{queue = queue:in(Req, Q)}) of
        {noreply, State2} -> {noreply, State2, Timeout};
        Error -> Error
    end.

handle_cast({{Method, From, Ref}, Command} = Req, State)
        when (Method == cast), is_pid(From), is_reference(Ref) ->
    #state{queue = Q} = State,
    command(Command, State#state{queue = queue:in(Req, Q)});

handle_cast(stop, #state{sock = Sock} = State) ->
    case is_port(Sock) of
        true -> gen_tcp:close(Sock);
        _ -> false
    end,
    {stop, normal, flush_queue(State, {stop, closed})}.

flush_queue(#state{queue = Q} = State, Error) ->
    case queue:is_empty(Q) of
        false ->
            flush_queue(finish(State, Error), Error);
        true -> State
    end.

handle_info(timeout, State) ->
    {stop, timeout, flush_queue(State, {error, timeout})};

% Receive messages from socket:
% on socket close
handle_info({Closed, Sock}, #state{sock = Sock} = State) when Closed == tcp_closed; Closed == ssl_closed ->
    ?odi_debug_sock("Socket closed by the remote side~n", []),
    {stop, sock_closed, flush_queue(State, {error, sock_closed})};

% on socket error
handle_info({Error, Sock, Reason}, #state{sock = Sock} = State) when Error == tcp_error; Error == ssl_error ->
    ?odi_debug_sock("Socket error: ~p~n", [Reason]),
    Why = {sock_error, Reason},
    {stop, Why, flush_queue(State, {error, Why})};

% socket ok
handle_info({inet_reply, _, ok}, State) ->
    {noreply, State};

% socket is not ok
handle_info({inet_reply, Reason, Status}, State) ->
    ?odi_debug_sock("Socket not OK: ~p~n", [Reason]),
    {stop, Status, flush_queue(State, {error, Status})};

% receive data from socket
handle_info({tcp, Sock, Data2}, #state{data = Data, sock = Sock} = State) ->
    loop(State#state{data = <<Data/binary, Data2/binary>>}).

%Handle code change
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

% -- Commands processing --

% ?
% command(Command, State = #state{sync_required = true})
%   when Command /= sync ->
%     {noreply, finish(State, {error, sync_required})};

%This is the first operation requested by the client when it needs to work with the server instance without openning a database.
%It returns the session id of the client.
%  Request: (driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)
%           (serialization-impl:string)(token-session:boolean)(support-push)(collect-stats)
%           (user-name:string)(user-password:string)
%  Response: (session-id:int)(token:bytes)
command({connect, Host, Username, Password, Opts}, State) ->
    % % storing login data in the process dictionary for security reasons?
    % put(username, Username),
    % put(password, Password),
    {ok, State2} = pre_connect(State, Host, Opts),
    sendRequest(State2, ?O_CONNECT,
        [string, string, short, string, string, bool, bool, bool, string, string],
        [?O_DRV_NAME, ?O_DRV_VER, ?O_PROTO_VER, null, ?O_RECORD_SERIALIZER_BINARY, false, false, false, Username, Password]),
    {noreply, State2};

%This is the first operation the client should call. It opens a database on the remote OrientDB Server.
%Returns the Session-Id to being reused for all the next calls and the list of configured clusters.
%  Request: (driver-name:string)(driver-version:string)(protocol-version:short)(client-id:string)
%           (serialization-impl:string)(token-session:boolean)(support-push:boolean)(collect-stats:boolean)
%           (database-name:string)(user-name:string)(user-password:string)
%  Response: (session-id:int)(token:bytes)(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)]
%           (cluster-config:bytes)(orientdb-release:string)
%dbType = document | graph.
command({db_open, Host, DBName, Username, Password, Opts}, State) ->
    % % storing login data in the process dictionary for security reasons?
    % put(username, Username),
    % put(password, Password),
    case pre_connect(State, Host, Opts) of
        {error, _} = E -> {noreply, finish(State, E)};
        {ok, State2} ->
            sendRequest(State2, ?O_DB_OPEN,
                [string, string, short, string, string, bool, bool, bool, string, string, string],
                [?O_DRV_NAME, ?O_DRV_VER, ?O_PROTO_VER, null, ?O_RECORD_SERIALIZER_BINARY, false,
                false, false, DBName, Username, Password]),
            {noreply, State2}
    end;

%Creates a database in the remote OrientDB server instance
%   Request: (database-name:string)(database-type:string)(storage-type:string)(backup-path)
%   Response: empty
command({db_create, DatabaseName, DatabaseType, StorageType, BackupPath}, State) ->
    sendRequest(State, ?O_DB_CREATE,
        [string, string, string, string],
        [DatabaseName, DatabaseType, StorageType, BackupPath]),
    {noreply, State};

%Closes the database and the network connection to the OrientDB Server instance. No return is expected. The socket is also closed.
%  Request:  empty
%  Response: no response, the socket is just closed at server side
command({db_close}, State) ->
    sendRequest(State, ?O_DB_CLOSE, [], []),
    handle_cast(stop, State),
    {stop, normal, State};

%Asks if a database exists in the OrientDB Server instance. It returns true (non-zero) or false (zero).
%   Request:  (database-name:string) <-- before 1.0rc1 this was empty (server-storage-type:string - since 1.5-snapshot)
%   Response: (result:bool)
command({db_exist, DatabaseName, StorageType}, State) ->
    sendRequest(State, ?O_DB_EXIST,
        [string, string],
        [DatabaseName, StorageType]),
    {noreply, State};

%Reloads database information. Available since 1.0rc4.
%   Request:  empty
%   Response: (num-of-clusters:short)[(cluster-name:string)(cluster-id:short)]
command({db_reload}, State) ->
    sendRequest(State, ?O_DB_RELOAD, [], []),
    {noreply, State};

%Removes a database from the OrientDB Server instance.
%It returns nothing if the database has been deleted or throws a OStorageException if the database doesn't exists.
%   Request:  (database-name:string)(server-storage-type:string - since 1.5-snapshot)
%   Response: empty
command({db_delete, DatabaseName, ServerStorageType}, State) ->
    sendRequest(State, ?O_DB_DELETE,
        [string, string],
        [DatabaseName, ServerStorageType]),
    {noreply, State};

%Asks for the size of a database in the OrientDB Server instance.
%   Request:  empty
%   Response: (size:long)
command({db_size}, State) ->
    sendRequest(State, ?O_DB_SIZE, [], []),
    {noreply, State};

%Asks for the number of records in a database in the OrientDB Server instance.
%   Request:  empty
%   Response: (count:long)
command({db_countrecords}, State) ->
    sendRequest(State, ?O_DB_COUNTRECORDS, [], []),
    {noreply, State};

%Add a new data cluster.
%   Request:  (type:string)(name:string)(location:string)(datasegment-name:string)
%   Response: (new-cluster:short)
command({datacluster_add, Name, ClusterId}, State) ->
    sendRequest(State, ?O_DATACLUSTER_ADD,
        [string, short],
        [Name, ClusterId]),
    {noreply, State};

%Remove a cluster.
%   Request:  (cluster-number:short)
%   Response: (delete-on-clientside:byte)
command({datacluster_remove, ClusterId}, State) ->
    sendRequest(State, ?O_DATACLUSTER_REMOVE,
        [short],
        [ClusterId]),
    {noreply, State};

%Create a new record. Returns the position in the cluster of the new record. New records can have version > 0 (since v1.0) in case the RID has been recycled.
%   Request: (cluster-id:short)(record-content:bytes)(record-type:byte)(mode:byte)
%   Response: (cluster-id:short)(cluster-position:long)(record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
command({record_create, ClusterId, RecordContent, RecordType, Mode}, State) ->
    sendRequest(State, ?O_RECORD_CREATE,
        [short, bytes, byte, byte],
        [ClusterId, RecordContent,
        odi_bin:encode_record_type(RecordType),
        odi_bin:mode_to_byte(Mode)]),
    {noreply, State};

%Load a record by RecordID, according to a fetch plan
%   Request: (cluster-id:short)(cluster-position:long)(fetch-plan:string)(ignore-cache:boolean)(load-tombstones:boolean)
%   Response: [(payload-status:byte)[(record-type:byte)(record-version:int)(record-content:bytes)]*]+
command({record_load, ClusterId, ClusterPosition, FetchPlan, IgnoreCache}, State) ->
    sendRequest(State, ?O_RECORD_LOAD,
        [short, long, string, bool, bool],
        [ClusterId, ClusterPosition, FetchPlan, IgnoreCache, false]),
    {noreply, State};

%Update a record. Returns the new record's version.
%   Request: (cluster-id:short)(cluster-position:long)(update-content:boolean)(record-content:bytes)(record-version:int)(record-type:byte)(mode:byte)
%   Response: (record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
command({record_update, ClusterId, ClusterPosition, UpdateContent, RecordContent, RecordVersion, RecordType, Mode}, State) ->
    sendRequest(State, ?O_RECORD_UPDATE,
        [short, long, bool, bytes, integer, byte, byte],
        [ClusterId, ClusterPosition, UpdateContent, RecordContent,
        RecordVersion,
        odi_bin:encode_record_type(RecordType),
        odi_bin:mode_to_byte(Mode)]),
    {noreply, State};

%Delete a record by its RecordID. During the optimistic transaction the record will be deleted only if the versions match.
%Returns true if has been deleted otherwise false.
%   Request:  (cluster-id:short)(cluster-position:long)(record-version:int)(mode:byte)
%   Response: (has-been-deleted:boolean)
command({record_delete, ClusterId, ClusterPosition, RecordVersion, Mode}, State) ->
    sendRequest(State, ?O_RECORD_DELETE,
        [short, long, integer, byte],
        [ClusterId, ClusterPosition, RecordVersion,
        odi_bin:mode_to_byte(Mode)]),
    {noreply, State};

%Executes remote commands.
%   Request:  (mode:byte)(command-payload-length:int)(class-name:string)(command-payload)
%   Response:
%   - synchronous commands: [(synch-result-type:byte)[(synch-result-content:?)]]+
%   - asynchronous commands: [(asynch-result-type:byte)[(asynch-result-content:?)]*](pre-fetched-record-size)[(pre-fetched-record)]*+
command({command, Query, sync}, State) ->
    CommandPayload = case Query of
        {select, QueryText, Limit, FetchPlan} ->
            %% (class-name:string)(text:string)(non-text-limit:int)[(fetch-plan:string)](serialized-params:bytes[])
            odi_bin:encode(
                [string, string, integer, string, bytes],
                ["q", QueryText, Limit, FetchPlan,
                 <<>>]);  % TODO: support params
        {command, Text} ->
            %% (class-name:string)(text:string)(has-simple-parameters:boolean)(simple-paremeters:bytes[])(has-complex-parameters:boolean)(complex-parameters:bytes[])
            odi_bin:encode([string, string, bool, bool], ["c", Text, false, false]);  % TODO: support params
        {script, Language, Text} ->
            %% (class-name:string)(language:string)(text:string)(has-simple-parameters:boolean)(simple-paremeters:bytes[])(has-complex-parameters:boolean)(complex-parameters:bytes[])
            odi_bin:encode([string, string, string, bool, bool], ["s", Language, Text, false, false])  % TODO: support params
    end,
    sendRequest(State, ?O_COMMAND,
        [byte, bytes],
        [$s, CommandPayload]),
    {noreply, State};

%generic_query:$s,CommandPayload("com.orientechnologies.orient.core.sql.OCommandSQL",QueryText)

%Commits a transaction. This operation flushes all the pending changes to the server side.
%   Request: (transaction-id:int)(using-tx-log:boolean)(tx-entry)*(0-byte indicating end-of-records)
%     tx-entry:  (1:byte)(operation-type:byte)(cluster-id:short)(cluster-position:long)(record-type:byte)(entry-content)
%   Response: (created-record-count:int)[(client-specified-cluster-id:short)(client-specified-cluster-position:long)(created-cluster-id:short)(created-cluster-position:long)]*(updated-record-count:int)[(updated-cluster-id:short)(updated-cluster-position:long)(new-record-version:int)]*(count-of-collection-changes:int)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
%   Operations: [[OperationType, ClusterId, ClusterPosition, RecordType]]
command({tx_commit, TxId, UsingTxLog, Operations}, State) ->
    UnknownStuff = <<0:24>>,
    sendRequest(State, ?O_TX_COMMIT,
        [integer, bool, {zero_end, rawbytes}, bytes],
        [TxId, UsingTxLog, lists:map(fun encode_tx_operation/1, Operations), UnknownStuff]),
    {noreply, State};

command(_Command, State) ->
    {error, State}.

% support functions ---

pre_connect(State, Host, Opts) ->
    Timeout = proplists:get_value(timeout, Opts, 5000),
    Port = proplists:get_value(port, Opts, 2424),
    SockOpts = [{active, true}, {packet, raw}, binary, {nodelay, true}],
    case gen_tcp:connect(Host, Port, SockOpts, Timeout) of
        {ok, Sock} -> {ok, State#state{mod = gen_tcp, sock = Sock, timeout = Timeout}};
        {error, _} = E -> E
    end.

sendRequest(#state{mod = Mod, sock = Sock, session_id = SessionId}, CommandType, Types, Values) ->
    Data = <<CommandType:?o_byte, SessionId:?o_int, (iolist_to_binary(odi_bin:encode(Types, Values)))/binary>>,
    %erlang:display({send, binary_to_list(Data)}),
    do_send(Mod, Sock, Data).

% port_command() more efficient then gen_tcp:send()
do_send(gen_tcp, Sock, Bin) ->
    ?odi_debug_sock("Sending: 0x~s~n", [hex:bin_to_hexstr(Bin)]),
    try erlang:port_command(Sock, Bin) of
        true ->
            ok
    catch
        error:_Error ->
            {error,einval}
    end;

do_send(ssl, _Sock, _Bin) ->
    {error, ssl_unsupported}.

finish(State, Result) ->
    finish(State, Result, Result).

finish(State = #state{queue = Q}, _Notice, Result) ->
    case queue:get(Q) of
        % {{cast, From, Ref}, _} ->
        %     From ! {self(), Ref, Result};
        % {{incremental, From, Ref}, _} ->
        %     From ! {self(), Ref, Notice};
        {{call, From}, _} ->
            gen_server:reply(From, Result)
    end,
    State#state{queue = queue:drop(Q)}.

current_command(#state{queue = Q}) ->
    case queue:len(Q) == 0 of
        true ->
            none;
        false ->
            {_, Req} = queue:get(Q),
            Req
    end.

command_tag(State) ->
    case current_command(State) of
        none -> none;
        Req when is_tuple(Req) -> element(1, Req);
        Req when is_atom(Req) -> Req
    end.

%% -- backend message handling --

%main loop
loop(#state{data = Data, timeout = Timeout} = State) -> %timeout = Timeout
    Cmd = command_tag(State),
    %erlang:display({recv, Cmd, binary_to_list(Data)}),
    %erlang:display({recv, Cmd, size(Data)}),
    case Cmd of
        none -> {noreply, #state{data = <<>>}};
        _ ->
            case byte_size(Data) > 0 of
                true ->
                    ?odi_debug_sock("Received: 0x~s~n", [hex:bin_to_hexstr(Data)]),
                    case on_response(Cmd, Data, State) of
                        {fetch_more, State2} -> {noreply, State2, Timeout};
                        {noreply, #state{data = <<>>} = State2} -> {noreply, State2};
                        {noreply, State2}                       -> loop(State2);
                        R = {stop, _Reason2, _State2}           -> R
                    end;
                false ->
                    {noreply, State}
            end
    end.

%Process empty response message
on_empty_response(Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            State2 = finish(State#state{data = Rest}, {error, ErrorInfo});
        0 -> State2 = finish(State#state{data = Message}, ok)
    end,
    {noreply, State2}.

%Process response message without changing State (excl. Data)
on_simple_response(Bin, State, Format) ->
    try
        <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
        case Status of
            1 ->
                {ErrorInfo,Rest} = odi_bin:decode_error(Message),
                ?odi_debug_sock("Got an error: ~p~n", [ErrorInfo]),
                State2 = finish(State#state{data = Rest}, {error, ErrorInfo});
            0 ->
                {Result, Rest} = odi_bin:decode(Format, Message),
                State2 = finish(State#state{data = Rest}, Result)
        end,
        {noreply, State2}
    catch
        X:Y ->
            ?odi_debug_sock("Error while parsing simple response: ~p:~p~n~p~n", [X, Y, erlang:get_stacktrace()]),
            {fetch_more, State}
    end.


on_response(_Command, Bin, #state{open_mode = wait_version} = State) ->
    <<Version:?o_short, Rest/binary>> = Bin,
    ?odi_debug_sock("Got version ~p~n", [Version]),
    true = Version >= ?O_PROTO_VER,
    {fetch_more, State#state{open_mode = wait_answer, data = Rest}};

% Response: (session-id:int)(token:bytes)
on_response(connect, Bin, #state{sock = Sock, open_mode = wait_answer} = State) ->
    <<Status:?o_byte, _OldSessionId:?o_int, Message/binary>> = Bin,
    case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            State2 = State#state{data = Rest},
            gen_tcp:close(Sock),
            State3 = finish(State2, {error, ErrorInfo});
        0 -> <<SessionId:?o_int, _Token:?o_int, Rest/binary>> = Message,
            State2 = State#state{session_id = SessionId, open_mode = connect, data = Rest},
            State3 = finish(State2, ok);
        _ ->
            State2 = State#state{data = <<>>},
            gen_tcp:close(Sock),
            State3 = finish(State2, {error, error_server_response})
    end,
	{noreply, State3};

% Response: (session-id:int)(token:bytes)(num-of-clusters:short)[(cluster-name:string)(cluster-id:short)](cluster-config:bytes)(orientdb-release:string)
on_response(db_open, Bin, #state{sock = Sock, open_mode = wait_answer} = State) ->
    <<Status:?o_byte, _OldSessionId:?o_int, Message/binary>> = Bin,
    try
        case Status of
            1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
                gen_tcp:close(Sock),
                {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
            0 ->
                {{SessionId, _Token, ClusterParams, ClusterConfig, _OrientdbRelease}, Rest}
                    = odi_bin:decode([integer, bytes, {short, [string, short]}, bytes, string], Message),
                {noreply, finish(State#state{session_id = SessionId, open_mode = db_open, data = Rest},
                    {ClusterParams, ClusterConfig})};
             _ ->
                gen_tcp:close(Sock),
                {noreply, finish(State#state{data = <<>>}, {error, error_server_response, Bin})}
        end
    catch
        _:_ -> {fetch_more, State}
    end;

% Response: empty
on_response(db_create, Bin, State) ->
    on_empty_response(Bin, State);

% Response: none, socket closed.
on_response(db_close, _Bin, State) ->
    {stop, normal, finish(State, db_closed)};

% Response: empty
on_response(db_exist, Bin, State) ->
    on_simple_response(Bin, State, [bool]);

% Response: (num-of-clusters:short)[(cluster-name:string)(cluster-id:short)]
on_response(db_reload, Bin, State) ->
    on_simple_response(Bin, State, [{short, [string, short]}]);

on_response(db_delete, Bin, State) ->
    on_empty_response(Bin, State);

% Response: (size:long)
on_response(db_size, Bin, State) ->
    on_simple_response(Bin, State, [long]);

% Response: (count:long)
on_response(db_countrecords, Bin, State) ->
    on_simple_response(Bin, State, [long]);

% Response: (new-cluster:short)
on_response(datacluster_add, Bin, State) ->
    on_simple_response(Bin, State, [short]);

% Response: (delete-on-clientside:byte)
on_response(datacluster_remove, Bin, State) ->
    on_simple_response(Bin, State, [bool]);

% Response: (cluster-id:short)(cluster-position:long)(record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
on_response(record_create, Bin, State) ->
    on_simple_response(Bin, State, [short, long, integer, {integer, [longlong, long, long, integer]}]);

% Response: [(payload-status:byte)[(record-content:bytes)(record-version:int)(record-type:byte)]*]+
on_response(record_load, Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    try case Status of
        1 ->
            {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
        0 ->
            {Records, Rest} = decode_records_iterable(Message, State#state.global_properties, []),
            State2 = case current_command(State) of
                {record_load, 0, 1, "*:-1 index:0", true} ->
                    [{true, document, _Version, _Class, RawSchemas}] = Records,
                    GlobalProperties = odi_typed:index_global_properties(odi_typed:untypify_record(RawSchemas)),
                    ?odi_debug_sock("Capturing the GlobalProperties: ~p~n", [GlobalProperties]),
                    State#state{global_properties=GlobalProperties};
                _ ->
                    State
            end,
            {noreply, finish(State2#state{data = Rest}, Records)}
        end
    catch
        X:Y ->
            ?odi_debug_sock("Error while parsing record_load response: ~p:~p~n~p~n", [X, Y, erlang:get_stacktrace()]),
            {fetch_more, State}
    end;


% Response: (record-version:int)(count-of-collection-changes)[(uuid-most-sig-bits:long)(uuid-least-sig-bits:long)(updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)]*
on_response(record_update, Bin, State) ->
    on_simple_response(Bin, State, [integer, {integer, [longlong, long, long, integer]}]);

% Response: (has-been-deleted:boolean)
on_response(record_delete, Bin, State) ->
    on_simple_response(Bin, State, [bool]);

% Response:
% - synchronous commands: [(synch-result-type:byte)[(synch-result-content:?)]]+
on_response(command, Bin, State) ->
    <<Status:?o_byte, _SessionId:?o_int, Message/binary>> = Bin,
    try case Status of
        1 -> {ErrorInfo,Rest} = odi_bin:decode_error(Message),
            {noreply, finish(State#state{data = Rest}, {error, ErrorInfo})};
        0 ->
            {Results, Rest} = decode_command_answer(Message, State),
            {noreply, finish(State#state{data = Rest}, Results)}
        end
    catch
        X:Y ->
            ?odi_debug_sock("Error while parsing command response: ~p:~p~n~p~n", [X, Y, erlang:get_stacktrace()]),
            {fetch_more, State}
    end;

% Response: Response: (created-record-count:int)[
%                       (client-specified-cluster-id:short)(client-specified-cluster-position:long)
%                       (created-cluster-id:short)(created-cluster-position:long)
%                      ]*
%                     (updated-record-count:int)[
%                       (updated-cluster-id:short)(updated-cluster-position:long)
%                       (new-record-version:int)
%                     ]*
%                     (count-of-collection-changes:int)[
%                       (uuid-most-sig-bits:long)(uuid-least-sig-bits:long)
%                       (updated-file-id:long)(updated-page-index:long)(updated-page-offset:int)
%                     ]*
on_response(tx_commit, Bin, State) ->
    on_simple_response(Bin, State, [{integer, [[short, long], [short, long]]},
                                    {integer, [[short, long], integer]},
                                    {integer, [longlong, long, long, integer]}]);

on_response(_Command, _Bin, State) ->
    {error, State}.


encode_tx_operation({update, Rid, RecordType, Version, UpdateContent, RecordContent}) ->
    Base = encode_base_tx_operation(1, Rid, RecordType),
    %% (version:int)(update-content:boolean)(record-content:bytes)  (wrong order...)
    odi_bin:encode([rawbytes, integer, bytes, bool], [Base, Version, RecordContent, UpdateContent]);
encode_tx_operation({delete, Rid, RecordType, Version}) ->
    Base = encode_base_tx_operation(2, Rid, RecordType),
    %% (version:int)
    odi_bin:encode([rawbytes, integer], [Base, Version]);
encode_tx_operation({create, Rid, RecordType, RecordContent}) ->
    Base = encode_base_tx_operation(3, Rid, RecordType),
    %% (record-content:bytes)
    odi_bin:encode([rawbytes, bytes], [Base, RecordContent]).

encode_base_tx_operation(OperationType, {ClusterId, ClusterPosition}, RecordType) ->
    odi_bin:encode(
        [byte, byte, short, long, byte],
        [1, OperationType, ClusterId, ClusterPosition, odi_bin:encode_record_type(RecordType)]).


decode_records_iterable(<<0:?o_byte, Msg/binary>>, _GlobalProperties, Acc) ->
    {lists:reverse(Acc), Msg};
decode_records_iterable(<<1:?o_byte, Msg/binary>>, GlobalProperties, Acc) ->
    {{RecordType, RecordVersion, RecordBin}, NextRecord} = odi_bin:decode([byte, integer, bytes], Msg),
    {Class, Data, <<>>} = odi_record_binary:decode_record(RecordType, RecordBin, RecordBin, GlobalProperties),
    decode_records_iterable(NextRecord, GlobalProperties,
        [{true, odi_bin:decode_record_type(RecordType), RecordVersion, Class, Data} | Acc]);
decode_records_iterable(<<2:?o_byte, Msg/binary>>, GlobalProperties, Records) ->
    {Record, NextRecord} = decode_record(Msg, GlobalProperties),
    decode_records_iterable(NextRecord, GlobalProperties, [Record | Records]).

decode_record(<<0:?o_short, Bin/binary>>, GlobalProperties) ->
    {{RecordType, ClusterId, RecordPosition, RecordVersion, RecordBin}, Rest} = odi_bin:decode([byte, short, long, integer, bytes], Bin),
    {Class, Data, <<>>} = odi_record_binary:decode_record(RecordType, RecordBin, RecordBin, GlobalProperties),
    {{{ClusterId, RecordPosition}, odi_bin:decode_record_type(RecordType), RecordVersion, Class, Data}, Rest};
decode_record(<<-2:?o_short, Rest/binary>>, _GlobalProperties) ->
    {null, Rest};
decode_record(<<-3:?o_short, Bin/binary>>, _GlobalProperties) ->
    {{ClusterId, RecordPosition}, Rest} = odi_bin:decode([short, long], Bin),
    {{{ClusterId, RecordPosition}, null, null, null, null}, Rest}.


decode_command_answer(Bin, #state{global_properties = GlobalProperties}) ->
    {Results, CachedBin} = decode_command_answer_primary(Bin, GlobalProperties),
    {Cached, Rest} = decode_records_iterable(CachedBin, GlobalProperties, []),
    {{Results, Cached}, Rest}.

decode_command_answer_primary(<<$n:?o_byte, Rest/binary>>, _GlobalProperties) ->
    {[], Rest};
decode_command_answer_primary(<<$l:?o_byte, Num:?o_int, Rest/binary>>, GlobalProperties) ->
    decode_record_list(Num, Rest, GlobalProperties, []);
decode_command_answer_primary(<<$s:?o_byte, Num:?o_int, Rest/binary>>, GlobalProperties) ->
    decode_record_list(Num, Rest, GlobalProperties, []);
decode_command_answer_primary(<<$i:?o_byte, Rest/binary>>, GlobalProperties) ->
    decode_records_iterable(Rest, GlobalProperties, []);
decode_command_answer_primary(<<$r:?o_byte, Bin/binary>>, GlobalProperties) ->
    {Record, Rest} = decode_record(Bin, GlobalProperties),
    {[Record], Rest};
decode_command_answer_primary(<<$w:?o_byte, Bin/binary>>, GlobalProperties) ->
    {Record, Rest} = decode_record(Bin, GlobalProperties),
    {[Record], Rest}.

decode_record_list(0, Rest, _GlobalProperties, Acc) ->
    {lists:reverse(Acc), Rest};
decode_record_list(N, Bin, GlobalProperties, Acc) ->
    {Record, Rest} = decode_record(Bin, GlobalProperties),
    decode_record_list(N - 1, Rest, GlobalProperties, [Record | Acc]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

decode_record_load_test() ->
    %% one record and a linked record
    Bin = <<1,100,0,0,0,2,0,0,0,29,0,2,86,8,111,117,116,95,0,0,0,14,
        22,0,1,0,0,0,1,0,17,0,0,0,0,0,0,0,0,
        2,0,0,100,0,17,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,25,0,2,69,6,111,
        117,116,0,0,0,21,13,4,105,110,0,0,0,
        23,13,0,20,0,18,0,0>>,
    {[
        {true, document, 2, "V", #{"out_" := {linkbag, [{17, 0}]}}},
        {{17, 0}, document, 1, "E", #{"in" := {link, {9, 0}}, "out" := {link, {10, 0}}}}
    ], <<>>} = decode_records_iterable(Bin, #{}, []).


decode_record_iterable_test() ->
    GlobalProperties = #{
        23 => #{"id" => 23,"name" => "field1","type" => "STRING"},
        24 => #{"id" => 24,"name" => "field2","type" => "LONG"},
        25 => #{"id" => 25,"name" => "field3","type" => "BOOLEAN"},
        26 => #{"id" => 26,"name" => "in","type" => "LINK"},
        27 => #{"id" => 27,"name" => "out","type" => "LINK"}
    },
    Bin = hex:hexstr_to_bin("016400000002000000520008546573742F0000002D3100000033086F75745F0000003416186F75745F54657374456467650000004316000A68656C6C6F540100000001002900000000000000000100000001002A000000000000000002000064002A0000000000000000000000010000001900105465737445646765370000001535000000170032004200020000640029000000000000000000000001000000190010546573744564676535000000153700000017004200320000"),
    Records = decode_records_iterable(Bin, GlobalProperties, []),
    Expected = [
        {true, document, 2, "Test", #{
            "field1" => {string, "hello"},
            "field2" => {long, 42},
            "out_" => {linkbag, [{41, 0}]},
            "out_TestEdge" => {linkbag, [{42, 0}]}
        }},
        {{42, 0}, document, 1, "TestEdge", #{
            "in" => {link, {33, 0}},
            "out" => {link, {25, 0}}
        }},
        {{41,0}, document, 1, "TestEdge", #{
            "in" => {link, {33, 0}},
            "out" => {link, {25, 0}}
        }}
    ],
    {Expected, <<>>} = Records.

-endif.

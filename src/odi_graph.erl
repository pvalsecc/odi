-module(odi_graph).
-behaviour(gen_server).

%% API
-export([begin_transaction/1, create_vertex/3, create_edge/5, update/3, commit/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("odi_debug.hrl").

-define(SERVER, ?MODULE).

-type record_data()::#{string()|atom() => any()}.

-record(state, {
    con :: pid(),
    commands = [] :: [odi:tx_operation()],
    command_pos = #{} :: #{odi:rid() => pos_integer()},
    classes :: #{string() => record_data()},
    global_properties :: #{non_neg_integer() => record_data()}
}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts a transaction
%%
%% @end
%%--------------------------------------------------------------------
-spec begin_transaction(Con::pid()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}.
begin_transaction(Con) ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [Con], []).

-spec create_vertex(T::pid(), TempId::neg_integer()|odi:rid(), Record::{Class::string(), Data:: record_data()}) -> ok.
create_vertex(T, TempId, Record) ->
    gen_server:call(T, {create_vertex, TempId, Record}).

-spec create_edge(T::pid(), TempId::neg_integer()|odi:rid(), FromId::pos_integer()|odi:rid(),
                  ToId::pos_integer()|odi:rid(), Record::{Class::string(), Data:: record_data()}) -> ok.
create_edge(T, TempId, FromId, ToId, Record) ->
    gen_server:call(T, {create_edge, TempId, FromId, ToId, Record}).

-spec update(T::pid(), Rid::odi:rid(), Data:: record_data()) -> ok.
update(T, Rid, Data) ->
    gen_server:call(T, {update, Rid, Data}).

%%TODO: delete

-spec commit(T::pid(), TxId::pos_integer()) -> IdRemaps::#{integer() => odi:rid()}.
commit(T, TxId) ->
    gen_server:call(T, {commit, TxId}).


%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
-spec(init(Args :: term()) ->
    {ok, State :: #state{}} | {ok, State :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Con]) ->
%%    [{true, raw, 0, raw, Config}] = odi:record_load(Con, {0, 0}, "", false),
%%    io:format("Config: ~s~n", [Config]),

    [{true, document, _Version, _Class, RawSchemas}] = odi:record_load(Con, {0, 1}, "*:-1 index:0", true),
    Schema = odi_typed:untypify_record(RawSchemas),
    Classes = index_classes(Schema),
    GlobalProperties = odi_typed:index_global_properties(Schema),
    ?odi_debug_graph("Classes: ~p~n", [Classes]),
    ?odi_debug_graph("GlobalProperties: ~p~n", [GlobalProperties]),

%%    Indexes = odi:record_load(Con, {0, 2}, "*:-1 index:0", true),
%%    io:format("Indexes: ~p~n", [Indexes]),

%%    {Sequences, []} = odi:query(Con, "SELECT FROM OSequence", -1, ""),
%%    io:format("Sequences: ~p~n", [Sequences]),

    {ok, #state{con=Con, classes=Classes, global_properties=GlobalProperties}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #state{}) ->
    {reply, Reply :: term(), NewState :: #state{}} |
    {reply, Reply :: term(), NewState :: #state{}, timeout() | hibernate} |
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #state{}} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_call({create_vertex, TempId, Record}, _From, #state{classes=Classes}=State) ->
    TranslatedRecord = odi_typed:typify_record(Record, Classes),
    ?odi_debug_graph("Translated: ~p~n", [TranslatedRecord]),
    State2 = add_create_command(rid(TempId), TranslatedRecord, State),
    {reply, ok, State2};
handle_call({create_edge, TempId, FromId, ToId, {Class, Data}}, _From, #state{classes=Classes}=State) ->
    LinkedData = Data#{"out" => rid(FromId), "in" => rid(ToId)},
    TranslatedRecord = odi_typed:typify_record({Class, LinkedData}, Classes),
    ?odi_debug_graph("Translated edge: ~p~n", [TranslatedRecord]),
    Rid = rid(TempId),
    State2 = add_edge_ref(rid(FromId), Rid, "out_" ++ Class, State),
    State3 = add_edge_ref(rid(ToId), Rid, "in_" ++ Class, State2),
    State4 = add_create_command(Rid, TranslatedRecord,State3),
    {reply, ok, State4};
handle_call({update, Rid, Data}, _From, State) ->
    {reply, ok, update_impl(rid(Rid), Data, State)};
handle_call({commit, TxId}, _From, #state{con=Con, commands=Commands}=State) ->
    ?odi_debug_graph("Committing ~p~n", [Commands]),
    {Ids, _Update, _Changes} = odi:tx_commit(Con, TxId, true, Commands),
    {stop, normal, get_id_remaps(Ids, #{}), State};
handle_call(Request, _From, State) ->
    io:format("Unknown call: ~p~n", [Request]),
    {reply, ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @end
%%--------------------------------------------------------------------
-spec(handle_cast(Request :: term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_cast(Request, State) ->
    io:format("Unknown cast: ~p~n", [Request]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
-spec(handle_info(Info :: timeout() | term(), State :: #state{}) ->
    {noreply, NewState :: #state{}} |
    {noreply, NewState :: #state{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #state{}}).
handle_info(Info, State) ->
    io:format("Unknown info: ~p~n", [Info]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #state{}) -> term()).
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #state{},
    Extra :: term()) ->
    {ok, NewState :: #state{}} | {error, Reason :: term()}).
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================


index_classes(Schemas) ->
    #{"classes" := ClassList} = Schemas,
    IndexedClasses = odi_typed:index_records(ClassList, "name", #{}),
    maps:map(fun(_K, V) -> index_embedded_set_field("properties", "name", V) end, IndexedClasses).

index_embedded_set_field(FieldName, IndexName, Record) ->
    #{FieldName := ValueList} = Record,
    IndexedField = odi_typed:index_records(ValueList, IndexName, #{}),
    Record#{FieldName => IndexedField}.

rid({_ClusterId, _ClusterPosition}=Id) ->
    Id;
rid(_TempPosition) when is_integer(_TempPosition) ->
    {-1, _TempPosition}.

add_edge_ref(VertexId, EdgeId, PropertyName, #state{command_pos=CommandPos}=State) ->
    ?odi_debug_graph("Trying to find ~p for updating ~p~n", [VertexId, PropertyName]),
    case CommandPos of
        #{VertexId := VertexCommandPos} ->
            add_edge_ref_to_transaction(VertexCommandPos, EdgeId, PropertyName, State);
        _ ->
            add_edge_ref_to_existing(VertexId, EdgeId, PropertyName, State)
    end.

add_edge_ref_to_transaction(VertexCommandPos, EdgeId, PropertyName, #state{commands=Commands}=State) ->
    NewCommand = case lists:nth(VertexCommandPos, Commands) of
        {create, Rid, document, {Class, Data}} ->
            ?odi_debug_graph("updating ~p in create ~p~n", [PropertyName, Rid]),
            NewData = add_edge_ref_to_data(Data, PropertyName, EdgeId),
            {create, Rid, document, {Class, NewData}};
        {update, Rid, document, Version, true, {Class, Data}} ->
            ?odi_debug_graph("updating ~p in update ~p~n", [PropertyName, Rid]),
            NewData = add_edge_ref_to_data(Data, PropertyName, EdgeId),
            {update, Rid, document, Version, true, {Class, NewData}}
    end,
    NewCommands = replace_nths(NewCommand, VertexCommandPos, Commands),
    State#state{commands = NewCommands}.

add_edge_ref_to_existing(VertexId, EdgeId, PropertyName, #state{con=Con}=State) ->
    ?odi_debug_graph("fetching ~p to update ~p~n", [VertexId, PropertyName]),
    [{true, document, Version, Class, Data}] = odi:record_load(Con, VertexId, "", true),
    NewData = add_edge_ref_to_data(Data, PropertyName, EdgeId),
    add_update_command(VertexId, Version, {Class, NewData}, State).

add_edge_ref_to_data(Data, PropertyName, EdgeId) ->
    case Data of
        #{PropertyName := {linkbag, Links}} ->
            Data#{PropertyName => {linkbag, Links ++ [EdgeId]}};
        _ ->
            Data#{PropertyName => {linkbag, [EdgeId]}}
    end.


add_create_command(Rid, Record, #state{commands=Commands, command_pos=CommandPos}=State) ->
    State#state{
        commands=Commands ++ [{create, Rid, document, Record}],
        command_pos =CommandPos#{Rid => length(Commands) + 1}
    }.


add_update_command(Rid, Version, Record, #state{commands=Commands, command_pos=CommandPos}=State) ->
    State#state{
        commands=Commands ++ [{update, Rid, document, Version, true, Record}],
        command_pos =CommandPos#{Rid => length(Commands) + 1}
    }.


replace_nths(Value, N, List) ->
    lists:sublist(List, N - 1) ++ [Value] ++
        lists:nthtail(N, List).


get_id_remaps([], Map) ->
    Map;
get_id_remaps([{{-1, OldId}, NewRid} | Rest], Map) ->
    get_id_remaps(Rest, Map#{OldId => NewRid});
get_id_remaps([{Rid, Rid} | Rest], Map) ->
    get_id_remaps(Rest, Map).


update_impl(Rid, Data, #state{command_pos=CommandPos, con=Con}=State) ->
    ?odi_debug_graph("Trying to find ~p for updating data~n", [Rid]),
    case CommandPos of
        #{Rid := VertexCommandPos} ->
            update_in_transaction(VertexCommandPos, Data, State);
        _ ->
            update_existing(Con, Rid, Data, State)
    end.


update_in_transaction(VertexCommandPos, UpdateData, #state{commands=Commands}=State) ->
    NewCommand = case lists:nth(VertexCommandPos, Commands) of
                     {create, Rid, document, {Class, Data}} ->
                         ?odi_debug_graph("updating ~p in create ~p~n", [UpdateData, Rid]),
                         NewData = update_data(Class, Data, UpdateData, State),
                         {create, Rid, document, {Class, NewData}};
                     {update, Rid, document, Version, true, {Class, Data}} ->
                         ?odi_debug_graph("updating ~p in update ~p~n", [UpdateData, Rid]),
                         NewData = update_data(Class, Data, UpdateData, State),
                         {update, Rid, document, Version, true, {Class, NewData}}
                 end,
    NewCommands = replace_nths(NewCommand, VertexCommandPos, Commands),
    State#state{commands = NewCommands}.

update_existing(Con, Rid, UpdateData, State) ->
    ?odi_debug_graph("fetching ~p to update ~p~n", [Rid, UpdateData]),
    [{true, document, Version, Class, Data}] = odi:record_load(Con, Rid, "", true),
    NewData = update_data(Class, Data, UpdateData, State),
    add_update_command(Rid, Version, {Class, NewData}, State).

update_data(Class, Orig, Updates, #state{classes=Classes}) ->
    {Class, TypifiedUpdates} = odi_typed:typify_record({Class, Updates}, Classes),
    maps:merge(Orig, TypifiedUpdates).

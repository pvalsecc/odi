-module(odi_graph).
-behaviour(gen_server).

%% API
-export([begin_transaction/1, create_vectice/3, commit/2]).

%% gen_server callbacks
-export([init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3]).

-include("odi_debug.hrl").

-define(SERVER, ?MODULE).

-record(state, {
    con :: pid(),
    commands = [] :: [odi:tx_operation()],
    classes :: #{}
}).

-type record()::#{string() => any()}.

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

-spec create_vectice(T::pid(), TempId::pos_integer(), Record::{Class::string(), Data::record()}) -> ok.
create_vectice(T, TempId, Record) ->
    gen_server:call(T, {create_vectice, TempId, Record}).

-spec commit(T::pid(), TxId::pos_integer()) -> any().
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

    [{true, document, _Version, _Class, Schemas}] = odi:record_load(Con, {0, 1}, "*:-1 index:0", true),
    IndexedClasses = index_classes(odi_typed:untypify_record(Schemas)),
    ?odi_debug_graph("Classes: ~p~n", [IndexedClasses]),

%%    Indexes = odi:record_load(Con, {0, 2}, "*:-1 index:0", true),
%%    io:format("Indexes: ~p~n", [Indexes]),

%%    {Sequences, []} = odi:query(Con, "SELECT FROM OSequence", -1, ""),
%%    io:format("Sequences: ~p~n", [Sequences]),

    {ok, #state{con=Con, classes= IndexedClasses}}.

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
handle_call({create_vectice, TempId, Record}, _From, #state{classes=Classes, commands=Commands}=State) ->
    TranslatedRecord = odi_typed:typify_record(Record, Classes),
    ?odi_debug_graph("Translated: ~p~n", [TranslatedRecord]),
    {reply, ok, State#state{commands=[{create, -1, TempId, document, TranslatedRecord} | Commands]}};
handle_call({commit, TxId}, _From, #state{con=Con, commands=Commands}=State) ->
    odi:tx_commit(Con, TxId, true, Commands),
    {stop, normal, ok, State};
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
    IndexedClasses = index_records(ClassList, "name", #{}),
    maps:map(fun(_K, V) -> index_embedded_set_field("properties", "name", V) end, IndexedClasses).


index_records([], _Field, Acc) ->
    Acc;
index_records([Record | Rest], Field, Acc) ->
    #{Field := Value} = Record,
    index_records(Rest, Field, Acc#{Value => Record}).

index_embedded_set_field(FieldName, IndexName, Record) ->
    #{FieldName := ValueList} = Record,
    IndexedField = index_records(ValueList, IndexName, #{}),
    Record#{FieldName => IndexedField}.

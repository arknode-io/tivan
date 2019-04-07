%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2018, danny
%%% @doc
%%%
%%% @end
%%% Created : 2018-08-08 10:26:55.093338
%%%-------------------------------------------------------------------
-module(tivan_schema).

-behaviour(gen_server).

%% API
-export([start_link/0
        ,create/2
        ,create/3
        ,create/4
        ,drop/1
        ,clear/1
        ,transform/2
        ,transform/3]).


%% gen_server callbacks
-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
  gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create(Table, Attributes) ->
  create(Table, Attributes, true, []).

create(Table, AttributesIndexes, PersistFlag) ->
  create(Table, AttributesIndexes, PersistFlag, []).

create(Table, AttributesIndexes, PersistFlag, Options) ->
  gen_server:call(?MODULE, {create, Table, AttributesIndexes, PersistFlag, Options}).

drop(Table) ->
  gen_server:cast(?MODULE, {drop, Table}).

clear(Table) ->
  gen_server:call(?MODULE, {clear, Table}).

transform(Table, AttributesIndexes) ->
  transform(Table, AttributesIndexes, #{}).

transform(Table, AttributesIndexes, DefaultValues) ->
  gen_server:call(?MODULE, {transform, Table, AttributesIndexes, DefaultValues}).

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
init([]) ->
  init_schema(),
  {ok, #{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call({create, Table, AttributesIndexes, PersistFlag, Options}, _From, State) ->
  Reply = handle_create(Table, AttributesIndexes, PersistFlag, Options),
  {reply, Reply, State};
handle_call({clear, Table}, _From, State) ->
  Reply = handle_clear(Table),
  {reply, Reply, State};
handle_call({transform, Table, AttributesIndexes, DefaultValues}, _From, State) ->
  Reply = handle_transform(Table, AttributesIndexes, DefaultValues),
  {reply, Reply, State};
handle_call(_Request, _From, State) ->
  Reply = ok,
  {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast({drop, Table}, State) ->
  handle_drop(Table),
  {noreply, State};
handle_cast(_Msg, State) ->
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
handle_info(_Info, State) ->
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
code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

init_schema() ->
  case application:get_env(tivan, remote_node, undefined) of
    undefined ->
      init_standalone_schema();
    RemoteNode ->
      case net_adm:ping(RemoteNode) of
        pang ->
          init_standalone_schema();
        pong ->
          init_cluster_schema(RemoteNode)
      end
  end.

init_cluster_schema(RemoteNode) ->
  MnesiaNodes = mnesia:system_info(running_db_nodes),
  case lists:member(RemoteNode, MnesiaNodes) of
    true ->
      init_standalone_schema();
    false ->
      rpc:call(RemoteNode, mnesia, change_config,
               [extra_db_nodes, [node()]]),
      init_standalone_schema()
  end.

init_standalone_schema() ->
  PersistFlag = application:get_env(tivan, persist_db, true),
  case mnesia:table_info(schema, storage_type) of
    disc_copies when not PersistFlag ->
      vaporize();
    ram_copies when PersistFlag ->
      persist();
    _ ->
      ok
  end.

vaporize() ->
  LocalTables = mnesia:system_info(local_tables) -- [schema],
  Vaporize = fun(Table) ->
                 case mnesia:table_info(Table, storage_type) of
                   disc_copies ->
                     catch mnesia:change_table_copy_type(Table, node(), ram_copies);
                   _ ->
                     ok
                 end
             end,
  lists:map(Vaporize, LocalTables),
  mnesia:change_table_copy_type(schema, node(), ram_copies).

persist() ->
  mnesia:change_table_copy_type(schema, node(), disc_copies).

handle_create(Table, AttributesIndexes, PersistFlag, Options) ->
  SchemaPersistFlag = application:get_env(tivan, persist_db, true),
  {Attributes, Indexes} = get_attributes_indexes(AttributesIndexes),
  case catch mnesia:table_info(Table, storage_type) of
    {'EXIT', _Reason} ->
      StorageOption = if PersistFlag, SchemaPersistFlag -> {disc_copies, [node()]};
                         true -> {ram_copies, [node()]} end,
      case mnesia:create_table(Table, [{attributes, Attributes}, {index, Indexes},
                                  StorageOption|Options]) of
        {atomic, ok} -> ok;
        Error -> Error
      end;
    unknown ->
      StorageType = if PersistFlag, SchemaPersistFlag -> disc_copies;
                       true -> ram_copies end,
      case mnesia:add_table_copy(Table, node(), StorageType) of
        {atomic, ok} -> ok;
        Error -> Error
      end;
    ram_copies when PersistFlag, SchemaPersistFlag ->
      mnesia:change_table_copy_type(Table, node(), disc_copies);
    _Other ->
      ok
  end.

handle_drop(Table) ->
  mnesia:delete_table(Table).

handle_clear(Table) ->
  mnesia:clear_table(Table).

handle_transform(Table, AttributesIndexes, DefaultValues) ->
  {Attributes, Indexes} = get_attributes_indexes(AttributesIndexes),
  AttributesNow = mnesia:table_info(Table, attributes),
  IndexesNow = [ lists:nth(X-1, AttributesNow) || X <- mnesia:table_info(Table, index) ],
  [ mnesia:del_table_index(Table, Index) || Index <- IndexesNow -- Indexes ],
  if
    Attributes == AttributesNow -> ok;
    true ->
      Transfun = transform_function(Table, AttributesNow, Attributes, DefaultValues),
      mnesia:transform_table(Table, Transfun, Attributes)
  end,
  [ mnesia:add_table_index(Table, Index) || Index <- Indexes -- IndexesNow ].

transform_function(Table, AttributesNow, Attributes, DefaultValues) ->
  fun(Row) ->
      RowU = lists:map(
               fun(Column) ->
                   case string:str(AttributesNow, [Column]) of
                     0 -> maps:get(Column, DefaultValues, undefined);
                     Pos -> element(Pos + 1, Row)
                   end
               end,
               Attributes
              ),
      list_to_tuple([Table|RowU])
  end.

get_attributes_indexes(AttributesIndexes) ->
  lists:foldr(
    fun({Attribute}, {AttributesAcc, IndexesAcc}) ->
        {[Attribute|AttributesAcc], [Attribute|IndexesAcc]};
       (Attribute, {AttributesAcc, IndexesAcc}) ->
        {[Attribute|AttributesAcc], IndexesAcc}
    end,
    {[], []},
    AttributesIndexes
   ).


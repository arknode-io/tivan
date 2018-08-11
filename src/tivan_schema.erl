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
-export([start_link/0, create/2, create/3, drop/1, transform/2, transform/3]).


%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

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
  create(Table, Attributes, []).

create(Table, AttributesIndexes, Options) ->
  gen_server:call(?MODULE, {create, Table, AttributesIndexes, Options}).

drop(Table) ->
  gen_server:cast(?MODULE, {drop, Table}).

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
handle_call({create, Table, AttributesIndexes, Options}, _From, State) ->
  Reply = handle_create(Table, AttributesIndexes, Options),
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
  ok.

handle_create(Table, AttributesIndexes, Options) ->
  {Attributes, Indexes} = get_attributes_indexes(AttributesIndexes),
  mnesia:create_table(Table, [{attributes, Attributes}, {index, Indexes}|Options]).

handle_drop(Table) ->
  mnesia:delete_table(Table).

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


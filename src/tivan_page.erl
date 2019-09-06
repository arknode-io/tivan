%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-04-19 18:38:31.676004
%%%-------------------------------------------------------------------
-module(tivan_page).

-behaviour(gen_server).

-define(TIMEOUT, 600).
-define(OPTIONS, [public, compressed]).

%% API
-export([start_link/0
        ,new/0
        ,new/2
        ,put/2
        ,get/1
        ,get/2
        ,sort/2
        ,remove/1
        ,list/0
        ,purge/0
        ,info/1]).


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

new() -> new(page, #{}).

new(Name, Options) ->
  CacheOptions = maps:get(cache_options, Options, ?OPTIONS),
  case maps:get(manage, Options, false) of
    true ->
      do_new(Name, CacheOptions);
    false ->
      InactiveTimeout = maps:get(inactive_timeout, Options, ?TIMEOUT),
      gen_server:call(?MODULE, {new, Name, CacheOptions, InactiveTimeout})
  end.

put(Id, Objects) ->
  case inspect(Id) of
    {_, public} -> do_put(Id, Objects);
    {caller, _} -> do_put(Id, Objects);
    {?MODULE, _} -> gen_server:call(?MODULE, {put, Id, Objects});
    Error -> Error
  end.

get(Id) -> get(Id, #{}).

get(Id, Options) ->
  case inspect(Id) of
    {?MODULE, private} -> gen_server:call(?MODULE, {get, Id, Options});
    {?MODULE, _} ->
      gen_server:cast(?MODULE, {access, Id}),
      do_get(Id, Options);
    {caller, _} -> do_get(Id, Options);
    Error -> Error
  end.

sort(Id, SortArgs) ->
  case inspect(Id) of
    {?MODULE, public} ->
      gen_server:cast(?MODULE, {access, Id}),
      do_sort(Id, SortArgs);
    {_, public} -> do_sort(Id, SortArgs);
    {caller, _} -> do_sort(Id, SortArgs);
    {?MODULE, _} -> gen_server:call(?MODULE, {sort, Id, SortArgs});
    Error -> Error
  end.

remove(Id) ->
  case inspect(Id) of
    {caller, _} -> do_remove(Id);
    {?MODULE, _} -> gen_server:cast(?MODULE, {remove, Id});
    Error -> Error
  end.

list() -> gen_server:call(?MODULE, list).

purge() -> gen_server:cast(?MODULE, purge).

info(Id) -> ets:info(Id).

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
handle_call({new, Name, CacheOptions, InactiveTimeout}, _From, State) ->
  Id = do_new(Name, CacheOptions),
  StateNew = State#{Id => {erlang:system_time(second), InactiveTimeout}},
  {reply, Id, StateNew};
handle_call({put, Id, Objects}, _From, State) ->
  {_, InactiveTimeout} = maps:get(Id, State),
  Reply = do_put(Id, Objects),
  StateNew = State#{Id => {erlang:system_time(second), InactiveTimeout}},
  {reply, Reply, StateNew};
handle_call({get, Id, Options}, _From, State) ->
  {_, InactiveTimeout} = maps:get(Id, State),
  Reply = do_get(Id, Options),
  StateNew = State#{Id => {erlang:system_time(second), InactiveTimeout}},
  {reply, Reply, StateNew};
handle_call({sort, Id, SortArgs}, _From, State) ->
  {_, InactiveTimeout} = maps:get(Id, State),
  Reply = do_sort(Id, SortArgs),
  StateNew = State#{Id => {erlang:system_time(second), InactiveTimeout}},
  {reply, Reply, StateNew};
handle_call({remove, Id}, _From, State) ->
  Reply = do_remove(Id),
  StateNew = maps:remove(Id, State),
  {reply, Reply, StateNew};
handle_call(list, _From, State) ->
  {reply, maps:keys(State), State};
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
handle_cast({access, Id}, State) ->
  {_, Timeout} = maps:get(Id, State),
  StateNew = State#{Id => {erlang:system_time(second), Timeout}},
  {noreply, StateNew};
handle_cast({remove, Id}, State) ->
  do_remove(Id),
  StateNew = maps:remove(Id, State),
  {noreply, StateNew};
handle_cast(purge, State) ->
  StateNew = do_purge(State),
  {noreply, StateNew};
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

do_new(Name, CacheOptions) ->
  ets:new(Name, CacheOptions).

inspect(Id) ->
  case ets:info(Id) of
    undefined ->
      {error, undefined};
    InfoList ->
      Access = proplists:get_value(protection, InfoList),
      Self = self(),
      Page = whereis(?MODULE),
      case proplists:get_value(owner, InfoList) of
        Self ->
          {caller, Access};
        Page ->
          {?MODULE, Access};
        _ ->
          {error, not_allowed}
      end
  end.

do_put(Id, Objects) ->
  lists:foldl(
    fun(O, K) ->
        ets:insert(Id, {K, O}),
        K + 1
    end,
    1,
    Objects
   ),
  ok.

do_get(Id, #{search := Pattern}) ->
  PatternLower = pattern_lowercase(Pattern),
  ets:foldr(
    fun({_, O}, Os) ->
        OBin = object_to_lowercase(O),
        case binary:match(OBin, PatternLower) of
          nomatch -> Os;
          _matches -> [O|Os]
        end
    end,
    [],
    Id
   );
do_get(Id, Options) ->
  Start = maps:get(start, Options, 1),
  Stop = Start + maps:get(limit, Options, ets:info(Id, size)) - 1,
  lists:foldr(
    fun(K, Os) ->
        case ets:lookup(Id, K) of
          [] ->
            Os;
          [{K, O}] ->
            [O|Os]
        end
    end,
    [],
    lists:seq(Start, Stop)
   ).

object_to_lowercase(Object) ->
  list_to_binary(
    string:lowercase(
      lists:flatten(
        io_lib:format("~p", [Object])
       )
     )
   ).

pattern_lowercase(Pattern) when is_list(Pattern) ->
  [ pattern_lowercase(X) || X <- Pattern ];
pattern_lowercase(Pattern) when is_binary(Pattern) ->
  string:lowercase(Pattern).

do_sort(Id, SortArgs) ->
  case SortArgs of
    Fun when is_function(Fun, 2) ->
      ObjectsList = [ V || {_K, V} <- ets:tab2list(Id) ],
      ObjectsListSorted = lists:sort(Fun, ObjectsList),
      reload(Id, ObjectsListSorted);
    {Key, Order} when Order == desc; Order == asc ->
      ObjectsList = lists:map(
                      fun({_K, #{Key := Kv} = V}) ->
                          {Kv, V};
                         ({_K, V}) ->
                          {undefined, V}
                      end,
                      ets:tab2list(Id)
                     ),
      Fun = case Order of
              desc -> fun({K1, _}, {K2, _}) -> K1 > K2 end;
              _ -> fun({K1, _}, {K2, _}) -> K1 < K2 end
            end,
      ObjectsListSorted = [ V || {_, V} <- lists:sort(Fun, ObjectsList)],
      reload(Id, ObjectsListSorted);
    Other ->
      do_sort(Id, {Other, asc})
  end.

reload(Id, ObjectsListSorted) ->
  ets:delete_all_objects(Id),
  lists:foldl(
    fun(O, K) ->
        ets:insert(Id, {K, O}),
        K + 1
    end,
    1,
    ObjectsListSorted
   ),
  ok.

do_remove(Id) ->
  ets:delete(Id).

do_purge(State) ->
  TsNow = erlang:system_time(second),
  maps:fold(
    fun(Id, {Ts, T}, S) ->
        if
          TsNow - Ts > T ->
            ets:delete(Id),
            S;
          true ->
            S#{Id => {Ts, T}}
        end
    end,
    #{},
    State
   ).

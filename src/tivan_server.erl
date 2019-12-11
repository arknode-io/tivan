%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-05-01 09:43:53.183980
%%%-------------------------------------------------------------------
-module(tivan_server).

-behaviour(gen_server).

-callback init(Args :: list()) -> {'ok', State :: map()}.
-callback handle_info(Info :: term()) -> 'ok'.

-optional_callbacks([handle_info/1]).

-define(NATIVE_TYPES, [binary, list, tuple, atom, integer, float, second, millisecond, microsecond
                      ,map ,nanosecond, uuid, pid, boolean]).

%% API
-export([start_link/4
        ,drop/2
        ,table_defs/1
        ,put/3
        ,put_s/3
        ,get/3
        ,get_s/3
        ,remove/3
        ,remove_s/3
        ,initialize/1]).

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
start_link(Registration, Callback, Arguments, Options) ->
  Server = case Registration of
             {local, Name} -> Name;
             {global, GlobalName} -> GlobalName;
             {via, _Module, ViaName} -> ViaName
           end,
  gen_server:start_link(Registration, ?MODULE, [Callback, Server|Arguments], Options).

drop(Server, Table) ->
  gen_server:cast(Server, {drop, Table}).

table_defs(Server) ->
  gen_server:call(Server, table_defs).

put(Server, Table, Object) ->
  TableDefs = persistent_term:get({Server, table_defs}),
  do_put(Table, Object, TableDefs).

put_s(Server, Table, Object) ->
  gen_server:call(Server, {put, Table, Object}).

get(Server, Table, Options) ->
  TableDefs = persistent_term:get({Server, table_defs}),
  do_get(Table, Options, TableDefs).

get_s(Server, Table, Options) ->
  gen_server:call(Server, {get, Table, Options}).

remove(Server, Table, Object) ->
  TableDefs = persistent_term:get({Server, table_defs}),
  do_remove(Table, Object, TableDefs).

remove_s(Server, Table, Object) ->
  gen_server:call(Server, {remove, Table, Object}).

initialize(Server) ->
  gen_server:cast(Server, initialize).

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
init([Callback, Server|Arguments]) ->
  case Callback:init(Arguments) of
    {ok, TableDefs} ->
      TableDefsU = init_tables(TableDefs),
      persistent_term:put({Server, table_defs}, TableDefsU),
      {ok, #{callback => Callback, init_args => Arguments
            ,server => Server, table_defs => TableDefsU}};
    Other ->
      Other
  end.

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
handle_call(table_defs, _From, #{table_defs := TableDefs} = State) ->
  {reply, TableDefs, State};
handle_call({put, Table, Object}, _From, #{table_defs := TableDefs} = State) ->
  Reply = do_put(Table, Object, TableDefs),
  {reply, Reply, State};
handle_call({get, Table, Options}, _From, #{table_defs := TableDefs} = State) ->
  Reply = do_get(Table, Options, TableDefs),
  {reply, Reply, State};
handle_call({remove, Table, Object}, _From, #{table_defs := TableDefs} = State) ->
  Reply = do_remove(Table, Object, TableDefs),
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
handle_cast({drop, Table}, #{table_defs := TableDefs, server := Server} = State) ->
  tivan:drop(Table),
  TableDefsU = maps:remove(Table, TableDefs),
  persistent_term:put({Server, table_defs}, TableDefsU),
  {noreply, State#{table_defs => TableDefsU}};
handle_cast(initialize, #{callback := Callback
                         ,server := Server
                         ,init_args := Arguments} = State) ->
  case init([Callback, Server|Arguments]) of
    {ok, NewState} ->
      {noreply, NewState};
    {stop, Reason} ->
      {stop, Reason, State}
  end;
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
handle_info(Info, #{callback := Callback} = State) ->
  lager:info("Info received ~p", [Info]),
  case erlang:function_exported(Callback, handle_info, 1) of
    true -> Callback:handle_info(Info);
    false -> ok
  end,
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

%%--------------------------------------------------------------------
%% #{Table => #{columns => #{Column => #{type => binary | list | tuple | atom | integer | float
%%                                               | second | millisecond | microsecond | nanosecond
%%                                               | OtherTable | {OtherTable, Field} | [OtherTable]
%%                                               | [{OtherTable, Field}] | uuid | pid
%%                                               | {any, [Type1, Type2]}
%%                                      ,limit => undefined | Length | {Min, Max} | [Item1, Item2]
%%                                      ,key => false | true
%%                                      ,index => false | true
%%                                      ,unique => false | true
%%                                      ,null => true | false
%%                                      ,default => undefined | Value }}
%%             ,memory => true | false
%%             ,persist => true | false
%%             ,type => set |ordered_set | bag
%%             ,audit => false | true
%%             ,tags => undefined | TagStoreName
%%             ,unique_combo => [] | [{column1, column2}, {... },... ]
%%             ,read_context => SYSCONFIG | async_dirty | transaction | sync_transaction | etc
%%             ,write_context => SYSCONFIG | async_dirty | transaction | sync_transaction | etc
%%             ,mnesia_options => [] | OtherMnesiaOptions}}
%%     ** The first option is the default **
%%--------------------------------------------------------------------

init_tables(TableDefs) ->
  maps:map(
    fun(Table, TableDef) ->
        TableDefU = process_tabledef(TableDef),
        init_table(Table, TableDefU),
        TableDefU
    end,
    TableDefs
   ).

process_tabledef(#{columns := ColumnsNoKey} = TableDef) ->
  {Key, ColumnsNoAudit} = case get_key(ColumnsNoKey) of
                            undefined -> {uuid, ColumnsNoKey#{uuid => #{type => uuid, key => true}}};
                            {K, _KeyDef} -> {K, ColumnsNoKey}
                          end,
  Columns = case maps:get(audit, TableDef, false) of
              false ->
                ColumnsNoAudit;
              true ->
                ColumnsNoAudit#{a_ctime => #{type => integer}
                               ,a_mtime => #{type => integer}}
            end,
  TableDef#{columns => Columns, key => Key}.

init_table(Table, #{columns := ColumnsWithDef} = TableDef) ->
  {Key, _} = get_key(ColumnsWithDef),
  ColumnsRest = maps:fold(
                  fun(Column, #{index := true}, ColumnsAcc) -> [{Column}|ColumnsAcc];
                     (Column, #{unique := true}, ColumnsAcc) -> [{Column}|ColumnsAcc];
                     (Column, _, ColumnsAcc) -> [Column|ColumnsAcc]
                  end,
                  [],
                  maps:remove(Key, ColumnsWithDef)
                 ),
  Columns = [Key|lists:sort(ColumnsRest)],
  Defaults = maps:fold(
               fun(Column, ColumnDef, DefaultsAcc) ->
                   case maps:find(default, ColumnDef) of
                     error -> DefaultsAcc;
                     {ok, Value} -> DefaultsAcc#{Column => Value}
                   end
               end,
               #{},
               ColumnsWithDef
              ),
  case maps:get(tags, TableDef, undefined) of
    undefined ->
      lager:info("No tags assoication needed");
    TagName ->
      lager:info("Creating Tags association ~p", [TagName]),
      tivan_tags:create(TagName)
  end,
  lager:info("Initiaing creation of ~p", [{TableDef, Columns, Defaults}]),
  tivan:create(Table, TableDef#{columns => Columns, defaults => Defaults}).

get_key(Columns) when is_map(Columns) -> get_key(maps:to_list(Columns));
get_key([{Column, #{key := true} = ColumnDef}|_Columns]) -> {Column, ColumnDef};
get_key([_Column|Columns]) -> get_key(Columns);
get_key([]) -> undefined.

do_put(Table, Object, TableDefs) ->
  case maps:find(Table, TableDefs) of
    error ->
      {error, no_definition};
    {ok, #{key := Key} = TableDef} ->
      ObjectWithKey = update_key_curr_object(Object, Table, TableDef),
      ObjectWithAudit = update_audit(ObjectWithKey, TableDef),
      case validate(ObjectWithAudit, Table, TableDef) of
        {ok, ObjectValidated} ->
          Options = case maps:find(write_context, TableDef) of
                      error ->
                        #{};
                      {ok, Context} ->
                        #{context => Context}
                    end,
          KeyValue = tivan:put(Table, ObjectValidated, Options),
          case maps:get(tags, TableDef, undefined) of
            undefined ->
              ok;
            TagStoreName ->
              Tags = maps:get(tags, Object, undefined),
              tag(TagStoreName, KeyValue, Tags)
          end,
          #{Key => KeyValue};
        Error ->
          Error
      end
  end.

update_key_curr_object(Object, Table, #{columns := Columns, key := Key} = TableDef) ->
  KeyDef = maps:get(Key, Columns),
  KeyType = maps:get(type, KeyDef, binary),
  TableType = maps:get(type, TableDef, set),
  case maps:find(Key, Object) of
    error when KeyType == uuid ->
      Value = list_to_binary(uuid:uuid_to_string(uuid:get_v4())),
      Object#{Key => Value};
    {ok, undefined} when KeyType == uuid ->
      Value = list_to_binary(uuid:uuid_to_string(uuid:get_v4())),
      Object#{Key => Value};
    {ok, Value} when TableType /= bag ->
      case tivan:get(Table, Value) of
        [] ->
          Object;
        [ObjectPrev] ->
          maps:merge(ObjectPrev, Object)
      end;
    _ ->
      Object
  end.

update_audit(Object, #{audit := true}) ->
  Now = erlang:system_time(millisecond),
  Object#{a_ctime => maps:get(a_ctime, Object, Now)
         ,a_mtime => Now};
update_audit(Object, _TableDef) -> Object.

validate(Object, Table, #{columns := Columns, key := Key} = TableDef) ->
  case validate(Object, Table, Key, maps:iterator(Columns)) of
    {ok, ObjectU} ->
      UniqueComboList = maps:get(unique_combo, TableDef, []),
      case validate_unique_combo(Object, Table, Key, UniqueComboList) of
        ok -> {ok, ObjectU};
        error -> {error, already_exists}
      end;
    Error ->
      Error
  end.

validate(Object, Table, Key, ColumnsIter) ->
  case maps:next(ColumnsIter) of
    {Column, ColumnDef, ColumnsIterU} ->
      case get_column_value(Object, Column, ColumnDef) of
        error ->
          {error, {Column, not_found}};
        {ok, Value} ->
          KeyValue = maps:get(Key, Object, undefined),
          case validate_unique(Value, Column, ColumnDef, Table, Key, KeyValue) of
            error ->
              {error, {Column, already_exists}};
            ok ->
              case validate_value(Value, ColumnDef, Table, Key, KeyValue) of
                ok ->
                  validate(Object#{Column => Value}, Table, Key, ColumnsIterU);
                Error ->
                  {error, {Column, Error}}
              end
          end
      end;
    none ->
      {ok, Object}
  end.

get_column_value(Object, Column, ColumnDef) ->
  Value = maps:get(Column, Object, undefined),
  get_column_value(Value, ColumnDef).

get_column_value(undefined, #{null := false}) -> error;
get_column_value(undefined, #{unique := true}) -> error;
get_column_value(undefined, #{key := true}) -> error;
get_column_value(undefined, #{default := Value}) -> {ok, Value};
get_column_value(Value, _) -> {ok, Value}.

validate_unique(Value, Column, #{unique := true}, Table, Key, KeyValue) ->
  case tivan:get(Table, #{match => #{Column => Value, Key => {eval, '/=', KeyValue}}}) of
    [] -> ok;
    _ -> error
  end;
validate_unique(_Value, _Column, _ColumnDef, _Table, _Key, _KeyValue) -> ok.

validate_value(undefined, _ColumnDef, _Table, _Key, _KeyValue) -> ok;
validate_value(Value, ColumnDef, Table, Key, KeyValue) ->
  Type = maps:get(type, ColumnDef, binary),
  case validate_type(Value, Type, Table, Key, KeyValue) of
    true ->
      Limit = maps:get(limit, ColumnDef, undefined),
      case validate_limit(Value, Type, Limit) of
        true ->
          ok;
        false ->
          limit_failed
      end;
    false ->
      type_failed
  end.

validate_type(Value, uuid, _Table, _Key, _KeyValue) ->
  uuid:is_v4(uuid:string_to_uuid(binary_to_list(Value)));
validate_type(Value, binary, _Table, _Key, _KeyValue) ->
  is_binary(Value);
validate_type(Value, list, _Table, _Key, _KeyValue) ->
  is_list(Value);
validate_type(Value, tuple, _Table, _Key, _KeyValue) ->
  is_tuple(Value);
validate_type(Value, boolean, _Table, _Key, _KeyValue) ->
  is_boolean(Value);
validate_type(Value, atom, _Table, _Key, _KeyValue) ->
  is_atom(Value);
validate_type(Value, integer, _Table, _Key, _KeyValue) ->
  is_integer(Value);
validate_type(Value, float, _Table, _Key, _KeyValue) ->
  is_float(Value);
validate_type(Value, map, _Table, _Key, _KeyValue) ->
  is_map(Value);
validate_type(Value, pid, _Table, _Key, _KeyValue) ->
  is_pid(Value) andalso is_process_alive(Value);
validate_type(Value, second, _Table, _Key, _KeyValue) when is_integer(Value) ->
  (Value =< erlang:system_time(second)) and (Value > 0);
validate_type(Value, millisecond, _Table, _Key, _KeyValue) when is_integer(Value) ->
  (Value =< erlang:system_time(millisecond)) and (Value > 0);
validate_type(Value, microsecond, _Table, _Key, _KeyValue) when is_integer(Value) ->
  (Value =< erlang:system_time(microsecond)) and (Value > 0);
validate_type(Value, nanosecond, _Table, _Key, _KeyValue) when is_integer(Value) ->
  (Value =< erlang:system_time(nanosecond)) and (Value > 0);
validate_type(Value, {any, Types}, _Table, _Key, _KeyValue) when is_list(Types) ->
  lists:any(
    fun(Type) -> validate_type(Value, Type, _Table, _Key, _KeyValue) end,
    Types
   );
validate_type(Values, [{Table, Field}], _Table, _Key, _KeyValue) when is_list(Values) ->
  lists:all(
    fun(Value) ->
        validate_type(Value, {Table, Field}, _Table, _Key, _KeyValue)
    end,
    Values
   );
validate_type(Value, {Table, Field}, _Table, _Key, _KeyValue) when is_atom(Table), is_atom(Field) ->
  case catch tivan:get(Table, #{match => #{Field => Value}}) of
    {'EXIT', _Reason} -> false;
    [] -> false;
    _ -> true
  end;
validate_type(Values, [Table], _Table, _Key, _KeyValue) when is_list(Values) ->
  lists:all(
    fun(Value) ->
        validate_type(Value, Table, _Table, _Key, _KeyValue)
    end,
    Values
   );
validate_type(Value, Table, Table, _Key, Value) -> false;
validate_type(Value, Table, _Table, _Key, _KeyValue) when is_atom(Table) ->
  case catch tivan:get(Table, Value) of
    {'EXIT', _Reason} -> false;
    [] -> false;
    _ -> true
  end;
validate_type(_Value, _Type, _Table, _Key, _KeyValue) -> false.

validate_limit(_Value, _Type, undefined) -> true;
validate_limit(Value, binary, Size) when is_integer(Size) -> size(Value) =< Size;
validate_limit(Value, tuple, Size) when is_integer(Size) -> size(Value) =< Size;
validate_limit(Value, map, Size) when is_integer(Size) -> map_size(Value) =< Size;
validate_limit(Value, list, Size) when is_integer(Size) -> length(Value) =< Size;
validate_limit(Value, integer, Size) when is_integer(Size) -> Value =< Size;
validate_limit(Value, binary, {re, RegExp}) when is_binary(Value) ->
  case re:run(Value, RegExp) of
    nomatch -> false;
    {match, _} -> true
  end;
validate_limit(Value, binary, {Min, Max}) when is_integer(Min),is_integer(Max) ->
  Size = size(Value),
  (Size >= Min) and (Size =< Max);
validate_limit(Value, tuple, {Min, Max}) when is_integer(Min),is_integer(Max) ->
  Size = size(Value),
  (Size >= Min) and (Size =< Max);
validate_limit(Value, map, {Min, Max}) when is_integer(Min),is_integer(Max) ->
  Size = map_size(Value),
  (Size >= Min) and (Size =< Max);
validate_limit(Value, list, {Min, Max}) when is_integer(Min),is_integer(Max) ->
  Size = length(Value),
  (Size >= Min) and (Size =< Max);
validate_limit(Value, integer, {Min, Max}) when is_integer(Min),is_integer(Max) ->
  (Value >= Min) and (Value =< Max);
validate_limit(Value, _Type, List) when is_list(List) -> lists:member(Value, List);
validate_limit(_Value, _Type, _Limit) -> true.

validate_unique_combo(Object, Table, Key, [ComboTuple|UniqueComboList]) ->
  KeyValue = maps:get(Key, Object, undefined),
  ColumnValueMap = lists:foldl(
                     fun(Column, CVMap) ->
                         CVMap#{Column => maps:get(Column, Object, undefined)}
                     end,
                     #{},
                     tuple_to_list(ComboTuple)
                    ),
  case tivan:get(Table, #{match => ColumnValueMap#{Key => {eval, '/=', KeyValue}}}) of
    [] ->
      validate_unique_combo(Object, Table, Key, UniqueComboList);
    _ ->
      error
  end;
validate_unique_combo(_Object, _Table, _Key, []) -> ok.

tag(TagStoreName, KeyValue, undefined) ->
  tag(TagStoreName, KeyValue, []);
tag(TagStoreName, KeyValue, TagsAfter) ->
  TagsBefore = tivan_tags:tags(TagStoreName, KeyValue),
  [ tivan_tags:untag(TagStoreName, KeyValue, Tag) || Tag <- TagsBefore -- TagsAfter ],
  [ tivan_tags:tag(TagStoreName, KeyValue, Tag) || Tag <- TagsAfter -- TagsBefore ].

do_get(Table, Options, TableDefs) ->
  case maps:find(Table, TableDefs) of
    error ->
      {error, no_definition};
    {ok, TableDef} ->
      do_get_1(Table, Options, TableDef, TableDefs)
  end.

do_get_1(Table, Options, #{columns := ColumnsMap} = TableDef, TableDefs)
  when is_map(Options) ->
  ColumnsToMatch = ['_'|maps:keys(ColumnsMap)],
  OptionsFormatted = interpret_get_options(Options, ColumnsToMatch),
  OptionsForTags = options_for_tags(OptionsFormatted, TableDef),
  OptionsWithContext = case maps:find(read_context, TableDef) of
                         error ->
                           OptionsForTags;
                         {ok, Context} ->
                           OptionsForTags#{context => Context}
                       end,
  Objects = tivan:get(Table, OptionsWithContext),
  ObjectsWithTags = objects_with_tags(OptionsWithContext, Table, TableDef, Objects),
  ObjectsCleanedUp = remove_key_if_not_asked(OptionsFormatted, TableDef, ObjectsWithTags),
  ObjectsExpanded = [ expand(Object, OptionsFormatted, TableDef, TableDefs)
                      || Object <- ObjectsCleanedUp ],
  ObjectsFlattend = flatten(ObjectsExpanded, OptionsFormatted),
  paginate(ObjectsFlattend, OptionsFormatted, Table);
do_get_1(Table, KeyValue, #{key := Key} = TableDef, TableDefs) ->
  do_get_1(Table, #{Key => KeyValue}, TableDef, TableDefs).


interpret_get_options(#{match := _} = Options, _Columns) ->
  Options;
interpret_get_options(Options, Columns) ->
  Match = maps:filter(
            fun(Option, _Value) ->
                lists:member(Option, Columns)
            end,
            Options
           ),
  lists:foldl(
    fun({Alternate, Original}, OptionsAcc) ->
        case maps:find(Original, Options) of
          error ->
            case maps:find(Alternate, Options) of
              error -> OptionsAcc;
              {ok, Value} -> OptionsAcc#{Original => Value}
            end;
          {ok, Value} -> OptionsAcc#{Original => Value}
        end
    end,
    #{match => Match},
    [{'_select',select}
    ,{'_expand', expand}
    ,{'_start', start}
    ,{'_limit', limit}
    ,{'_sort_column', sort_column}
    ,{'_sort_order', sort_order}
    ,{'_cache', cache}
    ,{'_flatten', flatten}]
   ).

options_for_tags(Options, #{key := Key} = TableDef) ->
  TagsDef = maps:get(tags, TableDef, undefined),
  Selects = maps:get(select, Options, []),
  Match = maps:get(match, Options, #{}),
  IsTagSelected = Selects == [] orelse lists:member(tags, Selects),
  IsKeySelected = Selects == [] orelse lists:member(Key, Selects),
  IsTagForMatch = maps:is_key(tags, Match) orelse maps:is_key('_', Match),
  if
    TagsDef =/= undefined
    , (IsTagSelected or IsTagForMatch)
    , not IsKeySelected ->
      Options#{select => [Key|Selects]};
    true ->
      Options
  end.

objects_with_tags(Options, Table, #{key := Key} = TableDef, Objects) ->
  TagsDef = maps:get(tags, TableDef, undefined),
  Selects = maps:get(select, Options, []),
  Match = maps:get(match, Options, #{}),
  IsTagSelected = Selects == [] orelse lists:member(tags, Selects),
  IsTagForMatch = maps:is_key(tags, Match) orelse maps:is_key('_', Match),
  lager:info("TagsDef ~p, Select ~p, Match ~p, IsTagSelected ~p, IsTagForMatch ~p"
             , [TagsDef, Selects, Match, IsTagSelected, IsTagForMatch]),
  case TagsDef of
    undefined ->
      Objects;
    TagStoreName ->
      TagsToSearch = maps:get(tags, Match, []) ++ maps:get('_', Match, []),
      Entities = tivan_tags:entities(TagStoreName, TagsToSearch),
      ObjectsU = if Objects == [], Entities =/= undefined ->
                      tivan:get(Table, Options#{match => maps:remove('_', Match)});
                    true -> Objects end,
      lists:filtermap(
        fun(Object) when IsTagForMatch, Entities =/= undefined ->
            #{Key := KeyValue} = Object,
            case lists:member(KeyValue, Entities) of
              false -> false;
              true when IsTagSelected ->
                Tags = tivan_tags:tags(TagStoreName, KeyValue),
                {true, Object#{tags => Tags}};
              true -> true
            end;
           (Object) when IsTagSelected ->
            #{Key := KeyValue} = Object,
            Tags = tivan_tags:tags(TagStoreName, KeyValue),
            {true, Object#{tags => Tags}};
           (Object) -> Object
        end,
        ObjectsU
       )
  end.

remove_key_if_not_asked(Options, #{key := Key} = TableDef, Objects) ->
  TagsDef = maps:get(tags, TableDef, undefined),
  Selects = maps:get(select, Options, []),
  Match = maps:get(match, Options, #{}),
  IsTagSelected = Selects == [] orelse lists:member(tags, Selects),
  IsKeySelected = Selects == [] orelse lists:member(Key, Selects),
  IsTagForMatch = maps:is_key(tags, Match) orelse maps:is_key('_', Match),
  if
    TagsDef =/= undefined
    , (IsTagSelected or IsTagForMatch)
    , not IsKeySelected ->
      [ maps:remove(Key, O) || O <- Objects ];
    true ->
      Objects
  end.

expand(Object, #{expand := 0}, _TableDef, _TableDefs) -> Object;
expand(Object, #{expand := Level}, TableDef, TableDefs) when is_integer(Level) andalso Level > 0 ->
  maps:map(
    fun(Column, Value) ->
        case TableDef of
          #{columns := #{Column := #{type := ColumnType}}} ->
            ValueExpanded = get_referred_object(ColumnType, Value),
            case maps:find(ColumnType, TableDefs) of
              error -> ValueExpanded;
              {ok, TableDefOfValue} ->
                expand(ValueExpanded, #{expand => Level - 1}, TableDefOfValue, TableDefs)
            end;
          _ ->
            Value
        end
    end,
    Object
   );
expand(Object, _Options, _TableDef, _TableDefs) -> Object.

get_referred_object(_ColumnType, undefined) -> undefined;
get_referred_object(ColumnType, Value) ->
  IsNative = lists:member(ColumnType, ?NATIVE_TYPES),
  case ColumnType of
    _NativeType when IsNative -> Value;
    {any, ColumnTypes} ->
      try
        lists:foldl(
          fun(Type, ValueA) ->
              case get_referred_object(Type, ValueA) of
                Value -> Value;
                ValueExpanded -> throw(ValueExpanded)
              end
          end,
          Value,
          ColumnTypes
         )
      catch
        throw:ValueExpanded -> ValueExpanded
      end;
    [{Table, Field}] when is_list(Value) ->
      lists:flatten([ get_referred_object({Table, Field}, X) || X <- Value ]);
    [Table] when is_list(Value) ->
      lists:flatten([ get_referred_object(Table, X) || X <- Value ]);
    {Table, Field} ->
      case tivan:get(Table, #{match => #{Field => Value}}) of
        [] -> Value;
        [ValueExpanded] -> ValueExpanded#{'_type' => Table};
        ValuesExpanded -> [ X#{'_type' => Table} || X <- ValuesExpanded ]
      end;
    Table ->
      case tivan:get(Table, Value) of
        [] -> Value;
        [ValueExpanded] -> ValueExpanded#{'_type' => Table};
        ValuesExpanded -> [ X#{'_type' => Table} || X <- ValuesExpanded ]
      end
  end.

flatten(Objects, #{flatten := True}) when True == true; True == <<"true">> ->
  [ flatten(Object) || Object <- Objects ];
flatten(Objects, _OptionsU) -> Objects.

flatten(Object) ->
  case maps:to_list(Object) of
    [{_Key, Value}] -> Value;
    _ -> Object
  end.

paginate(Objects, #{limit := Limit} = Options, Table) ->
  Start = maps:get(start, Options, 1),
  Cache = case maps:find(cache, Options) of
            error ->
              initialize_cache(Objects);
            {ok, Id} when is_reference(Id) ->
              case tivan_page:info(Id) of
                undefined ->
                  initialize_cache(Objects);
                _ ->
                  Id
              end
          end,
  case maps:find(sort_column, Options) of
    error -> ok;
    {ok, SortColumn} ->
      SortOrder = case maps:get(sort_order, Options, asc) of
                    desc -> desc;
                    _ -> asc
                  end,
      tivan_page:sort(Cache, {SortColumn, SortOrder})
  end,
  ObjectsLimited = tivan_page:get(Cache, #{start => Start, limit => Limit}),
  #{size := Size} = tivan_page:info(Cache),
  #{Table => ObjectsLimited, cache => Cache, size => Size};
paginate(Objects, #{sort_column := SortColumn} = Options, _Table) ->
  SortFun = case maps:get(sort_order, Options, asc) of
              desc ->
                fun(#{SortColumn := ValueA}, #{SortColumn := ValueB}) ->
                    ValueA > ValueB;
                   (_A, _B) -> true
                end;
              _ ->
                fun(#{SortColumn := ValueA}, #{SortColumn := ValueB}) ->
                    ValueA < ValueB;
                   (_A, _B) -> true
                end
            end,
  lists:sort(SortFun, Objects);
paginate(Objects, _Options, _Table) -> Objects.

initialize_cache(Objects) ->
  Id = tivan_page:new(),
  ok = tivan_page:put(Id, Objects),
  Id.

do_remove(Table, Object, TableDefs) ->
  case maps:find(Table, TableDefs) of
    error ->
      {error, no_definition};
    {ok, TableDef} ->
      do_remove_1(Table, Object, TableDef)
  end.

do_remove_1(Table, Object, #{key := Key} = TableDef) when is_map(Object) ->
  case maps:find(Key, Object) of
    error ->
      {error, no_key};
    {ok, KeyValue} ->
      Options = case maps:find(write_context, TableDef) of
                  error ->
                    #{};
                  {ok, Context} ->
                    #{context => Context}
                end,
      case maps:get(tags, TableDef, undefined) of
        undefined ->
          ok;
        TagStoreName ->
          Tags = tivan_tags:tags(TagStoreName, KeyValue),
          [ tivan_tags:untag(TagStoreName, KeyValue, Tag) || Tag <- Tags ]
      end,
      tivan:remove(Table, KeyValue, Options)
  end;
do_remove_1(Table, KeyValue, #{key := Key} = TableDef) ->
  do_remove_1(Table, #{Key => KeyValue}, TableDef).

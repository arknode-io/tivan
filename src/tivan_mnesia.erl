%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-04-19 18:38:31.676004
%%%-------------------------------------------------------------------
-module(tivan_mnesia).
-export([put/2
        ,put/3
        ,get/1
        ,get/2
        ,remove/2
        ,remove/3]).

-define(LIMIT, 1000).

put(Table, Objects) ->
  Context = application:get_env(tivan, write_context, transaction),
  put(Table, Objects, #{context => Context}).

put(Table, Object, Options) when is_map(Object) ->
  [Key] = put(Table, [Object], Options),
  Key;
put(Table, Objects, #{context := Context}) when is_atom(Table), is_list(Objects) ->
  Attributes = mnesia:table_info(Table, attributes),
  WriteFun = fun() ->
                 lists:map(
                   fun(Object) ->
                       Record = list_to_tuple(
                                  [Table|
                                   lists:map(
                                     fun(Attribute) ->
                                         maps:get(Attribute, Object, undefined)
                                     end,
                                     Attributes
                                    )
                                  ]
                                 ),
                       mnesia:write(Record),
                       element(2, Record)
                   end,
                   Objects
                  )
             end,
  mnesia:activity(Context, WriteFun).

get(Table) ->
  Limit = application:get_env(tivan, default_rows_limit, ?LIMIT),
  get(Table, #{limit => Limit}).

get(Table, #{limit := Limit} = Options) when map_size(Options) == 1 ->
  get(Table, #{start_key => mnesia:dirty_first(Table), limit => Limit});
get(Table, #{start_key := StartKey} = Options) when map_size(Options) == 1 ->
  Limit = application:get_env(tivan, default_rows_limit, ?LIMIT),
  get(Table, #{start_key => StartKey, limit => Limit});
get(Table, #{start_key := StartKey, limit := Limit}) when is_atom(Table), is_integer(Limit) ->
  Context = application:get_env(tivan, read_context, async_dirty),
  GetFunc = fun() ->
                {NKey, Objs} = lists:foldl(
                                 fun(_C, {'$end_of_table', Rs}) ->
                                     {'$end_of_table', Rs};
                                    (_C, {K, Rs}) ->
                                     {mnesia:next(Table, K), mnesia:read(Table, K) ++ Rs}
                                 end,
                                 {StartKey, []},
                                 lists:seq(1, Limit)),
                {NKey, lists:reverse(Objs)}
            end,
  {NextKey, Objects} = mnesia:activity(Context, GetFunc),
  Attributes = mnesia:table_info(Table, attributes),
  {NextKey, objects_to_map(Objects, Attributes, [], #{})};
get(Table, Options) when is_atom(Table), is_map(Options) ->
  Match = maps:get(match, Options, #{}),
  Select = maps:get(select, Options, []),
  Context = maps:get(context, Options, application:get_env(tivan, read_context, async_dirty)),
  Attributes = mnesia:table_info(Table, attributes),
  SelectWithPos = select_with_position(Attributes, Select),
  case maps:find(cont, Options) of
    error ->
      {MatchHead, GuardList} = prepare_mnesia_select(Table, Attributes, Match, Select),
      case maps:find(limit, Options) of
        error ->
          Objects = mnesia:activity(Context, fun mnesia:select/2,
                                    [Table, [{MatchHead, GuardList, ['$_']}]]),
          objects_to_map(Objects, Attributes, SelectWithPos, Match);
        {ok, Limit} ->
          case mnesia:activity(Context, fun mnesia:select/4,
                               [Table, [{MatchHead, GuardList, ['$_']}], Limit, read])of
            '$end_of_table' -> {'$end_of_table', []};
            {Objects, C} ->
              {C, objects_to_map(Objects, Attributes, SelectWithPos, Match)}
          end
      end;
    {ok, Cont} ->
      case mnesia:activity(Context, fun mnesia:select/1, [Cont]) of
        '$end_of_table' -> {'$end_of_table', []};
        {Objects, C} ->
          {C, objects_to_map(Objects, Attributes, SelectWithPos, Match)}
      end
  end;
get(Table, Key) when is_atom(Table) ->
  Context = application:get_env(tivan, read_context, async_dirty),
  Attributes = mnesia:table_info(Table, attributes),
  Objects = mnesia:activity(Context, fun mnesia:read/2, [Table, Key]),
  objects_to_map(Objects, Attributes, [], #{}).

prepare_mnesia_select(Table, Attributes, Match, Select) ->
  {MatchList, GuardList} = prepare_mnesia_select(Attributes, Match, Select, 1, [], []),
  {list_to_tuple([Table|MatchList]), GuardList}.

prepare_mnesia_select([], _Match, _Select, _Ref, MatchList, GuardList) ->
  {lists:reverse(MatchList), GuardList};
prepare_mnesia_select([Attribute|Attributes], Match, Select, Ref, MatchList, GuardList) ->
  case maps:find(Attribute, Match) of
    error ->
      case lists:member(Attribute, Select) of
        false ->
          prepare_mnesia_select(Attributes, Match, Select, Ref, ['_'|MatchList], GuardList);
        true ->
          RefAtom = mk_ref_atom(Ref),
          prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefAtom|MatchList], GuardList)
      end;
    {ok, {eval, OpCode, Value}} when is_atom(OpCode) ->
      RefAtom = mk_ref_atom(Ref),
      Guard = {OpCode, RefAtom, Value},
      prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefAtom|MatchList],
                            [Guard|GuardList]);
    {ok, {Pos, Size, eval, OpCode, Value}} when is_integer(Pos), is_integer(Size), Pos =< Size,
                                                is_atom(OpCode) ->
      RefAtom = mk_ref_atom(Ref),
      RefTuple = erlang:make_tuple(Size, '_', [{Pos, RefAtom}]),
      Guard = {OpCode, RefAtom, Value},
      prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefTuple|MatchList],
                            [Guard|GuardList]);
    {ok, {range, MinValue, MaxValue}} ->
      RefAtom = mk_ref_atom(Ref),
      GuardA = {'>=', RefAtom, MinValue},
      GuardB = {'=<', RefAtom, MaxValue},
      prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefAtom|MatchList],
                            [GuardA, GuardB|GuardList]);
    {ok, {Pos, Size, range, MinValue, MaxValue}} when is_integer(Pos), is_integer(Size),
                                                      Pos =< Size ->
      RefAtom = mk_ref_atom(Ref),
      RefTuple = erlang:make_tuple(Size, '_', [{Pos, RefAtom}]),
      GuardA = {'>=', RefAtom, MinValue},
      GuardB = {'=<', RefAtom, MaxValue},
      prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefTuple|MatchList],
                            [GuardA, GuardB|GuardList]);
    {ok, {Pos, Size, Pre}} when is_integer(Pos),is_integer(Size),is_binary(Pre)
                               andalso binary_part(Pre, {byte_size(Pre), -3}) == <<"...">> ->
      RefAtom = mk_ref_atom(Ref),
      RefTuple = erlang:make_tuple(Size, '_', [{Pos, RefAtom}]),
      StartsWith = binary_part(Pre, {0, byte_size(Pre)-3}),
      GuardA = {'>=', RefAtom, StartsWith},
      GuardB = {'=<', RefAtom, << StartsWith/binary, 255 >>},
      prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefTuple|MatchList],
                            [GuardA, GuardB|GuardList]);
    {ok, {Pos, Size, Value}} when is_integer(Pos),is_integer(Size) ->
      RefTuple = erlang:make_tuple(Size, '_', [{Pos, Value}]),
      prepare_mnesia_select(Attributes, Match, Select, Ref, [RefTuple|MatchList], GuardList);
    {ok, Pre} when is_binary(Pre) andalso binary_part(Pre, {byte_size(Pre), -3}) == <<"...">> ->
      RefAtom = mk_ref_atom(Ref),
      StartsWith = binary_part(Pre, {0, byte_size(Pre)-3}),
      GuardA = {'>=', RefAtom, StartsWith},
      GuardB = {'=<', RefAtom, << StartsWith/binary, 255 >>},
      prepare_mnesia_select(Attributes, Match, Select, Ref + 1, [RefAtom|MatchList],
                            [GuardA, GuardB|GuardList]);
    {ok, Value} ->
      prepare_mnesia_select(Attributes, Match, Select, Ref, [Value|MatchList], GuardList)
  end.

mk_ref_atom(Ref) -> list_to_atom([$$|integer_to_list(Ref)]).

select_with_position(Attributes, Select) ->
  select_with_position(Attributes, Select, 2, []).

select_with_position([], _Select, _Pos, SelectWithPos) -> lists:reverse(SelectWithPos);
select_with_position([Attribute|Attributes], Select, Pos, SelectWithPos) ->
  case lists:member(Attribute, Select) of
    false -> select_with_position(Attributes, Select, Pos + 1 , SelectWithPos);
    true -> select_with_position(Attributes, Select, Pos + 1 , [{Pos, Attribute}|SelectWithPos])
  end.

objects_to_map(Objects, Attributes, SelectWithPos, #{'_' := Pattern}) ->
  ObjectsFiltered = filter_objects(Objects, Pattern),
  objects_to_map(ObjectsFiltered, Attributes, SelectWithPos);
objects_to_map(Objects, Attributes, SelectWithPos, _Match) ->
  objects_to_map(Objects, Attributes, SelectWithPos).

objects_to_map(Objects, Attributes, SelectWithPos) ->
  lists:map(
    fun(Object) when SelectWithPos == [] ->
        maps:from_list(
          lists:zip(Attributes, tl(tuple_to_list(Object)))
         );
       (Object) ->
        maps:from_list(
          lists:map(
            fun({Pos, Attribute}) ->
                {Attribute, element(Pos, Object)}
            end,
            SelectWithPos
           )
         )
    end,
    Objects
   ).

filter_objects(Objects, Pattern) ->
  PatternLower = pattern_lowercase(Pattern),
  lists:filter(
    fun(Object) ->
        ObjectWithoutUndefined = [ X || X <- tuple_to_list(Object), X /= undefined ],
        ObjectBinLower = object_to_lowercase(ObjectWithoutUndefined),
        case binary:match(ObjectBinLower, PatternLower) of
          nomatch -> false;
          _matches -> true
        end
    end,
    Objects
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

remove(Table, Objects) ->
  Context = application:get_env(tivan, write_context, transaction),
  remove(Table, Objects, #{context => Context}).

remove(Table, Object, Options) when is_map(Object) ->
  remove(Table, [Object], Options);
remove(Table, Objects, #{context := Context}) when is_atom(Table), is_list(Objects) ->
  Key = hd(mnesia:table_info(Table, attributes)),
  RemoveFun = fun() ->
                  lists:foreach(
                    fun(#{Key := KeyValue}) ->
                        mnesia:delete({Table, KeyValue})
                    end,
                    Objects
                   )
              end,
  mnesia:activity(Context, RemoveFun).

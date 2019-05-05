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
        ,get/3
        ,remove/2
        ,remove/3]).

-define(LIMIT, 10000).

put(Table, Objects) ->
  Context = application:get_env(tivan, write_context, transaction),
  put(Table, Objects, #{context => Context}).

put(Table, Object, Options) when is_map(Object) ->
  put(Table, [Object], Options);
put(Table, Objects, #{context := Context}) when is_atom(Table), is_list(Objects) ->
  Attributes = mnesia:table_info(Table, attributes),
  WriteFun = fun() ->
                 lists:foreach(
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
                       {ok, element(2, Record)}
                   end,
                   Objects
                  )
             end,
  mnesia:activity(Context, WriteFun).

get(Table) ->
  get(Table, mnesia:dirty_first(Table), ?LIMIT).

get(Table, StartKey, Limit) when is_atom(Table), is_integer(Limit) ->
  Context = application:get_env(tivan, read_context, async_dirty),
  ReadFunc = fun() ->
                 (fun F(0, K, Rs) ->
                      {K, lists:reverse(Rs)};
                    F(_C, '$end_of_table', Rs) ->
                      {'$end_of_table', lists:reverse(Rs)};
                    F(C, K, Rs) ->
                      F(C-1, mnesia:next(Table, K),
                        mnesia:read(Table, K) ++ Rs)
                end)(Limit, StartKey, [])
             end,
  {NextKey, Objects} = mnesia:activity(Context, ReadFunc),
  Attributes = mnesia:table_info(Table, attributes),
  {NextKey, objects_to_map(Objects, Attributes, [], #{})}.

get(Table, Options) when is_atom(Table), is_map(Options) ->
  Match = maps:get(match, Options, #{}),
  Select = maps:get(select, Options, []),
  Context = maps:get(context, Options, application:get_env(tivan, read_context, async_dirty)),
  Attributes = mnesia:table_info(Table, attributes),
  {MatchHead, GuardList} = prepare_mnesia_select(Table, Attributes, Match, Select),
  Objects = mnesia:activity(Context,
                            fun mnesia:select/2, [Table, [{MatchHead, GuardList, ['$_']}]]),
  SelectWithPos = select_with_position(Attributes, Select),
  objects_to_map(Objects, Attributes, SelectWithPos, Match);
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
    {ok, {range, MinValue, MaxValue}} ->
      RefAtom = mk_ref_atom(Ref),
      GuardA = {'>=', RefAtom, MinValue},
      GuardB = {'=<', RefAtom, MaxValue},
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
        ObjectBinLower = object_to_lowercase(Object),
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

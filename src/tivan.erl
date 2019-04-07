-module(tivan).
-export([put/2
        ,put/3
        ,get/1
        ,get/2
        ,get/3
        ,get/4
        ,remove/2
        ,remove/3
        ,update/3
        ,update/4]).

put(Table, Row) ->
  WriteContext = application:get_env(tivan, write_context, transaction),
  put(Table, Row, WriteContext).

put(Table, Row, WriteContext) ->
  Attributes = mnesia:table_info(Table, attributes),
  Record = list_to_tuple(
             [Table|
              lists:map(
                fun(Attribute) ->
                    maps:get(Attribute, Row, undefined)
                end,
                Attributes
               )
             ]
            ),
  mnesia:activity(WriteContext, fun mnesia:write/1, [Record]).

get(Table) when is_atom(Table) ->
  ReadContext = application:get_env(tivan, read_context, transaction),
  get(Table, #{}, [], ReadContext).

get(Table, ReadContext) when is_atom(ReadContext) ->
  get(Table, #{}, [], ReadContext);
get(Table, Match) when is_map(Match) ->
  ReadContext = application:get_env(tivan, read_context, transaction),
  get(Table, Match, [], ReadContext);
get(Table, Select) when is_list(Select) ->
  ReadContext = application:get_env(tivan, read_context, transaction),
  get(Table, #{}, Select, ReadContext).

get(Table, Match, Select) when is_map(Match),is_list(Select) ->
  ReadContext = application:get_env(tivan, read_context, transaction),
  get(Table, Match, Select, ReadContext);
get(Table, Match, ReadContext) when is_map(Match) ->
  get(Table, Match, [], ReadContext);
get(Table, Select, ReadContext) when is_list(Select) ->
  get(Table, #{}, Select, ReadContext).

get(Table, Match, Select, ReadContext) ->
  Attributes = mnesia:table_info(Table, attributes),
  {MatchHead, GuardList} = prepare_mnesia_select(Table, Attributes, Match, Select),
  Objects = mnesia:activity(ReadContext,
                            fun mnesia:select/2, [Table, [{MatchHead, GuardList, ['$_']}]]),
  SelectWithPos = select_with_position(Attributes, Select),
  objects_to_map(Objects, Attributes, SelectWithPos, Match).

prepare_mnesia_select(Table, Attributes, Match) ->
  prepare_mnesia_select(Table, Attributes, Match, []).

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
  ObjectsFiltered = lists:filter(
                      fun(Object) ->
                          ObjectBinLower = object_to_lowercase(Object),
                          PatternLower = pattern_lowercase(Pattern),
                          case binary:match(ObjectBinLower, PatternLower) of
                            nomatch -> false;
                            _matches -> true
                          end
                      end,
                      Objects
                     ),
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

remove(Table, Match) ->
  WriteContext = application:get_env(tivan, write_context, transaction),
  remove(Table, Match, WriteContext).

remove(Table, Match, WriteContext) ->
  Attributes = mnesia:table_info(Table, attributes),
  {MatchHead, GuardList} = prepare_mnesia_select(Table, Attributes, Match),
  RemoveFun = fun() ->
                  Objects = mnesia:select(Table, [{MatchHead, GuardList, ['$_']}]),
                  lists:foreach(
                    fun(Object) ->
                        mnesia:delete({Table, Object})
                    end,
                    Objects
                   ),
                  length(Objects)
              end,
  ObjectsCount = mnesia:activity(WriteContext, RemoveFun),
  {ok, ObjectsCount}.

update(Table, Match, Updates) ->
  WriteContext = application:get_env(tivan, write_context, transaction),
  update(Table, Match, Updates, WriteContext).

update(Table, Match, Updates, WriteContext) ->
  Attributes = mnesia:table_info(Table, attributes),
  UpdatesWithPos = lists:map(
                     fun({Column, Value}) ->
                         Position = case string:str(Attributes, [Column]) of
                                      0 -> throw({invalid_column, Column});
                                      Pos -> Pos + 1
                                    end,
                        {Position, Value}
                     end,
                     maps:to_list(Updates)
                    ),
  {MatchHead, GuardList} = prepare_mnesia_select(Table, Attributes, Match),
  UpdateFun = fun() ->
                  Objects = mnesia:select(Table, [{MatchHead, GuardList, ['$_']}]),
                  lists:foreach(
                    fun(Object) ->
                        ObjectU = lists:foldl(
                                    fun({Pos, Value}, ObjectA) ->
                                        setelement(Pos, ObjectA, Value)
                                    end,
                                    Object,
                                    UpdatesWithPos
                                   ),
                        mnesia:write(ObjectU)
                    end,
                    Objects
                   ),
                  length(Objects)
              end,
  ObjectsCount = mnesia:activity(WriteContext, UpdateFun),
  {ok, ObjectsCount}.


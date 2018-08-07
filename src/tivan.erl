-module(tivan).
-export([add/2, put/2, get/1, get/2, get/3]).

add(Table, Attributes) ->
  mnesia:create_table(Table, [{attributes, Attributes}]).

put(Table, Row) ->
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
  mnesia:activity(transaction, fun mnesia:write/1, [Record]).

get(Table) when is_atom(Table) ->
  get(Table, #{}, []).

get(Table, Match) when is_map(Match) ->
  get(Table, Match, []);
get(Table, Select) when is_list(Select) ->
  get(Table, #{}, Select).

get(Table, Match, Select) ->
  Attributes = mnesia:table_info(Table, attributes),
  {MatchHead, GuardList} = prepare_mnesia_select(Table, Attributes, Match, Select),
  Objects = mnesia:activity(transaction,
                            fun mnesia:select/2, [Table, [{MatchHead, GuardList, ['$_']}]]),
  SelectWithPos = select_with_position(Attributes, Select),
  objects_to_map(Objects, Attributes, SelectWithPos, Match).

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

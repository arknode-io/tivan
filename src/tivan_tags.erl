%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-06-20 21:35:56.736576
%%%-------------------------------------------------------------------
-module(tivan_tags).

%% API
-export([create/1
        ,tag/3
        ,untag/3
        ,tags/2
        ,entities/2]).


%%%===================================================================
%%% API
%%%===================================================================

create(Name) ->
  tivan:create(Name, #{columns => [tag_entity, created_on]
                       ,type => ordered_set}).

tag(Name, Entity, TagUnknownCase) ->
  Tag = string:uppercase(TagUnknownCase),
  case tivan:get(Name, {Tag, Entity}) of
    [] ->
      Now = erlang:system_time(second),
      tivan:put(Name, #{tag_entity => {Tag, Entity}, created_on => Now});
    _ ->
      {Tag, Entity}
  end.

untag(Name, Entity, TagUnknownCase) ->
  Tag = string:uppercase(TagUnknownCase),
  tivan:remove(Name, {Tag, Entity}).

tags(Name, Entity) ->
  TagEntities = tivan:get(Name, #{match => #{tag_entity => {2, 2, Entity}}
                                   ,select => [tag_entity]}),
  [ T || #{tag_entity := {T, _}} <- TagEntities ].


entities(Name, Tags) when is_list(Tags) ->
  case lists:filtermap(
         fun(TagUnknownCase) ->
             Tag = string:uppercase(TagUnknownCase),
             case tivan:get(Name, #{match => #{tag_entity => {1, 2, Tag}}
                                    , select => [tag_entity]}) of
               [] -> false;
               TagEntities -> {true, [ E || #{tag_entity := {_, E}} <- TagEntities ]}
             end
         end,
         Tags
        ) of
    [] -> undefined;
    [EntityGroup|EntityGroups] ->
      lists:filter(
        fun(Entity) ->
            lists:all(
              fun(OtherEntityGroup) ->
                  lists:member(Entity, OtherEntityGroup)
              end,
              EntityGroups
             )
        end,
        EntityGroup
       )
  end;
entities(Name, Tag) -> entities(Name, [Tag]).


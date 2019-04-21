-module(tivan).
-export([put/2
        ,put/3
        ,get/1
        ,get/2
        ,get/3
        ,remove/2
        ,remove/3
        ,update/3
        ,update/4]).

put(Table, Row) ->
  tivan_mnesia:put(Table, Row).

put(Table, Row, Options) ->
  tivan_mnesia:put(Table, Row, Options).

get(Table) ->
  tivan_mnesia:get(Table).

get(Table, Options) ->
  tivan_mnesia:get(Table, Options).

get(Table, StartKey, Limit) ->
  tivan_mnesia:get(Table, StartKey, Limit).

remove(Table, Match) ->
  tivan_mnesia:remove(Table, Match).

remove(Table, Match, Options) ->
  tivan_mnesia:remove(Table, Match, Options).

update(Table, Match, Updates) ->
  tivan_mnesia:update(Table, Match, Updates).

update(Table, Match, Updates, Options) ->
  tivan_mnesia:update(Table, Match, Updates, Options).

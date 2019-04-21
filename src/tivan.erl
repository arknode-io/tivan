%%%-------------------------------------------------------------------
%%% @author danny
%%% @copyright (C) 2019, danny
%%% @doc
%%%
%%% @end
%%% Created : 2019-04-19 18:38:31.676004
%%%-------------------------------------------------------------------
-module(tivan).
-export([put/2
        ,put/3
        ,get/1
        ,get/2
        ,get/3
        ,remove/2
        ,remove/3]).

put(Table, ObjectOrObjects) ->
  tivan_mnesia:put(Table, ObjectOrObjects).

put(Table, ObjectOrObjects, Options) ->
  tivan_mnesia:put(Table, ObjectOrObjects, Options).

get(Table) ->
  tivan_mnesia:get(Table).

get(Table, Options) ->
  tivan_mnesia:get(Table, Options).

get(Table, StartKey, Limit) ->
  tivan_mnesia:get(Table, StartKey, Limit).

remove(Table, ObjectOrObjects) ->
  tivan_mnesia:remove(Table, ObjectOrObjects).

remove(Table, ObjectOrObjects, Options) ->
  tivan_mnesia:remove(Table, ObjectOrObjects, Options).

%%%-------------------------------------------------------------------
%% @doc tivan top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(tivan_sup).

-behaviour(supervisor).

-define(CHILD(ID), #{id => ID, start => {ID, start_link, []}}).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

init([]) ->
  SupervisorFlags = #{strategy => one_for_one,
                      intensity => 25,
                      period => 60},
  ChildSpecs = [?CHILD(tivan_schema)],
  {ok, {SupervisorFlags, ChildSpecs}}.

%%====================================================================
%% Internal functions
%%====================================================================

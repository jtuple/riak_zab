
-module(riak_zab_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(I, Type), {I, {I, start_link, []}, permanent, 5000, Type, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    ProcessSup = ?CHILD(riak_zab_process_sup, supervisor),
    Process = ?CHILD(riak_zab_process, worker),
    EnsembleMaster = ?CHILD(riak_zab_ensemble_master, worker),

    Processes = lists:flatten([ProcessSup, Process, EnsembleMaster]),

    {ok, { {one_for_one, 5, 10}, Processes} }.

-module(riak_zab_process_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).
-export([start_vnode/2]).

start_vnode(Mod, Index) when is_integer(Index) -> 
    supervisor:start_child(?MODULE, [Mod, Index]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Restart ensembles if riak_zab_process_sup is restarting after error.
    %% case whereis(riak_zab_ensemble_master) of
    %%     undefined ->
    %%         ok;
    %%     _Pid ->
    %%         timer:apply_after(500, gen_server, cast,
    %%                           [riak_zab_ensemble_master, restart_ensembles])
    %% end,
    {ok, {{one_for_one, 5, 10}, []}}.

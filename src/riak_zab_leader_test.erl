-module(riak_zab_leader_test).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).

setup_zab() ->
    Nodes = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    net_kernel:start(['test@127.0.0.1']),
    erlang:set_cookie(node(), riak_zab),
    mock_ensemble(Nodes),
    Nodes.

cleanup_zab(Nodes) ->
    [rpc:cast(N, riak_zab_ensemble_master, zab_down, [[]]) || N <- Nodes],
    ep:stop(Nodes),
    unload_mocks(),
    ok.

start_zab(Nodes) ->
    [begin
         rpc:call(N, riak_zab_ensemble_master, zab_up, [[]], 75),
         timer:sleep(250)
     end || N <- Nodes].

get_mock_ensembles(Size) ->
    GroupF = fun(Prefs, Groups) ->
                     {_Indicies, Nodes} = lists:unzip(Prefs),
                     Key = lists:usort(Nodes),
                     case dict:is_key(Key, Groups) of
                         false ->
                             dict:store(Key, [Prefs], Groups);
                         true ->
                             dict:append_list(Key, [Prefs], Groups)
                     end
             end,
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    AllPrefs = riak_core_ring:all_preflists(Ring, Size),
    PrefGroups = lists:foldl(GroupF, dict:new(), AllPrefs),
    L = dict:to_list(PrefGroups),
    E1 = ['dev1@127.0.0.1', 'dev2@127.0.0.1', 'dev3@127.0.0.1'],
    [{Key, Prefs} || {Key, Prefs} <- L, Key == E1].

mock_ensemble(Nodes) ->
    [rpc:call(N, ?MODULE, mock_ensemble, [], 75) || N <- Nodes],
    ok.
mock_ensemble() ->
    code:unstick_mod(riak_zab_ensemble_util),
    meck:new(riak_zab_ensemble_util, [passthrough, no_link]),
    meck:expect(riak_zab_ensemble_util, all_ensembles, 
                fun(Size) -> get_mock_ensembles(Size) end).

unload_mocks(Nodes) ->
    [rpc:call(N, ?MODULE, unload_mocks, [], 75) || N <- Nodes],
    ok.
unload_mocks() ->
    meck:unload(riak_zab_ensemble_util).

test_initialize(Nodes) ->
    mock_ensemble(),
    ep:load_expectations(?MODULE, initialize_expect, [], Nodes),
    start_zab(Nodes),
    timer:sleep(2000),
    ?assert(ep:check_expectations(Nodes)).
initialize_expect() ->
    ep:new(),

    %% Nodes start correctly
    ep:once([], 'dev1@127.0.0.1', initialize, ep:wait()),
    ep:once([], 'dev2@127.0.0.1', initialize, ep:wait()),
    ep:once([], 'dev3@127.0.0.1', initialize, ep:wait()),

    ep:start(),
    ok.

test_initial_election(Nodes) ->
    ep:load_expectations(?MODULE, initial_election_expect, [], Nodes),
    ep:release(Nodes),
    timer:sleep(7000),
    ?assert(ep:check_expectations(Nodes)).
initial_election_expect() ->
    ep:clear(),

    %% Nodes all start election
    S1 = ep:once([], 'dev1@127.0.0.1', start_election),
    S2 = ep:once([], 'dev2@127.0.0.1', start_election),
    S3 = ep:once([], 'dev3@127.0.0.1', start_election),
    
    %% Nodes elect dev3
    ep:once([S1], 'dev1@127.0.0.1', {elected, 'dev3@127.0.0.1'}),
    ep:once([S2], 'dev2@127.0.0.1', {elected, 'dev3@127.0.0.1'}),

    %% Saving dev3's ensemble peer pid in order to kill it in the next test
    ep:once([S3], 'dev3@127.0.0.1', {elected, 'dev3@127.0.0.1'},
            fun() ->
                    ep:put(leader, self())
            end),

    ep:start(),
    ok.

test_kill_leader(Nodes) ->
    ep:load_expectations(?MODULE, kill_leader_expect, [], Nodes),

    %% Let's kill dev3 to kick off the expected behavior
    {ok, Pid} = ep:get('dev3@127.0.0.1', leader),
    exit(Pid, kill),

    timer:sleep(10000),
    ?assert(ep:check_expectations(Nodes)).
kill_leader_expect() ->
    ep:clear(),

    %% dev3 should restart, which we delay to ensure dev2 is elected.
    _I3 = ep:once([], 'dev3@127.0.0.1', initialize, ep:wait()),

    %% dev1/2 should start a new election, and elect dev2
    S1 = ep:once([], 'dev1@127.0.0.1', start_election),
    S2 = ep:once([], 'dev2@127.0.0.1', start_election),

    ep:once([S1], 'dev1@127.0.0.1', {elected, 'dev2@127.0.0.1'}),
    ep:once([S2], 'dev2@127.0.0.1', {elected, 'dev2@127.0.0.1'}),

    ep:start(),
    ok.

test_elect_existing(Nodes) ->
    ep:load_expectations(?MODULE, elect_existing_expect, [], Nodes),
    ep:release(Nodes),
    timer:sleep(7000),
    ?assert(ep:check_expectations(Nodes)).
elect_existing_expect() ->
    %% dev3 will start election and elect existing leader dev2
    S3 = ep:once([], 'dev3@127.0.0.1', start_election),
    ep:once([S3], 'dev3@127.0.0.1', {elected, 'dev2@127.0.0.1'}),

    ep:start(),
    ok.

run_timed(Nodes, TestFn, Time) ->
    {timeout, Time, ?_test(TestFn(Nodes))}.

leader_test_() ->
    {setup,
     fun setup_zab/0,
     fun cleanup_zab/1,
     fun(Nodes) ->
             {inorder,
              [run_timed(Nodes, fun test_initialize/1, 20000),
              run_timed(Nodes, fun test_initial_election/1, 20000),
              run_timed(Nodes, fun test_kill_leader/1, 20000),
              run_timed(Nodes, fun test_elect_existing/1, 20000)]}
     end}.

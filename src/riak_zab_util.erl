-module(riak_zab_util).
-export([command/3, command/4, sync_command/3]).

%% API
command(Preflist, Msg, VMaster) ->
    command(Preflist, Msg, noreply, VMaster).
command(Key, Msg, Sender, VMaster) ->
    Preflist = get_preflist(Key),
    riak_zab_ensemble_master:command(Preflist, Msg, Sender, VMaster).

sync_command(Key, Msg, VMaster) ->
    Preflist = get_preflist(Key),
    riak_zab_ensemble_master:sync_command(Preflist, Msg, VMaster).

%% @private
get_preflist(Key) ->
    Idx = chash:key_of(Key),
    ESize = riak_zab_ensemble_master:get_ensemble_size(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Preflist = preflist(Idx, ESize, Ring),
    Preflist.

%% @private
preflist(Key, N, Ring) ->
    Preflist = riak_core_ring:preflist(Key, Ring),
    {Preflist2, _} = lists:split(N, Preflist),
    Preflist2.

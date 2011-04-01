-module(riak_zab_ensemble_util).
-export([all_ensembles/1]).

-type index() :: non_neg_integer().
-type preflist() :: [{index(), node()}].
-type ensemble() :: {[node()], [preflist()]}.

%% Determine the set of ensembles. Currently, there is one ensemble of each
%% unique set of preflist owning nodes.
-spec all_ensembles(pos_integer()) -> [ensemble()].
all_ensembles(Size) ->
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
    dict:to_list(PrefGroups).

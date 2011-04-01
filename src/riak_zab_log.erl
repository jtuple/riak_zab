-module(riak_zab_log).
-compile(export_all).

-define(BC, riak_zab_bitcask_backend).

-record(synclog, {sync_log :: term(),
                  sync_idx :: non_neg_integer()}).

%-define(DOUT(Msg, Args), io:format(Msg, Args)).
-define(DOUT(Msg, Args), true).

%% History API
init_history_log(VG) ->
    <<P:128/integer>> = crypto:md5(term_to_binary(VG)),
    {ok, BC} = ?BC:start(P, [{data_root, "data/zab_bitcask"}]),
    F = fun({propose, Zxid}, _V, {MaxP, MaxC}) ->
                NewP = erlang:max(Zxid, MaxP),
                {NewP, MaxC};
	   ({last_commit, _Prefs}, Zxid, {MaxP, MaxC}) ->
                NewC = erlang:max(binary_to_term(Zxid), MaxC),
                {MaxP, NewC};
           ({_B, _K}, _V, {MaxP, MaxC}) ->
                {MaxP, MaxC}
	end,
    {LastProposedZxid, LastCommittedZxid} = ?BC:fold(BC, F, {{0,0}, {0,0}}),
    LastZxid = erlang:max(LastProposedZxid, LastCommittedZxid),
    {LastZxid, LastCommittedZxid, BC}.

get_or_default(BC, BKey, Default) ->
    case ?BC:get(BC, BKey) of
        {ok, Value} ->
            binary_to_term(Value);
        _ ->
            Default
    end.

get_old_proposals(BC, LastCommittedZxid) ->
    ?BC:fold(BC,
             fun({propose, Zxid}, V, L) -> 
                     case Zxid > LastCommittedZxid of
                         true -> [{Zxid, binary_to_term(V)} | L];
                         false -> L
                     end;
                ({_B, _K}, _V, L) ->
                     L
             end,
             []).

sync(BC={Ref,_}) ->
    bitcask:sync(Ref),
    BC.

log_proposal(Zxid, Msg, BC) ->
    ok = ?BC:put(BC, {propose, Zxid}, term_to_binary(Msg)),
    sync(BC).

get_proposed_keys(BC) ->
    ?BC:list_bucket(BC, propose).

get_proposal(Zxid, BC) ->
    {ok, Value} = ?BC:get(BC, {propose, Zxid}),
    binary_to_term(Value).

del_proposal(Zxid, BC) ->
    ok = ?BC:delete(BC, {propose, Zxid}),
    sync(BC).

log_accepted_epoch(BC, Epoch) ->
    ok = ?BC:put(BC, {meta, accepted_epoch}, term_to_binary(Epoch)),
    sync(BC).

log_current_epoch(BC, Epoch) ->
    ok = ?BC:put(BC, {meta, current_epoch}, term_to_binary(Epoch)),
    sync(BC).

log_last_commit(Preflist, Zxid, BC) ->
    ok = ?BC:put(BC, {last_commit, Preflist}, term_to_binary(Zxid)),
    sync(BC).

get_accepted_epoch(BC) ->
    get_or_default(BC, {meta, accepted_epoch}, 0).
get_current_epoch(BC) ->
    get_or_default(BC, {meta, current_epoch}, 0).
get_last_commit(Preflist, BC) ->
    get_or_default(BC, {last_commit, Preflist}, {0,0}).

clear_proposals(BC) ->
    Keys = get_proposed_keys(BC),
    [?BC:delete(BC, Key) || Key <- Keys],
    sync(BC).

init_sync_log(VG) ->
    <<P:128/integer>> = crypto:md5(term_to_binary(VG)),
    {ok, BC} = ?BC:start(P, [{data_root, "data/zab_sync_bitcask"}]),
    SyncIdx = ?BC:fold(BC,
                       fun({_B,K}, _V, Max) -> erlang:max(K, Max) end,
                       0),
    #synclog{sync_log=BC, sync_idx=SyncIdx}.

log_sync_message(Msg, SyncLog=#synclog{sync_log=BC, sync_idx=Idx}) ->
    Idx2 = Idx + 1,
    ?BC:put(BC, {<<"sync">>, Idx2}, term_to_binary(Msg)),
    ?DOUT("Sync: Logging ~p~n", [{Idx2, Msg}]),
    SyncLog#synclog{sync_idx=Idx2}.

clear_sync_log(SyncLog=#synclog{sync_log=BC, sync_idx=Idx}) ->
    [?BC:delete(BC, {<<"sync">>, N}) || N <- lists:seq(1,Idx)],
    SyncLog#synclog{sync_idx=0}.

fold_sync_log(SyncLog=#synclog{}, Fun, Acc) ->
    fold_sync_log(1, SyncLog, Fun, Acc).

fold_sync_log(Idx, #synclog{sync_idx=N}, _Fun, Acc) when Idx > N ->
    Acc;
fold_sync_log(Idx, SyncLog=#synclog{sync_log=BC}, Fun, Acc) ->
    {ok, Value} = ?BC:get(BC, {<<"sync">>, Idx}),
    Msg = binary_to_term(Value),
    ?DOUT("Sync: Handling ~p~n", [{Idx, Msg}]),
    Acc2 = Fun(Msg, Acc),
    fold_sync_log(Idx + 1, SyncLog, Fun, Acc2).

%% Old History API
%% init_history_log() ->
%%     {{0,0}, {0,0}, dict:new()}.

%% get_old_proposals(#state{history=History,
%%                       last_committed_zxid=LastCommittedZxid}) ->
%%     lists:keysort(1, dict:to_list(dict:filter(fun(K,_V) -> (K > LastCommittedZxid) end, History))).

%% log_proposal(Zxid, Msg, History) ->
%%     dict:store(Zxid, Msg, History).

%% get_proposal(Zxid, History) ->
%%     dict:fetch(Zxid, History).


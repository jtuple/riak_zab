-module(riak_zab_vnode).
-export([command/3, sync_command/3, standard_sync/4, reply/2]).

-include_lib("riak_core/include/riak_core_vnode.hrl").
-include_lib("riak_zab_vnode.hrl").

%-define(DOUT(Msg, Args), io:format(Msg, Args)).
-define(DOUT(Msg, Args), true).

command(VNode, Req, VMaster) ->
    riak_core_vnode_master:command(VNode, Req, VMaster).

sync_command(VNode, Req, VMaster) ->
    riak_core_vnode_master:sync_command(VNode, Req, VMaster).

standard_sync(Mod, State, Peer, Idxs) ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Mod:handle_command(?FOLD_REQ{foldfun=fun sync_item/3,
				 acc0={Mod, Peer, Idxs, Ring}},
		       none, State).

sync_item(K, V, Acc={Mod, Peer, Idxs, Ring}) ->
    BKey = Mod:hash_key(K),
    {Idx, _INode} = hd(riak_core_ring:preflist(BKey, Ring)),

    case ordsets:is_element(Idx, Idxs) of
	true ->
	    ?DOUT("Standard sync :: sending ~p~n", [{K,V}]),
	    riak_zab_peer:send_sync_message(Peer, ?ZAB_SYNC_DATA{data={K,V}}),
	    Acc;
	false ->
	    ?DOUT("Standard sync :: skipping ~p~n", [{K,V}]),
	    ?DOUT("    K: ~p~n", [K]),
	    ?DOUT(" BKey: ~p~n", [BKey]),
	    ?DOUT("  Idx: ~p~n", [Idx]),
	    ?DOUT(" Idxs: ~p~n", [Idxs]),
	    Acc
    end.

reply({fsm_sync, undefined, From}, Reply) ->
    gen_fsm:reply(From, Reply);
reply(Sender, Reply) ->
    riak_core_vnode:reply(Sender, Reply).


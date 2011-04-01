-module(riak_zab_ensemble_master).
-behaviour(gen_server).
-include_lib("riak_zab_peer.hrl").

%% API
-export([start_link/0]).
-export([command/3, command/4, sync_command/3, get_ensemble_size/0]).

%% API for riak-zab-admin
-export([zab_up/1, zab_down/1, zab_info/1]).

%% API used by riak_zab_peer
-export([get_peers/1]).

-export([ring_changed/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-type index() :: non_neg_integer().
-type preflist() :: [{index(), node()}].
-type ensemble() :: {[node()], [preflist()]}.

-record(state, {esize :: pos_integer(),
                quorum :: pos_integer(),
                ringhash :: binary(),
                ensembles :: tid(),
                metabc :: term()}).

-define(BC, riak_zab_bitcask_backend).

%-define(DOUT(Msg, Args), io:format(Msg, Args)).
-define(DOUT(Msg, Args), true).

%% API
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec command(preflist(), term(), atom()) -> ok | {error, term()}.
command(Preflist, Msg, VMaster) ->
    command(Preflist, Msg, noreply, VMaster).

-spec command(preflist(), term(), zsender(), atom()) -> ok | {error, term()}.
command(Preflist, Msg, Sender, _VMaster) ->
    Ensemble = ensemble(Preflist),
    {ENodes, _Prefs} = Ensemble,
    Leader = ensemble_leader(ENodes),
    case Leader of
        none ->
            {error, ensemble_down};
        {_LNode, LPid} ->
            Proposal = make_proposal(Preflist, Sender, Msg),
            ?DOUT("Broadcast to ensemble: ~p :: ~p~n", [ENodes, Leader]),
            gen_fsm:send_event(LPid, Proposal),
            ok
    end.

-spec sync_command(preflist(), term(), atom()) -> {ok, term()} | {error, term()}.
sync_command(Preflist, Msg, _VMaster) ->    
    Ensemble = ensemble(Preflist),
    {ENodes, _Prefs} = Ensemble,
    Leader = ensemble_leader(ENodes),
    case Leader of
        none ->
            {error, ensemble_down};
        {_LNode, LPid} ->
            Sender = {fsm_sync, undefined, undefined},
            Proposal = make_proposal(Preflist, Sender, Msg),
            ?DOUT("Broadcast to ensemble: ~p :: ~p~n", [ENodes, Leader]),
            gen_fsm:sync_send_event(LPid, Proposal)
    end.

-spec get_ensemble_size() -> pos_integer().
get_ensemble_size() ->
    case application:get_env(riak_zab, ensemble_size) of
        {ok, Num} ->
            Num;
        _ ->
            error_logger:warning_msg("Ensemble_size unset, defaulting to 3~n",
                                     []),
            3
    end.

%% API used by riak-zab-admin
-spec zab_up([]) -> ok.
zab_up([]) ->
    gen_server:cast(?MODULE, zab_up).

-spec zab_down([]) -> ok.
zab_down([]) ->
    gen_server:cast(?MODULE, zab_down).

-spec zab_info([]) -> ok.
zab_info([]) ->
    ESize = get_ensemble_size(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    RingSize = riak_core_ring:num_partitions(Ring),
    Ens = riak_zab_ensemble_util:all_ensembles(ESize),
    EInfo = [{Nodes, Prefs, ensemble_leader(Nodes)} || {Nodes, Prefs} <- Ens],
    io:format("~32..=s Riak Zab Info ~32..=s~n", ["", ""]),
    io:format("Ring size:     ~B~n", [RingSize]),
    io:format("Ensemble size: ~B~n", [ESize]),
    io:format("Nodes:         ~p~n", [riak_core_ring:all_members(Ring)]),
    io:format("~34..=s Ensembles ~34..=s~n", ["", ""]),
    io:format("~8s   ~6s   ~-30s ~s~n",
              ["Ensemble", "Ring", "Leader", "Nodes"]),
    io:format("~79..-s~n", [""]),
    lists:foldl(fun zab_ensemble_info/2, {RingSize, 1}, EInfo),
    ok.

%% @private
zab_ensemble_info({Nodes, Prefs, Leader}, {RingSize, Num}) ->
    RingPercent = erlang:length(Prefs) * 100 / RingSize,
    LeaderOut = case Leader of
                    none -> "(down)";
                    {Node,_} -> Node
                end,
    io:format("~8B   ~5.1f%   ~-30.30s ~p~n",
              [Num, RingPercent, LeaderOut, Nodes]),
    io:format("~79..-s~n", [""]),
    {RingSize, Num + 1}.

%% API used by riak-zab-peer
%% Returns the pid for the riak_zab_peer processes associated with an ensemble
-spec get_peers([node()]) -> [{node(), pid()}].
get_peers(Ensemble) ->
    {Peers, _} = gen_server:multi_call(Ensemble, ?MODULE,
                                       {get_peer, Ensemble}, 100),
    Peers.

%% Called by riak_zab_ring_handler on ring change.
ring_changed() ->
    gen_server:cast(?MODULE, ring_changed).

%% @private
init(_Args) ->
    ESize = get_ensemble_size(),
    Quorum = erlang:trunc(ESize / 2 + 1),
    RingHash = get_ring_hash(),
    {ok, BC} = ?BC:start(0, [{data_root, "data/zab_meta_bitcask"}]),
    timer:apply_after(500, gen_server, cast, [?MODULE, restart_ensembles]),
    State=#state{esize=ESize,
                 quorum=Quorum,
                 ringhash=RingHash,
                 ensembles=ets:new(ensemble_tbl, []),
                 metabc=BC},
    {ok, State}.

handle_cast(ring_changed, State=#state{ringhash=RingHash}) ->
    case get_ring_hash() of
        RingHash ->
            ?DOUT("Ring update. Same.~n", []),
            {noreply, State};
        NewRingHash ->
            ?DOUT("Ring update. Changed~n", []),
            stop_all_ensembles(),
            {noreply, State#state{ringhash=NewRingHash}}
    end;
handle_cast(zab_up, State) ->
    put_zab_ring_hash(get_ring_hash(), State),
    ensure_ensembles_started(State),
    {noreply, State};
handle_cast(zab_down, State) ->
    %% Delete ring hash so zab won't restart on node failure.
    del_zab_ring_hash(State),
    stop_all_ensembles(),
    {noreply, State};
handle_cast(restart_ensembles, State) ->
    RingHash = get_ring_hash(),
    ZabRingHash = get_zab_ring_hash(State),
    case RingHash == ZabRingHash of
        true ->
            ensure_ensembles_started(State),
            {noreply, State};
        false ->
            %% Delete zab hash so that future restart attempts do not
            %% succeed given a future qualifying ring update. We want
            %% the user to manually restart zab if restart ever fails.
            del_zab_ring_hash(State),
            {noreply, State}
    end;
handle_cast({ensemble_all_state_event, Ensemble, Event}, State) ->
    ensemble_all_state_event(Ensemble, Event),
    {noreply, State};
handle_cast(_Req, State) ->
    {noreply, State}.

handle_call({ensemble, Preflist}, _From, State=#state{ensembles=Ens}) ->
    Reply = case ets:lookup(Ens, Preflist) of
                [] ->
                    {[], []};
                [{Preflist,Val}] ->
                    Val
            end,
    {reply, Reply, State};
handle_call({get_peer, Ensemble}, _From, State) ->
    Pid = riak_zab_process:get_pid({peer, Ensemble}),
    {reply, Pid, State};
handle_call({ensemble_sync_all_state_event, Ensemble, Event}, From, State) ->
    spawn(fun() ->
                  Reply = ensemble_sync_all_state_event(Ensemble, Event),
                  case Reply of
                      error -> noreply;
                      Value -> gen_server:reply(From, Value)
                  end
          end),
    {noreply, State}.
%%handle_call(_Req, _From, State) ->
%%    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
make_proposal(Preflist, Sender, Msg) ->
    #riak_zab_proposal{preflist=Preflist,
                       sender=Sender,
                       message=Msg}.

%% @private
ensemble_leader([]) ->
    ?DOUT("M: ensemble_leader :: empty enodes~n", []),
    none;
ensemble_leader(ENodes) ->
    Quorum = quorum_size(ENodes),
    Msg = {ensemble_sync_all_state_event, ENodes, get_leader},
    R = gen_server:multi_call(ENodes, ?MODULE, Msg, 1000),
    ?DOUT("M: ensemble_leader results: ~p~n", [R]),
    case R of
        {[], _} ->
            none;
        {Replies, _} ->
            {_Nodes, Leaders} = lists:unzip(Replies),
            Counts = count_elements(Leaders),
            Valid = [Leader || {Leader, Count} <- Counts, Count >= Quorum],
            case Valid of
                [] ->
                    none;
                [Leader|_] ->
                    Leader
            end
    end.

%% Routes a gen_fsm all_state_event to a local ensemble peer.
%% @private
ensemble_all_state_event(Ensemble, Event) ->
    Pid = riak_zab_process:get_pid({peer, Ensemble}),
    ensemble_all_state_event(Pid, Ensemble, Event).
ensemble_all_state_event(no_match, _Ensemble, _Event) ->
    ok;
ensemble_all_state_event(Pid, _Ensemble, Event) ->
    gen_fsm:send_all_state_event(Pid, Event).

%% Routes a gen_fsm sync_all_state_event to a local ensemble peer.
%% @private
ensemble_sync_all_state_event(Ensemble, Event) ->
    Pid = riak_zab_process:get_pid({peer, Ensemble}),
    ensemble_sync_all_state_event(Pid, Ensemble, Event).
ensemble_sync_all_state_event(no_match, _Ensemble, _Event) ->
    error;
ensemble_sync_all_state_event(Pid, _Ensemble, Event) ->
    gen_fsm:sync_send_all_state_event(Pid, Event).

%% @private
quorum_size([_]) ->
    1;
quorum_size(Ensemble) ->
    ESize = erlang:max(ordsets:size(Ensemble), get_ensemble_size()),
    erlang:trunc(ESize / 2 + 1).

%% @private
count_elements(L) ->
    Counts = lists:foldl(fun(E, Acc) ->
                                 dict:update_counter(E, 1, Acc)
                         end, dict:new(), L),
    dict:to_list(Counts).

%% @private
-spec ensemble(preflist()) -> ensemble().
ensemble(Preflist) ->
    %% {_, L} = lists:unzip(Preflist),
    %% lists:sort(L).
    gen_server:call(?MODULE, {ensemble, Preflist}).

%% @private
get_ring_hash() ->
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    crypto:md5(term_to_binary(Ring)).

%% @private
really_start_ensemble(App, Quorum, {Nodes, Prefs}) ->
    ?DOUT("Starting ensemble ~p~n", [{Nodes,Prefs}]),
    riak_zab_process:start_process({peer, Nodes}, riak_zab_peer,
                                   [App, Quorum, Nodes, Prefs]).

%% @private
start_ensemble(App, Quorum, Ensemble={Nodes, _Prefs}) ->
    Size = ordsets:size(Nodes),
    case {Size == 1, Size < Quorum} of
        {true, _} ->
            error_logger:warning_msg("Starting ensemble of size 1. Consistent "
                                     "but not highly available.~n", []),
            really_start_ensemble(App, 1, Ensemble);
        {false, true} ->
            error_logger:warning_msg("Ensemble can never meet quorum. "
                                     "Skipping.~n", []);
        {false, false} ->
            really_start_ensemble(App, Quorum, Ensemble)
    end.

%% @private
ensure_ensembles_started(State) ->
    case application:get_env(riak_zab, application_vnode) of
        {ok, App} ->
            ensure_ensembles_started(App, State);
        undefined ->
            error_logger:error_msg("Unable to start zab, no application vnode "
                                   "defined.~n")
    end.

%% @private
add_ensemble_to_index(Ensemble={_Nodes, Prefs}, State) ->
    [ets:insert(State#state.ensembles, {P, Ensemble}) || P <- Prefs].

%% @private
ensure_ensembles_started(App, State=#state{esize=ESize, quorum=Quorum}) ->
    Ensembles = riak_zab_ensemble_util:all_ensembles(ESize),
    [begin
         add_ensemble_to_index(Ensemble, State),
         ShouldStart = ordsets:is_element(node(), Nodes),
         ShouldStart andalso start_ensemble(App, Quorum, Ensemble)
     end || Ensemble={Nodes,_} <- Ensembles].

%% @private
stop_all_ensembles() ->
    Keys = riak_zab_process:all_processes(peer),
    [riak_zab_process:stop_process(K) || K <- Keys].

%% @private
put_zab_ring_hash(RingHash, #state{metabc=BC}) ->
    ok = ?BC:put(BC, {<<"meta">>, <<"ringhash">>}, RingHash).

%% @private
del_zab_ring_hash(#state{metabc=BC}) ->
    ok = ?BC:delete(BC, {<<"meta">>, <<"ringhash">>}).

%% @private
get_zab_ring_hash(#state{metabc=BC}) ->
    case ?BC:get(BC, {<<"meta">>, <<"ringhash">>}) of
        {ok, Value} ->
            Value;
        _ ->
            <<>>
    end.

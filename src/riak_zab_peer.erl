-module(riak_zab_peer).
-behaviour(gen_fsm).
-include_lib("riak_zab_peer.hrl").

%% API
-export([start_link/1]).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3,
         handle_sync_event/4]).

%% API used by election FSM
-export([node_state/1, elected/3, election_event/3]).
-export([send_sync_message/2]).

%% Shared leader/follower states
-export([initialize/2, looking/2]).
%% Leader states
-export([leading/2, leading/3, lead_new_quorum/2, lead_new_epoch/2,
         lead_synchronize/2]).
%% Follower states
-export([following/2, follow_new_quorum/2, follow_new_epoch/2,
         follow_synchronize/2]).

%% Type definitions
-type index() :: non_neg_integer().
-type preflist() :: [{index(), node()}].
%%-type ensemble() :: {[node()], [preflist()]}.
-type zxid() :: {non_neg_integer(), non_neg_integer()}.
-type allzxids() :: [{preflist(), [{index(), zxid()}]}].
-type peer() :: {node(), pid()}.

-record(state, {node_state,
                electmod,
                electfsm,
                timer,
                ping_timer,
                ping_acks,
                id,
                app,
                quorum,
                ensemble,
                synchronize_pid,
                peerinfo,
                peers,
                preflists,
                history,
                sync_log,
                last_zxid,
                last_commit_zxid,
                followers0,
                followers,
                accepted_epoch,
                largest_epoch,
                current_epoch,
                proposals,
                leader}).

-record(serverinfo, {from,
                     current_epoch,
                     accepted_epoch,
                     zxid}).

-include_lib("riak_zab_vnode.hrl").
-include_lib("eprobe/include/eprobe.hrl").

-define(TIMEOUT, 4000).
-define(PINGTIME, 1000).

-define(INFO, error_logger:info_msg).

%-define(DOUT(Msg, Args), io:format(Msg, Args)).
-define(DOUT(Msg, Args), true).

%% API
start_link(Args) ->
    gen_fsm:start_link(?MODULE, Args, []).

%% API used by riak_zab_vnode synchronization
-spec send_sync_message({index(), pid()}, term()) -> ok.
send_sync_message({Idx, Pid}, Msg) ->
    gen_fsm:send_event(Pid, {sync, Idx, Msg}).

%% API used by election FSM
-spec node_state(pid()) -> looking | leading | following.
node_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, node_state).

-spec election_event(any(), peer(), term()) -> ok.
election_event(_Ensemble, {_Node, Pid}, Event) ->
    gen_fsm:send_all_state_event(Pid, {election, Event}).

-spec elected(pid(), peer(), zxid()) -> ok.
elected(Pid, Leader, Zxid) ->
    gen_fsm:send_event(Pid, {elected, Leader, Zxid}).

%% @private
init([App, Quorum, Ensemble, Prefs]) ->
    ElectMod = riak_zab_fast_election_fsm,
    {LastZxid, LastCommittedZxid, History} =
        riak_zab_log:init_history_log(Ensemble),
    SyncLog = riak_zab_log:init_sync_log(Ensemble),
    AcceptedEpoch = riak_zab_log:get_accepted_epoch(History),
    CurrentEpoch = riak_zab_log:get_current_epoch(History),

    ?INFO("~p: riak_zab_peer started~n"
          "  Quorum:         ~p~n"
          "  Ensemble:       ~p~n"
          "  Last Zxid:      ~p~n"
          "  Laxt Commit:    ~p~n"
          "  Current Epoch:  ~p~n"
          "  Accepted Epoch: ~p~n",
          [self(), Quorum, Ensemble, LastZxid, LastCommittedZxid,
           CurrentEpoch, AcceptedEpoch]),
    %timer:sleep(2000),

    %% Ensure history sanity
    {LastEpoch, _} = LastZxid,
    true = (LastCommittedZxid =< LastZxid),
    true = (CurrentEpoch =< AcceptedEpoch),
    true = (LastEpoch =< CurrentEpoch),

    State = #state{node_state=looking,
                   electmod=ElectMod,
                   electfsm=undefined,
                   timer=none,
                   ping_timer=none,
                   ping_acks=ordsets:new(),
                   id=get_id(),
                   app=App,
                   quorum=Quorum,
                   ensemble=Ensemble,
                   peerinfo = ets:new(peertable, []),
                   peers = [],
                   preflists=Prefs,
                   history=History,
                   sync_log = SyncLog,
                   followers0=[],
                   followers=[],
                   last_zxid=LastZxid,
                   last_commit_zxid=LastCommittedZxid,
                   proposals=dict:new(),
                   accepted_epoch=AcceptedEpoch,
                   largest_epoch=0,
                   current_epoch=CurrentEpoch},
    {ok, initialize, State, 0}.

initialize(timeout, State=#state{electmod=ElectMod, ensemble=Ens, quorum=Q,
                                 last_zxid=LastZxid}) ->
    ?PROBE(node(), initialize),
    {ok, Pid} = ElectMod:start_link(self(), LastZxid, Ens, Q),
    State2 = State#state{electfsm=Pid},
    State3 = play_sync_log(State2),
    start_election(State3).

looking(timeout, State) ->
    start_election(State);
looking(cancel, State) ->
    start_election(State);
looking({elected, Leader, Zxid}, State) ->
    monitor_peer(Leader, State),
    State2 = restart_timer(State),
    ?PROBE(node(), {elected, element(1,Leader)}),
    case Leader == get_id() of
        true ->
            ?INFO("riak_zab_peer: elected myself :: ~p~n", [[Leader, Zxid]]),
            case State#state.quorum of
                1 ->
                    %% Quorum of one. Move straight to leading/broadcast.
                    Epoch = State#state.accepted_epoch + 1,
                    State3 = restart_timer(State2),
                    State4 = restart_ping_timer(State3),
                    State5 = update_accepted_epoch(Epoch, State4),
                    State6 = update_current_epoch(Epoch, State5),
                    State7 = State6#state{leader=Leader,
                                          node_state=leading,
                                          followers=[get_id()],
                                          last_zxid={Epoch, 0},
                                          proposals=dict:new()},
                    {next_state, leading, State7};
                _ ->
                    State3 = State2#state{node_state=leading,
                                          leader=Leader,
                                          followers=[get_id()]},
                    {next_state, lead_new_quorum, State3}
            end;
        false ->
            ?INFO("riak_zab_peer: elected ~p~n", [[Leader, Zxid]]),
            State3 = State2#state{node_state=following, leader=Leader},
            {next_state, follow_new_quorum, State3, 0}
    end;
looking(_Event, State) ->
    {next_state, looking, State}.

%% @private
wait_for_quorum(Peer, StateName, NextStateName,
                State=#state{quorum=Quorum, followers=Followers}) ->
    Followers2 = ordsets:add_element(Peer, Followers),
    case length(Followers2) >= Quorum of
        true ->
            State2 = State#state{followers0=Followers2, followers=[]},
            State3 = restart_timer(State2),
            ?MODULE:NextStateName(init, State3);
        false ->
            State2 = State#state{followers=Followers2},
            {next_state, StateName, State2}
    end.

lead_new_quorum(cancel, State) ->
    ?INFO("L: ~p :: Timed out while establishing quorum~n", [self()]),
    start_election(State);
lead_new_quorum(#serverinfo{from=From,
                            current_epoch=CurrentEpoch,
                            accepted_epoch=AcceptedEpoch,
                            zxid=FollowerZxid},
                State=#state{current_epoch=LeaderEpoch,
                             largest_epoch=LargestEpoch,
                             last_zxid=LastZxid}) ->
    monitor_peer(From, State),
    peer_send_event(State, From, msg_serverinfo(State)),
    case valid_follower_epoch(LeaderEpoch, LastZxid,
                              CurrentEpoch, FollowerZxid) of
        true ->
            LargestEpoch2 = erlang:max(AcceptedEpoch, LargestEpoch),
            State2 = State#state{largest_epoch=LargestEpoch2},
            wait_for_quorum(From, lead_new_quorum, lead_new_epoch, State2);
        false ->
            {next_state, lead_new_quorum, State}
    end;
lead_new_quorum(_Event, State) ->
    {next_state, lead_new_quorum, State}.

lead_new_epoch(cancel, State) ->
    ?INFO("L: ~p :: Timed out while establishing epoch~n", [self()]),
    start_election(State);
lead_new_epoch(init, State=#state{followers0=Followers,
                                  largest_epoch=LargestEpoch}) ->
    ?INFO("L: ~p :: New quorum established :: ~p~n", [self(), Followers]),
    NewEpochMsg = {newepoch, get_id(), LargestEpoch+1},
    [peer_send_event(State, N, NewEpochMsg) || N <- Followers, N /= get_id()],
    {next_state, lead_new_epoch, State#state{followers=[get_id()]}};
lead_new_epoch({ack, From, {Epoch,0}}, State)
  when (Epoch == (State#state.largest_epoch+1)) ->
    wait_for_quorum(From, lead_new_epoch, lead_synchronize, State);
lead_new_epoch(_Event, State) ->
    {next_state, lead_new_epoch, State}.

lead_synchronize(cancel, State) ->
    ?INFO("L: ~p :: Timed out while trying to synchronize~n", [self()]),
    start_election(State);
lead_synchronize(init, State=#state{largest_epoch=LargestEpoch,
                                    followers0=Followers0}) ->
    Epoch = LargestEpoch + 1,
    ?INFO("L: ~p :: New epoch (~p) established :: ~p~n",
          [self(), Epoch, Followers0]),
    State2 = update_accepted_epoch(Epoch, State),

    ?INFO("L: ~p :: Starting synchronization~n", [self()]),
    Proposals = get_old_proposals(State2),
    LeaderAllZxids = get_all_zxids(State2),

    %% Synchronize in parallel
    SyncFn =
        fun(N) ->
                synchronize_peer(LeaderAllZxids, Proposals, N, Epoch, State2)
        end,
    SyncPid =
        spawn(fun() -> [spawn_link(fun() -> SyncFn(N) end) || N <- Followers0,
                                                              N /= get_id()]
              end),

    State3 = State2#state{synchronize_pid=SyncPid, followers=[get_id()]},
    {next_state, lead_synchronize, State3};
lead_synchronize({ack, From, Zxid}, State=#state{accepted_epoch=Epoch})
  when (Zxid == {Epoch,0}) ->
    wait_for_quorum(From, lead_synchronize, leading, State);
lead_synchronize(_Event, State) ->
    {next_state, lead_synchronize, State}.

leading(init, State=#state{followers0=Followers0, accepted_epoch=Epoch}) ->
    ?INFO("L: ~p :: leading ; followers: ~p~n", [self(), Followers0]),
    [peer_send_event(State, N, {uptodate, get_id()})
     || N <- Followers0, N /= get_id()],
    State2 = restart_timer(State),
    State3 = restart_ping_timer(State2),
    State4 = update_current_epoch(Epoch, State3),
    {next_state, leading, State4#state{followers=Followers0,
                                       last_zxid={Epoch, 0},
                                       proposals=dict:new()}};
leading(cancel, State=#state{ping_acks=Acks}) ->
    Quorum = State#state.quorum,
    case length(Acks) >= Quorum of
        true ->
            State2 = restart_timer(State),
            {next_state, leading, State2#state{ping_acks=ordsets:new()}};
        false ->
            ?INFO("L: ~p :: Timed out while leading~n", [self()]),
            start_election(State)
    end;
leading(ping, State=#state{followers=Followers}) ->
    [peer_send_event(State, N, {ping, get_id()}) || N <- Followers],
    State2 = restart_ping_timer(State),
    {next_state, leading, State2};
leading({ackping, From}, State=#state{ping_acks=Acks}) ->
    Acks2 = ordsets:add_element(From, Acks),
    {next_state, leading, State#state{ping_acks=Acks2}};
leading(Event=?ZAB_PROPOSAL{}, State) ->
    {ok, State2} = handle_broadcast(Event, State),
    {next_state, leading, State2};
leading(Event={ping, _From}, State) ->
    {ok, State2} = handle_ping(Event, State),
    {next_state, leading, State2};
leading(Event={propose, _From, _Zxid, _Msg}, State) ->
    {ok, State2} = handle_propose(Event, State),
    {next_state, leading, State2};
leading(Event={commit, _From, _Zxid}, State) ->
    {ok, State2} = handle_commit(Event, State),
    {next_state, leading, State2};
leading(#serverinfo{from=From}, State=#state{current_epoch=Epoch,
                                             followers=Followers}) ->
    monitor_peer(From, State),
    peer_send_event(State, From, msg_serverinfo(State)),
    peer_send_event(State, From, {newepoch, get_id(), Epoch}),
    synchronize_peer(From, Epoch, State),
    peer_send_event(State, From, {uptodate, get_id()}),
    ?INFO("L: ~p :: Synchronized with new follower: ~p~n", [self(), From]),
    Followers2 = ordsets:add_element(From, Followers),
    {next_state, leading, State#state{followers=Followers2}};
leading({ack, From, Zxid}, State=#state{proposals=Proposals,
                                        ping_acks=Acks,
                                        followers=Followers}) ->
    Quorum = State#state.quorum,
    Acks2 = ordsets:add_element(From, Acks),
    State2 = State#state{ping_acks=Acks2},
    case dict:find(Zxid, Proposals) of
        {ok, Count} ->
            Count2 = Count + 1,
            case Count2 of
                Quorum ->
                    ?DOUT("L: Received enough ACK(~p), sending COMMIT~n",
                          [Zxid]),
                    [peer_send_event(State, N, {commit, get_id(), Zxid})
                     || N <- Followers],
                    Proposals2 = dict:erase(Zxid, Proposals),
                    {next_state, leading, State2#state{proposals=Proposals2}};
                _ ->
                    ?DOUT("L: Received ACK(~p), still need more~n", [Zxid]),
                    Proposals2 = dict:store(Zxid, Count2, Proposals),
                    {next_state, leading, State2#state{proposals=Proposals2}}
            end;
        error ->
            {next_state, leading, State}
    end;
leading(_Event, State) ->
    {next_state, leading, State}.

leading(Event=?ZAB_PROPOSAL{sender={fsm_sync, undefined, undefined}},
        From, State) ->
    Event2 = Event?ZAB_PROPOSAL{sender={fsm_sync, undefined, From}},
    {ok, State2} = handle_broadcast(Event2, State),
    {next_state, leading, State2};
leading(_Event, _From, State) ->
    {next_state, leading, State}.

follow_new_quorum(cancel, State) ->
    ?INFO("F: ~p :: Timed out while establishing quorum~n", [self()]),
    start_election(State);
follow_new_quorum(timeout, State=#state{leader=Leader}) ->
    peer_send_event(State, Leader, msg_serverinfo(State)),
    {next_state, follow_new_quorum, State, 1000};
follow_new_quorum(#serverinfo{}, State) ->
    State2 = restart_timer(State),
    {next_state, follow_new_epoch, State2};
follow_new_quorum(_Event, State) ->
    {next_state, follow_new_quorum, State}.

follow_new_epoch(cancel, State) ->
    ?INFO("F: ~p :: Timed out while establishing epoch~n", [self()]),
    start_election(State);
follow_new_epoch({newepoch, From, Epoch},
                 State=#state{accepted_epoch=AccEpoch}) ->
    if
        Epoch > AccEpoch ->
            %% Accept the new epoch proposal
            State2 = restart_timer(State),
            peer_send_event(State, From, {ack, get_id(), {Epoch,0}}),
            State3 = update_accepted_epoch(Epoch, State2),
            {next_state, follow_synchronize, State3};
        Epoch < AccEpoch ->
            %% Abandon the leader
            ?INFO("F: ~p :: Abandoning leader (new epoch)~n", [self()]),
            start_election(State);
        Epoch == AccEpoch ->
            %% Move into synchronize state without ACKing proposal
            State2 = restart_timer(State),
            {next_state, follow_synchronize, State2}
    end;
follow_new_epoch(_Event, State) ->
    {next_state, follow_new_epoch, State}.

follow_synchronize(cancel, State) ->
    ?INFO("F: ~p :: Timed out while trying to synchronize~n", [self()]),
    start_election(State);
follow_synchronize(Msg={sync, _Idx, _SyncMsg}, State) ->
    State2 = log_sync_message(Msg, State),
    {next_state, follow_synchronize, State2};
follow_synchronize(Msg={diff, _From, _Preflist, _Zxid}, State) ->
    State2 = log_sync_message(Msg, State),
    {next_state, follow_synchronize, State2};
follow_synchronize(Msg={oldpropose, _From, _Zxid, _Msg}, State) ->
    State2 = log_sync_message(Msg, State),
    {next_state, following, State2};
follow_synchronize(Msg={newleader, From, _Epoch}, State) ->
    State2 = log_sync_message(Msg, State),
    State3 = play_sync_log(State2),
    Epoch2 = State3#state.current_epoch,
    peer_send_event(State3, From, {ack, get_id(), {Epoch2, 0}}),
    {next_state, follow_synchronize, State3};
follow_synchronize({uptodate, _From}, State) ->
    State2 = restart_timer(State),
    ?INFO("F: ~p :: Moving to following/broadcast~n", [self()]),
    State3 = commit_all_proposals(State2),
    {next_state, following, State3};
follow_synchronize(_Event, State) ->
    {next_state, follow_synchronize, State}.

handle_sync_message({sync, Idx, Msg}, State=#state{app=App}) ->
    ok = riak_core_vnode_master:sync_command({Idx, node()}, Msg, App),
    State;
handle_sync_message({diff, _From, Preflist, Zxid},
                    State=#state{history=Hist,
                                 last_commit_zxid=LastCommit}) ->
    Hist2 = riak_zab_log:log_last_commit(Preflist, Zxid, Hist),
    LastCommit2 = erlang:max(LastCommit, Zxid),
    State#state{last_zxid=LastCommit2, last_commit_zxid=LastCommit2,
                history=Hist2};
handle_sync_message({oldpropose, _From, Zxid, Msg},
                    State=#state{history=History}) ->
    History2 = riak_zab_log:log_proposal(Zxid, Msg, History),
    {ok, State#state{history=History2, last_zxid=Zxid}};
handle_sync_message({newleader, _From, Epoch},
                    State=#state{last_zxid=LastZxid}) ->
    LastZxid2 = erlang:max(LastZxid, {Epoch, 0}),
    State2 = update_current_epoch(Epoch, State),
    State2#state{last_zxid=LastZxid2}.

following(cancel, State) ->
    ?INFO("F: ~p :: Timed out while following~n", [self()]),
    start_election(State);
following(Event={ping, _From}, State) ->
    {ok, State2} = handle_ping(Event, State),
    State3 = restart_timer(State2),
    {next_state, following, State3};
following(Event={propose, _From, _Zxid, _Msg}, State) ->
    {ok, State2} = handle_propose(Event, State),
    State3 = restart_timer(State2),
    {next_state, following, State3};
following(Event={commit, _From, _Zxid}, State) ->
    {ok, State2} = handle_commit(Event, State),
    {next_state, following, State2};
following(_Event, State) ->
    {next_state, following, State}.

valid_follower_epoch(LeaderEpoch, LeaderZxid, FollowerEpoch, FollowerZxid) ->
    (FollowerEpoch =< LeaderEpoch)
        orelse ((FollowerEpoch == LeaderEpoch)
                andalso (FollowerZxid < LeaderZxid)).

log_sync_message(Msg, State=#state{sync_log=SyncLog}) ->
    SyncLog2 = riak_zab_log:log_sync_message(Msg, SyncLog),
    State#state{sync_log=SyncLog2}.

%% @private
peer_send_event(#state{ensemble=Ensemble}, Node, Msg) ->
    peer_send_event(Ensemble, Node, Msg);
peer_send_event(_Ensemble, {_Node, Pid}, Msg) ->
    gen_fsm:send_event(Pid, Msg).

%% @private
peer_all_sync_event(_Ensemble, {_Node, Pid}, Event) ->
    gen_fsm:sync_send_all_state_event(Pid, Event).

%% @private
get_id() ->
    {node(), self()}.

%% @private
monitor_peer({_Node, no_match}, _State) ->
    ok;
monitor_peer({Node, Pid}, State) ->
    PInfo = State#state.peerinfo,
    case ets:match_object(PInfo, {Node, Pid, '_'}) of
        [] ->
            %% Start monitoring peer
            M = erlang:monitor(process, Pid),
            ets:insert(PInfo, {Node, Pid, M});
        _ ->
            %% Already monitoring peer
            ok
    end.

%% @private
remove_peer(Pid, State=#state{peerinfo=PInfo,
                              followers0=Followers0,
                              followers=Followers}) ->
    case ets:match_object(PInfo, {'_', Pid, '_'}) of
        [{Node, Pid, _}] ->
            ?DOUT("Removing peer ~p~n", [{Node, Pid}]),
            ets:delete(PInfo, Node),
            NFollowers0 = ordsets:del_element(Node, Followers0),
            NFollowers = ordsets:del_element(Node, Followers),
            Peers = ets:foldl(fun({N,P,_},L) -> [{N,P}|L] end, [], PInfo),
            State#state{followers0=NFollowers0,
                        followers=NFollowers,
                        peers=Peers};
        [] ->
            State
    end.

%% @private
start_election(State=#state{ensemble=Ensemble,
                            peerinfo=PInfo,
                            quorum=Quorum,
                            last_zxid=LastZxid,
                            synchronize_pid=SyncPid}) ->
    cancel_timer(State#state.ping_timer),
    catch exit(SyncPid, kill),
    ?PROBE(node(), start_election),
    ?DOUT("M: Looking for ensemble peers~n  ~p~n", [Ensemble]),
    Peers = riak_zab_ensemble_master:get_peers(Ensemble),
    ?DOUT("Peers: ~p~n", [Peers]),
    [monitor_peer(Peer, State) || Peer <- Peers],
    AllPeers = ets:foldl(fun({N,P,_},L) -> [{N,P}|L] end, [], PInfo),
    ?DOUT("AllPeers: ~p~n", [AllPeers]),
    SyncLog2 = riak_zab_log:clear_sync_log(State#state.sync_log),
    State2 = State#state{sync_log=SyncLog2},
    State3 = restart_timer(State2),
    case length(AllPeers) >= Quorum of
        true ->
            ?INFO("~p: starting election~n", [self()]),
            StartMsg = {start_election, AllPeers, LastZxid},
            State4 = handle_election_event(StartMsg, State3),
            {next_state, looking, State4#state{node_state=looking,
                                               peers=AllPeers,
                                               followers0=[],
                                               followers=[]}};
        false ->
            {next_state, looking, State3#state{node_state=looking,
                                               peers=AllPeers}}
    end.

%% @private
play_sync_log(State=#state{history=History, sync_log=SyncLog}) ->
    History2 = riak_zab_log:clear_proposals(History),
    State2 = State#state{history=History2},
    State3 = riak_zab_log:fold_sync_log(SyncLog,
                                        fun handle_sync_message/2,
                                        State2),
    SyncLog2 = riak_zab_log:clear_sync_log(SyncLog),
    State3#state{sync_log=SyncLog2}.

cancel_timer(none) ->
    ok;
cancel_timer(T) ->
    gen_fsm:cancel_timer(T).
restart_timer(State=#state{timer=T}) ->
    cancel_timer(T),
    T2 = gen_fsm:send_event_after(?TIMEOUT, cancel),
    State#state{timer=T2}.
restart_ping_timer(State=#state{ping_timer=T}) ->
    cancel_timer(T),
    T2 = gen_fsm:send_event_after(?PINGTIME, ping),
    State#state{ping_timer=T2}.

msg_serverinfo(#state{current_epoch=CurrentEpoch,
                      accepted_epoch=AcceptedEpoch,
                      last_zxid=LastZxid}) ->
    #serverinfo{from=get_id(),
                current_epoch=CurrentEpoch,
                accepted_epoch=AcceptedEpoch,
                zxid=LastZxid}.

handle_broadcast(Msg=?ZAB_PROPOSAL{}, State=#state{last_zxid={ZEpoch, Zxid},
                                                   followers=Followers,
                                                   proposals=Proposals}) ->
    Zxid2 = {ZEpoch, Zxid+1},
    ?DOUT("L: Sending PROPOSE(~p)~n", [Zxid2]),
    ?DOUT("  Followers: ~p~n", [Followers]),
    [peer_send_event(State, N, {propose, get_id(), Zxid2, Msg})
     || N <- Followers],
    Proposals2 = dict:store(Zxid2, 0, Proposals),
    State2 = State#state{last_zxid=Zxid2, proposals=Proposals2},
    {ok, State2}.

handle_ping({ping, From}, State) when From == State#state.leader ->
    peer_send_event(State, From, {ackping, get_id()}),
    {ok, State};
handle_ping(_Event, State) ->
    {ok, State}.

handle_propose({propose, From, Zxid, Msg}, State=#state{history=History})
  when From == State#state.leader ->
    ?DOUT("F: Received PROPOSE(~p)~n", [Zxid]),
    ?DOUT("F: Sending ACK(~p)~n", [Zxid]),
    History2 = riak_zab_log:log_proposal(Zxid, Msg, History),
    peer_send_event(State, From, {ack, get_id(), Zxid}),
    {ok, State#state{history=History2, last_zxid=Zxid}};
handle_propose(_Event, State) ->
    {ok, State}.

commit_all_proposals(State=#state{history=History}) ->
    Proposals = lists:sort(riak_zab_log:get_proposed_keys(History)),
    lists:foldl(fun do_commit/2, State, Proposals).

do_commit(Zxid, State=#state{history=History, app=App, leader=Leader}) ->
    Proposal = riak_zab_log:get_proposal(Zxid, History),
    ?ZAB_PROPOSAL{preflist=Preflist,
                  sender=Sender,
                  message=Msg} = Proposal,
    LeaderVN = ensemble_leader_vnode(Preflist, Leader),
    LocalVN = [{Idx, Node} || {Idx, Node} <- Preflist, Node == node()],
    Req = ?ZAB_REQ{zxid=Zxid, req=Msg, sender=Sender, leading=false},
    [begin
         Leading = (VN == LeaderVN),
         Req2 = Req?ZAB_REQ{leading=Leading},
         ok = riak_core_vnode_master:sync_command(VN, Req2, App, 1000)
     end || VN <- LocalVN],

    History2 = riak_zab_log:log_last_commit(Preflist, Zxid, History),
    History3 = riak_zab_log:del_proposal(Zxid, History2),
    State#state{history=History3, last_commit_zxid=Zxid}.

handle_commit({commit, From, Zxid}, State) when From == State#state.leader ->
    ?DOUT("F: Received COMMIT(~p)~n", [Zxid]),
    State2 = do_commit(Zxid, State),
    {ok, State2};
handle_commit(_Event, State) ->
    {ok, State}.

handle_election_event(Event, State) ->
    gen_fsm:send_event(State#state.electfsm, Event),
    State.

handle_all_election_event(Event, State) ->
    gen_fsm:send_all_state_event(State#state.electfsm, Event),
    State.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, StateName, State) ->
    State2 = remove_peer(Pid, State),
    case State#state.leader of
        {_, Pid} ->
            ?INFO("~p: Leader is down, abandoning.~n", [self()]),
            start_election(State2);
        _ ->
            {next_state, StateName, State2}
    end;
handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

handle_event({election, ElectionEvent}, StateName, State) ->
    State2 = handle_all_election_event(ElectionEvent, State),
    {next_state, StateName, State2};
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(get_leader, _From, StateName,
                  State=#state{leader=Leader, node_state=NState}) ->
    Reply = case NState of
                leading -> Leader;
                following -> Leader;
                _Other -> none
            end,
    {reply, Reply, StateName, State};
handle_sync_event(all_zxids, _From, StateName, State) ->
    Zxids = get_all_zxids(State),
    {reply, Zxids, StateName, State};
handle_sync_event(node_state, _From, StateName, State) ->
    {reply, State#state.node_state, StateName, State};
handle_sync_event(last_zxid, _From, StateName, State) ->
    {reply, State#state.last_zxid, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% Select a vnode as the leader vnode for a preflist.
%% For now, select the first vnode owned by the leader.
%% @private
ensemble_leader_vnode(Preflist, {LNode, _LPid}) ->
    %% We sort the preflist to be safe about ordering.
    L = lists:keysort(1, Preflist),
    VNodes = [VN || VN={_,Node} <- L, Node == LNode],
    hd(VNodes).

get_old_proposals(#state{history=Hist, last_commit_zxid=LastCommit}) ->
    riak_zab_log:get_old_proposals(Hist, LastCommit).

update_accepted_epoch(Epoch, State=#state{history=Hist}) ->
    Hist2 = riak_zab_log:log_accepted_epoch(Hist, Epoch),
    State#state{accepted_epoch=Epoch, history=Hist2}.

update_current_epoch(Epoch, State=#state{history=Hist}) ->
    Hist2 = riak_zab_log:log_current_epoch(Hist, Epoch),
    State#state{current_epoch=Epoch, history=Hist2}.

synchronize_peer(Follower, Epoch, State) ->
    LeaderAllZxids = get_all_zxids(State),
    Proposals = get_old_proposals(State),
    synchronize_peer(LeaderAllZxids, Proposals, Follower, Epoch, State).

synchronize_peer(LeaderAllZxids, Proposals, Follower, Epoch, State) ->
    Id = State#state.id,
    synchronize_vnodes(Follower, LeaderAllZxids, State),
    ?DOUT("L: Sending old proposals to ~p~n", [Follower]),
    [peer_send_event(State, Follower, {oldpropose, Id, Zxid, Msg})
     || {Zxid, Msg} <- Proposals],
    peer_send_event(State, Follower, {newleader, Id, Epoch}).

%% For all preflists owned by this ensemble peer, return the last committed
%% zxid for each partition index owned by this node.
-spec get_all_zxids(#state{}) -> allzxids().
get_all_zxids(State=#state{preflists=Preflists}) ->
    [{Preflist, get_zxids(Preflist, State)} || Preflist <- Preflists].

%% Given a preflist, return the last committed zxid for each partition index
%% owned by this node.
-spec get_zxids(preflist(), term()) -> [{index(), zxid()}].
get_zxids(Preflist, #state{history=Hist}) ->
    LoggedZxid = riak_zab_log:get_last_commit(Preflist, Hist),
    [{Idx, LoggedZxid} || {Idx, Node} <- Preflist, Node == node()].

%% Application state is owned by application vnodes, but riak_zab is in charge
%% of coordinating necessary synchronization. This function determines which
%% follower preflists are out of date with respect to the leader, and has the
%% appropriate leader vnode initiate synchronization with the corresponding
%% follower vnodes. Staleness is determined based on the last committed zxid
%% metadata that riak_zab maintains for each preflist.
synchronize_vnodes(Follower={_FNode, FPid},
                   LeaderAllZxids,
                   State=#state{ensemble=Ensemble, app=App}) ->
    FollowerAllZxids = case Follower == State#state.id of
                           true ->
                               LeaderAllZxids;
                           false ->
                               peer_all_sync_event(Ensemble, Follower,
                                                   all_zxids)
                       end,
    StaleIndices = get_stale_indices(LeaderAllZxids, FollowerAllZxids, []),

    %% Merge multiple (leader-zxid, follower-zxid) cases in the stale indices
    %% set (possible due to preflist overlap) into a single case. Reduces the
    %% number of vnode synchronizations needed.
    VNodesToSync = merge_stale_indices(StaleIndices),

    %% Determine the last committed zxid for each stale preflist.
    LatestPreflistZxids = get_latest_preflist_zxids(StaleIndices),

    %% Have stale vnodes perform synchronization
    [begin
         Cmd = ?ZAB_SYNC{peer={FIdx, FPid}, idxs=Idxs},
         riak_core_vnode_master:sync_command({LIdx, node()}, Cmd, App)
     end || {{LIdx, FIdx}, Idxs} <- VNodesToSync],

    %% Send DIFF messages to indiciate new preflist state after sync
    [begin
         Msg = {diff, State#state.id, Preflist, LRecent},
         ?DOUT("Sending DIFF(~p, ~p)~n", [Preflist, LRecent]),
         peer_send_event(State, Follower, Msg)
     end || {Preflist, LRecent} <- LatestPreflistZxids].

get_stale_indices([], [], Acc) ->
    Acc;
get_stale_indices([X|Xs], [Y|Ys], Acc) ->
    {LPreflist, LZxids} = X,
    {FPreflist, FZxids} = Y,
    %% Test invariant
    LPreflist = FPreflist,
    %% Compare preflists
    {LastIdx, LastZxid} = most_recent_index(LZxids),
    StaleIndices = [{LastIdx, FIdx, LastZxid, LPreflist}
                    || {FIdx, FZxid} <- FZxids, FZxid < LastZxid],
    get_stale_indices(Xs, Ys, StaleIndices ++ Acc).

merge_stale_indices(StaleIndices) ->
    D = lists:foldl(fun({LastIdx, FIdx, _LastZxid, Preflist}, Acc) ->
                            Key = {LastIdx, FIdx},
                            Idxs = [Idx || {Idx,_N} <- Preflist],
                            dict:update(Key, fun(L) ->
                                                     lists:usort(Idxs ++ L)
                                             end, Idxs, Acc)
                    end, dict:new(), StaleIndices),
    dict:to_list(D).

get_latest_preflist_zxids(StaleIndices) ->
    L = [{Preflist, LastZxid}
         || {_LastIdx, _FIdx, LastZxid, Preflist} <- StaleIndices],
    lists:ukeysort(1, L).

most_recent_index(Zxids) ->
    lists:foldl(fun({Idx, Zxid}, Acc={_, RecentZxid}) ->
                        case Zxid > RecentZxid of
                            true -> {Idx, Zxid};
                            false -> Acc
                        end
                end,
                hd(Zxids),
                Zxids).

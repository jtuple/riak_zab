-module(riak_zab_fast_election_fsm).
-behaviour(gen_fsm).

%% API
-export([start_link/4]).
-export([init/1, handle_info/3, terminate/3, code_change/4, handle_event/3,
         handle_sync_event/4]).

%% States
-export([initialize/2, not_looking/2, looking/2, finalize/2]).

-type peer() :: {node(), pid()}.
-type zxid() :: {non_neg_integer(), non_neg_integer()}.

-record(vote, {from :: peer(),
               leader :: peer(),
               zxid :: zxid(),
               epoch :: non_neg_integer(),
               state :: term()}).

-record(state, {curvote :: #vote{},
                proposed :: #vote{},
                parent :: pid(),
                epoch :: non_neg_integer(),
                ensemble :: [node()],
                quorum :: non_neg_integer(),
                last_zxid :: zxid(),
                peers :: [peer()],
                ntimeout :: pos_integer(),
                votes :: term(),
                outofelection :: term()}).

-define(ELECTION_FINALIZE_WAIT, 200).
-define(ELECTION_MAX_WAIT, 60000).

%-define(DOUT(Msg, Args), io:format(Msg, Args)).
-define(DOUT(Msg, Args), true).

-spec start_link(pid(), zxid(), [node()], pos_integer()) -> term().
start_link(ParentPid, LastZxid, Ensemble, Quorum) ->
    gen_fsm:start_link(?MODULE, [ParentPid, LastZxid, Ensemble, Quorum], []).

%% @private
init([ParentPid, LastZxid, Ensemble, Quorum]) ->
    State = #state{curvote=#vote{},
                   proposed=#vote{},
                   parent = ParentPid,
                   epoch=0,
                   ensemble=Ensemble,
                   quorum=Quorum,
                   last_zxid=LastZxid,
                   peers=[],
                   votes=dict:new(),
                   outofelection=dict:new(),
                   ntimeout=?ELECTION_FINALIZE_WAIT},
    {ok, initialize, State, 0}.

initialize(timeout, State=#state{epoch=Epoch}) ->
    SelfVote = nominate_self(Epoch, State),
    {next_state, not_looking, State#state{curvote=SelfVote,
                                          proposed=SelfVote}}.

%% @private
start_election(Peers, LastZxid, State=#state{epoch=Epoch}) ->
    NEpoch = Epoch + 1,
    State2 = State#state{last_zxid=LastZxid},
    SelfVote = nominate_self(NEpoch, State2),
    State3 = State2#state{curvote=SelfVote,
                          proposed=SelfVote,
                          epoch=NEpoch,
                          votes=dict:new(),
                          peers=Peers,
                          outofelection=dict:new(),
                          ntimeout=?ELECTION_FINALIZE_WAIT},
    {next_state, looking, State3, 0}.

%% Peer is not looking, but responds to others who are with current vote.
not_looking(#vote{from=Peer, state=looking},
            State=#state{curvote=CurVote, parent=ParentPid, ensemble=Ens}) ->
    ParentState = riak_zab_peer:node_state(ParentPid),
    Reply = CurVote#vote{state=ParentState},
    riak_zab_peer:election_event(Ens, Peer, Reply),
    {next_state, not_looking, State};
not_looking({start_election, Peers, LastZxid}, State) ->
    ?DOUT("Starting election~n", []),
    start_election(Peers, LastZxid, State);
not_looking(_Event, State) ->
    {next_state, not_looking, State}.

%% @private
continue_looking(State=#state{ntimeout=Wait}) ->
    {next_state, looking, State, Wait}.

looking({start_election, Peers, LastZxid}, State) ->
    ?DOUT("Restarting election~n", []),
    start_election(Peers, LastZxid, State);
looking(timeout, State=#state{ntimeout=Wait}) ->
    ?DOUT("Sending notifications...~n", []),
    send_notifications(State),
    Wait2 = erlang:min(Wait*2, ?ELECTION_MAX_WAIT),
    continue_looking(State#state{ntimeout=Wait2});
looking(Vote=#vote{state=looking}, State) ->
    handle_vote(Vote, State);
looking(Vote=#vote{}, State) ->
    handle_outofelection(Vote, State);
looking(_Event, State) ->
    continue_looking(State).

finalize(timeout, State) ->
    %% We have waited long enough, accept the proposed leader
    ?DOUT("Election complete, finalized! ~p~n", [State#state.proposed]),
    finish_election(State);
finalize(Event, State) ->
    %% We have received an event, return to election process
    looking(Event, State).

%% @private
finish_election(State=#state{proposed=Elected, parent=Parent, epoch=Epoch}) ->
    #vote{leader=Leader, zxid=Zxid} = Elected,
    Elected2 = Elected#vote{epoch=Epoch},
    ?DOUT("FE: Elected: ~p~n", [Elected2]),
    riak_zab_peer:elected(Parent, Leader, Zxid),
    {next_state, not_looking, State#state{curvote=Elected2}}.

%% @private
handle_vote(Vote=#vote{from=Peer, epoch=PeerEpoch},
            State=#state{proposed=Proposed, epoch=Epoch, ensemble=Ens}) ->
    if
        PeerEpoch < Epoch ->
            %% Ignore vote from previous election and respond with proposal.
            ?DOUT("Ignoring old vote~n", []),
            riak_zab_peer:election_event(Ens, Peer, Proposed),
            continue_looking(State);
        PeerEpoch > Epoch ->
            %% Fast forward to more recent election.
            ?DOUT("Fast forwarding to new epoch~n", []),
            {_Changed, Proposed2} = update_proposed(Proposed, Vote),
            Proposed3 = Proposed2#vote{epoch=PeerEpoch},
            State2 = State#state{epoch=PeerEpoch,
                                 proposed=Proposed3,
                                 votes=dict:new()},
            send_notifications(State2),
            add_vote(Vote, State2);
        PeerEpoch == Epoch ->
            ?DOUT("Considering vote...~n", []),
            {Changed, Proposed2} = update_proposed(Proposed, Vote),
            State2 = State#state{proposed=Proposed2},
            %% Send notifications if vote has changed
            case Changed of
                true ->
                    ?DOUT("Vote changed~p~n", [Proposed2]),
                    send_notifications(State2);
                false ->
                    ?DOUT("Vote didn't change~n", []),
                    true
            end,
            add_vote(Vote, State2)
    end.

%% @private
add_vote(Vote, State=#state{votes=Votes, ensemble=Ens, proposed=Proposed}) ->
    NVotes = dict:store(Vote#vote.from, Vote, Votes),
    State2 = State#state{votes=NVotes},
    ReceivedAll = (dict:size(NVotes) == ordsets:size(Ens)),
    HaveQuorum = count_votes(State2, Proposed, NVotes),
    case {HaveQuorum, ReceivedAll} of
        {false, _} ->
            %% No quorum yet, continue looking.
            continue_looking(State2);
        {true, true} ->
            %% Quorum met and we have heard from all nodes, election complete.
            ?DOUT("Election complete! ~p~n", [Proposed]),
            finish_election(State2);
        {true, false} ->
            %% Quorum met without heading from all nodes, move to finalize.
            {next_state, finalize, State2, ?ELECTION_FINALIZE_WAIT}
    end.

%% @private
handle_outofelection(Vote, State) when Vote#vote.epoch == State#state.epoch ->
    #vote{from=Peer, state=PeerState, leader=Leader} = Vote,
    case PeerState of
        leading ->
            %% There is at most one leader for each epoch, so if a peer
            %% claims to be the leader, then it must be the leader. 
            ?DOUT("Found current leader: ~p~n", [Leader]),
            State2 = set_proposed(State, Vote),
            finish_election(State2);
        _ ->
            NVotes = dict:store(Peer, Vote, State#state.votes),
            State2 = State#state{votes=NVotes},
            HaveQuorum = count_votes(State2, Vote, NVotes),
            IsLeader = check_leader(State2#state.outofelection, Leader,
                                    get_id(State)),
            if
                HaveQuorum and IsLeader ->
                    ?DOUT("Found supported leader: ~p~n", [Leader]),
                    State3 = set_proposed(State2, Vote),
                    finish_election(State3);
                true ->
                    check_outofelection(Vote, State)
            end
    end;
handle_outofelection(Vote, State) ->
    check_outofelection(Vote, State).

%% @private
check_outofelection(Vote=#vote{from=Peer, leader=Leader, epoch=PeerEpoch},
                    State=#state{outofelection=OutVotes}) ->
    OutVotes2 = dict:store(Peer, Vote, OutVotes),
    State2 = State#state{outofelection=OutVotes2},
    HaveQuorum = count_votes(State2, Vote, OutVotes2),
    IsLeader = check_leader(OutVotes2, Leader, get_id(State)),
    if
        HaveQuorum and IsLeader ->
            ?DOUT("Found leader from different epoch: ~p~n", [Leader]),
            State3 = set_proposed(State2, Vote),
            State4 = State3#state{epoch=PeerEpoch},
            finish_election(State4);
        true ->
            continue_looking(State2)
    end.

handle_info(_Msg, StateName, State) ->
    {next_state, StateName, State}.

%% send_all_state_event is used by riak_zab_peer to route messages to the
%% election FSM. Chain to the defined state handlers.
handle_event(Event, StateName, State) ->
    ?MODULE:StateName(Event, State).

handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @private
get_id(State) ->
    {node(), State#state.parent}.

%% @private
nominate_self(Epoch, State=#state{last_zxid=LastZxid}) ->
    Self = get_id(State),
    #vote{from=Self, leader=Self, zxid=LastZxid, epoch=Epoch}.

%% @private
total_order(NewId, NewZxid, CurId, CurZxid) ->
    (NewZxid > CurZxid) orelse ((NewZxid == CurZxid) andalso (NewId > CurId)).

%% @private
update_proposed(Proposed=#vote{leader=CurLeader, zxid=CurZxid},
                _Vote=#vote{leader=NewLeader, zxid=NewZxid}) ->
    Proposed2 = case total_order(NewLeader, NewZxid, CurLeader, CurZxid) of
                    true ->
                        Proposed#vote{leader=NewLeader, zxid=NewZxid};
                    false ->
                        Proposed
                end,
    {Proposed2 =/= Proposed, Proposed2}.

%% @private
send_notifications(#state{proposed=Proposed, ensemble=Ens, peers=Peers}) ->
    Vote = Proposed#vote{state=looking},
    [riak_zab_peer:election_event(Ens, Peer, Vote) || Peer <- Peers].

%% @private
count_votes(#state{quorum=Quorum}, Proposed, Votes) ->
    {_, NumVotes} = dict:fold(fun check_vote/3, {Proposed, 0}, Votes),
    ?DOUT("Checking votes: ~p / ~p ~n", [NumVotes, Quorum]),
    ?DOUT("Ensemble: ~p~n", [State#state.ensemble]),
    ?DOUT("   Votes: ~p~n", [dict:to_list(Votes)]),
    if
        NumVotes >= Quorum ->
            ?DOUT("Found quorum~n", []),
            true;
        NumVotes < Quorum ->
            ?DOUT("No quorum yet~n", []),
            false
    end.

%% @private
check_vote(_PeerId, Vote, {Proposed, Count}) ->
    V1 = {Vote#vote.leader, Vote#vote.zxid, Vote#vote.epoch},
    V2 = {Proposed#vote.leader, Proposed#vote.zxid, Proposed#vote.epoch},
    case V1 of
        V2 ->
            {Proposed, Count + 1};
        _ ->
            {Proposed, Count}
    end.

%% (Comment from Zookeeper Implementation)
%% In the case there is a leader elected, and a quorum supporting
%% this leader, we have to check if the leader has voted and acked
%% that it is leading. We need this check to avoid that peers keep
%% electing over and over a peer that has crashed and it is no
%% longer leading.
%%
%% @private
check_leader(_Votes, Leader, MyId) when (Leader == MyId) ->
    %% If everyone thinks I'm the leader, then I must be.
    true;
check_leader(Votes, Leader, _MyId) ->
    case dict:find(Leader, Votes) of
        error ->
            false;
        {ok, Vote} ->
            Vote#vote.state == leading
    end.

%% @private
set_proposed(State=#state{proposed=Proposed},
             #vote{leader=Leader, zxid=Zxid}) ->
    Proposed2 = Proposed#vote{leader=Leader, zxid=Zxid},
    State#state{proposed=Proposed2}.

%% @private
terminate(_Reason, _StateName, _State) ->
    ok.

%% @private
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

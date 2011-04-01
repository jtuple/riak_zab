%% -------------------------------------------------------------------
%%
%% riak_vnode_master: dispatch to vnodes
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------

%% @doc dispatch to vnodes

-module(riak_zab_process).
-behaviour(gen_server).
-export([start_link/0, start_process/3, stop_process/1, start_process_link/3, get_pid/1, all_pids/1, all_processes/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3]).
-record(idxrec, {idx, pid, monref}).
-record(state, {idxtab}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

start_process_link(Key, Mod, Args) ->
    {ok, Pid} = Mod:start_link(Args),
    gen_server:cast(?MODULE, {add_pid, Key, Pid}),
    {ok, Pid}.

start_process(Key, Mod, Args) ->
    {ok, Pid} = gen_server:call(?MODULE, {start_process, Key, Mod, Args}, infinity),
    Pid.

stop_process(Key) ->
    gen_server:call(?MODULE, {stop_process, Key}, infinity).

get_pid(Key) ->
    {ok, Pid} = gen_server:call(?MODULE, {get_pid, Key}, infinity),
    Pid.
    
all_pids(Type) ->
    gen_server:call(?MODULE, {all_pids, Type}, infinity).

all_processes(Type) ->
    gen_server:call(?MODULE, {all_processes, Type}, infinity).

%% @private
init(_Args) ->
    %% Get the current list of pids running in the supervisor. We use this
    %% to rebuild our ETS table.
    Pids = [{Pid, Key} || {Key, Pid, worker, _}
                              <- supervisor:which_children(riak_zab_process_sup)],
    IdxTable = ets:new(idxtable, [{keypos, 2}]),

    %% Populate the ETS table with processes running this VNodeMod (filtered
    %% in the list comprehension)
    F = fun(Pid, Idx) ->
                Mref = erlang:monitor(process, Pid),
                #idxrec { idx = Idx, pid = Pid, monref = Mref }
        end,
    IdxRecs = [F(Pid, Idx) || {Pid, Idx} <- Pids],
    true = ets:insert_new(IdxTable, IdxRecs),
    {ok, #state{idxtab=IdxTable}}.

handle_cast({add_pid, Key, Pid}, State) ->
    MonRef = erlang:monitor(process, Pid),
    add_vnode_rec(#idxrec{idx=Key,pid=Pid,monref=MonRef}, State),
    {noreply, State}.

handle_call({all_pids, Type}, _From, State) ->
    {reply, lists:flatten(ets:match(State#state.idxtab,
                                    {idxrec, {Type, '_'}, '$1', '_'})), State};
handle_call({all_processes, Type}, _From, State) ->
    M = ets:match(State#state.idxtab, {idxrec, {Type, '$1'}, '_', '_'}),
    {reply, [{Type, hd(L)} || L <- M], State};
handle_call({get_pid, Key}, _From, State) ->
    Pid = idx2vnode(Key, State),
    {reply, {ok, Pid}, State};
handle_call({start_process, Key, Mod, Args}, _From, State) ->
    Ref = {Key, {?MODULE, start_process_link, [Key, Mod, Args]},
           permanent, 5000, worker, [Mod]},
    Pid = case supervisor:start_child(riak_zab_process_sup, Ref) of
              {ok, Child} -> Child;
              {error, {already_started, Child}} -> Child
          end,
    {reply, {ok, Pid}, State};
handle_call({stop_process, Key}, _From, State) ->
    supervisor:terminate_child(riak_zab_process_sup, Key),
    supervisor:delete_child(riak_zab_process_sup, Key),
    {reply, ok, State}.

handle_info({'DOWN', MonRef, process, _P, _I}, State) ->
    delmon(MonRef, State),
    {noreply, State}.

%% @private
terminate(_Reason, _State) -> 
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->  {ok, State}.

%% @private
idx2vnode(Idx, _State=#state{idxtab=T}) ->
    case ets:match(T, {idxrec, Idx, '$1', '_'}) of
        [[VNodePid]] -> VNodePid;
        [] -> no_match
    end.

%% @private
delmon(MonRef, _State=#state{idxtab=T}) ->
    ets:match_delete(T, {idxrec, '_', '_', MonRef}).

%% @private
add_vnode_rec(I,  _State=#state{idxtab=T}) -> ets:insert(T,I).

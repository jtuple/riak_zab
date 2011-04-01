-module(riak_zab_ring_handler).
-behaviour(gen_event).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).
-record(state, {}).


%% ===================================================================
%% gen_event callbacks
%% ===================================================================

init([]) ->
    %% Pull the initial ring and make sure all vnodes are started
    {ok, _Ring} = riak_core_ring_manager:get_my_ring(),
    {ok, #state{}}.

handle_event({ring_update, _Ring}, State) ->
    riak_zab_ensemble_master:ring_changed(),
    {ok, State}.

handle_call(_Event, State) ->
    {ok, ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

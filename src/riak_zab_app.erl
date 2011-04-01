-module(riak_zab_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1, full_stop/0, full_stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    riak_core_util:start_app_deps(riak_zab),

    case riak_zab_sup:start_link() of
        {ok, Pid} ->
	    ok = riak_core_ring_events:add_handler(riak_zab_ring_handler, []),
            {ok, Pid};
        {error, Reason} ->
            {error, Reason}
    end.

stop(_State) ->
    ok.


%% @spec full_stop() -> ok
%% @doc Stop the riak application and the calling process.
full_stop() -> stop("riak stop requested").
full_stop(Reason) ->
    % we never do an application:stop because that makes it very hard
    %  to really halt the runtime, which is what we need here.
    error_logger:info_msg(io_lib:format("~p~n",[Reason])),
    init:stop().

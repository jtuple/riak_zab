-include_lib("riak_core/include/riak_core_vnode.hrl").

-type zsender() :: {fsm_sync, reference(), term()} |
                   {fsm_sync, undefined, undefined} |
                   sender().

-record(riak_zab_proposal, {
          preflist,
          sender,
          message}).

-define(ZAB_PROPOSAL, #riak_zab_proposal).

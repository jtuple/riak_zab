%-include_lib("riak_core/include/riak_core_vnode.hrl").

-record(riak_zab_last_zxid, {}).
-record(riak_zab_sync, {peer, idxs}).
-record(riak_zab_req, {req, zxid, sender, leading}).
-record(riak_zab_sync_data, {data}).

-define(ZAB_LAST_ZXID, #riak_zab_last_zxid).
-define(ZAB_SYNC, #riak_zab_sync).
-define(ZAB_REQ, #riak_zab_req).
-define(ZAB_SYNC_DATA, #riak_zab_sync_data).


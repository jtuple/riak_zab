%% -*- tab-width: 4;erlang-indent-level: 4;indent-tabs-mode: nil -*-
%% ex: ts=4 sw=4 etnnononono
{application,riak_zab,
             [{description,[]},
              {vsn,"0.1.0"},
              {registered,[]},
              {applications, [
                              kernel,
                              stdlib,
                              riak_core
                             ]},
              {mod,{riak_zab_app,[]}},
              {env,[]},
              {modules, [
                         riak_zab_app,
                         riak_zab_sup,
                         riak_zab_peer,
                         riak_zab_vnode,
                         riak_zab_ring_handler,
                         riak_zab_fast_election_fsm,
                         riak_zab_watcher,
                         riak_zab_backend,
                         riak_zab_bitcask_backend,
                         riak_zab_status,
                         riak_zab_console,
                         riak_zab_process,
                         riak_zab_process_sup,
                         riak_zab_ensemble_master,
                         riak_zab_ensemble_util,
                         riak_zab_util,
                         riak_zab_log,
                         riak_zab_leader_test
                        ]}
             ]}.

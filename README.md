riak_zab
========

riak_zab is an extension for [riak_core](https://github.com/basho/riak_core)
that provides totally ordered atomic broadcast capabilities. This is
accomplished through a pure Erlang implementation of Zab, the Zookeeper Atomic
Broadcast protocol invented by Yahoo! Research. Zab has nothing to do with the
high-level Zookeeper API that you may be familar with, but is the underlying
broadcast layer used by Zookeeper to implement that API. riak_zab does not
implement the Zookeeper API, only Zab. Zab is conceptually similar to the
[Paxos algorithm](http://en.wikipedia.org/wiki/Paxos_algorithm) with different
design choices that lead to higher throughput and, in my opinion, a more
straight-forward implementation.

At the high-level, Zab is a leader-based quorum/ensemble protocol that utilizes
two-phase commit.  An ensemble (a group of nodes) elects a leader, and all
messages across the ensemble are sent through the leader therefore providing a
total order. The leader utilizes a two-phase propose/commit protocol that ensures
certain guarantees even in the presence of node failures. All in all, Zab guarantees the
following properties:

* __Reliable delivery:__ If a message m is delivered by one node, than it will
  eventually be delivered by all valid nodes.
* __Total order:__ If one node delivers message a before message b, than all
  nodes that deliver messages a and b will deliver a before b.

More details on Zab can be found in the [original research
paper](http://research.yahoo.com/pub/3274), the more recent [technical
report](http://research.fy4.b.yahoo.com/files/YL-2010-007.pdf) (which includes
a detailed formal proof), as well on the Zookeeper
[wiki](https://cwiki.apache.org/confluence/display/ZOOKEEPER/Zab1.0).

Design
------
The goal of riak_zab is to provide the atomic broadcast capabilities of Zab to
riak_core while staying as true as possible to the underlying philosophy that
riak_core is based upon. To this end, an application that utilizes riak_zab
remains essentially a riak_core application. The ideas championed by
riak_core/Dynamo remain: vnodes, consistent hashing, the ring, an (object,
payload) command set, handoff, etc. Likewise, a cluster can scale transparently
over time by adding additional nodes.

To acheive these goals, riak_zab partitions a riak cluster into a set of
ensembles where a given ensemble is responsible for a subset of preference
lists within the riak ring. The members of the ensemble correspond to the
nodes that own the indicies within the respective preflists. riak_zab then
provides a command interface that is similar to that provided by riak_core,
where commands are dispatched to preference lists and preference lists
are obtained by hashing object keys to locations in the ring. Each ensemble
is an independent Zab instance, that elects its own ensemble leader and
engages in atomic broadcast amongst themselves. Commands that are dispatched
to a particular preference list are sent to the leader of the ensemble
associated with the preflist, and the command is totally ordered and
reliably delivered to all vnodes within the preflist. Unlike traditional
riak, this enforces a strict quorum at all times, which is necessary in
a quorum based protocol. 

    %% Verbose example
    Idx = chash:key_of(Key),
    ESize = riak_zab_ensemble_master:get_ensemble_size(),
    {ok, Ring} = riak_core_ring_manager:get_my_ring(),
    Preflist = preflist(Idx, ESize, Ring),
    Msg = {store, Key, Value},
    riak_zab_ensemble_master:command(Preflist, Msg, Sender, MyApp).

    %% Or using riak_zab_util:
    riak_zab_util:command(Key, {store, Key, Value}, MyApp).
    riak_zab_util:sync_command(Key, {get, Key}, MyApp).
    %% ...

Okay, I lied.
-------------
Technically, riak_zab isn't focused on message delivery. Rather, riak_zab
provides consistent replicated state.

When all nodes are up, riak_zab does ensure that all commands are delivered
in a fixed order to all ensemble nodes. However, if a node is down, riak_zab
obviously cannot deliver a command. So what happens when that node comes back
online? In that case, riak_zab is aware the node is behind, and forces the
node to synchronize with the current leader. The leader performs a state
transfer which brings the lagging node up to date. Therefore, riak_zab
guarantees that all active nodes within an ensemble have a consistent state
that is derived from a particular ordering of delivered messages.

In reality, Zookeeper does something similar at the Zab layer, but this is
masked by the higher-level Zookeper API that Zookeeper based apps interact
with.

So, what is this state? How is synchronization performed?

State and synchronization are features of your application that builds on
riak_zab. Just like riak_core, riak_zab applications are implemented using
vnodes. State is whatever your application wants it to be. riak_zab simply
provides the following features:

* An atomic command concept, where commands are sent like riak_core but are
  guaranteed to be ordered and delievered to a quorum of nodes (or fail).

* Synchronization. When a vnode rejoins a cluster (restart, net split, etc),
  riak_zab will ask the current leader vnode to sync with the out-of-date
  follower vnode. The leader vnode then does a state transfer similar to
  standard handoff in order to bring the node up to date. Most applications
  can utilize the standard synchronization logic provided with riak_zab, which
  folds over a (key, value) representation of the vnode state and ships it off
  to the follower vnode. More complex apps are free to implement custom
  synchronization behaviour.

Scaling
-------

How does a ensemble based protocol scale? Don't ensemble protocols get slower
as you add more nodes?

Yes. Yes, they do. That's why riak_zab fixes the number of nodes in an
ensemble. It's an app.config value. The default is 3, and there really
isn't a reason to use a value other than 3 or 5. With 3 you can tolerate
1 node failure and remain available. With 5, you can tolerate 2. Many
people chose 5 (in the Zookeeper case), so that they can take 1 node offline
for maintainence and still be able to tolerate 1 additional node failure.

Since the ensemble size is fixed, as you add more nodes to a riak_zab cluster,
you end up with more and more ensembles, not more nodes per ensemble. Each
ensemble therefore owns fewer and fewer preflists as you scale out. 

Assuming your workload is distributed across preflists (a similar assumption
underlying riak_core performance), your workload will be distributed across
different ensembles and therefore different nodes and Zab instances. You will
always pay a price (latency) for leader based two-phase commit, but you can
have multiple independent two-phase commit protocols running completely
independently across completely independent nodes. Thus, predictable scaling of
throughput just like riak_core.

What about ring rebalancing and handoff?
-----------------------------------------
An astute reader (probably from Basho) will likely wonder how this all works
with dynamic ring membership. Assuming the ring is static, it's easy to see
how ensembles map to preflists and how preflist state is owned by static vnodes.
Dynamic vnode ownership and handoff complicate matters.

The current solution isn't elegant, but it works. riak_zab assumes the ring
state is stable. When a riak_zab cluster starts up, the zab layer is disabled.
It's up to the user (ie. operations) to setup the cluster and then manually
start the zab layer. Specifically, the user must ensure that the ring is
consistent across the cluster and that no handoffs are pending. The 'ringready'
and 'transfers' commands for the riak-admin/riak-zab-admin tool are the
solution here.

     > ./dev/dev1/bin/riak-zab-admin join dev2@127.0.0.1
     Sent join request to dev2@127.0.0.1
     > ./dev/dev2/bin/riak-zab-admin join dev3@127.0.0.1
     Sent join request to dev3@127.0.0.1
     > ./dev/dev3/bin/riak-zab-admin join dev4@127.0.0.1
     Sent join request to dev4@127.0.0.1
     > ./dev/dev4/bin/riak-zab-admin join dev1@127.0.0.1
     Sent join request to dev1@127.0.0.1
     > ./dev/dev1/bin/riak-zab-admin ringready
     TRUE All nodes agree on the ring ['dev1@127.0.0.1','dev2@127.0.0.1',
                                       'dev3@127.0.0.1','dev4@127.0.0.1']

     > ./dev/dev1/bin/riak-zab-admin transfers
     No transfers active.

     > ./dev/dev1/bin/riak-zab-admin zab-up
     > ./dev/dev2/bin/riak-zab-admin zab-up
     > ./dev/dev3/bin/riak-zab-admin zab-up
     > ./dev/dev4/bin/riak-zab-admin zab-up

     > ./dev/dev1/bin/riak-zab-admin info

     ================================ Riak Zab Info ================================
     Ring size:     64
     Ensemble size: 3
     Nodes:         ['dev1@127.0.0.1','dev2@127.0.0.1','dev3@127.0.0.1',
                     'dev4@127.0.0.1']
     ================================== Ensembles ==================================
     Ensemble     Ring   Leader                         Nodes
     -------------------------------------------------------------------------------
            1    25.0%   dev2@127.0.0.1                 ['dev1@127.0.0.1',
                                                         'dev2@127.0.0.1',
                                                         'dev3@127.0.0.1']
     -------------------------------------------------------------------------------
            2    25.0%   dev2@127.0.0.1                 ['dev1@127.0.0.1',
                                                         'dev2@127.0.0.1',
                                                         'dev4@127.0.0.1']
     -------------------------------------------------------------------------------
            3    25.0%   dev3@127.0.0.1                 ['dev1@127.0.0.1',
                                                         'dev3@127.0.0.1',
                                                         'dev4@127.0.0.1']
     -------------------------------------------------------------------------------
            4    25.0%   dev3@127.0.0.1                 ['dev2@127.0.0.1',
                                                         'dev3@127.0.0.1',
                                                         'dev4@127.0.0.1']
     -------------------------------------------------------------------------------

Likewise, if the ring membership ever changes while zab is running,
the zab layer will disable itself, thus making zab ensembles
unavailable but consistent. It's once again up to the user to
ensure that all handoffs are completed and so forth, and then
once again issue a 'zab-up' command.

For those unfamilar with Riak, after the ring is stable the
membership only changes when a user explictly adds or removes
nodes from the cluster. Nodes going offline or being partitioned
don't rebalance the ring. Therefore, this is a rare, operationally
initiated event in the first place.

Quick Start
-----------
This repository isn't a stand-alone Erlang release, much like riak_core. You
can clone the [riak_zab_example](https://github.com/jtuple/riak_zab_example)
repository to have a runnable release, that also demonstrates how to build a
simple riak_zab vnode-based application.

Testing
-------
riak_zab is very much an alpha release right now, and woefully lacking in
tests. I have many script based systems-level integration tests that test a
cluster of riak_zab nodes in a variety of scenarios, including using virtual
networking to simulate net-splits. I am slowly porting this over to a pure
Erlang based multi-node expectation framework that I am designing concurrently
with this porting task. An example is the riak_zab_leader_test.erl file included
in this repository. Further porting is underway.

However, the real plan is to test the entire application using QuickCheck.
Testing an atomic broadcast protocol isn't easy, and relying on traditional
unit tests and systems-level tests is a bad idea. QuickCheck is a much better
approach, however it will take some effort to make everything testable. I am
hoping to follow in the footsteps of [jonmeredith](https://github.com/jonmeredith)
and [slfritchie](https://github.com/slfritchie) from Basho who has embarked
on a heroric effort to test Riak using QuickCheck.

Specifically, my plan is to test that all of the invariants and claims listed
within the formal Zab [technical
report](http://research.fy4.b.yahoo.com/files/YL-2010-007.pdf)from Yahoo! do in
fact hold for this implementation.

Can I use this in production?
-----------------------------
As just mentioned, there is still a long way to go with testing riak_zab.
I suggest folks who are interested play around with it, but not use it in
production. I know of a few very rare corner cases that currently cause
trouble (fixed within Zookeeper within the last year), that I still need
to address.

I used an older version of this within a closed beta for a social gaming
project. While that may be a quasi-production deployment, the reality was
that money was never on the line -- if things didn't work, you just buy
the players off with free virtual goods.

Zookeeper has been out for years, and yet new edge cases are found on
occasion. This is a difficult task to get right. My hope is that future testing
with QuickCheck (and possibly McErlang or McErlang/QuickCheck) will eventually
lead to a truly robust and trustable atomic broadcast implementation. Building
this in Erlang also helps greatly.

Where's the killer app?
-----------------------
By the way, riak_zab is actually an extracted reusable layer that arose as part
of a project I called riakual, that provides an immediately consistent access
layer on top of riak_kv that supports optimistic concurrency. I'll soon be
releasing a version of riakual that is built as a riak_zab vnode application.
Hopefully that gives you an idea of what you can do with riak_zab.

And for those who ask, why would you add immediate consistent to riak? Well,
Amazon's SimpleDB (Dynamo based) added such a few years back. It's certainly
not that crazy.

Also, if your application can be expressed in an eventually consistent manner
80-90% of the time, wouldn't it be nice if you could use the same predictably
scable and simple to deploy datastore (ie. Riak) for the 10% that must be
immediately consistent? 

What if your app was more eventually consistent than you previously thought?
The awesome
[CALM](http://databeta.wordpress.com/2010/10/28/the-calm-conjecture-reasoning-about-consistency)
and [Bloom](http://www.bloom-lang.net) research from Professor Hellerstein
group may expand your mind.

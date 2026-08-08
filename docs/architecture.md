# Beacon — architecture notes

Working notes that sit behind the README summary. Expanded as each step lands.

## The problem

Presence looks trivial with one server: hold a map of `userId -> status`, push
changes to whoever is watching. It stops being trivial the moment there is more
than one server, because the map is now split across processes that cannot see
each other's memory.

Three things break at once:

1. **Visibility.** A client on gateway A subscribes to a user connected to
   gateway C. Neither node holds the other's state, so the subscription has to
   resolve through something shared.
2. **Resolution.** A `JOIN` targeting a user connected elsewhere has to return
   that user's current `placeId`/`serverId`. The node answering the request is
   usually not the node holding the session.
3. **Cleanup.** A client that vanishes without closing cleanly leaves a session
   behind. Something has to notice and transition it to `OFFLINE` — exactly once,
   not once per replica, and reliably even when the node that owned the session
   is the thing that died.

Beacon's shape follows from those three.

## Component roles

| Component | Role |
| --- | --- |
| Gateway (N=3) | Terminates client WebSockets; owns no authoritative state |
| Redis — hashes | Authoritative session state, readable by any replica |
| Redis — pub/sub | Cross-node presence fan-out on per-user channels |
| Redis — registry | Liveness of gateway nodes, via TTL heartbeat keys |
| Hash ring | Assigns reaper ownership of each user shard to one live node |

The gateways are deliberately stateless with respect to correctness. In-memory
connection tables exist for efficiency, and the drift reconciler treats any
divergence between them and Redis as a defect to be reported.

## Why a ring at all

Reaping is the one job that must not be done N times. Every replica can see
every expired session in Redis, so absent coordination all three would scan and
delete the same keys, racing to publish duplicate `OFFLINE` transitions.

A consistent hash ring over live gateway IDs gives each user shard exactly one
owner, with a property a simple modulo would not: when a node leaves, only the
shards that node owned move. The surviving nodes keep their existing assignments
instead of every shard being reshuffled. That matters during failover, which is
precisely when the system is least able to absorb extra churn.

Ring membership derives from the Redis node registry, so a dead gateway drops
out on TTL expiry and the ring rebalances without any explicit failover signal.

## Questions the benchmarks answered

- **Reaper cadence versus heartbeat TTL.** Settled empirically at a 2 s sweep
  against a 30 s session TTL and a 6 s node TTL. The sweep interval turned out to
  matter for a reason not anticipated: it is the throughput ceiling. Sweep p99
  crosses 2 s somewhere between 20,000 and 30,000 sessions, and join latency
  degrades in step, because an unchunked pipelined scan contends with the request
  path on the same Redis pool.

- **Can drift be held at exactly zero under load?** Yes in steady state — it read
  zero on all three gateways at every load level once connections plateaued. Not
  during failover, and it should not be: after a gateway is killed, drift is the
  signal that Redis still holds sessions no live node is serving. Measured peak
  −1,920 with a return to zero 3.0 s later.

- **Is failover time dominated by anything interesting?** No — it is dominated by
  `BEACON_NODE_TTL` elapsing, 6 s of a ~9.5 s convergence. The mechanism is
  boring on purpose: nothing detects the failure, so nothing about the detection
  can fail.

## Open questions

- Whether chunking the reaper sweep across intervals lifts the ceiling
  proportionally, or merely moves the contention elsewhere.
- Whether a dedicated Redis connection pool for the reaper is a cleaner fix than
  chunking, given it trades contention for connection count.

## Branch history

Per-branch narrative lives in the README's Build log rather than here, so there
is exactly one place tracking what landed when. These notes cover design
reasoning that outlives any single branch.

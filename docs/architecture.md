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
  matter for a reason not anticipated: it was the throughput ceiling. Sweep p99
  crossed 2 s somewhere between 20,000 and 30,000 sessions, and join latency
  degraded in step, because an unchunked pipelined scan contended with the
  request path on the same Redis pool. Fixed on `perf/chunked-reaper`; the
  cadence itself was never the problem, the shape of the work inside it was.

- **Does chunking the sweep lift the ceiling, or move the contention?** It lifts
  it. The highest fully clean load result went from 20,000 to at least 50,000
  connections, and JOIN p99 at 40,000 fell from 4,894 ms to 53 ms. The
  contention did not reappear elsewhere: connect latency improved by the same
  order, and no other resource moved toward its limit.

- **Is a dedicated Redis pool a cleaner fix than chunking?** Neither replaces
  the other, so this was a false choice. Chunking bounds how long any single
  burst occupies Redis, which is what stops the server being monopolised. A
  separate pool bounds which *connections* background work may hold, which is
  what stops cleanup and the request path competing at all. Both shipped
  together, and the cost of the pool is eight extra connections per gateway.

- **What does a sweep that cannot keep up look like?** Previously: nothing. It
  ran past its interval and the next sweep started anyway, so the only visible
  symptom was JOIN latency climbing for reasons a dashboard could not attribute.
  A sweep now stops at a time budget and increments
  `beacon_reaper_sweeps_truncated_total`, which turns "cleanup is falling
  behind" into a signal rather than an inference.

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

- Where the connection ceiling actually is now. 50,000 is clean and nothing in
  the gateways is near a limit; answering this needs a load generator that is
  not sharing a memory budget with the service it is measuring.
- Whether the session index should be sharded across several Redis keys rather
  than walked as one set. Every gateway scans the whole index to find the
  fraction it owns, which is wasted work proportional to cluster size — a
  per-shard index would make the scan proportional to what a gateway actually
  reaps.
- Whether the reaper's time budget should adapt to observed sweep duration
  rather than being a fixed 1.5 s. A budget that is too generous at one
  connection count is too tight at another.

## Branch history

Per-branch narrative lives in the README's Build log rather than here, so there
is exactly one place tracking what landed when. These notes cover design
reasoning that outlives any single branch.

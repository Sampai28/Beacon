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

## Open questions carried forward

- Reaper cadence versus heartbeat TTL: too tight and a briefly-slow client is
  reaped; too loose and orphaned sessions linger. Needs measurement, not a guess.
- Whether drift can be held at exactly zero under load, or only converge to zero
  shortly after. Step 9 answers this with data.

## Branch history

Per-branch narrative lives in the README's Build log rather than here, so there
is exactly one place tracking what landed when. These notes cover design
reasoning that outlives any single branch.

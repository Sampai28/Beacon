# Beacon

A horizontally-sharded presence and session-join service in Go. It is the
backend primitive behind "see which friends are online" and "join my friend's
game", built at local scale and measured honestly.

Presence is easy on one server: keep a map of `userId -> status` and push changes
to whoever is watching. Add a second server and the map is split across processes
that cannot see each other's memory. Three problems appear at once. A client on
gateway A subscribing to a user on gateway C needs a path for that update to
cross nodes. A `JOIN` has to resolve a target whose session is almost never held
by the node answering. And a client that vanishes without closing leaves a
session that something must clean up once, not once per replica, and reliably
even when the node that died is the node that owned it.

Beacon solves those three and measures what happens when a gateway is killed.

## Architecture

```mermaid
flowchart TB
    subgraph Clients
        C1["Client A<br/>(browser tab)"]
        C2["Client B"]
        C3["Client C"]
    end

    subgraph Gateways["Gateway replicas (N=3, stateless for correctness)"]
        G1["gateway-1<br/>:8080"]
        G2["gateway-2<br/>:8080"]
        G3["gateway-3<br/>:8080"]
    end

    subgraph Redis["Redis 7 — shared source of truth"]
        H["Session hashes<br/>userId, status, placeId,<br/>serverId, gatewayId, lastSeen"]
        P["Pub/Sub<br/>per-user channels"]
        N["Node registry<br/>TTL heartbeat keys"]
    end

    R{{"Consistent hash ring<br/>membership derived from registry<br/>assigns reaper ownership<br/>of each user shard to ONE live node"}}

    C1 -- WebSocket --> G1
    C2 -- WebSocket --> G2
    C3 -- WebSocket --> G3

    G1 <--> H
    G2 <--> H
    G3 <--> H

    G1 -- publish --> P
    P -- fan-out --> G3
    P -- fan-out --> G2

    G1 -- heartbeat --> N
    G2 -- heartbeat --> N
    G3 -- heartbeat --> N

    N -. membership .-> R
    R -. "shard ownership<br/>(reaping only)" .-> G1
    R -.-> G2
    R -.-> G3
```

Clients attach to any gateway; there is no affinity. Sessions are written to
Redis hashes so any replica can answer for any user. Presence changes fan out
over per-user pub/sub channels, and a gateway subscribes only to the channels its
own clients asked for, so a change on gateway 1 reaches a subscriber on gateway 3
without the two nodes talking directly.

The ring is dotted in the diagram because it carries no client traffic. Its only
job is deciding which live gateway reaps stale sessions for a given user.
Membership comes from the TTL node registry, so a dead gateway stops
heartbeating, drops out, and the ring rebalances with no explicit failover
signal.

## Protocol

JSON frames over one WebSocket.

| Direction | Message | Payload |
|---|---|---|
| C→S | `HELLO` | `{userId, token}` |
| S→C | `WELCOME` | `{sessionId, gatewayId}` |
| C→S | `SUBSCRIBE` | `{userIds: []}` — returns current snapshots |
| C→S | `HEARTBEAT` | `{}` → `ACK` |
| C→S | `SET_PRESENCE` | `{status, placeId, serverId}` |
| C→S | `JOIN` | `{targetUserId}` → `JOIN_OK {placeId, serverId}` or `JOIN_DENIED {reason}` |
| S→C | `PRESENCE` | `{userId, status, placeId, ts}` |
| S→C | `ERROR` | `{code, message}` |

`token` is a dev-mode shared secret from an environment variable. It is not real
authentication and no credentials are stored.

## Integrity checks

Six checks run continuously, each with its own Prometheus collector and a panel
on the Integrity dashboard.

| # | Check | Behaviour |
|---|---|---|
| 1 | Frame validation | Rejects malformed JSON, unknown types, missing fields and payloads over 8KB. Never panics. |
| 2 | Duplicate sessions | A user connecting while connected elsewhere evicts the older session. |
| 3 | Out-of-order events | Presence events older than the stored `lastSeen` are dropped. |
| 4 | Stale-session reaper | Sessions with a lapsed heartbeat TTL are removed by the shard's ring owner. |
| 5 | Orphan detection | Sessions whose `gatewayId` is gone from the registry are reclaimed. |
| 6 | Drift reconciliation | In-memory connection totals compared against Redis session cardinality, exported as `beacon_presence_drift`. |

## Quickstart

**Prerequisites:** Go 1.25+, Docker Desktop with Compose v2, plus GNU Make and
`jq` for the bench targets. k6 is not needed on the host; it runs as a Compose
service.

Build and test:

```bash
go build ./... && go test ./...
```

The race detector needs cgo, which a stock Windows Go install lacks. Run it in a
container:

```bash
docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=1 golang:1.25 go test -race ./...
```

Bring up the stack:

```bash
docker compose -f deploy/docker-compose.yml up -d --build
```

| Service | URL |
|---|---|
| Demo client | http://localhost:8081 (also `:8082`, `:8083`) |
| Grafana | http://localhost:3000 (anonymous, dashboards pre-provisioned) |
| Prometheus | http://localhost:9090 |
| Ring state | http://localhost:8081/debug/ring |

To watch fan-out cross nodes, open `:8081` and `:8082` in two tabs, connect as
`alice` and `bob`, have bob subscribe to `alice`, then change alice's presence.

Integration tests, against a running stack:

```bash
go test -tags=integration -count=1 -v ./test/...
```

`make help` lists the wrappers: `build`, `test`, `check`, `up`, `down`, `logs`,
`integration`, `load`, `chaos`.

### Configuration

| Variable | Default | Meaning |
|---|---|---|
| `BEACON_GATEWAY_ID` | container hostname | Ring key and session owner; unique per replica |
| `BEACON_HTTP_ADDR` | `:8080` | Listen address |
| `BEACON_REDIS_ADDR` | `localhost:6379` | Store, pub/sub and registry |
| `BEACON_DEV_TOKEN` | `beacon-dev-token` | Dev-mode shared secret; never logged |
| `BEACON_SESSION_TTL` | `30s` | Session lifetime without a client heartbeat |
| `BEACON_NODE_TTL` | `6s` | Ring membership lifetime without a registry heartbeat |
| `BEACON_REAPER_INTERVAL` | `2s` | Sweep cadence |
| `BEACON_DRIFT_INTERVAL` | `1s` | Reconciliation cadence |
| `BEACON_REAPER_BATCH` | `256` | `HMGET`s per pipeline. Larger means fewer round trips and longer bursts. |
| `BEACON_REAPER_SCAN_PAGE` | `512` | `SSCAN` `COUNT` hint while walking the session index |
| `BEACON_REAPER_BUDGET` | `1500ms` | A sweep exceeding this stops instead of overlapping the next tick. Keep it below `BEACON_REAPER_INTERVAL`. |
| `BEACON_REDIS_POOL` | `64` | Connection pool for the request path |
| `BEACON_REDIS_BG_POOL` | `8` | Separate pool for the reaper and reconciler, so cleanup cannot starve JOIN |

## HTTP surface

| Endpoint | Purpose |
|---|---|
| `GET /healthz` | Process liveness. Does not consult Redis. |
| `GET /readyz` | Whether this replica should take new connections. |
| `GET /metrics` | Prometheus exposition |
| `GET /debug/ring` | Ring membership and shard ownership as JSON |
| `GET /ws` | Client WebSocket upgrade |
| `GET /` | Static demo client |

Liveness and readiness are separate. A gateway that has lost Redis is still
alive, and restarting it would drop healthy WebSocket connections without fixing
anything, so it reports unready and keeps serving what it holds.

## Benchmark results

Measured on this machine. Raw output is in [`bench/results/`](bench/results/).

**Hardware:** AMD Ryzen 7 260 (8 cores / 16 threads), 31.3 GB RAM, Windows 11,
Docker Desktop 29.4.3 on WSL2 with 16 CPUs and 15.3 GB allocated. All six
containers and the k6 load generator share that budget.

### Connection ceiling

The 10,000 target was met on the first run, so the ramp continued until it broke.
That first ceiling was the reaper's fault, and the second set of runs is after
fixing it — same machine, same script, same parameters, chunked sweep on its own
Redis connection pool.

| Connections | Connect p99 | JOIN p95 | JOIN p99 | JOINs answered | Verdict |
|---:|---:|---:|---:|---:|---|
| 10,000 | 44 ms | 18 ms | 27 ms | 100% (204,850) | clean |
| 20,000 | 59 ms | 18 ms | 38 ms | 100% (469,215) | clean |
| 30,000 | 227 ms | 195 ms | 1,323 ms | 100% (784,130) | latency threshold missed |
| 40,000 | 4,429 ms | 4,498 ms | 4,894 ms | 97.2% (900,641 / 926,347) | broken, 43 socket errors |

**After chunking the sweep** ([`perf/chunked-reaper`](#perfchunked-reaper--chunked-sweep-and-a-dedicated-redis-pool)):

| Connections | Connect p99 | JOIN p95 | JOIN p99 | JOINs answered | Verdict |
|---:|---:|---:|---:|---:|---|
| 20,000 | 37 ms | 5 ms | 9 ms | 100% (469,504) | clean |
| 30,000 | 69 ms | 63 ms | 75 ms | 100% (794,185) | clean, all thresholds passed |
| 40,000 | 49 ms | 42 ms | 53 ms | 100% (1,178,401) | clean, no socket errors |
| 50,000 | 144 ms | 79 ms | 230 ms | 100% (1,621,767) | clean, all thresholds passed |

The highest fully clean result moved from **20,000 to at least 50,000**. The
ceiling was not found: 50,000 still passes every threshold, and past that the
single k6 container holding the sockets starts competing for the same 15.3 GB,
so a failure there would describe the load generator rather than Beacon.

At matched load the difference is the whole point of the change:

| | 30,000 before | 30,000 after | 40,000 before | 40,000 after |
|---|---:|---:|---:|---:|
| JOIN p99 | 1,323 ms | **75 ms** | 4,894 ms | **53 ms** |
| JOIN p95 | 195 ms | **63 ms** | 4,498 ms | **42 ms** |
| JOIN max | 4,072 ms | **125 ms** | 6,361 ms | **139 ms** |
| Connect p99 | 227 ms | **69 ms** | 4,429 ms | **49 ms** |
| JOINs answered | 100% | 100% | 97.2% | **100%** |

Reproduce the comparison from the committed raw output with
`python3 bench/compare.py 40000 40000-chunked`.

Every JOIN here is a cross-node resolution. The load script pairs each client
with a peer on a different gateway, so there is no local-lookup fast path.
Gateway-side counters agree with the client figures: at 10,000 the three replicas
reported 3,600 / 3,200 / 3,200 active connections.

### What limited it

The usual suspects were ruled out from the gateways' own process metrics at peak.

| Resource | Peak | Limit | Binding? |
|---|---:|---:|---|
| Open file descriptors | 13,673 | 1,048,576 | No |
| Resident memory per gateway | 505 MB | ~15 GB available | No |
| CPU per gateway | 1.55 cores | 16 cores | No |
| Ephemeral ports | — | per-container namespace | No |
| Reaper sweep p99 | ≥ 5 s | 2 s interval | **Yes** |

The reaper read the whole session index with `SMEMBERS` and then issued one
pipelined `HMGET` per session it owned. At 40,000 sessions that is roughly 13,000
commands in a single burst per gateway. Redis is single-threaded, so a JOIN
lookup issued during that burst waits behind every command still queued ahead of
it — and both came from the same connection pool. JOIN latency tracked sweep
duration almost exactly, and once a sweep outlasted its own 2 s interval, sweeps
overlapped and cleanup fell behind as well.

Nothing there was Redis being slow. It was one client deciding to use the whole
server at once.

After the fix, across 1,995 sweeps on three gateways spanning all four load
levels, 11 hit the time budget and stopped early rather than overrunning; on
gateway-1, 647 of 664 sweeps finished within one second.

### Killing a gateway under load

`docker kill` on `gateway-2` with 6,000 connections established. SIGKILL, so
there is no graceful deregistration and the ring has to notice via TTL expiry.

| Measurement | Result |
|---|---:|
| Connections before the kill | 6,000 |
| Sessions dropped | 1,920 (everything the victim held) |
| Connections retained on survivors | 4,080 |
| Drift first non-zero | T+6.60 s |
| Peak drift | −1,920 |
| Drift back to zero | T+9.56 s (3.0 s to reconcile) |
| Ring excluded the dead node | 9.54 s |
| Orphaned sessions reclaimed | 1,920, exactly the victim's count |
| JOINs served by survivors during the window | 4,241 |
| Restarted node rejoined the ring | 1.73 s |

The victim's registry key expires 6 s after the kill, at which point its
connections leave the in-memory total while its sessions remain in Redis, so
drift drops to −1,920. Survivors rebuild the ring, take ownership of the orphaned
shards and reclaim them over the next few sweeps. The committed drift trace shows
each step: `−1920 → −1883 → −307 → 0`.

Nothing was lost or double-counted, and the survivors kept answering JOINs
throughout. Failover time is dominated by `BEACON_NODE_TTL` at 6 s. Lowering it
shortens failover, at the cost of evicting healthy gateways during a GC pause.

Reproduce with:

```bash
docker compose -f deploy/docker-compose.yml run --rm -e LOAD_VUS=20000 -e CONNS_PER_VU=40 k6 run /scripts/load.js
```

```bash
bash bench/chaos.sh 6000 beacon-gateway-2
```

## Design decisions

**Consistent hashing for reaper ownership.** Every replica can see every expired
session in Redis, so without coordination all three would scan the same keys and
race to publish duplicate `OFFLINE` transitions. A ring gives each shard one
owner, and unlike plain modulo, only the departed node's shards move when a node
leaves. Survivors keep their existing assignments, which matters most during
failover.

**Redis pub/sub instead of node-to-node gossip.** Gossip means every gateway
holding connections to every other, an O(N²) mesh with its own membership and
retry logic. Redis is already required for session state, so routing fan-out
through it keeps one coordination mechanism instead of two. The costs are real:
Redis becomes a single point of failure, and pub/sub is at-most-once, so a
subscriber disconnected at the moment of publish misses the event.
Snapshot-on-subscribe and the drift reconciler cover that rather than assuming
delivery.

**Duplicate sessions: last writer wins.** A new connection takes the session and
the old one is evicted. Rejecting the newcomer would strand a user whose previous
session is a half-dead socket the owning gateway has not noticed yet, which is
the common case. The eviction notice goes to the specific gateway holding the old
session over a per-gateway control channel, costing one subscription per gateway
rather than one per connection.

The evicted client gets an `OFFLINE` frame and an `ERROR`, then is closed, but
that `OFFLINE` is not published to the bus. The session ended; the user did not.
Publishing it cluster-wide would race the new session's `ONLINE` and could leave
watchers believing a connected user is offline. Eviction still holds if the
notice never arrives: the evicted connection's next heartbeat fails the
session-ID check in Redis and closes itself.

**Stateless gateways.** In-memory connection tables exist for efficiency. Redis
is authoritative, and divergence between them is reported via
`beacon_presence_drift` rather than reconciled quietly.

## Known limitations

- **The connection ceiling is unknown.** 50,000 is clean and the reaper is no
  longer what binds; finding the real limit needs a load generator that is not
  sharing a 15.3 GB budget with the service under test.
- **A sweep can still exceed its budget by one batch.** The deadline is checked
  between batches, so a sweep whose final batch is slow finishes late rather
  than being interrupted. Bounding it harder would mean abandoning work already
  paid for.
- **Redis is a single point of failure.** Losing it does not drop existing
  connections, but no presence propagates and no JOIN resolves.
- **Pub/sub is at-most-once.** A subscriber disconnected at the instant of a
  publish misses that event.
- **Failover time is mostly a TTL.** 6 s of the ~9.5 s convergence is
  `BEACON_NODE_TTL` elapsing.
- **`token` is not authentication**, and there is no persistence, no multi-region
  support, and no sharding of Redis itself.

## Repository layout

```
cmd/gateway/          gateway process, WebSocket transport, HTTP surface
internal/ring/        consistent hash ring + tests
internal/protocol/    frame types, codec + tests
internal/metrics/     Prometheus collectors
internal/presence/    session store, pub/sub, reaper, integrity checks
web/                  static demo client
deploy/               compose, Dockerfile, prometheus, grafana
bench/                load.js, chaos.sh, results/
test/                 cross-node integration tests
docs/                 architecture notes
```

## Build log

Built one step per branch. Each entry covers what that branch was for.

### `scaffold` — repository skeleton and a gateway that runs

Set up the Go module, the package tree with each package's responsibility as a
doc comment, and a Makefile. The gateway is a real process from the start: env
config, structured `slog` output, graceful SIGTERM drain, and separate liveness
and readiness probes. `.gitattributes` forces LF endings so shell scripts and the
Makefile survive being run in Linux containers from a Windows working copy.

*Verified:* build, vet, gofmt clean, 5 tests, race-clean in a container, plus a
runtime check of the probes.

### `ring` — consistent hash ring, protocol codec, metrics

Three packages testable without Redis or a network. The ring places each gateway
at 150 virtual positions and rebuilds on membership change, so assignment depends
only on the member set and never on the order membership was learned. The codec
validates in two stages: envelope first (8KB cap before parsing, UTF-8, JSON,
type allowed from a client), then payload. The metrics package declares all 28
collectors centrally and pre-seeds known label values so a check that has not
fired reads zero instead of "No data".

Adding `prometheus/client_golang` raised the Go directive from 1.22 to 1.25.

*Verified:* 56 tests, 74 subtests, race-clean, `FuzzDecode` at 390,827 executions
with no crash. Ring distribution across 3 nodes was 31.02 / 35.35 / 33.63% over
100,000 keys, worst deviation 6.93%. Removing a node reassigned exactly the 6,967
keys it owned and moved no others. Adding a fourth moved 22.57% of keys, all to
the new node. `Lookup` benchmarks at 197.8 ns/op.

### `system` — presence layer, gateway, demo client, deploy stack

Session state moved into Redis behind Lua scripts, because three gateways can act
on the same user concurrently and a read-modify-write in Go would let a stale
presence overwrite a fresh one between the read and the write. Node liveness is a
TTL rather than a flag, so a dead gateway disappears on its own and there is no
failover path that can itself fail. The reaper handles both expired sessions and
sessions whose gateway vanished, only for shards the ring assigns it. The gateway
added WebSocket transport, `/metrics`, `/debug/ring` and the demo client, and
`deploy/` brings up three replicas with Prometheus and Grafana provisioned as
code.

One bug found here: `close()` tore down the socket directly, so a rejected client
never received the `ERROR` explaining why. The write pump now owns the socket and
drains before closing.

*Verified:* 121 tests and 86 subtests, race-clean. Against the live stack, all
three gateways agreed on ring membership, presence propagated gateway-1 to
gateway-3, `JOIN` resolved a cross-node target from all three, duplicate eviction
worked across gateways, and drift read zero everywhere. Every Grafana panel query
was checked against live Prometheus data.

### `bench` — load and chaos measurement

The k6 script uses `k6/experimental/websockets` rather than the blocking `k6/ws`,
since one VU per connection would need 10,000 VUs and several GB of generator
overhead. k6 would have run out of memory before Beacon ran out of capacity, and
the resulting number would describe the load generator. Each client is paired
with a peer on a different gateway, so every JOIN measured crosses nodes.

Two measurement bugs were worth their cost. The chaos script first reported that
drift never went non-zero, which was wrong: the gauge updated every 5 s and the
convergence loop's nine HTTP calls per iteration gave it about 2 s resolution, so
a 3 s transient fell between samples. Tightening the gauge to 1 s and adding a
dedicated 10 Hz sampler turned that into a real timing. Separately, Git Bash's
MSYS path conversion rewrote the container-side `/scripts/load.js` into
`C:/Program Files/Git/scripts/load.js`, so k6 started, found nothing and exited,
reporting zero connections instead of an error.

*Verified:* four load runs and one chaos run, raw output committed. Headline
numbers are in [Benchmark results](#benchmark-results).

### `perf/chunked-reaper` — chunked sweep and a dedicated Redis pool

The `bench` branch left the throughput ceiling diagnosed but not fixed: the
reaper read the whole index with `SMEMBERS` and fired one pipelined `HMGET` per
owned session, ~13,000 commands in a single burst, from the same connection pool
serving JOIN. This branch does both of the fixes that entry proposed, because
they solve different halves of the problem. Chunking shortens each burst; only a
separate pool makes it structurally impossible for cleanup to hold the
connections clients are waiting on. The sweep now walks the index with `SSCAN`,
flushes `HMGET` in batches of 256, and stops at a 1.5 s budget rather than
overlapping the next tick.

The interesting bug was one this change introduced and its own tests caught.
Reaping *removes members from the very set being scanned* — both `ForgetUser`
and `Delete` `SREM` from it — and `SSCAN` only promises to return members
present for the whole iteration. Deleting while walking made the cursor skip
entries it had not reached yet, and the first chunked implementation silently
under-reaped by about a quarter: 228 of 300 sessions on a three-gateway ring.
That is the worst shape a bug can have here. The sweep reports success, drift
stays non-zero, and nothing points at the reaper. The fix is to separate the two
phases — walk the index and decide, then act — so the cursor is never
invalidated by the reaper's own writes. `SSCAN` may also return a member twice,
which matters because `SREM` on an absent member succeeds: without a per-sweep
dedup the same expired session would increment `sessions_reaped_total` twice and
publish a second OFFLINE that subscribers would see as a real transition.

Three metrics were added so the sweep's shape is visible from outside the
process: `reaper_batches_total`, `reaper_scan_pages_total` and
`reaper_sweeps_truncated_total`. The last one is the important one — it makes
"cleanup is falling behind" an explicit signal, where previously that state
arrived silently as sweeps ran past their own interval.

*Verified:* `go test ./...` and `-race` clean, cross-node integration tests
against a live three-replica stack, and four load runs at 20,000 / 30,000 /
40,000 / 50,000 with raw output committed beside the originals. JOIN p99 at
40,000 went 4,894 ms to 53 ms, and the highest clean result moved from 20,000 to
at least 50,000.

## License

Unlicensed personal project. All dependencies are free and open source. Nothing
here needs an account, an API key, or a paid service.

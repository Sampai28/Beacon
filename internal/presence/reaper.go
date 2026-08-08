package presence

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/protocol"
	"github.com/Sampai28/Beacon/internal/ring"
)

// Reaper removes sessions that no live connection stands behind.
//
// Two distinct failures produce such sessions, and they need different fixes:
//
//   - The session hash expired because heartbeats stopped, but the userId
//     remains in the session index. Integrity check 4.
//   - The session hash is alive, but the gateway named in it has vanished from
//     the registry. Its TTL has not lapsed yet, so nothing else will clean it up
//     and JOIN would keep resolving to a node that is gone. Integrity check 5.
//
// Both are handled by the ring-designated owner of the user's shard, so the work
// happens once per sweep across the cluster rather than once per gateway.
type Reaper struct {
	store     *Store
	registry  *Registry
	bus       *Bus
	ring      *ring.Ring
	gatewayID string
	m         *metrics.Metrics
	log       *slog.Logger
}

func NewReaper(
	store *Store,
	registry *Registry,
	bus *Bus,
	r *ring.Ring,
	gatewayID string,
	m *metrics.Metrics,
	log *slog.Logger,
) *Reaper {
	return &Reaper{
		store: store, registry: registry, bus: bus, ring: r,
		gatewayID: gatewayID, m: m, log: log,
	}
}

// SweepResult reports what one pass did. Returned rather than only counted so
// tests can assert on behaviour instead of scraping metrics.
type SweepResult struct {
	Owned    int
	Expired  int
	Orphaned int
	Skipped  int
}

// Sweep runs one reaping pass.
//
// Ring membership is refreshed from the registry first. Doing it here rather
// than on a separate timer means ownership and the data being reaped are always
// derived from the same view of the cluster — a node cannot reap using stale
// membership it inherited from a previous tick.
func (r *Reaper) Sweep(ctx context.Context) (SweepResult, error) {
	start := time.Now()
	r.m.ReaperRuns.Inc()
	defer func() { r.m.ReaperDuration.Observe(time.Since(start).Seconds()) }()

	var res SweepResult

	nodes, err := r.registry.LiveNodes(ctx)
	if err != nil {
		r.m.RedisErrors.WithLabelValues("registry_read").Inc()
		return res, err
	}

	liveIDs := make([]string, 0, len(nodes))
	liveSet := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		liveIDs = append(liveIDs, n.GatewayID)
		liveSet[n.GatewayID] = struct{}{}
	}

	before := r.ring.Members()
	r.ring.Set(liveIDs)
	if !sameStrings(before, r.ring.Members()) {
		r.m.RingRebuilds.Inc()
		r.log.Info("ring membership changed",
			"before", before, "after", r.ring.Members())
	}
	r.m.RingMembers.Set(float64(r.ring.Len()))

	if r.ring.Len() == 0 {
		// No live gateways means this node cannot even see itself in the
		// registry. Reaping now would delete sessions on the strength of a view
		// we already know is broken.
		return res, nil
	}

	userIDs, err := r.store.IndexedUsers(ctx)
	if err != nil {
		r.m.RedisErrors.WithLabelValues("indexed_users").Inc()
		return res, err
	}

	owned := make([]string, 0, len(userIDs))
	for _, id := range userIDs {
		if r.ring.Owns(id, r.gatewayID) {
			owned = append(owned, id)
		} else {
			res.Skipped++
		}
	}
	res.Owned = len(owned)
	r.m.ReaperOwnedKeys.Set(float64(len(owned)))

	if len(owned) == 0 {
		return res, nil
	}

	// One pipelined HMGET per owned user rather than HGETALL: the sweep only
	// needs three fields, and at load-test connection counts the difference in
	// bytes moved is the difference between a sweep that keeps up and one that
	// does not.
	pipe := r.store.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(owned))
	for i, id := range owned {
		cmds[i] = pipe.HMGet(ctx, SessionKey(id), "sessionId", "gatewayId", "lastSeen")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		r.m.RedisErrors.WithLabelValues("reaper_scan").Inc()
		return res, err
	}

	now := NowMillis()
	for i, id := range owned {
		vals, err := cmds[i].Result()
		if err != nil && err != redis.Nil {
			continue
		}

		sessionID, _ := vals[0].(string)
		gatewayID, _ := vals[1].(string)

		switch {
		case sessionID == "":
			// The hash expired; only the index entry survives.
			if err := r.store.ForgetUser(ctx, id); err != nil {
				r.m.RedisErrors.WithLabelValues("forget_user").Inc()
				continue
			}
			r.m.SessionsReaped.Inc()
			r.publishOffline(ctx, id, now)
			res.Expired++

		case !isLive(liveSet, gatewayID):
			// The session is alive but its gateway is not. Left alone it would
			// keep answering JOIN with a node that no longer exists, until its
			// TTL happens to lapse.
			ok, err := r.store.Delete(ctx, id, sessionID)
			if err != nil {
				r.m.RedisErrors.WithLabelValues("reclaim_orphan").Inc()
				continue
			}
			if !ok {
				// Another connection claimed the user between the scan and the
				// delete. That session is current, so leaving it is correct.
				continue
			}
			r.m.OrphanSessionsReclaimed.Inc()
			r.publishOffline(ctx, id, now)
			res.Orphaned++
			r.log.Info("reclaimed orphaned session",
				"user_id", id, "dead_gateway", gatewayID)
		}
	}

	return res, nil
}

// publishOffline announces a reaped session. Failure is counted but not
// propagated: the session is already gone from Redis, and refusing to continue
// the sweep because one notification failed would leave the rest of the shard
// uncleaned.
func (r *Reaper) publishOffline(ctx context.Context, userID string, ts int64) {
	err := r.bus.Publish(ctx, protocol.Presence{
		UserID: userID,
		Status: protocol.StatusOffline,
		TS:     ts,
	})
	if err != nil {
		r.log.Warn("could not publish OFFLINE for reaped session",
			"user_id", userID, "err", err)
		return
	}
	r.m.PresenceEvents.WithLabelValues(string(protocol.StatusOffline)).Inc()
}

// Run sweeps on a ticker until ctx is cancelled.
func (r *Reaper) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if _, err := r.Sweep(ctx); err != nil && ctx.Err() == nil {
				r.log.Warn("reaper sweep failed", "err", err)
			}
		}
	}
}

func isLive(live map[string]struct{}, gatewayID string) bool {
	if gatewayID == "" {
		// A session with no gateway recorded is malformed. Treating it as live
		// would make it permanently unreapable.
		return false
	}
	_, ok := live[gatewayID]
	return ok
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

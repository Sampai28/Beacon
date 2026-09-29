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
//
// # Why the sweep is chunked
//
// The first version read the whole session index with SMEMBERS and then queued
// one HMGET per owned session into a single pipeline. That is one enormous
// burst: at 40,000 sessions across three gateways it is roughly 13,000 commands
// arriving at Redis back to back, on connections taken from the same pool the
// request path uses. Redis is single-threaded, so a JOIN lookup issued during
// that burst waits behind every command still queued ahead of it. Measured, the
// sweep's p99 crossed its own 2 s interval somewhere between 20,000 and 30,000
// connections, and JOIN p99 went from 38 ms to 1.3 s with it.
//
// Nothing about that is Redis being slow. It is one client deciding to use the
// whole server at once. So the sweep now walks the index with SSCAN and flushes
// work in bounded batches, yielding between them, and a sweep that cannot
// finish inside its budget stops rather than overlapping the next tick.
type Reaper struct {
	store     *Store
	registry  *Registry
	bus       *Bus
	ring      *ring.Ring
	gatewayID string
	m         *metrics.Metrics
	log       *slog.Logger

	batchSize int
	scanPage  int64
	budget    time.Duration
}

// ReaperOptions tunes the sweep's shape. Zero values take the defaults, which
// are what the shipped configuration uses.
type ReaperOptions struct {
	// BatchSize is how many HMGETs go into one pipeline. The tradeoff is
	// round trips against how long a single burst occupies Redis; 256 is
	// roughly 20x fewer round trips than issuing commands singly while still
	// leaving gaps a JOIN can land in.
	BatchSize int

	// ScanPage is the SSCAN COUNT hint.
	ScanPage int64

	// Budget bounds one sweep. Exceeding it stops the sweep early and counts
	// it, which is strictly better than running past the interval: overlapping
	// sweeps do the same work twice and compound the contention that made the
	// sweep slow in the first place. Whatever is left is picked up next tick,
	// because SSCAN restarts from the beginning and the index is shuffled by
	// then anyway.
	Budget time.Duration
}

const (
	defaultBatchSize = 256
	defaultBudget    = 1500 * time.Millisecond
)

func (o ReaperOptions) withDefaults() ReaperOptions {
	if o.BatchSize <= 0 {
		o.BatchSize = defaultBatchSize
	}
	if o.ScanPage <= 0 {
		o.ScanPage = defaultScanPage
	}
	if o.Budget <= 0 {
		o.Budget = defaultBudget
	}
	return o
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
	return NewReaperWithOptions(store, registry, bus, r, gatewayID, m, log, ReaperOptions{})
}

func NewReaperWithOptions(
	store *Store,
	registry *Registry,
	bus *Bus,
	r *ring.Ring,
	gatewayID string,
	m *metrics.Metrics,
	log *slog.Logger,
	opts ReaperOptions,
) *Reaper {
	opts = opts.withDefaults()
	return &Reaper{
		store: store, registry: registry, bus: bus, ring: r,
		gatewayID: gatewayID, m: m, log: log,
		batchSize: opts.BatchSize,
		scanPage:  opts.ScanPage,
		budget:    opts.Budget,
	}
}

// SweepResult reports what one pass did. Returned rather than only counted so
// tests can assert on behaviour instead of scraping metrics.
type SweepResult struct {
	Owned    int
	Expired  int
	Orphaned int
	Skipped  int

	// Pages is SSCAN round trips, Batches is HMGET pipelines flushed. Both are
	// reported so a test can assert the sweep actually chunked rather than
	// quietly falling back to one giant batch.
	Pages   int
	Batches int

	// Truncated means the budget ran out before the index was fully walked.
	// The counts above then describe a partial pass.
	Truncated bool
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

	deadline := start.Add(r.budget)

	// Phase 1: walk the index and decide what this gateway is responsible for.
	// Phase 2: act on it.
	//
	// The two phases are separate because reaping *removes members from the
	// very set being scanned* — ForgetUser and Delete both SREM from it. SSCAN
	// only promises to return members that are present for the whole
	// iteration; deleting as you walk can make the cursor skip members it has
	// not reached yet. Interleaving the two silently under-reaped by roughly a
	// quarter in testing, which is the worst kind of bug here: the sweep
	// reports success, drift stays non-zero, and nothing points at the reaper.
	//
	// Collecting first costs one slice of owned ids, which is a fraction of
	// the index and lives for one sweep. What this rewrite had to stop
	// materialising was the Redis command burst, not a Go slice.
	owned, err := r.collect(ctx, deadline, &res)
	if err != nil {
		return res, err
	}
	r.m.ReaperOwnedKeys.Set(float64(res.Owned))

	for i := 0; i < len(owned); i += r.batchSize {
		end := i + r.batchSize
		if end > len(owned) {
			end = len(owned)
		}
		if err := r.flush(ctx, owned[i:end], liveSet, &res); err != nil {
			return res, err
		}
		if end < len(owned) && r.outOfTime(ctx, deadline) {
			res.Truncated = true
			break
		}
	}

	// Reported once, here, rather than at each place the budget can run out:
	// a sweep truncated during the walk and again during the flush is still
	// one truncated sweep, and counting it twice would overstate how often
	// cleanup is falling behind.
	if res.Truncated {
		r.reportTruncated(res)
	}
	return res, nil
}

// collect walks the session index and returns the user ids this gateway owns.
//
// It performs no writes, which is what makes the SSCAN cursor trustworthy.
func (r *Reaper) collect(ctx context.Context, deadline time.Time, res *SweepResult) ([]string, error) {
	// SSCAN may also return the same member twice when the set is rehashed
	// mid-walk. Acting on a duplicate is not harmless: SREM on an absent
	// member succeeds, so the same expired session would increment
	// sessions_reaped_total twice and publish a second OFFLINE for a user who
	// is already offline, which subscribers would see as a real transition.
	seen := make(map[string]struct{}, r.batchSize)
	owned := make([]string, 0, r.batchSize)

	var cursor uint64
	for {
		page, next, err := r.store.ScanIndexedUsers(ctx, cursor, r.scanPage)
		if err != nil {
			r.m.RedisErrors.WithLabelValues("indexed_users").Inc()
			return nil, err
		}
		res.Pages++
		r.m.ReaperScanPages.Inc()

		for _, id := range page {
			if !r.ring.Owns(id, r.gatewayID) {
				res.Skipped++
				continue
			}
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			owned = append(owned, id)
		}
		res.Owned = len(owned)

		if next == 0 {
			return owned, nil
		}
		cursor = next

		// An index made almost entirely of shards this gateway does not own
		// still costs a scan per page, so a sweep can run long on the walk
		// alone and the budget has to apply here too.
		if r.outOfTime(ctx, deadline) {
			res.Truncated = true
			return owned, nil
		}
	}
}

// outOfTime reports whether the sweep should stop now.
func (r *Reaper) outOfTime(ctx context.Context, deadline time.Time) bool {
	if ctx.Err() != nil {
		return true
	}
	return !time.Now().Before(deadline)
}

func (r *Reaper) reportTruncated(res SweepResult) {
	r.m.ReaperSweepsTruncated.Inc()
	r.m.ReaperOwnedKeys.Set(float64(res.Owned))
	r.log.Warn("reaper sweep hit its budget and stopped early",
		"budget", r.budget,
		"pages", res.Pages,
		"batches", res.Batches,
		"owned_seen", res.Owned,
		"expired", res.Expired,
		"orphaned", res.Orphaned)
}

// flush reads one batch of sessions and acts on whatever is wrong with them.
//
// One pipelined HMGET per user rather than HGETALL: the sweep needs three
// fields, and at these connection counts the bytes not moved are the difference
// between a sweep that keeps up and one that does not.
func (r *Reaper) flush(ctx context.Context, ids []string, liveSet map[string]struct{}, res *SweepResult) error {
	if len(ids) == 0 {
		return nil
	}
	res.Batches++
	r.m.ReaperBatches.Inc()

	pipe := r.store.rdb.Pipeline()
	cmds := make([]*redis.SliceCmd, len(ids))
	for i, id := range ids {
		cmds[i] = pipe.HMGet(ctx, SessionKey(id), "sessionId", "gatewayId", "lastSeen")
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		r.m.RedisErrors.WithLabelValues("reaper_scan").Inc()
		return err
	}

	now := NowMillis()
	for i, id := range ids {
		vals, err := cmds[i].Result()
		if err != nil && err != redis.Nil {
			continue
		}
		if len(vals) < 2 {
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
	return nil
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

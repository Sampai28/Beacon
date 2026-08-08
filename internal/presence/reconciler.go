package presence

import (
	"context"
	"log/slog"
	"time"

	"github.com/Sampai28/Beacon/internal/metrics"
)

// Reconciler compares what the gateways believe against what Redis records.
//
// This is integrity check 6, and it is the one check that can catch bugs the
// others cannot. Every other check validates a single operation in isolation;
// drift validates the *aggregate* — if eviction, reaping and orphan reclamation
// are each individually correct but their interaction loses a session, no
// per-operation check notices, and drift does.
//
// Under steady state it must read zero. The interesting question is not whether
// it is ever non-zero — it goes non-zero briefly whenever a gateway dies — but
// how quickly it returns to zero afterwards, which is what the chaos benchmark
// measures.
type Reconciler struct {
	store    *Store
	registry *Registry
	m        *metrics.Metrics
	log      *slog.Logger
}

func NewReconciler(store *Store, registry *Registry, m *metrics.Metrics, log *slog.Logger) *Reconciler {
	return &Reconciler{store: store, registry: registry, m: m, log: log}
}

// DriftReport is one reconciliation pass.
type DriftReport struct {
	// InMemory is the sum of connection counts every live gateway last reported
	// through its registry heartbeat.
	InMemory int64
	// InRedis is the cardinality of the session index.
	InRedis int64
	// Drift is InMemory - InRedis. Signed on purpose: the direction says which
	// side is stale. Positive means gateways believe in connections Redis has no
	// record of; negative means Redis holds sessions no gateway is serving.
	Drift int64
}

// Check runs one reconciliation pass and publishes the result.
func (r *Reconciler) Check(ctx context.Context) (DriftReport, error) {
	var rep DriftReport

	inRedis, err := r.store.SessionCount(ctx)
	if err != nil {
		r.m.RedisErrors.WithLabelValues("session_count").Inc()
		return rep, err
	}

	inMemory, err := r.registry.TotalConnections(ctx)
	if err != nil {
		r.m.RedisErrors.WithLabelValues("registry_read").Inc()
		return rep, err
	}

	rep = DriftReport{InMemory: inMemory, InRedis: inRedis, Drift: inMemory - inRedis}
	r.m.RecordDrift(inMemory, inRedis)
	return rep, nil
}

// Run reconciles on a ticker until ctx is cancelled.
//
// Sustained non-zero drift is logged rather than only exported, because it is
// the signal that some invariant is broken and a metric nobody is watching is
// no signal at all. Transient drift is expected — a heartbeat is up to one
// interval stale by construction — so only a run of consecutive non-zero
// readings is worth a line in the log.
func (r *Reconciler) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	const sustainedThreshold = 3
	consecutive := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			rep, err := r.Check(ctx)
			if err != nil {
				if ctx.Err() == nil {
					r.log.Warn("drift check failed", "err", err)
				}
				continue
			}

			if rep.Drift == 0 {
				if consecutive >= sustainedThreshold {
					r.log.Info("presence drift returned to zero",
						"after_checks", consecutive)
				}
				consecutive = 0
				continue
			}

			consecutive++
			if consecutive == sustainedThreshold {
				r.log.Warn("presence drift is sustained",
					"drift", rep.Drift,
					"in_memory", rep.InMemory,
					"in_redis", rep.InRedis,
					"consecutive_checks", consecutive)
			}
		}
	}
}

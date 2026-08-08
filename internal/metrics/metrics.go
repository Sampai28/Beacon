package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Namespace prefixes every collector. Grafana dashboards in deploy/ bind to
// these exact names, so changing one silently empties a panel — hence the
// round-trip test asserting the full name set.
const Namespace = "beacon"

// Frame rejection reasons, matching protocol error codes. Pre-declared so the
// Integrity dashboard shows a zero series rather than "No data" before the first
// bad frame arrives — an empty panel is indistinguishable from a broken one.
var frameRejectReasons = []string{
	"BAD_FRAME",
	"UNKNOWN_TYPE",
	"MISSING_FIELD",
	"INVALID_FIELD",
	"FRAME_TOO_LARGE",
	"UNAUTHORIZED",
}

// Connection close reasons.
var closeReasons = []string{
	"client_close",
	"read_error",
	"write_error",
	"evicted_duplicate",
	"shutdown",
	"reaped",
}

// Join outcomes.
var joinResults = []string{"ok", "denied", "error"}

// Metrics is every collector Beacon exports.
//
// They are declared in one place rather than beside their call sites so the
// checked-in dashboards have a single authoritative list to bind against, and so
// adding an integrity check without exposing it is an obvious omission rather
// than a silent one.
type Metrics struct {
	// --- Connection and traffic ---------------------------------------------

	// ConnectionsActive is this gateway's live WebSocket count. Summed across
	// replicas it is one half of the drift comparison; Redis session-set
	// cardinality is the other.
	ConnectionsActive prometheus.Gauge
	ConnectionsTotal  prometheus.Counter
	ConnectionsClosed *prometheus.CounterVec

	FramesReceived prometheus.Counter
	FramesSent     prometheus.Counter

	// --- Presence -----------------------------------------------------------

	PresenceEvents      *prometheus.CounterVec
	PresenceFanout      prometheus.Counter
	SubscriptionsActive prometheus.Gauge
	JoinDuration        *prometheus.HistogramVec
	JoinsTotal          *prometheus.CounterVec

	// --- Integrity check 1: frame validation --------------------------------

	FramesRejected *prometheus.CounterVec

	// --- Integrity check 2: duplicate-session policy ------------------------

	DuplicateSessionsEvicted prometheus.Counter

	// --- Integrity check 3: out-of-order rejection --------------------------

	PresenceEventsOutOfOrder prometheus.Counter

	// --- Integrity check 4: stale-session reaper ----------------------------

	SessionsReaped  prometheus.Counter
	ReaperRuns      prometheus.Counter
	ReaperDuration  prometheus.Histogram
	ReaperOwnedKeys prometheus.Gauge

	// --- Integrity check 5: orphan detection --------------------------------

	OrphanSessionsReclaimed prometheus.Counter

	// --- Integrity check 6: drift reconciliation ----------------------------

	// PresenceDrift is the signed difference between the sum of in-memory
	// connection counts and Redis session-set cardinality. Signed, not absolute:
	// the direction says which side is wrong. Positive means gateways believe in
	// connections Redis has no record of; negative means Redis holds sessions no
	// gateway is serving.
	PresenceDrift          prometheus.Gauge
	DriftChecks            prometheus.Counter
	SessionsInRedis        prometheus.Gauge
	ConnectionsClusterwide prometheus.Gauge

	// --- Ring and registry --------------------------------------------------

	RingMembers        prometheus.Gauge
	RingRebuilds       prometheus.Counter
	RegistryHeartbeats prometheus.Counter
	RegistryFailures   prometheus.Counter

	// --- Dependencies -------------------------------------------------------

	RedisErrors  *prometheus.CounterVec
	PubSubErrors prometheus.Counter
}

// New builds and registers every collector against reg.
//
// gatewayID is attached as a constant label rather than threaded through each
// call site. Prometheus already adds an `instance` label from the scrape target,
// but that is the container's address — it changes when a replica restarts,
// while gateway_id is the same identity the ring and session hashes use. The
// dashboards need to join on the latter.
//
// Panics on duplicate registration, which can only happen if New is called twice
// against the same registry — a wiring bug that should fail at startup rather
// than produce a half-registered process.
func New(reg prometheus.Registerer, gatewayID string) *Metrics {
	r := promauto.With(prometheus.WrapRegistererWith(
		prometheus.Labels{"gateway_id": gatewayID}, reg,
	))

	m := &Metrics{
		ConnectionsActive: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "connections_active",
			Help:      "WebSocket connections currently held by this gateway.",
		}),
		ConnectionsTotal: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "connections_total",
			Help:      "WebSocket connections accepted by this gateway since start.",
		}),
		ConnectionsClosed: r.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "connections_closed_total",
			Help:      "WebSocket connections closed, by reason.",
		}, []string{"reason"}),

		FramesReceived: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "frames_received_total",
			Help:      "Frames read from clients, including those later rejected.",
		}),
		FramesSent: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "frames_sent_total",
			Help:      "Frames written to clients.",
		}),

		PresenceEvents: r.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "presence_events_total",
			Help:      "Presence transitions applied, by resulting status.",
		}, []string{"status"}),
		PresenceFanout: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "presence_fanout_total",
			Help:      "Presence events delivered to subscribed clients on this gateway.",
		}),
		SubscriptionsActive: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "subscriptions_active",
			Help:      "Distinct users this gateway is subscribed to on behalf of its clients.",
		}),

		JoinDuration: r.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "join_duration_seconds",
			Help:      "End-to-end JOIN resolution latency, by outcome.",
			// Tuned for a local Redis round trip: the interesting range is
			// hundreds of microseconds to tens of milliseconds. The default
			// buckets start at 5ms and would put almost every observation in the
			// first bucket, making p50 meaningless.
			Buckets: []float64{
				0.0005, 0.001, 0.0025, 0.005, 0.01,
				0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5,
			},
		}, []string{"result"}),
		JoinsTotal: r.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "joins_total",
			Help:      "JOIN requests handled, by outcome.",
		}, []string{"result"}),

		FramesRejected: r.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "frames_rejected_total",
			Help:      "Integrity check 1: inbound frames rejected by validation, by reason.",
		}, []string{"reason"}),

		DuplicateSessionsEvicted: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "duplicate_sessions_evicted_total",
			Help:      "Integrity check 2: older sessions evicted because the user reconnected elsewhere.",
		}),

		PresenceEventsOutOfOrder: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "presence_events_out_of_order_total",
			Help:      "Integrity check 3: presence events dropped for carrying a timestamp older than stored lastSeen.",
		}),

		SessionsReaped: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "sessions_reaped_total",
			Help:      "Integrity check 4: sessions removed after their heartbeat TTL lapsed.",
		}),
		ReaperRuns: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "reaper_runs_total",
			Help:      "Reaper sweeps executed by this gateway.",
		}),
		ReaperDuration: r.NewHistogram(prometheus.HistogramOpts{
			Namespace: Namespace,
			Name:      "reaper_duration_seconds",
			Help:      "Duration of a reaper sweep.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5},
		}),
		ReaperOwnedKeys: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "reaper_owned_sessions",
			Help:      "Sessions this gateway owns for reaping under the current ring.",
		}),

		OrphanSessionsReclaimed: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "orphan_sessions_reclaimed_total",
			Help:      "Integrity check 5: sessions reclaimed whose owning gateway is absent from the registry.",
		}),

		PresenceDrift: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "presence_drift",
			Help:      "Integrity check 6: in-memory connection count minus Redis session cardinality. Zero in steady state; sign indicates which side is stale.",
		}),
		DriftChecks: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "drift_checks_total",
			Help:      "Drift reconciliation passes executed.",
		}),
		SessionsInRedis: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "sessions_in_redis",
			Help:      "Cardinality of the Redis session set, as observed by this gateway.",
		}),
		ConnectionsClusterwide: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "connections_clusterwide",
			Help:      "Sum of per-gateway in-memory connection counts, as observed by this gateway.",
		}),

		RingMembers: r.NewGauge(prometheus.GaugeOpts{
			Namespace: Namespace,
			Name:      "ring_members",
			Help:      "Live gateways on the consistent hash ring.",
		}),
		RingRebuilds: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "ring_rebuilds_total",
			Help:      "Ring rebuilds triggered by observed membership change.",
		}),
		RegistryHeartbeats: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "registry_heartbeats_total",
			Help:      "Heartbeats this gateway wrote to the node registry.",
		}),
		RegistryFailures: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "registry_failures_total",
			Help:      "Heartbeat writes that failed; sustained non-zero means this node is about to fall off the ring.",
		}),

		RedisErrors: r.NewCounterVec(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "redis_errors_total",
			Help:      "Redis command failures, by operation.",
		}, []string{"op"}),
		PubSubErrors: r.NewCounter(prometheus.CounterOpts{
			Namespace: Namespace,
			Name:      "pubsub_errors_total",
			Help:      "Pub/sub publish or receive failures.",
		}),
	}

	m.initLabelSeries()
	return m
}

// initLabelSeries touches every known label value so its series exists at zero
// from process start.
//
// A CounterVec series does not appear in /metrics until it is first incremented,
// so a fresh gateway reports nothing for these — and a Grafana panel showing
// "No data" looks identical whether the check has never fired or the metric name
// is wrong. Pre-seeding makes a working-but-quiet check visibly zero.
func (m *Metrics) initLabelSeries() {
	for _, reason := range frameRejectReasons {
		m.FramesRejected.WithLabelValues(reason)
	}
	for _, reason := range closeReasons {
		m.ConnectionsClosed.WithLabelValues(reason)
	}
	for _, result := range joinResults {
		m.JoinsTotal.WithLabelValues(result)
		m.JoinDuration.WithLabelValues(result)
	}
	for _, status := range []string{"ONLINE", "OFFLINE", "AWAY", "IN_GAME"} {
		m.PresenceEvents.WithLabelValues(status)
	}
}

// ObserveJoin records one JOIN outcome and its latency together, so the counter
// and the histogram cannot disagree about how many joins happened.
func (m *Metrics) ObserveJoin(result string, seconds float64) {
	m.JoinsTotal.WithLabelValues(result).Inc()
	m.JoinDuration.WithLabelValues(result).Observe(seconds)
}

// RecordDrift publishes one reconciliation pass. Both inputs are exported
// alongside the difference so a non-zero drift can be attributed to a side
// without re-deriving it from other panels.
func (m *Metrics) RecordDrift(inMemory, inRedis int64) {
	m.DriftChecks.Inc()
	m.ConnectionsClusterwide.Set(float64(inMemory))
	m.SessionsInRedis.Set(float64(inRedis))
	m.PresenceDrift.Set(float64(inMemory - inRedis))
}

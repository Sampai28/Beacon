package metrics

import (
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func gather(t *testing.T, reg *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := make(map[string]*dto.MetricFamily, len(families))
	for _, f := range families {
		out[f.GetName()] = f
	}
	return out
}

func newTestMetrics(t *testing.T) (*Metrics, *prometheus.Registry) {
	t.Helper()
	reg := prometheus.NewRegistry()
	return New(reg, "gw-test"), reg
}

// The dashboards in deploy/ bind to these exact names. Renaming a collector
// without updating them empties a panel silently, so the name set is asserted
// rather than assumed. Update this list and the dashboard JSON together.
func TestExportedMetricNamesAreStable(t *testing.T) {
	_, reg := newTestMetrics(t)

	want := []string{
		"beacon_connections_active",
		"beacon_connections_closed_total",
		"beacon_connections_clusterwide",
		"beacon_connections_total",
		"beacon_drift_checks_total",
		"beacon_duplicate_sessions_evicted_total",
		"beacon_frames_received_total",
		"beacon_frames_rejected_total",
		"beacon_frames_sent_total",
		"beacon_join_duration_seconds",
		"beacon_joins_total",
		"beacon_orphan_sessions_reclaimed_total",
		"beacon_presence_drift",
		"beacon_presence_events_out_of_order_total",
		"beacon_presence_events_total",
		"beacon_presence_fanout_total",
		"beacon_pubsub_errors_total",
		"beacon_reaper_duration_seconds",
		"beacon_reaper_owned_sessions",
		"beacon_reaper_runs_total",
		// beacon_redis_errors_total is intentionally omitted: its label values
		// are Redis operation names, which are not known ahead of time, so it
		// cannot be pre-seeded. See TestRedisErrorsAppearsOnlyOnceUsed.
		"beacon_registry_failures_total",
		"beacon_registry_heartbeats_total",
		"beacon_ring_members",
		"beacon_ring_rebuilds_total",
		"beacon_sessions_in_redis",
		"beacon_sessions_reaped_total",
		"beacon_subscriptions_active",
	}

	families := gather(t, reg)
	got := make([]string, 0, len(families))
	for name := range families {
		got = append(got, name)
	}
	sort.Strings(got)

	missing := []string{}
	for _, w := range want {
		if _, ok := families[w]; !ok {
			missing = append(missing, w)
		}
	}
	if len(missing) > 0 {
		t.Errorf("expected metrics absent from /metrics: %v", missing)
	}

	wantSet := make(map[string]struct{}, len(want))
	for _, w := range want {
		wantSet[w] = struct{}{}
	}
	for _, g := range got {
		if _, ok := wantSet[g]; !ok {
			t.Errorf("undeclared metric %q is being exported; add it to this test and to the dashboards", g)
		}
	}
}

// redis_errors_total is deliberately absent from the list above: it is the one
// CounterVec with no pre-seeded label values, because the set of Redis
// operations is not known until they run. Documented here so its absence reads
// as intentional rather than as an oversight.
func TestRedisErrorsAppearsOnlyOnceUsed(t *testing.T) {
	m, reg := newTestMetrics(t)

	if _, present := gather(t, reg)["beacon_redis_errors_total"]; present {
		t.Error("beacon_redis_errors_total exists before any error; label values should not be pre-seeded")
	}

	m.RedisErrors.WithLabelValues("hgetall").Inc()

	if _, present := gather(t, reg)["beacon_redis_errors_total"]; !present {
		t.Error("beacon_redis_errors_total missing after an error was recorded")
	}
}

// Every metric carries gateway_id. Prometheus supplies an `instance` label from
// the scrape target, but that is the container address and changes on restart;
// gateway_id is the same identity the ring and session hashes use, and the
// dashboards join on it.
func TestEveryMetricCarriesGatewayIDLabel(t *testing.T) {
	_, reg := newTestMetrics(t)

	for name, family := range gather(t, reg) {
		for _, metric := range family.GetMetric() {
			found := false
			for _, label := range metric.GetLabel() {
				if label.GetName() == "gateway_id" {
					found = true
					if label.GetValue() != "gw-test" {
						t.Errorf("%s: gateway_id = %q, want gw-test", name, label.GetValue())
					}
				}
			}
			if !found {
				t.Errorf("%s is missing the gateway_id label", name)
			}
		}
	}
}

// A panel showing "No data" looks the same whether a check has never fired or
// the metric name is wrong. Pre-seeded series make a quiet check visibly zero.
func TestIntegrityCountersStartAtZeroRatherThanAbsent(t *testing.T) {
	_, reg := newTestMetrics(t)
	families := gather(t, reg)

	for _, reason := range frameRejectReasons {
		if !hasLabelValue(families["beacon_frames_rejected_total"], "reason", reason) {
			t.Errorf("beacon_frames_rejected_total has no pre-seeded series for reason=%q", reason)
		}
	}
	for _, result := range joinResults {
		if !hasLabelValue(families["beacon_joins_total"], "result", result) {
			t.Errorf("beacon_joins_total has no pre-seeded series for result=%q", result)
		}
	}
	for _, reason := range closeReasons {
		if !hasLabelValue(families["beacon_connections_closed_total"], "reason", reason) {
			t.Errorf("beacon_connections_closed_total has no pre-seeded series for reason=%q", reason)
		}
	}

	if v := counterValue(t, families["beacon_frames_rejected_total"], "reason", "BAD_FRAME"); v != 0 {
		t.Errorf("pre-seeded counter should start at 0, got %v", v)
	}
}

func TestCountersIncrement(t *testing.T) {
	m, reg := newTestMetrics(t)

	m.FramesRejected.WithLabelValues("BAD_FRAME").Inc()
	m.FramesRejected.WithLabelValues("BAD_FRAME").Inc()
	m.DuplicateSessionsEvicted.Inc()
	m.PresenceEventsOutOfOrder.Add(3)
	m.SessionsReaped.Add(7)
	m.OrphanSessionsReclaimed.Inc()

	families := gather(t, reg)

	if got := counterValue(t, families["beacon_frames_rejected_total"], "reason", "BAD_FRAME"); got != 2 {
		t.Errorf("frames_rejected_total{BAD_FRAME}: got %v, want 2", got)
	}
	if got := simpleCounterValue(t, families["beacon_duplicate_sessions_evicted_total"]); got != 1 {
		t.Errorf("duplicate_sessions_evicted_total: got %v, want 1", got)
	}
	if got := simpleCounterValue(t, families["beacon_presence_events_out_of_order_total"]); got != 3 {
		t.Errorf("presence_events_out_of_order_total: got %v, want 3", got)
	}
	if got := simpleCounterValue(t, families["beacon_sessions_reaped_total"]); got != 7 {
		t.Errorf("sessions_reaped_total: got %v, want 7", got)
	}
	if got := simpleCounterValue(t, families["beacon_orphan_sessions_reclaimed_total"]); got != 1 {
		t.Errorf("orphan_sessions_reclaimed_total: got %v, want 1", got)
	}
}

// Drift is signed on purpose: the direction identifies which side is stale.
// Positive means gateways believe in connections Redis has no record of;
// negative means Redis holds sessions no gateway is serving.
func TestRecordDriftIsSignedAndPublishesBothSides(t *testing.T) {
	cases := []struct {
		name              string
		inMemory, inRedis int64
		wantDrift         float64
	}{
		{"steady state", 100, 100, 0},
		{"gateways ahead", 105, 100, 5},
		{"redis ahead", 95, 100, -5},
		{"both empty", 0, 0, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, reg := newTestMetrics(t)
			m.RecordDrift(tc.inMemory, tc.inRedis)
			families := gather(t, reg)

			if got := gaugeValue(t, families["beacon_presence_drift"]); got != tc.wantDrift {
				t.Errorf("presence_drift: got %v, want %v", got, tc.wantDrift)
			}
			if got := gaugeValue(t, families["beacon_connections_clusterwide"]); got != float64(tc.inMemory) {
				t.Errorf("connections_clusterwide: got %v, want %v", got, tc.inMemory)
			}
			if got := gaugeValue(t, families["beacon_sessions_in_redis"]); got != float64(tc.inRedis) {
				t.Errorf("sessions_in_redis: got %v, want %v", got, tc.inRedis)
			}
			if got := simpleCounterValue(t, families["beacon_drift_checks_total"]); got != 1 {
				t.Errorf("drift_checks_total: got %v, want 1", got)
			}
		})
	}
}

// The counter and the histogram must not disagree about how many joins
// happened, which is why they are incremented through one call.
func TestObserveJoinKeepsCounterAndHistogramConsistent(t *testing.T) {
	m, reg := newTestMetrics(t)

	m.ObserveJoin("ok", 0.002)
	m.ObserveJoin("ok", 0.004)
	m.ObserveJoin("denied", 0.001)

	families := gather(t, reg)

	if got := counterValue(t, families["beacon_joins_total"], "result", "ok"); got != 2 {
		t.Errorf("joins_total{ok}: got %v, want 2", got)
	}
	if got := counterValue(t, families["beacon_joins_total"], "result", "denied"); got != 1 {
		t.Errorf("joins_total{denied}: got %v, want 1", got)
	}

	hist := findMetric(families["beacon_join_duration_seconds"], "result", "ok")
	if hist == nil {
		t.Fatal("join_duration_seconds{ok} missing")
	}
	if n := hist.GetHistogram().GetSampleCount(); n != 2 {
		t.Errorf("join_duration_seconds{ok} sample count: got %d, want 2", n)
	}
	if sum := hist.GetHistogram().GetSampleSum(); sum < 0.0059 || sum > 0.0061 {
		t.Errorf("join_duration_seconds{ok} sum: got %v, want ~0.006", sum)
	}
}

// Default Prometheus buckets start at 5ms, which would put nearly every local
// join in the first bucket and make p50 meaningless. Verify the sub-millisecond
// resolution the dashboards depend on actually exists.
func TestJoinLatencyBucketsResolveSubMillisecond(t *testing.T) {
	m, reg := newTestMetrics(t)

	m.ObserveJoin("ok", 0.0004) // 400µs

	hist := findMetric(gather(t, reg)["beacon_join_duration_seconds"], "result", "ok")
	if hist == nil {
		t.Fatal("join_duration_seconds{ok} missing")
	}

	var smallest float64 = -1
	for _, b := range hist.GetHistogram().GetBucket() {
		if smallest < 0 || b.GetUpperBound() < smallest {
			smallest = b.GetUpperBound()
		}
	}
	if smallest > 0.001 {
		t.Errorf("smallest bucket bound is %v; need <=0.001 to resolve local join latency", smallest)
	}

	for _, b := range hist.GetHistogram().GetBucket() {
		if b.GetUpperBound() == 0.0005 && b.GetCumulativeCount() != 1 {
			t.Errorf("400µs observation not counted in the 500µs bucket: got %d", b.GetCumulativeCount())
		}
	}
}

func TestAllMetricsUseTheBeaconNamespace(t *testing.T) {
	_, reg := newTestMetrics(t)

	for name := range gather(t, reg) {
		if !strings.HasPrefix(name, Namespace+"_") {
			t.Errorf("metric %q is outside the %q namespace", name, Namespace)
		}
	}
}

// Two gateways in one process is a wiring bug that must fail loudly at startup
// rather than leave the process half-registered.
func TestDuplicateRegistrationPanics(t *testing.T) {
	reg := prometheus.NewRegistry()
	New(reg, "gw-1")

	defer func() {
		if recover() == nil {
			t.Error("registering twice against one registry did not panic")
		}
	}()
	New(reg, "gw-1")
}

// Separate registries must not collide, which is what lets each test build a
// clean set of collectors.
func TestSeparateRegistriesAreIndependent(t *testing.T) {
	m1, reg1 := newTestMetrics(t)
	_, reg2 := newTestMetrics(t)

	m1.SessionsReaped.Add(5)

	if got := simpleCounterValue(t, gather(t, reg1)["beacon_sessions_reaped_total"]); got != 5 {
		t.Errorf("registry 1: got %v, want 5", got)
	}
	if got := simpleCounterValue(t, gather(t, reg2)["beacon_sessions_reaped_total"]); got != 0 {
		t.Errorf("registry 2 leaked from registry 1: got %v, want 0", got)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func findMetric(family *dto.MetricFamily, labelName, labelValue string) *dto.Metric {
	if family == nil {
		return nil
	}
	for _, m := range family.GetMetric() {
		for _, l := range m.GetLabel() {
			if l.GetName() == labelName && l.GetValue() == labelValue {
				return m
			}
		}
	}
	return nil
}

func hasLabelValue(family *dto.MetricFamily, labelName, labelValue string) bool {
	return findMetric(family, labelName, labelValue) != nil
}

func counterValue(t *testing.T, family *dto.MetricFamily, labelName, labelValue string) float64 {
	t.Helper()
	m := findMetric(family, labelName, labelValue)
	if m == nil {
		t.Fatalf("no series with %s=%q", labelName, labelValue)
	}
	return m.GetCounter().GetValue()
}

func simpleCounterValue(t *testing.T, family *dto.MetricFamily) float64 {
	t.Helper()
	if family == nil || len(family.GetMetric()) == 0 {
		t.Fatal("metric family is absent or empty")
	}
	return family.GetMetric()[0].GetCounter().GetValue()
}

func gaugeValue(t *testing.T, family *dto.MetricFamily) float64 {
	t.Helper()
	if family == nil || len(family.GetMetric()) == 0 {
		t.Fatal("metric family is absent or empty")
	}
	return family.GetMetric()[0].GetGauge().GetValue()
}

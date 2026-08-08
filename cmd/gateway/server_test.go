package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/presence"
	"github.com/Sampai28/Beacon/internal/protocol"
)

const testToken = "test-token"

// newTestServer builds a fully wired gateway against miniredis. The presence
// layer is real — only Redis is substituted — so these tests exercise the same
// code path a live gateway runs.
func newTestServer(t *testing.T, gatewayID string) *server {
	t.Helper()

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	registry := prometheus.NewRegistry()
	m := metrics.New(registry, gatewayID)

	cfg := config{
		GatewayID: gatewayID,
		HTTPAddr:  ":0",
		RedisAddr: mr.Addr(),
		DevToken:  testToken,
		WebDir:    t.TempDir(),
		Presence:  presence.DefaultConfig(gatewayID),
	}

	svc := presence.NewService(ctx, rdb, cfg.Presence, m, log)
	h := newHub(m)
	svc.SetConnectionCounter(h.count)

	srv := newServer(cfg, log, m, registry, svc, h)
	svc.Bus.OnControl(func(cm presence.ControlMessage) {
		if cm.Type == presence.ControlEvict {
			srv.evictLocal(cm.UserID, cm.SessionID)
		}
	})

	busDone := make(chan struct{})
	go func() {
		defer close(busDone)
		_ = svc.Bus.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = svc.Bus.Close()
		<-busDone
	})

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("presence.Start: %v", err)
	}
	return srv
}

// ---------------------------------------------------------------------------
// HTTP surface
// ---------------------------------------------------------------------------

func TestHealthzIsAliveBeforeReady(t *testing.T) {
	srv := newTestServer(t, "gw-test")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("healthz before ready: got %d, want %d", rec.Code, http.StatusOK)
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("healthz body is not JSON: %v", err)
	}
	if body["gatewayId"] != "gw-test" {
		t.Errorf("gatewayId: got %v, want gw-test", body["gatewayId"])
	}
	if body["status"] != "ok" {
		t.Errorf("status: got %v, want ok", body["status"])
	}
}

// Liveness and readiness must not be the same signal. A replica that is up but
// not yet joined to the ring has to fail readiness, or Compose routes
// connections to a node that cannot serve them.
func TestReadyzGatesOnReadiness(t *testing.T) {
	srv := newTestServer(t, "gw-test")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before markReady: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	srv.markReady()

	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after markReady: got %d, want %d", rec.Code, http.StatusOK)
	}

	srv.markUnready()

	rec = httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz after markUnready: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestMetricsEndpointExposesBeaconCollectors(t *testing.T) {
	srv := newTestServer(t, "gw-test")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: got %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, want := range []string{
		"beacon_connections_active",
		"beacon_presence_drift",
		"beacon_frames_rejected_total",
		"beacon_duplicate_sessions_evicted_total",
		"beacon_presence_events_out_of_order_total",
		"beacon_sessions_reaped_total",
		"beacon_orphan_sessions_reclaimed_total",
		"beacon_ring_members",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics is missing %s", want)
		}
	}
	if !strings.Contains(body, `gateway_id="gw-test"`) {
		t.Error("/metrics output carries no gateway_id label")
	}
}

func TestDebugRingReportsMembership(t *testing.T) {
	srv := newTestServer(t, "gw-test")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/ring", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("/debug/ring: got %d, want 200 (body %s)", rec.Code, rec.Body.String())
	}

	var snap presence.RingSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
		t.Fatalf("/debug/ring body is not the expected shape: %v", err)
	}
	if snap.GatewayID != "gw-test" {
		t.Errorf("gatewayId: got %q, want gw-test", snap.GatewayID)
	}
	if snap.MemberCount != 1 || len(snap.Members) != 1 || snap.Members[0] != "gw-test" {
		t.Errorf("members: got %v (count %d), want [gw-test]", snap.Members, snap.MemberCount)
	}
	if snap.VirtualNodes <= 0 {
		t.Errorf("virtualNodes: got %d, want positive", snap.VirtualNodes)
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	srv := newTestServer(t, "gw-test")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown route: got %d, want %d", rec.Code, http.StatusNotFound)
	}
}

// A gateway that is not ready must refuse the upgrade rather than accept a
// socket it cannot serve.
func TestWebSocketRefusedWhenUnready(t *testing.T) {
	srv := newTestServer(t, "gw-test")

	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws", nil))

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("/ws while unready: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

// ---------------------------------------------------------------------------
// WebSocket session lifecycle
// ---------------------------------------------------------------------------

type testClient struct {
	t  *testing.T
	ws *websocket.Conn
}

func dial(t *testing.T, ts *httptest.Server) *testClient {
	t.Helper()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	ws, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial %s: %v", url, err)
	}
	t.Cleanup(func() { _ = ws.Close() })
	return &testClient{t: t, ws: ws}
}

func (c *testClient) send(typ protocol.Type, payload any) {
	c.t.Helper()
	raw, err := protocol.Encode(typ, payload)
	if err != nil {
		c.t.Fatalf("encode %s: %v", typ, err)
	}
	if err := c.ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		c.t.Fatalf("write %s: %v", typ, err)
	}
}

func (c *testClient) sendRaw(raw string) {
	c.t.Helper()
	if err := c.ws.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
		c.t.Fatalf("write raw: %v", err)
	}
}

// expect reads until a frame of the wanted type arrives, so a test asserting on
// JOIN_OK is not derailed by an unrelated PRESENCE frame in between.
func (c *testClient) expect(want protocol.Type) *protocol.Frame {
	c.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.ws.SetReadDeadline(deadline)
		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			c.t.Fatalf("waiting for %s: %v", want, err)
		}
		var f protocol.Frame
		if err := json.Unmarshal(raw, &f); err != nil {
			c.t.Fatalf("undecodable frame: %v", err)
		}
		if f.Type == want {
			return &f
		}
	}
	c.t.Fatalf("timed out waiting for %s", want)
	return nil
}

func (c *testClient) hello(userID string) protocol.Welcome {
	c.t.Helper()
	c.send(protocol.TypeHello, protocol.Hello{UserID: userID, Token: testToken})
	f := c.expect(protocol.TypeWelcome)

	var w protocol.Welcome
	if err := json.Unmarshal(f.Payload, &w); err != nil {
		c.t.Fatalf("decode WELCOME: %v", err)
	}
	return w
}

func startGateway(t *testing.T, gatewayID string) (*server, *httptest.Server) {
	t.Helper()
	srv := newTestServer(t, gatewayID)
	srv.markReady()
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return srv, ts
}

func TestHelloEstablishesSession(t *testing.T) {
	srv, ts := startGateway(t, "gw-1")
	c := dial(t, ts)

	w := c.hello("alice")
	if w.GatewayID != "gw-1" {
		t.Errorf("WELCOME gatewayId: got %q, want gw-1", w.GatewayID)
	}
	if len(w.SessionID) != 32 {
		t.Errorf("sessionId: got %q, want 32 hex chars", w.SessionID)
	}

	waitFor(t, func() bool { return srv.hub.count() == 1 })

	sess, err := srv.presence.Store.Get(context.Background(), "alice")
	if err != nil {
		t.Fatalf("session was not written to Redis: %v", err)
	}
	if sess.GatewayID != "gw-1" || sess.Status != protocol.StatusOnline {
		t.Errorf("stored session: %+v", sess)
	}
}

func TestHelloWithBadTokenIsRejected(t *testing.T) {
	_, ts := startGateway(t, "gw-1")
	c := dial(t, ts)

	c.send(protocol.TypeHello, protocol.Hello{UserID: "alice", Token: "wrong"})
	f := c.expect(protocol.TypeError)

	var e protocol.ErrorPayload
	if err := json.Unmarshal(f.Payload, &e); err != nil {
		t.Fatalf("decode ERROR: %v", err)
	}
	if e.Code != protocol.CodeUnauthorized {
		t.Errorf("code: got %q, want %q", e.Code, protocol.CodeUnauthorized)
	}
}

// Any frame before HELLO is a protocol violation: the gateway has no idea who
// it is talking to, so it cannot act on anything.
func TestFrameBeforeHelloIsRejected(t *testing.T) {
	_, ts := startGateway(t, "gw-1")
	c := dial(t, ts)

	c.send(protocol.TypeJoin, protocol.Join{TargetUserID: "bob"})
	f := c.expect(protocol.TypeError)

	var e protocol.ErrorPayload
	_ = json.Unmarshal(f.Payload, &e)
	if e.Code != protocol.CodeUnauthorized {
		t.Errorf("code: got %q, want %q", e.Code, protocol.CodeUnauthorized)
	}
}

// Integrity check 1 at the transport boundary: malformed input must produce an
// ERROR frame and a metric, never a panic or a dropped connection.
func TestMalformedFramesAreRejectedNotFatal(t *testing.T) {
	_, ts := startGateway(t, "gw-1")
	c := dial(t, ts)
	c.hello("alice")

	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"not json", "{{{not json", protocol.CodeBadFrame},
		{"unknown type", `{"type":"TELEPORT"}`, protocol.CodeUnknownType},
		{"server type from client", `{"type":"PRESENCE"}`, protocol.CodeUnknownType},
		{"missing type", `{"payload":{}}`, protocol.CodeMissingField},
		{"join with no target", `{"type":"JOIN","payload":{}}`, protocol.CodeMissingField},
		{"bad status", `{"type":"SET_PRESENCE","payload":{"status":"PARTYING"}}`, protocol.CodeInvalidField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c.sendRaw(tc.raw)
			f := c.expect(protocol.TypeError)

			var e protocol.ErrorPayload
			_ = json.Unmarshal(f.Payload, &e)
			if e.Code != tc.want {
				t.Errorf("code: got %q, want %q", e.Code, tc.want)
			}
		})
	}

	// Still alive after all of that.
	c.send(protocol.TypeHeartbeat, protocol.Heartbeat{})
	c.expect(protocol.TypeAck)
}

func TestHeartbeatIsAcknowledged(t *testing.T) {
	_, ts := startGateway(t, "gw-1")
	c := dial(t, ts)
	c.hello("alice")

	c.send(protocol.TypeHeartbeat, protocol.Heartbeat{})
	f := c.expect(protocol.TypeAck)

	var ack protocol.Ack
	if err := json.Unmarshal(f.Payload, &ack); err != nil {
		t.Fatalf("decode ACK: %v", err)
	}
	if ack.TS == 0 {
		t.Error("ACK carried no timestamp")
	}
}

func TestSubscribeReturnsSnapshots(t *testing.T) {
	_, ts := startGateway(t, "gw-1")

	target := dial(t, ts)
	target.hello("bob")
	target.send(protocol.TypeSetPresence, protocol.SetPresence{
		Status: protocol.StatusInGame, PlaceID: "place-1", ServerID: "srv-1",
	})

	watcher := dial(t, ts)
	watcher.hello("alice")
	watcher.send(protocol.TypeSubscribe, protocol.Subscribe{UserIDs: []string{"bob", "nobody"}})

	// Snapshot-on-subscribe is what makes at-most-once pub/sub acceptable: a
	// subscriber learns current state without waiting for the next change.
	seen := map[string]protocol.Presence{}
	for i := 0; i < 2; i++ {
		f := watcher.expect(protocol.TypePresence)
		var p protocol.Presence
		_ = json.Unmarshal(f.Payload, &p)
		seen[p.UserID] = p
	}

	if seen["bob"].Status != protocol.StatusInGame || seen["bob"].PlaceID != "place-1" {
		t.Errorf("bob snapshot: %+v", seen["bob"])
	}
	// A user with no session is reported OFFLINE rather than omitted, so the
	// client does not show "unknown" forever.
	if seen["nobody"].Status != protocol.StatusOffline {
		t.Errorf("nobody snapshot: %+v", seen["nobody"])
	}
}

func TestJoinResolvesTargetOnSameGateway(t *testing.T) {
	_, ts := startGateway(t, "gw-1")

	target := dial(t, ts)
	target.hello("bob")
	target.send(protocol.TypeSetPresence, protocol.SetPresence{
		Status: protocol.StatusInGame, PlaceID: "place-9", ServerID: "srv-4",
	})
	target.send(protocol.TypeHeartbeat, protocol.Heartbeat{})
	target.expect(protocol.TypeAck)

	joiner := dial(t, ts)
	joiner.hello("alice")
	joiner.send(protocol.TypeJoin, protocol.Join{TargetUserID: "bob"})

	f := joiner.expect(protocol.TypeJoinOK)
	var ok protocol.JoinOK
	_ = json.Unmarshal(f.Payload, &ok)
	if ok.PlaceID != "place-9" || ok.ServerID != "srv-4" {
		t.Errorf("JOIN_OK: %+v", ok)
	}
}

func TestJoinDeniedForUnknownTarget(t *testing.T) {
	_, ts := startGateway(t, "gw-1")
	c := dial(t, ts)
	c.hello("alice")

	c.send(protocol.TypeJoin, protocol.Join{TargetUserID: "ghost"})
	f := c.expect(protocol.TypeJoinDenied)

	var denied protocol.JoinDenied
	_ = json.Unmarshal(f.Payload, &denied)
	if denied.Reason != protocol.ReasonTargetUnknown {
		t.Errorf("reason: got %q, want %q", denied.Reason, protocol.ReasonTargetUnknown)
	}
}

func TestJoinDeniedForOnlineButIdleTarget(t *testing.T) {
	_, ts := startGateway(t, "gw-1")

	target := dial(t, ts)
	target.hello("bob")
	target.send(protocol.TypeHeartbeat, protocol.Heartbeat{})
	target.expect(protocol.TypeAck)

	joiner := dial(t, ts)
	joiner.hello("alice")
	joiner.send(protocol.TypeJoin, protocol.Join{TargetUserID: "bob"})

	f := joiner.expect(protocol.TypeJoinDenied)
	var denied protocol.JoinDenied
	_ = json.Unmarshal(f.Payload, &denied)
	if denied.Reason != protocol.ReasonTargetNotJoinable {
		t.Errorf("reason: got %q, want %q", denied.Reason, protocol.ReasonTargetNotJoinable)
	}
}

// Integrity check 2 within one gateway: the newer connection wins and the older
// is told it has been superseded.
func TestDuplicateConnectionOnSameGatewayEvictsTheOlder(t *testing.T) {
	srv, ts := startGateway(t, "gw-1")

	first := dial(t, ts)
	firstWelcome := first.hello("alice")
	waitFor(t, func() bool { return srv.hub.count() == 1 })

	second := dial(t, ts)
	secondWelcome := second.hello("alice")

	if firstWelcome.SessionID == secondWelcome.SessionID {
		t.Fatal("the two connections were given the same session ID")
	}

	// The displaced connection is told, then closed.
	f := first.expect(protocol.TypeError)
	var e protocol.ErrorPayload
	_ = json.Unmarshal(f.Payload, &e)
	if e.Code != protocol.CodeUnauthorized {
		t.Errorf("eviction error code: got %q, want %q", e.Code, protocol.CodeUnauthorized)
	}

	// Redis reflects the newer session.
	sess, err := srv.presence.Store.Get(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if sess.SessionID != secondWelcome.SessionID {
		t.Errorf("stored session is not the newest: got %q, want %q", sess.SessionID, secondWelcome.SessionID)
	}

	// Exactly one connection remains for this user.
	waitFor(t, func() bool { return srv.hub.count() == 1 })
}

// The user is not offline — they are live on the connection that displaced this
// one. A cluster-wide OFFLINE would race the new session's ONLINE and leave
// watchers with a wrong view, so the evicted client is told locally instead.
func TestEvictedSessionIsToldOfflineWithoutPublishing(t *testing.T) {
	srv, ts := startGateway(t, "gw-1")

	first := dial(t, ts)
	first.hello("alice")
	waitFor(t, func() bool { return srv.hub.count() == 1 })

	second := dial(t, ts)
	second.hello("alice")

	f := first.expect(protocol.TypePresence)
	var p protocol.Presence
	_ = json.Unmarshal(f.Payload, &p)
	if p.UserID != "alice" || p.Status != protocol.StatusOffline {
		t.Errorf("evicted client received %+v, want alice OFFLINE", p)
	}

	// Redis still shows the user online via the newer session.
	sess, err := srv.presence.Store.Get(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if sess.Status != protocol.StatusOnline {
		t.Errorf("eviction marked a live user offline in Redis: %+v", sess)
	}
}

func TestDisconnectRemovesSession(t *testing.T) {
	srv, ts := startGateway(t, "gw-1")

	c := dial(t, ts)
	c.hello("alice")
	waitFor(t, func() bool { return srv.hub.count() == 1 })

	_ = c.ws.Close()

	waitFor(t, func() bool { return srv.hub.count() == 0 })
	waitFor(t, func() bool {
		_, err := srv.presence.Store.Get(context.Background(), "alice")
		return err != nil
	})
}

func TestSetPresenceUpdatesStoredSession(t *testing.T) {
	srv, ts := startGateway(t, "gw-1")

	c := dial(t, ts)
	c.hello("alice")
	c.send(protocol.TypeSetPresence, protocol.SetPresence{
		Status: protocol.StatusAway, PlaceID: "", ServerID: "",
	})
	c.send(protocol.TypeHeartbeat, protocol.Heartbeat{})
	c.expect(protocol.TypeAck)

	sess, err := srv.presence.Store.Get(context.Background(), "alice")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if sess.Status != protocol.StatusAway {
		t.Errorf("status: got %q, want AWAY", sess.Status)
	}
}

// The 8KB cap is enforced by the socket read limit, which closes the connection
// rather than returning an oversized payload to the decoder.
func TestOversizedFrameClosesConnection(t *testing.T) {
	_, ts := startGateway(t, "gw-1")
	c := dial(t, ts)
	c.hello("alice")

	huge := `{"type":"JOIN","payload":{"targetUserId":"` + strings.Repeat("x", protocol.MaxFrameBytes) + `"}}`
	c.sendRaw(huge)

	_ = c.ws.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		if _, _, err := c.ws.ReadMessage(); err != nil {
			return // closed, as intended
		}
	}
}

// ---------------------------------------------------------------------------
// Hub
// ---------------------------------------------------------------------------

func TestHubUnregisterIsGuardedBySessionID(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry(), "gw-1")
	h := newHub(m)

	older := &connection{userID: "alice", sessionID: "s1"}
	newer := &connection{userID: "alice", sessionID: "s2"}

	h.register(older)
	h.register(newer)

	// The displaced connection tearing down must not remove its replacement.
	if h.unregister("alice", "s1") {
		t.Error("a stale session ID unregistered the current connection")
	}
	if h.count() != 1 {
		t.Errorf("count: got %d, want 1", h.count())
	}

	if !h.unregister("alice", "s2") {
		t.Error("the current session ID failed to unregister")
	}
	if h.count() != 0 {
		t.Errorf("count: got %d, want 0", h.count())
	}
}

func TestHubLookupForEvictionRequiresMatchingSession(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry(), "gw-1")
	h := newHub(m)

	c := &connection{userID: "alice", sessionID: "s2"}
	h.register(c)

	// A notice for a session this gateway already replaced is stale; acting on
	// it would kill the connection that replaced it.
	if got := h.lookupForEviction("alice", "s1"); got != nil {
		t.Error("a stale eviction notice matched the current connection")
	}
	if got := h.lookupForEviction("alice", "s2"); got != c {
		t.Error("the current session was not matched")
	}
	if got := h.lookupForEviction("nobody", "s1"); got != nil {
		t.Error("an unknown user matched a connection")
	}
}

func TestHubRegisterReturnsDisplacedConnection(t *testing.T) {
	m := metrics.New(prometheus.NewRegistry(), "gw-1")
	h := newHub(m)

	first := &connection{userID: "alice", sessionID: "s1"}
	if displaced := h.register(first); displaced != nil {
		t.Error("the first registration reported a displacement")
	}

	second := &connection{userID: "alice", sessionID: "s2"}
	displaced := h.register(second)
	if displaced != first {
		t.Errorf("register returned %v, want the first connection", displaced)
	}
	if h.count() != 1 {
		t.Errorf("count: got %d, want 1", h.count())
	}
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

func TestLoadConfigDefaults(t *testing.T) {
	t.Setenv("BEACON_GATEWAY_ID", "")
	t.Setenv("BEACON_HTTP_ADDR", "")

	cfg := loadConfig()

	if cfg.GatewayID == "" {
		t.Error("GatewayID fell back to empty string")
	}
	if cfg.HTTPAddr == "" {
		t.Error("HTTPAddr fell back to empty string")
	}
	if cfg.ShutdownGrace <= 0 {
		t.Errorf("ShutdownGrace: got %v, want positive", cfg.ShutdownGrace)
	}
	if cfg.Presence.GatewayID != cfg.GatewayID {
		t.Errorf("presence config has gateway ID %q, gateway has %q",
			cfg.Presence.GatewayID, cfg.GatewayID)
	}
}

func TestEnvDurationRejectsGarbage(t *testing.T) {
	t.Setenv("BEACON_SHUTDOWN_GRACE", "not-a-duration")

	// A malformed duration must not collapse to zero: a zero grace period turns
	// every rolling restart into an abrupt connection drop.
	if got := envDuration("BEACON_SHUTDOWN_GRACE", 10*time.Second); got != 10*time.Second {
		t.Errorf("envDuration with garbage: got %v, want the 10s fallback", got)
	}

	t.Setenv("BEACON_SHUTDOWN_GRACE", "-5s")
	if got := envDuration("BEACON_SHUTDOWN_GRACE", 10*time.Second); got != 10*time.Second {
		t.Errorf("envDuration with a negative value: got %v, want the 10s fallback", got)
	}
}

func TestEnvIntRejectsGarbage(t *testing.T) {
	t.Setenv("BEACON_RING_REPLICAS", "lots")
	if got := envInt("BEACON_RING_REPLICAS", 150); got != 150 {
		t.Errorf("envInt with garbage: got %d, want 150", got)
	}

	t.Setenv("BEACON_RING_REPLICAS", "0")
	if got := envInt("BEACON_RING_REPLICAS", 150); got != 150 {
		t.Errorf("envInt with zero: got %d, want the 150 fallback", got)
	}

	t.Setenv("BEACON_RING_REPLICAS", "64")
	if got := envInt("BEACON_RING_REPLICAS", 150); got != 64 {
		t.Errorf("envInt: got %d, want 64", got)
	}
}

func TestNewSessionIDIsUniqueAndHex(t *testing.T) {
	seen := make(map[string]struct{}, 1000)
	for i := 0; i < 1000; i++ {
		id := newSessionID()
		if len(id) != 32 {
			t.Fatalf("session ID %q is %d chars, want 32", id, len(id))
		}
		if _, dup := seen[id]; dup {
			t.Fatalf("session ID collision on %q", id)
		}
		seen[id] = struct{}{}
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 5s")
}

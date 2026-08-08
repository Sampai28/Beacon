//go:build integration

// Package integration exercises a running Beacon cluster over the wire.
//
// These tests deliberately do not build a gateway in-process. Everything the
// unit tests can prove about presence they already prove against miniredis;
// what only a real cluster can show is that two clients on *different* gateway
// processes see each other — that fan-out crosses a process boundary, a network
// hop and a real Redis, not just a function call.
//
// Requires the Compose stack:
//
//	docker compose -f deploy/docker-compose.yml up -d --build
//	go test -tags=integration ./test/...
package integration

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Sampai28/Beacon/internal/presence"
	"github.com/Sampai28/Beacon/internal/protocol"
)

// Gateway endpoints as published by deploy/docker-compose.yml.
var gateways = []string{
	envOr("BEACON_GW1", "localhost:8081"),
	envOr("BEACON_GW2", "localhost:8082"),
	envOr("BEACON_GW3", "localhost:8083"),
}

var devToken = envOr("BEACON_DEV_TOKEN", "beacon-dev-token")

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// uniq keeps user IDs distinct across runs so a leftover session from a previous
// run cannot make a test pass or fail for the wrong reason.
func uniq(name string) string {
	return fmt.Sprintf("%s-%d", name, time.Now().UnixNano()%1_000_000)
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

type client struct {
	t       *testing.T
	ws      *websocket.Conn
	userID  string
	gateway string
	frames  chan *protocol.Frame
	done    chan struct{}
}

// connect opens a session against a specific gateway. Pinning matters: the whole
// point is to have the two ends of a test on different processes.
func connect(t *testing.T, addr, userID string) *client {
	t.Helper()

	ws, _, err := websocket.DefaultDialer.Dial("ws://"+addr+"/ws", nil)
	if err != nil {
		t.Fatalf("dial %s: %v (is the Compose stack up?)", addr, err)
	}

	c := &client{
		t:      t,
		ws:     ws,
		userID: userID,
		frames: make(chan *protocol.Frame, 256),
		done:   make(chan struct{}),
	}
	go c.readLoop()
	t.Cleanup(func() { _ = ws.Close() })

	c.send(protocol.TypeHello, protocol.Hello{UserID: userID, Token: devToken})

	var w protocol.Welcome
	c.expectInto(protocol.TypeWelcome, &w)
	c.gateway = w.GatewayID

	// Heartbeats keep the session alive; without them the reaper removes it
	// mid-test and the failure looks like a fan-out bug.
	go c.heartbeat()
	return c
}

func (c *client) readLoop() {
	defer close(c.done)
	for {
		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			return
		}
		var f protocol.Frame
		if err := json.Unmarshal(raw, &f); err != nil {
			continue
		}
		select {
		case c.frames <- &f:
		default: // slow test; drop rather than block the reader
		}
	}
}

func (c *client) heartbeat() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.done:
			return
		case <-ticker.C:
			raw, err := protocol.Encode(protocol.TypeHeartbeat, protocol.Heartbeat{})
			if err != nil {
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, raw); err != nil {
				return
			}
		}
	}
}

func (c *client) send(typ protocol.Type, payload any) {
	c.t.Helper()
	raw, err := protocol.Encode(typ, payload)
	if err != nil {
		c.t.Fatalf("encode %s: %v", typ, err)
	}
	if err := c.ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		c.t.Fatalf("write %s: %v", typ, err)
	}
}

// await returns the first frame matching pred, ignoring everything else.
func (c *client) await(timeout time.Duration, pred func(*protocol.Frame) bool) *protocol.Frame {
	c.t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case f := <-c.frames:
			if pred(f) {
				return f
			}
		case <-deadline:
			return nil
		case <-c.done:
			c.t.Fatalf("%s: connection to %s closed while waiting", c.userID, c.gateway)
		}
	}
}

func (c *client) expectInto(typ protocol.Type, dst any) {
	c.t.Helper()
	f := c.await(10*time.Second, func(f *protocol.Frame) bool { return f.Type == typ })
	if f == nil {
		c.t.Fatalf("%s: timed out waiting for %s", c.userID, typ)
	}
	if dst != nil && len(f.Payload) > 0 {
		if err := json.Unmarshal(f.Payload, dst); err != nil {
			c.t.Fatalf("decode %s: %v", typ, err)
		}
	}
}

// awaitPresence waits for a PRESENCE frame about a specific user in a specific
// status, tolerating the snapshot and any intermediate transitions in between.
func (c *client) awaitPresence(userID string, status protocol.Status, timeout time.Duration) *protocol.Presence {
	c.t.Helper()
	f := c.await(timeout, func(f *protocol.Frame) bool {
		if f.Type != protocol.TypePresence {
			return false
		}
		var p protocol.Presence
		if err := json.Unmarshal(f.Payload, &p); err != nil {
			return false
		}
		return p.UserID == userID && p.Status == status
	})
	if f == nil {
		return nil
	}
	var p protocol.Presence
	_ = json.Unmarshal(f.Payload, &p)
	return &p
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestClusterIsUpWithThreeGateways(t *testing.T) {
	seen := map[string]bool{}

	for _, addr := range gateways {
		resp, err := http.Get("http://" + addr + "/readyz")
		if err != nil {
			t.Fatalf("GET %s/readyz: %v (is the Compose stack up?)", addr, err)
		}
		body, _ := readAllClose(resp)
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s/readyz: got %d, want 200 (%s)", addr, resp.StatusCode, body)
		}

		var probe struct {
			GatewayID string `json:"gatewayId"`
		}
		if err := json.Unmarshal([]byte(body), &probe); err != nil {
			t.Fatalf("%s/readyz body: %v", addr, err)
		}
		if seen[probe.GatewayID] {
			t.Fatalf("two endpoints reported the same gateway_id %q; replicas must be distinct",
				probe.GatewayID)
		}
		seen[probe.GatewayID] = true
	}

	if len(seen) != 3 {
		t.Fatalf("found %d distinct gateways, want 3", len(seen))
	}
}

// Ring membership derives from the TTL registry, so every node must independently
// converge on the same view. Disagreement here means shards are either
// double-owned or unowned.
func TestAllGatewaysAgreeOnRingMembership(t *testing.T) {
	var first []string

	for _, addr := range gateways {
		snap := fetchRing(t, addr)
		if snap.MemberCount != 3 {
			t.Errorf("%s sees %d ring members, want 3: %v", addr, snap.MemberCount, snap.Members)
		}
		if first == nil {
			first = snap.Members
			continue
		}
		if strings.Join(first, ",") != strings.Join(snap.Members, ",") {
			t.Errorf("ring membership disagreement: %v sees %v, first node saw %v",
				addr, snap.Members, first)
		}
	}
}

// The core claim of the whole project: a presence change published by a client
// on one gateway reaches a subscriber on a different one. Neither gateway holds
// the other's state and they never talk directly.
func TestPresencePropagatesAcrossGateways(t *testing.T) {
	alice := uniq("alice")
	carol := uniq("carol")

	a := connect(t, gateways[0], alice)
	c := connect(t, gateways[2], carol)

	if a.gateway == c.gateway {
		t.Fatalf("both clients landed on %s; the test needs them on different nodes", a.gateway)
	}
	t.Logf("alice on %s, carol on %s", a.gateway, c.gateway)

	// Carol watches alice and receives her current snapshot first.
	c.send(protocol.TypeSubscribe, protocol.Subscribe{UserIDs: []string{alice}})
	if got := c.awaitPresence(alice, protocol.StatusOnline, 10*time.Second); got == nil {
		t.Fatal("carol never received alice's ONLINE snapshot on subscribe")
	}

	// Alice changes state on her gateway.
	a.send(protocol.TypeSetPresence, protocol.SetPresence{
		Status: protocol.StatusInGame, PlaceID: "place-42", ServerID: "srv-7",
	})

	got := c.awaitPresence(alice, protocol.StatusInGame, 10*time.Second)
	if got == nil {
		t.Fatalf("presence change on %s never reached the subscriber on %s", a.gateway, c.gateway)
	}
	if got.PlaceID != "place-42" || got.ServerID != "srv-7" {
		t.Errorf("fan-out lost payload: %+v", got)
	}
	t.Logf("IN_GAME propagated %s -> %s with place %s", a.gateway, c.gateway, got.PlaceID)
}

// A JOIN is answered by whichever gateway the joiner happens to be on, for a
// target it has never served. That works only because session state is shared
// rather than pinned to a node.
func TestJoinResolvesTargetOnAnotherGateway(t *testing.T) {
	alice := uniq("alice")
	carol := uniq("carol")

	a := connect(t, gateways[0], alice)
	c := connect(t, gateways[2], carol)

	if a.gateway == c.gateway {
		t.Fatalf("both clients landed on %s; the test needs them on different nodes", a.gateway)
	}

	a.send(protocol.TypeSetPresence, protocol.SetPresence{
		Status: protocol.StatusInGame, PlaceID: "place-99", ServerID: "srv-3",
	})

	// Subscribe so the test can observe the write landing before joining,
	// rather than sleeping and hoping.
	c.send(protocol.TypeSubscribe, protocol.Subscribe{UserIDs: []string{alice}})
	if got := c.awaitPresence(alice, protocol.StatusInGame, 10*time.Second); got == nil {
		t.Fatal("alice's IN_GAME never propagated; JOIN would be racing the write")
	}

	c.send(protocol.TypeJoin, protocol.Join{TargetUserID: alice})

	var ok protocol.JoinOK
	f := c.await(10*time.Second, func(f *protocol.Frame) bool {
		return f.Type == protocol.TypeJoinOK || f.Type == protocol.TypeJoinDenied
	})
	if f == nil {
		t.Fatal("no answer to JOIN")
	}
	if f.Type == protocol.TypeJoinDenied {
		var denied protocol.JoinDenied
		_ = json.Unmarshal(f.Payload, &denied)
		t.Fatalf("JOIN denied for a target on another gateway: %s", denied.Reason)
	}
	_ = json.Unmarshal(f.Payload, &ok)

	if ok.PlaceID != "place-99" || ok.ServerID != "srv-3" {
		t.Errorf("JOIN_OK: got %+v, want place-99 / srv-3", ok)
	}
	t.Logf("%s resolved a JOIN for a user connected to %s", c.gateway, a.gateway)
}

// Every gateway must answer identically for a user connected to exactly one of
// them, which is what makes the replicas interchangeable.
func TestEveryGatewayResolvesTheSameTarget(t *testing.T) {
	alice := uniq("alice")
	a := connect(t, gateways[0], alice)

	a.send(protocol.TypeSetPresence, protocol.SetPresence{
		Status: protocol.StatusInGame, PlaceID: "place-7", ServerID: "srv-1",
	})

	watcher := connect(t, gateways[1], uniq("watcher"))
	watcher.send(protocol.TypeSubscribe, protocol.Subscribe{UserIDs: []string{alice}})
	if got := watcher.awaitPresence(alice, protocol.StatusInGame, 10*time.Second); got == nil {
		t.Fatal("alice's IN_GAME never landed in Redis")
	}

	for i, addr := range gateways {
		joiner := connect(t, addr, uniq(fmt.Sprintf("joiner%d", i)))
		joiner.send(protocol.TypeJoin, protocol.Join{TargetUserID: alice})

		f := joiner.await(10*time.Second, func(f *protocol.Frame) bool {
			return f.Type == protocol.TypeJoinOK || f.Type == protocol.TypeJoinDenied
		})
		if f == nil {
			t.Errorf("%s did not answer a JOIN", addr)
			continue
		}
		if f.Type != protocol.TypeJoinOK {
			var denied protocol.JoinDenied
			_ = json.Unmarshal(f.Payload, &denied)
			t.Errorf("%s denied a JOIN the other nodes allow: %s", addr, denied.Reason)
			continue
		}
		var ok protocol.JoinOK
		_ = json.Unmarshal(f.Payload, &ok)
		if ok.PlaceID != "place-7" {
			t.Errorf("%s resolved place %q, want place-7", addr, ok.PlaceID)
		}
	}
}

// Integrity check 2 across nodes: reconnecting to a different gateway takes the
// session, and the gateway that held it is told to drop the old connection.
func TestDuplicateSessionAcrossGatewaysEvictsTheOlder(t *testing.T) {
	alice := uniq("alice")

	first := connect(t, gateways[0], alice)
	second := connect(t, gateways[1], alice)

	if first.gateway == second.gateway {
		t.Fatalf("both connections landed on %s; the test needs different nodes", first.gateway)
	}

	// The displaced connection is told it has been superseded.
	f := first.await(10*time.Second, func(f *protocol.Frame) bool { return f.Type == protocol.TypeError })
	if f == nil {
		t.Fatalf("the connection on %s was never told it had been evicted by %s",
			first.gateway, second.gateway)
	}
	var e protocol.ErrorPayload
	_ = json.Unmarshal(f.Payload, &e)
	if e.Code != protocol.CodeUnauthorized {
		t.Errorf("eviction code: got %q, want %q", e.Code, protocol.CodeUnauthorized)
	}
	t.Logf("session on %s evicted by a newer one on %s", first.gateway, second.gateway)

	// The user is still online — on the newer connection.
	watcher := connect(t, gateways[2], uniq("watcher"))
	watcher.send(protocol.TypeSubscribe, protocol.Subscribe{UserIDs: []string{alice}})
	if got := watcher.awaitPresence(alice, protocol.StatusOnline, 10*time.Second); got == nil {
		t.Error("eviction left the user looking offline; they are live on the newer connection")
	}
}

// Integrity check 1 over the wire, against a real gateway rather than the codec
// in isolation.
func TestMalformedFramesAreRejectedByALiveGateway(t *testing.T) {
	c := connect(t, gateways[0], uniq("fuzzer"))

	for _, raw := range []string{
		`{{{`,
		`{"type":"NONSENSE"}`,
		`{"type":"PRESENCE"}`,
		`{"type":"JOIN","payload":{}}`,
		`{"type":"SET_PRESENCE","payload":{"status":"PARTYING"}}`,
	} {
		if err := c.ws.WriteMessage(websocket.TextMessage, []byte(raw)); err != nil {
			t.Fatalf("write: %v", err)
		}
		f := c.await(5*time.Second, func(f *protocol.Frame) bool { return f.Type == protocol.TypeError })
		if f == nil {
			t.Errorf("no ERROR returned for %s", raw)
		}
	}

	// The connection survived every one of them.
	c.send(protocol.TypeHeartbeat, protocol.Heartbeat{})
	if f := c.await(5*time.Second, func(f *protocol.Frame) bool { return f.Type == protocol.TypeAck }); f == nil {
		t.Error("connection did not survive a run of malformed frames")
	}
}

// Drift is the aggregate check: it catches losses no single-operation check
// would notice. In steady state every gateway must report zero.
func TestPresenceDriftIsZeroInSteadyState(t *testing.T) {
	clients := make([]*client, 0, 6)
	for i := 0; i < 6; i++ {
		clients = append(clients, connect(t, gateways[i%len(gateways)], uniq(fmt.Sprintf("drift%d", i))))
	}

	// Drift is sampled on a timer and the registry heartbeat is up to one
	// interval stale by construction, so allow the loops to catch up rather than
	// asserting instantaneously.
	deadline := time.Now().Add(30 * time.Second)
	var last map[string]float64

	for time.Now().Before(deadline) {
		last = map[string]float64{}
		clean := true
		for _, addr := range gateways {
			v, ok := scrapeGauge(t, addr, "beacon_presence_drift")
			if !ok {
				clean = false
				break
			}
			last[addr] = v
			if v != 0 {
				clean = false
			}
		}
		if clean {
			t.Logf("drift is zero on all gateways with %d clients connected", len(clients))
			return
		}
		time.Sleep(2 * time.Second)
	}

	t.Errorf("drift did not settle to zero within 30s: %v", last)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func fetchRing(t *testing.T, addr string) presence.RingSnapshot {
	t.Helper()

	resp, err := http.Get("http://" + addr + "/debug/ring")
	if err != nil {
		t.Fatalf("GET %s/debug/ring: %v", addr, err)
	}
	body, _ := readAllClose(resp)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s/debug/ring: got %d (%s)", addr, resp.StatusCode, body)
	}

	var snap presence.RingSnapshot
	if err := json.Unmarshal([]byte(body), &snap); err != nil {
		t.Fatalf("%s/debug/ring body: %v", addr, err)
	}
	return snap
}

// scrapeGauge pulls a single unlabelled-value gauge out of a /metrics response.
// Deliberately a crude line scan rather than a Prometheus client: the test wants
// the gateway's own instantaneous value, not whatever Prometheus last sampled.
func scrapeGauge(t *testing.T, addr, name string) (float64, bool) {
	t.Helper()

	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		return 0, false
	}
	body, _ := readAllClose(resp)

	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, name) {
			continue
		}
		if strings.HasPrefix(line, "# ") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(fields[len(fields)-1], "%g", &v); err != nil {
			continue
		}
		return v, true
	}
	return 0, false
}

func readAllClose(resp *http.Response) (string, error) {
	defer func() { _ = resp.Body.Close() }()
	var sb strings.Builder
	buf := make([]byte, 32*1024)
	for {
		n, err := resp.Body.Read(buf)
		sb.Write(buf[:n])
		if err != nil {
			return sb.String(), nil
		}
	}
}

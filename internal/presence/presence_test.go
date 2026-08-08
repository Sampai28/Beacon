package presence

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/protocol"
	"github.com/Sampai28/Beacon/internal/ring"
)

const testSessionTTL = 30 * time.Second

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func testMetrics() *metrics.Metrics {
	return metrics.New(prometheus.NewRegistry(), "gw-test")
}

// newRedis returns a miniredis-backed client. miniredis is used rather than a
// live Redis so the integrity checks are testable in `go test ./...` without
// Docker; the Lua scripts and pub/sub are exercised for real against it.
func newRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	return mr, rdb
}

func newStore(t *testing.T) (*miniredis.Miniredis, *Store) {
	t.Helper()
	mr, rdb := newRedis(t)
	return mr, NewStore(rdb, testSessionTTL)
}

func session(userID, sessionID, gatewayID string, status protocol.Status, ts int64) Session {
	return Session{
		UserID:    userID,
		SessionID: sessionID,
		Status:    status,
		GatewayID: gatewayID,
		LastSeen:  ts,
	}
}

// ---------------------------------------------------------------------------
// Store: claim and the duplicate-session policy (integrity check 2)
// ---------------------------------------------------------------------------

func TestClaimCreatesSession(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	evicted, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000))
	if err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if evicted != nil {
		t.Errorf("first claim reported an eviction: %+v", evicted)
	}

	got, err := store.Get(ctx, "u1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.SessionID != "s1" || got.GatewayID != "gw-1" || got.Status != protocol.StatusOnline {
		t.Errorf("stored session: %+v", got)
	}
	if got.LastSeen != 1000 {
		t.Errorf("lastSeen: got %d, want 1000", got.LastSeen)
	}
}

// The policy is last-writer-wins across gateways: a user reconnecting to a
// different node takes the session, and the old holder is told to drop it.
func TestClaimEvictsSessionOnAnotherGateway(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("first claim: %v", err)
	}

	evicted, err := store.Claim(ctx, session("u1", "s2", "gw-3", protocol.StatusOnline, 2000))
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if evicted == nil {
		t.Fatal("second claim did not report an eviction")
	}
	if evicted.SessionID != "s1" {
		t.Errorf("evicted session: got %q, want s1", evicted.SessionID)
	}
	if evicted.GatewayID != "gw-1" {
		t.Errorf("evicted gateway: got %q, want gw-1", evicted.GatewayID)
	}

	got, _ := store.Get(ctx, "u1")
	if got.SessionID != "s2" || got.GatewayID != "gw-3" {
		t.Errorf("newest connection did not win: %+v", got)
	}
}

// Re-claiming the same session ID is a reconnect of the same logical session,
// not a duplicate. Counting it would inflate the eviction metric on every
// transient blip.
func TestReclaimingSameSessionIDIsNotAnEviction(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	evicted, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 2000))
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if evicted != nil {
		t.Errorf("re-claiming the same session reported an eviction: %+v", evicted)
	}
}

func TestClaimAddsUserToSessionIndex(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	for _, u := range []string{"u1", "u2", "u3"} {
		if _, err := store.Claim(ctx, session(u, "s-"+u, "gw-1", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim(%s): %v", u, err)
		}
	}

	n, err := store.SessionCount(ctx)
	if err != nil {
		t.Fatalf("SessionCount: %v", err)
	}
	if n != 3 {
		t.Errorf("SessionCount: got %d, want 3", n)
	}
}

// ---------------------------------------------------------------------------
// Store: out-of-order rejection (integrity check 3)
// ---------------------------------------------------------------------------

func TestUpdateRejectsOlderTimestamps(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 5000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	// A newer event applies.
	res, err := store.Update(ctx, session("u1", "s1", "gw-1", protocol.StatusInGame, 6000))
	if err != nil {
		t.Fatalf("Update newer: %v", err)
	}
	if res != UpdateApplied {
		t.Fatalf("newer event: got %v, want UpdateApplied", res)
	}

	// An older one is dropped rather than applied.
	res, err = store.Update(ctx, session("u1", "s1", "gw-1", protocol.StatusAway, 4000))
	if err != nil {
		t.Fatalf("Update older: %v", err)
	}
	if res != UpdateStale {
		t.Errorf("older event: got %v, want UpdateStale", res)
	}

	got, _ := store.Get(ctx, "u1")
	if got.Status != protocol.StatusInGame {
		t.Errorf("stale event overwrote fresher state: status is %q, want IN_GAME", got.Status)
	}
	if got.LastSeen != 6000 {
		t.Errorf("lastSeen regressed to %d, want 6000", got.LastSeen)
	}
}

// Equal timestamps must apply. Two events in the same millisecond are ordinary
// at these rates, and dropping them would silently discard real changes.
func TestUpdateAcceptsEqualTimestamp(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 5000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	res, err := store.Update(ctx, session("u1", "s1", "gw-1", protocol.StatusAway, 5000))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res != UpdateApplied {
		t.Errorf("equal timestamp: got %v, want UpdateApplied", res)
	}
}

func TestUpdateOnMissingSession(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	res, err := store.Update(ctx, session("ghost", "s1", "gw-1", protocol.StatusOnline, 1000))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res != UpdateNoSession {
		t.Errorf("got %v, want UpdateNoSession", res)
	}
}

// An evicted connection must not be able to keep writing presence for a user
// who has since reconnected elsewhere.
func TestUpdateFromEvictedSessionIsRejected(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := store.Claim(ctx, session("u1", "s2", "gw-2", protocol.StatusOnline, 2000)); err != nil {
		t.Fatalf("second claim: %v", err)
	}

	res, err := store.Update(ctx, session("u1", "s1", "gw-1", protocol.StatusAway, 3000))
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if res != UpdateWrongSession {
		t.Errorf("got %v, want UpdateWrongSession", res)
	}

	got, _ := store.Get(ctx, "u1")
	if got.SessionID != "s2" || got.Status != protocol.StatusOnline {
		t.Errorf("evicted session mutated live state: %+v", got)
	}
}

// ---------------------------------------------------------------------------
// Store: heartbeat and TTL
// ---------------------------------------------------------------------------

func TestHeartbeatExtendsTTL(t *testing.T) {
	ctx := context.Background()
	mr, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	mr.FastForward(20 * time.Second)
	if exists, _ := store.Exists(ctx, "u1"); !exists {
		t.Fatal("session expired early")
	}

	if _, err := store.Heartbeat(ctx, "u1", "s1", 21000); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	mr.FastForward(20 * time.Second)
	exists, err := store.Exists(ctx, "u1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if !exists {
		t.Error("session expired despite a heartbeat inside the TTL window")
	}
}

func TestSessionExpiresWithoutHeartbeat(t *testing.T) {
	ctx := context.Background()
	mr, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	mr.FastForward(testSessionTTL + time.Second)

	exists, err := store.Exists(ctx, "u1")
	if err != nil {
		t.Fatalf("Exists: %v", err)
	}
	if exists {
		t.Error("session survived past its TTL with no heartbeat")
	}

	// The index entry outlives the hash. That gap is exactly what the reaper
	// exists to close, and what drift measures in the meantime.
	n, _ := store.SessionCount(ctx)
	if n != 1 {
		t.Errorf("session index: got %d, want 1 (stale entry awaiting the reaper)", n)
	}
}

// An evicted connection heartbeating must not resurrect its session. This is
// what makes eviction stick without the two gateways coordinating.
func TestHeartbeatFromEvictedSessionIsRejected(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := store.Claim(ctx, session("u1", "s2", "gw-2", protocol.StatusOnline, 2000)); err != nil {
		t.Fatalf("second claim: %v", err)
	}

	res, err := store.Heartbeat(ctx, "u1", "s1", 3000)
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if res != UpdateWrongSession {
		t.Errorf("got %v, want UpdateWrongSession", res)
	}
}

// ---------------------------------------------------------------------------
// Store: guarded delete
// ---------------------------------------------------------------------------

// A slow disconnect must not remove a session the user has already
// re-established elsewhere — otherwise a fast reconnect produces a spurious
// OFFLINE.
func TestDeleteIsGuardedBySessionID(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if _, err := store.Claim(ctx, session("u1", "s2", "gw-2", protocol.StatusOnline, 2000)); err != nil {
		t.Fatalf("second claim: %v", err)
	}

	removed, err := store.Delete(ctx, "u1", "s1")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if removed {
		t.Error("a stale session ID deleted the current session")
	}
	if _, err := store.Get(ctx, "u1"); err != nil {
		t.Errorf("current session was removed: %v", err)
	}

	removed, err = store.Delete(ctx, "u1", "s2")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !removed {
		t.Error("the current session ID failed to delete")
	}
	if _, err := store.Get(ctx, "u1"); !errors.Is(err, ErrNoSession) {
		t.Errorf("after delete: got %v, want ErrNoSession", err)
	}
}

func TestUnguardedDeleteAlwaysRemoves(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	removed, err := store.Delete(ctx, "u1", "")
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if !removed {
		t.Error("unguarded delete failed to remove the session")
	}
	if n, _ := store.SessionCount(ctx); n != 0 {
		t.Errorf("session index: got %d, want 0", n)
	}
}

func TestGetManyPipelinesAndOmitsMissing(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	for _, u := range []string{"u1", "u2"} {
		if _, err := store.Claim(ctx, session(u, "s-"+u, "gw-1", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim(%s): %v", u, err)
		}
	}

	got, err := store.GetMany(ctx, []string{"u1", "u2", "ghost"})
	if err != nil {
		t.Fatalf("GetMany: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("GetMany returned %d sessions, want 2", len(got))
	}
	if _, present := got["ghost"]; present {
		t.Error("GetMany invented a session for a user with none")
	}
	if got["u1"].SessionID != "s-u1" {
		t.Errorf("u1: %+v", got["u1"])
	}
}

func TestGetManyOnEmptyInput(t *testing.T) {
	ctx := context.Background()
	_, store := newStore(t)

	got, err := store.GetMany(ctx, nil)
	if err != nil {
		t.Fatalf("GetMany(nil): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("got %d sessions, want 0", len(got))
	}
}

func TestJoinable(t *testing.T) {
	cases := []struct {
		name string
		sess *Session
		want bool
	}{
		{"nil", nil, false},
		{"in game with place", &Session{Status: protocol.StatusInGame, PlaceID: "p", ServerID: "s"}, true},
		{"in game no place", &Session{Status: protocol.StatusInGame, ServerID: "s"}, false},
		{"in game no server", &Session{Status: protocol.StatusInGame, PlaceID: "p"}, false},
		{"online with place", &Session{Status: protocol.StatusOnline, PlaceID: "p", ServerID: "s"}, false},
		{"offline", &Session{Status: protocol.StatusOffline}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.sess.Joinable(); got != tc.want {
				t.Errorf("Joinable: got %v, want %v", got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

func TestRegistryHeartbeatAndLiveNodes(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	reg := NewRegistry(rdb, 6*time.Second)

	if err := reg.Heartbeat(ctx, "gw-1", 10); err != nil {
		t.Fatalf("Heartbeat gw-1: %v", err)
	}
	if err := reg.Heartbeat(ctx, "gw-2", 25); err != nil {
		t.Fatalf("Heartbeat gw-2: %v", err)
	}

	nodes, err := reg.LiveNodes(ctx)
	if err != nil {
		t.Fatalf("LiveNodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Fatalf("LiveNodes: got %d, want 2", len(nodes))
	}

	total, err := reg.TotalConnections(ctx)
	if err != nil {
		t.Fatalf("TotalConnections: %v", err)
	}
	if total != 35 {
		t.Errorf("TotalConnections: got %d, want 35", total)
	}
}

// A gateway that stops heartbeating disappears on its own. Nothing detects the
// failure or announces it, so there is no failover path that can itself fail.
func TestRegistryDropsNodesThatStopHeartbeating(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedis(t)
	reg := NewRegistry(rdb, 6*time.Second)

	for _, id := range []string{"gw-1", "gw-2", "gw-3"} {
		if err := reg.Heartbeat(ctx, id, 5); err != nil {
			t.Fatalf("Heartbeat(%s): %v", id, err)
		}
	}

	mr.FastForward(7 * time.Second)

	// gw-1 keeps going; the other two are dead.
	if err := reg.Heartbeat(ctx, "gw-1", 5); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	ids, err := reg.LiveNodeIDs(ctx)
	if err != nil {
		t.Fatalf("LiveNodeIDs: %v", err)
	}
	if len(ids) != 1 || ids[0] != "gw-1" {
		t.Errorf("LiveNodeIDs: got %v, want [gw-1]", ids)
	}
}

// The TTL key vanishes on its own but set membership does not, so dead nodes
// leave entries behind. They must be pruned or the set grows without bound.
func TestRegistryPrunesStaleSetEntries(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedis(t)
	reg := NewRegistry(rdb, 6*time.Second)

	for _, id := range []string{"gw-1", "gw-2"} {
		if err := reg.Heartbeat(ctx, id, 1); err != nil {
			t.Fatalf("Heartbeat(%s): %v", id, err)
		}
	}
	mr.FastForward(7 * time.Second)

	if _, err := reg.LiveNodes(ctx); err != nil {
		t.Fatalf("LiveNodes: %v", err)
	}

	members, err := rdb.SMembers(ctx, nodeSetKey).Result()
	if err != nil {
		t.Fatalf("SMembers: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("stale node-set entries survived: %v", members)
	}
}

// A planned restart should rebalance the ring at once rather than leaving shards
// unowned for a full TTL.
func TestDeregisterRemovesNodeImmediately(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	reg := NewRegistry(rdb, 60*time.Second)

	if err := reg.Heartbeat(ctx, "gw-1", 3); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if err := reg.Deregister(ctx, "gw-1"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}

	ids, err := reg.LiveNodeIDs(ctx)
	if err != nil {
		t.Fatalf("LiveNodeIDs: %v", err)
	}
	if len(ids) != 0 {
		t.Errorf("LiveNodeIDs after deregister: got %v, want empty", ids)
	}
}

func TestRegistryOnEmptyCluster(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	reg := NewRegistry(rdb, 6*time.Second)

	nodes, err := reg.LiveNodes(ctx)
	if err != nil {
		t.Fatalf("LiveNodes on empty registry: %v", err)
	}
	if len(nodes) != 0 {
		t.Errorf("got %d nodes, want 0", len(nodes))
	}
}

// ---------------------------------------------------------------------------
// Bus
// ---------------------------------------------------------------------------

func startBus(t *testing.T, rdb *redis.Client, gatewayID string) (*Bus, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	bus := NewBus(ctx, rdb, gatewayID, testMetrics(), testLogger())

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = bus.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = bus.Close()
		<-done
	})
	return bus, cancel
}

// The core cross-node claim: a presence change published by one gateway reaches
// a subscriber held by a different one.
func TestBusDeliversPresenceAcrossGateways(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)

	publisher, _ := startBus(t, rdb, "gw-1")
	subscriber, _ := startBus(t, rdb, "gw-3")

	received := make(chan protocol.Presence, 1)
	unsub, err := subscriber.Subscribe(ctx, "u1", func(p protocol.Presence) {
		received <- p
	})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsub()

	// miniredis registers the subscription synchronously, but the pump goroutine
	// needs a moment to attach before a publish can be observed.
	waitFor(t, func() bool { return subscriber.SubscribedUsers() == 1 })

	want := protocol.Presence{UserID: "u1", Status: protocol.StatusInGame, PlaceID: "p9", TS: 1234}
	if err := publisher.Publish(ctx, want); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case got := <-received:
		if got.UserID != want.UserID || got.Status != want.Status || got.PlaceID != want.PlaceID || got.TS != want.TS {
			t.Errorf("received %+v, want %+v", got, want)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("presence event published on gw-1 never reached the subscriber on gw-3")
	}
}

// Redis subscriptions are refcounted: a gateway with many clients watching one
// popular user must hold a single subscription, not one per client.
func TestBusRefcountsSubscriptions(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	bus, _ := startBus(t, rdb, "gw-1")

	var mu sync.Mutex
	var calls int
	handler := func(protocol.Presence) {
		mu.Lock()
		calls++
		mu.Unlock()
	}

	unsub1, err := bus.Subscribe(ctx, "u1", handler)
	if err != nil {
		t.Fatalf("Subscribe 1: %v", err)
	}
	unsub2, err := bus.Subscribe(ctx, "u1", handler)
	if err != nil {
		t.Fatalf("Subscribe 2: %v", err)
	}

	if got := bus.SubscribedUsers(); got != 1 {
		t.Errorf("SubscribedUsers with 2 local subscribers to 1 user: got %d, want 1", got)
	}

	unsub1()
	if got := bus.SubscribedUsers(); got != 1 {
		t.Errorf("after releasing 1 of 2: got %d, want 1", got)
	}

	unsub2()
	if got := bus.SubscribedUsers(); got != 0 {
		t.Errorf("after releasing both: got %d, want 0", got)
	}
}

func TestBusUnsubscribeIsIdempotent(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	bus, _ := startBus(t, rdb, "gw-1")

	unsub, err := bus.Subscribe(ctx, "u1", func(protocol.Presence) {})
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	unsub()
	unsub()
	unsub()

	if got := bus.SubscribedUsers(); got != 0 {
		t.Errorf("SubscribedUsers: got %d, want 0", got)
	}
}

// A gateway must only receive events for users its own clients asked about.
func TestBusDoesNotDeliverUnsubscribedUsers(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)

	publisher, _ := startBus(t, rdb, "gw-1")
	subscriber, _ := startBus(t, rdb, "gw-2")

	got := make(chan protocol.Presence, 4)
	unsub, err := subscriber.Subscribe(ctx, "u1", func(p protocol.Presence) { got <- p })
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	defer unsub()
	waitFor(t, func() bool { return subscriber.SubscribedUsers() == 1 })

	if err := publisher.Publish(ctx, protocol.Presence{UserID: "u2", Status: protocol.StatusOnline, TS: 1}); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if err := publisher.Publish(ctx, protocol.Presence{UserID: "u1", Status: protocol.StatusAway, TS: 2}); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	select {
	case p := <-got:
		if p.UserID != "u1" {
			t.Errorf("received an event for an unsubscribed user: %+v", p)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscribed event never arrived")
	}
}

// Eviction is addressed to a specific gateway rather than broadcast, so it costs
// one subscription per gateway instead of one per connection.
func TestBusDeliversControlMessageToNamedGateway(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)

	sender, _ := startBus(t, rdb, "gw-1")
	target, _ := startBus(t, rdb, "gw-2")
	bystander, _ := startBus(t, rdb, "gw-3")

	hit := make(chan ControlMessage, 1)
	miss := make(chan ControlMessage, 1)
	target.OnControl(func(cm ControlMessage) { hit <- cm })
	bystander.OnControl(func(cm ControlMessage) { miss <- cm })

	err := sender.SendControl(ctx, "gw-2", ControlMessage{
		Type: ControlEvict, UserID: "u1", SessionID: "s1",
	})
	if err != nil {
		t.Fatalf("SendControl: %v", err)
	}

	select {
	case cm := <-hit:
		if cm.Type != ControlEvict || cm.UserID != "u1" || cm.SessionID != "s1" {
			t.Errorf("control message: %+v", cm)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("control message never reached the addressed gateway")
	}

	select {
	case cm := <-miss:
		t.Errorf("an unaddressed gateway received the control message: %+v", cm)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestBusSubscribeAfterCloseFails(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	bus := NewBus(ctx, rdb, "gw-1", testMetrics(), testLogger())
	if err := bus.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	if _, err := bus.Subscribe(ctx, "u1", func(protocol.Presence) {}); err == nil {
		t.Error("Subscribe on a closed bus returned no error")
	}
}

// ---------------------------------------------------------------------------
// Reaper: stale sessions (check 4) and orphans (check 5)
// ---------------------------------------------------------------------------

type reaperFixture struct {
	mr    *miniredis.Miniredis
	rdb   *redis.Client
	store *Store
	reg   *Registry
	bus   *Bus
	ring  *ring.Ring
	reap  *Reaper
}

func newReaperFixture(t *testing.T, gatewayID string) *reaperFixture {
	t.Helper()
	mr, rdb := newRedis(t)
	store := NewStore(rdb, testSessionTTL)
	reg := NewRegistry(rdb, 6*time.Second)
	bus, _ := startBus(t, rdb, gatewayID)
	r := ring.New(ring.DefaultVirtualNodes)

	return &reaperFixture{
		mr: mr, rdb: rdb, store: store, reg: reg, bus: bus, ring: r,
		reap: NewReaper(store, reg, bus, r, gatewayID, testMetrics(), testLogger()),
	}
}

// A session whose hash expired leaves an index entry behind. The reaper is what
// closes that gap.
func TestReaperRemovesExpiredSessions(t *testing.T) {
	ctx := context.Background()
	f := newReaperFixture(t, "gw-1")

	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	for _, u := range []string{"u1", "u2", "u3"} {
		if _, err := f.store.Claim(ctx, session(u, "s-"+u, "gw-1", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim(%s): %v", u, err)
		}
	}

	// Sessions expire; node keys are refreshed so the gateway stays live.
	f.mr.FastForward(testSessionTTL + time.Second)
	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	res, err := f.reap.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Expired != 3 {
		t.Errorf("Expired: got %d, want 3", res.Expired)
	}

	n, _ := f.store.SessionCount(ctx)
	if n != 0 {
		t.Errorf("session index after sweep: got %d, want 0", n)
	}
}

// A session whose gateway vanished is still alive in Redis, so nothing else will
// clean it up — JOIN would keep resolving to a node that no longer exists.
func TestReaperReclaimsOrphanedSessions(t *testing.T) {
	ctx := context.Background()
	f := newReaperFixture(t, "gw-1")

	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat gw-1: %v", err)
	}
	if err := f.reg.Heartbeat(ctx, "gw-dead", 5); err != nil {
		t.Fatalf("Heartbeat gw-dead: %v", err)
	}

	// Sessions served by a gateway that is about to die.
	for _, u := range []string{"u1", "u2"} {
		if _, err := f.store.Claim(ctx, session(u, "s-"+u, "gw-dead", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim(%s): %v", u, err)
		}
	}

	// gw-dead stops heartbeating; its node key expires but the sessions do not.
	f.mr.FastForward(7 * time.Second)
	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	res, err := f.reap.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Orphaned != 2 {
		t.Errorf("Orphaned: got %d, want 2 (result: %+v)", res.Orphaned, res)
	}

	if _, err := f.store.Get(ctx, "u1"); !errors.Is(err, ErrNoSession) {
		t.Errorf("orphaned session survived: %v", err)
	}
	if n, _ := f.store.SessionCount(ctx); n != 0 {
		t.Errorf("session index: got %d, want 0", n)
	}
}

// The whole reason the ring exists: each session is reaped by exactly one
// gateway, so N replicas do not each do the same work and race to publish
// duplicate OFFLINE transitions.
func TestReaperOnlySweepsShardsItOwns(t *testing.T) {
	ctx := context.Background()
	mr, rdb := newRedis(t)
	store := NewStore(rdb, testSessionTTL)
	reg := NewRegistry(rdb, 6*time.Second)
	bus, _ := startBus(t, rdb, "gw-1")

	for _, id := range []string{"gw-1", "gw-2", "gw-3"} {
		if err := reg.Heartbeat(ctx, id, 0); err != nil {
			t.Fatalf("Heartbeat(%s): %v", id, err)
		}
	}

	const users = 300
	ids := make([]string, users)
	for i := range ids {
		ids[i] = "user-" + itoa(i)
		if _, err := store.Claim(ctx, session(ids[i], "s", "gw-1", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim: %v", err)
		}
	}

	mr.FastForward(testSessionTTL + time.Second)
	for _, id := range []string{"gw-1", "gw-2", "gw-3"} {
		if err := reg.Heartbeat(ctx, id, 0); err != nil {
			t.Fatalf("Heartbeat(%s): %v", id, err)
		}
	}

	// Ownership is checked against the full keyspace before any sweeping, because
	// each sweep removes what it reaps: by the time the third gateway runs, only
	// its own shards remain, so it would legitimately skip nothing.
	shared := ring.New(ring.DefaultVirtualNodes)
	shared.Add("gw-1", "gw-2", "gw-3")
	for _, gw := range []string{"gw-1", "gw-2", "gw-3"} {
		owned := 0
		for _, id := range ids {
			if shared.Owns(id, gw) {
				owned++
			}
		}
		if owned == 0 {
			t.Errorf("%s owns no shards; ownership is not distributed", gw)
		}
		if owned == users {
			t.Errorf("%s owns every shard; ownership is not distributed", gw)
		}
	}

	// Each gateway sweeps the same store; between them they must cover every
	// session exactly once.
	total := 0
	for _, gw := range []string{"gw-1", "gw-2", "gw-3"} {
		r := ring.New(ring.DefaultVirtualNodes)
		reaper := NewReaper(store, reg, bus, r, gw, testMetrics(), testLogger())
		res, err := reaper.Sweep(ctx)
		if err != nil {
			t.Fatalf("Sweep(%s): %v", gw, err)
		}
		total += res.Expired
	}

	if total != users {
		t.Errorf("the three gateways reaped %d sessions between them, want %d", total, users)
	}
	if n, _ := store.SessionCount(ctx); n != 0 {
		t.Errorf("session index after all sweeps: got %d, want 0", n)
	}
}

// A gateway that cannot see itself in the registry has a broken view of the
// cluster. Reaping on the strength of that view would delete live sessions.
func TestReaperDoesNothingWhenRingIsEmpty(t *testing.T) {
	ctx := context.Background()
	f := newReaperFixture(t, "gw-1")

	if _, err := f.store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	f.mr.FastForward(testSessionTTL + time.Second)

	// No registry heartbeat at all, so the ring is empty.
	res, err := f.reap.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Expired != 0 || res.Orphaned != 0 {
		t.Errorf("reaped with an empty ring: %+v", res)
	}
	if n, _ := f.store.SessionCount(ctx); n != 1 {
		t.Errorf("session index: got %d, want 1 (untouched)", n)
	}
}

func TestReaperLeavesHealthySessionsAlone(t *testing.T) {
	ctx := context.Background()
	f := newReaperFixture(t, "gw-1")

	if err := f.reg.Heartbeat(ctx, "gw-1", 2); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	for _, u := range []string{"u1", "u2"} {
		if _, err := f.store.Claim(ctx, session(u, "s-"+u, "gw-1", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim(%s): %v", u, err)
		}
	}

	res, err := f.reap.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Expired != 0 || res.Orphaned != 0 {
		t.Errorf("healthy sessions were reaped: %+v", res)
	}
	if n, _ := f.store.SessionCount(ctx); n != 2 {
		t.Errorf("session index: got %d, want 2", n)
	}
}

// A session with no gateway recorded is malformed; treating it as live would
// make it permanently unreapable.
func TestReaperReclaimsSessionWithNoGatewayRecorded(t *testing.T) {
	ctx := context.Background()
	f := newReaperFixture(t, "gw-1")

	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if _, err := f.store.Claim(ctx, session("u1", "s1", "", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}

	res, err := f.reap.Sweep(ctx)
	if err != nil {
		t.Fatalf("Sweep: %v", err)
	}
	if res.Orphaned != 1 {
		t.Errorf("Orphaned: got %d, want 1", res.Orphaned)
	}
}

// ---------------------------------------------------------------------------
// Reconciler: drift (check 6)
// ---------------------------------------------------------------------------

func TestDriftIsZeroInSteadyState(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	store := NewStore(rdb, testSessionTTL)
	reg := NewRegistry(rdb, 6*time.Second)
	rec := NewReconciler(store, reg, testMetrics(), testLogger())

	for i := 0; i < 5; i++ {
		if _, err := store.Claim(ctx, session("u"+itoa(i), "s", "gw-1", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim: %v", err)
		}
	}
	if err := reg.Heartbeat(ctx, "gw-1", 5); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	rep, err := rec.Check(ctx)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rep.Drift != 0 {
		t.Errorf("drift in steady state: got %d (in-memory %d, redis %d)", rep.Drift, rep.InMemory, rep.InRedis)
	}
}

// The sign identifies which side is stale, which is why drift is not reported
// as an absolute value.
func TestDriftSignIdentifiesTheStaleSide(t *testing.T) {
	ctx := context.Background()
	_, rdb := newRedis(t)
	store := NewStore(rdb, testSessionTTL)
	reg := NewRegistry(rdb, 6*time.Second)
	rec := NewReconciler(store, reg, testMetrics(), testLogger())

	// Gateways claim more connections than Redis has sessions for.
	if _, err := store.Claim(ctx, session("u1", "s1", "gw-1", protocol.StatusOnline, 1000)); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	if err := reg.Heartbeat(ctx, "gw-1", 4); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	rep, err := rec.Check(ctx)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rep.Drift != 3 {
		t.Errorf("drift: got %d, want +3", rep.Drift)
	}

	// Now the other direction: Redis holds sessions no gateway is serving.
	if err := reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	rep, err = rec.Check(ctx)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rep.Drift != -1 {
		t.Errorf("drift: got %d, want -1", rep.Drift)
	}
}

// Drift going non-zero after a gateway dies, then returning to zero once the
// reaper has cleaned up, is the exact sequence the chaos benchmark measures.
func TestDriftReturnsToZeroAfterReaping(t *testing.T) {
	ctx := context.Background()
	f := newReaperFixture(t, "gw-1")
	rec := NewReconciler(f.store, f.reg, testMetrics(), testLogger())

	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat gw-1: %v", err)
	}
	if err := f.reg.Heartbeat(ctx, "gw-dead", 3); err != nil {
		t.Fatalf("Heartbeat gw-dead: %v", err)
	}
	for _, u := range []string{"u1", "u2", "u3"} {
		if _, err := f.store.Claim(ctx, session(u, "s-"+u, "gw-dead", protocol.StatusOnline, 1000)); err != nil {
			t.Fatalf("Claim(%s): %v", u, err)
		}
	}

	if rep, err := rec.Check(ctx); err != nil {
		t.Fatalf("Check: %v", err)
	} else if rep.Drift != 0 {
		t.Fatalf("drift before the failure: got %d, want 0", rep.Drift)
	}

	// gw-dead dies. Its connections vanish from the in-memory total, but its
	// sessions remain in Redis.
	f.mr.FastForward(7 * time.Second)
	if err := f.reg.Heartbeat(ctx, "gw-1", 0); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}

	rep, err := rec.Check(ctx)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rep.Drift != -3 {
		t.Fatalf("drift after the failure: got %d, want -3", rep.Drift)
	}

	if _, err := f.reap.Sweep(ctx); err != nil {
		t.Fatalf("Sweep: %v", err)
	}

	rep, err = rec.Check(ctx)
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rep.Drift != 0 {
		t.Errorf("drift did not return to zero after reaping: got %d", rep.Drift)
	}
}

// ---------------------------------------------------------------------------
// Service
// ---------------------------------------------------------------------------

func newService(t *testing.T, gatewayID string) (*miniredis.Miniredis, *Service) {
	t.Helper()
	mr, rdb := newRedis(t)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	cfg := DefaultConfig(gatewayID)
	svc := NewService(ctx, rdb, cfg, testMetrics(), testLogger())
	svc.SetConnectionCounter(func() int64 { return 0 })

	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = svc.Bus.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		_ = svc.Bus.Close()
		<-done
	})

	if err := svc.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	return mr, svc
}

func TestServiceConnectAndJoin(t *testing.T) {
	ctx := context.Background()
	_, svc := newService(t, "gw-1")

	if _, err := svc.Connect(ctx, "target", "s-target"); err != nil {
		t.Fatalf("Connect: %v", err)
	}

	// Online but nowhere in particular: nothing to join.
	out, err := svc.Join(ctx, "target")
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if out.OK || out.Reason != protocol.ReasonTargetNotJoinable {
		t.Errorf("join an online-but-idle user: %+v", out)
	}

	res, err := svc.SetPresence(ctx, "target", "s-target", protocol.StatusInGame, "place-7", "srv-2")
	if err != nil {
		t.Fatalf("SetPresence: %v", err)
	}
	if res != UpdateApplied {
		t.Fatalf("SetPresence: got %v, want UpdateApplied", res)
	}

	out, err = svc.Join(ctx, "target")
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if !out.OK || out.PlaceID != "place-7" || out.ServerID != "srv-2" {
		t.Errorf("join an in-game user: %+v", out)
	}
}

func TestServiceJoinUnknownUser(t *testing.T) {
	ctx := context.Background()
	_, svc := newService(t, "gw-1")

	out, err := svc.Join(ctx, "nobody")
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if out.OK || out.Reason != protocol.ReasonTargetUnknown {
		t.Errorf("got %+v, want denied TARGET_UNKNOWN", out)
	}
}

func TestServiceJoinOfflineUser(t *testing.T) {
	ctx := context.Background()
	_, svc := newService(t, "gw-1")

	if _, err := svc.Connect(ctx, "u1", "s1"); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := svc.SetPresence(ctx, "u1", "s1", protocol.StatusOffline, "", ""); err != nil {
		t.Fatalf("SetPresence: %v", err)
	}

	out, err := svc.Join(ctx, "u1")
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if out.OK || out.Reason != protocol.ReasonTargetOffline {
		t.Errorf("got %+v, want denied TARGET_OFFLINE", out)
	}
}

func TestServiceDisconnectIsGuarded(t *testing.T) {
	ctx := context.Background()
	_, svc := newService(t, "gw-1")

	if _, err := svc.Connect(ctx, "u1", "s1"); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if _, err := svc.Connect(ctx, "u1", "s2"); err != nil {
		t.Fatalf("reconnect: %v", err)
	}

	// The old connection tearing down must not remove the new session.
	if err := svc.Disconnect(ctx, "u1", "s1"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if _, err := svc.Store.Get(ctx, "u1"); err != nil {
		t.Errorf("a stale disconnect removed the live session: %v", err)
	}

	if err := svc.Disconnect(ctx, "u1", "s2"); err != nil {
		t.Fatalf("Disconnect: %v", err)
	}
	if _, err := svc.Store.Get(ctx, "u1"); !errors.Is(err, ErrNoSession) {
		t.Errorf("session survived its own disconnect: %v", err)
	}
}

func TestServiceSnapshot(t *testing.T) {
	ctx := context.Background()
	_, svc := newService(t, "gw-1")

	for _, u := range []string{"a", "b"} {
		if _, err := svc.Connect(ctx, u, "s-"+u); err != nil {
			t.Fatalf("Connect(%s): %v", u, err)
		}
	}

	snap, err := svc.Snapshot(ctx, []string{"a", "b", "missing"})
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if len(snap) != 2 {
		t.Errorf("Snapshot returned %d entries, want 2", len(snap))
	}
	if snap["a"].Status != protocol.StatusOnline {
		t.Errorf("a: %+v", snap["a"])
	}
}

func TestServiceRingState(t *testing.T) {
	ctx := context.Background()
	_, svc := newService(t, "gw-1")

	for i := 0; i < 20; i++ {
		if _, err := svc.Connect(ctx, "u"+itoa(i), "s"+itoa(i)); err != nil {
			t.Fatalf("Connect: %v", err)
		}
	}

	snap, err := svc.RingState(ctx)
	if err != nil {
		t.Fatalf("RingState: %v", err)
	}
	if snap.GatewayID != "gw-1" {
		t.Errorf("gatewayId: %q", snap.GatewayID)
	}
	if snap.MemberCount != 1 || len(snap.Members) != 1 {
		t.Errorf("members: %+v", snap.Members)
	}
	if snap.TotalShards != 20 {
		t.Errorf("totalShards: got %d, want 20", snap.TotalShards)
	}
	// Sole member of the ring owns everything.
	if snap.OwnedShards != 20 {
		t.Errorf("ownedShards: got %d, want 20", snap.OwnedShards)
	}
	if snap.VirtualNodes != ring.DefaultVirtualNodes {
		t.Errorf("virtualNodes: got %d, want %d", snap.VirtualNodes, ring.DefaultVirtualNodes)
	}
}

// Timing relationships matter more than the individual values: too tight and
// healthy nodes get evicted, too loose and dead ones keep owning shards.
func TestDefaultConfigTimingsAreCoherent(t *testing.T) {
	cfg := DefaultConfig("gw-1")

	if cfg.NodeTTL <= cfg.RegistryInterval {
		t.Errorf("NodeTTL (%v) must exceed RegistryInterval (%v) or every node flaps",
			cfg.NodeTTL, cfg.RegistryInterval)
	}
	if cfg.NodeTTL < 2*cfg.RegistryInterval {
		t.Errorf("NodeTTL (%v) tolerates fewer than 2 missed heartbeats (interval %v)",
			cfg.NodeTTL, cfg.RegistryInterval)
	}
	if cfg.SessionTTL <= cfg.NodeTTL {
		t.Errorf("SessionTTL (%v) should exceed NodeTTL (%v); orphan detection, not expiry, cleans up after a dead gateway",
			cfg.SessionTTL, cfg.NodeTTL)
	}
	if cfg.ReaperInterval >= cfg.SessionTTL {
		t.Errorf("ReaperInterval (%v) must be well under SessionTTL (%v)",
			cfg.ReaperInterval, cfg.SessionTTL)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			// Give the pub/sub pump a moment to attach after the bookkeeping
			// says the subscription exists.
			time.Sleep(50 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within 3s")
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}

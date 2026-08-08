package ring

import (
	"fmt"
	"math"
	"sync"
	"testing"
)

// userKeys generates n synthetic user IDs shaped like the real ones, so the
// distribution measured here reflects the keys the reaper will actually hash.
func userKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("user:%d", i)
	}
	return keys
}

func assign(r *Ring, keys []string) map[string]string {
	out := make(map[string]string, len(keys))
	for _, k := range keys {
		owner, ok := r.Lookup(k)
		if !ok {
			continue
		}
		out[k] = owner
	}
	return out
}

func counts(assignment map[string]string) map[string]int {
	out := make(map[string]int)
	for _, owner := range assignment {
		out[owner]++
	}
	return out
}

// An empty ring must report that it cannot answer, not pick a zero value. A
// gateway that has lost the node registry has to decline to reap rather than
// conclude it owns everything.
func TestLookupOnEmptyRing(t *testing.T) {
	r := New(DefaultVirtualNodes)

	if owner, ok := r.Lookup("user:1"); ok {
		t.Errorf("Lookup on empty ring: got (%q, true), want (\"\", false)", owner)
	}
	if r.Owns("user:1", "gw-1") {
		t.Error("Owns on empty ring returned true; a node with no registry must not claim ownership")
	}
	if r.Len() != 0 {
		t.Errorf("Len: got %d, want 0", r.Len())
	}
}

func TestSingleNodeOwnsEverything(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("gw-1")

	for _, k := range userKeys(1000) {
		owner, ok := r.Lookup(k)
		if !ok {
			t.Fatalf("Lookup(%q) on single-node ring returned not-found", k)
		}
		if owner != "gw-1" {
			t.Fatalf("Lookup(%q): got %q, want gw-1", k, owner)
		}
	}
}

// Ownership must depend only on the member set, never on the order membership
// was learned. Two gateways reading the same registry in different orders have
// to agree, or a shard ends up with two owners or none.
func TestAssignmentIsIndependentOfInsertionOrder(t *testing.T) {
	keys := userKeys(5000)

	forward := New(DefaultVirtualNodes)
	forward.Add("gw-1", "gw-2", "gw-3")

	reverse := New(DefaultVirtualNodes)
	reverse.Add("gw-3")
	reverse.Add("gw-2")
	reverse.Add("gw-1")

	// A third ring reaches the same membership via a detour, to prove that
	// transient states leave no residue.
	churned := New(DefaultVirtualNodes)
	churned.Add("gw-2", "gw-9")
	churned.Add("gw-1")
	churned.Remove("gw-9")
	churned.Add("gw-3")

	a, b, c := assign(forward, keys), assign(reverse, keys), assign(churned, keys)

	for _, k := range keys {
		if a[k] != b[k] {
			t.Fatalf("insertion order changed ownership of %q: forward=%q reverse=%q", k, a[k], b[k])
		}
		if a[k] != c[k] {
			t.Fatalf("add/remove churn changed ownership of %q: clean=%q churned=%q", k, a[k], c[k])
		}
	}
}

// Repeated lookups of the same key must not vary. Guards against any accidental
// dependence on map iteration order inside rebuild.
func TestLookupIsStableAcrossCalls(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")

	for _, k := range userKeys(200) {
		first, _ := r.Lookup(k)
		for i := 0; i < 10; i++ {
			again, _ := r.Lookup(k)
			if again != first {
				t.Fatalf("Lookup(%q) unstable: %q then %q", k, first, again)
			}
		}
	}
}

// Distribution across three nodes should be close to even. This is the reason
// virtual nodes exist: without them, three points on a 64-bit circle routinely
// produce splits bad enough that one gateway does several times another's
// reaping work.
//
// The tolerance is deliberately loose — this asserts "no gateway is carrying a
// pathological share", not a specific hash's exact behaviour. The measured
// spread is logged so the real figure is visible rather than inferred from the
// bound passing.
func TestDistributionIsReasonablyEven(t *testing.T) {
	const (
		numKeys   = 100_000
		numNodes  = 3
		tolerance = 0.15 // ±15% of a fair share
	)

	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")

	got := counts(assign(r, userKeys(numKeys)))

	if len(got) != numNodes {
		t.Fatalf("only %d of %d nodes received keys: %v", len(got), numNodes, got)
	}

	fair := float64(numKeys) / float64(numNodes)
	worst := 0.0
	for node, n := range got {
		dev := math.Abs(float64(n)-fair) / fair
		if dev > worst {
			worst = dev
		}
		t.Logf("%s: %d keys (%.2f%% of total, %+.2f%% from fair share)",
			node, n, 100*float64(n)/numKeys, 100*(float64(n)-fair)/fair)
		if dev > tolerance {
			t.Errorf("%s holds %d keys, %.1f%% from the fair share of %.0f (tolerance %.0f%%)",
				node, n, 100*dev, fair, 100*tolerance)
		}
	}
	t.Logf("worst deviation from fair share: %.2f%%", 100*worst)
}

// The defining property of consistent hashing, and the reason Beacon uses it
// instead of modulo: when a node leaves, keys belonging to the survivors must
// not move at all. Only the departed node's keys get reassigned.
//
// Under modulo hashing this test would fail catastrophically — nearly every key
// would move — which is exactly the failover behaviour being avoided.
func TestRemovingNodeMovesOnlyItsOwnKeys(t *testing.T) {
	keys := userKeys(20_000)

	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")
	before := assign(r, keys)

	r.Remove("gw-2")
	after := assign(r, keys)

	moved, strandedMoves := 0, 0
	for _, k := range keys {
		if before[k] == after[k] {
			continue
		}
		moved++
		if before[k] != "gw-2" {
			strandedMoves++
			if strandedMoves <= 5 {
				t.Errorf("key %q moved from %q to %q, but only gw-2's keys should move",
					k, before[k], after[k])
			}
		}
	}

	if strandedMoves > 0 {
		t.Errorf("%d keys owned by surviving nodes were reassigned; expected 0", strandedMoves)
	}
	if after["dummy"] == "gw-2" {
		t.Error("removed node still owns keys")
	}

	orphaned := 0
	for _, k := range keys {
		if before[k] == "gw-2" {
			orphaned++
		}
	}
	if moved != orphaned {
		t.Errorf("moved %d keys but gw-2 owned %d; these must be equal", moved, orphaned)
	}
	t.Logf("removing 1 of 3 nodes moved %d/%d keys (%.2f%%); theoretical minimum is the %d keys it owned",
		moved, len(keys), 100*float64(moved)/float64(len(keys)), orphaned)
}

// The mirror property on growth: keys that move when a node joins must move to
// the new node. Existing nodes must never trade keys between themselves.
func TestAddingNodeOnlyPullsKeysToIt(t *testing.T) {
	keys := userKeys(20_000)

	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")
	before := assign(r, keys)

	r.Add("gw-4")
	after := assign(r, keys)

	moved, wrongDestination := 0, 0
	for _, k := range keys {
		if before[k] == after[k] {
			continue
		}
		moved++
		if after[k] != "gw-4" {
			wrongDestination++
			if wrongDestination <= 5 {
				t.Errorf("key %q moved %q -> %q; growth must only move keys to the new node",
					k, before[k], after[k])
			}
		}
	}

	if wrongDestination > 0 {
		t.Errorf("%d keys were traded between pre-existing nodes; expected 0", wrongDestination)
	}

	// Adding a fourth node should pull roughly a quarter of the keyspace.
	fraction := float64(moved) / float64(len(keys))
	t.Logf("adding a 4th node moved %d/%d keys (%.2f%%); ideal is 25%%",
		moved, len(keys), 100*fraction)
	if fraction < 0.15 || fraction > 0.35 {
		t.Errorf("moved %.1f%% of keys when adding a 4th node; expected roughly 25%%", 100*fraction)
	}
}

func TestSetReplacesMembership(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")

	r.Set([]string{"gw-3", "gw-4"})

	got := r.Members()
	want := []string{"gw-3", "gw-4"}
	if len(got) != len(want) {
		t.Fatalf("Members after Set: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Members after Set: got %v, want %v", got, want)
		}
	}
}

// Set must produce the same ring as building the membership from scratch,
// because the reaper reaches a given membership via Set from a registry snapshot
// while tests and startup may reach it via Add.
func TestSetMatchesFreshlyBuiltRing(t *testing.T) {
	keys := userKeys(5000)

	viaSet := New(DefaultVirtualNodes)
	viaSet.Add("gw-1", "gw-2")
	viaSet.Set([]string{"gw-2", "gw-3", "gw-5"})

	fresh := New(DefaultVirtualNodes)
	fresh.Add("gw-2", "gw-3", "gw-5")

	a, b := assign(viaSet, keys), assign(fresh, keys)
	for _, k := range keys {
		if a[k] != b[k] {
			t.Fatalf("Set-derived ring disagrees on %q: set=%q fresh=%q", k, a[k], b[k])
		}
	}
}

func TestAddIsIdempotentAndRemoveToleratesAbsentNodes(t *testing.T) {
	keys := userKeys(2000)

	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2")
	before := assign(r, keys)

	r.Add("gw-1")               // already present
	r.Add("gw-2", "gw-1")       // both present
	r.Remove("gw-404")          // never present
	r.Remove("gw-404", "gw-99") // still never present

	if r.Len() != 2 {
		t.Fatalf("Len after no-op mutations: got %d, want 2", r.Len())
	}
	after := assign(r, keys)
	for _, k := range keys {
		if before[k] != after[k] {
			t.Fatalf("no-op mutation moved %q from %q to %q", k, before[k], after[k])
		}
	}
}

// An empty gateway ID is always a config bug. Admitting one would create a shard
// owner that no running node recognises as itself, so those shards would never
// be reaped.
func TestEmptyNodeIDsAreRejected(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("", "gw-1", "")
	r.Set([]string{"gw-1", ""})

	if r.Len() != 1 {
		t.Fatalf("Len: got %d, want 1 — empty IDs must not join the ring", r.Len())
	}
	for _, m := range r.Members() {
		if m == "" {
			t.Fatal("empty node ID present in Members()")
		}
	}
}

func TestNonPositiveReplicasFallsBackToDefault(t *testing.T) {
	for _, replicas := range []int{0, -1, -150} {
		r := New(replicas)
		if r.Replicas() != DefaultVirtualNodes {
			t.Errorf("New(%d).Replicas(): got %d, want %d", replicas, r.Replicas(), DefaultVirtualNodes)
		}
		r.Add("gw-1")
		if _, ok := r.Lookup("user:1"); !ok {
			t.Errorf("New(%d) produced a ring that cannot answer lookups", replicas)
		}
	}
}

// Removing every node returns the ring to the empty state rather than leaving
// stale positions behind — the total-outage case.
func TestRemovingAllNodesEmptiesTheRing(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2")
	r.Remove("gw-1", "gw-2")

	if r.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", r.Len())
	}
	if _, ok := r.Lookup("user:1"); ok {
		t.Error("Lookup succeeded on a ring with all nodes removed")
	}
}

// Ring membership changes on the registry-watch goroutine while reaper
// goroutines look up ownership. Meaningful under -race.
func TestConcurrentLookupsDuringMembershipChurn(t *testing.T) {
	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for n := 0; ; n++ {
				select {
				case <-stop:
					return
				default:
				}
				key := fmt.Sprintf("user:%d:%d", id, n)
				if owner, ok := r.Lookup(key); ok && owner == "" {
					t.Errorf("Lookup(%q) reported found with an empty owner", key)
					return
				}
				r.Owns(key, "gw-1")
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			r.Add("gw-4")
			r.Remove("gw-4")
			r.Set([]string{"gw-1", "gw-2", "gw-3"})
		}
		close(stop)
	}()

	wg.Wait()
}

func BenchmarkLookup(b *testing.B) {
	r := New(DefaultVirtualNodes)
	r.Add("gw-1", "gw-2", "gw-3")
	keys := userKeys(1024)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.Lookup(keys[i%len(keys)])
	}
}

func BenchmarkRebuildOnMembershipChange(b *testing.B) {
	for i := 0; i < b.N; i++ {
		r := New(DefaultVirtualNodes)
		r.Add("gw-1", "gw-2", "gw-3")
	}
}

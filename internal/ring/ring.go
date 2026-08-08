package ring

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"strconv"
	"sync"
)

// DefaultVirtualNodes is how many points each physical gateway occupies on the
// ring.
//
// Beacon runs three replicas. With one point per node, three random positions
// on a 64-bit circle divide it very unevenly — splits like 60/25/15 are common,
// which would hand one gateway four times another's reaping work. Replicating
// each node across many positions averages that out: the spread narrows roughly
// with the square root of the replica count. 150 is the usual choice and costs
// only 450 sorted entries at N=3, rebuilt only on membership change.
const DefaultVirtualNodes = 150

// Ring is a consistent hash ring mapping arbitrary keys to one of the gateway
// nodes currently on it.
//
// Beacon uses it for exactly one thing: deciding which live gateway owns
// stale-session reaping for a given user. It does not route client connections
// — clients may attach anywhere — so a brief disagreement between nodes about
// ring membership costs at most a duplicated or delayed reap, never a misrouted
// client.
//
// The property that matters here is not just "spreads keys evenly" but "moves
// as few keys as possible when membership changes". When a gateway dies, only
// the keys it owned are reassigned; every other key keeps its existing owner.
// That matters most during failover, precisely when the cluster can least
// afford a full reshuffle.
//
// A Ring is safe for concurrent use. Lookups take a read lock; membership
// changes take a write lock and rebuild the ring wholesale.
type Ring struct {
	mu sync.RWMutex

	replicas int

	// members is the set of physical node IDs on the ring.
	members map[string]struct{}

	// positions holds every virtual node's hash, sorted ascending. Lookup
	// binary-searches this for the first position at or after the key's hash.
	positions []uint64

	// owner maps a virtual node's position back to its physical node ID.
	owner map[uint64]string
}

// New returns an empty ring whose nodes each occupy replicas positions.
// A non-positive replicas falls back to DefaultVirtualNodes rather than
// producing a ring with no points, which would silently swallow every lookup.
func New(replicas int) *Ring {
	if replicas <= 0 {
		replicas = DefaultVirtualNodes
	}
	return &Ring{
		replicas: replicas,
		members:  make(map[string]struct{}),
		owner:    make(map[uint64]string),
	}
}

// Add places nodes on the ring. Adding a node already present is a no-op, so
// replaying a registry snapshot is safe. Empty IDs are ignored: an empty
// gateway ID is always a configuration bug, and admitting one would create a
// shard owner that no node believes is itself.
func (r *Ring) Add(nodes ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	changed := false
	for _, n := range nodes {
		if n == "" {
			continue
		}
		if _, exists := r.members[n]; exists {
			continue
		}
		r.members[n] = struct{}{}
		changed = true
	}
	if changed {
		r.rebuild()
	}
}

// Remove takes nodes off the ring. Removing an absent node is a no-op, so a
// gateway observed as dead by two different code paths does not need
// coordination.
func (r *Ring) Remove(nodes ...string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	changed := false
	for _, n := range nodes {
		if _, exists := r.members[n]; !exists {
			continue
		}
		delete(r.members, n)
		changed = true
	}
	if changed {
		r.rebuild()
	}
}

// Set replaces ring membership wholesale.
//
// This is the form the reaper actually uses: the Redis node registry yields a
// full snapshot of live gateways, and diffing it against current membership by
// hand would be a chance to get failover wrong. Replacing outright cannot drift.
func (r *Ring) Set(nodes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	next := make(map[string]struct{}, len(nodes))
	for _, n := range nodes {
		if n == "" {
			continue
		}
		next[n] = struct{}{}
	}

	if sameSet(r.members, next) {
		return
	}
	r.members = next
	r.rebuild()
}

// Lookup returns the node owning key. The second result is false only when the
// ring is empty — a real condition during startup and total outage, and one the
// caller must handle rather than treat as "owned by me".
func (r *Ring) Lookup(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.positions) == 0 {
		return "", false
	}

	h := hashKey(key)
	i := sort.Search(len(r.positions), func(i int) bool {
		return r.positions[i] >= h
	})
	// Past the last position means the key falls in the arc between the highest
	// point and the lowest, so it wraps to the first.
	if i == len(r.positions) {
		i = 0
	}
	return r.owner[r.positions[i]], true
}

// Owns reports whether nodeID owns key. It is the question the reaper actually
// asks — "is this shard mine?" — and returns false on an empty ring, so a
// gateway that has lost the registry declines to reap rather than assuming
// ownership of everything.
func (r *Ring) Owns(key, nodeID string) bool {
	owner, ok := r.Lookup(key)
	return ok && owner == nodeID
}

// Members returns the physical nodes on the ring, sorted, as a copy.
func (r *Ring) Members() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]string, 0, len(r.members))
	for n := range r.members {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// Len is the number of physical nodes, not virtual positions.
func (r *Ring) Len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.members)
}

// Replicas is the number of positions each physical node occupies. Exposed for
// /debug/ring so the endpoint can report how the ring was built, not just who
// is on it.
func (r *Ring) Replicas() int {
	return r.replicas
}

// rebuild recomputes every position from current membership. The caller must
// hold the write lock.
//
// Rebuilding wholesale rather than splicing entries in and out is what makes
// the ring order-independent: the result depends only on the member set, never
// on the sequence of Add and Remove calls that produced it. Two gateways that
// learn about the same membership in different orders therefore agree on every
// assignment, which is the whole point of using a ring for ownership.
func (r *Ring) rebuild() {
	ids := make([]string, 0, len(r.members))
	for n := range r.members {
		ids = append(ids, n)
	}
	sort.Strings(ids)

	r.positions = make([]uint64, 0, len(ids)*r.replicas)
	r.owner = make(map[uint64]string, len(ids)*r.replicas)

	for _, id := range ids {
		for i := 0; i < r.replicas; i++ {
			h := hashKey(virtualNodeKey(id, i))
			if _, taken := r.owner[h]; taken {
				// A 64-bit collision between two virtual nodes is vanishingly
				// unlikely, but "unlikely" is not "impossible" and the outcome
				// must not depend on map iteration order. ids is sorted, so the
				// incumbent is always the lexicographically smaller node; it
				// keeps the position and the newcomer drops this one point.
				continue
			}
			r.owner[h] = id
			r.positions = append(r.positions, h)
		}
	}

	sort.Slice(r.positions, func(i, j int) bool {
		return r.positions[i] < r.positions[j]
	})
}

// virtualNodeKey names the i-th virtual node of a physical node. The separator
// keeps "gw-1" replica 11 distinct from "gw-11" replica 1.
func virtualNodeKey(nodeID string, i int) string {
	return nodeID + "#" + strconv.Itoa(i)
}

// hashKey maps a string onto the ring.
//
// SHA-256 truncated to 64 bits, rather than a cheaper non-cryptographic hash,
// for two reasons. Virtual node keys are near-identical by construction
// ("gw-1#0", "gw-1#1", ...), and a hash with weak avalanche clusters them,
// which is exactly the uneven distribution virtual nodes exist to prevent.
// Second, the digest is fixed by the standard and stable across Go versions and
// architectures, so two gateways on different builds cannot disagree about
// ownership. Cost is a few hundred nanoseconds on a path that runs per reap
// scan, not per message.
func hashKey(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}

func sameSet(a, b map[string]struct{}) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

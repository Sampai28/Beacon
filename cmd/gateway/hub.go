package main

import (
	"sync"

	"github.com/Sampai28/Beacon/internal/metrics"
)

// hub is this gateway's in-memory connection table.
//
// It is keyed by userId, not by connection, because the duplicate-session policy
// guarantees at most one live session per user cluster-wide — so a second
// connection for the same user on this node displaces the first exactly as it
// would across nodes.
//
// This table is an optimisation, never a source of truth. Redis is
// authoritative, and any divergence between the two is reported as drift rather
// than reconciled silently.
type hub struct {
	mu    sync.RWMutex
	conns map[string]*connection
	m     *metrics.Metrics
}

func newHub(m *metrics.Metrics) *hub {
	return &hub{conns: make(map[string]*connection), m: m}
}

// register adds a connection, returning any it displaced on this same gateway.
//
// The displaced connection is returned rather than closed here so the caller can
// close it outside the lock: closing writes a frame, and holding the hub lock
// across a socket write would let one unresponsive client stall every other
// registration on this node.
func (h *hub) register(c *connection) *connection {
	h.mu.Lock()
	previous := h.conns[c.userID]
	h.conns[c.userID] = c
	active := len(h.conns)
	h.mu.Unlock()

	h.m.ConnectionsActive.Set(float64(active))
	h.m.ConnectionsTotal.Inc()
	return previous
}

// unregister removes a connection only if it is still the registered one. A
// slow teardown must not evict the connection that replaced it.
func (h *hub) unregister(userID, sessionID string) bool {
	h.mu.Lock()
	current, ok := h.conns[userID]
	removed := ok && current.sessionID == sessionID
	if removed {
		delete(h.conns, userID)
	}
	active := len(h.conns)
	h.mu.Unlock()

	h.m.ConnectionsActive.Set(float64(active))
	return removed
}

// get returns the connection serving a user on this gateway, if any.
func (h *hub) get(userID string) *connection {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.conns[userID]
}

// lookupForEviction returns the connection matching both user and session. The
// session must match: an eviction notice for a session this gateway has already
// replaced is stale and must be ignored, or a fast reconnect would kill itself.
func (h *hub) lookupForEviction(userID, sessionID string) *connection {
	h.mu.RLock()
	defer h.mu.RUnlock()

	c, ok := h.conns[userID]
	if !ok || c.sessionID != sessionID {
		return nil
	}
	return c
}

// count is the live connection count this gateway reports to the registry, and
// therefore one half of the cluster-wide drift comparison.
func (h *hub) count() int64 {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return int64(len(h.conns))
}

// snapshot returns every live connection, for shutdown.
func (h *hub) snapshot() []*connection {
	h.mu.RLock()
	defer h.mu.RUnlock()

	out := make([]*connection, 0, len(h.conns))
	for _, c := range h.conns {
		out = append(out, c)
	}
	return out
}

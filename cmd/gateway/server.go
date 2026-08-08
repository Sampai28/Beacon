package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/presence"
)

// server owns the HTTP surface exposed by every gateway replica.
type server struct {
	cfg      config
	log      *slog.Logger
	m        *metrics.Metrics
	registry *prometheus.Registry
	presence *presence.Service
	hub      *hub

	mux     *http.ServeMux
	started time.Time
	upgrade websocket.Upgrader

	// ready is separate from liveness on purpose. A gateway that has lost Redis
	// is still alive — killing it would drop healthy WebSocket connections for
	// nothing — but it must not be handed new work, so it reports unready and
	// keeps serving what it already has.
	ready atomic.Bool
}

func newServer(
	cfg config,
	log *slog.Logger,
	m *metrics.Metrics,
	registry *prometheus.Registry,
	svc *presence.Service,
	h *hub,
) *server {
	s := &server{
		cfg:      cfg,
		log:      log,
		m:        m,
		registry: registry,
		presence: svc,
		hub:      h,
		mux:      http.NewServeMux(),
		started:  time.Now(),
		upgrade: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			// Any origin is accepted because this is a localhost demo with a
			// dev-mode shared secret and no cookies or ambient authority — there
			// is no session for a cross-origin page to ride on. A deployment
			// with real auth would need an allowlist here.
			CheckOrigin: func(*http.Request) bool { return true },
		},
	}
	s.routes()
	return s
}

func (s *server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
	s.mux.HandleFunc("GET /debug/ring", s.handleDebugRing)
	s.mux.HandleFunc("GET /ws", s.handleWebSocket)

	s.mux.Handle("GET /metrics", promhttp.HandlerFor(s.registry, promhttp.HandlerOpts{
		// One bad collector should surface as an error rather than silently
		// truncating the scrape and making a panel look merely quiet.
		ErrorHandling: promhttp.ContinueOnError,
	}))

	s.mux.Handle("GET /", s.staticHandler())
}

// staticHandler serves the demo client from disk.
//
// Read from a directory rather than embedded in the binary so the client can be
// edited and reloaded without a rebuild — the point of it is to let a human
// watch cross-node fan-out in two browser tabs, and a compile cycle between
// tweaks defeats that.
func (s *server) staticHandler() http.Handler {
	if _, err := os.Stat(s.cfg.WebDir); err != nil {
		s.log.Warn("web directory not found; demo client will not be served",
			"dir", s.cfg.WebDir, "err", err)
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "demo client not available", http.StatusNotFound)
		})
	}
	return http.FileServer(http.Dir(s.cfg.WebDir))
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *server) markReady()   { s.ready.Store(true) }
func (s *server) markUnready() { s.ready.Store(false) }

// handleHealthz reports process liveness only. It deliberately does not consult
// Redis: a dependency outage is a readiness concern, and conflating the two
// makes an orchestrator restart-loop a node that is merely degraded.
func (s *server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"status":        "ok",
		"gatewayId":     s.cfg.GatewayID,
		"uptimeSeconds": int64(time.Since(s.started).Seconds()),
		"connections":   s.hub.count(),
	})
}

// handleReadyz reports whether this replica should receive new connections.
func (s *server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"status":    "unready",
			"gatewayId": s.cfg.GatewayID,
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"status":    "ready",
		"gatewayId": s.cfg.GatewayID,
	})
}

// handleDebugRing exposes ring membership and this node's share of shard
// ownership. During a failover this is the fastest way to see whether the ring
// has actually noticed a dead node, as opposed to inferring it from metrics.
func (s *server) handleDebugRing(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	snap, err := s.presence.RingState(ctx)
	if err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{
			"error":     "could not read ring state",
			"gatewayId": s.cfg.GatewayID,
			"members":   snap.Members,
		})
		return
	}
	writeJSON(w, http.StatusOK, snap)
}

func (s *server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if !s.ready.Load() {
		http.Error(w, "gateway not ready", http.StatusServiceUnavailable)
		return
	}

	ws, err := s.upgrade.Upgrade(w, r, nil)
	if err != nil {
		// Upgrade has already written a response by this point.
		s.log.Debug("websocket upgrade failed", "err", err)
		return
	}

	c := newConnection(ws, s)
	go c.writePump()
	go c.readPump(context.Background())
}

// evictLocal handles an eviction notice addressed to this gateway.
//
// The session ID must still match: a notice for a session this node has already
// replaced is stale, and acting on it would kill the connection that replaced it.
func (s *server) evictLocal(userID, sessionID string) {
	c := s.hub.lookupForEviction(userID, sessionID)
	if c == nil {
		return
	}
	s.log.Info("evicting session displaced by a newer connection",
		"user_id", userID, "session_id", sessionID)
	c.evict()
}

// closeAllConnections drops every live connection during shutdown.
func (s *server) closeAllConnections() {
	for _, c := range s.hub.snapshot() {
		c.close(reasonShutdown)
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already written, so there is nothing useful left to
		// say to the client; the connection will simply look truncated.
		return
	}
}

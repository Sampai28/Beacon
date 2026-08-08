package main

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// server owns the HTTP surface exposed by every gateway replica.
//
// Liveness and readiness exist from the scaffold onward because Compose health
// checks and the node registry both depend on them. The WebSocket endpoint,
// /metrics and /debug/ring are registered in later steps.
type server struct {
	cfg     config
	log     *slog.Logger
	mux     *http.ServeMux
	started time.Time

	// ready is separate from liveness on purpose. A gateway that has lost Redis
	// is still alive — killing it would drop healthy WebSocket connections for
	// nothing — but it must not be handed new work, so it reports unready and
	// keeps serving what it already has.
	ready atomic.Bool
}

func newServer(cfg config, log *slog.Logger) *server {
	s := &server{
		cfg:     cfg,
		log:     log,
		mux:     http.NewServeMux(),
		started: time.Now(),
	}
	s.routes()
	return s
}

func (s *server) routes() {
	s.mux.HandleFunc("GET /healthz", s.handleHealthz)
	s.mux.HandleFunc("GET /readyz", s.handleReadyz)
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

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		// The status line is already written, so there is nothing useful left to
		// say to the client; the connection will simply look truncated.
		return
	}
}

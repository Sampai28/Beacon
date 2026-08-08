package main

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func testServer(t *testing.T) *server {
	t.Helper()
	return newServer(
		config{GatewayID: "gw-test", HTTPAddr: ":0", RedisAddr: "localhost:6379"},
		slog.New(slog.NewTextHandler(io.Discard, nil)),
	)
}

func TestHealthzIsAliveBeforeReady(t *testing.T) {
	s := testServer(t)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))

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
// not yet joined to the ring has to fail readiness, or Compose will route
// connections to a node that cannot serve them.
func TestReadyzGatesOnReadiness(t *testing.T) {
	s := testServer(t)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz before markReady: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}

	s.markReady()

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("readyz after markReady: got %d, want %d", rec.Code, http.StatusOK)
	}

	s.markUnready()

	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("readyz after markUnready: got %d, want %d", rec.Code, http.StatusServiceUnavailable)
	}
}

func TestUnknownRouteIs404(t *testing.T) {
	s := testServer(t)

	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/nope", nil))

	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown route: got %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	// No BEACON_* variables set in the test environment, so every field should
	// fall back rather than end up empty — an empty listen address or gateway ID
	// would fail confusingly at runtime.
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
}

func TestEnvDurationRejectsGarbage(t *testing.T) {
	t.Setenv("BEACON_SHUTDOWN_GRACE", "not-a-duration")

	// A malformed duration must not collapse to zero: a zero grace period would
	// turn every rolling restart into an abrupt connection drop.
	if got := envDuration("BEACON_SHUTDOWN_GRACE", 10); got != 10 {
		t.Errorf("envDuration with garbage: got %v, want fallback 10", got)
	}
}

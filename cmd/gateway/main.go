// Command gateway is a single Beacon gateway replica.
//
// A gateway terminates client WebSocket connections, mirrors session state into
// Redis so that any replica can answer for any user, and participates in a
// consistent hash ring that assigns stale-session reaper ownership to exactly
// one live node. Replicas are interchangeable: a client may connect to any of
// them and observe the same presence view.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// config is the full set of knobs a gateway reads at startup. Everything is
// environment-driven so a Compose replica differs from its siblings only by
// BEACON_GATEWAY_ID.
type config struct {
	// GatewayID uniquely identifies this replica. It is the key this node
	// occupies on the consistent hash ring and the value written into every
	// session hash this node owns, so it must be stable and unique.
	GatewayID string

	// HTTPAddr is the listen address serving the WebSocket upgrade endpoint,
	// the probes, /metrics and /debug/ring.
	HTTPAddr string

	// RedisAddr points at the shared session store, pub/sub bus and node
	// registry.
	RedisAddr string

	// DevToken is the dev-mode shared secret clients present in HELLO. This is
	// deliberately not real authentication; it exists so the protocol has a
	// rejection path to exercise. It is never logged.
	DevToken string

	// ShutdownGrace bounds how long in-flight work may take to drain before
	// the process exits regardless.
	ShutdownGrace time.Duration
}

func loadConfig() config {
	return config{
		GatewayID:     env("BEACON_GATEWAY_ID", defaultGatewayID()),
		HTTPAddr:      env("BEACON_HTTP_ADDR", ":8080"),
		RedisAddr:     env("BEACON_REDIS_ADDR", "localhost:6379"),
		DevToken:      env("BEACON_DEV_TOKEN", "beacon-dev-token"),
		ShutdownGrace: envDuration("BEACON_SHUTDOWN_GRACE", 10*time.Second),
	}
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}

// defaultGatewayID falls back to the container hostname, which Compose already
// makes unique per replica. If even that is unavailable we would rather fail
// loudly at ring-join time than silently collide, so the placeholder is
// obviously wrong rather than plausibly right.
func defaultGatewayID() string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "gateway-unidentified"
	}
	return h
}

func main() {
	cfg := loadConfig()

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})).With("gateway_id", cfg.GatewayID)

	if err := run(cfg, log); err != nil {
		log.Error("gateway exited with error", "err", err)
		os.Exit(1)
	}
	log.Info("gateway stopped cleanly")
}

func run(cfg config, log *slog.Logger) error {
	srv := newServer(cfg, log)

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: the WebSocket endpoint added in a later step hijacks
		// the connection and would be killed mid-stream by one.
		IdleTimeout: 120 * time.Second,
	}

	// Trap termination first, so a signal arriving during startup is still
	// honoured rather than racing the listener.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("http listener starting", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	// Redis connection, ring join and reaper start land here in later steps.
	// Readiness flips only once those succeed; for now the scaffold is ready as
	// soon as it is listening.
	srv.markReady()
	log.Info("gateway ready", "redis_addr", cfg.RedisAddr)

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// Fail readiness before draining so load balancers and the node registry
	// stop steering new work here while existing connections finish.
	srv.markUnready()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return err
	}
	return nil
}

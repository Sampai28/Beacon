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
	"strconv"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/presence"
)

// config is the full set of knobs a gateway reads at startup. Everything is
// environment-driven so a Compose replica differs from its siblings only by
// BEACON_GATEWAY_ID.
type config struct {
	// GatewayID uniquely identifies this replica. It is the key this node
	// occupies on the consistent hash ring and the value written into every
	// session hash this node owns, so it must be stable and unique.
	GatewayID string

	// HTTPAddr is the listen address serving the WebSocket endpoint, the probes,
	// /metrics, /debug/ring and the demo client.
	HTTPAddr string

	// RedisAddr points at the shared session store, pub/sub bus and node
	// registry.
	RedisAddr string

	// DevToken is the dev-mode shared secret clients present in HELLO. This is
	// deliberately not real authentication; it exists so the protocol has a
	// rejection path to exercise. It is never logged.
	DevToken string

	// WebDir holds the static demo client.
	WebDir string

	// ShutdownGrace bounds how long in-flight work may take to drain before the
	// process exits regardless.
	ShutdownGrace time.Duration

	// Presence carries the timing knobs that govern presence correctness.
	Presence presence.Config
}

func loadConfig() config {
	gatewayID := env("BEACON_GATEWAY_ID", defaultGatewayID())

	p := presence.DefaultConfig(gatewayID)
	p.SessionTTL = envDuration("BEACON_SESSION_TTL", p.SessionTTL)
	p.NodeTTL = envDuration("BEACON_NODE_TTL", p.NodeTTL)
	p.RegistryInterval = envDuration("BEACON_REGISTRY_INTERVAL", p.RegistryInterval)
	p.ReaperInterval = envDuration("BEACON_REAPER_INTERVAL", p.ReaperInterval)
	p.DriftInterval = envDuration("BEACON_DRIFT_INTERVAL", p.DriftInterval)
	p.RingReplicas = envInt("BEACON_RING_REPLICAS", p.RingReplicas)

	return config{
		GatewayID:     gatewayID,
		HTTPAddr:      env("BEACON_HTTP_ADDR", ":8080"),
		RedisAddr:     env("BEACON_REDIS_ADDR", "localhost:6379"),
		DevToken:      env("BEACON_DEV_TOKEN", "beacon-dev-token"),
		WebDir:        env("BEACON_WEB_DIR", "./web"),
		ShutdownGrace: envDuration("BEACON_SHUTDOWN_GRACE", 10*time.Second),
		Presence:      p,
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
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return fallback
	}
	return n
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
	// Trap termination first, so a signal arriving during startup is honoured
	// rather than racing the listener.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	registry := prometheus.NewRegistry()
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	m := metrics.New(registry, cfg.GatewayID)

	rdb := redis.NewClient(&redis.Options{
		Addr: cfg.RedisAddr,
		// A gateway holding thousands of sockets issues a Redis command per
		// heartbeat and per presence change; the default pool of 10 per CPU
		// becomes the bottleneck well before Redis does.
		PoolSize:     64,
		MinIdleConns: 8,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	defer func() { _ = rdb.Close() }()

	pingCtx, cancelPing := context.WithTimeout(ctx, 10*time.Second)
	err := rdb.Ping(pingCtx).Err()
	cancelPing()
	if err != nil {
		return err
	}
	log.Info("connected to redis", "addr", cfg.RedisAddr)

	svc := presence.NewService(ctx, rdb, cfg.Presence, m, log)
	h := newHub(m)
	svc.SetConnectionCounter(h.count)

	srv := newServer(cfg, log, m, registry, svc, h)

	// Eviction notices arrive on this gateway's control channel. Wired before
	// the service starts so a notice for a session claimed during startup is not
	// missed.
	svc.Bus.OnControl(func(cm presence.ControlMessage) {
		if cm.Type == presence.ControlEvict {
			srv.evictLocal(cm.UserID, cm.SessionID)
		}
	})

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           srv,
		ReadHeaderTimeout: 5 * time.Second,
		// No WriteTimeout: the WebSocket endpoint hijacks the connection and
		// would be killed mid-stream by one.
		IdleTimeout: 120 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		log.Info("http listener starting", "addr", cfg.HTTPAddr)
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	// Join the ring before reporting ready. Accepting connections while
	// invisible to the registry would mean nothing reaps this node's sessions if
	// it died in that window.
	if err := svc.Start(ctx); err != nil {
		return err
	}

	serviceDone := make(chan struct{})
	go func() {
		defer close(serviceDone)
		svc.Run(ctx)
	}()

	srv.markReady()
	log.Info("gateway ready",
		"redis_addr", cfg.RedisAddr,
		"session_ttl", cfg.Presence.SessionTTL,
		"node_ttl", cfg.Presence.NodeTTL,
		"reaper_interval", cfg.Presence.ReaperInterval)

	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
		return nil
	case <-ctx.Done():
		log.Info("shutdown signal received, draining")
	}

	// Fail readiness first so nothing new is steered here, then leave the ring
	// so surviving nodes pick up this node's shards immediately rather than
	// waiting out a TTL.
	srv.markUnready()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
	defer cancel()

	if err := svc.Shutdown(shutdownCtx); err != nil {
		log.Warn("presence shutdown incomplete", "err", err)
	}
	srv.closeAllConnections()

	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return err
	}

	select {
	case <-serviceDone:
	case <-shutdownCtx.Done():
		log.Warn("background loops did not stop within the grace period")
	}
	return nil
}

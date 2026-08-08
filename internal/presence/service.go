package presence

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/protocol"
	"github.com/Sampai28/Beacon/internal/ring"
)

// Config holds the timing knobs that govern presence correctness.
//
// The relationships between these matter more than the individual values.
// SessionTTL must exceed the client heartbeat interval by enough margin to
// survive a slow round trip, or healthy clients get reaped. NodeTTL must exceed
// the registry heartbeat by a similar margin, or a healthy gateway falls off the
// ring and its shards churn for no reason.
type Config struct {
	GatewayID string

	// SessionTTL is how long a session survives without a client heartbeat.
	SessionTTL time.Duration

	// NodeTTL is how long a gateway stays on the ring without heartbeating.
	// This is the dominant term in failover time: a killed gateway's shards stay
	// unowned until its key expires.
	NodeTTL time.Duration

	// RegistryInterval is how often this gateway announces itself.
	RegistryInterval time.Duration

	// ReaperInterval is how often the owned shard is swept.
	ReaperInterval time.Duration

	// DriftInterval is how often in-memory and Redis counts are compared.
	DriftInterval time.Duration

	// RingReplicas is virtual nodes per gateway on the ring.
	RingReplicas int
}

// DefaultConfig returns timings tuned for a local Compose stack.
//
// NodeTTL is 3x RegistryInterval: two missed heartbeats are tolerated before a
// node is declared dead, which absorbs a GC pause or a slow Redis round trip
// without triggering a needless rebalance. SessionTTL is deliberately longer
// than NodeTTL — a session should outlive a brief gateway hiccup, and orphan
// detection rather than expiry is the right mechanism for cleaning up after a
// gateway that is genuinely gone.
func DefaultConfig(gatewayID string) Config {
	return Config{
		GatewayID:        gatewayID,
		SessionTTL:       30 * time.Second,
		NodeTTL:          6 * time.Second,
		RegistryInterval: 2 * time.Second,
		ReaperInterval:   2 * time.Second,
		DriftInterval:    5 * time.Second,
		RingReplicas:     ring.DefaultVirtualNodes,
	}
}

// Service is the presence layer as the gateway sees it.
type Service struct {
	cfg Config
	log *slog.Logger
	m   *metrics.Metrics

	Store      *Store
	Registry   *Registry
	Bus        *Bus
	Ring       *ring.Ring
	Reaper     *Reaper
	Reconciler *Reconciler

	mu          sync.RWMutex
	connections func() int64
}

// NewService wires the presence layer. The caller must call SetConnectionCounter
// before Run, since the registry heartbeat publishes the count that the drift
// reconciler depends on.
func NewService(
	ctx context.Context,
	rdb redis.UniversalClient,
	cfg Config,
	m *metrics.Metrics,
	log *slog.Logger,
) *Service {
	store := NewStore(rdb, cfg.SessionTTL)
	registry := NewRegistry(rdb, cfg.NodeTTL)
	bus := NewBus(ctx, rdb, cfg.GatewayID, m, log)
	r := ring.New(cfg.RingReplicas)

	return &Service{
		cfg:         cfg,
		log:         log,
		m:           m,
		Store:       store,
		Registry:    registry,
		Bus:         bus,
		Ring:        r,
		Reaper:      NewReaper(store, registry, bus, r, cfg.GatewayID, m, log),
		Reconciler:  NewReconciler(store, registry, m, log),
		connections: func() int64 { return 0 },
	}
}

// SetConnectionCounter supplies the gateway's live connection count.
func (s *Service) SetConnectionCounter(f func() int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections = f
}

func (s *Service) connectionCount() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.connections()
}

// Config exposes the timings, which the debug endpoint reports so an operator
// can see what the running process actually believes rather than what the
// compose file says.
func (s *Service) Config() Config { return s.cfg }

// Start joins the ring immediately and launches the background loops.
//
// The first heartbeat and ring refresh happen synchronously so the gateway is
// on the ring before it reports ready. Starting to accept connections while
// invisible to the registry would mean nothing reaps this node's sessions if it
// died in that window.
func (s *Service) Start(ctx context.Context) error {
	if err := s.Registry.Heartbeat(ctx, s.cfg.GatewayID, s.connectionCount()); err != nil {
		return err
	}
	s.m.RegistryHeartbeats.Inc()

	ids, err := s.Registry.LiveNodeIDs(ctx)
	if err != nil {
		return err
	}
	s.Ring.Set(ids)
	s.m.RingMembers.Set(float64(s.Ring.Len()))
	s.log.Info("joined ring", "members", s.Ring.Members())
	return nil
}

// Run blocks until ctx is cancelled, driving every background loop.
func (s *Service) Run(ctx context.Context) {
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := s.Bus.Run(ctx); err != nil && ctx.Err() == nil {
			s.log.Error("pub/sub pump stopped", "err", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.runRegistryHeartbeat(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Reaper.Run(ctx, s.cfg.ReaperInterval)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s.Reconciler.Run(ctx, s.cfg.DriftInterval)
	}()

	wg.Wait()
}

func (s *Service) runRegistryHeartbeat(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.RegistryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.Registry.Heartbeat(ctx, s.cfg.GatewayID, s.connectionCount()); err != nil {
				if ctx.Err() != nil {
					return
				}
				// A sustained run of these means this node is about to fall off
				// the ring and have its shards reassigned while it is still
				// serving traffic.
				s.m.RegistryFailures.Inc()
				s.log.Warn("registry heartbeat failed", "err", err)
				continue
			}
			s.m.RegistryHeartbeats.Inc()
		}
	}
}

// Shutdown deregisters this gateway and closes the bus.
//
// Deregistering rather than waiting for the TTL means a planned restart
// rebalances the ring immediately, instead of leaving this node's shards
// unowned for a full NodeTTL while everyone waits for a key to expire.
func (s *Service) Shutdown(ctx context.Context) error {
	var firstErr error
	if err := s.Registry.Deregister(ctx, s.cfg.GatewayID); err != nil {
		firstErr = err
		s.log.Warn("deregister failed", "err", err)
	}
	if err := s.Bus.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// ---------------------------------------------------------------------------
// Operations the gateway performs on behalf of a client
// ---------------------------------------------------------------------------

// Connect claims a session for a user and announces them online.
//
// Duplicate-session policy — integrity check 2 — is "last writer wins": the new
// connection takes the session and the previous one is evicted. The alternative,
// rejecting the newcomer, strands a user whose old session is a half-dead socket
// the owning gateway has not yet noticed, which is the common case rather than
// the rare one. The displaced session's OFFLINE transition is emitted by the
// gateway that held it, on receipt of the eviction control message.
func (s *Service) Connect(ctx context.Context, userID, sessionID string) (*Evicted, error) {
	now := NowMillis()
	sess := Session{
		UserID:    userID,
		SessionID: sessionID,
		Status:    protocol.StatusOnline,
		GatewayID: s.cfg.GatewayID,
		LastSeen:  now,
	}

	evicted, err := s.Store.Claim(ctx, sess)
	if err != nil {
		s.m.RedisErrors.WithLabelValues("claim").Inc()
		return nil, err
	}

	if evicted != nil {
		s.m.DuplicateSessionsEvicted.Inc()
		s.log.Info("evicted duplicate session",
			"user_id", userID,
			"evicted_session", evicted.SessionID,
			"evicted_gateway", evicted.GatewayID)

		// Tell the holder to drop the old connection. If that gateway is already
		// gone the message goes nowhere, which is fine: the session hash now
		// names this gateway, so orphan detection has nothing left to find.
		if evicted.GatewayID != "" {
			err := s.Bus.SendControl(ctx, evicted.GatewayID, ControlMessage{
				Type:      ControlEvict,
				UserID:    userID,
				SessionID: evicted.SessionID,
			})
			if err != nil {
				s.log.Warn("could not deliver eviction notice",
					"user_id", userID, "gateway", evicted.GatewayID, "err", err)
			}
		}
	}

	s.m.PresenceEvents.WithLabelValues(string(protocol.StatusOnline)).Inc()
	if err := s.Bus.Publish(ctx, sess.Presence()); err != nil {
		s.log.Warn("could not publish ONLINE", "user_id", userID, "err", err)
	}
	return evicted, nil
}

// SetPresence applies a presence change and fans it out.
//
// The timestamp is taken here rather than accepted from the client: a client
// supplying its own is a client that can rewrite history, and out-of-order
// rejection would then be enforcing an ordering the client controls.
func (s *Service) SetPresence(
	ctx context.Context,
	userID, sessionID string,
	status protocol.Status,
	placeID, serverID string,
) (UpdateResult, error) {
	sess := Session{
		UserID:    userID,
		SessionID: sessionID,
		Status:    status,
		PlaceID:   placeID,
		ServerID:  serverID,
		GatewayID: s.cfg.GatewayID,
		LastSeen:  NowMillis(),
	}

	res, err := s.Store.Update(ctx, sess)
	if err != nil {
		s.m.RedisErrors.WithLabelValues("update").Inc()
		return res, err
	}

	switch res {
	case UpdateApplied:
		s.m.PresenceEvents.WithLabelValues(string(status)).Inc()
		if err := s.Bus.Publish(ctx, sess.Presence()); err != nil {
			s.log.Warn("could not publish presence", "user_id", userID, "err", err)
		}
	case UpdateStale:
		s.m.PresenceEventsOutOfOrder.Inc()
	}
	return res, nil
}

// ApplyRemotePresence applies an event that arrived over pub/sub from another
// gateway, enforcing the same ordering rule as a local change.
//
// Without this the out-of-order check would only guard the local path, and a
// delayed event crossing nodes could still overwrite fresher state.
func (s *Service) ApplyRemotePresence(ctx context.Context, p protocol.Presence) (UpdateResult, error) {
	current, err := s.Store.Get(ctx, p.UserID)
	if err != nil {
		if errors.Is(err, ErrNoSession) {
			return UpdateNoSession, nil
		}
		return UpdateNoSession, err
	}
	if p.TS < current.LastSeen {
		s.m.PresenceEventsOutOfOrder.Inc()
		return UpdateStale, nil
	}
	return UpdateApplied, nil
}

// Heartbeat refreshes a session's TTL.
func (s *Service) Heartbeat(ctx context.Context, userID, sessionID string) (UpdateResult, error) {
	res, err := s.Store.Heartbeat(ctx, userID, sessionID, NowMillis())
	if err != nil {
		s.m.RedisErrors.WithLabelValues("heartbeat").Inc()
	}
	return res, err
}

// JoinOutcome is the resolved answer to a JOIN.
type JoinOutcome struct {
	OK       bool
	PlaceID  string
	ServerID string
	Reason   string
}

// Join resolves where a target user is.
//
// The target is almost never connected to the gateway answering the request,
// which is the entire point: session state lives in Redis, so this is a lookup
// rather than a cross-node request. No gateway needs to know which other gateway
// holds the user.
func (s *Service) Join(ctx context.Context, targetUserID string) (JoinOutcome, error) {
	start := time.Now()

	sess, err := s.Store.Get(ctx, targetUserID)
	if err != nil {
		if errors.Is(err, ErrNoSession) {
			s.m.ObserveJoin("denied", time.Since(start).Seconds())
			return JoinOutcome{Reason: protocol.ReasonTargetUnknown}, nil
		}
		s.m.RedisErrors.WithLabelValues("join_lookup").Inc()
		s.m.ObserveJoin("error", time.Since(start).Seconds())
		return JoinOutcome{}, err
	}

	switch {
	case sess.Status == protocol.StatusOffline:
		s.m.ObserveJoin("denied", time.Since(start).Seconds())
		return JoinOutcome{Reason: protocol.ReasonTargetOffline}, nil
	case !sess.Joinable():
		s.m.ObserveJoin("denied", time.Since(start).Seconds())
		return JoinOutcome{Reason: protocol.ReasonTargetNotJoinable}, nil
	}

	s.m.ObserveJoin("ok", time.Since(start).Seconds())
	return JoinOutcome{OK: true, PlaceID: sess.PlaceID, ServerID: sess.ServerID}, nil
}

// Snapshot returns current presence for a set of users, used to answer
// SUBSCRIBE. Sending a snapshot at subscribe time is what makes at-most-once
// pub/sub acceptable: a client that missed events while disconnected is brought
// current on reconnect rather than waiting for the next change.
func (s *Service) Snapshot(ctx context.Context, userIDs []string) (map[string]*Session, error) {
	sessions, err := s.Store.GetMany(ctx, userIDs)
	if err != nil {
		s.m.RedisErrors.WithLabelValues("snapshot").Inc()
		return nil, err
	}
	return sessions, nil
}

// Disconnect removes a session and announces the user offline.
//
// The delete is conditional on sessionID so a slow disconnect cannot remove a
// session the user has already re-established on another gateway — the exact
// race that would otherwise turn a fast reconnect into a spurious OFFLINE.
func (s *Service) Disconnect(ctx context.Context, userID, sessionID string) error {
	removed, err := s.Store.Delete(ctx, userID, sessionID)
	if err != nil {
		s.m.RedisErrors.WithLabelValues("delete").Inc()
		return err
	}
	if !removed {
		return nil
	}

	s.m.PresenceEvents.WithLabelValues(string(protocol.StatusOffline)).Inc()
	err = s.Bus.Publish(ctx, protocol.Presence{
		UserID: userID,
		Status: protocol.StatusOffline,
		TS:     NowMillis(),
	})
	if err != nil {
		s.log.Warn("could not publish OFFLINE", "user_id", userID, "err", err)
	}
	return nil
}

// RingSnapshot is the /debug/ring payload.
type RingSnapshot struct {
	GatewayID    string   `json:"gatewayId"`
	Members      []string `json:"members"`
	MemberCount  int      `json:"memberCount"`
	VirtualNodes int      `json:"virtualNodes"`
	OwnedShards  int      `json:"ownedShards"`
	TotalShards  int      `json:"totalShards"`
	NodeTTL      string   `json:"nodeTtl"`
	SessionTTL   string   `json:"sessionTtl"`
}

// RingState reports ring membership and this node's share of ownership.
//
// Ownership is computed against the live session index rather than a synthetic
// keyspace, so the numbers describe the work this gateway is actually
// responsible for right now — which is what makes the endpoint useful during a
// failover rather than merely descriptive.
func (s *Service) RingState(ctx context.Context) (RingSnapshot, error) {
	snap := RingSnapshot{
		GatewayID:    s.cfg.GatewayID,
		Members:      s.Ring.Members(),
		VirtualNodes: s.Ring.Replicas(),
		NodeTTL:      s.cfg.NodeTTL.String(),
		SessionTTL:   s.cfg.SessionTTL.String(),
	}
	snap.MemberCount = len(snap.Members)

	users, err := s.Store.IndexedUsers(ctx)
	if err != nil {
		return snap, err
	}
	snap.TotalShards = len(users)
	for _, u := range users {
		if s.Ring.Owns(u, s.cfg.GatewayID) {
			snap.OwnedShards++
		}
	}
	return snap, nil
}

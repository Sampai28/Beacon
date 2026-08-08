package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/presence"
	"github.com/Sampai28/Beacon/internal/protocol"
)

const (
	// sendBuffer is how many frames may queue for one client before it is
	// considered unable to keep up.
	//
	// A slow client must be disconnected rather than allowed to apply
	// backpressure: presence fan-out runs on the pub/sub pump shared by every
	// connection on this gateway, so one blocked write would stall delivery for
	// everyone. Dropping the straggler is the only option that keeps the rest
	// correct.
	sendBuffer = 64

	// writeWait bounds a single socket write.
	writeWait = 10 * time.Second

	// pongWait is how long a client may go silent before the connection is
	// considered dead. It must exceed pingPeriod with room for a slow round trip.
	pongWait = 60 * time.Second

	// pingPeriod is how often the server pings. Kept below pongWait so a client
	// gets at least two chances to answer before being cut.
	pingPeriod = (pongWait * 9) / 10

	// helloWait bounds how long a connection may sit unauthenticated. Without
	// it, opening sockets and never speaking is a free way to exhaust the
	// gateway's file descriptors.
	helloWait = 10 * time.Second
)

// closeReason values match the metric label set in internal/metrics.
const (
	reasonClientClose = "client_close"
	reasonReadError   = "read_error"
	reasonWriteError  = "write_error"
	reasonEvictedDup  = "evicted_duplicate"
	reasonShutdown    = "shutdown"
	reasonReaped      = "reaped"
)

// connection is one client WebSocket and the state the gateway keeps for it.
type connection struct {
	ws  *websocket.Conn
	srv *server
	log *slog.Logger
	m   *metrics.Metrics

	userID    string
	sessionID string

	send chan []byte

	// closeOnce guards teardown. A connection can be closed from the read pump,
	// the write pump, an eviction notice and shutdown concurrently; without this
	// they race to double-close the socket and the send channel.
	closeOnce sync.Once
	closed    chan struct{}

	subMu sync.Mutex
	subs  map[string]func() // watched userId -> release
}

func newConnection(ws *websocket.Conn, srv *server) *connection {
	return &connection{
		ws:     ws,
		srv:    srv,
		log:    srv.log,
		m:      srv.m,
		send:   make(chan []byte, sendBuffer),
		closed: make(chan struct{}),
		subs:   make(map[string]func()),
	}
}

// newSessionID returns an unguessable session identifier. Sessions are compared
// for equality across gateways to decide who owns a user, so a predictable ID
// would let a client claim a session it does not hold.
func newSessionID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand failing is not a condition to paper over with a weaker
		// source; the caller turns this into a connection rejection.
		return ""
	}
	return hex.EncodeToString(b[:])
}

// enqueue queues a frame for delivery, closing the connection if it cannot keep
// up. Never blocks.
func (c *connection) enqueue(frame []byte) {
	select {
	case c.send <- frame:
	case <-c.closed:
	default:
		c.log.Warn("client cannot keep up, dropping connection",
			"user_id", c.userID, "buffered", len(c.send))
		c.close(reasonWriteError)
	}
}

func (c *connection) sendFrame(t protocol.Type, payload any) {
	raw, err := protocol.Encode(t, payload)
	if err != nil {
		c.log.Error("could not encode outbound frame", "type", t, "err", err)
		return
	}
	c.enqueue(raw)
	c.m.FramesSent.Inc()
}

func (c *connection) sendError(code, message string) {
	c.enqueue(protocol.MustEncodeError(code, message))
	c.m.FramesSent.Inc()
}

// close signals teardown exactly once.
//
// It deliberately does not touch the socket. Closing it here would discard
// whatever is still queued — and the last frame before a close is usually the
// ERROR explaining why, or the OFFLINE telling an evicted client it was
// superseded. The write pump owns the socket and closes it after draining, so
// the client always learns the reason it was dropped.
func (c *connection) close(reason string) {
	c.closeOnce.Do(func() {
		c.m.ConnectionsClosed.WithLabelValues(reason).Inc()
		close(c.closed)
	})
}

// releaseSubscriptions drops every presence subscription this connection held.
// Called on teardown so a departed client stops costing this gateway a Redis
// subscription.
func (c *connection) releaseSubscriptions() {
	c.subMu.Lock()
	releases := make([]func(), 0, len(c.subs))
	for _, release := range c.subs {
		releases = append(releases, release)
	}
	c.subs = make(map[string]func())
	c.subMu.Unlock()

	for _, release := range releases {
		release()
	}
}

// writePump owns the socket's write side.
//
// Every write goes through this one goroutine. gorilla/websocket permits only
// one concurrent writer, and funnelling pings and frames through the same place
// is simpler to reason about than a write mutex held across fan-out.
func (c *connection) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.close(reasonWriteError)
		// The write pump owns the socket: closing it here, and only here, means
		// there is exactly one place that can cut a client off mid-frame.
		_ = c.ws.Close()
	}()

	for {
		select {
		case <-c.closed:
			c.drain()
			// Best effort: tell the client why before the socket goes away.
			_ = c.ws.SetWriteDeadline(time.Now().Add(writeWait))
			_ = c.ws.WriteMessage(websocket.CloseMessage,
				websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
			return

		case frame := <-c.send:
			if err := c.ws.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}

		case <-ticker.C:
			if err := c.ws.SetWriteDeadline(time.Now().Add(writeWait)); err != nil {
				return
			}
			if err := c.ws.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// drain writes whatever is already queued before the connection goes away.
//
// Bounded by sendBuffer and a wall-clock budget: a client that has stopped
// reading must not be able to hold a teardown open, and by this point the
// connection is going regardless.
func (c *connection) drain() {
	deadline := time.Now().Add(writeWait)
	for i := 0; i < sendBuffer; i++ {
		select {
		case frame := <-c.send:
			if time.Now().After(deadline) {
				return
			}
			if err := c.ws.SetWriteDeadline(deadline); err != nil {
				return
			}
			if err := c.ws.WriteMessage(websocket.TextMessage, frame); err != nil {
				return
			}
		default:
			return
		}
	}
}

// readPump owns the socket's read side and drives the whole session lifecycle.
func (c *connection) readPump(ctx context.Context) {
	defer func() {
		c.releaseSubscriptions()
		if c.userID != "" {
			if c.srv.hub.unregister(c.userID, c.sessionID) {
				// Only the connection still registered announces the departure,
				// so a session already displaced does not publish an OFFLINE for
				// a user who is live elsewhere.
				disconnectCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := c.srv.presence.Disconnect(disconnectCtx, c.userID, c.sessionID); err != nil {
					c.log.Warn("disconnect cleanup failed", "user_id", c.userID, "err", err)
				}
				cancel()
			}
		}
		c.close(reasonClientClose)
	}()

	c.ws.SetReadLimit(protocol.MaxFrameBytes)
	_ = c.ws.SetReadDeadline(time.Now().Add(helloWait))
	c.ws.SetPongHandler(func(string) error {
		return c.ws.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		select {
		case <-c.closed:
			return
		case <-ctx.Done():
			return
		default:
		}

		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				c.m.ConnectionsClosed.WithLabelValues(reasonReadError).Inc()
			}
			return
		}
		c.m.FramesReceived.Inc()

		// The read limit above closes the socket on oversize rather than
		// returning the payload, so count it here to keep the integrity metric
		// honest about what the gateway rejected.
		frame, err := protocol.Decode(raw)
		if err != nil {
			code := protocol.CodeOf(err)
			c.m.FramesRejected.WithLabelValues(code).Inc()
			c.sendError(code, err.Error())
			continue
		}

		if c.userID == "" && frame.Type != protocol.TypeHello {
			c.m.FramesRejected.WithLabelValues(protocol.CodeUnauthorized).Inc()
			c.sendError(protocol.CodeUnauthorized, "HELLO must be the first frame")
			c.close(reasonReadError)
			return
		}

		if err := c.handle(ctx, frame); err != nil {
			c.log.Warn("frame handling failed",
				"type", frame.Type, "user_id", c.userID, "err", err)
			c.sendError(protocol.CodeInternal, "internal error")
		}
	}
}

func (c *connection) handle(ctx context.Context, frame *protocol.Frame) error {
	switch frame.Type {
	case protocol.TypeHello:
		return c.handleHello(ctx, frame)
	case protocol.TypeSubscribe:
		return c.handleSubscribe(ctx, frame)
	case protocol.TypeHeartbeat:
		return c.handleHeartbeat(ctx, frame)
	case protocol.TypeSetPresence:
		return c.handleSetPresence(ctx, frame)
	case protocol.TypeJoin:
		return c.handleJoin(ctx, frame)
	default:
		// Unreachable: Decode already rejects non-client types. Handled anyway
		// so adding a type without a handler fails loudly rather than silently.
		c.m.FramesRejected.WithLabelValues(protocol.CodeUnknownType).Inc()
		c.sendError(protocol.CodeUnknownType, "unhandled message type")
		return nil
	}
}

func (c *connection) handleHello(ctx context.Context, frame *protocol.Frame) error {
	if c.userID != "" {
		c.m.FramesRejected.WithLabelValues(protocol.CodeInvalidField).Inc()
		c.sendError(protocol.CodeInvalidField, "HELLO already received")
		return nil
	}

	hello, err := protocol.DecodeHello(frame)
	if err != nil {
		code := protocol.CodeOf(err)
		c.m.FramesRejected.WithLabelValues(code).Inc()
		c.sendError(code, err.Error())
		c.close(reasonReadError)
		return nil
	}

	// Dev-mode shared secret, not authentication. It exists so the protocol has
	// a rejection path to exercise; no credential is stored or logged.
	if hello.Token != c.srv.cfg.DevToken {
		c.m.FramesRejected.WithLabelValues(protocol.CodeUnauthorized).Inc()
		c.sendError(protocol.CodeUnauthorized, "invalid token")
		c.close(reasonReadError)
		return nil
	}

	sessionID := newSessionID()
	if sessionID == "" {
		c.sendError(protocol.CodeInternal, "could not allocate a session")
		c.close(reasonReadError)
		return nil
	}

	c.userID = hello.UserID
	c.sessionID = sessionID
	c.log = c.log.With("user_id", hello.UserID)

	if _, err := c.srv.presence.Connect(ctx, c.userID, c.sessionID); err != nil {
		c.userID = "" // not registered; let teardown skip the disconnect path
		c.sendError(protocol.CodeInternal, "could not establish session")
		c.close(reasonReadError)
		return nil
	}

	// A same-gateway duplicate never produces a control message — the eviction
	// notice would be addressed to this very node — so it is handled directly.
	if displaced := c.srv.hub.register(c); displaced != nil && displaced != c {
		displaced.evict()
	}

	// The HELLO deadline is replaced by the ping/pong deadline now that the
	// connection is a real session.
	_ = c.ws.SetReadDeadline(time.Now().Add(pongWait))

	c.sendFrame(protocol.TypeWelcome, protocol.Welcome{
		SessionID: c.sessionID,
		GatewayID: c.srv.cfg.GatewayID,
	})
	return nil
}

func (c *connection) handleSubscribe(ctx context.Context, frame *protocol.Frame) error {
	sub, err := protocol.DecodeSubscribe(frame)
	if err != nil {
		code := protocol.CodeOf(err)
		c.m.FramesRejected.WithLabelValues(code).Inc()
		c.sendError(code, err.Error())
		return nil
	}

	for _, target := range sub.UserIDs {
		c.subMu.Lock()
		_, already := c.subs[target]
		c.subMu.Unlock()
		if already {
			continue
		}

		watched := target
		release, err := c.srv.presence.Bus.Subscribe(ctx, watched, func(p protocol.Presence) {
			c.sendFrame(protocol.TypePresence, p)
		})
		if err != nil {
			return err
		}

		c.subMu.Lock()
		if _, raced := c.subs[watched]; raced {
			// Two SUBSCRIBE frames for the same user arrived concurrently.
			c.subMu.Unlock()
			release()
			continue
		}
		c.subs[watched] = release
		c.subMu.Unlock()
	}

	// Snapshot-on-subscribe is what makes at-most-once pub/sub acceptable: a
	// client that missed events while disconnected is brought current now rather
	// than waiting for each watched user to change.
	snapshot, err := c.srv.presence.Snapshot(ctx, sub.UserIDs)
	if err != nil {
		return err
	}
	for _, target := range sub.UserIDs {
		sess, ok := snapshot[target]
		if !ok {
			// No session means offline. Saying so explicitly avoids a client
			// showing "unknown" indefinitely for a friend who is simply away.
			c.sendFrame(protocol.TypePresence, protocol.Presence{
				UserID: target,
				Status: protocol.StatusOffline,
				TS:     presence.NowMillis(),
			})
			continue
		}
		c.sendFrame(protocol.TypePresence, sess.Presence())
	}
	return nil
}

func (c *connection) handleHeartbeat(ctx context.Context, frame *protocol.Frame) error {
	if _, err := protocol.DecodeHeartbeat(frame); err != nil {
		code := protocol.CodeOf(err)
		c.m.FramesRejected.WithLabelValues(code).Inc()
		c.sendError(code, err.Error())
		return nil
	}

	res, err := c.srv.presence.Heartbeat(ctx, c.userID, c.sessionID)
	if err != nil {
		return err
	}

	switch res {
	case presence.UpdateWrongSession:
		// This session was evicted while the notice was in flight. Failing the
		// heartbeat is what makes eviction stick even if the control message was
		// never delivered.
		c.sendError(protocol.CodeUnauthorized, "session superseded by a newer connection")
		c.close(reasonEvictedDup)
		return nil
	case presence.UpdateNoSession:
		// The session was reaped. The socket is alive but the gateway no longer
		// speaks for this user, so it must not keep serving them.
		c.sendError(protocol.CodeNotReady, "session expired")
		c.close(reasonReaped)
		return nil
	}

	c.sendFrame(protocol.TypeAck, protocol.Ack{TS: presence.NowMillis()})
	return nil
}

func (c *connection) handleSetPresence(ctx context.Context, frame *protocol.Frame) error {
	sp, err := protocol.DecodeSetPresence(frame)
	if err != nil {
		code := protocol.CodeOf(err)
		c.m.FramesRejected.WithLabelValues(code).Inc()
		c.sendError(code, err.Error())
		return nil
	}

	res, err := c.srv.presence.SetPresence(ctx, c.userID, c.sessionID, sp.Status, sp.PlaceID, sp.ServerID)
	if err != nil {
		return err
	}

	switch res {
	case presence.UpdateWrongSession:
		c.sendError(protocol.CodeUnauthorized, "session superseded by a newer connection")
		c.close(reasonEvictedDup)
	case presence.UpdateNoSession:
		c.sendError(protocol.CodeNotReady, "session expired")
		c.close(reasonReaped)
	case presence.UpdateStale:
		// Counted as an integrity violation by the service. The client is told
		// so it can stop sending stale state rather than silently having it
		// ignored.
		c.sendError(protocol.CodeInvalidField, "presence update is older than current state")
	}
	return nil
}

func (c *connection) handleJoin(ctx context.Context, frame *protocol.Frame) error {
	join, err := protocol.DecodeJoin(frame)
	if err != nil {
		code := protocol.CodeOf(err)
		c.m.FramesRejected.WithLabelValues(code).Inc()
		c.sendError(code, err.Error())
		return nil
	}

	outcome, err := c.srv.presence.Join(ctx, join.TargetUserID)
	if err != nil {
		return err
	}
	if !outcome.OK {
		c.sendFrame(protocol.TypeJoinDenied, protocol.JoinDenied{Reason: outcome.Reason})
		return nil
	}
	c.sendFrame(protocol.TypeJoinOK, protocol.JoinOK{
		PlaceID:  outcome.PlaceID,
		ServerID: outcome.ServerID,
	})
	return nil
}

// evict closes this connection because a newer one claimed the same user.
//
// Duplicate-session policy: the *session* ends, but the user does not go
// offline — they are live on the connection that displaced this one. So the
// OFFLINE transition is delivered to this client, which genuinely is going away,
// and deliberately NOT published to the bus. Publishing it cluster-wide would
// race the new session's ONLINE event and could leave every watcher believing a
// connected user is offline.
func (c *connection) evict() {
	c.sendFrame(protocol.TypePresence, protocol.Presence{
		UserID: c.userID,
		Status: protocol.StatusOffline,
		TS:     presence.NowMillis(),
	})
	c.sendError(protocol.CodeUnauthorized, "session superseded by a newer connection")
	c.close(reasonEvictedDup)
}

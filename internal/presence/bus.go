package presence

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/metrics"
	"github.com/Sampai28/Beacon/internal/protocol"
)

// Control message types sent gateway-to-gateway.
const (
	ControlEvict = "EVICT"
)

// ControlMessage is the only node-to-node message Beacon sends. It exists
// because eviction is the one action that must reach a specific gateway rather
// than everyone interested in a user.
type ControlMessage struct {
	Type      string `json:"type"`
	UserID    string `json:"userId"`
	SessionID string `json:"sessionId"`
}

// PresenceHandler receives a presence event for a subscribed user.
type PresenceHandler func(protocol.Presence)

// ControlHandler receives a control message addressed to this gateway.
type ControlHandler func(ControlMessage)

// Bus is the cross-node fan-out layer.
//
// Gateways never talk to each other directly. A gateway subscribes only to the
// per-user channels its own clients have asked for, so fan-out cost scales with
// what is actually being watched rather than with cluster size. This is the
// deliberate alternative to node-to-node gossip: gossip would need an O(N²) mesh
// with its own membership and retry logic — a second distributed system bolted
// onto the first — while Redis is already a hard dependency for session state.
//
// The tradeoff is real: pub/sub is at-most-once, so a subscriber disconnected at
// the instant of publish misses the event. Beacon compensates with
// snapshot-on-subscribe and the drift reconciler rather than pretending delivery
// is guaranteed.
type Bus struct {
	rdb       redis.UniversalClient
	gatewayID string
	log       *slog.Logger
	m         *metrics.Metrics

	mu       sync.Mutex
	ps       *redis.PubSub
	refs     map[string]int                        // userID -> number of local subscribers
	handlers map[string]map[uint64]PresenceHandler // userID -> subscriberID -> handler
	nextID   uint64
	closed   bool

	onControl ControlHandler
}

// NewBus subscribes to this gateway's control channel and returns a bus ready to
// run. The control subscription is established eagerly so the pub/sub connection
// always has at least one channel, and so an eviction addressed to this node
// during startup is not missed.
func NewBus(ctx context.Context, rdb redis.UniversalClient, gatewayID string, m *metrics.Metrics, log *slog.Logger) *Bus {
	b := &Bus{
		rdb:       rdb,
		gatewayID: gatewayID,
		log:       log,
		m:         m,
		refs:      make(map[string]int),
		handlers:  make(map[string]map[uint64]PresenceHandler),
	}
	b.ps = rdb.Subscribe(ctx, ControlChannel(gatewayID))
	return b
}

// OnControl registers the handler for control messages addressed to this
// gateway. Must be called before Run.
func (b *Bus) OnControl(h ControlHandler) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onControl = h
}

// Run pumps the pub/sub connection until ctx is cancelled. It returns nil on
// clean shutdown.
func (b *Bus) Run(ctx context.Context) error {
	ch := b.ps.Channel()
	for {
		select {
		case <-ctx.Done():
			return nil
		case msg, ok := <-ch:
			if !ok {
				// go-redis closes the channel only when the PubSub is closed,
				// which for us means shutdown.
				return nil
			}
			b.dispatch(msg)
		}
	}
}

func (b *Bus) dispatch(msg *redis.Message) {
	if msg.Channel == ControlChannel(b.gatewayID) {
		var cm ControlMessage
		if err := json.Unmarshal([]byte(msg.Payload), &cm); err != nil {
			b.m.PubSubErrors.Inc()
			b.log.Warn("undecodable control message", "err", err)
			return
		}
		b.mu.Lock()
		h := b.onControl
		b.mu.Unlock()
		if h != nil {
			h(cm)
		}
		return
	}

	userID, ok := userIDFromPresenceChannel(msg.Channel)
	if !ok {
		// Not a channel we recognise. Counted rather than ignored: it means
		// something is publishing into Beacon's keyspace.
		b.m.PubSubErrors.Inc()
		return
	}

	var p protocol.Presence
	if err := json.Unmarshal([]byte(msg.Payload), &p); err != nil {
		b.m.PubSubErrors.Inc()
		b.log.Warn("undecodable presence event", "user_id", userID, "err", err)
		return
	}

	// Snapshot the handlers under the lock, then invoke them outside it. A
	// handler writes to a client's send queue, and holding the bus lock across
	// that would let one slow client stall fan-out for every other user on this
	// gateway.
	b.mu.Lock()
	subs := make([]PresenceHandler, 0, len(b.handlers[userID]))
	for _, h := range b.handlers[userID] {
		subs = append(subs, h)
	}
	b.mu.Unlock()

	for _, h := range subs {
		h(p)
		b.m.PresenceFanout.Inc()
	}
}

// Subscribe registers interest in a user's presence and returns a function that
// releases it.
//
// Redis subscription is refcounted: the first local subscriber to a user causes
// a SUBSCRIBE, the last one to leave causes an UNSUBSCRIBE. Without this, a
// gateway with a thousand clients watching the same popular user would hold a
// thousand redundant subscriptions.
func (b *Bus) Subscribe(ctx context.Context, userID string, h PresenceHandler) (func(), error) {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil, errors.New("presence: bus is closed")
	}

	needsRedisSubscribe := b.refs[userID] == 0
	b.refs[userID]++

	id := b.nextID
	b.nextID++
	if b.handlers[userID] == nil {
		b.handlers[userID] = make(map[uint64]PresenceHandler)
	}
	b.handlers[userID][id] = h
	b.m.SubscriptionsActive.Set(float64(len(b.refs)))
	b.mu.Unlock()

	if needsRedisSubscribe {
		if err := b.ps.Subscribe(ctx, PresenceChannel(userID)); err != nil {
			// Roll the bookkeeping back so a failed subscribe does not leave a
			// phantom refcount that suppresses the next real attempt.
			b.release(userID, id)
			b.m.PubSubErrors.Inc()
			return nil, fmt.Errorf("subscribe %s: %w", userID, err)
		}
	}

	var once sync.Once
	return func() {
		once.Do(func() { b.release(userID, id) })
	}, nil
}

func (b *Bus) release(userID string, id uint64) {
	b.mu.Lock()
	if handlers, ok := b.handlers[userID]; ok {
		delete(handlers, id)
		if len(handlers) == 0 {
			delete(b.handlers, userID)
		}
	}

	b.refs[userID]--
	last := b.refs[userID] <= 0
	if last {
		delete(b.refs, userID)
	}
	b.m.SubscriptionsActive.Set(float64(len(b.refs)))
	closed := b.closed
	b.mu.Unlock()

	if last && !closed {
		// Unsubscribing is best effort: a failure leaves a subscription open,
		// which wastes a little memory but never delivers to a departed client,
		// since the handler map entry is already gone.
		if err := b.ps.Unsubscribe(context.Background(), PresenceChannel(userID)); err != nil {
			b.m.PubSubErrors.Inc()
		}
	}
}

// Publish fans a presence event out to every gateway with a subscriber.
func (b *Bus) Publish(ctx context.Context, p protocol.Presence) error {
	raw, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("encode presence event: %w", err)
	}
	if err := b.rdb.Publish(ctx, PresenceChannel(p.UserID), raw).Err(); err != nil {
		b.m.PubSubErrors.Inc()
		return fmt.Errorf("publish presence: %w", err)
	}
	return nil
}

// SendControl delivers a control message to a specific gateway.
func (b *Bus) SendControl(ctx context.Context, gatewayID string, cm ControlMessage) error {
	raw, err := json.Marshal(cm)
	if err != nil {
		return fmt.Errorf("encode control message: %w", err)
	}
	if err := b.rdb.Publish(ctx, ControlChannel(gatewayID), raw).Err(); err != nil {
		b.m.PubSubErrors.Inc()
		return fmt.Errorf("send control: %w", err)
	}
	return nil
}

// SubscribedUsers is the number of distinct users this gateway watches.
func (b *Bus) SubscribedUsers() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.refs)
}

// Close tears down the pub/sub connection.
func (b *Bus) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	b.mu.Unlock()

	return b.ps.Close()
}

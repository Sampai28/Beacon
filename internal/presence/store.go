package presence

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/Sampai28/Beacon/internal/protocol"
)

// Key layout. Everything Beacon writes lives under the beacon: prefix, and the
// protocol layer rejects identifiers containing ':' so a client cannot construct
// a userId that forges one of these.
const (
	keyPrefix     = "beacon:"
	sessionKeyFmt = keyPrefix + "session:%s"  // HASH, TTL-bearing
	sessionSetKey = keyPrefix + "sessions"    // SET of userIds with a session
	presenceChFmt = keyPrefix + "presence:%s" // pub/sub, per user

	// controlChFmt is per *gateway*, not per user. Eviction notices are the only
	// node-to-node message Beacon sends, and the claiming gateway already learns
	// the displaced gateway's ID from the claim script — so addressing it
	// directly costs one subscription per gateway instead of one per connection.
	controlChFmt = keyPrefix + "gwctl:%s"

	nodeKeyFmt = keyPrefix + "node:%s" // STRING, TTL-bearing, holds conn count
	nodeSetKey = keyPrefix + "nodes"   // SET of gateway IDs
)

// ErrNoSession is returned when a user has no live session.
var ErrNoSession = errors.New("presence: no session for user")

func SessionKey(userID string) string        { return fmt.Sprintf(sessionKeyFmt, userID) }
func PresenceChannel(userID string) string   { return fmt.Sprintf(presenceChFmt, userID) }
func ControlChannel(gatewayID string) string { return fmt.Sprintf(controlChFmt, gatewayID) }
func NodeKey(gatewayID string) string        { return fmt.Sprintf(nodeKeyFmt, gatewayID) }

// userIDFromPresenceChannel reverses PresenceChannel. Returns false for any
// channel that is not a presence channel.
func userIDFromPresenceChannel(channel string) (string, bool) {
	const prefix = keyPrefix + "presence:"
	if len(channel) <= len(prefix) || channel[:len(prefix)] != prefix {
		return "", false
	}
	return channel[len(prefix):], true
}

// Session is the authoritative record of where a user is and who is serving
// them. It lives in Redis rather than gateway memory so any replica can answer
// for any user.
type Session struct {
	UserID    string          `json:"userId"`
	SessionID string          `json:"sessionId"`
	Status    protocol.Status `json:"status"`
	PlaceID   string          `json:"placeId,omitempty"`
	ServerID  string          `json:"serverId,omitempty"`
	GatewayID string          `json:"gatewayId"`

	// LastSeen is milliseconds since the epoch. It is both the freshness marker
	// the reaper reads and the ordering token that out-of-order rejection
	// compares against, which is why every write carries one.
	LastSeen int64 `json:"lastSeen"`
}

// Joinable reports whether this session can satisfy a JOIN. A user who is
// online but not in a place has nothing to join, which is a denial rather than
// an error.
func (s *Session) Joinable() bool {
	return s != nil && s.Status == protocol.StatusInGame && s.PlaceID != "" && s.ServerID != ""
}

// Presence projects the session into the wire event subscribers receive.
func (s *Session) Presence() protocol.Presence {
	return protocol.Presence{
		UserID:   s.UserID,
		Status:   s.Status,
		PlaceID:  s.PlaceID,
		ServerID: s.ServerID,
		TS:       s.LastSeen,
	}
}

// Evicted describes a session displaced by a newer connection for the same user.
type Evicted struct {
	SessionID string
	GatewayID string
}

// Store is the Redis-backed session store.
//
// Every mutation that has to read-then-write runs as a Lua script. This is not
// premature caution: three gateways can act on the same user concurrently, and a
// read-modify-write done in Go would let a stale presence overwrite a fresh one
// between the read and the write — exactly the corruption the out-of-order check
// exists to prevent.
type Store struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

func NewStore(rdb redis.UniversalClient, sessionTTL time.Duration) *Store {
	return &Store{rdb: rdb, ttl: sessionTTL}
}

// TTL is the session lifetime a heartbeat refreshes.
func (s *Store) TTL() time.Duration { return s.ttl }

// claimScript writes a new session and reports whichever session it displaced.
//
// Claim always wins: the newest connection is authoritative. The alternative —
// rejecting the new connection while an old one exists — strands a user whose
// previous session is a half-dead socket the gateway has not noticed yet, which
// is the common case rather than the rare one.
var claimScript = redis.NewScript(`
local old_session = redis.call('HGET', KEYS[1], 'sessionId')
local old_gateway = redis.call('HGET', KEYS[1], 'gatewayId')

redis.call('HSET', KEYS[1],
  'userId',    ARGV[1],
  'sessionId', ARGV[2],
  'status',    ARGV[3],
  'placeId',   ARGV[4],
  'serverId',  ARGV[5],
  'gatewayId', ARGV[6],
  'lastSeen',  ARGV[7])
redis.call('PEXPIRE', KEYS[1], ARGV[8])
redis.call('SADD', KEYS[2], ARGV[1])

if old_session then
  return {old_session, old_gateway or ''}
end
return {'', ''}
`)

// Claim registers a session for a user, displacing any existing one.
//
// The returned Evicted is non-nil only when a *different* session was displaced.
// A gateway re-claiming its own session ID is a reconnect, not an eviction, and
// must not be counted as a duplicate.
func (s *Store) Claim(ctx context.Context, sess Session) (*Evicted, error) {
	res, err := claimScript.Run(ctx, s.rdb,
		[]string{SessionKey(sess.UserID), sessionSetKey},
		sess.UserID, sess.SessionID, string(sess.Status), sess.PlaceID,
		sess.ServerID, sess.GatewayID, sess.LastSeen, s.ttl.Milliseconds(),
	).Result()
	if err != nil {
		return nil, fmt.Errorf("claim session: %w", err)
	}

	pair, ok := res.([]any)
	if !ok || len(pair) != 2 {
		return nil, fmt.Errorf("claim session: unexpected script result %T", res)
	}
	oldSession, _ := pair[0].(string)
	oldGateway, _ := pair[1].(string)

	if oldSession == "" || oldSession == sess.SessionID {
		return nil, nil
	}
	return &Evicted{SessionID: oldSession, GatewayID: oldGateway}, nil
}

// updateScript applies a presence change only if it is not older than what is
// stored. This is integrity check 3, and it lives in Lua because the comparison
// and the write must not be separable.
var updateScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
if redis.call('HGET', KEYS[1], 'sessionId') ~= ARGV[1] then
  return -2
end

local stored = redis.call('HGET', KEYS[1], 'lastSeen')
if stored and tonumber(stored) > tonumber(ARGV[5]) then
  return 0
end

redis.call('HSET', KEYS[1],
  'status',   ARGV[2],
  'placeId',  ARGV[3],
  'serverId', ARGV[4],
  'lastSeen', ARGV[5])
redis.call('PEXPIRE', KEYS[1], ARGV[6])
return 1
`)

// UpdateResult distinguishes the ways a presence update can fail to apply.
// Callers map each to a different metric, so collapsing them into a bool would
// make an out-of-order event indistinguishable from a vanished session.
type UpdateResult int

const (
	// UpdateApplied means the new presence is now authoritative.
	UpdateApplied UpdateResult = iota
	// UpdateStale means the event carried a timestamp older than stored
	// lastSeen and was dropped. Integrity check 3.
	UpdateStale
	// UpdateNoSession means the session expired or was never claimed.
	UpdateNoSession
	// UpdateWrongSession means another connection has since claimed this user,
	// so this caller no longer speaks for them.
	UpdateWrongSession
)

func (r UpdateResult) String() string {
	switch r {
	case UpdateApplied:
		return "applied"
	case UpdateStale:
		return "stale"
	case UpdateNoSession:
		return "no_session"
	case UpdateWrongSession:
		return "wrong_session"
	default:
		return "unknown"
	}
}

// Update applies a presence change, rejecting anything older than what is
// already stored.
func (s *Store) Update(ctx context.Context, sess Session) (UpdateResult, error) {
	res, err := updateScript.Run(ctx, s.rdb,
		[]string{SessionKey(sess.UserID)},
		sess.SessionID, string(sess.Status), sess.PlaceID, sess.ServerID,
		sess.LastSeen, s.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return UpdateNoSession, fmt.Errorf("update presence: %w", err)
	}

	switch res {
	case 1:
		return UpdateApplied, nil
	case 0:
		return UpdateStale, nil
	case -2:
		return UpdateWrongSession, nil
	default:
		return UpdateNoSession, nil
	}
}

// heartbeatScript refreshes a session's TTL, but only for the connection that
// still owns it. An evicted connection heartbeating must not resurrect its
// session, which is what makes eviction stick without cross-gateway coordination.
var heartbeatScript = redis.NewScript(`
if redis.call('EXISTS', KEYS[1]) == 0 then
  return -1
end
if redis.call('HGET', KEYS[1], 'sessionId') ~= ARGV[1] then
  return -2
end
redis.call('HSET', KEYS[1], 'lastSeen', ARGV[2])
redis.call('PEXPIRE', KEYS[1], ARGV[3])
return 1
`)

// Heartbeat refreshes the session TTL. It returns UpdateWrongSession when the
// caller has been evicted, which the connection loop treats as a signal to close.
func (s *Store) Heartbeat(ctx context.Context, userID, sessionID string, nowMillis int64) (UpdateResult, error) {
	res, err := heartbeatScript.Run(ctx, s.rdb,
		[]string{SessionKey(userID)},
		sessionID, nowMillis, s.ttl.Milliseconds(),
	).Int64()
	if err != nil {
		return UpdateNoSession, fmt.Errorf("heartbeat: %w", err)
	}

	switch res {
	case 1:
		return UpdateApplied, nil
	case -2:
		return UpdateWrongSession, nil
	default:
		return UpdateNoSession, nil
	}
}

// Get returns a user's session, or ErrNoSession.
func (s *Store) Get(ctx context.Context, userID string) (*Session, error) {
	fields, err := s.rdb.HGetAll(ctx, SessionKey(userID)).Result()
	if err != nil {
		return nil, fmt.Errorf("get session: %w", err)
	}
	if len(fields) == 0 {
		return nil, ErrNoSession
	}
	return sessionFromHash(fields), nil
}

// GetMany returns sessions for the given users, omitting those with none. It
// pipelines rather than looping so a SUBSCRIBE to 200 friends costs one round
// trip instead of 200 — the difference between a snappy client and a visibly
// slow one at the connection counts this service targets.
func (s *Store) GetMany(ctx context.Context, userIDs []string) (map[string]*Session, error) {
	if len(userIDs) == 0 {
		return map[string]*Session{}, nil
	}

	pipe := s.rdb.Pipeline()
	cmds := make(map[string]*redis.MapStringStringCmd, len(userIDs))
	for _, id := range userIDs {
		cmds[id] = pipe.HGetAll(ctx, SessionKey(id))
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("get sessions: %w", err)
	}

	out := make(map[string]*Session, len(userIDs))
	for id, cmd := range cmds {
		fields, err := cmd.Result()
		if err != nil || len(fields) == 0 {
			continue
		}
		out[id] = sessionFromHash(fields)
	}
	return out, nil
}

// deleteScript removes a session, optionally only when the caller still owns it.
// An empty sessionId argument forces the delete, which is what the reaper needs
// when cleaning up after a gateway that no longer exists.
var deleteScript = redis.NewScript(`
if ARGV[1] ~= '' then
  local current = redis.call('HGET', KEYS[1], 'sessionId')
  if current and current ~= ARGV[1] then
    return 0
  end
end
redis.call('DEL', KEYS[1])
redis.call('SREM', KEYS[2], ARGV[2])
return 1
`)

// Delete removes a session. When sessionID is non-empty the delete only applies
// if that session is still the current one, so a slow disconnect cannot remove a
// session the user has already re-established elsewhere.
func (s *Store) Delete(ctx context.Context, userID, sessionID string) (bool, error) {
	res, err := deleteScript.Run(ctx, s.rdb,
		[]string{SessionKey(userID), sessionSetKey},
		sessionID, userID,
	).Int64()
	if err != nil {
		return false, fmt.Errorf("delete session: %w", err)
	}
	return res == 1, nil
}

// ForgetUser drops a userId from the session index without touching the session
// hash. Used by the reaper when the hash has already expired and only the index
// entry remains.
func (s *Store) ForgetUser(ctx context.Context, userID string) error {
	if err := s.rdb.SRem(ctx, sessionSetKey, userID).Err(); err != nil {
		return fmt.Errorf("forget user: %w", err)
	}
	return nil
}

// SessionCount is the cardinality of the session index — one half of the drift
// comparison.
func (s *Store) SessionCount(ctx context.Context) (int64, error) {
	n, err := s.rdb.SCard(ctx, sessionSetKey).Result()
	if err != nil {
		return 0, fmt.Errorf("session count: %w", err)
	}
	return n, nil
}

// IndexedUsers returns every userId in the session index, including any whose
// session hash has already expired. Those are precisely what the reaper is
// looking for.
func (s *Store) IndexedUsers(ctx context.Context) ([]string, error) {
	ids, err := s.rdb.SMembers(ctx, sessionSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("indexed users: %w", err)
	}
	return ids, nil
}

// Exists reports whether a session hash is still live, distinguishing an expired
// session from an absent index entry.
func (s *Store) Exists(ctx context.Context, userID string) (bool, error) {
	n, err := s.rdb.Exists(ctx, SessionKey(userID)).Result()
	if err != nil {
		return false, fmt.Errorf("session exists: %w", err)
	}
	return n > 0, nil
}

func sessionFromHash(fields map[string]string) *Session {
	lastSeen, _ := strconv.ParseInt(fields["lastSeen"], 10, 64)
	return &Session{
		UserID:    fields["userId"],
		SessionID: fields["sessionId"],
		Status:    protocol.Status(fields["status"]),
		PlaceID:   fields["placeId"],
		ServerID:  fields["serverId"],
		GatewayID: fields["gatewayId"],
		LastSeen:  lastSeen,
	}
}

// NowMillis is the single source of wall-clock time for presence ordering.
// Centralised so tests can reason about ordering without reaching into each
// call site.
func NowMillis() int64 { return time.Now().UnixMilli() }

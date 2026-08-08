package protocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// MaxFrameBytes caps an inbound frame at 8KB.
//
// The largest legitimate frame is a SUBSCRIBE carrying a friends list, which
// fits comfortably. The cap exists because the decoder must allocate before it
// can know what it is holding: without a limit, one client can make a gateway
// allocate arbitrarily, and a gateway holding thousands of connections has no
// headroom to absorb that.
const MaxFrameBytes = 8 * 1024

// MaxSubscribeUsers bounds a single SUBSCRIBE. Each subscribed user costs a
// Redis pub/sub channel subscription on the serving gateway, so an unbounded
// list is a cheap way for one client to make a gateway do expensive work.
const MaxSubscribeUsers = 1000

// MaxIDBytes bounds identifier-shaped fields. These become Redis key fragments
// and pub/sub channel names; unbounded values would let a client inflate the
// keyspace.
const MaxIDBytes = 128

// Type is a frame's discriminator.
type Type string

// Client-to-server frame types.
const (
	TypeHello       Type = "HELLO"
	TypeSubscribe   Type = "SUBSCRIBE"
	TypeHeartbeat   Type = "HEARTBEAT"
	TypeSetPresence Type = "SET_PRESENCE"
	TypeJoin        Type = "JOIN"
)

// Server-to-client frame types.
const (
	TypeWelcome    Type = "WELCOME"
	TypeAck        Type = "ACK"
	TypePresence   Type = "PRESENCE"
	TypeJoinOK     Type = "JOIN_OK"
	TypeJoinDenied Type = "JOIN_DENIED"
	TypeError      Type = "ERROR"
)

// Status is a user's presence state.
type Status string

const (
	StatusOnline  Status = "ONLINE"
	StatusOffline Status = "OFFLINE"
	StatusAway    Status = "AWAY"
	StatusInGame  Status = "IN_GAME"
)

// validStatuses is the closed set a client may set. Presence is a small
// enumeration on purpose: accepting free-form strings would put unvalidated
// client input into every subscriber's view of the world.
var validStatuses = map[Status]struct{}{
	StatusOnline:  {},
	StatusOffline: {},
	StatusAway:    {},
	StatusInGame:  {},
}

// ValidStatus reports whether s is a status a client is allowed to set.
func ValidStatus(s Status) bool {
	_, ok := validStatuses[s]
	return ok
}

// Error codes sent in ERROR frames. Stable strings so the demo client and the
// integration tests can assert on them without matching prose.
const (
	CodeBadFrame      = "BAD_FRAME"
	CodeUnknownType   = "UNKNOWN_TYPE"
	CodeMissingField  = "MISSING_FIELD"
	CodeInvalidField  = "INVALID_FIELD"
	CodeFrameTooLarge = "FRAME_TOO_LARGE"
	CodeUnauthorized  = "UNAUTHORIZED"
	CodeNotReady      = "NOT_READY"
	CodeInternal      = "INTERNAL"
)

// Reasons a JOIN can be denied.
const (
	ReasonTargetOffline     = "TARGET_OFFLINE"
	ReasonTargetNotJoinable = "TARGET_NOT_JOINABLE"
	ReasonTargetUnknown     = "TARGET_UNKNOWN"
)

// Frame is the envelope every message shares. Payload stays raw until the type
// is known to be one this gateway handles, so an unknown type costs one small
// unmarshal rather than a speculative decode.
type Frame struct {
	Type    Type            `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Client-to-server payloads.
type (
	// Hello opens a session. Token is a dev-mode shared secret, not
	// authentication, and is never logged or persisted.
	Hello struct {
		UserID string `json:"userId"`
		Token  string `json:"token"`
	}

	// Subscribe asks for presence updates on a set of users and returns their
	// current snapshots.
	Subscribe struct {
		UserIDs []string `json:"userIds"`
	}

	// Heartbeat keeps the session's TTL alive. Deliberately empty.
	Heartbeat struct{}

	// SetPresence updates the caller's own presence.
	SetPresence struct {
		Status   Status `json:"status"`
		PlaceID  string `json:"placeId,omitempty"`
		ServerID string `json:"serverId,omitempty"`
	}

	// Join asks where a target user is, so the caller can follow them.
	Join struct {
		TargetUserID string `json:"targetUserId"`
	}
)

// Server-to-client payloads.
type (
	Welcome struct {
		SessionID string `json:"sessionId"`
		GatewayID string `json:"gatewayId"`
	}

	Ack struct {
		TS int64 `json:"ts"`
	}

	// Presence is the fan-out event. TS is the millisecond timestamp of the
	// originating change and is what out-of-order rejection compares against;
	// it is the event's own time, not the time it was relayed.
	Presence struct {
		UserID   string `json:"userId"`
		Status   Status `json:"status"`
		PlaceID  string `json:"placeId,omitempty"`
		ServerID string `json:"serverId,omitempty"`
		TS       int64  `json:"ts"`
	}

	JoinOK struct {
		PlaceID  string `json:"placeId"`
		ServerID string `json:"serverId"`
	}

	JoinDenied struct {
		Reason string `json:"reason"`
	}

	ErrorPayload struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
)

// ValidationError is a rejected frame, carrying the wire code to return to the
// client. Callers switch on Code to pick the metric to increment, so the reason
// a frame was rejected stays machine-readable rather than parsed from prose.
type ValidationError struct {
	Code    string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func invalid(code, format string, args ...any) *ValidationError {
	return &ValidationError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// CodeOf extracts the wire error code from err, or CodeInternal if err is not a
// validation failure. Lets a single call site map any error to a client-safe
// code without leaking internal detail.
func CodeOf(err error) string {
	var ve *ValidationError
	if errors.As(err, &ve) {
		return ve.Code
	}
	return CodeInternal
}

// Decode parses and validates one inbound frame.
//
// Every rejection path returns a *ValidationError; none panics, and none
// returns a partially-populated frame. Untrusted bytes go in, and either a
// fully valid frame or a typed error comes out.
func Decode(raw []byte) (*Frame, error) {
	if len(raw) == 0 {
		return nil, invalid(CodeBadFrame, "empty frame")
	}
	if len(raw) > MaxFrameBytes {
		// Reported before parsing: the whole point is to avoid decoding it.
		return nil, invalid(CodeFrameTooLarge, "frame is %d bytes, limit is %d", len(raw), MaxFrameBytes)
	}
	if !utf8.Valid(raw) {
		return nil, invalid(CodeBadFrame, "frame is not valid UTF-8")
	}

	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		// The parser error is not forwarded to the client: it can echo back
		// fragments of the input and reveals the parser in use.
		return nil, invalid(CodeBadFrame, "malformed JSON")
	}
	if f.Type == "" {
		return nil, invalid(CodeMissingField, "type is required")
	}
	if !isClientType(f.Type) {
		return nil, invalid(CodeUnknownType, "unknown message type %q", f.Type)
	}
	return &f, nil
}

// isClientType reports whether t is a type clients are allowed to send. Server
// frame types are rejected explicitly rather than falling through as unknown, so
// a client cannot inject a PRESENCE frame and have it treated as anything but a
// protocol violation.
func isClientType(t Type) bool {
	switch t {
	case TypeHello, TypeSubscribe, TypeHeartbeat, TypeSetPresence, TypeJoin:
		return true
	default:
		return false
	}
}

// DecodeHello validates a HELLO payload.
func DecodeHello(f *Frame) (*Hello, error) {
	var h Hello
	if err := unmarshalPayload(f, &h); err != nil {
		return nil, err
	}
	if err := validateID("userId", h.UserID); err != nil {
		return nil, err
	}
	if h.Token == "" {
		return nil, invalid(CodeMissingField, "token is required")
	}
	if len(h.Token) > MaxIDBytes {
		// Bounded before any comparison so an oversized token cannot be used to
		// probe timing.
		return nil, invalid(CodeInvalidField, "token exceeds %d bytes", MaxIDBytes)
	}
	return &h, nil
}

// DecodeSubscribe validates a SUBSCRIBE payload, rejecting empty and oversized
// lists and de-duplicating while preserving order.
func DecodeSubscribe(f *Frame) (*Subscribe, error) {
	var s Subscribe
	if err := unmarshalPayload(f, &s); err != nil {
		return nil, err
	}
	if len(s.UserIDs) == 0 {
		return nil, invalid(CodeMissingField, "userIds must contain at least one entry")
	}
	if len(s.UserIDs) > MaxSubscribeUsers {
		return nil, invalid(CodeInvalidField, "userIds has %d entries, limit is %d", len(s.UserIDs), MaxSubscribeUsers)
	}

	seen := make(map[string]struct{}, len(s.UserIDs))
	unique := make([]string, 0, len(s.UserIDs))
	for i, id := range s.UserIDs {
		if err := validateID(fmt.Sprintf("userIds[%d]", i), id); err != nil {
			return nil, err
		}
		if _, dup := seen[id]; dup {
			// Duplicates are a client bug, not an attack. Collapsing them is
			// cheaper than subscribing twice and then fanning out twice.
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	s.UserIDs = unique
	return &s, nil
}

// DecodeHeartbeat validates a HEARTBEAT. The payload is allowed to be absent,
// null or an empty object; anything else is a client that has misunderstood the
// protocol.
func DecodeHeartbeat(f *Frame) (*Heartbeat, error) {
	if len(f.Payload) == 0 || string(f.Payload) == "null" {
		return &Heartbeat{}, nil
	}
	var h Heartbeat
	if err := json.Unmarshal(f.Payload, &h); err != nil {
		return nil, invalid(CodeBadFrame, "malformed HEARTBEAT payload")
	}
	return &h, nil
}

// DecodeSetPresence validates a SET_PRESENCE payload.
func DecodeSetPresence(f *Frame) (*SetPresence, error) {
	var p SetPresence
	if err := unmarshalPayload(f, &p); err != nil {
		return nil, err
	}
	if p.Status == "" {
		return nil, invalid(CodeMissingField, "status is required")
	}
	if !ValidStatus(p.Status) {
		return nil, invalid(CodeInvalidField, "unknown status %q", p.Status)
	}
	if err := validateOptionalID("placeId", p.PlaceID); err != nil {
		return nil, err
	}
	if err := validateOptionalID("serverId", p.ServerID); err != nil {
		return nil, err
	}
	// IN_GAME without a location is not a rejection: a client may legitimately
	// be launching. It simply produces a presence that JOIN will deny.
	return &p, nil
}

// DecodeJoin validates a JOIN payload.
func DecodeJoin(f *Frame) (*Join, error) {
	var j Join
	if err := unmarshalPayload(f, &j); err != nil {
		return nil, err
	}
	if err := validateID("targetUserId", j.TargetUserID); err != nil {
		return nil, err
	}
	return &j, nil
}

func unmarshalPayload(f *Frame, dst any) error {
	if f == nil {
		return invalid(CodeBadFrame, "nil frame")
	}
	if len(f.Payload) == 0 || string(f.Payload) == "null" {
		return invalid(CodeMissingField, "payload is required for %s", f.Type)
	}
	if err := json.Unmarshal(f.Payload, dst); err != nil {
		return invalid(CodeBadFrame, "malformed %s payload", f.Type)
	}
	return nil
}

// validateID enforces the shape of identifier fields.
//
// These values become Redis key fragments and pub/sub channel names. Control
// characters and colons would let a client construct keys that collide with
// Beacon's own namespacing, so the check is a whitelist of shape rather than a
// blacklist of characters.
func validateID(field, v string) error {
	if v == "" {
		return invalid(CodeMissingField, "%s is required", field)
	}
	if len(v) > MaxIDBytes {
		return invalid(CodeInvalidField, "%s exceeds %d bytes", field, MaxIDBytes)
	}
	if !utf8.ValidString(v) {
		return invalid(CodeInvalidField, "%s is not valid UTF-8", field)
	}
	if strings.ContainsAny(v, ":\n\r\t\x00 ") {
		return invalid(CodeInvalidField, "%s contains a disallowed character", field)
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f {
			return invalid(CodeInvalidField, "%s contains a control character", field)
		}
	}
	return nil
}

func validateOptionalID(field, v string) error {
	if v == "" {
		return nil
	}
	return validateID(field, v)
}

// Encode marshals an outbound frame.
//
// Server frames are constructed by this process from validated state, so a
// failure here is a programming error rather than bad input. It is still
// returned rather than panicked on: a gateway holding thousands of connections
// must not die because one payload was malformed.
func Encode(t Type, payload any) ([]byte, error) {
	if payload == nil {
		return json.Marshal(Frame{Type: t})
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("encode %s payload: %w", t, err)
	}
	return json.Marshal(Frame{Type: t, Payload: raw})
}

// MustEncodeError builds an ERROR frame. Error frames are the last line of
// defence — often the response to something already malformed — so this returns
// a hand-built fallback rather than an error the caller would have to handle
// while already handling an error.
func MustEncodeError(code, message string) []byte {
	b, err := Encode(TypeError, ErrorPayload{Code: code, Message: message})
	if err != nil {
		return []byte(`{"type":"ERROR","payload":{"code":"INTERNAL","message":"error encoding failed"}}`)
	}
	return b
}

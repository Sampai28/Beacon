package protocol

import (
	"encoding/json"
	"strings"
	"testing"
)

func mustDecode(t *testing.T, raw string) *Frame {
	t.Helper()
	f, err := Decode([]byte(raw))
	if err != nil {
		t.Fatalf("Decode(%q) unexpectedly failed: %v", raw, err)
	}
	return f
}

// assertRejected checks that a decode failed with a specific wire code. The code
// matters as much as the failure: each one drives a different integrity metric,
// so a frame rejected for the wrong reason is counted in the wrong place.
func assertRejected(t *testing.T, err error, wantCode string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected rejection with code %s, got nil error", wantCode)
	}
	if got := CodeOf(err); got != wantCode {
		t.Fatalf("rejection code: got %s, want %s (message: %v)", got, wantCode, err)
	}
}

// ---------------------------------------------------------------------------
// Round-trip
// ---------------------------------------------------------------------------

func TestEncodeDecodeRoundTripsEveryClientFrame(t *testing.T) {
	cases := []struct {
		name    string
		typ     Type
		payload any
		verify  func(t *testing.T, f *Frame)
	}{
		{
			name:    "HELLO",
			typ:     TypeHello,
			payload: Hello{UserID: "user-1", Token: "dev-token"},
			verify: func(t *testing.T, f *Frame) {
				got, err := DecodeHello(f)
				if err != nil {
					t.Fatalf("DecodeHello: %v", err)
				}
				if got.UserID != "user-1" || got.Token != "dev-token" {
					t.Errorf("round trip lost data: %+v", got)
				}
			},
		},
		{
			name:    "SUBSCRIBE",
			typ:     TypeSubscribe,
			payload: Subscribe{UserIDs: []string{"a", "b", "c"}},
			verify: func(t *testing.T, f *Frame) {
				got, err := DecodeSubscribe(f)
				if err != nil {
					t.Fatalf("DecodeSubscribe: %v", err)
				}
				if len(got.UserIDs) != 3 {
					t.Errorf("userIds: got %v, want 3 entries", got.UserIDs)
				}
			},
		},
		{
			name:    "HEARTBEAT",
			typ:     TypeHeartbeat,
			payload: Heartbeat{},
			verify: func(t *testing.T, f *Frame) {
				if _, err := DecodeHeartbeat(f); err != nil {
					t.Fatalf("DecodeHeartbeat: %v", err)
				}
			},
		},
		{
			name:    "SET_PRESENCE",
			typ:     TypeSetPresence,
			payload: SetPresence{Status: StatusInGame, PlaceID: "place-9", ServerID: "srv-3"},
			verify: func(t *testing.T, f *Frame) {
				got, err := DecodeSetPresence(f)
				if err != nil {
					t.Fatalf("DecodeSetPresence: %v", err)
				}
				if got.Status != StatusInGame || got.PlaceID != "place-9" || got.ServerID != "srv-3" {
					t.Errorf("round trip lost data: %+v", got)
				}
			},
		},
		{
			name:    "JOIN",
			typ:     TypeJoin,
			payload: Join{TargetUserID: "user-2"},
			verify: func(t *testing.T, f *Frame) {
				got, err := DecodeJoin(f)
				if err != nil {
					t.Fatalf("DecodeJoin: %v", err)
				}
				if got.TargetUserID != "user-2" {
					t.Errorf("targetUserId: got %q, want user-2", got.TargetUserID)
				}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := Encode(tc.typ, tc.payload)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			f, err := Decode(raw)
			if err != nil {
				t.Fatalf("Decode of self-encoded frame: %v", err)
			}
			if f.Type != tc.typ {
				t.Fatalf("type: got %q, want %q", f.Type, tc.typ)
			}
			tc.verify(t, f)
		})
	}
}

func TestServerFramesEncodeToExpectedShape(t *testing.T) {
	cases := []struct {
		typ     Type
		payload any
		wantKey string
	}{
		{TypeWelcome, Welcome{SessionID: "s1", GatewayID: "gw-1"}, "sessionId"},
		{TypePresence, Presence{UserID: "u1", Status: StatusOnline, TS: 1234}, "userId"},
		{TypeJoinOK, JoinOK{PlaceID: "p1", ServerID: "s1"}, "placeId"},
		{TypeJoinDenied, JoinDenied{Reason: ReasonTargetOffline}, "reason"},
		{TypeError, ErrorPayload{Code: CodeBadFrame, Message: "nope"}, "code"},
	}

	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			raw, err := Encode(tc.typ, tc.payload)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}

			var envelope struct {
				Type    Type            `json:"type"`
				Payload json.RawMessage `json:"payload"`
			}
			if err := json.Unmarshal(raw, &envelope); err != nil {
				t.Fatalf("encoded frame is not valid JSON: %v", err)
			}
			if envelope.Type != tc.typ {
				t.Errorf("type: got %q, want %q", envelope.Type, tc.typ)
			}

			var fields map[string]any
			if err := json.Unmarshal(envelope.Payload, &fields); err != nil {
				t.Fatalf("payload is not an object: %v", err)
			}
			if _, ok := fields[tc.wantKey]; !ok {
				t.Errorf("payload missing %q; got keys %v", tc.wantKey, fields)
			}
		})
	}
}

// A PRESENCE event must survive the wire with its timestamp intact. Out-of-order
// rejection compares against this value, so a lost or truncated ts would silently
// disable that check.
func TestPresenceTimestampSurvivesRoundTrip(t *testing.T) {
	const ts = int64(1754661234567)

	raw, err := Encode(TypePresence, Presence{UserID: "u1", Status: StatusAway, TS: ts})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var envelope Frame
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	var p Presence
	if err := json.Unmarshal(envelope.Payload, &p); err != nil {
		t.Fatalf("Unmarshal payload: %v", err)
	}
	if p.TS != ts {
		t.Errorf("ts: got %d, want %d", p.TS, ts)
	}
}

// ---------------------------------------------------------------------------
// Frame-level rejection
// ---------------------------------------------------------------------------

func TestDecodeRejectsMalformedFrames(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode string
	}{
		{"empty", "", CodeBadFrame},
		{"not json", "this is not json", CodeBadFrame},
		{"truncated object", `{"type":"HELLO"`, CodeBadFrame},
		{"json array", `["HELLO"]`, CodeBadFrame},
		{"json string", `"HELLO"`, CodeBadFrame},
		{"json number", `42`, CodeBadFrame},
		{"json null", `null`, CodeMissingField},
		{"empty object", `{}`, CodeMissingField},
		{"empty type", `{"type":""}`, CodeMissingField},
		{"type wrong json type", `{"type":123}`, CodeBadFrame},
		{"unknown type", `{"type":"NONSENSE"}`, CodeUnknownType},
		{"lowercase type", `{"type":"hello"}`, CodeUnknownType},
		{"type with whitespace", `{"type":"HELLO "}`, CodeUnknownType},
		{"server type from client", `{"type":"PRESENCE"}`, CodeUnknownType},
		{"welcome from client", `{"type":"WELCOME"}`, CodeUnknownType},
		{"error from client", `{"type":"ERROR"}`, CodeUnknownType},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Decode([]byte(tc.raw))
			assertRejected(t, err, tc.wantCode)
		})
	}
}

// The size cap must be enforced before parsing, otherwise the allocation it
// exists to prevent has already happened.
func TestDecodeRejectsOversizedFrames(t *testing.T) {
	// Valid JSON, just too big — proves the rejection is about size, not shape.
	huge := `{"type":"SUBSCRIBE","payload":{"userIds":["` + strings.Repeat("x", MaxFrameBytes) + `"]}}`
	if len(huge) <= MaxFrameBytes {
		t.Fatalf("test fixture is only %d bytes, needs to exceed %d", len(huge), MaxFrameBytes)
	}

	_, err := Decode([]byte(huge))
	assertRejected(t, err, CodeFrameTooLarge)
}

func TestDecodeAcceptsFrameExactlyAtLimit(t *testing.T) {
	// Boundary check: the cap is inclusive, so a frame of exactly MaxFrameBytes
	// must be accepted. An off-by-one here would reject legitimate large
	// SUBSCRIBE lists.
	prefix := `{"type":"JOIN","payload":{"targetUserId":"u"},"pad":"`
	suffix := `"}`
	padding := MaxFrameBytes - len(prefix) - len(suffix)
	raw := prefix + strings.Repeat("x", padding) + suffix

	if len(raw) != MaxFrameBytes {
		t.Fatalf("fixture is %d bytes, want exactly %d", len(raw), MaxFrameBytes)
	}
	if _, err := Decode([]byte(raw)); err != nil {
		t.Errorf("frame of exactly %d bytes was rejected: %v", MaxFrameBytes, err)
	}
}

func TestDecodeRejectsInvalidUTF8(t *testing.T) {
	raw := []byte(`{"type":"HELLO","payload":{"userId":"` + "\xff\xfe" + `","token":"t"}}`)
	_, err := Decode(raw)
	assertRejected(t, err, CodeBadFrame)
}

// ---------------------------------------------------------------------------
// Payload-level rejection
// ---------------------------------------------------------------------------

func TestDecodeHelloRejectsBadPayloads(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode string
	}{
		{"no payload", `{"type":"HELLO"}`, CodeMissingField},
		{"null payload", `{"type":"HELLO","payload":null}`, CodeMissingField},
		{"payload not object", `{"type":"HELLO","payload":"nope"}`, CodeBadFrame},
		{"missing userId", `{"type":"HELLO","payload":{"token":"t"}}`, CodeMissingField},
		{"empty userId", `{"type":"HELLO","payload":{"userId":"","token":"t"}}`, CodeMissingField},
		{"missing token", `{"type":"HELLO","payload":{"userId":"u"}}`, CodeMissingField},
		{"empty token", `{"type":"HELLO","payload":{"userId":"u","token":""}}`, CodeMissingField},
		{"userId wrong type", `{"type":"HELLO","payload":{"userId":42,"token":"t"}}`, CodeBadFrame},
		{"userId with colon", `{"type":"HELLO","payload":{"userId":"a:b","token":"t"}}`, CodeInvalidField},
		{"userId with newline", `{"type":"HELLO","payload":{"userId":"a\nb","token":"t"}}`, CodeInvalidField},
		{"userId with space", `{"type":"HELLO","payload":{"userId":"a b","token":"t"}}`, CodeInvalidField},
		{"userId with null byte", `{"type":"HELLO","payload":{"userId":"a\u0000b","token":"t"}}`, CodeInvalidField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Decode([]byte(tc.raw))
			if err != nil {
				assertRejected(t, err, tc.wantCode)
				return
			}
			_, err = DecodeHello(f)
			assertRejected(t, err, tc.wantCode)
		})
	}
}

// Identifiers become Redis key fragments and pub/sub channel names. An
// over-long one inflates the keyspace; a colon lets a client forge Beacon's own
// namespacing.
func TestOversizedIdentifiersAreRejected(t *testing.T) {
	long := strings.Repeat("u", MaxIDBytes+1)

	f := mustDecode(t, `{"type":"HELLO","payload":{"userId":"`+long+`","token":"t"}}`)
	_, err := DecodeHello(f)
	assertRejected(t, err, CodeInvalidField)

	f = mustDecode(t, `{"type":"HELLO","payload":{"userId":"u","token":"`+long+`"}}`)
	_, err = DecodeHello(f)
	assertRejected(t, err, CodeInvalidField)

	f = mustDecode(t, `{"type":"JOIN","payload":{"targetUserId":"`+long+`"}}`)
	_, err = DecodeJoin(f)
	assertRejected(t, err, CodeInvalidField)
}

func TestIdentifierAtExactLimitIsAccepted(t *testing.T) {
	atLimit := strings.Repeat("u", MaxIDBytes)

	f := mustDecode(t, `{"type":"JOIN","payload":{"targetUserId":"`+atLimit+`"}}`)
	if _, err := DecodeJoin(f); err != nil {
		t.Errorf("identifier of exactly %d bytes rejected: %v", MaxIDBytes, err)
	}
}

func TestDecodeSubscribeRejectsBadPayloads(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode string
	}{
		{"no payload", `{"type":"SUBSCRIBE"}`, CodeMissingField},
		{"missing userIds", `{"type":"SUBSCRIBE","payload":{}}`, CodeMissingField},
		{"empty list", `{"type":"SUBSCRIBE","payload":{"userIds":[]}}`, CodeMissingField},
		{"null list", `{"type":"SUBSCRIBE","payload":{"userIds":null}}`, CodeMissingField},
		{"list of numbers", `{"type":"SUBSCRIBE","payload":{"userIds":[1,2]}}`, CodeBadFrame},
		{"entry empty", `{"type":"SUBSCRIBE","payload":{"userIds":["a",""]}}`, CodeMissingField},
		{"entry with colon", `{"type":"SUBSCRIBE","payload":{"userIds":["a","b:c"]}}`, CodeInvalidField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Decode([]byte(tc.raw))
			if err != nil {
				assertRejected(t, err, tc.wantCode)
				return
			}
			_, err = DecodeSubscribe(f)
			assertRejected(t, err, tc.wantCode)
		})
	}
}

// Each subscribed user costs a Redis pub/sub subscription on the serving
// gateway, so an unbounded list is a cheap way to make a gateway do expensive
// work.
func TestSubscribeListIsBounded(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"type":"SUBSCRIBE","payload":{"userIds":[`)
	for i := 0; i < MaxSubscribeUsers+1; i++ {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString(`"u`)
		sb.WriteString(strings.Repeat("0", 1))
		sb.WriteString(itoa(i))
		sb.WriteString(`"`)
	}
	sb.WriteString(`]}}`)

	// Validation is two-stage: Decode checks the envelope, DecodeSubscribe checks
	// the list. A frame this size is legal at the envelope level, so the count
	// limit is what must reject it. If the fixture also happens to exceed the
	// byte cap, the frame limit firing first is equally correct.
	f, err := Decode([]byte(sb.String()))
	if err != nil {
		if code := CodeOf(err); code != CodeFrameTooLarge {
			t.Fatalf("unexpected envelope rejection %s: %v", code, err)
		}
		return
	}

	_, err = DecodeSubscribe(f)
	assertRejected(t, err, CodeInvalidField)
}

func TestSubscribeDeduplicatesPreservingOrder(t *testing.T) {
	f := mustDecode(t, `{"type":"SUBSCRIBE","payload":{"userIds":["c","a","c","b","a"]}}`)

	s, err := DecodeSubscribe(f)
	if err != nil {
		t.Fatalf("DecodeSubscribe: %v", err)
	}

	want := []string{"c", "a", "b"}
	if len(s.UserIDs) != len(want) {
		t.Fatalf("userIds: got %v, want %v", s.UserIDs, want)
	}
	for i := range want {
		if s.UserIDs[i] != want[i] {
			t.Fatalf("userIds: got %v, want %v", s.UserIDs, want)
		}
	}
}

func TestDecodeSetPresenceRejectsBadPayloads(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode string
	}{
		{"no payload", `{"type":"SET_PRESENCE"}`, CodeMissingField},
		{"missing status", `{"type":"SET_PRESENCE","payload":{"placeId":"p"}}`, CodeMissingField},
		{"empty status", `{"type":"SET_PRESENCE","payload":{"status":""}}`, CodeMissingField},
		{"unknown status", `{"type":"SET_PRESENCE","payload":{"status":"PARTYING"}}`, CodeInvalidField},
		{"lowercase status", `{"type":"SET_PRESENCE","payload":{"status":"online"}}`, CodeInvalidField},
		{"status wrong type", `{"type":"SET_PRESENCE","payload":{"status":7}}`, CodeBadFrame},
		{"placeId with colon", `{"type":"SET_PRESENCE","payload":{"status":"ONLINE","placeId":"a:b"}}`, CodeInvalidField},
		{"serverId with control char", `{"type":"SET_PRESENCE","payload":{"status":"ONLINE","serverId":"a\u0001b"}}`, CodeInvalidField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Decode([]byte(tc.raw))
			if err != nil {
				assertRejected(t, err, tc.wantCode)
				return
			}
			_, err = DecodeSetPresence(f)
			assertRejected(t, err, tc.wantCode)
		})
	}
}

func TestSetPresenceAcceptsEveryValidStatus(t *testing.T) {
	for _, s := range []Status{StatusOnline, StatusOffline, StatusAway, StatusInGame} {
		t.Run(string(s), func(t *testing.T) {
			f := mustDecode(t, `{"type":"SET_PRESENCE","payload":{"status":"`+string(s)+`"}}`)
			got, err := DecodeSetPresence(f)
			if err != nil {
				t.Fatalf("status %q rejected: %v", s, err)
			}
			if got.Status != s {
				t.Errorf("status: got %q, want %q", got.Status, s)
			}
		})
	}
}

// Location fields are optional. IN_GAME with no place is a client that is
// launching, which is a valid state that JOIN will simply deny.
func TestSetPresenceAllowsOmittedLocation(t *testing.T) {
	f := mustDecode(t, `{"type":"SET_PRESENCE","payload":{"status":"IN_GAME"}}`)
	if _, err := DecodeSetPresence(f); err != nil {
		t.Errorf("IN_GAME without placeId was rejected: %v", err)
	}
}

func TestDecodeJoinRejectsBadPayloads(t *testing.T) {
	cases := []struct {
		name     string
		raw      string
		wantCode string
	}{
		{"no payload", `{"type":"JOIN"}`, CodeMissingField},
		{"missing target", `{"type":"JOIN","payload":{}}`, CodeMissingField},
		{"empty target", `{"type":"JOIN","payload":{"targetUserId":""}}`, CodeMissingField},
		{"target wrong type", `{"type":"JOIN","payload":{"targetUserId":[1]}}`, CodeBadFrame},
		{"target with colon", `{"type":"JOIN","payload":{"targetUserId":"a:b"}}`, CodeInvalidField},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := Decode([]byte(tc.raw))
			if err != nil {
				assertRejected(t, err, tc.wantCode)
				return
			}
			_, err = DecodeJoin(f)
			assertRejected(t, err, tc.wantCode)
		})
	}
}

// HEARTBEAT is the highest-volume frame. Accepting the three shapes a client
// might reasonably send avoids rejecting traffic over a formatting nicety.
func TestHeartbeatAcceptsAbsentNullAndEmptyPayloads(t *testing.T) {
	for _, raw := range []string{
		`{"type":"HEARTBEAT"}`,
		`{"type":"HEARTBEAT","payload":null}`,
		`{"type":"HEARTBEAT","payload":{}}`,
	} {
		f := mustDecode(t, raw)
		if _, err := DecodeHeartbeat(f); err != nil {
			t.Errorf("DecodeHeartbeat(%s): %v", raw, err)
		}
	}
}

func TestHeartbeatRejectsNonObjectPayload(t *testing.T) {
	f := mustDecode(t, `{"type":"HEARTBEAT","payload":"tick"}`)
	_, err := DecodeHeartbeat(f)
	assertRejected(t, err, CodeBadFrame)
}

// ---------------------------------------------------------------------------
// Robustness
// ---------------------------------------------------------------------------

// The decoder is the only thing standing between untrusted bytes and a process
// holding thousands of connections. It must reject, never panic.
func TestDecodeNeverPanics(t *testing.T) {
	inputs := [][]byte{
		nil,
		{},
		{0x00},
		{0xff, 0xfe, 0xfd},
		[]byte("{"),
		[]byte("}"),
		[]byte("[]"),
		[]byte(`{"type":`),
		[]byte(`{"type":"HELLO","payload":`),
		[]byte(`{"type":"HELLO","payload":{`),
		[]byte(`{"payload":{},"type":`),
		[]byte("\x00\x00\x00\x00"),
		[]byte(strings.Repeat("{", 5000)),
		[]byte(strings.Repeat(`{"a":`, 2000)),
		[]byte(`{"type":"HELLO","payload":{"userId":"\ud800"}}`),
		[]byte(`{"type":"SUBSCRIBE","payload":{"userIds":[[[[]]]]}}`),
	}

	for i, in := range inputs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("input %d panicked: %v", i, r)
				}
			}()
			f, err := Decode(in)
			if err != nil {
				return
			}
			// If it decoded, every payload decoder must also survive it.
			_, _ = DecodeHello(f)
			_, _ = DecodeSubscribe(f)
			_, _ = DecodeHeartbeat(f)
			_, _ = DecodeSetPresence(f)
			_, _ = DecodeJoin(f)
		}()
	}
}

func TestPayloadDecodersRejectNilFrame(t *testing.T) {
	if _, err := DecodeHello(nil); err == nil {
		t.Error("DecodeHello(nil) returned no error")
	}
	if _, err := DecodeSubscribe(nil); err == nil {
		t.Error("DecodeSubscribe(nil) returned no error")
	}
	if _, err := DecodeJoin(nil); err == nil {
		t.Error("DecodeJoin(nil) returned no error")
	}
	if _, err := DecodeSetPresence(nil); err == nil {
		t.Error("DecodeSetPresence(nil) returned no error")
	}
}

// Parser errors must not reach the client: they echo fragments of the input and
// disclose the parser in use.
func TestRejectionMessagesDoNotEchoInput(t *testing.T) {
	secret := "SUPERSECRETVALUE"
	_, err := Decode([]byte(`{"type":"` + secret + `x`))
	if err == nil {
		t.Fatal("expected rejection")
	}
	if strings.Contains(err.Error(), secret) {
		t.Errorf("rejection message echoed input: %v", err)
	}
}

func TestCodeOfUnknownErrorIsInternal(t *testing.T) {
	if got := CodeOf(errString("some internal failure")); got != CodeInternal {
		t.Errorf("CodeOf(non-validation error): got %s, want %s", got, CodeInternal)
	}
	if got := CodeOf(nil); got != CodeInternal {
		t.Errorf("CodeOf(nil): got %s, want %s", got, CodeInternal)
	}
}

func TestMustEncodeErrorAlwaysProducesDecodableFrame(t *testing.T) {
	raw := MustEncodeError(CodeUnauthorized, "bad token")

	var f Frame
	if err := json.Unmarshal(raw, &f); err != nil {
		t.Fatalf("ERROR frame is not valid JSON: %v", err)
	}
	if f.Type != TypeError {
		t.Errorf("type: got %q, want %q", f.Type, TypeError)
	}

	var p ErrorPayload
	if err := json.Unmarshal(f.Payload, &p); err != nil {
		t.Fatalf("ERROR payload is not decodable: %v", err)
	}
	if p.Code != CodeUnauthorized {
		t.Errorf("code: got %q, want %q", p.Code, CodeUnauthorized)
	}
}

func TestValidStatus(t *testing.T) {
	for _, s := range []Status{StatusOnline, StatusOffline, StatusAway, StatusInGame} {
		if !ValidStatus(s) {
			t.Errorf("ValidStatus(%q): got false, want true", s)
		}
	}
	for _, s := range []Status{"", "online", "BUSY", "IN GAME"} {
		if ValidStatus(s) {
			t.Errorf("ValidStatus(%q): got true, want false", s)
		}
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

type errString string

func (e errString) Error() string { return string(e) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func FuzzDecode(f *testing.F) {
	seeds := []string{
		`{"type":"HELLO","payload":{"userId":"u","token":"t"}}`,
		`{"type":"SUBSCRIBE","payload":{"userIds":["a"]}}`,
		`{"type":"HEARTBEAT"}`,
		`{"type":"SET_PRESENCE","payload":{"status":"ONLINE"}}`,
		`{"type":"JOIN","payload":{"targetUserId":"u"}}`,
		`{}`,
		`null`,
		``,
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}

	f.Fuzz(func(t *testing.T, raw []byte) {
		frame, err := Decode(raw)
		if err != nil {
			return
		}
		if frame == nil {
			t.Fatal("Decode returned nil frame with nil error")
		}
		if frame.Type == "" {
			t.Fatal("Decode accepted a frame with an empty type")
		}
		if !isClientType(frame.Type) {
			t.Fatalf("Decode accepted a non-client type %q", frame.Type)
		}
		_, _ = DecodeHello(frame)
		_, _ = DecodeSubscribe(frame)
		_, _ = DecodeHeartbeat(frame)
		_, _ = DecodeSetPresence(frame)
		_, _ = DecodeJoin(frame)
	})
}

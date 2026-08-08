// Package protocol defines the JSON frame types exchanged over the client
// WebSocket and the codec that validates them.
//
// Validation is a first-class concern rather than an afterthought: every frame
// arriving from a client is untrusted input. The codec rejects malformed JSON,
// unknown message types, missing required fields and oversized payloads, and it
// must never panic on any byte sequence. Each rejection path increments a
// dedicated metric so bad input is visible rather than silent.
//
// Implemented in step 3.
package protocol

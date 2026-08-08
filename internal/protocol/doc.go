// Package protocol defines the JSON frame types exchanged over the client
// WebSocket and the codec that validates them.
//
// Validation is a first-class concern rather than an afterthought: every frame
// arriving from a client is untrusted input. The codec rejects malformed JSON,
// unknown message types, missing required fields and oversized payloads, and it
// must never panic on any byte sequence. Each rejection path increments a
// dedicated metric so bad input is visible rather than silent.
//
// Validation is two-stage. Decode checks the envelope — size, encoding, JSON
// well-formedness, and whether the type is one clients may send. The per-type
// decoders then check the payload. Splitting them means an unknown type costs
// one small unmarshal rather than a speculative decode into every shape.
package protocol

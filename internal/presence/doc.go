// Package presence owns session state and the integrity checks that defend it.
//
// Session state lives in Redis, not in gateway memory, so any replica can answer
// a JOIN for a user connected to a different replica. Presence changes fan out
// through Redis pub/sub on per-user channels; a gateway subscribes only to the
// channels its own connected clients have asked for.
//
// The package also implements the checks that keep that shared state honest:
// duplicate-session eviction, out-of-order event rejection, the stale-session
// reaper, orphan detection for sessions whose gateway has vanished, and periodic
// drift reconciliation between in-memory connection counts and Redis
// cardinality. Each check exports its own metric; violations are surfaced, not
// swallowed.
//
// Implemented in step 5.
package presence

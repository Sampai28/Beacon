// Package ring implements the consistent hash ring that maps user shards to
// gateway nodes.
//
// The ring's job in Beacon is narrow and specific: it decides which single live
// gateway owns stale-session reaping for a given user. Without it, all N
// replicas would independently scan for expired sessions and race to delete
// them, doing N times the work and producing duplicate OFFLINE transitions.
//
// The ring is not used to route client connections — any client may connect to
// any gateway, and session state is shared through Redis rather than pinned to
// a node. Ownership is a cleanup concern only.
//
// Implemented in step 2. The package exists here so the module tree compiles
// and the layout is fixed before any dependency is introduced.
package ring

package presence

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Registry tracks which gateways are alive.
//
// Liveness is a TTL, not a flag. Each gateway writes a key with a short
// expiry and refreshes it on a timer; a node that dies stops refreshing and
// disappears on its own. Nothing has to detect the failure, announce it, or
// agree about it — which means there is no failover path that can itself fail.
//
// The registry doubles as the source of per-gateway connection counts, because
// the drift reconciler needs the cluster-wide in-memory total and the heartbeat
// is already a periodic write from every node.
type Registry struct {
	rdb redis.UniversalClient
	ttl time.Duration
}

// NewRegistry returns a registry whose node keys expire after ttl.
//
// ttl must be comfortably longer than the heartbeat interval. Too tight and a
// GC pause or a slow Redis round trip evicts a healthy gateway from the ring,
// causing a needless shard rebalance; too loose and a genuinely dead node keeps
// owning shards nobody reaps. The service wires this at 3x the heartbeat period.
func NewRegistry(rdb redis.UniversalClient, ttl time.Duration) *Registry {
	return &Registry{rdb: rdb, ttl: ttl}
}

// TTL is how long a node remains live without heartbeating.
func (r *Registry) TTL() time.Duration { return r.ttl }

// Heartbeat announces this gateway as live and publishes its current connection
// count. The set membership is written every time rather than once at startup so
// a registry flushed out from under the cluster repairs itself on the next tick.
func (r *Registry) Heartbeat(ctx context.Context, gatewayID string, connections int64) error {
	pipe := r.rdb.Pipeline()
	pipe.Set(ctx, NodeKey(gatewayID), strconv.FormatInt(connections, 10), r.ttl)
	pipe.SAdd(ctx, nodeSetKey, gatewayID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("registry heartbeat: %w", err)
	}
	return nil
}

// Node is one gateway's registry entry.
type Node struct {
	GatewayID   string
	Connections int64
}

// LiveNodes returns the gateways currently heartbeating.
//
// The node set can outlive its members: a TTL key vanishes on its own but set
// membership does not, so a dead gateway leaves an entry behind. Those are
// pruned here, by whichever node notices first. Pruning is idempotent, so
// several gateways racing to clean up the same corpse is harmless.
func (r *Registry) LiveNodes(ctx context.Context) ([]Node, error) {
	ids, err := r.rdb.SMembers(ctx, nodeSetKey).Result()
	if err != nil {
		return nil, fmt.Errorf("registry members: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	pipe := r.rdb.Pipeline()
	cmds := make(map[string]*redis.StringCmd, len(ids))
	for _, id := range ids {
		cmds[id] = pipe.Get(ctx, NodeKey(id))
	}
	// redis.Nil here means "this node's key expired", which is the answer we
	// came for rather than a failure.
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("registry read: %w", err)
	}

	live := make([]Node, 0, len(ids))
	var stale []string
	for id, cmd := range cmds {
		raw, err := cmd.Result()
		if err != nil {
			stale = append(stale, id)
			continue
		}
		conns, _ := strconv.ParseInt(raw, 10, 64)
		live = append(live, Node{GatewayID: id, Connections: conns})
	}

	if len(stale) > 0 {
		// Best effort. If this fails the entries are simply pruned on a later
		// pass; they are already excluded from the returned membership.
		_ = r.rdb.SRem(ctx, nodeSetKey, toAny(stale)...).Err()
	}

	return live, nil
}

// LiveNodeIDs is LiveNodes reduced to the identifiers the ring needs.
func (r *Registry) LiveNodeIDs(ctx context.Context) ([]string, error) {
	nodes, err := r.LiveNodes(ctx)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(nodes))
	for _, n := range nodes {
		ids = append(ids, n.GatewayID)
	}
	return ids, nil
}

// TotalConnections sums the connection counts every live gateway last reported.
// This is the in-memory side of the drift comparison.
func (r *Registry) TotalConnections(ctx context.Context) (int64, error) {
	nodes, err := r.LiveNodes(ctx)
	if err != nil {
		return 0, err
	}
	var total int64
	for _, n := range nodes {
		total += n.Connections
	}
	return total, nil
}

// Deregister removes this gateway immediately rather than waiting for its TTL.
// Called during graceful shutdown so a planned restart rebalances the ring at
// once instead of leaving shards unowned for a TTL window.
func (r *Registry) Deregister(ctx context.Context, gatewayID string) error {
	pipe := r.rdb.Pipeline()
	pipe.Del(ctx, NodeKey(gatewayID))
	pipe.SRem(ctx, nodeSetKey, gatewayID)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("deregister: %w", err)
	}
	return nil
}

func toAny(ss []string) []any {
	out := make([]any, len(ss))
	for i, s := range ss {
		out[i] = s
	}
	return out
}

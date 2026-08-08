// Package metrics declares every Prometheus collector Beacon exports.
//
// Collectors are defined centrally rather than beside their call sites so that
// the Grafana dashboards checked into deploy/ have a single authoritative list
// of metric names to bind against, and so a renamed metric breaks compilation
// instead of silently emptying a panel.
//
// Collectors carry a gateway_id constant label. Prometheus supplies an
// `instance` label from the scrape target, but that is the container address
// and changes when a replica restarts; gateway_id is the same identity the ring
// and session hashes use, so it is what the dashboards join on.
package metrics

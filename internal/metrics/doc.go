// Package metrics declares every Prometheus collector Beacon exports.
//
// Collectors are defined centrally rather than beside their call sites so that
// the Grafana dashboards checked into deploy/ have a single authoritative list
// of metric names to bind against, and so a renamed metric breaks compilation
// instead of silently emptying a panel.
//
// Implemented in step 4, ahead of the presence package that increments them.
package metrics

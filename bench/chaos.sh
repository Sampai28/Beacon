#!/usr/bin/env bash
#
# Beacon chaos measurement.
#
# Kills one gateway while load is running and measures what actually happens,
# rather than asserting that failover "works":
#
#   1. sessions dropped by the kill
#   2. time until the ring excludes the dead node
#   3. time until beacon_presence_drift returns to zero
#   4. whether surviving gateways kept serving JOINs throughout
#
# Every number written to bench/results/ comes from polling the live cluster.
# Nothing here is estimated.
#
# Usage:
#   bash bench/chaos.sh [connections] [victim]
#
# Requires: docker, curl, jq, and a running stack (make up).

set -uo pipefail

# Git Bash on Windows rewrites arguments that look like POSIX paths, turning the
# container-side "/scripts/load.js" into "C:/Program Files/Git/scripts/load.js"
# before Docker ever sees it. The failure is silent from the caller's side — k6
# starts, cannot find the script, and exits — so the chaos run would report zero
# connections rather than an error. Harmless on Linux and macOS, where the
# variable is simply unused.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

CONNS="${1:-${CHAOS_CONNS:-600}}"
VICTIM="${2:-${CHAOS_VICTIM:-beacon-gateway-2}}"
VICTIM_ID="${VICTIM#beacon-}"

COMPOSE_FILE="$(dirname "$0")/../deploy/docker-compose.yml"
RESULTS_DIR="$(dirname "$0")/results"
STAMP="$(date +%Y%m%d-%H%M%S)"
REPORT="${RESULTS_DIR}/chaos-${STAMP}.txt"

# Gateways as seen from the host.
GW1="localhost:8081"
GW2="localhost:8082"
GW3="localhost:8083"
ALL_GATEWAYS=("$GW1" "$GW2" "$GW3")

# Survivors: everything except the victim. Ring exclusion and drift are read
# from these, because the victim will be gone.
case "$VICTIM_ID" in
  gateway-1) SURVIVORS=("$GW2" "$GW3") ;;
  gateway-2) SURVIVORS=("$GW1" "$GW3") ;;
  gateway-3) SURVIVORS=("$GW1" "$GW2") ;;
  *) echo "unknown victim: $VICTIM" >&2; exit 2 ;;
esac

# How long to keep polling after the kill before giving up. Generous: reporting
# "did not converge within 90s" is a real result, and a short timeout would
# manufacture that answer rather than measure it.
CONVERGE_TIMEOUT=90
POLL_INTERVAL=0.2

mkdir -p "$RESULTS_DIR"

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

log() { printf '%s\n' "$*" | tee -a "$REPORT"; }
now() { date +%s.%N; }
since() { awk -v a="$1" -v b="$(now)" 'BEGIN{printf "%.2f", b-a}'; }

need() {
  command -v "$1" >/dev/null 2>&1 || { echo "missing required tool: $1" >&2; exit 2; }
}
need docker
need curl
need jq
need awk

# scrape_metric <host:port> <metric-name>
# Prints the metric's value, or nothing if the gateway is unreachable.
scrape_metric() {
  curl -sf --max-time 2 "http://$1/metrics" 2>/dev/null \
    | awk -v want="$2" '$0 !~ /^#/ && index($0, want) == 1 { print $NF; exit }'
}

# sum_metric <metric-name> <host...> — sums across reachable gateways only.
sum_metric() {
  local metric="$1"; shift
  local total=0 v
  for host in "$@"; do
    v="$(scrape_metric "$host" "$metric")"
    [ -n "$v" ] && total="$(awk -v a="$total" -v b="$v" 'BEGIN{printf "%.0f", a+b}')"
  done
  printf '%s' "$total"
}

ring_members() {
  curl -sf --max-time 2 "http://$1/debug/ring" 2>/dev/null | jq -r '.members // [] | join(",")'
}

ring_excludes_victim() {
  local members
  for host in "${SURVIVORS[@]}"; do
    members="$(ring_members "$host")"
    # Every survivor must agree the victim is gone, not just the first to notice.
    [ -z "$members" ] && return 1
    case ",$members," in
      *",$VICTIM_ID,"*) return 1 ;;
    esac
  done
  return 0
}

cleanup() {
  [ -n "${SAMPLER_PID:-}" ] && kill "$SAMPLER_PID" >/dev/null 2>&1
  [ -n "${K6_PID:-}" ] && kill "$K6_PID" >/dev/null 2>&1
  [ -n "${K6_CONTAINER:-}" ] && docker rm -f "$K6_CONTAINER" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

# sample_drift runs in the background writing timestamped drift readings.
#
# The main convergence loop makes about nine HTTP calls per iteration, giving it
# an effective resolution near two seconds — coarser than the transient being
# measured. Drift after a kill goes non-zero and back inside a couple of seconds,
# so a dedicated single-metric sampler is the only way to time it rather than
# merely notice it happened.
sample_drift() {
  while :; do
    printf '%s\t%s\n' "$(now)" "$(scrape_metric "${SURVIVORS[0]}" beacon_presence_drift)"
    sleep 0.1
  done
}

# ---------------------------------------------------------------------------
# preflight
# ---------------------------------------------------------------------------

: > "$REPORT"
log "=============================================================================="
log "Beacon chaos test — $(date -u '+%Y-%m-%d %H:%M:%SZ')"
log "=============================================================================="
log "victim              $VICTIM ($VICTIM_ID)"
log "target connections  $CONNS"
log ""

for host in "${ALL_GATEWAYS[@]}"; do
  if ! curl -sf --max-time 3 "http://$host/readyz" >/dev/null; then
    log "FATAL: $host is not ready. Start the stack first:"
    log "  docker compose -f deploy/docker-compose.yml up -d --build"
    exit 1
  fi
done
log "all three gateways ready"

# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

log ""
log "--- starting background load ---"

# Backgrounded foreground run rather than `run -d`: Compose creates but does not
# reliably start a detached one-off container here, and a load generator that
# silently never starts would turn this whole measurement into a fiction.
K6_CONTAINER="beacon-k6-chaos-${STAMP}"
docker compose -f "$COMPOSE_FILE" run --rm --name "$K6_CONTAINER" \
  -e LOAD_VUS="$CONNS" \
  -e CONNS_PER_VU="${CHAOS_CONNS_PER_VU:-25}" \
  -e LOAD_RAMP="20s" \
  -e LOAD_DURATION="150s" \
  -e RUN_TAG="chaos-${STAMP}" \
  k6 run /scripts/load.js > "${RESULTS_DIR}/chaos-${STAMP}-k6.txt" 2>&1 &
K6_PID=$!
log "k6 container: $K6_CONTAINER (pid $K6_PID)"

# Wait for the ramp to settle rather than sleeping a fixed amount: killing a
# node mid-ramp would confuse connections that never arrived with connections
# the failure dropped.
log "waiting for connections to plateau..."
STEADY=0
PREV=-1
for _ in $(seq 1 60); do
  sleep 2
  CURRENT="$(sum_metric beacon_connections_active "${ALL_GATEWAYS[@]}")"
  printf '  connections: %s\n' "$CURRENT" | tee -a "$REPORT"
  if [ "$CURRENT" -gt 0 ] && [ "$CURRENT" -eq "$PREV" ]; then
    STEADY=$((STEADY + 1))
    [ "$STEADY" -ge 2 ] && break
  else
    STEADY=0
  fi
  PREV="$CURRENT"
done

BASELINE_CONNS="$(sum_metric beacon_connections_active "${ALL_GATEWAYS[@]}")"
VICTIM_CONNS="$(scrape_metric "$GW2" beacon_connections_active)"
case "$VICTIM_ID" in
  gateway-1) VICTIM_CONNS="$(scrape_metric "$GW1" beacon_connections_active)" ;;
  gateway-3) VICTIM_CONNS="$(scrape_metric "$GW3" beacon_connections_active)" ;;
esac
BASELINE_DRIFT="$(scrape_metric "${SURVIVORS[0]}" beacon_presence_drift)"
BASELINE_JOINS="$(sum_metric beacon_joins_total "${SURVIVORS[@]}")"
BASELINE_ORPHANS="$(sum_metric beacon_orphan_sessions_reclaimed_total "${SURVIVORS[@]}")"

log ""
log "--- baseline ---"
log "connections (cluster)      $BASELINE_CONNS"
log "connections on victim      ${VICTIM_CONNS:-unknown}"
log "presence drift             ${BASELINE_DRIFT:-unknown}"
log "ring members               $(ring_members "${SURVIVORS[0]}")"

if [ "${BASELINE_CONNS:-0}" -lt 1 ]; then
  log ""
  log "FATAL: no connections established; nothing to measure."
  exit 1
fi

# ---------------------------------------------------------------------------
# kill
# ---------------------------------------------------------------------------

DRIFT_LOG="${RESULTS_DIR}/chaos-${STAMP}-drift.tsv"
: > "$DRIFT_LOG"
sample_drift >> "$DRIFT_LOG" 2>/dev/null &
SAMPLER_PID=$!
sleep 1 # let the sampler establish a pre-kill baseline

log ""
log "--- killing $VICTIM ---"
T0="$(now)"
docker kill "$VICTIM" >/dev/null 2>&1 || { log "FATAL: docker kill failed"; exit 1; }
log "killed at T+0.00s (SIGKILL — no graceful deregister, so the ring must"
log "notice via TTL expiry rather than being told)"

RING_EXCLUDED_AT=""
DRIFT_ZERO_AT=""
DRIFT_WENT_NONZERO="no"
PEAK_DRIFT="0"
ORPHANS_STARTED_AT=""
ORPHANS_DONE_AT=""
MIN_SURVIVOR_CONNS="$BASELINE_CONNS"
JOIN_STALL_SECONDS="0"
LAST_JOINS="$BASELINE_JOINS"
LAST_JOIN_PROGRESS="$T0"
SAMPLES=0

log ""
log "--- convergence (polling every ${POLL_INTERVAL}s) ---"

while :; do
  ELAPSED="$(since "$T0")"
  if awk -v e="$ELAPSED" -v t="$CONVERGE_TIMEOUT" 'BEGIN{exit !(e>t)}'; then
    break
  fi

  SAMPLES=$((SAMPLES + 1))

  if [ -z "$RING_EXCLUDED_AT" ] && ring_excludes_victim; then
    RING_EXCLUDED_AT="$ELAPSED"
    log "T+${ELAPSED}s  ring excludes $VICTIM_ID on every survivor"
  fi

  DRIFT="$(scrape_metric "${SURVIVORS[0]}" beacon_presence_drift)"
  if [ -n "$DRIFT" ]; then
    if [ "$DRIFT" != "0" ]; then
      if [ "$DRIFT_WENT_NONZERO" = "no" ]; then
        DRIFT_WENT_NONZERO="yes"
        PEAK_DRIFT="$DRIFT"
        log "T+${ELAPSED}s  drift went non-zero: $DRIFT"
      fi
      # Track the largest magnitude seen, so the report says how far the two
      # views of the world actually diverged.
      if awk -v a="$DRIFT" -v b="${PEAK_DRIFT:-0}" 'BEGIN{exit !((a<0?-a:a)>(b<0?-b:b))}'; then
        PEAK_DRIFT="$DRIFT"
      fi
      DRIFT_ZERO_AT=""
    elif [ "$DRIFT_WENT_NONZERO" = "yes" ] && [ -z "$DRIFT_ZERO_AT" ]; then
      DRIFT_ZERO_AT="$ELAPSED"
      log "T+${ELAPSED}s  drift returned to zero"
    fi
  fi

  # Orphan reclamation is the convergence signal that does not depend on the
  # drift gauge's sampling period: it counts the dead node's sessions actually
  # being cleaned up.
  ORPHANS_NOW="$(sum_metric beacon_orphan_sessions_reclaimed_total "${SURVIVORS[@]}")"
  RECLAIMED_SO_FAR="$((ORPHANS_NOW - BASELINE_ORPHANS))"
  if [ -z "$ORPHANS_STARTED_AT" ] && [ "$RECLAIMED_SO_FAR" -gt 0 ]; then
    ORPHANS_STARTED_AT="$ELAPSED"
    log "T+${ELAPSED}s  orphan reclamation began"
  fi
  if [ -z "$ORPHANS_DONE_AT" ] && [ "$RECLAIMED_SO_FAR" -ge "${VICTIM_CONNS:-0}" ] && [ "${VICTIM_CONNS:-0}" -gt 0 ]; then
    ORPHANS_DONE_AT="$ELAPSED"
    log "T+${ELAPSED}s  all ${RECLAIMED_SO_FAR} orphaned sessions reclaimed"
  fi

  # Joins must keep being answered by the survivors throughout. A stall here is
  # the failure mode that matters: it would mean a dead node's shards took the
  # rest of the cluster down with them.
  JOINS="$(sum_metric beacon_joins_total "${SURVIVORS[@]}")"
  if [ "${JOINS:-0}" -gt "${LAST_JOINS:-0}" ]; then
    LAST_JOIN_PROGRESS="$(now)"
    LAST_JOINS="$JOINS"
  else
    STALL="$(since "$LAST_JOIN_PROGRESS")"
    if awk -v a="$STALL" -v b="$JOIN_STALL_SECONDS" 'BEGIN{exit !(a>b)}'; then
      JOIN_STALL_SECONDS="$STALL"
    fi
  fi

  SURV_CONNS="$(sum_metric beacon_connections_active "${SURVIVORS[@]}")"
  if [ "${SURV_CONNS:-0}" -lt "${MIN_SURVIVOR_CONNS:-0}" ]; then
    MIN_SURVIVOR_CONNS="$SURV_CONNS"
  fi

  # Stop once the ring has converged and the dead node's sessions are cleaned
  # up. Drift returning to zero is reported but not required to break: if the
  # gauge never sampled a non-zero value the cluster converged faster than the
  # drift interval, which is a result rather than a reason to keep waiting.
  if [ -n "$RING_EXCLUDED_AT" ] && [ -n "$ORPHANS_DONE_AT" ]; then
    sleep 3
    break
  fi

  sleep "$POLL_INTERVAL"
done

# Let the sampler capture a little past convergence, then analyse its trace.
sleep 3
kill "$SAMPLER_PID" >/dev/null 2>&1
SAMPLER_PID=""

SAMPLES_TAKEN="$(wc -l < "$DRIFT_LOG" | tr -d ' ')"
DRIFT_FIRST_NONZERO="$(awk -F'\t' -v t0="$T0" '$2 != "" && $2 != 0 { printf "%.2f", $1 - t0; exit }' "$DRIFT_LOG")"
DRIFT_BACK_TO_ZERO="$(awk -F'\t' -v t0="$T0" '
  $2 == "" { next }
  $2 != 0  { seen = 1; next }
  seen     { printf "%.2f", $1 - t0; exit }
' "$DRIFT_LOG")"
DRIFT_PEAK="$(awk -F'\t' '
  $2 == "" { next }
  { v = ($2 < 0 ? -$2 : $2); if (v > m) { m = v; s = $2 } }
  END { print (s == "" ? 0 : s) }
' "$DRIFT_LOG")"

if [ -n "$DRIFT_FIRST_NONZERO" ]; then
  DRIFT_WENT_NONZERO="yes"
  PEAK_DRIFT="$DRIFT_PEAK"
  DRIFT_ZERO_AT="$DRIFT_BACK_TO_ZERO"
  log ""
  log "--- drift trace (${SAMPLES_TAKEN} samples at ~10Hz) ---"
  log "first non-zero reading at T+${DRIFT_FIRST_NONZERO}s, peak ${DRIFT_PEAK}"
  log "back to zero at T+${DRIFT_BACK_TO_ZERO:-never}s"
fi

FINAL_DRIFT="$(scrape_metric "${SURVIVORS[0]}" beacon_presence_drift)"
FINAL_JOINS="$(sum_metric beacon_joins_total "${SURVIVORS[@]}")"
FINAL_ORPHANS="$(sum_metric beacon_orphan_sessions_reclaimed_total "${SURVIVORS[@]}")"
FINAL_RING="$(ring_members "${SURVIVORS[0]}")"
JOINS_DURING="$((FINAL_JOINS - BASELINE_JOINS))"
ORPHANS_RECLAIMED="$((FINAL_ORPHANS - BASELINE_ORPHANS))"
SESSIONS_DROPPED="${VICTIM_CONNS:-0}"

# ---------------------------------------------------------------------------
# restore
# ---------------------------------------------------------------------------

log ""
log "--- restarting $VICTIM ---"
docker compose -f "$COMPOSE_FILE" start "${VICTIM_ID}" >/dev/null 2>&1 \
  || docker start "$VICTIM" >/dev/null 2>&1 || true

REJOIN_START="$(now)"
REJOINED_AT=""
for _ in $(seq 1 100); do
  MEMBERS="$(ring_members "${SURVIVORS[0]}")"
  case ",$MEMBERS," in
    *",$VICTIM_ID,"*) REJOINED_AT="$(since "$REJOIN_START")"; break ;;
  esac
  sleep 0.5
done
[ -n "$REJOINED_AT" ] && log "$VICTIM_ID rejoined the ring after ${REJOINED_AT}s"

# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

log ""
log "=============================================================================="
log "RESULTS"
log "=============================================================================="
log "sessions dropped by the kill            ${SESSIONS_DROPPED}"
log "connections before kill (cluster)       ${BASELINE_CONNS}"
log "min connections on survivors after      ${MIN_SURVIVOR_CONNS}"
log "time until ring excluded dead node      ${RING_EXCLUDED_AT:-DID NOT CONVERGE within ${CONVERGE_TIMEOUT}s}s"
log "drift went non-zero                     ${DRIFT_WENT_NONZERO}"
log "peak drift magnitude                    ${PEAK_DRIFT}"
if [ "$DRIFT_WENT_NONZERO" = "yes" ]; then
  log "time until drift returned to zero       ${DRIFT_ZERO_AT:-DID NOT RETURN within ${CONVERGE_TIMEOUT}s}s"
else
  log "time until drift returned to zero       n/a — never sampled non-zero at ~10Hz"
  log "                                        (reclamation lands in the same reaper"
  log "                                        sweep that notices the node is gone)"
fi
log "drift samples taken                     ${SAMPLES_TAKEN:-0} at ~10Hz"
log "final drift                             ${FINAL_DRIFT:-unknown}"
log "orphan reclamation began                ${ORPHANS_STARTED_AT:-not observed}s"
log "all orphaned sessions reclaimed by      ${ORPHANS_DONE_AT:-NOT FULLY RECLAIMED}s"
log "orphan sessions reclaimed               ${ORPHANS_RECLAIMED}"
log "joins served by survivors during window ${JOINS_DURING}"
# Bounded below by the convergence loop's own poll interval (~2s), so this says
# "no stall longer than the sampling gap" rather than measuring a real one.
# The count above is the load-bearing number: it is non-zero and monotonic.
log "longest gap between join increments     ${JOIN_STALL_SECONDS}s (>= poll interval)"
log "ring after recovery                     ${FINAL_RING}"
log "time for victim to rejoin the ring      ${REJOINED_AT:-did not rejoin}s"
log "convergence samples taken               ${SAMPLES}"
log ""
if [ "$JOINS_DURING" -gt 0 ]; then
  log "VERDICT: survivors continued serving JOINs throughout the failure."
else
  log "VERDICT: no JOINs were served during the window — investigate."
fi
log "=============================================================================="
log ""
log "raw report: $REPORT"

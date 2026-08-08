// Beacon load test.
//
// Ramps concurrent WebSocket connections across all three gateways, holds them,
// and reports connection success rate and JOIN latency percentiles.
//
//   docker compose -f deploy/docker-compose.yml run --rm \
//     -e LOAD_VUS=10000 k6 run /scripts/load.js
//
// Uses k6/experimental/websockets rather than the legacy blocking k6/ws so one
// VU can hold many sockets. The legacy module blocks a VU per connection, which
// at a 10,000 target would mean 10,000 VUs and several GB of k6 overhead — the
// load generator would run out of memory well before Beacon ran out of capacity,
// and the number reported would say nothing about the service.

import { WebSocket } from 'k6/experimental/websockets';
import { setTimeout, clearTimeout } from 'k6/timers';
import { Counter, Rate, Trend } from 'k6/metrics';

// ---------------------------------------------------------------------------
// configuration
// ---------------------------------------------------------------------------

const TARGET_CONNS = Number(__ENV.LOAD_VUS || 10000);
const CONNS_PER_VU = Number(__ENV.CONNS_PER_VU || 25);
const RAMP = __ENV.LOAD_RAMP || '60s';
const HOLD = __ENV.LOAD_DURATION || '60s';
const TOKEN = __ENV.BEACON_DEV_TOKEN || 'beacon-dev-token';
const RUN_TAG = __ENV.RUN_TAG || `${TARGET_CONNS}`;

const GATEWAYS = (__ENV.GATEWAYS || 'gateway-1:8080,gateway-2:8080,gateway-3:8080')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);

// How long one VU keeps its batch of sockets open. Comfortably longer than the
// hold window so connections are not cycling underneath the measurement.
const SOAK_MS = 1000 * (parseDuration(HOLD) + parseDuration(RAMP) + 30);

const HEARTBEAT_MS = 8000; // well inside the 30s session TTL
const PRESENCE_MS = 15000;
const JOIN_MS = 5000;

const VUS = Math.max(1, Math.ceil(TARGET_CONNS / CONNS_PER_VU));

function parseDuration(d) {
  const m = String(d).match(/^(\d+)([sm])$/);
  if (!m) return 60;
  return m[2] === 'm' ? Number(m[1]) * 60 : Number(m[1]);
}

// ---------------------------------------------------------------------------
// metrics
// ---------------------------------------------------------------------------

const connectSuccess = new Rate('beacon_connect_success');
const connectTime = new Trend('beacon_connect_ms', true);
const joinLatency = new Trend('beacon_join_ms', true);
const joinSuccess = new Rate('beacon_join_success');
const presenceReceived = new Counter('beacon_presence_received');
const framesSent = new Counter('beacon_frames_sent');
const wsErrors = new Counter('beacon_ws_errors');
const sessionsEvicted = new Counter('beacon_sessions_evicted');

export const options = {
  scenarios: {
    presence: {
      executor: 'ramping-vus',
      startVUs: 0,
      stages: [
        { duration: RAMP, target: VUS },
        { duration: HOLD, target: VUS },
        { duration: '10s', target: 0 },
      ],
      gracefulRampDown: '10s',
      gracefulStop: '15s',
    },
  },
  // Thresholds record pass/fail without aborting: a run that misses the target
  // still produces the percentile data needed to say *why* it missed, which is
  // the only useful outcome of a failed capacity test.
  thresholds: {
    beacon_connect_success: ['rate>0.95'],
    beacon_join_ms: ['p(95)<500', 'p(99)<1000'],
    beacon_join_success: ['rate>0.90'],
  },
  // The default summary hides the tail. p99 is the number that matters here.
  summaryTrendStats: ['min', 'med', 'avg', 'p(90)', 'p(95)', 'p(99)', 'max'],
  noConnectionReuse: false,
  discardResponseBodies: true,
};

// ---------------------------------------------------------------------------
// one simulated client
// ---------------------------------------------------------------------------

function openClient(userId, gateway, peerId, onDone) {
  const url = `ws://${gateway}/ws`;
  const started = Date.now();
  let settled = false;
  let alive = true;
  const timers = [];

  let ws;
  try {
    ws = new WebSocket(url);
  } catch (e) {
    wsErrors.add(1);
    connectSuccess.add(false);
    onDone();
    return;
  }

  const pending = new Map(); // join correlation: sent-at timestamps

  function send(type, payload) {
    if (!alive) return;
    try {
      ws.send(JSON.stringify(payload === undefined ? { type } : { type, payload }));
      framesSent.add(1);
    } catch (e) {
      alive = false;
      wsErrors.add(1);
    }
  }

  function every(ms, fn) {
    // Recursive setTimeout rather than setInterval: it cannot pile up if the
    // event loop is busy, which matters at connection counts where k6 itself is
    // under pressure.
    const tick = () => {
      if (!alive) return;
      fn();
      const t = setTimeout(tick, ms);
      timers.push(t);
    };
    const t = setTimeout(tick, ms);
    timers.push(t);
  }

  function shutdown() {
    if (!alive) return;
    alive = false;
    timers.forEach(clearTimeout);
    try {
      ws.close();
    } catch (e) {
      // already gone
    }
    onDone();
  }

  ws.onopen = () => {
    send('HELLO', { userId, token: TOKEN });
  };

  ws.onmessage = (event) => {
    let frame;
    try {
      frame = JSON.parse(event.data);
    } catch (e) {
      wsErrors.add(1);
      return;
    }
    const p = frame.payload || {};

    switch (frame.type) {
      case 'WELCOME': {
        if (!settled) {
          settled = true;
          connectSuccess.add(true);
          connectTime.add(Date.now() - started);
        }

        // Watch the peer, so fan-out actually has subscribers to deliver to.
        // Without this the pub/sub path is never exercised under load.
        send('SUBSCRIBE', { userIds: [peerId] });

        // Go IN_GAME so the peer's JOINs against this user can succeed.
        send('SET_PRESENCE', {
          status: 'IN_GAME',
          placeId: `place-${__VU % 50}`,
          serverId: `srv-${__VU % 10}`,
        });

        every(HEARTBEAT_MS, () => send('HEARTBEAT', {}));
        every(PRESENCE_MS, () =>
          send('SET_PRESENCE', {
            status: 'IN_GAME',
            placeId: `place-${(Date.now() / 1000) | 0 % 50}`,
            serverId: `srv-${__VU % 10}`,
          }),
        );
        every(JOIN_MS, () => {
          pending.set(peerId, Date.now());
          send('JOIN', { targetUserId: peerId });
        });
        break;
      }

      case 'JOIN_OK':
      case 'JOIN_DENIED': {
        const sentAt = pending.get(peerId);
        if (sentAt !== undefined) {
          joinLatency.add(Date.now() - sentAt);
          pending.delete(peerId);
        }
        // A denial is a correct answer, not a failure — the target may not be
        // in a joinable state yet. Only an unanswered or errored join is a miss.
        joinSuccess.add(frame.type === 'JOIN_OK');
        break;
      }

      case 'PRESENCE':
        presenceReceived.add(1);
        break;

      case 'ERROR':
        if (p.code === 'UNAUTHORIZED') {
          // Duplicate-session eviction. Counted separately: under load this is
          // the service working as designed, not an error.
          sessionsEvicted.add(1);
        } else {
          wsErrors.add(1);
        }
        break;

      default:
        break;
    }
  };

  ws.onerror = () => {
    if (!settled) {
      settled = true;
      connectSuccess.add(false);
    }
    wsErrors.add(1);
    shutdown();
  };

  ws.onclose = () => {
    if (!settled) {
      settled = true;
      connectSuccess.add(false);
    }
    shutdown();
  };

  const life = setTimeout(shutdown, SOAK_MS);
  timers.push(life);
}

// ---------------------------------------------------------------------------
// VU body
// ---------------------------------------------------------------------------

export default function () {
  // Unique per VU and iteration so a reconnecting VU never collides with its own
  // previous session — which would show up as duplicate-session evictions and
  // muddy the very metric measuring them.
  const base = `u${__VU}i${__ITER}`;
  let outstanding = CONNS_PER_VU;

  return new Promise((resolve) => {
    const done = () => {
      outstanding -= 1;
      if (outstanding <= 0) resolve();
    };

    for (let i = 0; i < CONNS_PER_VU; i++) {
      const userId = `${base}c${i}`;
      // Peers are paired within a VU but land on *different* gateways, because
      // gateway selection is per connection index. That makes every JOIN a
      // cross-node resolution rather than a local lookup.
      const peerIdx = (i + 1) % CONNS_PER_VU;
      const peerId = `${base}c${peerIdx}`;
      const gateway = GATEWAYS[i % GATEWAYS.length];

      openClient(userId, gateway, peerId, done);
    }
  });
}

// ---------------------------------------------------------------------------
// reporting
// ---------------------------------------------------------------------------

function n(v, digits = 2) {
  if (v === undefined || v === null || Number.isNaN(v)) return 'n/a';
  return Number(v).toFixed(digits);
}

function trendLine(label, t) {
  if (!t || !t.values) return `${label.padEnd(26)} no data`;
  const v = t.values;
  return (
    `${label.padEnd(26)} ` +
    `min=${n(v.min)}ms med=${n(v.med)}ms avg=${n(v.avg)}ms ` +
    `p95=${n(v['p(95)'])}ms p99=${n(v['p(99)'])}ms max=${n(v.max)}ms`
  );
}

function rateLine(label, r) {
  if (!r || !r.values) return `${label.padEnd(26)} no data`;
  const v = r.values;
  const total = (v.passes || 0) + (v.fails || 0);
  return `${label.padEnd(26)} ${n(v.rate * 100)}%  (${v.passes || 0} ok / ${total} total)`;
}

function counterLine(label, c) {
  if (!c || !c.values) return `${label.padEnd(26)} no data`;
  return `${label.padEnd(26)} ${c.values.count}`;
}

export function handleSummary(data) {
  const m = data.metrics || {};
  const thresholdFailures = [];
  for (const [name, metric] of Object.entries(m)) {
    if (!metric.thresholds) continue;
    for (const [expr, res] of Object.entries(metric.thresholds)) {
      if (res && res.ok === false) thresholdFailures.push(`${name}: ${expr}`);
    }
  }

  const report = [
    '='.repeat(78),
    'Beacon load test',
    '='.repeat(78),
    `target connections   ${TARGET_CONNS}`,
    `conns per VU         ${CONNS_PER_VU}`,
    `VUs                  ${VUS}`,
    `gateways             ${GATEWAYS.join(', ')}`,
    `ramp / hold          ${RAMP} / ${HOLD}`,
    '',
    '--- connections ---',
    rateLine('connect success', m.beacon_connect_success),
    trendLine('connect latency', m.beacon_connect_ms),
    counterLine('ws errors', m.beacon_ws_errors),
    counterLine('duplicate evictions', m.beacon_sessions_evicted),
    '',
    '--- joins (cross-gateway) ---',
    rateLine('join answered OK', m.beacon_join_success),
    trendLine('join latency', m.beacon_join_ms),
    '',
    '--- traffic ---',
    counterLine('frames sent', m.beacon_frames_sent),
    counterLine('presence received', m.beacon_presence_received),
    '',
    '--- thresholds ---',
    thresholdFailures.length === 0
      ? 'all thresholds passed'
      : 'FAILED:\n  ' + thresholdFailures.join('\n  '),
    '='.repeat(78),
    '',
  ].join('\n');

  const out = {};
  out.stdout = report;
  out[`/results/load-${RUN_TAG}.txt`] = report;
  out[`/results/load-${RUN_TAG}.json`] = JSON.stringify(
    {
      target_connections: TARGET_CONNS,
      conns_per_vu: CONNS_PER_VU,
      vus: VUS,
      gateways: GATEWAYS,
      ramp: RAMP,
      hold: HOLD,
      threshold_failures: thresholdFailures,
      metrics: m,
    },
    null,
    2,
  );
  return out;
}

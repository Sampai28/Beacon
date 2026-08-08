// Beacon demo client — vanilla JS, no build step.
//
// The point of this page is to make cross-node fan-out visible: connect two
// tabs to two different gateway ports, and a SET_PRESENCE on one appears as a
// PRESENCE frame on the other without the gateways ever talking directly.

(() => {
  'use strict';

  // Gateway ports published by deploy/docker-compose.yml. Listed explicitly
  // rather than discovered, because the whole demo depends on the user being
  // able to choose *which* node they land on.
  const GATEWAYS = [
    { label: 'gateway-1 (:8081)', port: 8081 },
    { label: 'gateway-2 (:8082)', port: 8082 },
    { label: 'gateway-3 (:8083)', port: 8083 },
  ];

  // Matches BEACON_SESSION_TTL's lower bound with generous margin. The server
  // reaps a session whose heartbeats stop, so the client must keep sending.
  const HEARTBEAT_MS = 10_000;

  const el = (id) => document.getElementById(id);
  const ui = {
    gateway: el('gateway'),
    userId: el('userId'),
    token: el('token'),
    connect: el('connect'),
    disconnect: el('disconnect'),
    status: el('status'),
    statusSelect: el('status-select'),
    placeId: el('placeId'),
    serverId: el('serverId'),
    setPresence: el('set-presence'),
    watchIds: el('watchIds'),
    subscribe: el('subscribe'),
    watched: el('watched'),
    log: el('log'),
    clearLog: el('clear-log'),
    frameCount: el('frame-count'),
  };

  let ws = null;
  let heartbeat = null;
  let sessionId = null;
  let frames = 0;
  const friends = new Map();

  // -- gateway picker -------------------------------------------------------

  GATEWAYS.forEach((gw) => {
    const opt = document.createElement('option');
    opt.value = String(gw.port);
    opt.textContent = gw.label;
    ui.gateway.appendChild(opt);
  });
  // Default to whichever gateway served this page, so opening :8082 and hitting
  // Connect does the obvious thing.
  const servedBy = window.location.port;
  if (GATEWAYS.some((gw) => String(gw.port) === servedBy)) {
    ui.gateway.value = servedBy;
  }

  // -- logging --------------------------------------------------------------

  function log(direction, text, isError) {
    const entry = document.createElement('div');
    entry.className = `entry ${isError ? 'err' : direction}`;

    const time = document.createElement('span');
    time.className = 'time';
    time.textContent = new Date().toLocaleTimeString('en-GB', { hour12: false });

    const dir = document.createElement('span');
    dir.className = 'dir';
    dir.textContent = direction === 'out' ? '↑' : '↓';

    const body = document.createElement('span');
    body.className = 'body';
    body.textContent = text;

    entry.append(time, dir, body);
    ui.log.appendChild(entry);
    ui.log.scrollTop = ui.log.scrollHeight;

    frames += 1;
    ui.frameCount.textContent = `${frames} frame${frames === 1 ? '' : 's'}`;

    // Unbounded logs make the tab unusable during a load test.
    while (ui.log.childElementCount > 400) ui.log.removeChild(ui.log.firstChild);
  }

  function setStatus(text, kind) {
    ui.status.textContent = text;
    ui.status.className = `status ${kind}`;
  }

  // -- protocol -------------------------------------------------------------

  function send(type, payload) {
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const frame = payload === undefined ? { type } : { type, payload };
    ws.send(JSON.stringify(frame));
    log('out', JSON.stringify(frame));
  }

  function connect() {
    const userId = ui.userId.value.trim();
    if (!userId) {
      setStatus('A user ID is required', 'error');
      return;
    }

    const port = ui.gateway.value;
    const scheme = window.location.protocol === 'https:' ? 'wss' : 'ws';
    const url = `${scheme}://${window.location.hostname}:${port}/ws`;

    setStatus(`Connecting to ${url}…`, 'idle');
    ws = new WebSocket(url);

    ws.onopen = () => {
      send('HELLO', { userId, token: ui.token.value });
    };

    ws.onmessage = (event) => {
      log('in', event.data);
      let frame;
      try {
        frame = JSON.parse(event.data);
      } catch {
        return;
      }
      handle(frame);
    };

    ws.onerror = () => setStatus('WebSocket error — is the gateway up?', 'error');

    ws.onclose = (event) => {
      stopHeartbeat();
      sessionId = null;
      setConnected(false);
      const why = event.reason ? ` (${event.reason})` : '';
      setStatus(`Disconnected${why}`, event.wasClean ? 'idle' : 'error');
    };
  }

  function handle(frame) {
    const p = frame.payload || {};
    switch (frame.type) {
      case 'WELCOME':
        sessionId = p.sessionId;
        setConnected(true);
        setStatus(`Connected to ${p.gatewayId} — session ${p.sessionId.slice(0, 8)}…`, 'live');
        startHeartbeat();
        break;

      case 'PRESENCE':
        // A PRESENCE frame naming this user is either their own echo or, if it
        // says OFFLINE while still connected, an eviction by a newer session.
        renderFriend(p);
        break;

      case 'JOIN_OK':
        setStatus(`JOIN_OK — place ${p.placeId} on ${p.serverId}`, 'live');
        break;

      case 'JOIN_DENIED':
        setStatus(`JOIN_DENIED — ${p.reason}`, 'error');
        break;

      case 'ERROR':
        setStatus(`ERROR ${p.code} — ${p.message}`, 'error');
        break;

      case 'ACK':
      default:
        break;
    }
  }

  function disconnect() {
    stopHeartbeat();
    if (ws) ws.close(1000, 'client disconnect');
  }

  // The server reaps sessions whose heartbeat TTL lapses, so a client that goes
  // quiet is indistinguishable from one that died — which is the point.
  function startHeartbeat() {
    stopHeartbeat();
    heartbeat = setInterval(() => send('HEARTBEAT', {}), HEARTBEAT_MS);
  }

  function stopHeartbeat() {
    if (heartbeat) clearInterval(heartbeat);
    heartbeat = null;
  }

  // -- watched users --------------------------------------------------------

  function renderFriend(p) {
    friends.set(p.userId, p);

    const empty = ui.watched.querySelector('.empty');
    if (empty) empty.remove();

    let row = ui.watched.querySelector(`[data-user="${CSS.escape(p.userId)}"]`);
    if (!row) {
      row = document.createElement('div');
      row.className = 'friend';
      row.dataset.user = p.userId;

      const name = document.createElement('span');
      name.className = 'name';
      name.textContent = p.userId;

      const pill = document.createElement('span');
      pill.className = 'pill';

      const where = document.createElement('span');
      where.className = 'where';

      const joinBtn = document.createElement('button');
      joinBtn.textContent = 'Join';
      joinBtn.addEventListener('click', () => send('JOIN', { targetUserId: p.userId }));

      row.append(name, where, pill, joinBtn);
      ui.watched.appendChild(row);
    }

    row.querySelector('.pill').className = `pill ${p.status}`;
    row.querySelector('.pill').textContent = p.status;
    row.querySelector('.where').textContent = p.placeId
      ? `${p.placeId}${p.serverId ? ' / ' + p.serverId : ''}`
      : '';
  }

  // -- wiring ---------------------------------------------------------------

  function setConnected(on) {
    ui.connect.disabled = on;
    ui.disconnect.disabled = !on;
    ui.setPresence.disabled = !on;
    ui.subscribe.disabled = !on;
    ui.userId.disabled = on;
    ui.gateway.disabled = on;
  }

  ui.connect.addEventListener('click', connect);
  ui.disconnect.addEventListener('click', disconnect);

  ui.setPresence.addEventListener('click', () => {
    send('SET_PRESENCE', {
      status: ui.statusSelect.value,
      placeId: ui.placeId.value.trim(),
      serverId: ui.serverId.value.trim(),
    });
  });

  ui.subscribe.addEventListener('click', () => {
    const ids = ui.watchIds.value
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean);
    if (ids.length) send('SUBSCRIBE', { userIds: ids });
  });

  ui.clearLog.addEventListener('click', () => {
    ui.log.textContent = '';
    frames = 0;
    ui.frameCount.textContent = '0 frames';
  });

  ui.userId.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !ui.connect.disabled) connect();
  });

  setConnected(false);
})();

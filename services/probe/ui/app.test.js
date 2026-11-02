import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import { createClient, TOKEN_KEY } from './app.js';

const fixtureText = readFileSync(new URL('./testdata/snapshot.json', import.meta.url), 'utf8');

class FakeStorage {
  constructor() { this.map = new Map(); }
  getItem(k) { return this.map.has(k) ? this.map.get(k) : null; }
  setItem(k, v) { this.map.set(k, String(v)); }
  removeItem(k) { this.map.delete(k); }
}

function harness({ fetchStatus = 204, token } = {}) {
  const sockets = [];
  class FakeWebSocket {
    constructor(url) {
      this.url = url;
      this.sent = [];
      this.closed = false;
      sockets.push(this);
    }
    send(data) { this.sent.push(data); }
    close() { this.closed = true; }
  }
  const timers = [];
  const calls = [];
  const storage = new FakeStorage();
  if (token) storage.setItem(TOKEN_KEY, token);
  const client = createClient({
    fetch: async (url, init) => { calls.push({ url, init }); return { ok: fetchStatus < 300, status: fetchStatus }; },
    WebSocket: FakeWebSocket,
    storage,
    setTimeout: (fn, ms) => { timers.push({ fn, ms }); return timers.length; },
    clearTimeout: () => {},
    location: { protocol: 'http:', host: 'probe.local:8094' },
  });
  return { client, sockets, timers, calls, storage };
}

test('token is kept only in sessionStorage', async () => {
  const h = harness();
  assert.equal(h.client.state.needLogin, true);
  h.client.login('s3cret-token');
  assert.equal(h.storage.getItem(TOKEN_KEY), 's3cret-token');
  assert.equal(h.client.state.needLogin, false);
  assert.equal(h.sockets.length, 1);
  assert.equal(h.sockets[0].url, 'ws://probe.local:8094/ws');
  await h.client.snooze('cache-hit-rate', 15);
  assert.equal(h.calls[0].url, '/api/snooze');
  assert.ok(!h.calls[0].url.includes('s3cret'));

  const source = readFileSync(new URL('./app.js', import.meta.url), 'utf8');
  assert.ok(!source.includes('localStorage'));
  assert.ok(!source.includes('cookie'));
  assert.match(source, /sessionStorage/);

  const resumed = harness({ token: 'kept' });
  resumed.client.start();
  assert.equal(resumed.sockets.length, 1);
  assert.equal(resumed.client.state.needLogin, false);

  const https = createClient({
    fetch: async () => ({ ok: true, status: 204 }),
    WebSocket: class { constructor(url) { this.url = url; https.url = url; } send() {} close() {} },
    storage: new FakeStorage(),
    setTimeout: () => 0,
    clearTimeout: () => {},
    location: { protocol: 'https:', host: 'probe.example' },
  });
  https.login('t');
  assert.equal(https.url, 'wss://probe.example/ws');
});

test('first message is the auth message', () => {
  const h = harness();
  h.client.login('abc');
  const ws = h.sockets[0];
  assert.equal(ws.sent.length, 0);
  ws.onopen();
  assert.equal(ws.sent.length, 1);
  assert.deepEqual(JSON.parse(ws.sent[0]), { auth: 'abc' });
  assert.ok(!ws.url.includes('abc'));
});

test('close 4401 clears the token and asks again', () => {
  const h = harness();
  h.client.login('wrong');
  const ws = h.sockets[0];
  ws.onopen();
  ws.onclose({ code: 4401 });
  assert.equal(h.storage.getItem(TOKEN_KEY), null);
  assert.equal(h.client.state.needLogin, true);
  assert.equal(h.client.state.message, 'The token was not accepted.');
  assert.equal(h.client.state.connected, false);
  assert.equal(h.timers.length, 0);

  h.client.login('right');
  assert.equal(h.client.state.message, '');
  assert.equal(h.sockets.length, 2);
});

test('401 on snooze clears the token', async () => {
  const h = harness({ fetchStatus: 401 });
  h.client.login('expired');
  const ws = h.sockets[0];
  ws.onopen();
  ws.onmessage({ data: fixtureText });
  const ok = await h.client.snooze('cache-hit-rate', 15);
  assert.equal(ok, false);
  assert.equal(h.storage.getItem(TOKEN_KEY), null);
  assert.equal(h.client.state.needLogin, true);
  assert.equal(h.client.state.message, 'The token was not accepted.');
  assert.equal(ws.closed, true);
  ws.onclose({ code: 1000 });
  assert.equal(h.timers.length, 0);

  const u = harness({ fetchStatus: 401 });
  u.client.login('expired');
  assert.equal(await u.client.unsnooze('cache-hit-rate'), false);
  assert.equal(u.storage.getItem(TOKEN_KEY), null);
});

test('snooze posts id and minutes with the bearer header', async () => {
  const h = harness();
  h.client.login('tok');
  assert.equal(await h.client.snooze('cache-hit-rate', 15), true);
  const { url, init } = h.calls[0];
  assert.equal(url, '/api/snooze');
  assert.equal(init.method, 'POST');
  assert.equal(init.headers.Authorization, 'Bearer tok');
  assert.equal(init.headers['Content-Type'], 'application/json');
  assert.deepEqual(JSON.parse(init.body), { id: 'cache-hit-rate', minutes: 15 });
  assert.equal(init.credentials, 'omit');

  assert.equal(await h.client.unsnooze('subtree-queue-age'), true);
  assert.equal(h.calls[1].url, '/api/unsnooze');
  assert.equal(h.calls[1].init.headers.Authorization, 'Bearer tok');
  assert.deepEqual(JSON.parse(h.calls[1].init.body), { id: 'subtree-queue-age' });

  const failing = harness({ fetchStatus: 400 });
  failing.client.login('tok');
  assert.equal(await failing.client.snooze('x', 15), false);
  assert.equal(failing.storage.getItem(TOKEN_KEY), 'tok');
  assert.equal(failing.client.state.needLogin, false);
});

test('reconnects with backoff after a close', () => {
  const h = harness();
  h.client.login('tok');
  const delays = [];
  for (let i = 0; i < 7; i++) {
    const ws = h.sockets[h.sockets.length - 1];
    ws.onopen();
    ws.onclose({ code: 1006 });
    assert.equal(h.client.state.connected, false);
    const timer = h.timers[h.timers.length - 1];
    delays.push(timer.ms);
    timer.fn();
  }
  assert.deepEqual(delays, [1000, 2000, 4000, 8000, 16000, 30000, 30000]);
  assert.equal(h.sockets.length, 8);

  const ws = h.sockets[h.sockets.length - 1];
  ws.onopen();
  assert.deepEqual(JSON.parse(ws.sent[0]), { auth: 'tok' });
  ws.onmessage({ data: fixtureText });
  ws.onclose({ code: 1006 });
  assert.equal(h.timers[h.timers.length - 1].ms, 1000);
  assert.equal(h.client.state.snapshot.node, 'teranode1');
  assert.equal(h.client.state.needLogin, false);
});

test('a snapshot frame updates the model', () => {
  const h = harness();
  const seen = [];
  h.client.onChange((state) => seen.push({ connected: state.connected, node: state.snapshot && state.snapshot.node }));
  h.client.login('tok');
  const ws = h.sockets[0];
  ws.onopen();
  assert.equal(h.client.state.snapshot, null);
  ws.onmessage({ data: fixtureText });
  assert.equal(h.client.state.connected, true);
  assert.equal(h.client.state.snapshot.node, 'teranode1');
  assert.equal(h.client.state.snapshot.alarms.length, 3);
  assert.deepEqual(seen[seen.length - 1], { connected: true, node: 'teranode1' });

  const next = JSON.parse(fixtureText);
  next.node = 'teranode2';
  ws.onmessage({ data: JSON.stringify(next) });
  assert.equal(h.client.state.snapshot.node, 'teranode2');

  ws.onmessage({ data: 'not json' });
  assert.equal(h.client.state.snapshot.node, 'teranode2');
});

const histPayload = (stage = 'cache', v = 0.6) => ({
  stage,
  series: [{ key: `${stage}.hitRate`, points: [{ t: '2026-10-28T11:30:00Z', v, st: 'ok' }, { t: '2026-10-28T11:30:02Z', v: null, st: 'nodata' }] }],
});

function historyHarness({ token = 'tok', handler } = {}) {
  const calls = [];
  const intervals = [];
  const timeouts = [];
  const storage = new FakeStorage();
  if (token) storage.setItem(TOKEN_KEY, token);
  const sockets = [];
  const client = createClient({
    fetch: (url, init) => {
      calls.push({ url, init });
      return handler(url, init, calls.length);
    },
    WebSocket: class {
      constructor() { sockets.push(this); this.sent = []; }
      send(d) { this.sent.push(d); }
      close() { this.closed = true; }
    },
    storage,
    setTimeout: (fn, ms) => { timeouts.push({ fn, ms, cleared: false }); return timeouts.length - 1; },
    clearTimeout: (id) => { if (timeouts[id]) timeouts[id].cleared = true; },
    setInterval: (fn, ms) => { intervals.push({ fn, ms, cleared: false }); return intervals.length - 1; },
    clearInterval: (id) => { if (intervals[id]) intervals[id].cleared = true; },
    location: { protocol: 'http:', host: 'probe.local:8094' },
  });
  client.start();
  return { client, calls, intervals, timeouts, storage, sockets };
}

const okJson = (payload) => async () => ({ ok: true, status: 200, json: async () => payload });
const flush = () => new Promise((resolve) => setImmediate(resolve));

test('history fetch uses the bearer header', async () => {
  const h = historyHarness({ handler: okJson(histPayload()) });
  assert.equal(h.calls.length, 0);
  assert.equal(h.intervals.length, 0);
  h.client.openStage('cache');
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].url, '/api/history?stage=cache&minutes=10');
  assert.equal(h.calls[0].init.headers.Authorization, 'Bearer tok');
  assert.equal(h.calls[0].init.credentials, 'omit');
  assert.ok(!h.calls[0].url.includes('tok'));
  assert.equal(h.intervals.length, 1);
  assert.equal(h.intervals[0].ms, 10000);
  assert.equal(h.client.state.history.stage, 'cache');
  assert.equal(h.client.state.history.error, false);
  assert.equal(h.client.state.history.data.get('cache.hitRate').length, 2);

  h.client.closeStage();
  h.client.openStage('not-a-stage');
  h.client.openStage('cache/../x');
  assert.equal(h.calls.length, 1);
});

test('history polling stops when leaving the stage view', async () => {
  const h = historyHarness({ handler: okJson(histPayload()) });
  h.client.openStage('cache');
  await flush();
  h.intervals[0].fn();
  await flush();
  h.intervals[0].fn();
  await flush();
  assert.equal(h.calls.length, 3);

  h.client.closeStage();
  assert.equal(h.intervals[0].cleared, true);
  assert.equal(h.client.state.history, null);
  await flush();
  assert.equal(h.calls.length, 3);

  h.client.openStage('cache');
  h.client.openStage('stores');
  await flush();
  assert.equal(h.intervals.length, 3);
  assert.equal(h.intervals[1].cleared, true);
  assert.equal(h.intervals[2].cleared, false);
  assert.equal(h.calls[h.calls.length - 1].url, '/api/history?stage=stores&minutes=10');
  h.client.closeStage();
  assert.equal(h.intervals[2].cleared, true);
});

test('history 401 clears the token', async () => {
  const h = historyHarness({ handler: async () => ({ ok: false, status: 401, json: async () => ({}) }) });
  h.client.openStage('cache');
  await flush();
  assert.equal(h.storage.getItem(TOKEN_KEY), null);
  assert.equal(h.client.state.needLogin, true);
  assert.equal(h.client.state.message, 'The token was not accepted.');
  assert.equal(h.sockets[0].closed, true);
  assert.equal(h.intervals[0].cleared, true);

  const before = h.calls.length;
  h.client.login('fresh');
  await flush();
  assert.ok(h.calls.length > before);
  assert.equal(h.calls[h.calls.length - 1].init.headers.Authorization, 'Bearer fresh');
});

test('overlapping history requests are skipped', async () => {
  const pending = [];
  const h = historyHarness({ handler: () => new Promise((resolve) => pending.push(resolve)) });
  h.client.openStage('cache');
  h.intervals[0].fn();
  h.intervals[0].fn();
  assert.equal(h.calls.length, 1);
  pending[0]({ ok: true, status: 200, json: async () => histPayload() });
  await flush();
  assert.ok(h.client.state.history.data);
  h.intervals[0].fn();
  assert.equal(h.calls.length, 2);
  pending[1]({ ok: true, status: 200, json: async () => histPayload() });
  await flush();
});

test('history failure keeps the last good sparklines', async () => {
  const replies = [
    okJson(histPayload('cache', 0.6)),
    async () => ({ ok: false, status: 500, json: async () => ({}) }),
    async () => { throw new Error('down'); },
    async () => ({ ok: true, status: 200, json: async () => { throw new Error('bad json'); } }),
    okJson({ stage: 'cache', series: 'hostile' }),
    okJson(histPayload('cache', 0.9)),
  ];
  const h = historyHarness({ handler: (url, init, n) => replies[n - 1]() });
  h.client.openStage('cache');
  await flush();
  const good = h.client.state.history.data;
  assert.equal(h.client.state.history.error, false);
  for (let i = 0; i < 4; i++) {
    h.intervals[0].fn();
    await flush();
    assert.equal(h.client.state.history.error, true, `reply ${i + 2}`);
    assert.equal(h.client.state.history.data, good, `reply ${i + 2}`);
    assert.equal(h.storage.getItem(TOKEN_KEY), 'tok');
  }
  h.intervals[0].fn();
  await flush();
  assert.equal(h.client.state.history.error, false);
  assert.notEqual(h.client.state.history.data, good);
  assert.equal(h.client.state.history.data.get('cache.hitRate')[0].v, 0.9);
});

test('history response after leaving the view is discarded', async () => {
  const pending = [];
  const h = historyHarness({ handler: () => new Promise((resolve) => pending.push(resolve)) });
  h.client.openStage('cache');
  h.client.closeStage();
  pending[0]({ ok: true, status: 200, json: async () => histPayload() });
  await flush();
  assert.equal(h.client.state.history, null);
  h.client.openStage('stores');
  pending[1]({ ok: true, status: 200, json: async () => histPayload('cache') });
  await flush();
  assert.equal(h.client.state.history.data, null);
  assert.equal(h.client.state.history.error, true);
});

test('history waits for a token before fetching', async () => {
  const h = historyHarness({ token: '', handler: okJson(histPayload()) });
  h.client.openStage('cache');
  await flush();
  assert.equal(h.calls.length, 0);
  assert.equal(h.intervals.length, 0);
  h.client.login('late');
  await flush();
  assert.equal(h.calls.length, 1);
  assert.equal(h.calls[0].init.headers.Authorization, 'Bearer late');
  assert.equal(h.intervals.length, 1);
});

function hangingHandler(first) {
  const pending = [];
  const handler = (url, init, n) => {
    if (n === 1 && first) return first();
    return new Promise((resolve, reject) => {
      pending.push({ resolve, reject, signal: init.signal });
      init.signal.addEventListener('abort', () => reject(new Error('aborted')));
    });
  };
  return { handler, pending };
}

test('hung history request is aborted after 8 seconds and polling continues', async () => {
  const { handler, pending } = hangingHandler(okJson(histPayload('cache', 0.6)));
  const h = historyHarness({ handler });
  h.client.openStage('cache');
  await flush();
  const good = h.client.state.history.data;
  assert.ok(good);

  h.intervals[0].fn();
  assert.equal(h.calls.length, 2);
  assert.ok(h.calls[1].init.signal);
  const timeout = h.timeouts[h.timeouts.length - 1];
  assert.equal(timeout.ms, 8000);
  assert.equal(pending[0].signal.aborted, false);

  h.intervals[0].fn();
  assert.equal(h.calls.length, 2);

  timeout.fn();
  await flush();
  assert.equal(pending[0].signal.aborted, true);
  assert.equal(h.client.state.history.error, true);
  assert.equal(h.client.state.history.data, good);
  assert.equal(h.storage.getItem(TOKEN_KEY), 'tok');

  h.intervals[0].fn();
  assert.equal(h.calls.length, 3);
  assert.equal(h.intervals[0].cleared, false);
});

test('leaving the view aborts the in-flight history request', async () => {
  const { handler, pending } = hangingHandler();
  const h = historyHarness({ handler });
  h.client.openStage('cache');
  assert.equal(pending.length, 1);
  assert.equal(pending[0].signal.aborted, false);
  h.client.closeStage();
  assert.equal(pending[0].signal.aborted, true);
  assert.equal(h.timeouts[0].cleared, true);
  await flush();
  assert.equal(h.client.state.history, null);

  h.client.openStage('cache');
  assert.equal(pending.length, 2);
  h.client.openStage('stores');
  assert.equal(pending[1].signal.aborted, true);
  assert.equal(pending.length, 3);
  assert.equal(pending[2].signal.aborted, false);
  assert.equal(h.calls[2].url, '/api/history?stage=stores&minutes=10');
  await flush();
  assert.equal(h.client.state.history.stage, 'stores');
  assert.equal(h.client.state.history.error, false);
});

test('late response after abort is ignored', async () => {
  const { handler, pending } = hangingHandler(okJson(histPayload('cache', 0.6)));
  const h = historyHarness({ handler });
  h.client.openStage('cache');
  await flush();
  const good = h.client.state.history.data;

  h.intervals[0].fn();
  h.timeouts[h.timeouts.length - 1].fn();
  await flush();
  assert.equal(h.client.state.history.error, true);

  h.intervals[0].fn();
  assert.equal(h.calls.length, 3);

  pending[0].resolve({ ok: true, status: 200, json: async () => histPayload('cache', 0.99) });
  await flush();
  assert.equal(h.client.state.history.error, true);
  assert.equal(h.client.state.history.data, good);

  pending[1].resolve({ ok: true, status: 200, json: async () => histPayload('cache', 0.7) });
  await flush();
  assert.equal(h.client.state.history.error, false);
  assert.equal(h.client.state.history.data.get('cache.hitRate')[0].v, 0.7);

  const stale = hangingHandler();
  const g = historyHarness({ handler: stale.handler });
  g.client.openStage('cache');
  g.client.closeStage();
  stale.pending[0].resolve({ ok: true, status: 200, json: async () => histPayload('cache') });
  await flush();
  assert.equal(g.client.state.history, null);
});

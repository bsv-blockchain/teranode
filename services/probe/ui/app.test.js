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

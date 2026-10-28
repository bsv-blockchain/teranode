import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import {
  panelStatus,
  formatReading,
  renderHome,
  renderAlarmPopover,
  renderAlarmList,
  renderTopBar,
  renderBottomBar,
  flowPlacement,
  nextBackoff,
  toMarkupString,
} from './view.js';

const fixture = JSON.parse(readFileSync(new URL('./testdata/snapshot.json', import.meta.url), 'utf8'));
const snap = () => structuredClone(fixture);
const NOW = Date.parse('2026-10-28T11:35:14Z');

function walk(node, out = []) {
  if (!node) return out;
  out.push(node);
  for (const c of node.children || []) walk(c, out);
  return out;
}

const texts = (node) => walk(node).filter((n) => typeof n.text === 'string').map((n) => n.text);
const byAttr = (node, name, value) => walk(node).filter((n) => n.attrs && n.attrs[name] === value);
const one = (node, name, value) => {
  const found = byAttr(node, name, value);
  assert.equal(found.length, 1, `expected one node with ${name}=${value}, got ${found.length}`);
  return found[0];
};

test('renders six panels in order', () => {
  const home = renderHome(snap());
  const panels = walk(home).filter((n) => n.attrs && n.attrs['data-stage']);
  assert.deepEqual(panels.map((p) => p.attrs['data-stage']), ['ingress', 'validator', 'cache', 'assembly', 'chain', 'stores']);
  const titles = panels.map((p) => walk(p).find((n) => n.attrs && n.attrs.class === 'ti').text);
  assert.deepEqual(titles, ['Ingress', 'Validator', 'Tx meta cache', 'Block Assembly', 'Blockchain', 'Data stores']);
  assert.equal(home.tag, 'svg');
  assert.equal(home.attrs.role, 'img');
  assert.match(home.attrs['aria-label'], /teranode1/);
  assert.match(home.attrs['aria-label'], /Validator critical/);
});

test('stale reading renders no data and age', () => {
  const r = formatReading('assembly.subtreeFill', { v: 0, st: 'nodata', note: 'no data', age: 12 });
  assert.equal(r.text, '–');
  assert.equal(r.word, 'no data, 12 s ago');
  assert.equal(r.st, 'n');

  const noAge = formatReading('chain.forkDepth', { v: 0, st: 'nodata' });
  assert.equal(noAge.text, '–');
  assert.equal(noAge.word, 'no data');

  const noted = formatReading('stores.aerospike.readP99Sec', { v: 0, st: 'nodata', note: 'latency not exposed', age: 75 });
  assert.equal(noted.word, 'latency not exposed, 1 min ago');

  const home = renderHome(snap());
  const panel = one(home, 'data-stage', 'assembly');
  assert.ok(texts(panel).includes('no data, 12 s ago'), texts(panel).join('|'));

  const strip = one(home, 'data-flow', 'blocks-final');
  assert.ok(texts(strip).includes('no data'), texts(strip).join('|'));
  assert.ok(texts(strip).includes('30 s ago'), texts(strip).join('|'));
  assert.ok(texts(strip).includes('lag 0'), texts(strip).join('|'));
  const path = walk(strip).find((n) => n.tag === 'path');
  assert.equal(path.attrs.stroke, '#56626f');
  assert.ok(!String(path.attrs.class || '').includes('flow'));
});

test('not configured store renders grey', () => {
  const home = renderHome(snap());
  const pg = one(home, 'data-store', 'postgres');
  assert.equal(pg.attrs['data-st'], 'n');
  const t = texts(pg);
  assert.ok(t.includes('not configured'), t.join('|'));
  assert.ok(t.includes('Postgres'));
  for (const s of t) assert.doesNotMatch(s, /\d/, `unexpected number in ${s}`);

  const as = one(home, 'data-store', 'aerospike');
  assert.equal(as.attrs['data-st'], 'r');
  assert.ok(!texts(as).includes('not configured'));
  assert.ok(texts(as).includes('Read p99 38 ms'), texts(as).join('|'));
  assert.ok(texts(as).includes('Normal 1.2 ms'), texts(as).join('|'));

  const s = snap();
  s.stores.aerospike.configured = false;
  s.stores.postgres.configured = true;
  const flipped = renderHome(s);
  assert.equal(one(flipped, 'data-store', 'aerospike').attrs['data-st'], 'n');
  assert.ok(texts(one(flipped, 'data-store', 'aerospike')).includes('not configured'));
  assert.ok(!texts(one(flipped, 'data-store', 'postgres')).includes('not configured'));
});

test('start of chain tag shown only on flagged alarm', () => {
  const s = snap();
  const list = renderAlarmList(s.alarms);
  const items = walk(list).filter((n) => n.attrs && n.attrs['data-alarm-id']);
  assert.equal(items.length, 3);
  for (const item of items) {
    const has = texts(item).includes('Start of chain');
    assert.equal(has, item.attrs['data-alarm-id'] === 'cache-hit-rate', item.attrs['data-alarm-id']);
  }

  const snoozed = items.find((i) => i.attrs['data-alarm-id'] === 'subtree-queue-age');
  assert.match(snoozed.attrs.class, /snoozed/);
  assert.ok(texts(snoozed).includes('Snoozed'));
  assert.equal(byAttr(snoozed, 'data-action', 'unsnooze').length, 1);
  assert.equal(byAttr(snoozed, 'data-action', 'snooze').length, 0);

  const cacheItem = items.find((i) => i.attrs['data-alarm-id'] === 'cache-hit-rate');
  const links = walk(cacheItem).filter((n) => n.tag === 'a');
  assert.deepEqual(links.map((l) => l.attrs.href), ['#/stage/cache', '#/stage/stores']);
  assert.ok(texts(cacheItem).includes('Open cache details'));
  const snoozeBtn = one(cacheItem, 'data-action', 'snooze');
  assert.equal(snoozeBtn.attrs['data-id'], 'cache-hit-rate');
  assert.equal(snoozeBtn.tag, 'button');

  assert.ok(texts(renderAlarmPopover(s.alarms[1], NOW)).includes('Start of chain'));
  assert.ok(!texts(renderAlarmPopover(s.alarms[0], NOW)).includes('Start of chain'));

  const home = renderHome(s, NOW);
  const pop = one(home, 'data-popover', 'cache-hit-rate');
  assert.ok(texts(pop).includes('Tx meta cache hit rate is low'));
});

test('calibration bar width equals fraction', () => {
  const s = snap();
  const bar = renderBottomBar(s, NOW);
  const fill = walk(bar).find((n) => n.attrs && n.attrs.class === 'cal-fill');
  assert.equal(fill.attrs.width, '0.6');
  const until = new Date(s.calibration.until);
  const hhmm = `${String(until.getHours()).padStart(2, '0')}:${String(until.getMinutes()).padStart(2, '0')}`;
  assert.ok(texts(bar).includes(`Calibrating baselines. Fixed limits apply until ${hhmm}.`), texts(bar).join('|'));

  s.calibration = { ready: false, until: s.calibration.until, fraction: 0.25 };
  assert.equal(walk(renderBottomBar(s, NOW)).find((n) => n.attrs && n.attrs.class === 'cal-fill').attrs.width, '0.25');

  s.calibration = { ready: true, until: s.calibration.until, fraction: 1 };
  const ready = renderBottomBar(s, NOW);
  assert.ok(texts(ready).includes('Baselines ready.'));
  assert.equal(walk(ready).find((n) => n.attrs && n.attrs.class === 'cal-fill').attrs.width, '1');

  assert.ok(texts(renderBottomBar(snap(), NOW)).includes('Updated 2 s ago'));
  assert.ok(texts(renderBottomBar(snap(), NOW + 120000)).includes('Updated 2 min ago'));
});

test('escapes text from the snapshot', () => {
  const s = snap();
  const evilTitle = '<script>alert(1)</script>';
  const evilNode = '"><img src=x onerror=alert(1)>';
  s.alarms[1].title = evilTitle;
  s.alarms[1].body = '<b>bold</b> & more';
  s.alarms[1].drilldowns = [{ label: '<i>x</i>', path: 'javascript:alert(1)' }];
  s.node = evilNode;
  s.flows[0].label = '<svg onload=alert(1)>';

  const outputs = [renderHome(s, NOW), renderAlarmList(s.alarms), renderAlarmPopover(s.alarms[1], NOW), renderTopBar(s), renderBottomBar(s, NOW)];

  for (const out of outputs) {
    for (const n of walk(out)) {
      assert.notEqual(n.tag, 'script');
      assert.notEqual(n.tag, 'img');
      assert.notEqual(n.tag, 'b');
      for (const k of Object.keys(n.attrs || {})) assert.doesNotMatch(k, /^on/i);
      if (n.tag === 'a') assert.match(n.attrs.href, /^#\//);
    }
    const markup = toMarkupString(out);
    assert.ok(!markup.includes('<script'), markup);
    assert.ok(!markup.includes('<img'), markup);
    assert.ok(!markup.includes('<b>'), markup);
    assert.ok(!markup.includes('javascript:'), markup);
  }

  assert.ok(texts(renderAlarmList(s.alarms)).includes(evilTitle));
  assert.ok(texts(renderTopBar(s)).includes(evilNode));
  assert.ok(toMarkupString(renderAlarmList(s.alarms)).includes('&lt;script&gt;alert(1)&lt;/script&gt;'));
  assert.ok(toMarkupString(renderTopBar(s)).includes('&quot;&gt;&lt;img src=x onerror=alert(1)&gt;'));
  const home = toMarkupString(renderHome(s, NOW));
  assert.ok(home.includes('aria-label="'));
  assert.ok(home.includes('&quot;&gt;&lt;img'), 'node name in aria-label is escaped');
  assert.equal(toMarkupString({ tag: 'text', attrs: { x: '1"2' }, text: "a&b<c>'" }), '<text x="1&quot;2">a&amp;b&lt;c&gt;&#39;</text>');
});

test('strips are placed by flow id', () => {
  const expected = {
    'tx-input': 0, subtrees: 0, txmeta: 0,
    lookups: 1,
    validated: 2,
    blocks: 3, 'blocks-final': 3,
    'utxo-ops': 4, 'block-writes': 4,
  };
  for (const [id, gap] of Object.entries(expected)) assert.equal(flowPlacement(id).gap, gap, id);
  assert.deepEqual([0, 1, 2].map((i) => ['tx-input', 'subtrees', 'txmeta'][i]).map((id) => flowPlacement(id).slot), [0, 1, 2]);
  assert.equal(flowPlacement('blocks-final').slot, 1);
  assert.equal(flowPlacement('nope'), null);

  const s = snap();
  for (const f of s.flows) { f.from = 'stores'; f.to = 'ingress'; }
  const home = renderHome(s, NOW);
  for (const [id, gap] of Object.entries(expected)) {
    const strip = one(home, 'data-flow', id);
    assert.equal(strip.attrs['data-gap'], String(gap), id);
  }

  const subtrees = one(home, 'data-flow', 'subtrees');
  assert.ok(texts(subtrees).includes('lag 1,840'), texts(subtrees).join('|'));
  assert.ok(texts(subtrees).includes('210/s'));
  const lookups = one(home, 'data-flow', 'lookups');
  assert.ok(texts(lookups).includes('11.1k/s'), texts(lookups).join('|'));
  assert.ok(!texts(lookups).some((t) => t.startsWith('lag')));
  const validated = one(home, 'data-flow', 'validated');
  assert.ok(texts(validated).includes('stalled'));
  const vpath = walk(validated).find((n) => n.tag === 'path');
  assert.equal(vpath.attrs.stroke, '#56626f');
  const upath = walk(one(home, 'data-flow', 'utxo-ops')).find((n) => n.tag === 'path');
  assert.match(upath.attrs.class, /flow/);
  assert.equal(upath.attrs.stroke, '#ef5f5f');

  s.flows.push({ id: 'unknown', from: 'a', to: 'b', label: 'mystery', rate: { v: 1, st: 'ok' } });
  assert.equal(byAttr(renderHome(s, NOW), 'data-flow', 'unknown').length, 0);
});

test('panel status is the worst reading', () => {
  assert.equal(panelStatus({ a: { v: 1, st: 'ok' }, b: { v: 1, st: 'warn' }, c: { v: 1, st: 'crit' }, w: [1, 2] }), 'r');
  assert.equal(panelStatus({ a: { v: 1, st: 'ok' }, b: { v: 1, st: 'warn' }, c: { v: 1, st: 'nodata' } }), 'a');
  assert.equal(panelStatus({ a: { v: 1, st: 'ok' }, c: { v: 1, st: 'nodata' }, d: { v: 0, st: 'idle' } }), 'g');
  assert.equal(panelStatus({ c: { v: 1, st: 'nodata' }, d: { v: 0, st: 'idle' }, blocksWaiting: [1] }), 'n');
  assert.equal(panelStatus({ aerospike: { configured: true, x: { v: 1, st: 'ok' } }, postgres: { configured: true, y: { v: 1, st: 'crit' } } }), 'r');
  assert.equal(panelStatus({}), 'n');

  const s = snap();
  assert.equal(panelStatus(s.validator), 'r');
  assert.equal(panelStatus(s.chain), 'a');
  assert.equal(panelStatus(s.stores), 'r');

  const home = renderHome(s, NOW);
  assert.equal(one(home, 'data-stage', 'validator').attrs['data-st'], 'r');
  assert.equal(one(home, 'data-stage', 'ingress').attrs['data-st'], 'a');
  assert.equal(one(home, 'data-stage', 'chain').attrs['data-st'], 'a');
});

test('warming note replaces the status word', () => {
  const warming = formatReading('cache.hitRate', { v: 0.4, st: 'ok', note: 'Warming' });
  assert.equal(warming.word, 'Warming');
  assert.equal(warming.text, '40%');
  assert.equal(formatReading('cache.hitRate', { v: 0.4, st: 'crit' }).word, 'Critical');
  assert.equal(formatReading('cache.hitRate', { v: 0.4, st: 'warn' }).word, 'Warning');
  assert.equal(formatReading('cache.hitRate', { v: 0.96, st: 'ok' }).word, '');
  assert.equal(formatReading('cache.hitRate', { v: 0.4, st: 'warn', note: 'Calibrating' }).word, 'Calibrating');

  const s = snap();
  s.cache.hitRate = { v: 0.4, st: 'ok', note: 'Warming' };
  const hit = one(renderHome(s, NOW), 'data-key', 'cache.hitRate');
  assert.ok(texts(hit).includes('Warming'));
  assert.ok(!texts(hit).includes('Critical'));
  assert.ok(texts(one(renderHome(snap(), NOW), 'data-key', 'cache.hitRate')).includes('Critical'));
  const ingress = one(renderHome(snap(), NOW), 'data-stage', 'ingress');
  const oldest = one(ingress, 'data-key', 'ingress.subtreeQueueOldestSec');
  assert.deepEqual(texts(oldest), ['Oldest waiting: 42 s', 'Calibrating']);
});

test('drain text variants', () => {
  const s = snap();
  assert.ok(texts(renderBottomBar(s, NOW)).includes('Drain time about 9 min'));
  s.drainSeconds = 40;
  assert.ok(texts(renderBottomBar(s, NOW)).includes('Drain time about 40 s'));
  s.drainSeconds = 7500;
  assert.ok(texts(renderBottomBar(s, NOW)).includes('Drain time about 2 h 5 min'));
  s.drainGrowing = true;
  const growing = texts(renderBottomBar(s, NOW));
  assert.ok(growing.includes('Backlog is growing'));
  assert.ok(!growing.some((t) => t.startsWith('Drain time')));
  delete s.drainSeconds;
  s.drainGrowing = false;
  const none = texts(renderBottomBar(s, NOW));
  assert.ok(!none.some((t) => t.startsWith('Drain time') || t === 'Backlog is growing'));
});

test('empty state when no snapshot', () => {
  for (const empty of [null, undefined]) {
    const home = renderHome(empty, NOW);
    assert.ok(texts(home).includes('Waiting for the first sample.'));
    assert.equal(walk(home).filter((n) => n.attrs && n.attrs['data-stage']).length, 0);
  }
  assert.deepEqual(texts(renderBottomBar(null, NOW)), []);
  assert.ok(Array.isArray(renderTopBar(null).children));
});

test('nextBackoff doubles and caps at 30 s', () => {
  assert.equal(nextBackoff(0), 1000);
  assert.equal(nextBackoff(undefined), 1000);
  assert.equal(nextBackoff(1000), 2000);
  assert.equal(nextBackoff(2000), 4000);
  assert.equal(nextBackoff(8000), 16000);
  assert.equal(nextBackoff(16000), 30000);
  assert.equal(nextBackoff(30000), 30000);
  let ms = 0;
  const seen = [];
  for (let i = 0; i < 8; i++) { ms = nextBackoff(ms); seen.push(ms); }
  assert.deepEqual(seen, [1000, 2000, 4000, 8000, 16000, 30000, 30000, 30000]);
});

test('hit rate shows normal value', () => {
  const r = formatReading('cache.hitRate', fixture.cache.hitRate);
  assert.equal(r.text, '61%');
  assert.equal(r.normal, '96%');
  assert.equal(r.st, 'r');
  const panel = one(renderHome(snap(), NOW), 'data-stage', 'cache');
  assert.ok(texts(panel).includes('Hit rate (normal 96%)'), texts(panel).join('|'));
  assert.ok(texts(panel).includes('61%'));
  assert.equal(formatReading('cache.hitRate', { v: 0.5, st: 'ok' }).normal, '');
});

test('formats numbers by unit', () => {
  const f = (k, v) => formatReading(k, { v, st: 'ok' }).text;
  assert.equal(f('ingress.propagationTxPerSec', 4100), '4,100');
  assert.equal(f('ingress.propagationTxPerSec', 1234567), '1,234,567');
  assert.equal(f('validator.latencyP99Sec', 0.64), '640 ms');
  assert.equal(f('validator.latencyP99Sec', 12), '12 s');
  assert.equal(f('stores.aerospike.readP99Sec', 0.0012), '1.2 ms');
  assert.equal(f('validator.queueFill', 0.94), '94%');
  assert.equal(f('stores.postgres.diskFill', 0.383), '38%');
  assert.equal(f('cache.sizeGB', 7.4), '7.4 GB');
  assert.equal(f('cache.capacityGB', 8), '8 GB');
  assert.equal(f('stores.postgres.sizeGB', 214), '214 GB');
  assert.equal(f('stores.aerospike.sizeGB', 1840), '1.84 TB');
  assert.equal(f('stores.aerospike.sizeGB', 0.25), '0.25 GB');
  assert.equal(f('assembly.tipLagBlocks', 3), '3 blocks');
  assert.equal(f('assembly.tipLagBlocks', 1), '1 block');
  assert.equal(f('chain.subscribersDropped', 0), '0');
  assert.equal(f('cache.entriesMillions', 118), '118M');
  assert.equal(f('assembly.paused', 0), 'Running');
  assert.equal(formatReading('assembly.paused', { v: 1, st: 'ok', note: 'reorging' }).text, 'Reorging');
  assert.equal(formatReading('assembly.paused', { v: 1, st: 'ok', note: 'reorging' }).word, '');
  assert.equal(f('flows.lookups.rate', 11100), '11.1k/s');
  assert.equal(f('flows.utxo-ops.rate', 31000), '31k/s');
  assert.equal(f('flows.tx-input.rate', 4100), '4,100/s');
  assert.equal(f('flows.blocks.rate', 0.2), '0.2/s');
});

test('top bar chips follow the snapshot', () => {
  const s = snap();
  const bar = renderTopBar(s);
  const t = texts(bar);
  assert.ok(t.includes('teranode1'));
  assert.ok(t.includes('FSM: CATCHINGBLOCKS'));
  assert.ok(t.includes('Tip 912,437, peer 912,440'));
  assert.ok(t.includes('3 alarms'));
  assert.equal(one(bar, 'data-chip', 'node').attrs['data-st'], 'a');
  assert.equal(one(bar, 'data-chip', 'fsm').attrs['data-st'], 'a');
  assert.equal(one(bar, 'data-chip', 'alarms').attrs['data-st'], 'r');
  assert.equal(one(bar, 'data-chip', 'alarms').tag, 'button');

  s.peerTip = 0;
  s.sources = s.sources.map((x) => ({ ...x, ok: true }));
  s.fsm = 'RUNNING';
  s.alarms = s.alarms.map((a) => ({ ...a, severity: a.id === 'subtree-queue-age' ? 'crit' : 'warn' }));
  const b2 = renderTopBar(s);
  assert.ok(texts(b2).includes('Tip 912,437'));
  assert.equal(one(b2, 'data-chip', 'node').attrs['data-st'], 'g');
  assert.equal(one(b2, 'data-chip', 'fsm').attrs['data-st'], 'g');
  assert.equal(one(b2, 'data-chip', 'alarms').attrs['data-st'], 'a');

  s.alarms = [];
  const b3 = renderTopBar(s);
  assert.equal(one(b3, 'data-chip', 'alarms').attrs['data-st'], 'g');
  assert.ok(texts(b3).includes('No alarms'));
});

test('popover shows the highest severity alarm that is not snoozed', () => {
  const s = snap();
  const home = renderHome(s, NOW);
  assert.equal(walk(home).filter((n) => n.attrs && n.attrs['data-popover']).length, 1);
  one(home, 'data-popover', 'cache-hit-rate');

  s.alarms = s.alarms.map((a) => ({ ...a, snoozed: a.severity === 'crit' }));
  one(renderHome(s, NOW), 'data-popover', 'subtree-queue-age');

  s.alarms = s.alarms.map((a) => ({ ...a, snoozed: true }));
  assert.equal(walk(renderHome(s, NOW)).filter((n) => n.attrs && n.attrs['data-popover']).length, 0);
});

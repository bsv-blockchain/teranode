import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

import {
  panelStatus,
  formatReading,
  renderHome,
  renderLanes,
  renderAlarmPopover,
  renderAlarmList,
  renderTopBar,
  renderBottomBar,
  flowPlacement,
  nextBackoff,
  toMarkupString,
  renderStage,
  sparkline,
  parseRoute,
  normalizeHistory,
  stageReadings,
  STAGES,
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
  assert.ok(texts(strip).includes('lag no data, 34 s ago'), texts(strip).join('|'));
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
  assert.ok(texts(subtrees).includes('lag no data, 34 s ago'), texts(subtrees).join('|'));
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

test('a long reading note is cut to fit the readout and keeps its full text as a tooltip', () => {
  const note = 'includes block-validation checks and rejected transactions';
  const s = snap();
  s.validator.validatedPerSec = { v: 0, st: 'ok', note };
  const node = one(renderHome(s, NOW), 'data-key', 'validator.validatedPerSec');
  const words = walk(node).filter((n) => n.attrs && n.attrs.class === 'wd');
  assert.equal(words.length, 1, 'one visible note and nothing below the well');
  assert.equal(Number(words[0].attrs.x), 36);
  assert.ok(words[0].text.endsWith('…'));
  assert.ok(words[0].text.length * 9.5 * 0.52 <= 118, `the cut note fits the well: ${words[0].text}`);
  assert.ok(note.startsWith(words[0].text.slice(0, -1)));
  const titles = walk(node).filter((n) => n.tag === 'title');
  assert.deepEqual(titles.map((n) => n.text), [note]);
});

test('a strip moves only while its flow carries traffic', () => {
  const s = snap();
  s.flows[0].rate = { v: 0, st: 'ok' };
  s.flows[1].rate = { v: 50, st: 'ok' };
  const home = renderHome(s, NOW);
  const moving = (id) => walk(one(home, 'data-flow', id)).some((n) => n.attrs && /\bflow\b/.test(n.attrs.class || ''));
  assert.equal(moving(s.flows[0].id), false);
  assert.equal(moving(s.flows[1].id), true);
  assert.equal(one(home, 'data-flow', s.flows[0].id).attrs['data-st'], 'n');
});

test('the cache panel shows the insertion rate and the stage view lists it', () => {
  const home = renderHome(snap(), NOW);
  const row = one(home, 'data-key', 'cache.insertionsPerSec');
  assert.ok(texts(row).includes('Inserts, per s'));
  assert.ok(texts(row).includes('1,200'));

  const stage = renderStage(snap(), 'cache', null, NOW);
  assert.ok(texts(stage).includes('Inserts, per s'));
});

test('a short reading note stays inside the readout', () => {
  const s = snap();
  s.assembly.tipLagBlocks = { v: 0, st: 'ok', note: 'Calibrating' };
  const node = one(renderHome(s, NOW), 'data-key', 'assembly.tipLagBlocks');
  const inWell = walk(node).filter((n) => n.attrs && n.attrs.class === 'wd' && Number(n.attrs.x) === 36);
  assert.deepEqual(inWell.map((n) => n.text), ['Calibrating']);
  assert.equal(walk(node).filter((n) => n.attrs && n.attrs.class === 'wd' && Number(n.attrs.x) === 12).length, 0);
});

test('store tank notes are not cut off', () => {
  const s = snap();
  s.stores.postgres.configured = true;
  s.stores.postgres.diskFill = { v: 0, st: 'nodata', note: 'disk capacity not set' };
  s.stores.postgres.readP99Sec = { v: 0, st: 'nodata', note: 'needs pg_stat_statements' };
  const all = texts(renderHome(s, NOW)).join(' ');
  assert.ok(all.includes('pg_stat_statements'));
  assert.ok(all.includes('capacity not'));
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
  assert.equal(f('cache.sizeGB', 0.000343962), '344 KB');
  assert.equal(f('cache.sizeGB', 0.0042), '4.2 MB');
  assert.equal(f('cache.sizeGB', 0.01), '0.01 GB');
  assert.equal(f('cache.sizeGB', 0), '0 MB');
  assert.equal(f('cache.entriesMillions', 0.003546), '3,546');
  assert.equal(f('cache.entriesMillions', 0.25), '250k');
  assert.equal(f('cache.entriesMillions', 1.5), '1.5M');
  assert.equal(f('cache.entriesMillions', 0), '0');
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

test('tip chip follows the p2p peer height reading', () => {
  const s = snap();
  s.tip = 900;
  s.peerTip = 950;

  s.chain.peerHeight = { v: 950, st: 'ok' };
  let bar = renderTopBar(s);
  assert.ok(texts(bar).includes('Tip 900, peer 950'));
  assert.equal(one(bar, 'data-chip', 'tip').attrs['data-st'], 'a');

  s.chain.peerHeight = { v: 900, st: 'ok' };
  bar = renderTopBar(s);
  assert.equal(one(bar, 'data-chip', 'tip').attrs['data-st'], 'g');

  s.chain.peerHeight = { v: 950, st: 'nodata', age: 45 };
  bar = renderTopBar(s);
  assert.ok(texts(bar).includes('Tip 900, peer no data, 45 s ago'));
  assert.equal(one(bar, 'data-chip', 'tip').attrs['data-st'], 'n');

  s.chain.peerHeight = { v: 0, st: 'ok', note: 'no peers' };
  bar = renderTopBar(s);
  assert.ok(texts(bar).includes('Tip 900, no peers'));
  assert.equal(one(bar, 'data-chip', 'tip').attrs['data-st'], 'n');
});

test('blocks waiting shows the true count and a more marker', () => {
  const s = snap();
  s.chain.blocksWaiting = Array.from({ length: 20 }, (_, i) => 1000 + i);
  s.chain.blocksWaitingCount = { v: 500, st: 'ok' };
  let t = texts(renderHome(s, NOW));
  assert.ok(t.includes('Blocks waiting (500)'));
  assert.ok(t.includes('+497 more'));

  s.chain.blocksWaitingCount = { v: 25, st: 'ok', note: '25 or more' };
  t = texts(renderHome(s, NOW));
  assert.ok(t.includes('Blocks waiting (25 or more)'));
  assert.ok(t.includes('+22 more'));

  s.chain.blocksWaiting = [7, 8];
  s.chain.blocksWaitingCount = { v: 2, st: 'ok' };
  t = texts(renderHome(s, NOW));
  assert.ok(t.includes('Blocks waiting (2)'));
  assert.ok(!t.some((x) => typeof x === 'string' && x.endsWith(' more')));

  delete s.chain.blocksWaitingCount;
  s.chain.blocksWaiting = [1, 2, 3, 4];
  t = texts(renderHome(s, NOW));
  assert.ok(t.includes('Blocks waiting (4)'));
  assert.ok(t.includes('+1 more'));
});

test('a failed chain source greys the fsm and tip with their age', () => {
  const s = snap();
  s.fsm = 'RUNNING';
  s.tip = 1000;
  s.chain.peerHeight = { v: 1000, st: 'ok' };
  s.sources = [{ name: 'metrics', ok: true }, { name: 'chain', ok: false, age: 45 }, { name: 'p2p', ok: true }];

  const bar = renderTopBar(s);
  const fsmChip = one(bar, 'data-chip', 'fsm');
  const tipChip = one(bar, 'data-chip', 'tip');
  assert.equal(fsmChip.attrs['data-st'], 'n');
  assert.equal(tipChip.attrs['data-st'], 'n');
  assert.ok(texts(bar).includes('FSM: no data, 45 s ago'));
  assert.ok(texts(bar).includes('Tip no data, 45 s ago'));
  assert.ok(!texts(bar).includes('FSM: RUNNING'));

  const home = texts(renderHome(s, NOW));
  assert.ok(home.includes('no data, 45 s ago'));
  assert.ok(!home.includes('RUNNING'));

  s.sources[1] = { name: 'chain', ok: true };
  const live = renderTopBar(s);
  assert.equal(one(live, 'data-chip', 'fsm').attrs['data-st'], 'g');
  assert.ok(texts(live).includes('FSM: RUNNING'));
});

test('a failed kafka source greys the lag with its age', () => {
  const s = snap();
  s.flows = s.flows.map((f) => (f.id === 'blocks' ? { ...f, lag: 3 } : f));
  s.sources = [{ name: 'kafka', ok: false, age: 75 }];
  let t = texts(renderHome(s, NOW));
  assert.ok(t.includes('lag no data, 1 min ago'), t.join('|'));
  assert.ok(!t.includes('lag 3'));

  s.sources = [{ name: 'kafka', ok: true }];
  t = texts(renderHome(s, NOW));
  assert.ok(t.includes('lag 3'));
});

test('a failed store source greys the tank instead of claiming not configured', () => {
  const s = snap();
  s.stores.postgres = { configured: false };
  s.sources = [{ name: 'postgres', ok: false, age: 30 }];
  const t = texts(renderHome(s, NOW));
  assert.ok(t.includes('no data, 30 s ago'), t.join('|'));

  s.sources = [];
  assert.ok(texts(renderHome(s, NOW)).includes('not configured'));
});

test('store tanks show reads and writes on separate lines', () => {
  const s = snap();
  s.sources = [];
  s.stores.aerospike = {
    configured: true,
    diskFill: { v: 0.71, st: 'ok' },
    sizeGB: { v: 1840, st: 'ok' },
    readP99Sec: { v: 0.0012, st: 'ok' },
    readsPerSec: { v: 31000, st: 'ok' },
    writesPerSec: { v: 4200, st: 'ok' },
    commitsPerSec: { v: 0, st: 'nodata' },
  };
  s.stores.postgres = {
    configured: true,
    diskFill: { v: 0.38, st: 'ok' },
    sizeGB: { v: 214, st: 'ok' },
    readP99Sec: { v: 0.004, st: 'ok', note: 'mean' },
    readsPerSec: { v: 1200, st: 'ok' },
    writesPerSec: { v: 300, st: 'ok' },
    commitsPerSec: { v: 40, st: 'ok' },
  };
  const home = renderHome(s, NOW);
  const aero = texts(one(home, 'data-store', 'aerospike'));
  for (const line of ['1.84 TB', 'Read p99 1.2 ms', 'Reads 31k/s', 'Writes 4,200/s']) assert.ok(aero.includes(line), `${line} in ${aero.join('|')}`);
  assert.ok(!aero.some((x) => typeof x === 'string' && x.includes('ops/s')));
  const pg = texts(one(home, 'data-store', 'postgres'));
  for (const line of ['214 GB', 'Query p99 4 ms', 'Reads 1,200/s', 'Writes 300/s', '40 commits/s']) assert.ok(pg.includes(line), `${line} in ${pg.join('|')}`);
  assert.ok(!pg.some((x) => typeof x === 'string' && x.includes('q/s')));

  s.stores.postgres.writesPerSec = { v: 0, st: 'nodata', age: 20 };
  const stale = texts(one(renderHome(s, NOW), 'data-store', 'postgres'));
  assert.ok(stale.includes('Writes –'), stale.join('|'));
  assert.ok(stale.includes('Writes: no data, 20 s ago'), stale.join('|'));

  s.sources = [{ name: 'aerospike', ok: false, age: 30 }];
  const down = texts(one(renderHome(s, NOW), 'data-store', 'aerospike'));
  assert.ok(down.includes('no data, 30 s ago'));
  assert.ok(!down.includes('Reads 31k/s'));
});

test('aerospike tank notes never reach the postgres tank', () => {
  const s = snap();
  s.sources = [];
  s.stores.postgres.configured = true;
  s.stores.aerospike = {
    configured: true,
    diskFill: { v: 0, st: 'nodata', age: 40 },
    sizeGB: { v: 0, st: 'nodata', age: 40 },
    readP99Sec: { v: 0, st: 'nodata', note: 'latency not exposed', age: 40 },
    readsPerSec: { v: 0, st: 'nodata', age: 40 },
    writesPerSec: { v: 0, st: 'nodata', age: 40 },
    commitsPerSec: { v: 0, st: 'nodata' },
  };
  const home = renderHome(s, NOW);
  const aero = one(home, 'data-store', 'aerospike');
  const pg = one(home, 'data-store', 'postgres');
  const pgLabel = walk(pg).find((n) => n.tag === 'text');
  const notes = walk(aero).filter((n) => n.tag === 'text' && n.attrs.class === 'wd');
  assert.ok(notes.length >= 1);
  for (const n of notes) assert.ok(Number(n.attrs.y) + 4 < Number(pgLabel.attrs.y) - 10, `note at ${n.attrs.y} reaches label at ${pgLabel.attrs.y}`);
  assert.ok(notes.some((n) => /^\+\d+ more in details$/.test(n.text)), notes.map((n) => n.text).join('|'));

  for (const key of ['diskFill', 'sizeGB', 'readP99Sec', 'readsPerSec', 'writesPerSec', 'commitsPerSec']) {
    s.stores.postgres[key] = { v: 0, st: 'nodata', note: 'a long explanation of why this reading has no data', age: 4000 };
  }
  const pgNotes = walk(one(renderHome(s, NOW), 'data-store', 'postgres')).filter((n) => n.tag === 'text' && n.attrs.class === 'wd');
  assert.ok(pgNotes.length <= 10);
  for (const n of pgNotes) assert.ok(Number(n.attrs.y) <= 510, `postgres note at ${n.attrs.y} leaves the panel`);

  const tankText = walk(aero).filter((n) => n.tag === 'text' && n.attrs.class !== 'wd' && n.attrs.class !== 'lb');
  const bottom = Math.max(...walk(aero).filter((n) => n.tag === 'ellipse').map((n) => Number(n.attrs.cy)));
  for (const n of tankText) assert.ok(Number(n.attrs.y) <= bottom, `line ${n.text} at ${n.attrs.y} below tank ${bottom}`);
});

test('stage view lists reads and writes rows for each store', () => {
  const s = snap();
  s.stores.postgres.configured = true;
  const view = renderStage(s, 'stores', null, NOW);
  for (const store of ['aerospike', 'postgres']) {
    const name = store === 'aerospike' ? 'Aerospike' : 'Postgres';
    assert.ok(texts(one(view, 'data-key', `stores.${store}.readsPerSec`)).includes(`${name}: Reads`));
    assert.ok(texts(one(view, 'data-key', `stores.${store}.writesPerSec`)).includes(`${name}: Writes`));
  }
  const reads = walk(one(view, 'data-key', 'stores.aerospike.readsPerSec')).filter((n) => n.tag === 'td');
  assert.equal(reads[0].text, '31k');
  assert.equal(walk(view).filter((n) => n.attrs && typeof n.attrs['data-key'] === 'string' && n.attrs['data-key'].endsWith('.opsPerSec')).length, 0);
});

test('drilldown path outside the stage allowlist renders as text', () => {
  const bad = [
    'javascript:alert(1)',
    '#/evil',
    '#/stage/unknown',
    '#/stage/cache/../x',
    'https://example.com',
    '//evil.example',
    '',
    `#/stage/${'a'.repeat(5000)}`,
    '#/stage/cache?x=1',
    '#/stage/Cache',
    ' #/stage/cache',
    '#/stage/cache ',
    '#/stage/cache\n',
    '#/stage/',
    '#/stage',
  ];
  for (const path of bad) {
    const alarm = { ...fixture.alarms[1], drilldowns: [{ label: 'Open details', path }] };
    for (const out of [renderAlarmList([alarm], NOW), renderAlarmPopover(alarm, NOW)]) {
      const nodes = walk(out);
      assert.equal(nodes.filter((n) => n.tag === 'a').length, 0, `anchor for ${JSON.stringify(path.slice(0, 40))}`);
      assert.equal(nodes.filter((n) => n.attrs && 'href' in n.attrs).length, 0, `href for ${JSON.stringify(path.slice(0, 40))}`);
      assert.ok(texts(out).includes('Open details'), `label kept as text for ${JSON.stringify(path.slice(0, 40))}`);
      const markup = toMarkupString(out);
      assert.ok(!markup.includes('href'), markup.slice(0, 200));
    }
  }
});

test('known stage paths render as anchors', () => {
  for (const stage of ['ingress', 'validator', 'cache', 'assembly', 'chain', 'stores']) {
    const path = `#/stage/${stage}`;
    const alarm = { ...fixture.alarms[1], drilldowns: [{ label: `Open ${stage}`, path }] };
    for (const out of [renderAlarmList([alarm], NOW), renderAlarmPopover(alarm, NOW)]) {
      const anchors = walk(out).filter((n) => n.tag === 'a');
      assert.equal(anchors.length, 1, stage);
      assert.equal(anchors[0].attrs.href, path);
      assert.equal(anchors[0].text, `Open ${stage}`);
    }
  }
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

const STAGE_KEYS = ['ingress', 'validator', 'cache', 'assembly', 'chain', 'stores'];
const READING_COUNTS = { ingress: 6, validator: 5, cache: 10, assembly: 6, chain: 3, stores: 6 };
const allAttrs = (node) => walk(node).flatMap((n) => Object.entries(n.attrs || {}));
const dataRows = (view) => walk(view).filter((n) => n.tag === 'tr' && n.attrs['data-key'] && !n.attrs['data-marker']);
const markerRows = (view) => walk(view).filter((n) => n.tag === 'tr' && n.attrs['data-marker']);

function nest(depth, leaf) {
  let o = leaf;
  for (let i = depth; i >= 1; i--) o = { [`l${i}`]: o };
  return o;
}

test('panelStatus and the table use the same readings', () => {
  for (const depth of [1, 3, 7, 8, 20]) {
    const s = snap();
    s.validator = { ok: { v: 1, st: 'ok' }, ...nest(depth, { deep: { v: 9, st: 'crit' } }) };
    assert.equal(panelStatus(s.validator), 'r', `depth ${depth}`);
    const walked = stageReadings(s.validator, 'validator').filter((r) => !r.marker);
    assert.equal(walked.length, 2);
    const rows = dataRows(renderStage(s, 'validator', null, NOW));
    assert.equal(rows.length, walked.length);
    const key = `validator.${Array.from({ length: depth }, (_, i) => `l${i + 1}`).join('.')}.deep`;
    assert.ok(rows.some((r) => r.attrs['data-key'] === key), key);
    assert.equal(one(renderStage(s, 'validator', null, NOW), 'data-role', 'stage-led').attrs['data-st'], 'r');
  }

  const cyclic = { a: { v: 1, st: 'warn' } };
  cyclic.self = cyclic;
  assert.equal(panelStatus(cyclic), 'a');
  assert.equal(stageReadings(cyclic, 'x').filter((r) => !r.marker).length, 1);

  const absurd = nest(5000, { deep: { v: 1, st: 'crit' } });
  assert.doesNotThrow(() => panelStatus(absurd));
  assert.equal(panelStatus(absurd), stageReadings(absurd, 'x').some((r) => !r.marker) ? 'r' : 'n');

  for (const key of STAGE_KEYS) {
    const stage = snap()[key];
    const fromWalker = stageReadings(stage, key).filter((r) => !r.marker).map((r) => r.reading.st);
    const worst = fromWalker.reduce((w, st) => ({ nodata: 0, idle: 0, ok: 1, warn: 2, crit: 3 }[st] > w ? { nodata: 0, idle: 0, ok: 1, warn: 2, crit: 3 }[st] : w), 0);
    assert.equal(panelStatus(stage), ['n', 'g', 'a', 'r'][worst], key);
  }
});

test('stage view shows a not configured row for an unconfigured store', () => {
  const view = renderStage(snap(), 'stores', null, NOW);
  const markers = markerRows(view);
  assert.equal(markers.length, 1);
  assert.equal(markers[0].attrs['data-key'], 'stores.postgres');
  assert.equal(markers[0].attrs['data-st'], 'n');
  const cells = walk(markers[0]).filter((n) => n.tag === 'th' || n.tag === 'td');
  assert.equal(cells[0].text, 'Postgres');
  assert.equal(cells[1].text, 'not configured');
  assert.equal(walk(markers[0]).filter((n) => n.tag === 'svg').length, 0);
  assert.equal(dataRows(view).length, 6);

  const s = snap();
  s.stores.aerospike.configured = false;
  assert.deepEqual(markerRows(renderStage(s, 'stores', null, NOW)).map((r) => walk(r).find((n) => n.tag === 'th').text), ['Aerospike', 'Postgres']);
  assert.equal(panelStatus(snap().stores), 'r');
  assert.equal(panelStatus({ postgres: { configured: false, x: { v: 1, st: 'crit' } } }), 'n');
});
const coords = (d) => [...d.matchAll(/[ML]\s*([-\d.]+)\s+([-\d.]+)/g)].map((m) => [Number(m[1]), Number(m[2])]);

function pt(minAgo, v, st = 'ok') {
  return { t: new Date(NOW - minAgo * 60000).toISOString(), v, st };
}

const spark = (points, extra = {}) => sparkline(points, { key: 'cache.hitRate', label: 'Hit rate', now: NOW, ...extra });

test('renderStage lists every reading of the stage', () => {
  for (const key of STAGE_KEYS) {
    const rows = dataRows(renderStage(snap(), key, null, NOW));
    assert.equal(rows.length, READING_COUNTS[key], key);
    for (const r of rows) assert.ok(r.attrs['data-key'].startsWith(`${key}.`), r.attrs['data-key']);
  }

  const s = snap();
  s.stores.postgres.configured = true;
  const rows = dataRows(renderStage(s, 'stores', null, NOW));
  assert.equal(rows.filter((r) => r.attrs['data-key'].startsWith('stores.postgres.')).length, 6);
  assert.equal(rows.length, 12);

  const view = renderStage(snap(), 'cache', null, NOW);
  assert.equal(walk(view).filter((n) => n.tag === 'table').length, 1);
  const heads = walk(view).filter((n) => n.tag === 'th' && n.attrs.scope === 'col');
  assert.deepEqual(heads.map((n) => n.text), ['Reading', 'Value', 'Status', 'Normal', 'Note', 'Last 10 minutes']);
  const hit = one(view, 'data-key', 'cache.hitRate');
  const cells = walk(hit).filter((n) => n.tag === 'th' || n.tag === 'td');
  assert.equal(cells[0].attrs.scope, 'row');
  assert.equal(cells[0].text, 'Hit rate');
  assert.equal(cells[1].text, '61%');
  assert.ok(texts(hit).includes('Critical'));
  assert.ok(texts(hit).includes('96%'));
  assert.equal(hit.attrs['data-st'], 'r');
  assert.equal(walk(view).find((n) => n.tag === 'h1').text, 'Tx meta cache');
  assert.equal(walk(view).find((n) => n.tag === 'a' && n.attrs.href === '#/').text, 'Back to overview');
  assert.equal(one(view, 'data-role', 'stage-led').attrs['data-st'], 'r');

  const aero = renderStage(snap(), 'stores', null, NOW);
  assert.ok(texts(one(aero, 'data-key', 'stores.aerospike.diskFill')).includes('Aerospike: Disk fill'));
  const unlabeled = snap();
  unlabeled.cache.mystery = { v: 5, st: 'ok' };
  assert.ok(texts(one(renderStage(unlabeled, 'cache', null, NOW), 'data-key', 'cache.mystery')).includes('mystery'));
});

test('renderStage shows alarms of the stage only', () => {
  const cache = renderStage(snap(), 'cache', null, NOW);
  const items = walk(cache).filter((n) => n.attrs && n.attrs['data-alarm-id']);
  assert.deepEqual(items.map((n) => n.attrs['data-alarm-id']), ['cache-hit-rate']);
  assert.ok(walk(cache).some((n) => n.attrs && n.attrs['data-action'] === 'snooze' && n.attrs['data-id'] === 'cache-hit-rate'));

  const ingress = renderStage(snap(), 'ingress', null, NOW);
  assert.ok(walk(ingress).some((n) => n.attrs && n.attrs['data-action'] === 'unsnooze'));

  const assembly = renderStage(snap(), 'assembly', null, NOW);
  assert.ok(texts(assembly).includes('No alarms for this stage.'));
  assert.equal(walk(assembly).filter((n) => n.attrs && n.attrs['data-alarm-id']).length, 0);
});

test('sparkline handles a gap in history', () => {
  const svg = spark([pt(9, 10), pt(8, 12), pt(7, 11), pt(6, null, 'nodata'), pt(5, 20), pt(4, 22), pt(3, 21)]);
  assert.equal(svg.tag, 'svg');
  const paths = walk(svg).filter((n) => n.tag === 'path');
  assert.equal(paths.length, 1);
  const d = paths[0].attrs.d;
  assert.equal((d.match(/M/g) || []).length, 2, d);
  assert.equal((d.match(/L/g) || []).length, 4, d);
  const segments = d.split('M').filter(Boolean).map((seg) => coords(`M${seg}`));
  assert.equal(segments.length, 2);
  assert.equal(segments[0].length, 3);
  assert.equal(segments[1].length, 3);
  const gapStart = segments[0][2][0];
  const gapEnd = segments[1][0][0];
  const expectedGapX = (4 / 10) * 120;
  assert.ok(gapStart < expectedGapX && gapEnd > expectedGapX, `${gapStart} ${expectedGapX} ${gapEnd}`);
  const ys = segments.flat().map((c) => c[1]);
  assert.ok(Math.max(...ys) <= 28 && Math.min(...ys) >= 0);
  assert.ok(segments[1][2][1] < segments[0][0][1]);
});

test('sparkline with fewer than two points renders a dash', () => {
  for (const points of [[], [pt(1, 5)], [pt(2, null, 'nodata'), pt(1, null, 'nodata')], [pt(2, 5), pt(1, null, 'nodata')], null, 'x']) {
    const out = spark(points);
    assert.notEqual(out.tag, 'svg');
    assert.equal(out.text, '–');
    assert.equal(walk(out).filter((n) => n.tag === 'path').length, 0);
  }
});

test('sparkline flat series is a flat line', () => {
  const svg = spark([pt(9, 7), pt(6, 7), pt(3, 7), pt(1, 7)]);
  const ys = coords(walk(svg).find((n) => n.tag === 'path').attrs.d).map((c) => c[1]);
  assert.equal(ys.length, 4);
  assert.ok(ys.every((y) => y === ys[0]));
  assert.equal(ys[0], 14);
  assert.equal(walk(svg).find((n) => n.tag === 'circle').attrs.cy, '14');
});

test('sparkline scales x over the window and y to min and max', () => {
  const pts = coords(walk(spark([pt(10, 0), pt(5, 50), pt(0, 100)])).find((n) => n.tag === 'path').attrs.d);
  assert.equal(pts.length, 3);
  assert.ok(pts[0][0] < pts[1][0] && pts[1][0] < pts[2][0]);
  assert.ok(Math.abs((pts[1][0] - pts[0][0]) - (pts[2][0] - pts[1][0])) < 0.01);
  assert.ok(pts[0][1] > pts[1][1] && pts[1][1] > pts[2][1]);
  assert.ok(pts[0][1] < 28 && pts[2][1] > 0);
  assert.ok(Math.abs(pts[1][1] - 14) < 0.01);
});

test('sparkline takes the status class and label of the latest point', () => {
  const svg = spark([pt(9, 0.9), pt(1, 0.61, 'crit')]);
  assert.match(svg.attrs.class, /\bst-crit\b/);
  assert.equal(svg.attrs.role, 'img');
  assert.equal(svg.attrs['aria-label'], '10 minute history of Hit rate, last value 61%');
  assert.equal(walk(svg).filter((n) => n.tag === 'circle').length, 1);
  assert.match(spark([pt(9, 1), pt(1, 2, '<x>')]).attrs.class, /\bst-nodata\b/);
  for (const st of ['ok', 'warn', 'idle', 'nodata']) {
    assert.match(spark([pt(9, 1), pt(1, 2, st)]).attrs.class, new RegExp(`\\bst-${st}\\b`));
  }
});

test('sparkline never emits NaN or Infinity', () => {
  const bad = [
    pt(9, 'abc'), pt(8, NaN), pt(7, Infinity), pt(6, -Infinity), pt(5, 1e308), pt(4, -1e308), pt(3, -5), pt(2, 1.7976931348623157e308),
    { t: 'not a date', v: 3, st: 'ok' }, { t: 12, v: 3 }, null, 7, { v: 4 }, pt(1, 5e-324), pt(0.5, -0),
  ];
  for (const opts of [{ key: 'cache.hitRate' }, { key: 'validator.latencyP99Sec', label: 'x' }, { key: 'nope', label: 'x' }, { key: 'cache.entriesMillions' }, { key: 'cache.sizeGB' }, { key: 'assembly.tipLagBlocks' }]) {
    const out = spark(bad, opts);
    assert.equal(out.tag, 'svg');
    for (const [k, v] of allAttrs(out)) assert.doesNotMatch(String(v), /NaN|Infinity|undefined|null/, `${k}=${v}`);
    assert.doesNotMatch(toMarkupString(out), /NaN|Infinity/);
  }
  const noOpts = sparkline(bad, {});
  assert.equal(noOpts.tag, 'svg');
  for (const [k, v] of allAttrs(noOpts)) assert.doesNotMatch(String(v), /NaN|Infinity|undefined|null/, `${k}=${v}`);
  assert.equal(sparkline(bad).tag, 'svg');
  assert.equal(spark([pt(2, 1e308), pt(1, -1e308)]).tag, 'svg');
  assert.notEqual(spark([pt(2, '5'), pt(1, '6')]).tag, 'svg');
});

test('sparkline drops points older than the window', () => {
  const d = walk(spark([pt(30, 1), pt(25, 2), pt(1, 3), pt(0.5, 4)])).find((n) => n.tag === 'path').attrs.d;
  assert.equal((d.match(/L/g) || []).length, 1);
});

test('parseRoute accepts only the home page and known stages', () => {
  assert.deepEqual(parseRoute(''), { name: 'home', stage: '' });
  assert.deepEqual(parseRoute('#/'), { name: 'home', stage: '' });
  for (const k of STAGE_KEYS) assert.deepEqual(parseRoute(`#/stage/${k}`), { name: 'stage', stage: k });
});

test('unknown hash falls back to home', () => {
  const hostile = ['#', '#/stage', '#/stage/', '#/stage/unknown', '#/stage/Cache', '#/stage/cache/', '#/stage/cache?x=1', '#/stage/cache\n',
    '#/stage/__proto__', '#/stage/constructor', 'javascript:alert(1)', '#/<script>', `#/stage/${'a'.repeat(5000)}`, null, undefined, 5, {}, '#/stages/cache'];
  for (const h of hostile) assert.deepEqual(parseRoute(h), { name: 'home', stage: '' }, String(h).slice(0, 30));
});

test('alarm link path resolves to an existing stage', () => {
  const s = snap();
  const paths = s.alarms.flatMap((a) => a.drilldowns.map((d) => d.path));
  assert.ok(paths.length >= 4);
  for (const p of paths) {
    const route = parseRoute(p);
    assert.equal(route.name, 'stage', p);
    assert.ok(STAGES.some((st) => st.key === route.stage), p);
    assert.ok(walk(renderStage(s, route.stage, null, NOW)).some((n) => n.tag === 'table'), p);
  }
});

test('stage title link is keyboard reachable', () => {
  const home = renderHome(snap(), NOW);
  for (const stage of STAGES) {
    const panel = one(home, 'data-stage', stage.key);
    const links = walk(panel).filter((n) => n.tag === 'a');
    assert.equal(links.length, 1, stage.key);
    assert.equal(links[0].attrs.href, `#/stage/${stage.key}`);
    assert.ok(!('tabindex' in links[0].attrs));
    assert.ok(texts(links[0]).includes(stage.title));
    assert.match(links[0].attrs.class, /ti-link/);
    assert.equal(parseRoute(links[0].attrs.href).stage, stage.key);
  }
});

test('stage view escapes text from the snapshot', () => {
  const s = snap();
  s.alarms[1].title = '<script>alert(1)</script>';
  s.alarms[1].body = '<img src=x onerror=alert(1)>';
  s.cache.weird = { v: 1, st: 'ok', note: '<b>n</b>' };
  s.cache['<img src=x>'] = { v: 1, st: 'ok' };
  const view = renderStage(s, 'cache', null, NOW);
  for (const n of walk(view)) {
    assert.notEqual(n.tag, 'script');
    assert.notEqual(n.tag, 'img');
    assert.notEqual(n.tag, 'b');
    for (const k of Object.keys(n.attrs || {})) assert.doesNotMatch(k, /^on/i);
  }
  const markup = toMarkupString(view);
  assert.ok(!markup.includes('<script'));
  assert.ok(!markup.includes('<img'));
  assert.ok(!markup.includes('<b>'));
  assert.ok(markup.includes('&lt;script&gt;alert(1)&lt;/script&gt;'));
  assert.ok(texts(view).includes('<script>alert(1)</script>'));
});

test('hostile history payload is ignored', () => {
  const good = { stage: 'cache', series: [{ key: 'cache.hitRate', points: [pt(2, 1), pt(1, 2)] }] };
  assert.equal(normalizeHistory(good, 'cache').get('cache.hitRate').length, 2);
  for (const bad of [null, undefined, 'x', 5, [], {}, { stage: 'cache' }, { stage: 'cache', series: 'x' }, { stage: 'validator', series: good.series }, { series: good.series }]) {
    assert.equal(normalizeHistory(bad, 'cache'), null, JSON.stringify(bad));
  }

  const mixed = normalizeHistory({
    stage: 'cache',
    series: [
      null, 3, 'x', { key: 7, points: [] }, { key: 'cache.a' }, { key: 'cache.b', points: 'x' },
      { key: 'validator.latencyP99Sec', points: [pt(1, 1), pt(2, 2)] },
      { key: '<img src=x onerror=alert(1)>', points: [pt(1, 1), pt(2, 2)] },
      { key: `cache.${'k'.repeat(500)}`, points: [pt(1, 1), pt(2, 2)] },
      { key: 'cache.ok', points: [pt(3, 1), null, 5, { t: 'bad', v: 1 }, { t: pt(1, 1).t, v: '9' }, { t: pt(0.5, 1).t, v: 2, st: 'bogus' }] },
    ],
  }, 'cache');
  assert.deepEqual([...mixed.keys()], ['cache.ok']);
  const ok = mixed.get('cache.ok');
  assert.equal(ok.length, 3);
  assert.equal(ok[1].v, null);
  assert.equal(ok[2].v, 2);
  assert.equal(ok[2].st, 'nodata');

  const many = normalizeHistory({
    stage: 'cache',
    series: Array.from({ length: 5000 }, (_, i) => ({ key: `cache.k${i}`, points: [pt(2, 1), pt(1, 2)] })),
  }, 'cache');
  assert.equal(many.size, 64);

  const long = normalizeHistory({
    stage: 'cache',
    series: [{ key: 'cache.hitRate', points: Array.from({ length: 100000 }, (_, i) => ({ t: new Date(NOW - (100000 - i) * 1000).toISOString(), v: i, st: 'ok' })) }],
  }, 'cache').get('cache.hitRate');
  assert.equal(long.length, 600);
  assert.equal(long[599].v, 99999);

  const hostile = normalizeHistory({
    stage: 'cache',
    series: [{ key: '<img src=x>', points: [pt(2, 1), pt(1, 2)] }, { key: 'cache.hitRate', points: [pt(2, 1), pt(1, 2)] }],
  }, 'cache');
  assert.deepEqual([...hostile.keys()], ['cache.hitRate']);
  const view = renderStage(snap(), 'cache', { data: hostile, error: false }, NOW);
  assert.ok(!toMarkupString(view).includes('<img'));
  assert.equal(walk(view).filter((n) => n.tag === 'svg' && /spark/.test(n.attrs.class)).length, 1);
});

test('stage view shows history unavailable calmly', () => {
  assert.ok(texts(renderStage(snap(), 'cache', { data: null, error: true }, NOW)).includes('History unavailable.'));
  assert.ok(!texts(renderStage(snap(), 'cache', { data: null, error: false }, NOW)).includes('History unavailable.'));
});

test('parseRoute knows the lanes view', () => {
  assert.deepEqual(parseRoute('#/lanes'), { name: 'lanes', stage: '' });
  assert.deepEqual(parseRoute('#/lanes/x'), { name: 'home', stage: '' });
});

test('lanes view shows four lanes then the shared panels', () => {
  const view = renderLanes(snap());
  const panels = walk(view).filter((n) => n.attrs && n.attrs['data-stage']);
  assert.deepEqual(panels.map((p) => p.attrs['data-stage']), ['lane-tx', 'lane-subtrees', 'lane-blocks', 'lane-mining', 'cache', 'stores', 'lane-blobs', 'lane-kafka']);
});

const kafkaUp = () => {
  const s = snap();
  s.sources.find((x) => x.name === 'kafka').ok = true;
  return s;
};

test('lanes view shows both blob stores with their readings', () => {
  const view = renderLanes(snap());
  const value = (key) => texts(one(view, 'data-key', key));
  assert.ok(value('blobs.subtree.readsPerSec').includes('410'));
  assert.ok(value('blobs.subtree.writesPerSec').includes('198'));
  assert.ok(value('blobs.subtree.errorsPerSec').includes('0'));
  assert.ok(value('blobs.block.readsPerSec').includes('2.5'));
  const subtree = one(view, 'data-blob', 'subtree');
  const block = one(view, 'data-blob', 'block');
  assert.ok(texts(subtree).includes('Subtree store'));
  assert.ok(texts(block).includes('Block store'));
});

test('lanes view shows no data for a blob reading without samples', () => {
  const view = renderLanes(snap());
  const t = texts(one(view, 'data-key', 'blobs.block.readP99Sec'));
  assert.ok(t.includes('–'));
  assert.ok(t.includes('no data'));
});

test('lanes view marks the blob panel grey when the node exports no blob metrics', () => {
  const s = snap();
  for (const store of ['subtree', 'block']) {
    for (const field of Object.keys(s.blobs[store])) s.blobs[store][field] = { v: 0, st: 'nodata', note: 'no data' };
  }
  const panel = one(renderLanes(s), 'data-stage', 'lane-blobs');
  assert.equal(panel.attrs['data-st'], 'n');
});

test('lanes view lists every Kafka topic in the order the probe sends', () => {
  const view = renderLanes(kafkaUp());
  const rows = walk(view).filter((n) => n.attrs && n.attrs['data-topic']);
  assert.deepEqual(rows.map((r) => r.attrs['data-topic']), ['subtrees', 'txmeta', 'blocks', 'rejectedtx']);
  assert.deepEqual(rows.map((r) => r.attrs['data-lag']), ['1840', '0', '0', undefined]);
  assert.ok(texts(rows[0]).includes('1,840'));
  assert.ok(texts(rows[3]).includes('no consumers'));
  assert.equal(one(view, 'data-stage', 'lane-kafka').attrs['data-st'], 'g');
});

test('lanes view draws a lag bar only for a topic that is behind', () => {
  const rows = walk(renderLanes(kafkaUp())).filter((n) => n.attrs && n.attrs['data-topic']);
  const bars = (row) => walk(row).filter((n) => n.tag === 'rect' && n.attrs.fill === '#5b8fc7').length;
  assert.equal(bars(rows[0]), 1);
  assert.equal(bars(rows[1]), 0);
  assert.equal(bars(rows[3]), 0);
});

test('lanes view shows a Kafka outage instead of stale lag', () => {
  const view = renderLanes(snap());
  assert.equal(walk(view).filter((n) => n.attrs && n.attrs['data-topic']).length, 0);
  assert.ok(texts(one(view, 'data-kafka', 'outage')).includes('Kafka no data, 34 s ago'));
  assert.equal(one(view, 'data-stage', 'lane-kafka').attrs['data-st'], 'n');
});

test('lanes view says so when no Kafka topic is watched', () => {
  const s = kafkaUp();
  s.kafka = [];
  const view = renderLanes(s);
  assert.ok(texts(one(view, 'data-kafka', 'none')).includes('No Kafka topics are watched on this node.'));
  assert.equal(one(view, 'data-stage', 'lane-kafka').attrs['data-st'], 'n');
});

test('lanes view grows taller with more Kafka topics', () => {
  const few = kafkaUp();
  const many = kafkaUp();
  for (let i = 0; i < 12; i++) many.kafka.push({ topic: `extra-${i}`, lag: i });
  const height = (s) => Number(renderLanes(s).attrs.viewBox.split(' ')[3]);
  assert.ok(height(many) > height(few));
});

test('lanes view ignores malformed Kafka and blob data', () => {
  const s = kafkaUp();
  s.kafka = [null, 7, { topic: 5, lag: 1 }, { topic: '', lag: 1 }, { topic: 'odd', lag: 'x' }, { topic: 'neg', lag: -4 }, { topic: '<b>', lag: 3 }];
  s.blobs = 'nope';
  const view = renderLanes(s);
  const rows = walk(view).filter((n) => n.attrs && n.attrs['data-topic']);
  assert.deepEqual(rows.map((r) => r.attrs['data-topic']), ['odd', 'neg', '<b>']);
  assert.deepEqual(rows.map((r) => r.attrs['data-lag']), [undefined, '0', '3']);
  assert.ok(texts(one(view, 'data-key', 'blobs.subtree.readsPerSec')).includes('–'));
  s.kafka = 'nope';
  assert.ok(texts(one(renderLanes(s), 'data-kafka', 'none')).length > 0);
});

test('lane panels have no drilldown link but the shared panels do', () => {
  const view = renderLanes(snap());
  const hrefs = walk(view).filter((n) => n.tag === 'a').map((n) => n.attrs.href);
  assert.deepEqual(hrefs, ['#/stage/cache', '#/stage/stores']);
});

test('lanes view formats each lane reading', () => {
  const view = renderLanes(snap());
  const value = (key) => texts(one(view, 'data-key', key));
  assert.ok(value('lanes.subtrees.completedPerSec').includes('198'));
  assert.ok(value('lanes.subtrees.txsFetchedAvg').includes('9.4'));
  assert.ok(value('lanes.blocks.announcedTotal').includes('6'));
  assert.ok(value('lanes.blocks.tip').includes('912,437'));
  assert.ok(value('lanes.mining.candidatesPerSec').includes('0.2'));
  assert.ok(value('ingress.propagationTxPerSec').includes('4,100'));
});

test('lanes view shows the skipped, missing-subtree and candidate-age readings', () => {
  const view = renderLanes(snap());
  const value = (key) => texts(one(view, 'data-key', key));
  assert.ok(value('lanes.tx.alreadySeenPerSec').includes('310'));
  assert.ok(value('lanes.blocks.missingSubtrees').includes('0'));
  assert.ok(value('lanes.mining.candidateAgeSec').some((t) => t.includes('4')));
});

test('lanes view shows no data when a node does not export a lanes metric', () => {
  const s = snap();
  for (const path of [['tx', 'alreadySeenPerSec'], ['blocks', 'missingSubtrees'], ['blocks', 'missingSubtreesPerSec'], ['mining', 'candidateAgeSec']]) {
    s.lanes[path[0]][path[1]] = { v: 0, st: 'nodata', note: 'no data' };
  }
  const view = renderLanes(s);
  for (const key of ['lanes.tx.alreadySeenPerSec', 'lanes.blocks.missingSubtrees', 'lanes.mining.candidateAgeSec']) {
    const t = texts(one(view, 'data-key', key));
    assert.ok(t.includes('–'), key);
    assert.ok(t.includes('no data'), key);
  }
  assert.equal(one(view, 'data-flow', 'missing-subtrees').attrs['data-st'], 'n');
});

test('lanes view draws the two dependency arrows', () => {
  const view = renderLanes(snap());
  const missingTxs = one(view, 'data-flow', 'missing-txs');
  assert.ok(texts(missingTxs).includes('txs'));
  assert.ok(texts(missingTxs).includes('1,950/s'));
  const missingSubtrees = one(view, 'data-flow', 'missing-subtrees');
  assert.ok(texts(missingSubtrees).includes('subtrees'));
  assert.ok(texts(missingSubtrees).includes('0/s'));
});

test('lanes view animates an arrow only while work flows', () => {
  const animated = (view) => walk(view).filter((n) => n.attrs && typeof n.attrs.class === 'string' && n.attrs.class.startsWith('flow')).length;
  assert.equal(animated(renderLanes(snap())), 1);
  const s = snap();
  s.lanes.subtrees.missingTxsPerSec.v = 0;
  assert.equal(animated(renderLanes(s)), 0);
});

test('lanes view copes with a missing or hostile snapshot', () => {
  assert.ok(texts(renderLanes(null)).includes('Waiting for the first sample.'));
  const s = snap();
  s.lanes = 'x';
  s.tip = 'x';
  s.ingress = null;
  assert.doesNotThrow(() => renderLanes(s));
  assert.doesNotThrow(() => renderLanes({}));
});

test('lanes view escapes text from the snapshot', () => {
  const s = snap();
  s.lanes.subtrees.completedPerSec = { v: 1, st: 'ok', note: '<img src=x onerror=alert(1)>' };
  assert.ok(!toMarkupString(renderLanes(s)).includes('<img'));
});

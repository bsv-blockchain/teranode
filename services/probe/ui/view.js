export const COLORS = { g: '#4cc38a', a: '#e5b44d', r: '#ef5f5f', n: '#56626f' };

const LIQUID = { g: 'lgG', a: 'lgA', r: 'lgR', n: 'lgN' };
const RANK = { n: 0, g: 1, a: 2, r: 3 };

export const STATUS_LETTERS = { ok: 'g', warn: 'a', crit: 'r', idle: 'n', nodata: 'n' };
export const STATUS_WORDS = { ok: '', warn: 'Warning', crit: 'Critical', idle: 'Idle', nodata: 'no data' };
export const PANEL_WORDS = { g: 'normal', a: 'warning', r: 'critical', n: 'no data' };
export const SEVERITY_WORDS = { crit: 'Critical', warn: 'Warning' };
const SEVERITY_RANK = { crit: 2, warn: 1 };

export const STAGES = [
  { key: 'ingress', title: 'Ingress' },
  { key: 'validator', title: 'Validator' },
  { key: 'cache', title: 'Tx meta cache' },
  { key: 'assembly', title: 'Block Assembly' },
  { key: 'chain', title: 'Blockchain' },
  { key: 'stores', title: 'Data stores' },
];

export const UNITS = {
  'ingress.propagationTxPerSec': 'rate',
  'ingress.subtreeIntakePerSec': 'rate',
  'ingress.peers': 'count',
  'ingress.rejectedPerSec': 'rate',
  'ingress.subtreeQueue': 'count',
  'ingress.subtreeQueueOldestSec': 'seconds',
  'validator.validatedPerSec': 'rate',
  'validator.latencyP99Sec': 'seconds',
  'validator.queueFill': 'fraction',
  'validator.spendBatchFill': 'fraction',
  'validator.errorsPerSec': 'rate',
  'cache.hitRate': 'fraction',
  'cache.missesPerSec': 'rate',
  'cache.evictionsPerSec': 'rate',
  'cache.insertionsPerSec': 'rate',
  'cache.fill': 'fraction',
  'cache.currentGenFrac': 'fraction',
  'cache.previousGenFrac': 'fraction',
  'cache.entriesMillions': 'millions',
  'cache.sizeGB': 'gb',
  'cache.capacityGB': 'gb',
  'assembly.paused': 'state',
  'assembly.tipLagBlocks': 'blocks',
  'assembly.queueHeadAgeSec': 'seconds',
  'assembly.transactions': 'compact',
  'lanes.tx.alreadySeenPerSec': 'rate',
  'lanes.subtrees.alreadyKnownPerSec': 'rate',
  'lanes.subtrees.completedPerSec': 'rate',
  'lanes.subtrees.missingTxsPerSec': 'rate',
  'lanes.subtrees.txsFetchedAvg': 'rate',
  'lanes.blocks.announcedTotal': 'count',
  'lanes.blocks.validatedTotal': 'count',
  'lanes.blocks.missingSubtrees': 'count',
  'lanes.blocks.missingSubtreesPerSec': 'rate',
  'lanes.blocks.tip': 'count',
  'lanes.mining.candidatesPerSec': 'rate',
  'lanes.mining.solutionsTotal': 'count',
  'lanes.mining.candidateAgeSec': 'seconds',
  'blobs.subtree.readsPerSec': 'rate',
  'blobs.subtree.writesPerSec': 'rate',
  'blobs.subtree.errorsPerSec': 'rate',
  'blobs.subtree.readP99Sec': 'seconds',
  'blobs.subtree.writeP99Sec': 'seconds',
  'blobs.block.readsPerSec': 'rate',
  'blobs.block.writesPerSec': 'rate',
  'blobs.block.errorsPerSec': 'rate',
  'blobs.block.readP99Sec': 'seconds',
  'blobs.block.writeP99Sec': 'seconds',
  'assembly.ingestQueueFill': 'fraction',
  'assembly.subtreeFill': 'fraction',
  'chain.forkDepth': 'blocks',
  'chain.persisterBehind': 'blocks',
  'chain.subscribersDropped': 'count',
  'chain.peerHeight': 'count',
  'chain.blocksWaitingCount': 'count',
  'stores.aerospike.diskFill': 'fraction',
  'stores.aerospike.sizeGB': 'gb',
  'stores.aerospike.readP99Sec': 'seconds',
  'stores.aerospike.readsPerSec': 'compact',
  'stores.aerospike.writesPerSec': 'compact',
  'stores.aerospike.commitsPerSec': 'compact',
  'stores.postgres.diskFill': 'fraction',
  'stores.postgres.sizeGB': 'gb',
  'stores.postgres.readP99Sec': 'seconds',
  'stores.postgres.readsPerSec': 'compact',
  'stores.postgres.writesPerSec': 'compact',
  'stores.postgres.commitsPerSec': 'compact',
};

export const LABELS = {
  'ingress.propagationTxPerSec': 'Propagation, tx/s',
  'ingress.subtreeIntakePerSec': 'Subtree intake, per s',
  'ingress.peers': 'Connected peers',
  'ingress.rejectedPerSec': 'Rejected, per s',
  'ingress.subtreeQueue': 'Subtree queue',
  'ingress.subtreeQueueOldestSec': 'Oldest waiting',
  'validator.validatedPerSec': 'Validated, tx/s',
  'validator.latencyP99Sec': 'Latency p99',
  'validator.queueFill': 'Internal queue',
  'validator.spendBatchFill': 'UTXO spend batch fill',
  'validator.errorsPerSec': 'Errors, per s',
  'cache.hitRate': 'Hit rate',
  'cache.missesPerSec': 'Misses, per s',
  'cache.evictionsPerSec': 'Evictions, per s',
  'cache.insertionsPerSec': 'Inserts, per s',
  'cache.fill': 'Fill',
  'cache.currentGenFrac': 'Current gen',
  'cache.previousGenFrac': 'Previous gen',
  'cache.entriesMillions': 'Entries',
  'cache.sizeGB': 'Size',
  'cache.capacityGB': 'Capacity',
  'assembly.paused': 'State',
  'assembly.tipLagBlocks': 'Tip lag',
  'assembly.queueHeadAgeSec': 'Queue head age',
  'assembly.transactions': 'Transactions held',
  'lanes.tx.alreadySeenPerSec': 'Already seen (skipped), per s',
  'lanes.subtrees.alreadyKnownPerSec': 'Already known (ignored), per s',
  'lanes.subtrees.completedPerSec': 'Completed, per s',
  'lanes.subtrees.missingTxsPerSec': 'Missing txs fetched, per s',
  'lanes.subtrees.txsFetchedAvg': 'Missing txs per subtree',
  'lanes.blocks.announcedTotal': 'Announced, since start',
  'lanes.blocks.validatedTotal': 'Validated, since start',
  'lanes.blocks.missingSubtrees': 'Missing subtrees outstanding',
  'lanes.blocks.missingSubtreesPerSec': 'Missing subtrees fetched, per s',
  'lanes.blocks.tip': 'Tip height',
  'lanes.mining.candidatesPerSec': 'Candidates served, per s',
  'lanes.mining.solutionsTotal': 'Solutions found, since start',
  'lanes.mining.candidateAgeSec': 'Candidate age',
  'assembly.ingestQueueFill': 'Ingest queue',
  'assembly.subtreeFill': 'Current subtree fill',
  'chain.fsm': 'FSM state',
  'chain.forkDepth': 'Fork depth',
  'chain.blocksWaiting': 'Blocks waiting',
  'chain.persisterBehind': 'Persister behind',
  'chain.subscribersDropped': 'Subscribers dropped',
  'chain.peerHeight': 'Best peer height',
  'chain.blocksWaitingCount': 'Blocks waiting, count',
  'stores.aerospike': 'Aerospike',
  'stores.postgres': 'Postgres',
  'stores.aerospike.readP99Sec': 'Read p99',
  'stores.postgres.readP99Sec': 'Query p99',
  'stores.aerospike.diskFill': 'Disk fill',
  'stores.postgres.diskFill': 'Disk fill',
  'stores.aerospike.sizeGB': 'Size',
  'stores.postgres.sizeGB': 'Size',
  'stores.aerospike.readsPerSec': 'Reads',
  'stores.aerospike.writesPerSec': 'Writes',
  'stores.postgres.readsPerSec': 'Reads',
  'stores.postgres.writesPerSec': 'Writes',
  'stores.aerospike.commitsPerSec': 'Commits',
  'stores.postgres.commitsPerSec': 'Commits',
};

export const STORE_RATE_SUFFIX = {
  'stores.aerospike.commitsPerSec': ' commits/s',
  'stores.postgres.commitsPerSec': ' commits/s',
};

export const STORE_RATE_PREFIXED = {
  'stores.aerospike.readsPerSec': true,
  'stores.aerospike.writesPerSec': true,
  'stores.postgres.readsPerSec': true,
  'stores.postgres.writesPerSec': true,
};

export const FLOW_PLACEMENT = {
  'tx-input': { gap: 0, slot: 0 },
  subtrees: { gap: 0, slot: 1 },
  txmeta: { gap: 0, slot: 2 },
  lookups: { gap: 1, slot: 0 },
  validated: { gap: 2, slot: 0 },
  blocks: { gap: 3, slot: 0 },
  'blocks-final': { gap: 3, slot: 1 },
  'utxo-ops': { gap: 4, slot: 0 },
  'block-writes': { gap: 4, slot: 1 },
};

export const ALARM_ANCHORS = {
  'cache-hit-rate': 'cache.hitRate',
  'aerospike-read-p99': 'stores.aerospike',
  'validator-latency-p99': 'validator.latencyP99Sec',
  'validator-queue-fill': 'validator.queueFill',
  'assembly-queue-head-age': 'assembly.queueHeadAgeSec',
  'assembly-tip-lag': 'assembly.tipLagBlocks',
  'subtree-queue-age': 'ingress.subtreeQueue',
  'persister-behind': 'chain.persisterBehind',
};

export const COUNT_METER_FULL = 10000;
export const SNOOZE_MINUTES = 15;
export const MAX_BACKOFF_MS = 30000;

const PX = 170;
const GAP = 70;
const X0 = 5;
const PY = 60;
const PH = 520;
const VIEW = { x: 0, y: 48, w: 1380, h: 536 };
const POP = { x: 231, y: 408, w: 640, h: 168 };
const STRIP_PITCH = 70;
const px = (i) => X0 + i * (PX + GAP);

export function nextBackoff(prevMs) {
  if (!(prevMs > 0)) return 1000;
  return Math.min(prevMs * 2, MAX_BACKOFF_MS);
}

export function flowPlacement(flowId) {
  return Object.prototype.hasOwnProperty.call(FLOW_PLACEMENT, flowId) ? FLOW_PLACEMENT[flowId] : null;
}

function num(n) {
  return String(Math.round(n * 10000) / 10000);
}

function cleanAttrs(attrs) {
  const out = {};
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === undefined || v === null || v === false || v === '') continue;
    out[k] = typeof v === 'number' ? num(v) : String(v);
  }
  return out;
}

export function h(tag, attrs, children) {
  if (typeof children === 'string') return { tag, attrs: cleanAttrs(attrs), children: [], text: children };
  return { tag, attrs: cleanAttrs(attrs), children: (children || []).filter(Boolean) };
}

const ESCAPES = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' };
const escapeText = (s) => String(s).replace(/[&<>"']/g, (c) => ESCAPES[c]);

export function toMarkupString(vnode) {
  if (!vnode) return '';
  const attrs = Object.entries(vnode.attrs || {}).map(([k, v]) => ` ${k}="${escapeText(v)}"`).join('');
  const inner = typeof vnode.text === 'string' ? escapeText(vnode.text) : (vnode.children || []).map(toMarkupString).join('');
  return `<${vnode.tag}${attrs}>${inner}</${vnode.tag}>`;
}

function group(n) {
  const r = Math.round(n);
  return String(r).replace(/\B(?=(\d{3})+(?!\d))/g, ',');
}

function trim(s) {
  return s.includes('.') ? s.replace(/0+$/, '').replace(/\.$/, '') : s;
}

function finite(v) {
  return typeof v === 'number' && Number.isFinite(v) ? v : 0;
}

function compact(v) {
  if (v >= 1e6) return `${trim((v / 1e6).toFixed(1))}M`;
  if (v >= 1e5) return `${Math.round(v / 1e3)}k`;
  if (v >= 1e4) return `${trim((v / 1e3).toFixed(1))}k`;
  if (v >= 10 || Number.isInteger(v)) return group(v);
  return trim(v.toFixed(1));
}

export function formatSeconds(v) {
  if (v < 1) {
    const ms = v * 1000;
    return ms < 10 ? `${trim(ms.toFixed(1))} ms` : `${Math.round(ms)} ms`;
  }
  if (v < 60) return `${Math.round(v)} s`;
  if (v < 3600) return `${Math.round(v / 60)} min`;
  return `${trim((v / 3600).toFixed(1))} h`;
}

function formatGB(v) {
  if (v >= 1000) return `${trim((v / 1000).toFixed(2))} TB`;
  if (v >= 100) return `${Math.round(v)} GB`;
  if (v >= 1) return `${trim(v.toFixed(1))} GB`;
  if (v >= 0.01) return `${trim(v.toFixed(2))} GB`;
  const mb = v * 1000;
  if (mb >= 1) return `${trim(mb.toFixed(1))} MB`;
  const kb = mb * 1000;
  if (kb >= 1) return `${Math.round(kb)} KB`;
  return '0 MB';
}

function formatMillions(v) {
  const n = v * 1e6;
  if (n < 10000) return group(Math.round(n));
  if (n < 1e6) return `${Math.round(n / 1000)}k`;
  if (v >= 100) return `${Math.round(v)}M`;
  if (v >= 10) return `${trim(v.toFixed(1))}M`;
  return `${trim(v.toFixed(2))}M`;
}

function capitalise(s) {
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : s;
}

function unitFor(key) {
  if (key.startsWith('flows.')) return 'flowRate';
  return UNITS[key] || 'count';
}

export function formatValue(v, unit) {
  const x = finite(v);
  switch (unit) {
    case 'rate':
      return x < 10 && !Number.isInteger(x) ? trim(x.toFixed(1)) : group(x);
    case 'seconds':
      return formatSeconds(x);
    case 'fraction':
      return `${Math.round(x * 100)}%`;
    case 'gb':
      return formatGB(x);
    case 'millions':
      return formatMillions(x);
    case 'blocks':
      return `${group(x)} block${Math.round(x) === 1 ? '' : 's'}`;
    case 'compact':
      return compact(x);
    case 'flowRate':
      return `${compact(x)}/s`;
    default:
      return group(x);
  }
}

export function formatAge(sec) {
  const s = Math.max(0, Math.floor(finite(sec)));
  if (s < 60) return `${s} s ago`;
  if (s < 3600) return `${Math.floor(s / 60)} min ago`;
  if (s < 86400) return `${Math.floor(s / 3600)} h ago`;
  return `${Math.floor(s / 86400)} d ago`;
}

export function formatReading(key, reading) {
  const r = reading && typeof reading === 'object' ? reading : { st: 'nodata' };
  const status = typeof r.st === 'string' ? r.st : 'nodata';
  const st = STATUS_LETTERS[status] || 'n';
  const unit = unitFor(key);
  const nodata = status === 'nodata';
  const note = typeof r.note === 'string' ? r.note : '';
  const normal = typeof r.normal === 'number' && Number.isFinite(r.normal) ? formatValue(r.normal, unit) : '';

  if (nodata) {
    const base = unit === 'state' ? 'no data' : note || 'no data';
    const age = finite(r.age);
    return { text: '–', word: age > 0 ? `${base}, ${formatAge(age)}` : base, st, normal, nodata: true };
  }

  if (unit === 'state') {
    const name = note || (finite(r.v) ? 'paused' : 'running');
    return { text: capitalise(name), word: STATUS_WORDS[status] || '', st, normal: '', nodata: false };
  }

  return { text: formatValue(r.v, unit), word: note || STATUS_WORDS[status] || '', st, normal, nodata: false };
}

const MAX_WALK_DEPTH = 64;

function walkReadings(o, key, depth, ancestors, out) {
  if (!o || typeof o !== 'object' || Array.isArray(o) || depth > MAX_WALK_DEPTH || ancestors.has(o)) return;
  if (typeof o.st === 'string') {
    out.push({ key, reading: o, marker: false });
    return;
  }
  if (o.configured === false) {
    out.push({ key, reading: null, marker: true });
    return;
  }
  ancestors.add(o);
  for (const [k, v] of Object.entries(o)) walkReadings(v, key ? `${key}.${k}` : k, depth + 1, ancestors, out);
  ancestors.delete(o);
}

export function stageReadings(stage, key) {
  const out = [];
  walkReadings(stage, typeof key === 'string' ? key : '', 0, new Set(), out);
  return out;
}

export function panelStatus(stage) {
  let worst = 'n';
  for (const entry of stageReadings(stage, '')) {
    if (entry.marker) continue;
    const letter = STATUS_LETTERS[entry.reading.st] || 'n';
    if (RANK[letter] > RANK[worst]) worst = letter;
  }
  return worst;
}

function worstOf(letters) {
  return letters.reduce((w, l) => (RANK[l] > RANK[w] ? l : w), 'n');
}

function textWidth(s, size, factor) {
  return s.length * size * factor;
}

function wrapWords(text, maxChars) {
  const lines = [];
  let line = '';
  for (const word of String(text).split(/\s+/).filter(Boolean)) {
    if (line && line.length + 1 + word.length > maxChars) {
      lines.push(line);
      line = word;
    } else {
      line = line ? `${line} ${word}` : word;
    }
  }
  if (line) lines.push(line);
  return lines;
}

function pulse(st) {
  return st === 'r' ? 'pulse' : null;
}

const READOUT_ROOM = 118;
const TANK_NOTE_LINES = 5;
const TANK_NOTE_BUDGET = { aerospike: 2, postgres: 10 };

function readout(y, label, fr, key) {
  let size = fr.text.length > 10 ? 12 : 15;
  const wordW = fr.word ? textWidth(fr.word, 9.5, 0.52) : 0;
  if (fr.word && textWidth(fr.text, size, 0.6) + wordW > 108) size = 12;
  const c = COLORS[fr.st];
  const room = READOUT_ROOM - textWidth(fr.text, size, 0.6);
  const fits = !fr.word || wordW <= room;
  const budget = Math.max(1, Math.floor(room / (9.5 * 0.52)));
  const shown = fits ? fr.word : `${fr.word.slice(0, budget - 1).trimEnd()}…`;
  const word = fits
    ? h('text', { x: 36, y: y + 22.5, class: 'wd' }, shown)
    : h('g', {}, [h('title', {}, fr.word), h('text', { x: 36, y: y + 22.5, class: 'wd' }, shown)]);
  return h('g', { 'data-key': key }, [
    h('text', { x: 12, y, class: 'lb' }, label),
    h('rect', { x: 12, y: y + 6, width: 146, height: 26, rx: 4, class: 'well' }),
    h('rect', { x: 12, y: y + 6, width: 3, height: 26, rx: 1.5, fill: c }),
    h('circle', { cx: 27, cy: y + 19, r: 3.5, fill: c, filter: 'url(#glow)', class: pulse(fr.st) }),
    word,
    h('text', { x: 152, y: y + 24, 'text-anchor': 'end', class: 'val', 'font-size': size }, fr.text),
  ]);
}

function meter(y, label, fr, pct, key) {
  const n = 24;
  const lit = fr.nodata ? 0 : Math.max(0, Math.min(n, Math.round((pct / 100) * n)));
  const segs = [];
  for (let i = 0; i < n; i++) {
    segs.push(h('rect', { x: 12 + i * 6.1, y: y + 6, width: 5, height: 12, rx: 1, fill: i < lit ? COLORS[fr.st] : '#18212b', opacity: i < lit ? 0.92 : 1 }));
  }
  return h('g', { 'data-key': key }, [
    h('text', { x: 12, y, class: 'lb' }, label),
    h('text', { x: 158, y, 'text-anchor': 'end', class: 'tx' }, fr.text),
    h('g', {}, segs),
    h('text', { x: 12, y: y + 31, class: 'wd' }, fr.word),
  ]);
}

function fitNotes(notes, maxLines) {
  const wrapped = notes.map((note) => wrapWords(note, 28));
  const total = wrapped.reduce((n, w) => n + w.length, 0);
  if (total <= maxLines) return wrapped.flat();
  const out = [];
  let shown = 0;
  for (const w of wrapped) {
    if (out.length + w.length > maxLines - 1) break;
    out.push(...w);
    shown += 1;
  }
  out.push(`+${notes.length - shown} more in details`);
  return out;
}

function tank(y, label, pct, st, lines, notes, attrs, maxNotes) {
  const hgt = 104;
  const top = y + 10;
  const bot = top + hgt;
  const known = typeof pct === 'number' && Number.isFinite(pct);
  const p = known ? Math.max(0, Math.min(100, pct)) : 0;
  const liqH = (hgt * p) / 100;
  const liqTop = bot - liqH;
  const ticks = [1, 2, 3].map((i) => h('line', { x1: 64, x2: 70, y1: top + (hgt * i) / 4, y2: top + (hgt * i) / 4, stroke: '#4a5866' }));
  const pctText = attrs['data-st'] === 'n' && !known ? '' : known ? `${Math.round(p)}%` : '–';
  const noteLines = fitNotes(notes, maxNotes || TANK_NOTE_LINES);
  return h('g', attrs, [
    h('text', { x: 12, y, class: 'lb' }, label),
    h('rect', { x: 14, y: top, width: 54, height: hgt, fill: 'url(#glassG)', stroke: '#3a4756' }),
    h('ellipse', { cx: 41, cy: top, rx: 27, ry: 6, fill: '#222e3a', stroke: '#3a4756' }),
    h('rect', { x: 15, y: liqTop, width: 52, height: liqH, fill: `url(#${LIQUID[st]})` }),
    h('ellipse', { cx: 41, cy: liqTop, rx: 26, ry: 5.5, fill: COLORS[st], opacity: liqH > 0 ? 0.85 : 0.3 }),
    h('ellipse', { cx: 41, cy: bot, rx: 27, ry: 6, fill: `url(#${LIQUID[st]})`, stroke: '#3a4756' }),
    h('rect', { x: 21, y: top + 4, width: 5, height: hgt - 8, fill: '#fff', opacity: 0.07 }),
    h('g', {}, ticks),
    h('text', { x: 41, y: liqTop + (liqH > 30 ? 22 : -10), 'text-anchor': 'middle', class: 'val', 'font-size': 14 }, pctText),
    h('text', { x: 80, y: top + 16, class: 'val', 'font-size': 13 }, lines[0] || ''),
    h('text', { x: 80, y: top + 36, class: 'tx' }, lines[1] || ''),
    h('text', { x: 80, y: top + 52, class: 'tx' }, lines[2] || ''),
    h('text', { x: 80, y: top + 68, class: 'tx' }, lines[3] || ''),
    h('text', { x: 80, y: top + 84, class: 'tx' }, lines[4] || ''),
    h('g', {}, noteLines.map((l, i) => h('text', { x: 12, y: bot + 22 + i * 14, class: 'wd' }, l))),
  ]);
}

function waitingTotal(count, listed) {
  if (!count || typeof count !== 'object' || count.st === 'nodata' || typeof count.st !== 'string') {
    return { total: listed, text: group(listed) };
  }
  const total = Math.max(finite(count.v), listed);
  const note = typeof count.note === 'string' && count.note ? count.note : '';
  return { total, text: note || group(total) };
}

function chips(y, label, heights, st, count) {
  const sorted = [...heights].filter((x) => typeof x === 'number').sort((a, b) => a - b);
  const shown = sorted.slice(0, 3).reverse();
  const waiting = waitingTotal(count, sorted.length);
  const kids = [h('text', { x: 12, y, class: 'lb' }, `${label} (${waiting.text})`)];
  if (shown.length === 0) {
    kids.push(h('text', { x: 12, y: y + 21, class: 'tx' }, 'None'));
  }
  shown.forEach((height, i) => {
    const x = 12 + i * 49;
    const active = i === shown.length - 1;
    kids.push(h('rect', {
      x, y: y + 6, width: 46, height: 22, rx: 3,
      fill: active ? '#2a2420' : '#1c3550',
      stroke: active ? COLORS[st] : '#2f5784',
      class: active && st !== 'g' ? 'pulse' : null,
    }));
    kids.push(h('text', { x: x + 23, y: y + 21, 'text-anchor': 'middle', class: 'val', 'font-size': 9.5 }, group(height)));
  });
  const more = waiting.total - shown.length;
  if (shown.length > 0 && more > 0) {
    kids.push(h('text', { x: 12 + shown.length * 49, y: y + 21, class: 'tx' }, `+${group(more)} more`));
  }
  return h('g', { 'data-key': 'chain.blocksWaiting' }, kids);
}

function labelWithNormal(key, fr) {
  const base = LABELS[key] || key;
  return fr.normal ? `${base} (normal ${fr.normal})` : base;
}

function readingAt(snapshot, key) {
  return key.split('.').reduce((o, k) => (o && typeof o === 'object' ? o[k] : undefined), snapshot);
}

function stdReadout(s, y, key, anchors) {
  const fr = formatReading(key, readingAt(s, key));
  anchors[key] = y + 19;
  return readout(y, labelWithNormal(key, fr), fr, key);
}

function fractionMeter(s, y, key, anchors) {
  const r = readingAt(s, key);
  const fr = formatReading(key, r);
  anchors[key] = y + 12;
  return meter(y, labelWithNormal(key, fr), fr, finite(r && r.v) * 100, key);
}

function countMeterPct(v) {
  if (!(v > 0)) return 0;
  return Math.min(100, (Math.log10(v + 1) / Math.log10(COUNT_METER_FULL)) * 100);
}

function sourceOutage(s, name) {
  const sources = s && Array.isArray(s.sources) ? s.sources : [];
  const src = sources.find((x) => x && typeof x === 'object' && x.name === name);
  if (!src || src.ok) return '';
  const age = finite(src.age);
  return age > 0 ? `no data, ${formatAge(age)}` : 'no data';
}

function fsmLetter(fsm) {
  if (fsm === 'CATCHINGBLOCKS') return 'a';
  if (fsm === 'RUNNING') return 'g';
  return 'n';
}

function ingressBody(s, anchors) {
  const qKey = 'ingress.subtreeQueue';
  const q = readingAt(s, qKey);
  const qfr = formatReading(qKey, q);
  anchors[qKey] = 292;
  const oKey = 'ingress.subtreeQueueOldestSec';
  const ofr = formatReading(oKey, readingAt(s, oKey));
  const oldest = `Oldest waiting: ${ofr.nodata ? '–' : ofr.text}`;
  return [
    stdReadout(s, 58, 'ingress.propagationTxPerSec', anchors),
    stdReadout(s, 110, 'ingress.subtreeIntakePerSec', anchors),
    stdReadout(s, 162, 'ingress.peers', anchors),
    stdReadout(s, 214, 'ingress.rejectedPerSec', anchors),
    meter(280, LABELS[qKey], qfr, countMeterPct(finite(q && q.v)), qKey),
    h('g', { 'data-key': oKey }, [
      h('text', { x: 12, y: 336, class: 'lb' }, oldest),
      h('text', { x: 12, y: 350, class: 'wd' }, ofr.word),
    ]),
  ];
}

function validatorBody(s, anchors) {
  return [
    stdReadout(s, 58, 'validator.validatedPerSec', anchors),
    stdReadout(s, 110, 'validator.latencyP99Sec', anchors),
    fractionMeter(s, 162, 'validator.queueFill', anchors),
    fractionMeter(s, 212, 'validator.spendBatchFill', anchors),
    stdReadout(s, 262, 'validator.errorsPerSec', anchors),
  ];
}

function noteFor(key, fr) {
  if (!fr.word || (!fr.nodata && Object.values(STATUS_WORDS).includes(fr.word))) return null;
  return `${LABELS[key] || key}: ${fr.word}`;
}

function cacheBody(s, anchors) {
  const c = s.cache || {};
  const fill = formatReading('cache.fill', c.fill);
  const size = formatReading('cache.sizeGB', c.sizeGB);
  const cap = formatReading('cache.capacityGB', c.capacityGB);
  const entries = formatReading('cache.entriesMillions', c.entriesMillions);
  const cur = formatReading('cache.currentGenFrac', c.currentGenFrac);
  const prev = formatReading('cache.previousGenFrac', c.previousGenFrac);
  const sizeText = size.text.endsWith(' GB') && cap.text.endsWith(' GB') ? size.text.slice(0, -3) : size.text;
  const notes = [
    ['cache.fill', fill], ['cache.sizeGB', size], ['cache.capacityGB', cap], ['cache.entriesMillions', entries],
  ].map(([k, fr]) => noteFor(k, fr)).filter(Boolean);
  anchors['cache.fill'] = 340;
  return [
    stdReadout(s, 58, 'cache.hitRate', anchors),
    stdReadout(s, 110, 'cache.missesPerSec', anchors),
    stdReadout(s, 162, 'cache.evictionsPerSec', anchors),
    stdReadout(s, 214, 'cache.insertionsPerSec', anchors),
    tank(274, labelWithNormal('cache.fill', fill), fill.nodata ? null : finite(c.fill && c.fill.v) * 100, fill.st, [
      `${sizeText} of ${cap.text}`,
      entries.nodata ? 'Entries –' : `${entries.text} entries`,
      `Current gen ${cur.text}`,
      `Previous gen ${prev.text}`,
    ], notes, { 'data-key': 'cache.fill', 'data-st': fill.st }),
  ];
}

function assemblyBody(s, anchors) {
  return [
    stdReadout(s, 58, 'assembly.paused', anchors),
    stdReadout(s, 110, 'assembly.tipLagBlocks', anchors),
    stdReadout(s, 162, 'assembly.queueHeadAgeSec', anchors),
    stdReadout(s, 214, 'assembly.transactions', anchors),
    fractionMeter(s, 280, 'assembly.ingestQueueFill', anchors),
    fractionMeter(s, 330, 'assembly.subtreeFill', anchors),
  ];
}

function chainBody(s, anchors) {
  const outage = sourceOutage(s, 'chain');
  const fsm = !outage && typeof s.fsm === 'string' && s.fsm ? s.fsm : '';
  const fsmFr = { text: fsm || '–', word: fsm ? '' : outage || 'no data', st: fsmLetter(fsm), normal: '', nodata: !fsm };
  const chain = s.chain || {};
  anchors['chain.fsm'] = 77;
  return [
    readout(58, LABELS['chain.fsm'], fsmFr, 'chain.fsm'),
    stdReadout(s, 110, 'chain.forkDepth', anchors),
    chips(162, LABELS['chain.blocksWaiting'], Array.isArray(chain.blocksWaiting) ? chain.blocksWaiting : [], fsmLetter(fsm) === 'g' ? 'g' : 'a', chain.blocksWaitingCount),
    stdReadout(s, 214, 'chain.persisterBehind', anchors),
    stdReadout(s, 266, 'chain.subscribersDropped', anchors),
  ];
}

function storeTank(s, name, y, anchors) {
  const base = `stores.${name}`;
  const store = readingAt(s, base) || {};
  const label = LABELS[base];
  const attrs = { 'data-key': base, 'data-store': name };
  anchors[base] = y + 62;
  const outage = sourceOutage(s, name);
  if (outage) {
    return tank(y, label, null, 'n', ['', outage], [], { ...attrs, 'data-st': 'n' });
  }
  if (store.configured === false || !store.configured) {
    return tank(y, label, null, 'n', ['', 'not configured'], [], { ...attrs, 'data-st': 'n' });
  }
  const st = panelStatus(store);
  const disk = formatReading(`${base}.diskFill`, store.diskFill);
  const size = formatReading(`${base}.sizeGB`, store.sizeGB);
  const p99Key = `${base}.readP99Sec`;
  const p99 = formatReading(p99Key, store.readP99Sec);
  const reads = formatReading(`${base}.readsPerSec`, store.readsPerSec);
  const writes = formatReading(`${base}.writesPerSec`, store.writesPerSec);
  const commits = formatReading(`${base}.commitsPerSec`, store.commitsPerSec);
  const rateLine = (key, fr) => {
    if (fr.nodata) return `${LABELS[key]} –`;
    return STORE_RATE_PREFIXED[key] ? `${LABELS[key]} ${fr.text}/s` : `${fr.text}${STORE_RATE_SUFFIX[key]}`;
  };
  const lines = [
    size.nodata ? 'Size –' : size.text,
    `${LABELS[p99Key]} ${p99.text}`,
    rateLine(`${base}.readsPerSec`, reads),
    rateLine(`${base}.writesPerSec`, writes),
  ].concat(name === 'postgres' ? [rateLine(`${base}.commitsPerSec`, commits)] : [p99.normal ? `Normal ${p99.normal}` : '']);
  const notes = [
    [`${base}.diskFill`, disk], [`${base}.sizeGB`, size], [p99Key, p99], [`${base}.readsPerSec`, reads], [`${base}.writesPerSec`, writes],
  ].concat(name === 'postgres' ? [[`${base}.commitsPerSec`, commits]] : [])
    .map(([k, fr]) => noteFor(k, fr)).filter(Boolean);
  const pct = disk.nodata ? null : finite(store.diskFill && store.diskFill.v) * 100;
  return tank(y, label, pct, st, lines, notes, { ...attrs, 'data-st': st }, TANK_NOTE_BUDGET[name]);
}

function storesBody(s, anchors) {
  return [storeTank(s, 'aerospike', 52, anchors), storeTank(s, 'postgres', 222, anchors)];
}

const BODIES = {
  ingress: ingressBody,
  validator: validatorBody,
  cache: cacheBody,
  assembly: assemblyBody,
  chain: chainBody,
  stores: storesBody,
};

function panel(i, stage, st, body, geo) {
  const w = (geo && geo.w) || PX;
  const hgt = (geo && geo.h) || PH;
  const y = (geo && geo.y) || PY;
  return h('g', { transform: `translate(${px(i)},${y})`, 'data-stage': stage.key, 'data-st': st }, [
    h('rect', { width: w, height: hgt, rx: 8, fill: 'url(#panelG)', stroke: 'rgba(255,255,255,.1)' }),
    h('path', { d: `M0 8a8 8 0 0 1 8-8h${w - 16}a8 8 0 0 1 8 8v26H0z`, fill: 'url(#hdG)' }),
    h('line', { x1: 0, x2: w, y1: 34, y2: 34, stroke: 'rgba(255,255,255,.08)' }),
    stage.lane
      ? h('text', { x: 12, y: 22, class: 'ti' }, stage.title)
      : h('a', { href: `#/stage/${stage.key}`, class: 'ti-link', 'aria-label': `${stage.title} details` }, [
        h('rect', { x: 0, y: 0, width: w - 30, height: 34, class: 'ti-hit' }),
        h('text', { x: 12, y: 22, class: 'ti' }, stage.title),
      ]),
    h('circle', { cx: w - 14, cy: 17, r: 4.5, fill: COLORS[st], filter: 'url(#glow)', class: pulse(st) }),
    h('g', {}, body),
  ]);
}

function flowSpeed(v) {
  if (v >= 1000) return 'fast';
  if (v < 10) return 'slow';
  return '';
}

function strip(flow, place, kafkaOutage, reverse) {
  const x = px(place.gap) + PX + 6;
  const w = GAP - 12;
  const y = PY + 76 + place.slot * STRIP_PITCH;
  const rate = flow.rate && typeof flow.rate === 'object' ? flow.rate : { st: 'nodata' };
  const stalled = rate.st === 'idle' || rate.st === 'nodata';
  const fr = formatReading(`flows.${flow.id}.rate`, rate);
  const v = finite(rate.v);
  const d = reverse ? `M${x + w - 7} ${y + 13} H${x + 7}` : `M${x + 7} ${y + 13} H${x + w - 7}`;
  const still = stalled || !(v > 0);
  const path = still
    ? h('path', { d, stroke: COLORS.n, 'stroke-width': 3, 'stroke-dasharray': '2 8', 'stroke-linecap': 'round', fill: 'none' })
    : h('path', {
      d, class: `flow ${flowSpeed(v)}`.trim(), stroke: COLORS[fr.st], 'stroke-width': Math.min(7, 3 + Math.log10(v + 1)),
      filter: 'url(#glowLine)', opacity: 0.9,
    });
  const split = fr.nodata ? fr.word.split(', ') : [];
  const rateText = rate.st === 'idle' ? 'stalled' : fr.nodata ? split[0] : fr.text;
  const ageText = fr.nodata && split.length > 1 ? split.slice(1).join(', ') : '';
  const hasLag = typeof flow.lag === 'number' && Number.isFinite(flow.lag);
  const lag = !hasLag ? '' : kafkaOutage ? `lag ${kafkaOutage}` : `lag ${group(flow.lag)}`;
  const sub = [ageText, lag].filter(Boolean);
  return h('g', { 'data-flow': flow.id, 'data-gap': place.gap, 'data-st': still ? 'n' : fr.st }, [
    h('text', { x: x + w / 2, y, 'text-anchor': 'middle', class: 'lb' }, typeof flow.label === 'string' ? flow.label : flow.id),
    h('rect', { x, y: y + 5, width: w, height: 16, rx: 8, class: 'well' }),
    path,
    h('text', { x: x + w / 2, y: y + 36, 'text-anchor': 'middle', class: 'tx' }, rateText),
    ...sub.map((t, i) => h('text', { x: x + w / 2, y: y + 49 + i * 13, 'text-anchor': 'middle', class: 'lb' }, t)),
  ]);
}

function defs() {
  const stops = (id, list, vertical) => h('linearGradient', vertical ? { id, x1: 0, y1: 0, x2: 0, y2: 1 } : { id, x1: 0, y1: 0, x2: 1, y2: 0 },
    list.map(([offset, color]) => h('stop', { offset, 'stop-color': color })));
  return h('defs', {}, [
    h('filter', { id: 'glow', x: '-80%', y: '-80%', width: '260%', height: '260%' }, [
      h('feGaussianBlur', { stdDeviation: 2.4, result: 'b' }),
      h('feMerge', {}, [h('feMergeNode', { in: 'b' }), h('feMergeNode', { in: 'SourceGraphic' })]),
    ]),
    h('filter', { id: 'glowLine', filterUnits: 'userSpaceOnUse', x: VIEW.x, y: VIEW.y, width: VIEW.w, height: VIEW.h }, [
      h('feGaussianBlur', { stdDeviation: 2.4, result: 'b' }),
      h('feMerge', {}, [h('feMergeNode', { in: 'b' }), h('feMergeNode', { in: 'SourceGraphic' })]),
    ]),
    stops('panelG', [['0', '#17202b'], ['1', '#0d1319']], true),
    stops('hdG', [['0', '#26323f'], ['1', '#1a232e']], true),
    stops('glassG', [['0', '#0b1015'], ['.35', '#1b2631'], ['1', '#0b1015']]),
    stops('lgG', [['0', '#1f6f4c'], ['.4', '#4cc38a'], ['1', '#1b5f42']]),
    stops('lgA', [['0', '#8a6a1c'], ['.4', '#e5b44d'], ['1', '#7a5c18']]),
    stops('lgR', [['0', '#9a3636'], ['.4', '#ef5f5f'], ['1', '#8a2f2f']]),
    stops('lgN', [['0', '#2a333d'], ['.4', '#56626f'], ['1', '#262e37']]),
  ]);
}

function severityRank(a) {
  return SEVERITY_RANK[a && a.severity] || 0;
}

function raisedMs(a) {
  const t = Date.parse(a && a.raisedAt);
  return Number.isFinite(t) ? t : 0;
}

export function sortAlarms(alarms) {
  return [...(Array.isArray(alarms) ? alarms : [])].filter((a) => a && typeof a === 'object').sort((x, y) =>
    (Number(!!x.snoozed) - Number(!!y.snoozed))
    || (severityRank(y) - severityRank(x))
    || (Number(!!y.startOfChain) - Number(!!x.startOfChain))
    || (raisedMs(x) - raisedMs(y)));
}

export function topAlarm(alarms) {
  const first = sortAlarms(alarms)[0];
  return first && !first.snoozed ? first : null;
}

function severityWord(a) {
  return SEVERITY_WORDS[a.severity] || 'Notice';
}

function severityLetter(a) {
  if (a.severity === 'crit') return 'r';
  if (a.severity === 'warn') return 'a';
  return 'n';
}

function hhmm(ms) {
  const d = new Date(ms);
  return `${String(d.getHours()).padStart(2, '0')}:${String(d.getMinutes()).padStart(2, '0')}`;
}

function raisedText(a, nowMs) {
  const t = raisedMs(a);
  if (!t) return '';
  if (typeof nowMs !== 'number' || !Number.isFinite(nowMs)) return `Raised ${hhmm(t)}`;
  return `Raised ${hhmm(t)}, ${formatAge((nowMs - t) / 1000).replace(/^0 s ago$/, 'just now')}`;
}

export const DRILLDOWN_PATH = /^#\/stage\/(ingress|validator|cache|assembly|chain|stores)$/;

export function isDrilldownPath(path) {
  return typeof path === 'string' && DRILLDOWN_PATH.test(path);
}

function drilldownLinks(a, extra) {
  return (Array.isArray(a.drilldowns) ? a.drilldowns : [])
    .map((d) => {
      if (!d || typeof d !== 'object') return null;
      const label = typeof d.label === 'string' ? d.label : '';
      if (isDrilldownPath(d.path)) return h('a', { class: 'btn', href: d.path, ...extra }, label || d.path);
      return label ? h('span', { class: 'link-text' }, label) : null;
    })
    .filter(Boolean);
}

function str(v) {
  return typeof v === 'string' ? v : '';
}

function alarmHead(a, nowMs, extra) {
  return h('div', { class: 'al-head' }, [
    h('span', { class: `sev sev-${severityLetter(a)}` }, severityWord(a)),
    h('span', { class: 'al-title' }, str(a.title)),
    a.startOfChain ? h('span', { class: 'chain-tag' }, 'Start of chain') : null,
    ...(extra || []),
    h('span', { class: 'al-raised' }, raisedText(a, nowMs)),
  ]);
}

export function renderAlarmPopover(alarm, nowMs) {
  if (!alarm) return null;
  const outOfTabOrder = { tabindex: '-1' };
  return h('div', { class: `popover sev-${severityLetter(alarm)}`, 'aria-hidden': 'true' }, [
    alarmHead(alarm, nowMs),
    h('p', { class: 'al-body' }, str(alarm.body)),
    h('div', { class: 'al-actions' }, [
      ...drilldownLinks(alarm, outOfTabOrder),
      h('button', { type: 'button', class: 'btn', 'data-action': 'snooze', 'data-id': str(alarm.id), ...outOfTabOrder }, `Snooze ${SNOOZE_MINUTES} min`),
    ]),
  ]);
}

export function renderAlarmList(alarms, nowMs) {
  const sorted = sortAlarms(alarms);
  if (sorted.length === 0) {
    return h('div', { class: 'alarm-list' }, [h('p', { class: 'al-empty' }, 'No alarms.')]);
  }
  return h('div', { class: 'alarm-list' }, [
    h('ul', { class: 'al-items' }, sorted.map((a) => h('li', {
      class: `alarm sev-${severityLetter(a)}${a.snoozed ? ' snoozed' : ''}`,
      'data-alarm-id': str(a.id),
    }, [
      alarmHead(a, nowMs, a.snoozed ? [h('span', { class: 'snooze-tag' }, 'Snoozed')] : []),
      h('p', { class: 'al-body' }, str(a.body)),
      h('div', { class: 'al-actions' }, [
        ...drilldownLinks(a),
        a.snoozed
          ? h('button', { type: 'button', class: 'btn', 'data-action': 'unsnooze', 'data-id': str(a.id) }, 'Unsnooze')
          : h('button', { type: 'button', class: 'btn', 'data-action': 'snooze', 'data-id': str(a.id) }, `Snooze ${SNOOZE_MINUTES} min`),
      ]),
    ]))),
  ]);
}

export function renderAlarmSummary(alarms) {
  const sorted = sortAlarms(alarms);
  return h('div', {}, [
    h('h2', {}, 'Alarms'),
    sorted.length === 0
      ? h('p', {}, 'No alarms.')
      : h('ul', {}, sorted.map((a) => h('li', {}, [
        severityWord(a), ': ', str(a.title), '.',
        a.startOfChain ? ' Start of chain.' : '',
        a.snoozed ? ' Snoozed.' : '',
        ` ${str(a.body)}`,
      ].join('')))),
  ]);
}

function leader(alarm, anchorsByStage) {
  const idx = STAGES.findIndex((st) => st.key === alarm.stage);
  if (idx < 0) return null;
  const key = ALARM_ANCHORS[alarm.id];
  const anchors = anchorsByStage[alarm.stage] || {};
  const hasAnchor = key && typeof anchors[key] === 'number';
  const sx = px(idx) + (hasAnchor ? 158 : PX - 14);
  const sy = PY + (hasAnchor ? anchors[key] : 17);
  const ex = px(idx) + PX + 3;
  let d = `M${num(sx)} ${num(sy)} H${num(ex)}`;
  if (ex < POP.x) d += ` V${POP.y + 24} H${POP.x}`;
  else if (ex > POP.x + POP.w) d += ` V${POP.y + 24} H${POP.x + POP.w}`;
  else d += ` V${POP.y}`;
  const c = COLORS[severityLetter(alarm)];
  return [
    h('path', { d, fill: 'none', stroke: c, 'stroke-width': 1.2, 'stroke-dasharray': '3 3', opacity: 0.85 }),
    h('circle', { cx: sx, cy: sy, r: 3, fill: c }),
  ];
}

function homeLabel(s) {
  const parts = STAGES.map((st) => `${st.title} ${PANEL_WORDS[panelStatus(s[st.key])]}`);
  const alarms = Array.isArray(s.alarms) ? s.alarms.length : 0;
  return `Node ${str(s.node)}. ${parts.join(', ')}. FSM ${sourceOutage(s, 'chain') || str(s.fsm) || 'unknown'}. ${alarms} ${alarms === 1 ? 'alarm' : 'alarms'}.`;
}

export function renderHome(snapshot, nowMs) {
  if (!snapshot || typeof snapshot !== 'object') {
    return h('div', { class: 'empty' }, [h('p', {}, 'Waiting for the first sample.')]);
  }
  const s = snapshot;
  const now = typeof nowMs === 'number' ? nowMs : Date.parse(s.time);
  const anchorsByStage = {};
  const panels = STAGES.map((stage, i) => {
    const anchors = {};
    anchorsByStage[stage.key] = anchors;
    return panel(i, stage, panelStatus(s[stage.key]), BODIES[stage.key](s, anchors));
  });
  const strips = (Array.isArray(s.flows) ? s.flows : [])
    .filter((f) => f && flowPlacement(f.id))
    .map((f) => strip(f, flowPlacement(f.id), sourceOutage(s, 'kafka')));
  const top = topAlarm(s.alarms);
  const pop = top ? h('g', { 'data-popover': str(top.id) }, [
    ...(leader(top, anchorsByStage) || []),
    h('foreignObject', { x: POP.x, y: POP.y, width: POP.w, height: POP.h }, [renderAlarmPopover(top, now)]),
  ]) : null;
  return h('svg', {
    class: 'stage',
    viewBox: `${VIEW.x} ${VIEW.y} ${VIEW.w} ${VIEW.h}`,
    role: 'img',
    'aria-label': homeLabel(s),
  }, [
    defs(),
    h('g', { class: 'panels' }, panels),
    h('text', { x: px(0) + PX + GAP / 2, y: PY + 56, 'text-anchor': 'middle', class: 'ti kafka' }, 'Kafka'),
    h('g', { class: 'strips' }, strips),
    h('g', { class: 'alarm-layer' }, pop ? [pop] : []),
  ]);
}

const LANE_PANELS = [
  { key: 'tx', title: 'Transactions' },
  { key: 'subtrees', title: 'Subtrees' },
  { key: 'blocks', title: 'Blocks' },
  { key: 'mining', title: 'Mining' },
];

function laneReading(s, key) {
  if (key === 'lanes.blocks.tip') return finite(s.tip) > 0 ? { v: s.tip, st: 'ok' } : { st: 'nodata' };
  return readingAt(s, key);
}

function laneReadout(s, y, key, label, source) {
  const fr = formatReading(key, laneReading(s, source || key));
  return readout(y, fr.normal ? `${label} (normal ${fr.normal})` : label, fr, key);
}

function laneStatus(s, sources) {
  return worstOf(sources.map((k) => STATUS_LETTERS[(laneReading(s, k) || {}).st] || 'n'));
}

const LANE_ROWS = {
  tx: [
    ['lanes.tx.alreadySeenPerSec', 'Already seen (skipped), per s'],
    ['ingress.propagationTxPerSec', 'Received, per s'],
    ['validator.validatedPerSec', 'Validated, per s'],
    ['ingress.rejectedPerSec', 'Rejected, per s'],
    ['validator.latencyP99Sec', 'Validation p99'],
    ['assembly.transactions', 'Held in block assembler'],
  ],
  subtrees: [
    ['ingress.subtreeIntakePerSec', 'Received, per s'],
    ['lanes.subtrees.alreadyKnownPerSec', 'Already known (ignored), per s'],
    ['lanes.subtrees.completedPerSec', 'Completed, per s'],
    ['lanes.subtrees.txsFetchedAvg', 'Missing txs per subtree'],
    ['ingress.subtreeQueue', 'Waiting to process'],
    ['ingress.subtreeQueueOldestSec', 'Oldest waiting'],
  ],
  blocks: [
    ['lanes.blocks.announcedTotal', 'Announced, since start'],
    ['lanes.blocks.validatedTotal', 'Validated, since start'],
    ['lanes.blocks.missingSubtrees', 'Missing subtrees outstanding'],
    ['lanes.blocks.tip', 'Tip height'],
    ['chain.forkDepth', 'Fork depth'],
  ],
  mining: [
    ['assembly.paused', 'Block assembly'],
    ['lanes.mining.candidateAgeSec', 'Candidate age'],
    ['lanes.mining.candidatesPerSec', 'Candidates served, per s'],
    ['lanes.mining.solutionsTotal', 'Solutions found, since start'],
    ['assembly.tipLagBlocks', 'Tip lag'],
  ],
};

const LANE_ARROWS = [
  { gap: 0, id: 'missing-txs', label: 'txs', key: 'lanes.subtrees.missingTxsPerSec' },
  { gap: 1, id: 'missing-subtrees', label: 'subtrees', key: 'lanes.blocks.missingSubtreesPerSec' },
];

const WIDE_W = 2 * PX + GAP;
const ROW2_Y = PY + PH + 24;
const BLOB_H = 352;
const KAFKA_ROW = 26;
const KAFKA_BAR = '#5b8fc7';

const BLOB_COLUMNS = [['subtree', 'Subtree store'], ['block', 'Block store']];
const BLOB_ROWS = [
  ['readsPerSec', 'Reads, per s'],
  ['writesPerSec', 'Writes, per s'],
  ['errorsPerSec', 'Errors, per s'],
  ['readP99Sec', 'Read p99'],
  ['writeP99Sec', 'Write p99'],
];
const BLOB_KEYS = BLOB_COLUMNS.flatMap(([store]) => BLOB_ROWS.map(([field]) => `blobs.${store}.${field}`));

function blobsBody(s) {
  return BLOB_COLUMNS.map(([store, title], col) => h('g', { transform: `translate(${col * (PX + GAP)},0)`, 'data-blob': store }, [
    h('text', { x: 12, y: 62, class: 'tx' }, title),
    ...BLOB_ROWS.map(([field, label], row) => laneReadout(s, 96 + row * 52, `blobs.${store}.${field}`, label)),
  ]));
}

function kafkaTopics(s) {
  const list = Array.isArray(s.kafka) ? s.kafka : [];
  return list.filter((t) => t && typeof t === 'object' && typeof t.topic === 'string' && t.topic);
}

function kafkaHeight(s) {
  return 58 + Math.max(1, kafkaTopics(s).length) * KAFKA_ROW + 14;
}

function kafkaRow(t, y) {
  const known = typeof t.lag === 'number' && Number.isFinite(t.lag);
  const lag = known ? Math.max(0, t.lag) : 0;
  const lagText = known ? group(lag) : 'no consumers';
  return h('g', { 'data-topic': t.topic, 'data-lag': known ? String(lag) : '' }, [
    h('title', {}, `${t.topic}: total lag of all consumer groups`),
    h('text', { x: 12, y: y + 11, class: 'tx' }, t.topic),
    h('rect', { x: 160, y: y + 3, width: 150, height: 8, rx: 2, fill: '#18212b' }),
    known && lag > 0 ? h('rect', { x: 160, y: y + 3, width: Math.max(2, (150 * countMeterPct(lag)) / 100), height: 8, rx: 2, fill: KAFKA_BAR }) : null,
    h('text', { x: WIDE_W - 12, y: y + 11, 'text-anchor': 'end', class: known ? 'tx' : 'wd' }, lagText),
  ].filter(Boolean));
}

function kafkaBody(s) {
  const outage = sourceOutage(s, 'kafka');
  if (outage) return [h('text', { x: 12, y: 66, class: 'wd', 'data-kafka': 'outage' }, `Kafka ${outage}`)];
  const topics = kafkaTopics(s);
  if (topics.length === 0) return [h('text', { x: 12, y: 66, class: 'wd', 'data-kafka': 'none' }, 'No Kafka topics are watched on this node.')];
  return topics.map((t, i) => kafkaRow(t, 52 + i * KAFKA_ROW));
}

function kafkaStatus(s) {
  return sourceOutage(s, 'kafka') || kafkaTopics(s).length === 0 ? 'n' : 'g';
}

function laneBody(s, lane) {
  return LANE_ROWS[lane.key].map(([key, label], row) => laneReadout(s, 58 + row * 52, key, label));
}

export function renderLanes(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') {
    return h('div', { class: 'empty' }, [h('p', {}, 'Waiting for the first sample.')]);
  }
  const s = snapshot;
  const lanes = LANE_PANELS.map((lane, i) => panel(
    i,
    { key: `lane-${lane.key}`, title: lane.title, lane: true },
    laneStatus(s, LANE_ROWS[lane.key].map(([key]) => key)),
    laneBody(s, lane),
  ));
  const shared = [
    panel(4, STAGES[2], panelStatus(s.cache), cacheBody(s, {})),
    panel(5, STAGES[5], panelStatus(s.stores), storesBody(s, {})),
  ];
  const arrows = LANE_ARROWS.map((a) => strip(
    { id: a.id, label: a.label, rate: laneReading(s, a.key) || { st: 'nodata' } },
    { gap: a.gap, slot: 0 },
    '',
    true,
  ));
  const kafkaH = kafkaHeight(s);
  const second = [
    panel(0, { key: 'lane-blobs', title: 'Blob stores', lane: true }, laneStatus(s, BLOB_KEYS), blobsBody(s), { y: ROW2_Y, w: WIDE_W, h: BLOB_H }),
    panel(2, { key: 'lane-kafka', title: 'Kafka topics', lane: true }, kafkaStatus(s), kafkaBody(s), { y: ROW2_Y, w: WIDE_W, h: kafkaH }),
  ];
  const viewH = ROW2_Y + Math.max(BLOB_H, kafkaH) + 16 - VIEW.y;
  return h('svg', {
    class: 'stage tall',
    viewBox: `${VIEW.x} ${VIEW.y} ${VIEW.w} ${viewH}`,
    role: 'img',
    'aria-label': `Lanes view. ${homeLabel(s)}`,
  }, [
    defs(),
    h('g', { class: 'panels' }, [...lanes, ...shared, ...second]),
    h('text', { x: px(4) - GAP / 2, y: PY + 56, 'text-anchor': 'middle', class: 'ti kafka' }, 'Shared'),
    h('g', { class: 'strips' }, arrows),
  ]);
}

function chip(name, st, text, tag, extra) {
  return h(tag || 'span', { class: 'chip', 'data-chip': name, 'data-st': st, ...(extra || {}) }, [
    h('span', { class: `led led-${st}`, 'aria-hidden': 'true' }, ''),
    h('span', { class: 'chip-text' }, text),
  ]);
}

function tipChipText(tip, peerTip) {
  if (!peerTip.known) return `Tip ${group(tip)}, ${peerTip.word}`;
  return peerTip.height > 0 ? `Tip ${group(tip)}, peer ${group(peerTip.height)}` : `Tip ${group(tip)}`;
}

function peerState(s) {
  const reading = s.chain && typeof s.chain === 'object' ? s.chain.peerHeight : undefined;
  if (!reading || typeof reading !== 'object') {
    return { known: true, height: finite(s.peerTip), word: '' };
  }
  const fr = formatReading('chain.peerHeight', reading);
  if (fr.nodata) return { known: false, height: 0, word: `peer ${fr.word}` };
  const height = finite(reading.v);
  if (height <= 0) return { known: false, height: 0, word: typeof reading.note === 'string' && reading.note ? reading.note : 'peer no data' };
  return { known: true, height, word: '' };
}

export function renderTopBar(snapshot, opts) {
  if (!snapshot || typeof snapshot !== 'object') return h('div', { class: 'chips' }, []);
  const s = snapshot;
  const sources = Array.isArray(s.sources) ? s.sources : [];
  const nodeSt = sources.every((x) => x && x.ok) ? 'g' : 'a';
  const chainOutage = sourceOutage(s, 'chain');
  const fsm = str(s.fsm);
  const tip = finite(s.tip);
  const peerTip = peerState(s);
  const peer = peerTip.height;
  const alarms = Array.isArray(s.alarms) ? s.alarms.filter((a) => a && typeof a === 'object') : [];
  const live = alarms.filter((a) => !a.snoozed);
  const alarmSt = live.some((a) => a.severity === 'crit') ? 'r' : live.length > 0 ? 'a' : 'g';
  const alarmText = alarms.length === 0 ? 'No alarms' : `${alarms.length} ${alarms.length === 1 ? 'alarm' : 'alarms'}`;
  const open = !!(opts && opts.alarmsOpen);
  return h('div', { class: 'chips' }, [
    chip('node', nodeSt, str(s.node)),
    chainOutage ? chip('fsm', 'n', `FSM: ${chainOutage}`) : chip('fsm', fsm === 'CATCHINGBLOCKS' ? 'a' : fsmLetter(fsm), `FSM: ${fsm || 'unknown'}`),
    chainOutage ? chip('tip', 'n', `Tip ${chainOutage}`) : chip('tip', peerTip.known ? (peer > tip ? 'a' : 'g') : 'n', tipChipText(tip, peerTip)),
    chip('alarms', alarmSt, alarmText, 'button', {
      type: 'button', 'data-action': 'toggle-alarms', 'aria-controls': 'alarm-list', 'aria-expanded': open ? 'true' : 'false',
    }),
  ]);
}

export function drainText(s) {
  if (s.drainGrowing) return 'Backlog is growing';
  const v = s.drainSeconds;
  if (typeof v !== 'number' || !Number.isFinite(v) || v < 0) return '';
  if (v < 60) return `Drain time about ${Math.round(v)} s`;
  if (v < 3600) return `Drain time about ${Math.round(v / 60)} min`;
  const hours = Math.floor(v / 3600);
  const mins = Math.round((v % 3600) / 60);
  return mins > 0 ? `Drain time about ${hours} h ${mins} min` : `Drain time about ${hours} h`;
}

export function updatedText(s, nowMs) {
  const t = Date.parse(s.time);
  if (!Number.isFinite(t)) return '';
  const secs = Math.max(0, Math.floor((nowMs - t) / 1000));
  if (secs < 60) return `Updated ${secs} s ago`;
  if (secs < 3600) return `Updated ${Math.floor(secs / 60)} min ago`;
  return `Updated ${Math.floor(secs / 3600)} h ago`;
}

export function renderBottomBar(snapshot, nowMs) {
  if (!snapshot || typeof snapshot !== 'object') return h('div', { class: 'bar' }, []);
  const s = snapshot;
  const cal = s.calibration && typeof s.calibration === 'object' ? s.calibration : { ready: false, fraction: 0 };
  const until = Date.parse(cal.until);
  const calText = cal.ready
    ? 'Baselines ready.'
    : Number.isFinite(until) ? `Calibrating baselines. Fixed limits apply until ${hhmm(until)}.` : 'Calibrating baselines. Fixed limits apply.';
  const fraction = cal.ready ? 1 : Math.max(0, Math.min(1, finite(cal.fraction)));
  const drain = drainText(s);
  return h('div', { class: 'bar' }, [
    h('span', { class: 'cal-text' }, calText),
    h('svg', {
      class: 'cal-bar', viewBox: '0 0 1 1', preserveAspectRatio: 'none', role: 'img',
      'aria-label': `Calibration ${Math.round(fraction * 100)}% complete`,
    }, [
      h('rect', { class: 'cal-track', width: 1, height: 1 }),
      h('rect', { class: 'cal-fill', width: fraction === 0 ? '0' : fraction, height: 1 }),
    ]),
    drain ? h('span', { class: 'drain' }, drain) : null,
    h('span', { class: 'updated' }, updatedText(s, nowMs)),
  ]);
}

export const HISTORY_MINUTES = 10;
export const MAX_SERIES = 64;
export const MAX_POINTS = 600;
const MAX_SERIES_SCANNED = 1024;
const MAX_KEY_LENGTH = 96;
const SPARK = { w: 120, h: 28, padX: 2, padY: 3 };
const VALUE_LIMIT = 1e15;
const HISTORY_STATUSES = ['ok', 'warn', 'crit', 'idle', 'nodata'];
const ROW_WORDS = { ok: 'Normal', warn: 'Warning', crit: 'Critical', idle: 'Idle', nodata: 'No data' };
const HOME_ROUTE = { name: 'home', stage: '' };
export const LANES_HASH = '#/lanes';

export function parseRoute(hash) {
  if (hash === LANES_HASH) return { name: 'lanes', stage: '' };
  const m = typeof hash === 'string' ? DRILLDOWN_PATH.exec(hash) : null;
  return m ? { name: 'stage', stage: m[1] } : { ...HOME_ROUTE };
}

function stageTitle(key) {
  const found = STAGES.find((s) => s.key === key);
  return found ? found.title : '';
}

function rowLabel(key) {
  const cut = key.lastIndexOf('.');
  const own = LABELS[key] || key.slice(cut + 1);
  const parent = key.slice(0, cut);
  if (parent.includes('.') && LABELS[parent]) return `${LABELS[parent]}: ${own}`;
  return own;
}

function validStatus(st) {
  return HISTORY_STATUSES.includes(st) ? st : 'nodata';
}

function clampValue(v) {
  return Math.max(-VALUE_LIMIT, Math.min(VALUE_LIMIT, v));
}

function safe(n) {
  return typeof n === 'number' && Number.isFinite(n) ? n : 0;
}

export function normalizeHistory(payload, stage) {
  if (!payload || typeof payload !== 'object' || Array.isArray(payload)) return null;
  if (payload.stage !== stage || !Array.isArray(payload.series)) return null;
  const prefix = `${stage}.`;
  const out = new Map();
  for (const s of payload.series.slice(0, MAX_SERIES_SCANNED)) {
    if (out.size >= MAX_SERIES) break;
    if (!s || typeof s !== 'object' || typeof s.key !== 'string' || !Array.isArray(s.points)) continue;
    if (s.key.length > MAX_KEY_LENGTH || !s.key.startsWith(prefix) || s.key.length === prefix.length) continue;
    const points = [];
    for (const p of s.points.slice(-MAX_POINTS)) {
      if (!p || typeof p !== 'object' || typeof p.t !== 'string') continue;
      const ms = Date.parse(p.t);
      if (!Number.isFinite(ms)) continue;
      const v = typeof p.v === 'number' && Number.isFinite(p.v) ? p.v : null;
      points.push({ t: p.t, ms, v, st: validStatus(p.st) });
    }
    points.sort((a, b) => a.ms - b.ms);
    out.set(s.key, points.map(({ t, v, st }) => ({ t, v, st })));
  }
  return out;
}

function sparkDash() {
  return h('span', { class: 'spark-none' }, '–');
}

export function sparkline(points, opts) {
  const o = opts && typeof opts === 'object' ? opts : {};
  const key = typeof o.key === 'string' ? o.key : '';
  const minutes = typeof o.minutes === 'number' && o.minutes > 0 && o.minutes <= 60 ? o.minutes : HISTORY_MINUTES;
  const windowMs = minutes * 60000;
  const clean = [];
  for (const p of Array.isArray(points) ? points : []) {
    if (!p || typeof p !== 'object' || typeof p.t !== 'string') continue;
    const ms = Date.parse(p.t);
    if (!Number.isFinite(ms)) continue;
    const v = typeof p.v === 'number' && Number.isFinite(p.v) ? clampValue(p.v) : null;
    clean.push({ ms, v, st: validStatus(p.st) });
  }
  clean.sort((a, b) => a.ms - b.ms);
  if (clean.length === 0) return sparkDash();
  const end = typeof o.now === 'number' && Number.isFinite(o.now) ? o.now : clean[clean.length - 1].ms;
  const start = end - windowMs;
  const inWindow = clean.filter((p) => p.ms >= start);
  const numeric = inWindow.filter((p) => p.v !== null);
  if (numeric.length < 2) return sparkDash();

  const values = numeric.map((p) => p.v);
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const span = hi - lo;
  const plotW = SPARK.w - 2 * SPARK.padX;
  const plotH = SPARK.h - 2 * SPARK.padY;
  const xOf = (ms) => SPARK.padX + Math.max(0, Math.min(1, (ms - start) / windowMs)) * plotW;
  const yOf = (v) => (span > 0 ? SPARK.padY + (1 - (v - lo) / span) * plotH : SPARK.h / 2);
  const at = (p) => `${num(safe(xOf(p.ms)))} ${num(safe(yOf(p.v)))}`;

  const segments = [];
  let run = null;
  for (const p of inWindow) {
    if (p.v === null) {
      run = null;
    } else {
      if (!run) {
        run = [];
        segments.push(run);
      }
      run.push(p);
    }
  }
  const d = segments
    .map((seg) => (seg.length === 1 ? `M${at(seg[0])} L${at(seg[0])}` : seg.map((p, i) => `${i === 0 ? 'M' : 'L'}${at(p)}`).join(' ')))
    .join(' ');

  const last = numeric[numeric.length - 1];
  const latest = inWindow[inWindow.length - 1];
  const label = typeof o.label === 'string' && o.label ? o.label : key.slice(key.lastIndexOf('.') + 1);
  return h('svg', {
    class: `spark st-${latest.st}`,
    viewBox: `0 0 ${SPARK.w} ${SPARK.h}`,
    role: 'img',
    'aria-label': `${num(minutes)} minute history of ${label}, last value ${formatValue(last.v, unitFor(key))}`,
  }, [
    h('path', { class: 'spark-line', d, fill: 'none' }),
    h('circle', { class: 'spark-dot', cx: safe(xOf(last.ms)), cy: safe(yOf(last.v)), r: 2 }),
  ]);
}

function statusCell(st) {
  return h('td', { class: 'c-st' }, [
    h('span', { class: `led led-${STATUS_LETTERS[st]}`, 'aria-hidden': 'true' }, ''),
    h('span', { class: 'st-word' }, ROW_WORDS[st]),
  ]);
}

function noteText(reading, fr) {
  const note = typeof reading.note === 'string' && reading.note !== 'no data' ? reading.note : '';
  if (!fr.nodata) return note;
  const age = finite(reading.age) > 0 ? formatAge(reading.age) : '';
  return [note, age].filter(Boolean).join(', ');
}

function markerRow(entry) {
  return h('tr', { 'data-key': entry.key, 'data-st': 'n', 'data-marker': 'true' }, [
    h('th', { scope: 'row', class: 'c-label' }, rowLabel(entry.key)),
    h('td', { class: 'c-val' }, 'not configured'),
    statusCell('nodata'),
    h('td', { class: 'c-norm' }, ''),
    h('td', { class: 'c-note' }, ''),
    h('td', { class: 'c-spark' }, [sparkDash()]),
  ]);
}

function readingRow(entry, series, nowMs) {
  if (entry.marker) return markerRow(entry);
  const { key, reading } = entry;
  const fr = formatReading(key, reading);
  const label = rowLabel(key);
  return h('tr', { 'data-key': key, 'data-st': fr.st }, [
    h('th', { scope: 'row', class: 'c-label' }, label),
    h('td', { class: 'c-val' }, fr.text),
    statusCell(validStatus(reading.st)),
    h('td', { class: 'c-norm' }, fr.normal),
    h('td', { class: 'c-note' }, noteText(reading, fr)),
    h('td', { class: 'c-spark' }, [series ? sparkline(series, { key, label, now: nowMs }) : sparkDash()]),
  ]);
}

function stageHeader(stageKey, st) {
  return h('div', { class: 'sv-head' }, [
    h('a', { class: 'btn', href: '#/' }, 'Back to overview'),
    h('span', { class: `led led-${st}`, 'data-role': 'stage-led', 'data-st': st, 'aria-hidden': 'true' }, ''),
    h('h1', { id: 'stage-title', tabindex: '-1' }, stageTitle(stageKey)),
    h('span', { class: 'sv-word' }, PANEL_WORDS[st]),
  ]);
}

export function renderStageAlarms(alarms, stageKey, nowMs) {
  const mine = (Array.isArray(alarms) ? alarms : []).filter((a) => a && typeof a === 'object' && a.stage === stageKey);
  if (mine.length === 0) return h('p', { class: 'al-empty' }, 'No alarms for this stage.');
  return renderAlarmList(mine, nowMs);
}

export function renderStage(snapshot, stageKey, history, nowMs, notice) {
  if (!stageTitle(stageKey) || !snapshot || typeof snapshot !== 'object') {
    return h('div', { class: 'stage-view' }, [
      stageHeader(stageKey, 'n'),
      h('p', { class: 'al-empty' }, 'Waiting for the first sample.'),
    ]);
  }
  const data = history && history.data instanceof Map ? history.data : null;
  const now = typeof nowMs === 'number' && Number.isFinite(nowMs) ? nowMs : Date.parse(snapshot.time);
  const rows = stageReadings(snapshot[stageKey], stageKey).map((e) => readingRow(e, data ? data.get(e.key) : null, now));
  const head = ['Reading', 'Value', 'Status', 'Normal', 'Note', `Last ${HISTORY_MINUTES} minutes`];
  return h('div', { class: 'stage-view' }, [
    stageHeader(stageKey, panelStatus(snapshot[stageKey])),
    h('table', { class: 'readings' }, [
      h('caption', { class: 'sr-only' }, `${stageTitle(stageKey)} readings`),
      h('thead', {}, [h('tr', {}, head.map((t) => h('th', { scope: 'col' }, t)))]),
      h('tbody', {}, rows),
    ]),
    h('p', { class: 'hist-note', role: 'status' }, history && history.error ? 'History unavailable.' : ''),
    h('h2', { class: 'sv-sub' }, 'Alarms for this stage'),
    notice ? h('p', { class: 'al-notice', role: 'alert' }, notice) : null,
    renderStageAlarms(snapshot.alarms, stageKey, now),
  ]);
}

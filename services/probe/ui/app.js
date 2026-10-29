import {
  renderHome,
  renderLanes,
  renderTopBar,
  renderBottomBar,
  renderAlarmList,
  renderAlarmSummary,
  renderStage,
  normalizeHistory,
  parseRoute,
  STAGES,
  HISTORY_MINUTES,
  nextBackoff,
  SNOOZE_MINUTES,
} from './view.js';

export const TOKEN_KEY = 'probe.token';
export const REJECTED_MESSAGE = 'The token was not accepted.';
export const ACTION_FAILED_MESSAGE = 'The request failed. Try again.';
const UNAUTHORIZED_CLOSE = 4401;
export const HISTORY_POLL_MS = 10000;
export const HISTORY_TIMEOUT_MS = 8000;

export function createClient({ fetch, WebSocket, storage, setTimeout, clearTimeout, setInterval, clearInterval, location }) {
  const state = { snapshot: null, connected: false, reconnecting: false, needLogin: true, message: '', notice: '', history: null };
  const listeners = [];
  let socket = null;
  let backoff = 0;
  let timer = null;
  let wantedStage = '';
  let historyTimer = null;
  let historyBusy = false;
  let historyReq = null;

  const emit = () => listeners.forEach((fn) => fn(state));
  const token = () => storage.getItem(TOKEN_KEY);

  function cancelTimer() {
    if (timer !== null) {
      clearTimeout(timer);
      timer = null;
    }
  }

  function dropSocket() {
    const s = socket;
    socket = null;
    if (s) s.close();
  }

  function abortHistory() {
    const req = historyReq;
    historyReq = null;
    historyBusy = false;
    if (!req) return;
    req.done = true;
    if (req.timeout !== null) {
      clearTimeout(req.timeout);
      req.timeout = null;
    }
    req.ctrl.abort();
  }

  function stopHistoryTimer() {
    abortHistory();
    if (historyTimer !== null) {
      clearInterval(historyTimer);
      historyTimer = null;
    }
  }

  function setHistoryError(stage) {
    const keep = state.history && state.history.stage === stage ? state.history.data : null;
    state.history = { stage, data: keep, error: true };
  }

  async function fetchHistory() {
    const t = token();
    if (!t || !wantedStage || historyBusy) return;
    const stage = wantedStage;
    const req = { ctrl: new AbortController(), timeout: null, done: false };
    historyReq = req;
    historyBusy = true;
    req.timeout = setTimeout(() => {
      if (req.done) return;
      req.timeout = null;
      abortHistory();
      setHistoryError(stage);
      emit();
    }, HISTORY_TIMEOUT_MS);
    let resp = null;
    try {
      resp = await fetch(`/api/history?stage=${stage}&minutes=${HISTORY_MINUTES}`, {
        headers: { Authorization: `Bearer ${t}` },
        credentials: 'omit',
        cache: 'no-store',
        signal: req.ctrl.signal,
      });
    } catch {
      resp = null;
    }
    let payload;
    if (resp && resp.ok && !req.done) {
      try {
        payload = await resp.json();
      } catch {
        payload = undefined;
      }
    }
    if (req.done) return;
    abortHistory();
    if (resp && resp.status === 401) {
      reject();
      return;
    }
    const data = resp && resp.ok ? normalizeHistory(payload, stage) : null;
    if (data) state.history = { stage, data, error: false };
    else setHistoryError(stage);
    emit();
  }

  function startHistory() {
    if (!wantedStage || !token() || historyTimer !== null) return;
    if (!state.history || state.history.stage !== wantedStage) state.history = { stage: wantedStage, data: null, error: false };
    historyTimer = setInterval(fetchHistory, HISTORY_POLL_MS);
    fetchHistory();
  }

  function openStage(stage) {
    if (!STAGES.some((s) => s.key === stage)) return;
    if (wantedStage === stage && historyTimer !== null) return;
    stopHistoryTimer();
    wantedStage = stage;
    state.history = { stage, data: null, error: false };
    startHistory();
    emit();
  }

  function closeStage() {
    wantedStage = '';
    stopHistoryTimer();
    state.history = null;
  }

  function reject() {
    storage.removeItem(TOKEN_KEY);
    stopHistoryTimer();
    cancelTimer();
    dropSocket();
    state.connected = false;
    state.reconnecting = false;
    state.needLogin = true;
    state.message = REJECTED_MESSAGE;
    emit();
  }

  function scheduleReconnect() {
    backoff = nextBackoff(backoff);
    timer = setTimeout(() => {
      timer = null;
      connect();
    }, backoff);
  }

  function connect() {
    const t = token();
    if (!t) {
      state.needLogin = true;
      emit();
      return;
    }
    const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
    const s = new WebSocket(`${scheme}//${location.host}/ws`);
    socket = s;
    s.onopen = () => {
      if (socket !== s) return;
      s.send(JSON.stringify({ auth: t }));
    };
    s.onmessage = (ev) => {
      if (socket !== s) return;
      let snap;
      try {
        snap = JSON.parse(ev.data);
      } catch {
        return;
      }
      if (!snap || typeof snap !== 'object' || Array.isArray(snap)) return;
      state.snapshot = snap;
      state.connected = true;
      state.reconnecting = false;
      backoff = 0;
      emit();
    };
    s.onerror = () => {};
    s.onclose = (ev) => {
      if (socket !== s) return;
      socket = null;
      state.connected = false;
      if (ev && ev.code === UNAUTHORIZED_CLOSE) {
        reject();
        return;
      }
      state.reconnecting = !!token();
      if (state.reconnecting) scheduleReconnect();
      emit();
    };
  }

  function start() {
    state.needLogin = !token();
    emit();
    if (!state.needLogin) connect();
  }

  function login(value) {
    storage.setItem(TOKEN_KEY, value);
    state.needLogin = false;
    state.reconnecting = false;
    state.message = '';
    backoff = 0;
    cancelTimer();
    dropSocket();
    emit();
    connect();
    startHistory();
  }

  async function post(path, body) {
    const t = token();
    if (!t) {
      reject();
      return false;
    }
    let resp;
    try {
      resp = await fetch(path, {
        method: 'POST',
        headers: { Authorization: `Bearer ${t}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
        credentials: 'omit',
        cache: 'no-store',
      });
    } catch {
      state.notice = ACTION_FAILED_MESSAGE;
      emit();
      return false;
    }
    if (resp.status === 401) {
      reject();
      return false;
    }
    state.notice = resp.ok ? '' : ACTION_FAILED_MESSAGE;
    emit();
    return resp.ok;
  }

  return {
    state,
    start,
    login,
    openStage,
    closeStage,
    onChange: (fn) => listeners.push(fn),
    snooze: (id, minutes) => post('/api/snooze', { id, minutes }),
    unsnooze: (id) => post('/api/unsnooze', { id }),
  };
}

const SVG_NS = 'http://www.w3.org/2000/svg';
const HTML_NS = 'http://www.w3.org/1999/xhtml';

function nsFor(tag, parentNs) {
  return tag === 'svg' ? SVG_NS : parentNs;
}

function childNs(tag, ns) {
  return tag === 'foreignObject' ? HTML_NS : ns;
}

function createNode(doc, v, parentNs) {
  const ns = nsFor(v.tag, parentNs);
  const el = doc.createElementNS(ns, v.tag);
  for (const [k, val] of Object.entries(v.attrs)) el.setAttribute(k, val);
  if (typeof v.text === 'string') {
    el.textContent = v.text;
  } else {
    const cns = childNs(v.tag, ns);
    for (const c of v.children) el.appendChild(createNode(doc, c, cns));
  }
  return el;
}

function patchNode(doc, el, v, parentNs) {
  const ns = nsFor(v.tag, parentNs);
  if (!el || el.localName !== v.tag || el.namespaceURI !== ns) {
    const fresh = createNode(doc, v, parentNs);
    if (el) el.replaceWith(fresh);
    return fresh;
  }
  for (const name of el.getAttributeNames()) {
    if (!Object.prototype.hasOwnProperty.call(v.attrs, name)) el.removeAttribute(name);
  }
  for (const [k, val] of Object.entries(v.attrs)) {
    if (el.getAttribute(k) !== val) el.setAttribute(k, val);
  }
  if (typeof v.text === 'string') {
    if (el.children.length > 0 || el.textContent !== v.text) el.textContent = v.text;
    return el;
  }
  if (el.firstChild && el.children.length === 0) el.textContent = '';
  const cns = childNs(v.tag, ns);
  v.children.forEach((c, i) => {
    const cur = el.children[i];
    if (cur) patchNode(doc, cur, c, cns);
    else el.appendChild(createNode(doc, c, cns));
  });
  while (el.children.length > v.children.length) el.lastElementChild.remove();
  return el;
}

function patchInto(doc, container, vnode) {
  if (!vnode) {
    container.replaceChildren();
    return;
  }
  const ns = container.namespaceURI || HTML_NS;
  const current = container.firstElementChild;
  const el = current ? patchNode(doc, current, vnode, ns) : container.appendChild(createNode(doc, vnode, ns));
  for (const n of [...container.childNodes]) {
    if (n !== el) n.remove();
  }
}

function mount(doc, win) {
  const $ = (id) => doc.getElementById(id);
  const ui = {
    login: $('login'),
    form: $('token-form'),
    input: $('probe-access-token'),
    loginMsg: $('login-msg'),
    topbar: $('topbar'),
    banner: $('banner'),
    alarmList: $('alarm-list'),
    home: $('home'),
    stage: $('stage-view'),
    lanes: $('lanes-view'),
    viewStages: $('view-stages'),
    viewLanes: $('view-lanes'),
    bottom: $('bottombar'),
    sr: $('sr-alarms'),
  };
  const client = createClient({
    fetch: win.fetch.bind(win),
    WebSocket: win.WebSocket,
    storage: win.sessionStorage,
    setTimeout: win.setTimeout.bind(win),
    clearTimeout: win.clearTimeout.bind(win),
    setInterval: win.setInterval.bind(win),
    clearInterval: win.clearInterval.bind(win),
    location: win.location,
  });
  let alarmsOpen = false;
  let route = parseRoute(win.location.hash);

  function renderBottom() {
    patchInto(doc, ui.bottom, renderBottomBar(client.state.snapshot, Date.now()));
  }

  function render(state) {
    const now = Date.now();
    ui.login.hidden = !state.needLogin;
    ui.loginMsg.textContent = state.message;
    doc.body.classList.toggle('locked', state.needLogin);
    doc.body.classList.toggle('stale', !state.connected && !!state.snapshot);
    ui.banner.hidden = state.needLogin || !state.reconnecting;
    patchInto(doc, ui.topbar, renderTopBar(state.snapshot, { alarmsOpen }));
    const onStage = route.name === 'stage';
    const onLanes = route.name === 'lanes';
    ui.home.hidden = onStage || onLanes;
    ui.lanes.hidden = !onLanes;
    ui.stage.hidden = !onStage;
    setCurrent(ui.viewStages, !onLanes);
    setCurrent(ui.viewLanes, onLanes);
    patchInto(doc, ui.home, onStage || onLanes ? null : renderHome(state.snapshot, now));
    patchInto(doc, ui.lanes, onLanes ? renderLanes(state.snapshot) : null);
    patchInto(doc, ui.stage, onStage ? renderStage(state.snapshot, route.stage, state.history, now, state.notice) : null);
    ui.alarmList.hidden = !alarmsOpen;
    patchInto(doc, ui.alarmList, alarmsOpen ? listWithNotice(state, now) : null);
    patchInto(doc, ui.sr, renderAlarmSummary(state.snapshot ? state.snapshot.alarms : []));
    renderBottom();
  }

  function setCurrent(link, current) {
    if (current) link.setAttribute('aria-current', 'page');
    else link.removeAttribute('aria-current');
  }

  function listWithNotice(state, now) {
    const list = renderAlarmList(state.snapshot ? state.snapshot.alarms : [], now);
    if (!state.notice) return list;
    return { ...list, children: [{ tag: 'p', attrs: { class: 'al-notice', role: 'alert' }, children: [], text: state.notice }, ...list.children] };
  }

  function setAlarmsOpen(open) {
    alarmsOpen = open;
    render(client.state);
  }

  function applyRoute(focusTitle) {
    route = parseRoute(win.location.hash);
    alarmsOpen = false;
    if (route.name === 'stage') client.openStage(route.stage);
    else client.closeStage();
    render(client.state);
    if (focusTitle && route.name === 'stage') {
      const title = doc.getElementById('stage-title');
      if (title) title.focus();
    }
  }

  client.onChange(render);
  win.addEventListener('hashchange', () => applyRoute(true));

  ui.form.addEventListener('submit', (ev) => {
    ev.preventDefault();
    const value = ui.input.value.trim();
    ui.input.value = '';
    if (!value) {
      ui.loginMsg.textContent = 'Enter the access token.';
      return;
    }
    client.login(value);
  });

  doc.addEventListener('click', (ev) => {
    const target = ev.target instanceof win.Element ? ev.target.closest('[data-action]') : null;
    if (!target) return;
    const action = target.getAttribute('data-action');
    const id = target.getAttribute('data-id') || '';
    if (action === 'toggle-alarms') {
      setAlarmsOpen(!alarmsOpen);
      return;
    }
    if (action === 'snooze' || action === 'unsnooze') {
      target.disabled = true;
      const done = action === 'snooze' ? client.snooze(id, SNOOZE_MINUTES) : client.unsnooze(id);
      done.finally(() => { target.disabled = false; });
    }
  });

  doc.addEventListener('keydown', (ev) => {
    if (ev.key === 'Escape' && alarmsOpen) {
      setAlarmsOpen(false);
      const chip = ui.topbar.querySelector('[data-action="toggle-alarms"]');
      if (chip) chip.focus();
    }
  });

  win.setInterval(renderBottom, 1000);
  if (route.name === 'stage') client.openStage(route.stage);
  client.start();
  if (client.state.needLogin) ui.input.focus();
}

if (typeof document !== 'undefined' && typeof window !== 'undefined') {
  mount(document, window);
}

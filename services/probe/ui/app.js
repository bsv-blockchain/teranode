import {
  renderHome,
  renderTopBar,
  renderBottomBar,
  renderAlarmList,
  renderAlarmSummary,
  nextBackoff,
  SNOOZE_MINUTES,
} from './view.js';

export const TOKEN_KEY = 'probe.token';
export const REJECTED_MESSAGE = 'The token was not accepted.';
export const ACTION_FAILED_MESSAGE = 'The request failed. Try again.';
const UNAUTHORIZED_CLOSE = 4401;

export function createClient({ fetch, WebSocket, storage, setTimeout, clearTimeout, location }) {
  const state = { snapshot: null, connected: false, reconnecting: false, needLogin: true, message: '', notice: '' };
  const listeners = [];
  let socket = null;
  let backoff = 0;
  let timer = null;

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

  function reject() {
    storage.removeItem(TOKEN_KEY);
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
    bottom: $('bottombar'),
    sr: $('sr-alarms'),
  };
  const client = createClient({
    fetch: win.fetch.bind(win),
    WebSocket: win.WebSocket,
    storage: win.sessionStorage,
    setTimeout: win.setTimeout.bind(win),
    clearTimeout: win.clearTimeout.bind(win),
    location: win.location,
  });
  let alarmsOpen = false;

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
    patchInto(doc, ui.home, renderHome(state.snapshot, now));
    ui.alarmList.hidden = !alarmsOpen;
    patchInto(doc, ui.alarmList, alarmsOpen ? listWithNotice(state, now) : null);
    patchInto(doc, ui.sr, renderAlarmSummary(state.snapshot ? state.snapshot.alarms : []));
    renderBottom();
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

  client.onChange(render);

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
  client.start();
  if (client.state.needLogin) ui.input.focus();
}

if (typeof document !== 'undefined' && typeof window !== 'undefined') {
  mount(document, window);
}

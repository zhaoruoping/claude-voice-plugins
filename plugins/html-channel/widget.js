/* html-channel chat widget logic (injected before </body>; read per-request → hot-reloadable).
   FLOATING panel (v0.3.5): drag the header to move, drag the bottom-right grip (CSS resize:both)
   to resize the whole box; position+size persist to localStorage (hc_panel_geom). Collapse to a
   right-edge tab. Keeps the SSE /html-channel/stream + POST /html-channel/send contract + offline
   (no CDN). Does NOT touch the page-aware shim (window.__hcSetPageId is injected separately by the
   frontend, BEFORE this widget) — only the panel chrome lives here. */
(function () {
  var panel = document.getElementById('hc-panel');
  var tab = document.getElementById('hc-tab');
  var head = document.getElementById('hc-head');
  var log = document.getElementById('hc-log');
  var form = document.getElementById('hc-form');
  var inp = document.getElementById('hc-input');
  var dot = document.getElementById('hc-dot');
  if (!panel || !tab) return;

  // ---- persisted geometry + collapsed state (single key) ----
  var LS = 'hc_panel_geom';
  var st = { left: null, top: null, width: null, height: null, collapsed: false };
  try { st = Object.assign(st, JSON.parse(localStorage.getItem(LS) || '{}')); } catch (e) {}
  function save() { try { localStorage.setItem(LS, JSON.stringify(st)); } catch (e) {} }

  var MINW = 260, MINH = 220, MARGIN = 8;
  // Fill any missing geometry with defaults + clamp everything back on-screen
  // (handles first load, viewport shrink, and "panel saved off-screen" recovery).
  function clampGeom() {
    var vw = window.innerWidth, vh = window.innerHeight;
    var w = st.width || 340;
    var h = st.height || Math.min(Math.round(vh * 0.7), 560);
    w = Math.max(MINW, Math.min(w, vw - 2 * MARGIN));
    h = Math.max(MINH, Math.min(h, vh - 2 * MARGIN));
    var left = (st.left == null) ? (vw - w - 12) : st.left;
    var top  = (st.top  == null) ? 56 : st.top;
    left = Math.max(MARGIN, Math.min(left, vw - w - MARGIN));
    top  = Math.max(MARGIN, Math.min(top,  vh - 40));   // keep the header grabbable
    st.left = left; st.top = top; st.width = w; st.height = h;
  }
  function applyGeom() {
    panel.style.left = st.left + 'px';
    panel.style.top = st.top + 'px';
    panel.style.width = st.width + 'px';
    panel.style.height = st.height + 'px';
  }
  function applyCollapsed() {
    if (st.collapsed) { panel.style.display = 'none'; tab.style.display = 'block'; }
    else { panel.style.display = 'flex'; tab.style.display = 'none'; }
  }

  // collapse → hide to tab; tab → restore
  document.getElementById('hc-collapse').onclick = function () { st.collapsed = true; save(); applyCollapsed(); };
  tab.onclick = function () { st.collapsed = false; save(); applyCollapsed(); };
  // reset position+size (repurposed from the old dock-toggle button)
  var resetBtn = document.getElementById('hc-dockbtn');
  if (resetBtn) resetBtn.onclick = function () {
    st.left = null; st.top = null; st.width = null; st.height = null;
    clampGeom(); applyGeom(); save();
  };

  // ---- drag the panel by its header ----
  var drag = null;
  if (head) head.addEventListener('mousedown', function (e) {
    if (e.target.closest('button')) return;          // clicking a header button must not start a drag
    drag = { x: e.clientX, y: e.clientY, left: st.left, top: st.top };
    document.body.style.userSelect = 'none';
    e.preventDefault();
  });
  window.addEventListener('mousemove', function (e) {
    if (!drag) return;
    var vw = window.innerWidth, vh = window.innerHeight;
    var nl = drag.left + (e.clientX - drag.x);
    var nt = drag.top  + (e.clientY - drag.y);
    nl = Math.max(MARGIN, Math.min(nl, vw - panel.offsetWidth - MARGIN));
    nt = Math.max(MARGIN, Math.min(nt, vh - 40));
    st.left = nl; st.top = nt;
    panel.style.left = nl + 'px'; panel.style.top = nt + 'px';
  });
  window.addEventListener('mouseup', function () {
    if (drag) { drag = null; document.body.style.userSelect = ''; save(); }
  });

  // ---- persist size after the user drags the CSS resize grip ----
  if (window.ResizeObserver) {
    var ro = new ResizeObserver(function () {
      if (st.collapsed || panel.offsetWidth === 0) return;   // ignore while hidden
      st.width = panel.offsetWidth; st.height = panel.offsetHeight; save();
    });
    ro.observe(panel);
  }
  // keep the panel on-screen if the window is resized
  window.addEventListener('resize', function () { clampGeom(); applyGeom(); });

  // ---- chat bubbles ----
  function add(role, content) {
    var d = document.createElement('div');
    d.className = 'hc-msg ' + (role === 'user' ? 'hc-user' : 'hc-slice');
    d.textContent = content;
    log.appendChild(d); log.scrollTop = log.scrollHeight;
  }

  // SSE: slice replies + live-reload
  var es = new EventSource('/html-channel/stream');
  es.addEventListener('message', function (e) { try { var m = JSON.parse(e.data); add(m.role, m.content); } catch (_) {} });
  es.addEventListener('reload', function () { location.reload(); });
  es.onopen = function () { if (dot) dot.style.background = '#7CFC00'; };
  es.onerror = function () { if (dot) dot.style.background = '#e74c3c'; };

  // multi-line input: auto-grow the textarea with content (capped by CSS max-height,
  // then it scrolls internally). Reset to 'auto' first so it shrinks when text is deleted.
  function autoGrow() {
    if (!inp) return;
    inp.style.height = 'auto';
    inp.style.height = inp.scrollHeight + 'px';
    inp.style.overflowY = (inp.scrollHeight > inp.clientHeight) ? 'auto' : 'hidden';
  }
  function resetInput() {
    if (!inp) return;
    inp.value = '';
    inp.style.height = 'auto';
    inp.style.overflowY = 'hidden';
  }

  // send (POST) — same contract as before; content may contain newlines.
  function sendMessage() {
    var t = inp.value.trim(); if (!t) return; resetInput();
    // no optimistic echo — the server echoes the user line back over SSE (role:user)
    fetch('/html-channel/send', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ content: t })
    }).catch(function () { add('slice', '[send failed]'); });
  }

  if (inp) {
    inp.addEventListener('input', autoGrow);
    // Enter = send, Shift+Enter = newline. Guard IME composition (Chinese pinyin):
    // during composition Enter confirms the candidate and must NOT send.
    inp.addEventListener('keydown', function (ev) {
      if (ev.key === 'Enter' && !ev.shiftKey && !ev.isComposing && ev.keyCode !== 229) {
        ev.preventDefault();
        sendMessage();
      }
    });
  }
  form.onsubmit = function (ev) { ev.preventDefault(); sendMessage(); };

  // init: place + size the floating panel, then apply collapsed state
  clampGeom(); applyGeom(); applyCollapsed();
})();

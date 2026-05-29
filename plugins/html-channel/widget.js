/* html-channel chat widget logic (injected before </body>; read per-request → hot-reloadable).
   EMBEDDED docked right-side column (v0.3.6): part of the page layout — reflows page content via
   the .hc-open-right margin on <html>; collapsible to a right-edge tab. PER-PAGE persistent chat
   history in localStorage (key hc_log_<page_id>), restored on mount AND on SPA page-switch, so
   switching channel tabs no longer wipes the conversation. Keeps the SSE /html-channel/stream +
   POST /html-channel/send contract + offline (no CDN). Does NOT touch the page-aware shim
   (window.__hcPageId / __hcSetPageId are injected separately by the frontend, BEFORE this widget;
   the shim already updates __hcPageId + clears #hc-log on switch) — we only add the panel chrome
   and the per-page history layer. */
(function () {
  var htmlEl = document.documentElement;
  var panel = document.getElementById('hc-panel');
  var tab = document.getElementById('hc-tab');
  var log = document.getElementById('hc-log');
  var form = document.getElementById('hc-form');
  var inp = document.getElementById('hc-input');
  var dot = document.getElementById('hc-dot');
  if (!panel || !tab) return;

  // ---- collapse state (global UI pref; docked column is OPEN by default) ----
  var LS_UI = 'hc_ui_v2';
  var ui = { collapsed: false };
  try { ui = Object.assign(ui, JSON.parse(localStorage.getItem(LS_UI) || '{}')); } catch (e) {}
  function saveUi() { try { localStorage.setItem(LS_UI, JSON.stringify(ui)); } catch (e) {} }
  function applyUi() {
    if (ui.collapsed) {
      panel.classList.add('hc-collapsed');
      htmlEl.classList.remove('hc-open-right');   // release the reflow margin → content reclaims width
      tab.style.display = 'block';
    } else {
      panel.classList.remove('hc-collapsed');
      htmlEl.classList.add('hc-open-right');       // reflow page content to make room for the column
      tab.style.display = 'none';
    }
  }
  document.getElementById('hc-collapse').onclick = function () { ui.collapsed = true; saveUi(); applyUi(); };
  tab.onclick = function () { ui.collapsed = false; saveUi(); applyUi(); };

  // ---- per-page persistent chat history (key hc_log_<page_id>) ----
  var LOG_PREFIX = 'hc_log_', LOG_CAP = 200;
  function curPid() {
    return window.__hcPageId ||
      ((document.querySelector('meta[name="html-page-id"]') || {}).content) || 'default';
  }
  function loadLog(pid) { try { return JSON.parse(localStorage.getItem(LOG_PREFIX + pid) || '[]'); } catch (e) { return []; } }
  function saveLog(pid, arr) { try { localStorage.setItem(LOG_PREFIX + pid, JSON.stringify(arr.slice(-LOG_CAP))); } catch (e) {} }

  function appendBubble(role, text) {
    var d = document.createElement('div');
    d.className = 'hc-msg ' + (role === 'user' ? 'hc-user' : 'hc-slice');
    d.textContent = text;
    log.appendChild(d); log.scrollTop = log.scrollHeight;
  }
  function renderLog(arr) {
    log.innerHTML = '';
    for (var i = 0; i < arr.length; i++) appendBubble(arr[i].role, arr[i].text);
    log.scrollTop = log.scrollHeight;
  }
  // append to the view AND persist under the CURRENT page
  function recordAndShow(role, text) {
    appendBubble(role, text);
    var pid = curPid();
    var arr = loadLog(pid);
    arr.push({ role: role, text: text, ts: Date.now() });
    saveLog(pid, arr);
  }

  // Hook the shim's page-switch (dashboard.js calls __hcSetPageId on SPA nav): the shim already
  // updates window.__hcPageId + clears #hc-log; we additionally re-render THIS page's stored
  // history afterwards. Net effect: switch a01710 → gomegakk shows gomegakk's own history;
  // switch back restores a01710's. (Widget DOM persists across SPA nav — only <main> is swapped.)
  var _origSet = window.__hcSetPageId;
  var lastRendered = null;
  window.__hcSetPageId = function (id) {
    if (typeof _origSet === 'function') { try { _origSet(id); } catch (e) {} }
    var pid = window.__hcPageId || id || curPid();
    if (pid !== lastRendered) { lastRendered = pid; renderLog(loadLog(pid)); }
  };

  // ---- chat bubbles via SSE (inbound replies + the server's echo of the user's own line) ----
  var es = new EventSource('/html-channel/stream');
  es.addEventListener('message', function (e) { try { var m = JSON.parse(e.data); recordAndShow(m.role, m.content); } catch (_) {} });
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

  // send (POST) — same contract; the server echoes the user line back over SSE (role:user),
  // which is where it gets shown + persisted (no optimistic echo → no double bubble).
  function sendMessage() {
    var t = inp.value.trim(); if (!t) return; resetInput();
    fetch('/html-channel/send', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ content: t })
    }).catch(function () { appendBubble('slice', '[send failed]'); });
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

  // init: dock the column (reflow page) + restore THIS page's stored conversation history
  applyUi();
  lastRendered = curPid();
  renderLog(loadLog(lastRendered));
})();

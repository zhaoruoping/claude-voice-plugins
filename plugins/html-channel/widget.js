/* html-channel chat widget logic (injected before </body>; read per-request → hot-reloadable).
   Docked side panel that reflows content; collapse/expand + left/right dock; state persisted.
   Keeps the SSE /html-channel/stream + POST /html-channel/send contract + offline (no CDN). */
(function () {
  var html = document.documentElement;
  var panel = document.getElementById('hc-panel');
  var tab = document.getElementById('hc-tab');
  var log = document.getElementById('hc-log');
  var form = document.getElementById('hc-form');
  var inp = document.getElementById('hc-input');
  var dot = document.getElementById('hc-dot');
  if (!panel || !tab) return;

  // persisted UI state
  var LS = 'hc_ui_v1';
  var st = { collapsed: false, dock: 'right' };
  try { st = Object.assign(st, JSON.parse(localStorage.getItem(LS) || '{}')); } catch (e) {}
  function save() { try { localStorage.setItem(LS, JSON.stringify(st)); } catch (e) {} }

  function applyState() {
    html.classList.remove('hc-open-right', 'hc-open-left');
    panel.classList.remove('hc-dock-right', 'hc-dock-left', 'hc-collapsed');
    tab.classList.remove('hc-dock-right', 'hc-dock-left');
    panel.classList.add('hc-dock-' + st.dock);
    tab.classList.add('hc-dock-' + st.dock);
    tab.textContent = st.dock === 'right' ? 'CHAT ◂' : 'CHAT ▸';
    if (st.collapsed) {
      panel.classList.add('hc-collapsed');
      tab.style.display = 'block';            // show the re-open tab
    } else {
      html.classList.add('hc-open-' + st.dock); // reflow page content
      tab.style.display = 'none';
    }
  }

  document.getElementById('hc-collapse').onclick = function () { st.collapsed = true; save(); applyState(); };
  tab.onclick = function () { st.collapsed = false; save(); applyState(); };
  document.getElementById('hc-dockbtn').onclick = function () {
    st.dock = (st.dock === 'right') ? 'left' : 'right'; save(); applyState();
  };

  // chat bubbles
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

  // send (POST)
  form.onsubmit = function (ev) {
    ev.preventDefault();
    var t = inp.value.trim(); if (!t) return; inp.value = '';
    // no optimistic echo — the server echoes the user line back over SSE (role:user)
    fetch('/html-channel/send', {
      method: 'POST', headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ content: t })
    }).catch(function () { add('slice', '[send failed]'); });
  };

  applyState();
})();

#!/usr/bin/env bun
/**
 * html-channel — bridge between an HTML page's embedded chat box and a claude slice.
 *
 * Sibling of slice-channel. Where slice-channel's "other side" is another claude
 * (reached via a unix socket), html-channel's other side is a BROWSER PAGE, reached
 * via an embedded HTTP server (Bun.serve) running inside this same MCP process.
 *
 *   INBOUND  (page → slice):  browser POST /html-channel/send {content}
 *                             → mcp.notification(notifications/claude/channel)
 *                             → claude injects <channel source="html-channel" ...> user msg
 *   OUTBOUND (slice → page):  slice calls MCP tool html_reply(content)
 *                             → pushed to SSE subscribers of GET /html-channel/stream
 *   LIVE-RELOAD:              fs.watch(PAGE_FILE) change → SSE event:reload → browser reloads
 *
 * The page is served at GET / with a chat widget + SSE client JS injected at serve time
 * (so the committed deck .html stays pure — no chat markup in the file).
 *
 * Designed to coexist with slice-channel in one slice (dual channel): distinct MCP server
 * name → distinct <channel source> tag, distinct tools, distinct env vars, distinct transport.
 *
 * Session/page identity (in order):
 *   1. $HTML_CHANNEL_PAGE_ID  (explicit routing key; default = session id)
 *   2. $SLICE_SESSION_ID / $CLAUDE_CODE_SESSION_ID
 *   3. random uuid (page still works; routing key just isn't human-meaningful)
 *
 * MCP tools exposed:  html_reply(content) · html_self()
 *
 * Per general/html_channel_dev/DESIGN.md (single-process MVP, 2026-05-27).
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import * as fs from 'fs'
import * as path from 'path'
import { randomUUID } from 'crypto'

// ── Configuration ───────────────────────────────────────────────────────

const PAGE_ID = (
  process.env.HTML_CHANNEL_PAGE_ID ||
  process.env.SLICE_SESSION_ID ||
  process.env.CLAUDE_CODE_SESSION_ID ||
  randomUUID()
).trim()

const PAGE_FILE = (process.env.HTML_CHANNEL_PAGE_FILE || '').trim()
const PORT = parseInt(process.env.HTML_CHANNEL_PORT || '8780', 10)
// Audit log (optional): full path via HTML_CHANNEL_LOG_FILE.
const CHANNEL_LOG = (process.env.HTML_CHANNEL_LOG_FILE || '').trim() || null

process.stderr.write(
  `html-channel: page_id=${PAGE_ID} port=${PORT} page_file=${PAGE_FILE || '(none)'} `
  + `widget_dir=${process.env.HTML_CHANNEL_WIDGET_DIR || '(plugin dir)'}\n`,
)

// ── Utility ─────────────────────────────────────────────────────────────

function logChannel(direction: 'in' | 'out' | 'reload', payload: any): void {
  if (!CHANNEL_LOG) return
  try {
    fs.mkdirSync(path.dirname(CHANNEL_LOG), { recursive: true })
    fs.appendFileSync(
      CHANNEL_LOG,
      JSON.stringify({
        ts: new Date().toISOString(),
        direction:
          direction === 'in' ? 'inbound_received'
          : direction === 'out' ? 'outbound_sent'
          : 'reload',
        ...payload,
      }) + '\n',
    )
  } catch (err) {
    process.stderr.write(`html-channel: log write failed: ${err}\n`)
  }
}

// ── SSE plumbing ────────────────────────────────────────────────────────

const encoder = new TextEncoder()
type Ctrl = ReadableStreamDefaultController<Uint8Array>
const sseClients = new Set<Ctrl>()
// In-memory chat ring buffer (replayed to each new SSE connection so a page
// reload — including the live-reload itself — does not blank the conversation).
const RING_MAX = 50
const ring: Array<{ role: string; content: string; ts: string }> = []

function sseFrame(event: string, data: any): Uint8Array {
  return encoder.encode(`event: ${event}\ndata: ${JSON.stringify(data)}\n\n`)
}

function broadcast(event: string, data: any): void {
  const frame = sseFrame(event, data)
  for (const c of [...sseClients]) {
    try {
      c.enqueue(frame)
    } catch {
      sseClients.delete(c) // dead client
    }
  }
}

function pushChat(role: 'user' | 'slice', content: string): void {
  const item = { role, content, ts: new Date().toISOString() }
  ring.push(item)
  while (ring.length > RING_MAX) ring.shift()
  broadcast('message', item)
}

// ── Frontend chat widget — read per-request from external files (hot-reloadable) ──
// The widget DOM/CSS/JS live in widget.html / widget.css / widget.js next to this
// server (or in $HTML_CHANNEL_WIDGET_DIR). They are read FRESH on every GET / so a
// future tweak applies on a page refresh WITHOUT restarting the slice. The widget
// is a DOCKED side panel that REFLOWS the page (not a floating overlay) — see
// widget.css (margin on <html>), and does NOT touch the report's own left TOC.

const WIDGET_DIR = (process.env.HTML_CHANNEL_WIDGET_DIR || '').trim() || import.meta.dir

function buildWidget(): string {
  try {
    const css = fs.readFileSync(path.join(WIDGET_DIR, 'widget.css'), 'utf8')
    const dom = fs.readFileSync(path.join(WIDGET_DIR, 'widget.html'), 'utf8')
    const js = fs.readFileSync(path.join(WIDGET_DIR, 'widget.js'), 'utf8')
    return `\n<style>\n${css}\n</style>\n${dom}\n<script>\n${js}\n</script>\n`
  } catch (err) {
    // Fallback so the page still serves (+ a minimal usable chat) if files missing.
    process.stderr.write(`html-channel: widget files unreadable in ${WIDGET_DIR}: ${err}\n`)
    return (
      '\n<div id="hc-panel" style="position:fixed;right:0;top:0;bottom:0;width:330px;background:#fff;'
      + 'border-left:1px solid #ccc;z-index:2147483646;display:flex;flex-direction:column">'
      + '<div style="padding:8px;background:#2b5fb3;color:#fff">editor chat (fallback)</div>'
      + '<div id="hc-log" style="flex:1;overflow:auto;padding:8px"></div>'
      + '<form id="hc-form" style="display:flex"><input id="hc-input" style="flex:1;padding:8px"/>'
      + '<button>Send</button></form></div>'
      + '<style>html{margin-right:330px!important}</style>'
      + '<script>(function(){var l=document.getElementById("hc-log"),f=document.getElementById("hc-form"),'
      + 'i=document.getElementById("hc-input");function a(r,c){var d=document.createElement("div");'
      + 'd.textContent=(r==="user"?"› ":"‹ ")+c;l.appendChild(d);l.scrollTop=l.scrollHeight;}'
      + 'var e=new EventSource("/html-channel/stream");e.addEventListener("message",function(ev){'
      + 'try{var m=JSON.parse(ev.data);a(m.role,m.content);}catch(_){}});'
      + 'e.addEventListener("reload",function(){location.reload();});'
      + 'f.onsubmit=function(ev){ev.preventDefault();var t=i.value.trim();if(!t)return;i.value="";'
      + 'fetch("/html-channel/send",{method:"POST",headers:{"content-type":"application/json"},'
      + 'body:JSON.stringify({content:t})});};})();</script>\n'
    )
  }
}

function injectWidget(html: string): string {
  const widget = buildWidget()
  const idx = html.toLowerCase().lastIndexOf('</body>')
  if (idx === -1) return html + widget // no </body> — append at end
  return html.slice(0, idx) + widget + html.slice(idx)
}

// MIME helper for relative deck assets.
function mimeOf(p: string): string {
  const ext = path.extname(p).toLowerCase()
  return ({
    '.html': 'text/html; charset=utf-8', '.htm': 'text/html; charset=utf-8',
    '.css': 'text/css', '.js': 'application/javascript', '.mjs': 'application/javascript',
    '.json': 'application/json', '.map': 'application/json', '.txt': 'text/plain; charset=utf-8',
    '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif',
    '.svg': 'image/svg+xml', '.webp': 'image/webp', '.ico': 'image/x-icon', '.pdf': 'application/pdf',
    // fonts — needed for katex/vendor (woff2 etc.)
    '.woff2': 'font/woff2', '.woff': 'font/woff', '.ttf': 'font/ttf',
    '.otf': 'font/otf', '.eot': 'application/vnd.ms-fontobject',
  } as Record<string, string>)[ext] || 'application/octet-stream'
}

// ── MCP server ──────────────────────────────────────────────────────────

const INSTRUCTIONS = [
  'html-channel bridges an HTML page chat box to this slice. The user types in the page; you reply to the page.',
  'Inbound messages from the user-on-the-page arrive as <channel source="html-channel" page_id="..." ts="...">.',
  'To reply to the user on the page, call the html_reply tool with argument: content (the message text). It appears as a chat bubble in the page chat box.',
  'IMPORTANT routing: source="html-channel" = the USER on the webpage → reply with html_reply. source="slice-channel" = MAIN (orchestrator) → reply with slice_send. Do not confuse the two.',
  'When you edit the bound HTML deck file, the page auto-reloads (live-reload); no action needed from you.',
].join('\n')

const mcp = new Server(
  { name: 'html-channel', version: '0.1.0' },
  {
    capabilities: {
      tools: {},
      experimental: { 'claude/channel': {} },
    },
    instructions: INSTRUCTIONS,
  },
)

// ── Tools ───────────────────────────────────────────────────────────────

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'html_reply',
      description:
        'Send a chat message to the bound HTML page chat box (the user reading the page sees it as a bubble). Use this to reply to a <channel source="html-channel"> message.',
      inputSchema: {
        type: 'object',
        properties: {
          content: {
            type: 'string',
            description: 'Message body (plain text). Appears as a chat bubble in the page.',
          },
        },
        required: ['content'],
      },
    },
    {
      name: 'html_self',
      description:
        "Return this html-channel's page_id, port, bound page file, and live SSE client count. Useful for debugging routing.",
      inputSchema: { type: 'object', properties: {} },
    },
  ],
}))

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  const name = req.params.name
  const args = (req.params.arguments ?? {}) as Record<string, any>

  if (name === 'html_self') {
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({
          page_id: PAGE_ID,
          port: PORT,
          page_file: PAGE_FILE || null,
          sse_clients: sseClients.size,
          channel_log: CHANNEL_LOG,
        }, null, 2),
      }],
    }
  }

  if (name === 'html_reply') {
    const content = String(args.content || '')
    if (!content) throw new Error('html_reply: content required')
    pushChat('slice', content)
    logChannel('out', { page_id: PAGE_ID, content })
    return {
      content: [{
        type: 'text',
        text: `sent to page ${PAGE_ID.slice(0, 8)}... (${sseClients.size} live client${sseClients.size === 1 ? '' : 's'})`,
      }],
    }
  }

  throw new Error(`unknown tool: ${name}`)
})

// ── Inbound: page chat → claude channel notification ──────────────────────

function injectInbound(content: string): void {
  const msgId = randomUUID()
  const ts = new Date().toISOString()
  logChannel('in', { page_id: PAGE_ID, msg_id: msgId, content })
  // meta.chat_id is claude's routing key for the channel; use page_id.
  const meta: Record<string, string> = {
    chat_id: PAGE_ID,
    message_id: msgId,
    page_id: PAGE_ID,
    user: 'page',
    user_id: PAGE_ID,
    ts,
    msg_id: msgId,
  }
  const notification = {
    method: 'notifications/claude/channel',
    params: { content, meta },
  }
  process.stderr.write(
    `html-channel: dispatching inbound to claude (len=${content.length}, page=${PAGE_ID.slice(0, 8)}...)\n`,
  )
  mcp.notification(notification)
    .then(() => process.stderr.write('html-channel: notification dispatched OK\n'))
    .catch(err => process.stderr.write(`html-channel: notification dispatch failed: ${err}\n`))
}

// ── HTTP server (Bun.serve) — browser-facing ──────────────────────────────

const httpServer = Bun.serve({
  hostname: '127.0.0.1',
  port: PORT,
  // SSE connections are long-lived; Bun.serve's default 10s idleTimeout would
  // close them mid-conversation (e.g. while the slice "thinks" for >10s before
  // html_reply). 255s = max; combined with the <10s heartbeat below the SSE
  // stream never goes idle. (Bug found in focused_sse_persist_test 2026-05-27.)
  idleTimeout: 255,
  async fetch(req) {
    const url = new URL(req.url)
    const p = url.pathname

    // SSE stream (slice replies + live-reload)
    if (p === '/html-channel/stream') {
      const stream = new ReadableStream<Uint8Array>({
        start(controller) {
          sseClients.add(controller)
          // initial comment + replay ring buffer so a (re)connect shows history
          try { controller.enqueue(encoder.encode(': connected\n\n')) } catch {}
          for (const m of ring) {
            try { controller.enqueue(sseFrame('message', m)) } catch {}
          }
        },
        cancel(reason) {
          // best-effort; broadcast() also prunes dead controllers
        },
      })
      return new Response(stream, {
        headers: {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache, no-transform',
          'Connection': 'keep-alive',
          'X-Accel-Buffering': 'no',
        },
      })
    }

    // Inbound chat from the page
    if (p === '/html-channel/send' && req.method === 'POST') {
      let content = ''
      try {
        const body = await req.json()
        content = String((body as any)?.content || '')
      } catch {
        return Response.json({ ok: false, error: 'bad json' }, { status: 400 })
      }
      if (!content.trim()) return Response.json({ ok: false, error: 'empty content' }, { status: 400 })
      pushChat('user', content)     // echo to page chat (+ ring)
      injectInbound(content)        // inject into claude
      return Response.json({ ok: true })
    }

    // Health / debug
    if (p === '/html-channel/health') {
      return Response.json({ page_id: PAGE_ID, port: PORT, page_file: PAGE_FILE || null, clients: sseClients.size })
    }

    // Serve the deck (with injected widget) at / or the deck's basename
    if (PAGE_FILE) {
      const deckName = path.basename(PAGE_FILE)
      if (p === '/' || p === '/' + deckName) {
        try {
          const html = fs.readFileSync(PAGE_FILE, 'utf8')
          return new Response(injectWidget(html), {
            headers: { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' },
          })
        } catch (err) {
          return new Response(`html-channel: cannot read page file ${PAGE_FILE}: ${err}`, { status: 500 })
        }
      }
      // Static-sibling serving (R4): serve any file UNDER the deck's directory
      // (e.g. /figures/x.png, /vendor/katex/katex.min.css, fonts) so reports can
      // reference EXTERNAL assets instead of base64-inlining them.
      // Path-safety: decode %xx (defeats %2e%2e traversal), resolve against the
      // deck dir, and serve ONLY if the resolved real path stays UNDER deckDir.
      const deckDir = path.dirname(path.resolve(PAGE_FILE))
      let decoded = p
      try { decoded = decodeURIComponent(p) } catch { decoded = p }
      const reqPath = path.resolve(deckDir, '.' + decoded)
      const inDir = reqPath === deckDir || reqPath.startsWith(deckDir + path.sep)
      if (inDir && fs.existsSync(reqPath) && fs.statSync(reqPath).isFile()) {
        return new Response(fs.readFileSync(reqPath), {
          headers: { 'Content-Type': mimeOf(reqPath), 'Cache-Control': 'no-cache' },
        })
      }
    }

    return new Response('not found', { status: 404 })
  },
  error(err) {
    process.stderr.write(`html-channel: http error: ${err}\n`)
    return new Response('internal error', { status: 500 })
  },
})

process.stderr.write(`html-channel: HTTP listening on http://127.0.0.1:${PORT}/\n`)

// ── Live-reload: watch the bound page file ────────────────────────────────

let reloadTimer: ReturnType<typeof setTimeout> | null = null
let watcher: fs.FSWatcher | null = null

function armWatcher(): void {
  if (!PAGE_FILE) return
  try {
    watcher = fs.watch(PAGE_FILE, () => {
      // debounce (editors write-then-rename → multiple events)
      if (reloadTimer) clearTimeout(reloadTimer)
      reloadTimer = setTimeout(() => {
        logChannel('reload', { page_id: PAGE_ID, page_file: PAGE_FILE })
        broadcast('reload', { ts: new Date().toISOString() })
        process.stderr.write('html-channel: page changed → SSE reload broadcast\n')
        // re-arm (some editors replace the inode → original watch goes stale)
        try { watcher?.close() } catch {}
        armWatcher()
      }, 200)
    })
  } catch (err) {
    process.stderr.write(`html-channel: fs.watch(${PAGE_FILE}) failed: ${err}\n`)
  }
}
armWatcher()

// Heartbeat keeps SSE connections active (every 8s — under Bun's default 10s
// idleTimeout AND any proxy idle cutoff) and prunes dead controllers.
const heartbeat = setInterval(() => {
  const frame = encoder.encode(': hb\n\n')
  for (const c of [...sseClients]) {
    try { c.enqueue(frame) } catch { sseClients.delete(c) }
  }
}, 8000)

// ── Cleanup (verbatim discipline from slice-channel — avoids orphaned bun) ─
// Parent claude closing the MCP stdio pipe yields stdin EOF. Without these the
// Bun.serve + fs.watch + heartbeat keep the event loop alive forever → orphaned
// server holding the port (and buffering against a dead stdout) — the slice-channel
// 48GB RSS leak precedent. See general/self_fork_skill_dev/slice_results/bun_leak_investigation.md.

function cleanup(): void {
  try { clearInterval(heartbeat) } catch {}
  try { watcher?.close() } catch {}
  try { httpServer.stop(true) } catch {}
  for (const c of [...sseClients]) { try { c.close() } catch {} }
  sseClients.clear()
  process.stderr.write(`html-channel: shutting down (page=${PAGE_ID})\n`)
}

process.on('SIGINT', () => { cleanup(); process.exit(0) })
process.on('SIGTERM', () => { cleanup(); process.exit(0) })
process.stdin.on('end', () => { cleanup(); process.exit(0) })
process.stdin.on('close', () => { cleanup(); process.exit(0) })
process.on('exit', cleanup)

process.on('unhandledRejection', (err) => {
  process.stderr.write(`html-channel: unhandled rejection: ${err}\n`)
})
process.on('uncaughtException', (err) => {
  process.stderr.write(`html-channel: uncaught exception: ${err}\n`)
})

// ── Boot ────────────────────────────────────────────────────────────────

await mcp.connect(new StdioServerTransport())
process.stderr.write(`html-channel: MCP server connected (page=${PAGE_ID})\n`)

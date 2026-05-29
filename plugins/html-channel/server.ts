#!/usr/bin/env bun
/**
 * html-channel v0.3.0 — thin MCP client to the central html-msg-broker.
 *
 * Replaces v0.2.x's per-bot bun-HTTP-server model. This plugin does NOT bind any port,
 * does NOT serve any HTML, does NOT spawn a bun child. It connects to the broker
 * at /tmp/html-msg-broker.sock (or $HTML_MSG_BROKER_SOCK) as a "bot" client, and
 * exposes MCP tools that wrap broker commands.
 *
 * Frontends (page servers) are independent processes — see tools/html_chat_frontend.py.
 *
 *   INBOUND  (page → bot): broker MESSAGE event with from_role="frontend"
 *                          → mcp.notification(notifications/claude/channel)
 *                          → claude injects <channel source="html-channel" page_id="..." ...>
 *   OUTBOUND (bot → page): bot calls html_reply(content, page_id?)
 *                          → broker publish on that page_id
 *                          → fanned out to every frontend subscribing the page
 *
 * MCP tools: html_bind · html_unbind · html_reply · html_list_pages · html_self
 *
 * Per general/html_msg_broker_dev/DESIGN.md.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import * as net from 'net'
import { randomUUID } from 'crypto'

// ── Configuration ───────────────────────────────────────────────────────
const BROKER_SOCK = (process.env.HTML_MSG_BROKER_SOCK || '/tmp/html-msg-broker.sock').trim()
const CONN_ID = randomUUID()
const CLIENT_LABEL = (
  process.env.HTML_CHANNEL_BOT_LABEL ||
  process.env.SLICE_LABEL ||
  process.env.CLAUDE_BOT_NAME ||
  'bot'
).trim()

// Soft-deprecation warnings (v0.2.x env vars no longer have effect).
for (const v of ['HTML_CHANNEL_PORT', 'HTML_CHANNEL_PAGE_FILE', 'HTML_CHANNEL_WIDGET_DIR']) {
  if (process.env[v]) {
    process.stderr.write(
      `html-channel v0.3.0: env ${v} is DEPRECATED — v0.3.0 plugin is broker-side only ` +
      `(no HTTP, no page binding). Use tools/html_chat_frontend.py to serve a page. Ignoring.\n`
    )
  }
}

process.stderr.write(
  `html-channel v0.3.0: broker_sock=${BROKER_SOCK} conn_id=${CONN_ID.slice(0, 8)}... label=${CLIENT_LABEL}\n`
)

// ── Broker client (line-JSON, auto-reconnect) ────────────────────────────
type Resolver = (v: any) => void

class BrokerClient {
  socket: net.Socket | null = null
  connected = false
  // Pages this bot is currently bound to (set replayed on reconnect).
  pages = new Set<string>()
  activePage: string | null = null
  // Pending responses keyed by FIFO order — broker replies match command order on
  // a single connection. We use a queue of resolvers.
  pending: Resolver[] = []
  inboundBuf = ''
  reconnectMs = 2000

  async start(): Promise<void> {
    this.connect()
  }

  connect(): void {
    process.stderr.write(`html-channel: connecting broker ${BROKER_SOCK}\n`)
    const s = net.createConnection({ path: BROKER_SOCK })
    this.socket = s
    s.setEncoding('utf8')
    s.on('connect', () => {
      this.connected = true
      this.reconnectMs = 2000
      process.stderr.write('html-channel: broker connected; registering as bot\n')
      // register first; then replay any existing bindings
      this.sendRaw({ cmd: 'register', role: 'bot', conn_id: CONN_ID, client_label: CLIENT_LABEL })
        .catch(err => process.stderr.write(`html-channel: register failed: ${err}\n`))
        .then(async () => {
          for (const page of [...this.pages]) {
            try {
              await this.sendRaw({ cmd: 'bind', page_id: page })
            } catch (err) {
              process.stderr.write(`html-channel: rebind ${page} failed: ${err}\n`)
            }
          }
        })
    })
    s.on('data', (chunk: string) => this.onData(chunk))
    s.on('close', () => {
      this.connected = false
      this.socket = null
      // reject all pending — they will never get a response from this conn
      for (const p of this.pending) {
        try { p({ ok: false, error: 'broker disconnected' }) } catch {}
      }
      this.pending = []
      process.stderr.write(`html-channel: broker closed; reconnect in ${this.reconnectMs}ms\n`)
      setTimeout(() => this.connect(), this.reconnectMs)
      this.reconnectMs = Math.min(30000, Math.floor(this.reconnectMs * 1.5))
    })
    s.on('error', err => {
      process.stderr.write(`html-channel: broker socket error: ${err}\n`)
      // close handler will trigger reconnect
    })
  }

  onData(chunk: string): void {
    this.inboundBuf += chunk
    let idx: number
    while ((idx = this.inboundBuf.indexOf('\n')) >= 0) {
      const line = this.inboundBuf.slice(0, idx).trim()
      this.inboundBuf = this.inboundBuf.slice(idx + 1)
      if (!line) continue
      let obj: any
      try {
        obj = JSON.parse(line)
      } catch {
        process.stderr.write(`html-channel: bad json from broker: ${line.slice(0, 120)}\n`)
        continue
      }
      if ('event' in obj) {
        this.handleEvent(obj)
      } else if ('ok' in obj) {
        const r = this.pending.shift()
        if (r) r(obj)
      } else {
        process.stderr.write(`html-channel: unknown msg from broker: ${JSON.stringify(obj).slice(0, 200)}\n`)
      }
    }
  }

  handleEvent(obj: any): void {
    const ev = obj.event
    if (ev === 'message') {
      // Only relay messages from FRONTEND (user-on-page) into claude — skip
      // echoes of other bots' replies (otherwise a multi-bot mesh creates loops).
      if (obj.from_role !== 'frontend') return
      const content = String(obj.content || '')
      const pageId = String(obj.page_id || '')
      const meta: Record<string, string> = {
        chat_id: pageId,
        message_id: randomUUID(),
        page_id: pageId,
        user: 'page',
        user_id: pageId,
        ts: String(obj.ts || new Date().toISOString()),
        from: String(obj.from || ''),
        from_role: 'frontend',
      }
      const notification = {
        method: 'notifications/claude/channel',
        params: { content, meta },
      }
      process.stderr.write(
        `html-channel: inbound msg from page=${pageId.slice(0, 12)}... len=${content.length}\n`
      )
      mcp.notification(notification).catch(err =>
        process.stderr.write(`html-channel: notification dispatch failed: ${err}\n`)
      )
    } else if (ev === 'peer_join' || ev === 'peer_leave') {
      process.stderr.write(
        `html-channel: ${ev} page=${obj.page_id} role=${obj.role} conn=${String(obj.conn_id || '').slice(0, 8)}\n`
      )
    }
  }

  sendRaw(obj: any): Promise<any> {
    if (!this.socket || !this.connected) {
      return Promise.resolve({ ok: false, error: 'broker disconnected' })
    }
    return new Promise<any>((resolve) => {
      this.pending.push(resolve)
      try {
        this.socket!.write(JSON.stringify(obj) + '\n')
      } catch (err) {
        // shift back off and resolve with error
        const idx = this.pending.indexOf(resolve)
        if (idx >= 0) this.pending.splice(idx, 1)
        resolve({ ok: false, error: `socket write failed: ${err}` })
      }
      // safety: time out after 10s
      setTimeout(() => {
        const idx = this.pending.indexOf(resolve)
        if (idx >= 0) {
          this.pending.splice(idx, 1)
          resolve({ ok: false, error: 'broker response timeout' })
        }
      }, 10000)
    })
  }

  async bind(pageId: string): Promise<any> {
    this.pages.add(pageId)
    this.activePage = pageId
    return this.sendRaw({ cmd: 'bind', page_id: pageId })
  }

  async unbind(pageId: string): Promise<any> {
    this.pages.delete(pageId)
    if (this.activePage === pageId) {
      this.activePage = this.pages.size > 0 ? [...this.pages][this.pages.size - 1] : null
    }
    return this.sendRaw({ cmd: 'unbind', page_id: pageId })
  }

  async publish(pageId: string, content: string): Promise<any> {
    return this.sendRaw({
      cmd: 'publish',
      page_id: pageId,
      content,
      from: CLIENT_LABEL,
      from_role: 'bot',
    })
  }

  async listPages(): Promise<any> {
    return this.sendRaw({ cmd: 'list_pages' })
  }
}

const broker = new BrokerClient()

// ── MCP server ──────────────────────────────────────────────────────────
const INSTRUCTIONS = [
  'html-channel v0.3.0 bridges HTML page chat boxes to this bot via a central broker.',
  'A bot can be bound to multiple pages; each page can host multiple bots.',
  '',
  'Inbound: user-on-page messages arrive as <channel source="html-channel" page_id="..." ...>.',
  '',
  'Outbound: call html_reply(content, page_id?) — page_id optional, defaults to last-bound page.',
  '',
  'Lifecycle: html_bind(page_id) before reading/replying on a page. html_unbind(page_id?) to leave.',
  'html_list_pages() shows broker-wide topic state. html_self() shows this bot\'s state.',
  '',
  'IMPORTANT routing: source="html-channel" = user on a page → reply with html_reply.',
  '  source="slice-channel" = main orchestrator → reply with slice_send. Do not confuse the two.',
].join('\n')

const mcp = new Server(
  { name: 'html-channel', version: '0.3.0' },
  {
    capabilities: {
      tools: {},
      experimental: { 'claude/channel': {} },
    },
    instructions: INSTRUCTIONS,
  },
)

mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'html_bind',
      description:
        'Subscribe this bot to a page topic. Inbound user messages on that page arrive as ' +
        '<channel source="html-channel" page_id="..."> notifications. Sets the page as active for html_reply.',
      inputSchema: {
        type: 'object',
        properties: {
          page_id: { type: 'string', description: 'topic id of the page (must match a frontend\'s --page-id)' },
        },
        required: ['page_id'],
      },
    },
    {
      name: 'html_unbind',
      description:
        'Unsubscribe this bot from a page topic. If page_id is omitted, unbinds the currently active page.',
      inputSchema: {
        type: 'object',
        properties: { page_id: { type: 'string' } },
      },
    },
    {
      name: 'html_reply',
      description:
        'Send a chat message to a page bound to this bot. page_id is optional and defaults to the most recently ' +
        'bound page. Use this to reply to a <channel source="html-channel"> message.',
      inputSchema: {
        type: 'object',
        properties: {
          content: { type: 'string', description: 'message body (plain text)' },
          page_id: { type: 'string', description: 'target page topic id (defaults to active page)' },
        },
        required: ['content'],
      },
    },
    {
      name: 'html_list_pages',
      description: 'List broker-known page topics and per-page subscriber counts.',
      inputSchema: { type: 'object', properties: {} },
    },
    {
      name: 'html_self',
      description:
        'Return this bot\'s broker state: conn_id, broker_socket, active_page_id, all_pages_bound, broker_connected.',
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
          conn_id: CONN_ID,
          client_label: CLIENT_LABEL,
          broker_socket: BROKER_SOCK,
          broker_connected: broker.connected,
          active_page_id: broker.activePage,
          all_pages_bound: [...broker.pages],
        }, null, 2),
      }],
    }
  }

  if (name === 'html_bind') {
    const pageId = String(args.page_id || '').trim()
    if (!pageId) throw new Error('html_bind: page_id required')
    const r = await broker.bind(pageId)
    return { content: [{ type: 'text', text: JSON.stringify(r, null, 2) }] }
  }

  if (name === 'html_unbind') {
    const pageId = (args.page_id ? String(args.page_id).trim() : broker.activePage)
    if (!pageId) throw new Error('html_unbind: no active page and page_id not provided')
    const r = await broker.unbind(pageId)
    return { content: [{ type: 'text', text: JSON.stringify(r, null, 2) }] }
  }

  if (name === 'html_reply') {
    const content = String(args.content || '')
    if (!content) throw new Error('html_reply: content required')
    const pageId = (args.page_id ? String(args.page_id).trim() : broker.activePage)
    if (!pageId) throw new Error('html_reply: no active page (call html_bind first or pass page_id)')
    const r = await broker.publish(pageId, content)
    if (!r?.ok) throw new Error(`html_reply: broker error: ${r?.error || 'unknown'}`)
    return {
      content: [{
        type: 'text',
        text: `sent to page ${pageId} (delivered_to=${r.delivered_to ?? '?'})`,
      }],
    }
  }

  if (name === 'html_list_pages') {
    const r = await broker.listPages()
    return { content: [{ type: 'text', text: JSON.stringify(r, null, 2) }] }
  }

  throw new Error(`unknown tool: ${name}`)
})

// ── Cleanup ─────────────────────────────────────────────────────────────
function cleanup(): void {
  try {
    if (broker.socket && !broker.socket.destroyed) {
      // best-effort unbind for each page, then destroy
      for (const page of [...broker.pages]) {
        try { broker.socket.write(JSON.stringify({ cmd: 'unbind', page_id: page }) + '\n') } catch {}
      }
      broker.socket.end()
      broker.socket.destroy()
    }
  } catch {}
  process.stderr.write('html-channel v0.3.0: shutting down\n')
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
await broker.start()
await mcp.connect(new StdioServerTransport())
process.stderr.write(`html-channel v0.3.0: MCP server connected (conn=${CONN_ID.slice(0, 8)}...)\n`)

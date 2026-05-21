#!/usr/bin/env bun
/**
 * slice-channel — local push-based IPC for Claude Code self-fork slices.
 *
 * Each claude instance loading this plugin gets a Unix-socket endpoint at
 *   /tmp/slice-channel/<session_id>.sock
 * Messages written to that socket from another claude are pushed into the
 * receiver's conversation as a `<channel source="slice-channel">` user message.
 *
 * Symmetric: main and slice both load this plugin; both can send/receive.
 *
 * Session id resolution (in order):
 *   1. $SLICE_SESSION_ID env var (explicit override)
 *   2. $CLAUDE_CODE_SESSION_ID env var (set by claude at launch)
 *   3. Fatal error (cannot register a socket endpoint without an identity)
 *
 * MCP tool exposed:
 *   slice_send(target_session_id, content, files?)
 *
 * Per general/self_fork_skill_dev/design.md §7.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import { z } from 'zod'
import * as net from 'net'
import * as fs from 'fs'
import * as path from 'path'
import * as os from 'os'
import { randomUUID } from 'crypto'

// ── Configuration ───────────────────────────────────────────────────────

const SOCKET_DIR = '/tmp/slice-channel'
const MY_SESSION_ID = (process.env.SLICE_SESSION_ID || process.env.CLAUDE_CODE_SESSION_ID || '').trim()

if (!MY_SESSION_ID) {
  process.stderr.write('slice-channel: no session id (set $CLAUDE_CODE_SESSION_ID or $SLICE_SESSION_ID); exiting\n')
  process.exit(1)
}

const MY_SOCKET = path.join(SOCKET_DIR, `${MY_SESSION_ID}.sock`)
// CHANNEL_LOG resolution order:
//   1. SLICE_CHANNEL_LOG_FILE (full path) — preferred. slice_manager.py sets
//      this to <task>/slices/instances/<uuid>/channel_log.jsonl so the audit
//      trail lives with the slice's metadata.
//   2. SLICE_CHANNEL_LOG_DIR (shared dir) — legacy fallback; appends
//      <session_id>.channel_log.jsonl within that dir.
//   3. null — no audit log.
const CHANNEL_LOG = process.env.SLICE_CHANNEL_LOG_FILE
  ? process.env.SLICE_CHANNEL_LOG_FILE
  : process.env.SLICE_CHANNEL_LOG_DIR
    ? path.join(process.env.SLICE_CHANNEL_LOG_DIR, `${MY_SESSION_ID}.channel_log.jsonl`)
    : null

try {
  fs.mkdirSync(SOCKET_DIR, { recursive: true, mode: 0o755 })
} catch (err) {
  process.stderr.write(`slice-channel: failed to mkdir ${SOCKET_DIR}: ${err}\n`)
  process.exit(1)
}

// Clean up stale socket at this path (server crash leaves stale node)
try { fs.unlinkSync(MY_SOCKET) } catch {}

process.stderr.write(`slice-channel: session=${MY_SESSION_ID} socket=${MY_SOCKET}\n`)

// ── Utility ─────────────────────────────────────────────────────────────

function logChannel(direction: 'in' | 'out', payload: any): void {
  if (!CHANNEL_LOG) return
  try {
    fs.mkdirSync(path.dirname(CHANNEL_LOG), { recursive: true })
    fs.appendFileSync(
      CHANNEL_LOG,
      JSON.stringify({
        ts: new Date().toISOString(),
        direction: direction === 'in' ? 'inbound_received' : 'outbound_sent',
        ...payload,
      }) + '\n',
    )
  } catch (err) {
    process.stderr.write(`slice-channel: log write failed: ${err}\n`)
  }
}

// ── MCP server ──────────────────────────────────────────────────────────

const INSTRUCTIONS = [
  'slice-channel is a local push-based IPC between a main claude session and its forked slices.',
  'Messages from another slice/main arrive as <channel source="slice-channel" from_session_id="..." from_label="..." ts="...">.',
  'To send a message, call the slice_send tool with arguments: target_session_id (UUID, receiver session) + content (message body — note: parameter name is `content`, NOT `text`; this differs from telegram-voice / weixin reply tools). You receive target_session_id at fork time in the SLICE IDENTITY block, or from slice_manager.py output.',
  'If you are a slice: only call slice_send to reach back to your main. Do NOT call slice_send to arbitrary session_ids — that breaks lineage and audit.',
  'If you are main: you may call slice_send to any of your active slice instances.',
].join('\n')

const mcp = new Server(
  { name: 'slice-channel', version: '0.1.0' },
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
      name: 'slice_send',
      description:
        'Send a push message to another slice/main via slice-channel. Target identified by their session_id. The receiver wakes with a <channel source="slice-channel"> tagged user message in their next turn.',
      inputSchema: {
        type: 'object',
        properties: {
          target_session_id: {
            type: 'string',
            description: 'UUID of the receiving session (main or slice).',
          },
          content: {
            type: 'string',
            description: 'Message body (text). Markdown/plain — receiver renders as user message.',
          },
          files: {
            type: 'array',
            description: 'Optional list of absolute paths to attach. Receiver reads them via Read tool.',
            items: { type: 'string' },
          },
          from_label: {
            type: 'string',
            description: 'Optional short label for the sender (e.g. "main", "slice/codebase-summary"). Defaults to "peer".',
          },
        },
        required: ['target_session_id', 'content'],
      },
    },
    {
      name: 'slice_self',
      description: 'Return this slice/main\'s own session_id and socket path. Useful for debugging routing.',
      inputSchema: {
        type: 'object',
        properties: {},
      },
    },
  ],
}))

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  const name = req.params.name
  const args = (req.params.arguments ?? {}) as Record<string, any>

  if (name === 'slice_self') {
    return {
      content: [{
        type: 'text',
        text: JSON.stringify({
          session_id: MY_SESSION_ID,
          socket: MY_SOCKET,
          channel_log: CHANNEL_LOG,
        }, null, 2),
      }],
    }
  }

  if (name === 'slice_send') {
    const target = String(args.target_session_id || '').trim()
    const content = String(args.content || '')
    const files = Array.isArray(args.files) ? args.files.map(String) : []
    const fromLabel = String(args.from_label || 'peer')

    if (!target) throw new Error('slice_send: target_session_id required')
    if (!content && files.length === 0) throw new Error('slice_send: content or files required')

    const targetSocket = path.join(SOCKET_DIR, `${target}.sock`)
    if (!fs.existsSync(targetSocket)) {
      throw new Error(`slice_send: target socket ${targetSocket} not found — target session may not be running, or slice-channel plugin not loaded on target side`)
    }

    const msgId = randomUUID()
    const payload = {
      msg_id: msgId,
      from_session_id: MY_SESSION_ID,
      from_label: fromLabel,
      to_session_id: target,
      ts: new Date().toISOString(),
      content,
      files,
    }

    await sendToSocket(targetSocket, payload)
    logChannel('out', payload)

    return {
      content: [{
        type: 'text',
        text: `sent (msg_id: ${msgId}) to session ${target.slice(0, 8)}... via ${targetSocket}`,
      }],
    }
  }

  throw new Error(`unknown tool: ${name}`)
})

function sendToSocket(socketPath: string, payload: any): Promise<void> {
  return new Promise((resolve, reject) => {
    const client = net.createConnection(socketPath, () => {
      const data = JSON.stringify(payload) + '\n'
      client.write(data, (err) => {
        if (err) reject(err)
        client.end()
      })
    })
    client.on('error', reject)
    client.on('end', () => resolve())
    setTimeout(() => {
      client.destroy()
      reject(new Error('slice_send: socket write timed out (5s)'))
    }, 5000)
  })
}

// ── Inbound socket listener ─────────────────────────────────────────────

const server = net.createServer((socket) => {
  let buf = ''
  socket.on('data', (chunk) => {
    buf += chunk.toString('utf8')
    // Process line-by-line — each JSON payload is newline-terminated
    let idx: number
    while ((idx = buf.indexOf('\n')) !== -1) {
      const line = buf.slice(0, idx)
      buf = buf.slice(idx + 1)
      if (!line.trim()) continue
      handleInbound(line)
    }
  })
  socket.on('error', (err) => {
    process.stderr.write(`slice-channel: inbound socket error: ${err}\n`)
  })
})

function handleInbound(line: string): void {
  let payload: any
  try {
    payload = JSON.parse(line)
  } catch (err) {
    process.stderr.write(`slice-channel: dropped malformed inbound: ${line.slice(0, 100)}\n`)
    return
  }

  const fromSid = String(payload.from_session_id || 'unknown')
  const fromLabel = String(payload.from_label || 'peer')
  const ts = String(payload.ts || new Date().toISOString())
  const msgId = String(payload.msg_id || randomUUID())
  const content = String(payload.content || '')
  const files = Array.isArray(payload.files) ? payload.files : []

  logChannel('in', payload)

  // Compose channel-tagged content (chat_id required by claude channel mechanism;
  // use sender's session_id as the routing key for slice-channel).
  const meta: Record<string, string> = {
    chat_id: fromSid,                    // claude routes channels by chat_id
    message_id: msgId,
    from_session_id: fromSid,
    from_label: fromLabel,
    user: fromLabel,
    user_id: fromSid,
    ts,
    msg_id: msgId,
  }
  if (files.length > 0) meta.files = files.join(',')

  const notification = {
    method: 'notifications/claude/channel',
    params: {
      content,
      meta,
    },
  }

  process.stderr.write(`slice-channel: dispatching notification to claude (content_len=${content.length}, from=${fromSid.slice(0,8)}...)\n`)
  mcp.notification(notification)
    .then(() => process.stderr.write(`slice-channel: notification dispatched OK\n`))
    .catch(err => {
      process.stderr.write(`slice-channel: notification dispatch failed: ${err}\n`)
    })
}

server.listen(MY_SOCKET, () => {
  try {
    fs.chmodSync(MY_SOCKET, 0o600)
  } catch (err) {
    process.stderr.write(`slice-channel: chmod failed: ${err}\n`)
  }
  process.stderr.write(`slice-channel: listening on ${MY_SOCKET}\n`)
})

server.on('error', (err) => {
  process.stderr.write(`slice-channel: server error: ${err}\n`)
})

// ── Cleanup ─────────────────────────────────────────────────────────────

function cleanup(): void {
  try { server.close() } catch {}
  try { fs.unlinkSync(MY_SOCKET) } catch {}
  process.stderr.write(`slice-channel: shutting down (session=${MY_SESSION_ID})\n`)
}

process.on('SIGINT', () => { cleanup(); process.exit(0) })
process.on('SIGTERM', () => { cleanup(); process.exit(0) })
process.on('exit', cleanup)

process.on('unhandledRejection', (err) => {
  process.stderr.write(`slice-channel: unhandled rejection: ${err}\n`)
})
process.on('uncaughtException', (err) => {
  process.stderr.write(`slice-channel: uncaught exception: ${err}\n`)
})

// ── Boot ────────────────────────────────────────────────────────────────

await mcp.connect(new StdioServerTransport())
process.stderr.write(`slice-channel: MCP server connected (session=${MY_SESSION_ID})\n`)

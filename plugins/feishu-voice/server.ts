#!/usr/bin/env bun
/**
 * Feishu (飞书) channel for Claude Code — M1 skeleton (v0.0.2 marker comment).
 *
 * Mirrors telegram-voice/server.ts architecture. Uses Lark Node SDK WebSocket
 * long-connection (no public IP needed) for receiving messages, OpenAPI for
 * sending replies. State lives in ${FEISHU_STATE_DIR:-~/.claude/channels/feishu}/
 * with .env (app credentials) and access.json (allowlist).
 *
 * M1 scope (this file): env load + MCP server + reply tool + inbound message
 * forwarding. Voice / files / edits / reactions / slash commands / memory
 * enhance to follow in M2-M6.
 *
 * v0.0.2 (2026-05-02): test marker for plugin auto-update mechanism experiment.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import * as Lark from '@larksuiteoapi/node-sdk'
import { readFileSync, writeFileSync, mkdirSync, chmodSync, existsSync } from 'fs'
import { homedir } from 'os'
import { join, sep } from 'path'

// ── env / state dir ─────────────────────────────────────────────────────
const STATE_DIR = process.env.FEISHU_STATE_DIR ?? join(homedir(), '.claude', 'channels', 'feishu')
const ACCESS_FILE = join(STATE_DIR, 'access.json')
const ENV_FILE = join(STATE_DIR, '.env')
const INBOX_DIR = join(STATE_DIR, 'inbox')

mkdirSync(STATE_DIR, { recursive: true })
mkdirSync(INBOX_DIR, { recursive: true })

// Load credentials from STATE_DIR/.env (real env wins).
try {
  if (existsSync(ENV_FILE)) {
    chmodSync(ENV_FILE, 0o600)
    for (const line of readFileSync(ENV_FILE, 'utf8').split('\n')) {
      const m = line.match(/^(\w+)=(.*)$/)
      if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2]
    }
  }
} catch {}

// Voice helpers env (DASHSCOPE_API_KEY etc) — same path as telegram-voice
try {
  const voiceEnv = join(homedir(), '.claude', 'voice', '.env')
  if (existsSync(voiceEnv)) {
    for (const line of readFileSync(voiceEnv, 'utf8').split('\n')) {
      const m = line.match(/^(\w+)=(.*)$/)
      if (m && process.env[m[1]] === undefined) process.env[m[1]] = m[2]
    }
  }
} catch {}

const APP_ID = process.env.FEISHU_APP_ID
const APP_SECRET = process.env.FEISHU_APP_SECRET
const DOMAIN_KIND = (process.env.FEISHU_DOMAIN ?? 'feishu.cn').toLowerCase()

if (!APP_ID || !APP_SECRET) {
  process.stderr.write(
    `feishu channel: FEISHU_APP_ID / FEISHU_APP_SECRET required\n` +
    `  set in ${ENV_FILE}\n` +
    `  format: FEISHU_APP_ID=cli_xxx\n          FEISHU_APP_SECRET=xxx\n`,
  )
  process.exit(1)
}

const DOMAIN = DOMAIN_KIND.includes('lark') ? Lark.Domain.Lark : Lark.Domain.Feishu

// ── access.json ─────────────────────────────────────────────────────────
type Access = {
  allowFrom: string[] // open_id of users (NOT chat_id like telegram — Feishu chat_ids change per group/p2p)
  allowChats?: string[] // chat_id allowlist (optional, for restricting which chats can use this bot)
  dmPolicy?: 'open' | 'disabled'
}
function loadAccess(): Access {
  try {
    return JSON.parse(readFileSync(ACCESS_FILE, 'utf8')) as Access
  } catch {
    return { allowFrom: [], dmPolicy: 'open' }
  }
}
function saveAccess(a: Access): void {
  writeFileSync(ACCESS_FILE, JSON.stringify(a, null, 2) + '\n')
  try { chmodSync(ACCESS_FILE, 0o600) } catch {}
}
// Initialize access.json if missing — start in 'open' mode (any sender allowed)
// for testing. Production should restrict via /feishu:access skill (TBD).
if (!existsSync(ACCESS_FILE)) saveAccess({ allowFrom: [], dmPolicy: 'open' })

function isAllowedSender(openId: string): boolean {
  const a = loadAccess()
  if (a.dmPolicy === 'disabled') return false
  // Empty allowFrom + open dmPolicy = accept all (testing). For prod, populate allowFrom.
  if (a.allowFrom.length === 0) return true
  return a.allowFrom.includes(openId)
}

// ── lark clients ────────────────────────────────────────────────────────
const baseConfig = {
  appId: APP_ID,
  appSecret: APP_SECRET,
  domain: DOMAIN,
}
const client = new Lark.Client({
  ...baseConfig,
  loggerLevel: Lark.LoggerLevel.warn,
})
const wsClient = new Lark.WSClient({
  ...baseConfig,
  loggerLevel: Lark.LoggerLevel.warn,
})

// ── chat log helper ─────────────────────────────────────────────────────
const LOG_FILE = join(STATE_DIR, 'chat.log')
function chatLog(role: 'USER' | 'BOT', text: string, extra?: Record<string, unknown>): void {
  const line = JSON.stringify({ ts: new Date().toISOString(), role, text, ...extra }) + '\n'
  try { writeFileSync(LOG_FILE, line, { flag: 'a' }) } catch {}
}

// ── MCP server ──────────────────────────────────────────────────────────
const mcp = new Server(
  { name: 'feishu', version: '0.0.1' },
  {
    capabilities: {
      tools: {},
      experimental: {
        'claude/channel': {},
      },
    },
    instructions: [
      'The sender reads Feishu (飞书), not this session. Anything you want them to see must go through the reply tool — your transcript output never reaches their chat.',
      '',
      'Messages from Feishu arrive as <channel source="feishu" chat_id="..." message_id="..." sender_open_id="..." ts="...">. Reply with the reply tool — pass chat_id back. Use reply_to (set to a message_id) only when replying to an earlier message; the latest message doesn\'t need a quote-reply, omit reply_to for normal responses.',
      '',
      'M1 scope: text messages only. Voice, images, files, edits, reactions to follow in later versions.',
    ].join('\n'),
  },
)

// ── MCP tools ───────────────────────────────────────────────────────────
mcp.setRequestHandler(ListToolsRequestSchema, async () => ({
  tools: [
    {
      name: 'reply',
      description:
        'Reply on Feishu. Pass chat_id from the inbound message. Optionally pass reply_to (message_id) for quote-threading.',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string' },
          text: { type: 'string' },
          reply_to: {
            type: 'string',
            description: 'Message ID to thread under. Use message_id from the inbound <channel> block.',
          },
        },
        required: ['chat_id', 'text'],
      },
    },
  ],
}))

mcp.setRequestHandler(CallToolRequestSchema, async req => {
  const args = (req.params.arguments ?? {}) as Record<string, unknown>
  try {
    switch (req.params.name) {
      case 'reply': {
        const chat_id = args.chat_id as string
        const text = args.text as string
        const reply_to = args.reply_to as string | undefined

        // Feishu text message body uses { text: "..." } JSON payload
        const content = JSON.stringify({ text })

        const resp = reply_to
          ? await client.im.v1.message.reply({
              path: { message_id: reply_to },
              data: { content, msg_type: 'text' },
            })
          : await client.im.v1.message.create({
              params: { receive_id_type: 'chat_id' },
              data: { receive_id: chat_id, msg_type: 'text', content },
            })

        if (resp.code !== 0) {
          throw new Error(`feishu API error code=${resp.code} msg=${resp.msg}`)
        }
        const id = resp.data?.message_id ?? '<unknown>'
        chatLog('BOT', text, { message_id: id })
        return { content: [{ type: 'text', text: `sent (id: ${id})` }] }
      }
      default:
        return {
          content: [{ type: 'text', text: `unknown tool: ${req.params.name}` }],
          isError: true,
        }
    }
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err)
    return {
      content: [{ type: 'text', text: `${req.params.name} failed: ${msg}` }],
      isError: true,
    }
  }
})

// ── inbound: WebSocket → MCP channel notification ───────────────────────
wsClient.start({
  eventDispatcher: new Lark.EventDispatcher({}).register({
    // typed: (data: any) — Lark SDK types are loose, we manually narrow
    'im.message.receive_v1': async (data: any) => {
      const event = data?.event ?? data
      const msg = event?.message
      const sender = event?.sender
      if (!msg || !sender) {
        process.stderr.write(`feishu: malformed event payload\n`)
        return
      }
      const chat_id = msg.chat_id as string
      const message_id = msg.message_id as string
      const chat_type = msg.chat_type as string // 'p2p' or 'group'
      const msg_type = msg.message_type as string
      const sender_open_id = sender.sender_id?.open_id as string | undefined
      const ts = new Date().toISOString()

      if (!sender_open_id) return // skip system messages
      if (!isAllowedSender(sender_open_id)) {
        chatLog('USER', '<gated, dropped>', { sender_open_id, chat_id })
        return
      }

      // Only handle text in M1
      let text = ''
      if (msg_type === 'text') {
        try {
          const content = JSON.parse(msg.content as string)
          text = content.text ?? ''
        } catch {
          text = `<unparseable text content>`
        }
      } else {
        text = `<received ${msg_type} message — not yet supported in M1>`
      }

      chatLog('USER', text, { sender_open_id, chat_id, message_id, chat_type, msg_type })

      // Push as MCP channel event into the running Claude Code session.
      // Method + params shape mirrors telegram-voice:
      //   { method: 'notifications/claude/channel', params: { content, meta } }
      // Claude Code wraps content in <channel source="feishu" ...> using the
      // MCP server name (declared as 'feishu' above) for the source attribute.
      void mcp.notification({
        method: 'notifications/claude/channel',
        params: {
          content: text,
          meta: {
            chat_id,
            message_id,
            user: sender_open_id,
            user_id: sender_open_id,
            chat_type,
            ts,
          },
        },
      })
    },
  }),
})

await mcp.connect(new StdioServerTransport())

// Shutdown handling — when Claude closes stdin, terminate ws client and exit
let shuttingDown = false
function shutdown(): void {
  if (shuttingDown) return
  shuttingDown = true
  process.stderr.write('feishu channel: shutting down\n')
  setTimeout(() => process.exit(0), 2000)
  try { (wsClient as any).stop?.() } catch {}
  process.exit(0)
}
process.stdin.on('end', shutdown)
process.stdin.on('close', shutdown)
process.on('SIGTERM', shutdown)
process.on('SIGINT', shutdown)

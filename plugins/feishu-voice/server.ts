#!/usr/bin/env bun
/**
 * Feishu (飞书) channel for Claude Code — M1 + M2.
 *
 * Mirrors telegram-voice/server.ts architecture. Uses Lark Node SDK WebSocket
 * long-connection (no public IP needed) for receiving messages, OpenAPI for
 * sending replies. State lives in ${FEISHU_STATE_DIR:-~/.claude/channels/feishu}/
 * with .env (app credentials) and access.json (allowlist).
 *
 * M1: text in/out, MCP reply tool. Verified end-to-end 2026-05-01 (chat.log).
 * M2 (v0.0.3, 2026-05-02): audio messages auto-transcribed via qwen3-asr-flash
 *   (DashScope). Audio download via im.v1.messageResource.get + writeFile,
 *   transcribe.py call mirrors telegram-voice STT flow with 3-attempt retry.
 *
 * Voice synthesis (voice_reply tool), files / images, edit_message, react,
 * slash commands, memory enhance: TBD in M3-M6.
 */

import { Server } from '@modelcontextprotocol/sdk/server/index.js'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import {
  ListToolsRequestSchema,
  CallToolRequestSchema,
} from '@modelcontextprotocol/sdk/types.js'
import * as Lark from '@larksuiteoapi/node-sdk'
import { readFileSync, writeFileSync, mkdirSync, chmodSync, existsSync, unlinkSync, createReadStream, statSync } from 'fs'
import { spawnSync } from 'child_process'
import { homedir } from 'os'
import { join, sep, extname, basename } from 'path'

// Image extensions sent as msg_type='image' (Feishu accepts these for image upload)
const IMAGE_EXTS = new Set(['.jpg', '.jpeg', '.png', '.webp', '.gif', '.tiff', '.bmp', '.ico'])
// Map non-image extensions to Feishu's file_type enum (opus/mp4/pdf/doc/xls/ppt/stream)
function feishuFileType(ext: string): string {
  const e = ext.toLowerCase().replace(/^\./, '')
  if (['opus', 'mp4'].includes(e)) return e
  if (['pdf', 'doc', 'docx'].includes(e)) return e === 'docx' ? 'doc' : e
  if (['xls', 'xlsx'].includes(e)) return 'xls'
  if (['ppt', 'pptx'].includes(e)) return 'ppt'
  return 'stream'  // generic fallback for any other extension
}

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
const DASHSCOPE_API_KEY = process.env.DASHSCOPE_API_KEY
const VOICE_PYTHON = process.env.VOICE_PYTHON ?? '/sw/miniconda3/envs/ccbesiii/bin/python'
const STT_PROVIDER = process.env.FEISHU_VOICE_STT_PROVIDER ?? (DASHSCOPE_API_KEY ? 'dashscope' : null)
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

// ── direct REST helpers (bypass Lark SDK for file uploads) ──────────────
// Reason: SDK's multipart serialization breaks MCP stdio transport on
// large/binary uploads (v0.0.4 ReadStream + v0.0.5 Buffer both fail with
// "socket closed"). Direct fetch + manual FormData has no SDK side-effects
// on the MCP stdio channel, so we use it for image/file upload.

const FEISHU_BASE = DOMAIN === Lark.Domain.Lark
  ? 'https://open.larksuite.com'
  : 'https://open.feishu.cn'

let _tenantTokenCache: { token: string; expires_at: number } | null = null

async function getTenantAccessToken(): Promise<string> {
  if (_tenantTokenCache && Date.now() < _tenantTokenCache.expires_at) {
    return _tenantTokenCache.token
  }
  const resp = await fetch(`${FEISHU_BASE}/open-apis/auth/v3/tenant_access_token/internal`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json; charset=utf-8' },
    body: JSON.stringify({ app_id: APP_ID, app_secret: APP_SECRET }),
  })
  const data = (await resp.json()) as any
  if (data.code !== 0) throw new Error(`tenant_access_token failed: ${JSON.stringify(data)}`)
  _tenantTokenCache = {
    token: data.tenant_access_token,
    expires_at: Date.now() + (data.expire ?? 7200) * 1000 - 60_000,  // 60s safety margin
  }
  return _tenantTokenCache.token
}

async function uploadImageDirect(buf: Buffer, fname: string): Promise<string> {
  const token = await getTenantAccessToken()
  const form = new FormData()
  form.append('image_type', 'message')
  form.append('image', new Blob([buf]), fname)
  const resp = await fetch(`${FEISHU_BASE}/open-apis/im/v1/images`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
    body: form,
  })
  const data = (await resp.json()) as any
  if (data.code !== 0) throw new Error(`image upload failed: ${JSON.stringify(data)}`)
  const key = data.data?.image_key
  if (!key) throw new Error(`image upload returned no image_key: ${JSON.stringify(data)}`)
  return key
}

async function uploadFileDirect(buf: Buffer, fname: string, file_type: string): Promise<string> {
  const token = await getTenantAccessToken()
  const form = new FormData()
  form.append('file_type', file_type)
  form.append('file_name', fname)
  form.append('file', new Blob([buf]), fname)
  const resp = await fetch(`${FEISHU_BASE}/open-apis/im/v1/files`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}` },
    body: form,
  })
  const data = (await resp.json()) as any
  if (data.code !== 0) throw new Error(`file upload failed: ${JSON.stringify(data)}`)
  const key = data.data?.file_key
  if (!key) throw new Error(`file upload returned no file_key: ${JSON.stringify(data)}`)
  return key
}

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
        'Reply on Feishu. Pass chat_id from the inbound message. Optionally pass reply_to (message_id) for quote-threading, and files (absolute paths) to attach images or documents.',
      inputSchema: {
        type: 'object',
        properties: {
          chat_id: { type: 'string' },
          text: { type: 'string' },
          reply_to: {
            type: 'string',
            description: 'Message ID to thread under. Use message_id from the inbound <channel> block.',
          },
          files: {
            type: 'array',
            items: { type: 'string' },
            description: 'Absolute file paths to attach. Images (jpg/png/webp/gif/etc) send as image msg via im.v1.image.create (max 10MB); other types send as file msg via im.v1.file.create with auto-detected file_type (max 30MB). Each file is sent as a separate Feishu message.',
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
        const files = (args.files as string[] | undefined) ?? []
        const sentIds: string[] = []

        // Send text message first (if non-empty)
        if (text && text.length > 0) {
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
            throw new Error(`feishu text API error code=${resp.code} msg=${resp.msg}`)
          }
          const id = resp.data?.message_id ?? '<unknown>'
          sentIds.push(id)
          chatLog('BOT', text, { message_id: id })
        }

        // Send each file as separate message (Feishu doesn't mix text+file in one msg)
        for (const f of files) {
          if (!existsSync(f)) {
            throw new Error(`file not found: ${f}`)
          }
          const ext = extname(f).toLowerCase()
          const fname = basename(f)
          let key: string
          let msg_type: string
          let content: string
          // Read file as Buffer
          const fileBuf = readFileSync(f)
          // v0.0.6: bypass Lark SDK for upload — use direct fetch (SDK
          // multipart serialization broke MCP stdio in v0.0.4 + v0.0.5)

          if (IMAGE_EXTS.has(ext)) {
            key = await uploadImageDirect(fileBuf, fname)
            msg_type = 'image'
            content = JSON.stringify({ image_key: key })
          } else {
            const ftype = feishuFileType(ext)
            key = await uploadFileDirect(fileBuf, fname, ftype)
            msg_type = 'file'
            content = JSON.stringify({ file_key: key })
          }

          const sendResp = reply_to
            ? await client.im.v1.message.reply({
                path: { message_id: reply_to },
                data: { content, msg_type },
              })
            : await client.im.v1.message.create({
                params: { receive_id_type: 'chat_id' },
                data: { receive_id: chat_id, msg_type, content },
              })
          if (sendResp.code !== 0) {
            throw new Error(`feishu ${msg_type} send error code=${sendResp.code} msg=${sendResp.msg}`)
          }
          const id = sendResp.data?.message_id ?? '<unknown>'
          sentIds.push(id)
          chatLog('BOT', `[${msg_type}] ${fname}`, { message_id: id, file_path: f, key })
        }

        const summary = sentIds.length === 1
          ? `sent (id: ${sentIds[0]})`
          : `sent ${sentIds.length} parts (ids: ${sentIds.join(', ')})`
        return { content: [{ type: 'text', text: summary }] }
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

      // M1: text. M2: audio (voice → STT via qwen3-asr-flash → forward as text).
      let text = ''
      if (msg_type === 'text') {
        try {
          const content = JSON.parse(msg.content as string)
          text = content.text ?? ''
        } catch {
          text = `<unparseable text content>`
        }
      } else if (msg_type === 'audio') {
        // Parse file_key from audio content
        let file_key = ''
        let duration = 0
        try {
          const content = JSON.parse(msg.content as string)
          file_key = content.file_key ?? ''
          duration = content.duration ?? 0
        } catch {
          // unparseable — fall through to error path below
        }

        if (!file_key) {
          text = `[Voice message received — could not parse file_key from content]`
        } else if (!STT_PROVIDER) {
          text =
            '[Voice message received — STT is not configured]\n' +
            'This Feishu bot has no speech-to-text backend enabled. ' +
            'Tell the user: 1) reply in text instead, or 2) configure DASHSCOPE_API_KEY in ~/.claude/voice/.env to enable qwen3-asr-flash STT.'
        } else {
          // Download audio + transcribe
          const tmpPath = join(INBOX_DIR, `voice_stt_${Date.now()}.opus`)
          try {
            const dlResp = await client.im.v1.messageResource.get({
              path: { message_id, file_key },
              params: { type: 'file' },
            })
            await dlResp.writeFile(tmpPath)

            // Retry loop matching telegram-voice (qwen3-asr-flash transient failures)
            const MAX_ATTEMPTS = 3
            const BACKOFF_MS = [500, 1500]
            let sttOut = ''
            let sttErr = ''
            for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
              const sttStart = Date.now()
              const result = spawnSync(
                VOICE_PYTHON,
                [join(homedir(), '.claude/voice/transcribe.py'), '--provider', STT_PROVIDER, tmpPath],
                { timeout: 60000, encoding: 'utf-8' },
              )
              const sttMs = Date.now() - sttStart
              process.stderr.write(
                `feishu STT attempt ${attempt}/${MAX_ATTEMPTS}: ms=${sttMs} status=${result.status} ` +
                `out_chars=${(result.stdout ?? '').length} err_chars=${(result.stderr ?? '').length}\n`,
              )
              if (result.status === 0 && result.stdout && result.stdout.trim().length > 0) {
                sttOut = result.stdout.trim()
                break
              }
              if (result.status === 0) {
                // Empty stdout = audio unrecognizable; retry won't help
                sttErr = '<empty transcription>'
                break
              }
              sttErr = result.stderr || `exit=${result.status}`
              if (attempt < MAX_ATTEMPTS) {
                await new Promise(r => setTimeout(r, BACKOFF_MS[attempt - 1]))
              }
            }
            try { unlinkSync(tmpPath) } catch {}

            if (sttOut) {
              text = sttOut
              process.stderr.write(`feishu STT success: "${text.slice(0, 80)}..." (duration=${duration}ms)\n`)
            } else {
              text = `[Voice message received — STT failed: ${sttErr || 'unknown'}]`
            }
          } catch (e) {
            const errMsg = e instanceof Error ? e.message : String(e)
            text = `[Voice message received — download/transcribe error: ${errMsg}]`
            try { unlinkSync(tmpPath) } catch {}
          }
        }
      } else {
        text = `<received ${msg_type} message — not yet supported (M3+ feature)>`
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

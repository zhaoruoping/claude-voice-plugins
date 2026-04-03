# Claude Voice Plugins

Private plugin marketplace for Claude Code with voice STT/TTS support.

## Telegram Voice Channel

Fork of official `telegram@claude-plugins-official` with:
- **STT**: DashScope `qwen3-asr-flash` auto-transcription for voice messages
- **TTS**: MiniMax `speech-02-turbo` voice reply (`voice_reply` tool)
- **Config**: `VOICE_PYTHON` from `~/.claude/voice/.env` (no hardcoded paths)

### Install

```bash
claude plugin marketplace add zhaoruoping/claude-voice-plugins
claude plugin install telegram-voice@claude-voice-plugins
```

### Setup

1. Create `~/.claude/voice/.env` from `plugins/telegram-voice/voice/.env.example`
2. Install Python deps: `pip install dashscope openai requests "httpx[socks]"`
3. Set `VOICE_PYTHON` to your Python path

### Usage

```bash
claude --dangerously-load-development-channels plugin:telegram-voice@claude-voice-plugins --dangerously-skip-permissions
```

# hurbit

Hermes Agent gateway plugin for Urbit. Enables bidirectional messaging between
a Hermes AI agent and users in an Urbit group — across chat, diary, and heap
channels — via Tlon Messenger.

## How It Works

- **Multi-channel:** Subscribes to all channels in a group (chat, diary, heap) with per-channel agent sessions
- **Receive:** Eyre SSE subscription — instant push delivery, no polling
- **Send:** JSON poke to `channels` agent with the `channel-action` mark (v7)
- **Auth:** Cookie-based via the moon's `+code`, with automatic re-auth
- **Discovery:** Auto-discovers all group channels on startup via scry
- **Access control:** Urbit group membership — managed on your planet, not in Hermes config
- **Config hot-reload:** Per-channel settings in YAML, reloaded on every message (zero restart)

No custom Hoon code. No external dependencies beyond Hermes's bundled `aiohttp`.

## Requirements

- [Hermes Agent](https://github.com/NousResearch/hermes-agent)
- A running Urbit moon (any hosting: local, VPS, Native Planet, Startram)
- A group on your planet with one or more channels, with the moon invited and accepted

## Installation

```bash
# Copy plugin to Hermes plugins directory
mkdir -p ~/.hermes/plugins/urbit
cp plugin.yaml adapter.py __init__.py ~/.hermes/plugins/urbit/

# Enable the plugin
hermes plugins enable urbit-platform

# Add env vars to ~/.hermes/.env
cat >> ~/.hermes/.env << 'EOF'
# Urbit Gateway Plugin
URBIT_SHIP_URL=https://your-moon.example.com
URBIT_SHIP_NAME=~your-moon-name
URBIT_ACCESS_CODE=your-plus-code-here
URBIT_OWNER_SHIP=~your-planet
URBIT_ALLOW_ALL_USERS=true
URBIT_HOME_CHANNEL=chat/~your-planet/channel-slug
# URBIT_GROUP_NAME=My Group Name    # optional: match by title if multiple groups
# URBIT_MENTION_TRIGGERS=hermes     # optional: comma-separated trigger strings
EOF

# Restart the gateway
hermes gateway restart
```

## Configuration

### Environment Variables (`~/.hermes/.env`)

| Variable | Required | Description |
|----------|----------|-------------|
| `URBIT_SHIP_URL` | Yes | Moon's Eyre URL |
| `URBIT_SHIP_NAME` | Yes | Moon's `@p` identity |
| `URBIT_ACCESS_CODE` | Yes | Moon's `+code` output |
| `URBIT_OWNER_SHIP` | Yes | Planet that hosts the group |
| `URBIT_ALLOW_ALL_USERS` | Recommended | `true` — let group membership control access |
| `URBIT_HOME_CHANNEL` | Recommended | Nest of the home/admin channel for cron delivery |
| `URBIT_GROUP_NAME` | No | Match group by title (if moon is in multiple groups) |
| `URBIT_MENTION_TRIGGERS` | No | Comma-separated trigger strings; empty = respond to all |
| `URBIT_ALLOWED_USERS` | No | Comma-separated `@p` allowlist (redundant with group membership) |

### Per-Channel Configuration (`~/.hermes/urbit-channels.yaml`)

Each channel in the group can have its own overrides — system prompt, skills,
reasoning visibility, and more. The file is hot-reloaded on every inbound
message (single `stat()` check, zero cost when unchanged).

```yaml
home: "Home Channel"   # Admin channel — receives cron delivery by default

channels:
  - match: "Home Channel"
    type: chat
    show_reasoning: true
    system_prompt: >
      You are Hermes, an AI assistant communicating via Urbit.
    notes: "Admin channel"

  - match: "Chat"
    type: chat
    system_prompt: "Be concise and casual."
    show_reasoning: false

  - match: "Gallery"
    type: heap
    notes: "Image sharing"

  - match: "Notebook"
    type: diary
    notes: "Long-form notes and summaries"
```

See `urbit-channels.example.yaml` for a full annotated example.

**Supported per-channel fields:**

| Field | Type | Description |
|-------|------|-------------|
| `match` | string | Channel title to match (case-insensitive) |
| `type` | string | Channel kind: `chat`, `diary`, or `heap` |
| `users` | list | `@p` identities allowed in this channel |
| `system_prompt` | string | System prompt injected into agent sessions for this channel |
| `skills` | list | Skills auto-loaded for this channel's sessions |
| `show_reasoning` | bool | Show/hide reasoning blocks in responses (default: false) |
| `cron_delivery` | bool | Mark channel for scheduled delivery |
| `notes` | string | Human-readable description (not used by adapter) |

## Multi-Channel Architecture

On startup, the adapter:
1. Authenticates to the moon
2. Scries the group hosted by `URBIT_OWNER_SHIP` for all channels
3. Scries group metadata to get channel titles
4. Matches each channel against YAML config for overrides
5. Opens a single Eyre channel with subscriptions to all channels
6. Routes inbound messages by nest — each channel gets its own agent session

```
Group channels              Moon                             Hermes Gateway
  Chat ──────────┐          │                                  │
  Notebook ──────┤── posts ▶│                                  │
  Gallery ───────┘          │◀── SSE sub (all channels) ───────│
                            │── push events ──────────────────▶│
                            │                     ┌─ Chat session
                            │                     ├─ Notebook session
                            │                     └─ Gallery session
                            │◀── JSON pokes ──────────────────│
  Chat ◀─────────── replies │                                  │
  Notebook ◀──── diary post │                                  │
```

### Channel Types

| Type | Inbound | Outbound | Notes |
|------|---------|----------|-------|
| **chat** | Text + images | New messages | Standard conversational chat |
| **diary** | Text + images | New notebook entries (with auto-generated title) | Title from first line of content, max 80 chars |
| **heap** | Text + images | New gallery items (optional title) | Heap posts support optional title via metadata |

### Dynamic Channel Management

**`/rescan` command:** Type `/rescan` in any Urbit channel to re-discover
channels. The adapter re-scries the group, diffs against current subscriptions,
and subscribes to any new channels — no gateway restart needed.

**YAML editing:** The agent can edit `~/.hermes/urbit-channels.yaml` directly
via `write_file`. Changes take effect on the next inbound message. Typical flow:

1. Create a new channel in the Tlon UI
2. Tell the agent: "I added a channel called Research"
3. Agent updates the YAML with a config entry
4. Type `/rescan` → adapter discovers and subscribes

## Cron Delivery Routing

Cron jobs can deliver to specific Urbit channels:

```bash
# Deliver to home channel
deliver=urbit

# Deliver to a specific channel by name
deliver=urbit:Notebook

# Deliver back to where the conversation originated
deliver=origin
```

Channel names are resolved by matching against discovered channel titles
(case-insensitive). You can also use a full nest string:
`deliver=urbit:chat/~your-planet/channel-slug`.

## Mention Triggers

`URBIT_MENTION_TRIGGERS` controls when the adapter responds:

```bash
# Respond to everything (default)
URBIT_MENTION_TRIGGERS=

# Only when triggered (case-insensitive substring match)
URBIT_MENTION_TRIGGERS=hermes,@hermes,hey hermes
```

## Optional: urbit-mcp

For broader ship management (scry, file ops, app installs), install
[urbit-mcp](https://github.com/gwbtc/urbit-mcp) on the moon and register
it as an MCP server in `~/.hermes/config.yaml`:

```yaml
mcp_servers:
  urbit:
    url: https://your-moon.example.com/mcp
    headers:
      Cookie: "urbauth-~your-moon=..."
```

Not required for chat messaging.

## Known Limitations

- **Reply threading:** Diary/heap replies (comments under a post) are coded but
  the gateway's `reply_to` parameter isn't reliably passed for plugin platforms yet.
  Responses currently create new top-level entries.
- **Documents:** Tlon SSE yields empty content for PDFs/DOCX. Only images and text
  are supported for inbound parsing.
- **Cookie expiry:** Cookies expire after 30 days; the adapter re-authenticates
  automatically via `+code`.
- **SSE backpressure:** 50 unacknowledged events = Eyre closes the channel. The
  adapter acks every event immediately.

## License

MIT

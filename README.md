# hurbit

Hermes Agent gateway plugin for Urbit. Enables bidirectional messaging between
a Hermes AI agent and an Urbit group chat channel via Tlon Messenger.

## How It Works

- **Receive:** Eyre SSE subscription to a group chat channel — instant push delivery
- **Send:** JSON poke to the `channels` agent with the `channel-action` mark
- **Auth:** Cookie-based via the moon's `+code`
- **Discovery:** Auto-discovers the group chat channel on startup (no opaque IDs)
- **Access control:** Urbit group membership — managed on your planet, not in config

No custom Hoon code. No external dependencies beyond Hermes's bundled `aiohttp`.

## Requirements

- [Hermes Agent](https://github.com/NousResearch/hermes-agent)
- A running Urbit moon (any hosting: local, VPS, Native Planet, Startram)
- A group on your planet with a chat channel, with the moon invited and accepted

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
# URBIT_GROUP_NAME=My Group Name    # optional: match by title if multiple groups
# URBIT_MENTION_TRIGGERS=hermes     # optional: comma-separated trigger strings
EOF

# Restart the gateway
hermes gateway restart
```

## Configuration

All configuration via `~/.hermes/.env`:

| Variable | Required | Description |
|----------|----------|-------------|
| `URBIT_SHIP_URL` | Yes | Moon's Eyre URL |
| `URBIT_SHIP_NAME` | Yes | Moon's `@p` identity |
| `URBIT_ACCESS_CODE` | Yes | Moon's `+code` output |
| `URBIT_OWNER_SHIP` | Yes | Planet that hosts the group |
| `URBIT_ALLOW_ALL_USERS` | Recommended | `true` — let group membership control access |
| `URBIT_GROUP_NAME` | No | Match group by title (if moon is in multiple groups) |
| `URBIT_MENTION_TRIGGERS` | No | Comma-separated trigger strings; empty = respond to all |
| `URBIT_HOME_CHANNEL` | No | Override auto-discovered channel nest |
| `URBIT_ALLOWED_USERS` | No | Comma-separated `@p` allowlist (redundant with group membership) |

### Auto-Discovery

On startup, the adapter:
1. Authenticates to the moon
2. Scries for groups hosted by `URBIT_OWNER_SHIP`
3. If `URBIT_GROUP_NAME` is set, matches by title; otherwise picks the first match
4. Finds the first `chat/` channel in that group
5. Subscribes and starts listening

No need to find or paste opaque channel slugs.

### Mention Triggers

`URBIT_MENTION_TRIGGERS` controls when the adapter responds:

```bash
# Respond to everything (default)
URBIT_MENTION_TRIGGERS=

# Only when triggered (case-insensitive substring match)
URBIT_MENTION_TRIGGERS=hermes,@hermes,hey hermes
```

## Architecture

```
Group members              Moon                             Hermes Gateway
        │                    │                                  │
        │── post ──────────▶│                                  │
        │                    │◀── SSE sub ──────────────────────│
        │                    │── push event ───────────────────▶│
        │                    │                                  │── agent session
        │                    │◀── JSON poke ────────────────────│
        │◀── reply ─────────│                                  │
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

- `hermes status --all` doesn't display plugin platforms (gateway log confirms connection)
- Cookie expires after 30 days; adapter re-authenticates automatically
- The `channel-action` mark accepts JSON; most other Tlon agent marks do not
- 50 unacknowledged SSE events = Eyre closes the channel; adapter acks every event

## License

MIT

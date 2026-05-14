# Hurbit v0.2 Plan — Multi-Channel, Channel Config, Diary/Heap Support

**Date:** 2026-05-13
**Status:** Design phase — requires review before coding

## Overview

Extend the Urbit gateway plugin from single-channel to full group management:
multi-channel subscriptions, per-channel configuration (users, model, skills),
conversational channel management, diary/heap support, and image fix verification.

## 1. Channel Configuration File

### Location: `~/.hermes/urbit-channels.yaml`

Auto-managed by the agent via conversational commands in the home channel.
Also safe to edit manually — agent reads on startup and on re-scan.

```yaml
home: "Michael"  # Channel title used as admin/home channel

channels:
  - match: "Michael"
    type: chat
    users:
      - "~dinnyt-divsud"
    cron_delivery: true
    reasoning_effort: high
    show_reasoning: true
    notes: "Michael's private channel"

  - match: "Megan"
    type: chat
    users:
      - "~rivdut-pitryl"
    cron_delivery: true
    reasoning_effort: medium
    show_reasoning: false
    notes: "Megan's private channel"

  - match: "Atlas Arms"
    type: chat
    users:
      - "~dinnyt-divsud"
      - "~rivdut-pitryl"
    skills:
      - odoo-17-backend-accounting
    system_prompt: "You are assisting with Atlas Arms operations."
    notes: "Shared project channel"

  - match: "Research"
    type: chat
    users:
      - "~dinnyt-divsud"
    model: "anthropic/claude-opus-4"
    provider: "openrouter"
    reasoning_effort: max
    show_reasoning: true
    notes: "Deep research using Claude Opus via OpenRouter"

  - match: "Local Testing"
    type: chat
    model: "unsloth/Qwen3.6-35B-A3B-GGUF:Q4_K_XL"
    provider: "custom:qwen3.6-35b-a3b:q4"
    reasoning_effort: low
    show_reasoning: false
    notes: "Testing with local model"

  - match: "Meeting Notes"
    type: diary
    notes: "Agent writes summaries here"

  - match: "Gallery"
    type: heap
    notes: "Agent posts images/links here"
```

### Field Reference

| Field | Type | Description |
|-------|------|-------------|
| `match` | string | Case-insensitive match against channel title in Tlon UI |
| `type` | string | `chat`, `diary`, or `heap` — informational, adapter infers from nest prefix |
| `users` | list | List of `@p` ships expected in this channel. Omit = any group member |
| `cron_delivery` | bool | This channel is a valid cron delivery target |
| `model` | string | Model override for sessions in this channel |
| `provider` | string | Provider override (e.g. `openrouter`, `nous`, `custom:name`) |
| `reasoning_effort` | string | Per-channel reasoning effort: `none`, `minimal`, `low`, `medium`, `high`, `max`. Omit = use global default |
| `show_reasoning` | bool | Show 💭 reasoning blocks in replies to this channel. Omit = `false` |
| `skills` | list | Auto-load these skills when processing messages from this channel |
| `system_prompt` | string | Additional system prompt injected for this channel's sessions |
| `notes` | string | Human-readable description (agent uses when deciding config) |

### Unconfigured Channels

Channels in the group without a YAML entry get default behavior:
- Respond to all messages from any group member
- Use the default model/provider
- No special skills or system prompt
- Not a cron delivery target

## 2. Multi-Channel Subscription

### Changes to adapter.py

**Current:** `connect()` discovers ONE chat channel and subscribes.

**New:** `connect()` discovers ALL channels in the group (chat + diary + heap),
subscribes to each one on the same Eyre channel.

```python
async def _discover_all_channels(self) -> Dict[str, dict]:
    """Discover all channels in the owner's group.
    
    Returns dict: {nest: channel_info, ...}
    """
    # Same scry logic as before, but return ALL channels, not just first chat/
    ...

async def _subscribe_to_channels(self, nests: List[str]) -> bool:
    """Subscribe to multiple channels on the same Eyre channel."""
    for nest in nests:
        # POST subscribe action for each
        ...
```

**Routing:** When an SSE event arrives, `json.nest` identifies which channel.
The adapter looks up the YAML config for that channel and applies overrides.

### Per-Channel Agent Sessions

The gateway already keys sessions by `chat_id`. Since each channel has a
different nest as `chat_id`, sessions are automatically isolated:
- `chat/~labbel/michael` → Michael's session with memory/context
- `chat/~labbel/megan` → Megan's session, separate memory/context
- `chat/~labbel/atlas` → Shared project session

## 3. Channel Discovery Without Polling

### Two mechanisms (no cron/timer):

**A. Conversational command (primary):**
User tells the agent in the home channel:
- "I added a new chat for Megan"
- "I created a Research notebook"
- "I updated the group, go check"

The agent:
1. Re-scries `channels/channels.json`
2. Diffs against currently subscribed channels
3. Subscribes to new channels
4. Asks user about purpose/model/users for each new channel
5. Updates `urbit-channels.yaml`
6. Confirms: "Got it — listening in 'Megan' for ~rivdut-pitryl"

**B. Group event detection (Approach B — auto):**
Subscribe to `groups /groups/ui` (confirmed accepted during investigation).
If it fires when channels are added:
1. Agent detects new channel automatically
2. Auto-subscribes with default config immediately
3. Messages owner in home channel: "I see 'Research' was added. What's it for?"
4. Owner replies → agent writes config

Both approaches coexist. B gives instant auto-subscribe; A lets the user
explicitly set things up or provide context when B doesn't fire.

## 4. Cron Delivery Routing

`cron_delivery: true` in the YAML marks a channel as a valid delivery target.

### How it works:

- Cron job created from Megan's channel → `deliver=urbit:megan`
  → routes to the channel where `match: "Megan"`
- Cron job from Michael's channel → `deliver=urbit:michael`
- Cron job with just `deliver=urbit` → the `home` channel
- The agent automatically sets the delivery target based on which
  channel the cron request originated from

### Implementation:
The `_env_enablement()` function would need to return multiple
home_channel entries, or the adapter handles routing in `send()` by
looking up the target name against YAML entries.

## 5. Diary and Heap Channel Support

### Reading (SSE subscription):
Expected to use the same `channels /v1/<type>/~host/slug` pattern.
**Needs verification** — subscribe to a diary/heap channel and send a
test post to confirm event format.

### Writing:
Same `channel-action` poke, different `kind-data`:
- Chat: `"kind-data": {"chat": null}`
- Diary: `"kind-data": {"diary": null}` + `metadata: {"title": "...", "image": "..."}`
- Heap: `"kind-data": {"heap": null}`

**Needs verification** — test poke to diary/heap channel.

### Use Cases:
- "Hermes, summarize today's conversation and post to Meeting Notes"
- Agent posts generated images to Gallery
- Daily briefings written as diary entries with titles

## 6. Image Support Status

**Confirmed working (2026-05-13).** The adapter correctly extracts image URLs
from Tlon Story `block.image` content, downloads them via `cache_image_from_url`,
and passes local paths in `event.media_urls` to the agent. The agent's vision
tool receives and analyzes the images.

Note: Image analysis quality depends on the auxiliary vision model/provider
configured in Hermes. An inaccurate description of image content is a model
issue, not an adapter issue.

## 7. Implementation Stages

### Stage A: YAML config loader + multi-channel subscribe
1. Create `urbit-channels.yaml` loader in adapter
2. Change `_discover_channel()` → `_discover_all_channels()`
3. Subscribe to all discovered channels
4. Route incoming events to correct session with per-channel overrides
5. Test: post in two different channels → two different sessions

### Stage B: Per-channel model/provider/skills/reasoning override

**Implemented (no core changes required):**
1. `system_prompt` → set as `channel_prompt` on `MessageEvent` — the gateway injects it as an ephemeral system prompt for that session
2. `skills` → set as `auto_skill` on `MessageEvent` — the gateway auto-loads the specified skills for that session
3. `show_reasoning` → adapter strips 💭 reasoning blocks from outbound messages in `send()` when `show_reasoning: false` (default); keeps them when `true`

**Requires core Hermes changes (future — not in Stage B):**
4. `model` / `provider` → `MessageEvent` has no model/provider override fields. The gateway's `_session_model_overrides` dict is internal to `GatewayRunner` and not accessible from the adapter. Options:
   - Add `model_override` + `provider_override` fields to `MessageEvent` (cleanest)
   - Add a `register_session_override()` method on the base adapter
   - Use a gateway hook to inject overrides pre-session
5. `reasoning_effort` → similarly requires `_session_reasoning_overrides` access, or a `reasoning_config` field on `MessageEvent`

**Test matrix:**
- Message in "Chat" channel → gets "Be concise and casual" system prompt, no reasoning blocks shown
- Message in "Home Channel" → no extra system prompt, reasoning blocks shown (show_reasoning: true)
- Message in channel with `skills: [odoo-17-backend-accounting]` → those skills auto-loaded

### Stage C: Conversational channel management

**Implemented:**
1. **YAML hot-reload** — adapter checks file mtime on every inbound message (single stat() call, ~0 cost). When the agent edits `~/.hermes/urbit-channels.yaml` with `write_file`, changes (system_prompt, skills, show_reasoning, etc.) take effect on the next message — no restart needed.
2. **`/rescan` command** — type `/rescan` in any Urbit channel to trigger immediate re-discovery. The adapter re-scries the group, diffs against current subscriptions, subscribes to new channels on the existing Eyre channel, and reports results (added/removed/unchanged).
3. **Home channel system_prompt** — tells the agent it can manage channels by editing the YAML file. When the user says "I added a Research channel", the agent can `write_file` to update the YAML, then tell the user to type `/rescan`.
4. **`rescan_channels()`** — public async method that re-discovers, diffs, and subscribes. Returns `{"added": [...], "removed": [...], "unchanged": [...]}`.

**Workflow:**
- User creates channel in Tlon UI
- User tells agent in home channel: "I added a new channel called Research"
- Agent edits `~/.hermes/urbit-channels.yaml` with a new entry
- User types `/rescan` → adapter discovers and subscribes to new channel
- Agent confirms: "Got it — listening in Research"

### Stage D: Group event auto-detection
1. Subscribe to `groups /groups/ui`
2. Parse channel-add events (if they fire)
3. Auto-subscribe + ask owner about purpose
4. Test: create channel → agent auto-detects and asks

### Stage E: Diary/heap support
1. Verify subscribe/event format for diary/heap channels
2. Verify poke format for posting to diary/heap
3. Update `send()` to handle different `kind-data` per channel type
4. Test: post to diary, post to heap

### Stage F: Cron delivery routing
1. Implement per-channel delivery targets
2. Route `deliver=urbit:<name>` to the matching YAML channel
3. Test: "remind me tomorrow" in Megan's channel → delivers to Megan's channel

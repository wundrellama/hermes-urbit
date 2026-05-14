     1|# Hurbit v0.2 Plan — Multi-Channel, Channel Config, Diary/Heap Support
     2|
     3|**Date:** 2026-05-13
     4|**Status:** Design phase — requires review before coding
     5|
     6|## Overview
     7|
     8|Extend the Urbit gateway plugin from single-channel to full group management:
     9|multi-channel subscriptions, per-channel configuration (users, model, skills),
    10|conversational channel management, diary/heap support, and image fix verification.
    11|
    12|## 1. Channel Configuration File
    13|
    14|### Location: `~/.hermes/urbit-channels.yaml`
    15|
    16|Auto-managed by the agent via conversational commands in the home channel.
    17|Also safe to edit manually — agent reads on startup and on re-scan.
    18|
    19|```yaml
    20|home: "Alice"  # Channel title used as admin/home channel
    21|
    22|channels:
    23|  - match: "Alice"
    24|    type: chat
    25|    users:
    26|      - "~sampel-palnet"
    27|    cron_delivery: true
    28|    reasoning_effort: high
    29|    show_reasoning: true
    30|    notes: "Alice's private channel"
    31|
    32|  - match: "Bob"
    33|    type: chat
    34|    users:
    35|      - "~wanzod-hosted"
    36|    cron_delivery: true
    37|    reasoning_effort: medium
    38|    show_reasoning: false
    39|    notes: "Bob's private channel"
    40|
    41|  - match: "Acme Corp"
    42|    type: chat
    43|    users:
    44|      - "~sampel-palnet"
    45|      - "~wanzod-hosted"
    46|    skills:
    47|      - project-management
    48|    system_prompt: "You are assisting with Acme Corp operations."
    49|    notes: "Shared project channel"
    50|
    51|  - match: "Research"
    52|    type: chat
    53|    users:
    54|      - "~sampel-palnet"
    55|    model: "anthropic/claude-opus-4"
    56|    provider: "openrouter"
    57|    reasoning_effort: max
    58|    show_reasoning: true
    59|    notes: "Deep research using Claude Opus via OpenRouter"
    60|
    61|  - match: "Local Testing"
    62|    type: chat
    63|    model: "unsloth/Qwen3.6-35B-A3B-GGUF:Q4_K_XL"
    64|    provider: "custom:qwen3.6-35b-a3b:q4"
    65|    reasoning_effort: low
    66|    show_reasoning: false
    67|    notes: "Testing with local model"
    68|
    69|  - match: "Meeting Notes"
    70|    type: diary
    71|    notes: "Agent writes summaries here"
    72|
    73|  - match: "Gallery"
    74|    type: heap
    75|    notes: "Agent posts images/links here"
    76|```
    77|
    78|### Field Reference
    79|
    80|| Field | Type | Description |
    81||-------|------|-------------|
    82|| `match` | string | Case-insensitive match against channel title in Tlon UI |
    83|| `type` | string | `chat`, `diary`, or `heap` — informational, adapter infers from nest prefix |
    84|| `users` | list | List of `@p` ships expected in this channel. Omit = any group member |
    85|| `cron_delivery` | bool | This channel is a valid cron delivery target |
    86|| `model` | string | Model override for sessions in this channel |
    87|| `provider` | string | Provider override (e.g. `openrouter`, `nous`, `custom:name`) |
    88|| `reasoning_effort` | string | Per-channel reasoning effort: `none`, `minimal`, `low`, `medium`, `high`, `max`. Omit = use global default |
    89|| `show_reasoning` | bool | Show 💭 reasoning blocks in replies to this channel. Omit = `false` |
    90|| `skills` | list | Auto-load these skills when processing messages from this channel |
    91|| `system_prompt` | string | Additional system prompt injected for this channel's sessions |
    92|| `notes` | string | Human-readable description (agent uses when deciding config) |
    93|
    94|### Unconfigured Channels
    95|
    96|Channels in the group without a YAML entry get default behavior:
    97|- Respond to all messages from any group member
    98|- Use the default model/provider
    99|- No special skills or system prompt
   100|- Not a cron delivery target
   101|
   102|## 2. Multi-Channel Subscription
   103|
   104|### Changes to adapter.py
   105|
   106|**Current:** `connect()` discovers ONE chat channel and subscribes.
   107|
   108|**New:** `connect()` discovers ALL channels in the group (chat + diary + heap),
   109|subscribes to each one on the same Eyre channel.
   110|
   111|```python
   112|async def _discover_all_channels(self) -> Dict[str, dict]:
   113|    """Discover all channels in the owner's group.
   114|    
   115|    Returns dict: {nest: channel_info, ...}
   116|    """
   117|    # Same scry logic as before, but return ALL channels, not just first chat/
   118|    ...
   119|
   120|async def _subscribe_to_channels(self, nests: List[str]) -> bool:
   121|    """Subscribe to multiple channels on the same Eyre channel."""
   122|    for nest in nests:
   123|        # POST subscribe action for each
   124|        ...
   125|```
   126|
   127|**Routing:** When an SSE event arrives, `json.nest` identifies which channel.
   128|The adapter looks up the YAML config for that channel and applies overrides.
   129|
   130|### Per-Channel Agent Sessions
   131|
   132|The gateway already keys sessions by `chat_id`. Since each channel has a
   133|different nest as `chat_id`, sessions are automatically isolated:
   134|- `chat/~sampel/alice` → Alice's session with memory/context
   135|- `chat/~sampel/bob` → Bob's session, separate memory/context
   136|- `chat/~sampel/acme` → Shared project session
   137|
   138|## 3. Channel Discovery Without Polling
   139|
   140|### Two mechanisms (no cron/timer):
   141|
   142|**A. Conversational command (primary):**
   143|User tells the agent in the home channel:
   144|- "I added a new chat for Bob"
   145|- "I created a Research notebook"
   146|- "I updated the group, go check"
   147|
   148|The agent:
   149|1. Re-scries `channels/channels.json`
   150|2. Diffs against currently subscribed channels
   151|3. Subscribes to new channels
   152|4. Asks user about purpose/model/users for each new channel
   153|5. Updates `urbit-channels.yaml`
   154|6. Confirms: "Got it — listening in 'Bob' for ~wanzod-hosted"
   155|
   156|**B. Group event detection (Approach B — auto):**
   157|Subscribe to `groups /groups/ui` (confirmed accepted during investigation).
   158|If it fires when channels are added:
   159|1. Agent detects new channel automatically
   160|2. Auto-subscribes with default config immediately
   161|3. Messages owner in home channel: "I see 'Research' was added. What's it for?"
   162|4. Owner replies → agent writes config
   163|
   164|Both approaches coexist. B gives instant auto-subscribe; A lets the user
   165|explicitly set things up or provide context when B doesn't fire.
   166|
   167|## 4. Cron Delivery Routing
   168|
   169|`cron_delivery: true` in the YAML marks a channel as a valid delivery target.
   170|
   171|### How it works:
   172|
   173|- Cron job created from Bob's channel → `deliver=urbit:bob`
   174|  → routes to the channel where `match: "Bob"`
   175|- Cron job from Alice's channel → `deliver=urbit:alice`
   176|- Cron job with just `deliver=urbit` → the `home` channel
   177|- The agent automatically sets the delivery target based on which
   178|  channel the cron request originated from
   179|
   180|### Implementation:
   181|The `_env_enablement()` function would need to return multiple
   182|home_channel entries, or the adapter handles routing in `send()` by
   183|looking up the target name against YAML entries.
   184|
   185|## 5. Diary and Heap Channel Support
   186|
   187|### Reading (SSE subscription):
   188|Expected to use the same `channels /v1/<type>/~host/slug` pattern.
   189|**Needs verification** — subscribe to a diary/heap channel and send a
   190|test post to confirm event format.
   191|
   192|### Writing:
   193|Same `channel-action` poke, different `kind-data`:
   194|- Chat: `"kind-data": {"chat": null}`
   195|- Diary: `"kind-data": {"diary": null}` + `metadata: {"title": "...", "image": "..."}`
   196|- Heap: `"kind-data": {"heap": null}`
   197|
   198|**Needs verification** — test poke to diary/heap channel.
   199|
   200|### Use Cases:
   201|- "Hermes, summarize today's conversation and post to Meeting Notes"
   202|- Agent posts generated images to Gallery
   203|- Daily briefings written as diary entries with titles
   204|
   205|## 6. Image Support Status
   206|
   207|**Confirmed working (2026-05-13).** The adapter correctly extracts image URLs
   208|from Tlon Story `block.image` content, downloads them via `cache_image_from_url`,
   209|and passes local paths in `event.media_urls` to the agent. The agent's vision
   210|tool receives and analyzes the images.
   211|
   212|Note: Image analysis quality depends on the auxiliary vision model/provider
   213|configured in Hermes. An inaccurate description of image content is a model
   214|issue, not an adapter issue.
   215|
   216|## 7. Implementation Stages
   217|
   218|### Stage A: YAML config loader + multi-channel subscribe
   219|1. Create `urbit-channels.yaml` loader in adapter
   220|2. Change `_discover_channel()` → `_discover_all_channels()`
   221|3. Subscribe to all discovered channels
   222|4. Route incoming events to correct session with per-channel overrides
   223|5. Test: post in two different channels → two different sessions
   224|
   225|### Stage B: Per-channel model/provider/skills/reasoning override
   226|
   227|**Implemented (no core changes required):**
   228|1. `system_prompt` → set as `channel_prompt` on `MessageEvent` — the gateway injects it as an ephemeral system prompt for that session
   229|2. `skills` → set as `auto_skill` on `MessageEvent` — the gateway auto-loads the specified skills for that session
   230|3. `show_reasoning` → adapter strips 💭 reasoning blocks from outbound messages in `send()` when `show_reasoning: false` (default); keeps them when `true`
   231|
   232|**Requires core Hermes changes (future — not in Stage B):**
   233|4. `model` / `provider` → `MessageEvent` has no model/provider override fields. The gateway's `_session_model_overrides` dict is internal to `GatewayRunner` and not accessible from the adapter. Options:
   234|   - Add `model_override` + `provider_override` fields to `MessageEvent` (cleanest)
   235|   - Add a `register_session_override()` method on the base adapter
   236|   - Use a gateway hook to inject overrides pre-session
   237|5. `reasoning_effort` → similarly requires `_session_reasoning_overrides` access, or a `reasoning_config` field on `MessageEvent`
   238|
   239|**Test matrix:**
   240|- Message in "Chat" channel → gets "Be concise and casual" system prompt, no reasoning blocks shown
   241|- Message in "Home Channel" → no extra system prompt, reasoning blocks shown (show_reasoning: true)
   242|- Message in channel with `skills: [project-management]` → those skills auto-loaded
   243|
   244|### Stage C: Conversational channel management
   245|
   246|**Implemented:**
   247|1. **YAML hot-reload** — adapter checks file mtime on every inbound message (single stat() call, ~0 cost). When the agent edits `~/.hermes/urbit-channels.yaml` with `write_file`, changes (system_prompt, skills, show_reasoning, etc.) take effect on the next message — no restart needed.
   248|2. **`/rescan` command** — type `/rescan` in any Urbit channel to trigger immediate re-discovery. The adapter re-scries the group, diffs against current subscriptions, subscribes to new channels on the existing Eyre channel, and reports results (added/removed/unchanged).
   249|3. **Home channel system_prompt** — tells the agent it can manage channels by editing the YAML file. When the user says "I added a Research channel", the agent can `write_file` to update the YAML, then tell the user to type `/rescan`.
   250|4. **`rescan_channels()`** — public async method that re-discovers, diffs, and subscribes. Returns `{"added": [...], "removed": [...], "unchanged": [...]}`.
   251|
   252|**Workflow:**
   253|- User creates channel in Tlon UI
   254|- User tells agent in home channel: "I added a new channel called Research"
   255|- Agent edits `~/.hermes/urbit-channels.yaml` with a new entry
   256|- User types `/rescan` → adapter discovers and subscribes to new channel
   257|- Agent confirms: "Got it — listening in Research"
   258|
   259|### Stage D: Group event auto-detection (deferred — Option C, future feature)
   260|Stage D was evaluated and deferred. `/rescan` from Stage C covers the use case
   261|with minimal friction. If needed later:
   262|1. Subscribe to `groups /groups/ui`
   263|2. Parse channel-add events (if they fire)
   264|3. Auto-subscribe + ask owner about purpose
   265|
   266|### Stage E: Diary/Heap send support
   267|
   268|**Implemented:**
   269|1. **`_build_kind_data()` helper** — builds the correct v7 `kind-data` object
   270|   based on channel type:
   271|   - Chat: `{"chat": null}`
   272|   - Heap: `{"heap": null}` (optional title from metadata)
   273|   - Diary: `{"diary": {"title": "...", "image": ""}}` with auto-generated
   274|     title from first line of content (max 80 chars), or explicit title from
   275|     metadata
   276|2. **`send()` updated** — resolves channel type from `self._channels[nest]["type"]`
   277|   and passes it to `_build_kind_data()`. Logging now shows channel type and title.
   278|3. **Inbound parsing** — already type-agnostic since Stage A; Story format is
   279|   the same across chat/heap/diary.
   280|4. **Reply poke format** (partially implemented) — `send()` now branches:
   281|   - For diary/heap with `reply_to` set → sends a reply (comment under parent post)
   282|   - Otherwise → creates a new top-level post
   283|   - Reply poke shape: `{channel: {nest, action: {post: {reply: {id: <post-id>, action: {add: <memo>}}}}}}`
   284|   - Memo = `{content: [...], author: "~ship", sent: N}` — no `kind-data` needed for replies
   285|   - `message_id` is now set on inbound `MessageEvent` so the gateway can pass it as `reply_to`
   286|
   287|**Known issue (E4 — reply routing):**
   288|The reply poke code is in `send()` and correct per the Hoon source, but the
   289|gateway's `reply_to` parameter isn't reliably passed for all platforms. The
   290|stream consumer does pass `initial_reply_to_id=event_message_id`, but the
   291|non-streaming path may not. Needs investigation:
   292|- Confirm which gateway code path fires for Urbit responses (streaming vs non-streaming)
   293|- Add debug logging to `send()` to verify `reply_to` value
   294|- If `reply_to` is consistently None, may need to stash `post_id` from the
   295|  inbound event in adapter state (keyed by session) and look it up in `send()`
   296|
   297|**Hoon source reference (from `/sur/channels/hoon` and `/lib/channel-json/hoon`):**
   298|
   299|v7 action types:
   300|```
   301|+$  a-post
   302|  $%  [%add =essay]              :: new top-level post
   303|      [%edit id=id-post =essay]   :: edit existing post
   304|      [%del id=id-post]           :: delete post
   305|      [%reply id=id-post =a-reply]  :: reply to a post
   306|      [%add-react ...]            :: react to post
   307|      [%del-react ...]
   308|  ==
   309|
   310|+$  a-reply
   311|  $%  [%add =memo]              :: new reply (memo, NOT essay — no kind-data)
   312|      [%del id=id-reply]
   313|      [%edit id=id-reply =memo]
   314|      [%add-react ...]
   315|      [%del-react ...]
   316|  ==
   317|```
   318|
   319|v7 essay (top-level post): `{content, author, sent, kind-data}`
   320|v7 memo (reply): `{content, author, sent}` — NO kind-data
   321|
   322|v9+ essay: `{content, author, sent, kind, meta, blob}` (kind is a path like "/chat")
   323|v9+ reply-essay: `{content, author, sent, blob}` — NO kind
   324|
   325|Mark versions:
   326|- `channel/action` (aka `channel-action`) → v7 JSON parser
   327|- `channel/action-1` → v9 JSON parser
   328|- `channel/action-2` → v10 JSON parser (adds bot author objects)
   329|
   330|We use v7 mark. It works. v9+ would give us `meta` (title/desc/image/cover)
   331|and `blob` (custom payload) on posts, but v7 is simpler and sufficient.
   332|
   333|### Stage F: Cron delivery routing
   334|
   335|**Analysis:** Most of the plumbing already exists:
   336|- `cron_deliver_env_var="URBIT_HOME_CHANNEL"` is registered in the plugin
   337|- The cron scheduler resolves `deliver=urbit` → reads `URBIT_HOME_CHANNEL` env → calls `adapter.send(chat_id, content)`
   338|- For `deliver=urbit:<nest>` the explicit nest is passed directly as `chat_id`
   339|- `send()` already resolves both nest strings AND channel titles as fallback
   340|
   341|**Implementation:**
   342|1. ✅ `URBIT_HOME_CHANNEL=chat/~host/slug` already set in `~/.hermes/.env`
   343|2. ✅ `cron_deliver_env_var="URBIT_HOME_CHANNEL"` registered in plugin
   344|3. ✅ `send()` title-based resolution works — `deliver=urbit:Bob` resolved channel name → nest → delivered
   345|4. ✅ Tested: cron job with `deliver=urbit:Bob` delivered to Bob's channel
   346|5. Untested: `deliver=origin` routing (should work — origin tracks platform + chat_id from inbound message)
   347|
   348|**Title resolution enhancement (if needed):**
   349|`send()` already does title→nest fallback matching. If `deliver=urbit:Bob` doesn't
   350|work because the scheduler calls `adapter.send("Bob", content)`, the existing
   351|fallback loop in `send()` should match it. If not, may need to also check YAML
   352|`match` field during resolution.
   353|
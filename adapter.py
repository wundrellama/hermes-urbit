"""
Urbit Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to an Urbit ship (moon) via
Eyre HTTP and relays messages between group chat channels and the Hermes
agent. Uses SSE subscriptions for instant message delivery and JSON pokes
for sending replies.

Supports multi-channel subscriptions with per-channel configuration via
~/.hermes/urbit-channels.yaml. Each channel gets its own isolated agent
session keyed by its nest (e.g. chat/~host/slug).

Zero external dependencies beyond Hermes's existing aiohttp.

Configuration via environment variables (see plugin.yaml):
    URBIT_SHIP_URL      — Moon's Eyre URL
    URBIT_SHIP_NAME     — Moon's @p identity
    URBIT_ACCESS_CODE   — Moon's +code
    URBIT_OWNER_SHIP    — Planet hosting the C&C group
    URBIT_GROUP_NAME    — (optional) Group title to match
    URBIT_MENTION_TRIGGERS — (optional) Comma-separated trigger strings
"""

import asyncio
import json
import logging
import os
import time
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin

import aiohttp

logger = logging.getLogger(__name__)

from gateway.platforms.base import (
    BasePlatformAdapter,
    SendResult,
    MessageEvent,
    MessageType,
)
from gateway.session import SessionSource
from gateway.config import PlatformConfig, Platform


# ---------------------------------------------------------------------------
# Urbit Adapter
# ---------------------------------------------------------------------------

class UrbitAdapter(BasePlatformAdapter):
    """Async Urbit adapter implementing the BasePlatformAdapter interface.

    Connects to an Urbit ship via Eyre HTTP, auto-discovers group chat
    channels, subscribes via SSE for incoming messages, and sends replies
    via JSON poke with the channel-action mark.

    Supports multi-channel subscriptions: all chat channels in the owner's
    group are discovered and subscribed. Per-channel config is loaded from
    ~/.hermes/urbit-channels.yaml.
    """

    MAX_MESSAGE_LENGTH = 10000  # Tlon chat chunk limit
    CHANNELS_YAML_FILENAME = "urbit-channels.yaml"

    def __init__(self, config, **kwargs):
        platform = Platform("urbit")
        super().__init__(config=config, platform=platform)

        extra = getattr(config, "extra", {}) or {}

        # Connection settings (env vars override config.yaml)
        self.ship_url = os.getenv("URBIT_SHIP_URL") or extra.get("ship_url", "")
        self.ship_name = os.getenv("URBIT_SHIP_NAME") or extra.get("ship_name", "")
        self.access_code = os.getenv("URBIT_ACCESS_CODE") or extra.get("access_code", "")
        self.owner_ship = os.getenv("URBIT_OWNER_SHIP") or extra.get("owner_ship", "")

        # Optional config
        self.group_name = os.getenv("URBIT_GROUP_NAME") or extra.get("group_name", "")
        self.home_channel_override = os.getenv("URBIT_HOME_CHANNEL") or extra.get("home_channel", "")
        self.mention_triggers = self._parse_triggers(
            os.getenv("URBIT_MENTION_TRIGGERS") or extra.get("mention_triggers", "")
        )

        # Runtime state
        self._cookie: Optional[str] = None
        self._eyre_channel_id: Optional[str] = None
        self._sse_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._event_id_counter = 0
        self._last_acked_event = -1

        # Multi-channel state
        # {nest: {title, type, group_flag, config}} — discovered channels + YAML config overlay
        self._channels: Dict[str, Dict[str, Any]] = {}
        # The nest of the home/admin channel
        self._home_channel: Optional[str] = None
        # Reverse map: subscription_id → nest (for routing SSE events)
        self._sub_id_to_nest: Dict[int, str] = {}
        # Channel YAML config (raw parsed dict)
        self._channel_configs: List[Dict[str, Any]] = []
        self._channel_yaml_home: Optional[str] = None  # "home" field from YAML
        self._channel_yaml_mtime: float = 0.0  # Last known mtime for hot-reload

    @property
    def name(self) -> str:
        return "Urbit"

    # ── Helpers ────────────────────────────────────────────────────────────

    @staticmethod
    def _parse_triggers(raw: str) -> List[str]:
        """Parse comma-separated trigger strings into a list."""
        if not raw or not raw.strip():
            return []
        return [t.strip().lower() for t in raw.split(",") if t.strip()]

    def _url(self, path: str) -> str:
        """Build a full URL from the ship URL and a path."""
        base = self.ship_url.rstrip("/")
        return f"{base}{path}"

    def _ship_name_no_sig(self) -> str:
        """Return ship name without the leading ~."""
        return self.ship_name.lstrip("~")

    def _new_channel_id(self) -> str:
        """Generate an Eyre channel ID."""
        return f"{int(time.time())}-{uuid.uuid4().hex[:12]}"

    # ── Authentication ─────────────────────────────────────────────────────

    async def _authenticate(self) -> bool:
        """Login to Eyre and obtain an urbauth cookie.

        Returns True on success, False on failure.
        """
        login_url = self._url("/~/login")
        data = f"password={self.access_code}"

        try:
            async with self._session.post(
                login_url,
                data=data,
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    # Extract cookie from Set-Cookie header
                    cookie_header = resp.headers.get("Set-Cookie", "")
                    if "urbauth" in cookie_header:
                        self._cookie = cookie_header.split(";")[0].strip()
                        logger.info("Urbit: authenticated to %s", self.ship_url)
                        return True
                    # Also check response body (Eyre returns the cookie value)
                    body = await resp.text()
                    if body.strip():
                        self._cookie = f"urbauth-{self.ship_name}={body.strip()}"
                        logger.info("Urbit: authenticated to %s", self.ship_url)
                        return True

                logger.error("Urbit: login failed (HTTP %d)", resp.status)
                return False

        except Exception as e:
            logger.error("Urbit: authentication error — %s", e)
            return False

    # ── Channel Configuration ───────────────────────────────────────────────

    def _get_channel_yaml_path(self) -> Path:
        """Return the path to the channel YAML config file."""
        try:
            from hermes_constants import get_hermes_home
            return get_hermes_home() / self.CHANNELS_YAML_FILENAME
        except ImportError:
            return Path.home() / ".hermes" / self.CHANNELS_YAML_FILENAME

    def _load_channel_yaml(self) -> None:
        """Load per-channel config from ~/.hermes/urbit-channels.yaml.

        Sets self._channel_configs (list of channel dicts) and
        self._channel_yaml_home (name of the home/admin channel).
        Missing file or parse errors result in empty config (all defaults).
        """
        yaml_path = self._get_channel_yaml_path()

        if not yaml_path.exists():
            logger.info("Urbit: no %s found — using defaults for all channels", yaml_path)
            self._channel_configs = []
            self._channel_yaml_home = None
            return

        try:
            import yaml
            with open(yaml_path, encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning("Urbit: failed to parse %s — %s (using defaults)", yaml_path, e)
            self._channel_configs = []
            self._channel_yaml_home = None
            return

        self._channel_yaml_home = str(data.get("home", "")).strip() or None
        raw_channels = data.get("channels", [])
        if not isinstance(raw_channels, list):
            logger.warning("Urbit: 'channels' in YAML is not a list — ignoring")
            raw_channels = []

        self._channel_configs = raw_channels
        logger.info(
            "Urbit: loaded %d channel configs from %s (home=%s)",
            len(self._channel_configs), yaml_path, self._channel_yaml_home,
        )
        # Track mtime for hot-reload detection
        try:
            self._channel_yaml_mtime = yaml_path.stat().st_mtime
        except OSError:
            self._channel_yaml_mtime = 0.0

    def _match_channel_config(self, title: str) -> Dict[str, Any]:
        """Find the YAML config entry matching a channel title.

        Returns the matching config dict, or {} if no match.
        Match is case-insensitive on the 'match' field.
        """
        title_lower = title.lower()
        for cfg in self._channel_configs:
            match_str = str(cfg.get("match", "")).strip()
            if match_str and match_str.lower() == title_lower:
                return cfg
        return {}

    def _maybe_reload_yaml(self) -> bool:
        """Check if the YAML config file changed and reload if needed.

        Called on each inbound message — cheap (single stat() call).
        Returns True if config was reloaded and channel configs may have changed.
        """
        yaml_path = self._get_channel_yaml_path()
        try:
            current_mtime = yaml_path.stat().st_mtime
        except OSError:
            return False

        if current_mtime <= self._channel_yaml_mtime:
            return False

        logger.info("Urbit: detected YAML config change, reloading...")
        old_configs = {cfg.get("match", ""): cfg for cfg in self._channel_configs}
        self._load_channel_yaml()

        # Re-apply YAML configs to already-discovered channels
        for nest, chan_info in self._channels.items():
            title = chan_info.get("title", "")
            if title:
                chan_info["config"] = self._match_channel_config(title)

        # Check if home channel resolution changed
        new_home = self._resolve_home_channel()
        if new_home != self._home_channel:
            self._home_channel = new_home
            logger.info("Urbit: home channel updated → %s", self._home_channel)

        return True

    async def rescan_channels(self) -> Dict[str, List[str]]:
        """Re-discover channels and subscribe to any new ones.

        Returns {"added": [nest, ...], "removed": [nest, ...], "unchanged": [nest, ...]}.
        Does NOT unsubscribe from removed channels (Eyre doesn't support
        selective unsubscribe on an existing channel cleanly).
        """
        if not self._cookie or not self._session:
            return {"added": [], "removed": [], "unchanged": [], "error": "Not connected"}

        # Reload YAML first
        self._load_channel_yaml()

        # Re-discover all channels
        new_channels = await self._discover_all_channels()
        if not new_channels:
            return {"added": [], "removed": [], "unchanged": [], "error": "Discovery returned no channels"}

        old_nests = set(self._channels.keys())
        new_nests = set(new_channels.keys())

        added = new_nests - old_nests
        removed = old_nests - new_nests
        unchanged = old_nests & new_nests

        # Update channel map
        self._channels = new_channels
        self._home_channel = self._resolve_home_channel()

        # Subscribe to newly discovered channels on the existing Eyre channel
        if added and self._eyre_channel_id:
            actions = []
            for nest in added:
                subscribe_path = f"/v1/{nest}"
                sub_id = self._event_id_counter
                actions.append({
                    "id": sub_id,
                    "action": "subscribe",
                    "ship": self._ship_name_no_sig(),
                    "app": "channels",
                    "path": subscribe_path,
                })
                self._sub_id_to_nest[sub_id] = nest
                self._event_id_counter += 1

            payload = json.dumps(actions)
            channel_url = self._url(f"/~/channel/{self._eyre_channel_id}")
            try:
                async with self._session.post(
                    channel_url,
                    data=payload,
                    headers={
                        "Cookie": self._cookie,
                        "Content-Type": "application/json",
                    },
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status in (200, 204):
                        logger.info("Urbit: subscribed to %d new channels: %s", len(added), list(added))
                    else:
                        logger.error("Urbit: failed to subscribe to new channels (HTTP %d)", resp.status)
            except Exception as e:
                logger.error("Urbit: failed to subscribe to new channels — %s", e)

        result = {
            "added": sorted(added),
            "removed": sorted(removed),
            "unchanged": sorted(unchanged),
        }

        if added:
            for nest in added:
                info = self._channels.get(nest, {})
                logger.info("Urbit: new channel '%s' [%s] subscribed", info.get("title", nest), nest)
        if removed:
            for nest in removed:
                logger.info("Urbit: channel %s no longer in group (will stop receiving on next reconnect)", nest)

        return result

    # ── Channel Discovery ──────────────────────────────────────────────────

    async def _discover_all_channels(self) -> Dict[str, Dict[str, Any]]:
        """Discover all channels in the owner's group.

        Returns {nest: {title, type, group_flag, config}, ...}
        where 'config' is the matching YAML config entry (or {}).

        Scries groups/groups/light.json to find the group flag, then
        channels/channels.json to enumerate channels in that group.
        """
        # Step 1: Find the group
        groups_url = self._url("/~/scry/groups/groups/light.json")
        try:
            async with self._session.get(
                groups_url,
                headers={"Cookie": self._cookie},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.error("Urbit: failed to scry groups (HTTP %d)", resp.status)
                    return {}
                groups_data = await resp.json()
        except Exception as e:
            logger.error("Urbit: failed to scry groups — %s", e)
            return {}

        # Filter for groups hosted by owner_ship
        owner_prefix = f"{self.owner_ship}/"
        matching_flags = [
            flag for flag in groups_data.keys()
            if flag.startswith(owner_prefix)
        ]

        if not matching_flags:
            logger.error(
                "Urbit: no groups found hosted by %s (found: %s)",
                self.owner_ship, list(groups_data.keys())
            )
            return {}

        # If group_name specified, match by title
        target_flag = None
        if self.group_name:
            for flag in matching_flags:
                group_info = groups_data[flag]
                meta = group_info.get("meta", {})
                title = meta.get("title", "")
                if title.lower() == self.group_name.lower():
                    target_flag = flag
                    break
            if not target_flag:
                logger.warning(
                    "Urbit: group '%s' not found among owner's groups, using first match",
                    self.group_name
                )
                target_flag = matching_flags[0]
        else:
            target_flag = matching_flags[0]

        logger.info("Urbit: discovered group %s", target_flag)

        # Step 2: Get channel metadata (titles, descriptions) from the group
        # The full group scry includes a 'channels' map with per-channel meta
        group_channels_meta: Dict[str, Dict] = {}
        group_detail_url = self._url(f"/~/scry/groups/groups/{target_flag}.json")
        try:
            async with self._session.get(
                group_detail_url,
                headers={"Cookie": self._cookie},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    group_detail = await resp.json()
                    group_channels_meta = group_detail.get("channels", {})
                else:
                    logger.warning("Urbit: failed to scry group detail (HTTP %d) — titles unavailable", resp.status)
        except Exception as e:
            logger.warning("Urbit: failed to scry group detail — %s (titles unavailable)", e)

        # Step 3: Scry all channels (for perms/group membership filtering)
        channels_url = self._url("/~/scry/channels/channels.json")
        try:
            async with self._session.get(
                channels_url,
                headers={"Cookie": self._cookie},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.error("Urbit: failed to scry channels (HTTP %d)", resp.status)
                    return {}
                channels_data = await resp.json()
        except Exception as e:
            logger.error("Urbit: failed to scry channels — %s", e)
            return {}

        # Step 4: Filter channels belonging to our group and build the map
        result: Dict[str, Dict[str, Any]] = {}
        for nest, chan_info in channels_data.items():
            perms = chan_info.get("perms", {})
            if perms.get("group") != target_flag:
                continue

            # Determine channel type from nest prefix
            chan_type = nest.split("/")[0] if "/" in nest else "unknown"

            # Get title from GROUP metadata (not from channels agent — it doesn't have titles)
            title = ""
            group_chan_meta = group_channels_meta.get(nest, {})
            if group_chan_meta:
                meta = group_chan_meta.get("meta", {})
                title = meta.get("title", "") if isinstance(meta, dict) else ""

            # Match against YAML config
            config_entry = self._match_channel_config(title) if title else {}

            result[nest] = {
                "title": title,
                "type": chan_type,
                "group_flag": target_flag,
                "config": config_entry,
            }

        if not result:
            logger.error("Urbit: no channels found in group %s", target_flag)
        else:
            for nest, info in result.items():
                cfg_note = " (configured)" if info["config"] else ""
                logger.info(
                    "Urbit: discovered %s '%s' [%s]%s",
                    info["type"], info["title"] or nest, nest, cfg_note,
                )

        return result

    # ── Connection Lifecycle ───────────────────────────────────────────────

    async def connect(self) -> bool:
        """Connect to Urbit: authenticate, discover channels, start SSE listener."""
        if not self.ship_url or not self.ship_name or not self.access_code:
            logger.error("Urbit: URBIT_SHIP_URL, URBIT_SHIP_NAME, and URBIT_ACCESS_CODE must be set")
            self._set_fatal_error(
                "config_missing",
                "URBIT_SHIP_URL, URBIT_SHIP_NAME, and URBIT_ACCESS_CODE must be set",
                retryable=False,
            )
            return False

        if not self.owner_ship:
            logger.error("Urbit: URBIT_OWNER_SHIP must be set")
            self._set_fatal_error(
                "config_missing",
                "URBIT_OWNER_SHIP must be set",
                retryable=False,
            )
            return False

        # Load per-channel YAML config (before discovery so matches work)
        self._load_channel_yaml()

        # Create aiohttp session
        self._session = aiohttp.ClientSession()

        # Authenticate
        if not await self._authenticate():
            await self._session.close()
            self._set_fatal_error("auth_failed", "Failed to authenticate to Urbit ship", retryable=True)
            return False

        # Discover all channels in the group
        self._channels = await self._discover_all_channels()
        if not self._channels:
            await self._session.close()
            self._set_fatal_error(
                "discovery_failed",
                "Could not discover any channels in the group",
                retryable=True,
            )
            return False

        # Determine the home channel
        self._home_channel = self._resolve_home_channel()
        logger.info("Urbit: home channel → %s", self._home_channel)

        # Open Eyre channel and subscribe to all discovered channels
        if not await self._open_eyre_channel():
            await self._session.close()
            self._set_fatal_error("sse_failed", "Failed to open Eyre SSE channel", retryable=True)
            return False

        self._sse_task = asyncio.create_task(self._sse_listener())
        logger.info(
            "Urbit: connected — %d channels subscribed (%s)",
            len(self._channels), ", ".join(self._channels.keys()),
        )
        return True

    def _resolve_home_channel(self) -> Optional[str]:
        """Determine which nest is the home/admin channel.

        Priority:
        1. URBIT_HOME_CHANNEL env var (exact nest override)
        2. YAML 'home' field matched against channel titles
        3. First chat/ channel (fallback)
        """
        # 1. Explicit override
        if self.home_channel_override:
            if self.home_channel_override in self._channels:
                return self.home_channel_override
            # Try matching as a title
            for nest, info in self._channels.items():
                if info["title"].lower() == self.home_channel_override.lower():
                    return nest
            logger.warning(
                "Urbit: URBIT_HOME_CHANNEL '%s' not found, falling back",
                self.home_channel_override,
            )

        # 2. YAML 'home' field
        if self._channel_yaml_home:
            for nest, info in self._channels.items():
                if info["title"].lower() == self._channel_yaml_home.lower():
                    return nest
            logger.warning(
                "Urbit: YAML home '%s' not found among channels",
                self._channel_yaml_home,
            )

        # 3. First chat/ channel
        for nest in self._channels:
            if nest.startswith("chat/"):
                return nest

        # 4. Any channel at all
        return next(iter(self._channels), None)

    async def disconnect(self):
        """Disconnect from Urbit, clean up resources."""
        if self._sse_task and not self._sse_task.done():
            self._sse_task.cancel()
            try:
                await self._sse_task
            except asyncio.CancelledError:
                pass

        if self._session and not self._session.closed:
            await self._session.close()

        self._cookie = None
        self._channels = {}
        self._home_channel = None
        self._eyre_channel_id = None
        self._sub_id_to_nest = {}
        logger.info("Urbit: disconnected")

    # ── Eyre Channel & SSE ─────────────────────────────────────────────────

    async def _open_eyre_channel(self) -> bool:
        """Open an Eyre channel and subscribe to all discovered channels.

        Creates a single Eyre channel and sends one subscribe action per
        discovered channel nest. Builds the _sub_id_to_nest map for routing.

        Returns True on success, False on failure.
        """
        self._eyre_channel_id = self._new_channel_id()
        self._event_id_counter = 1
        self._sub_id_to_nest = {}

        # Build subscribe actions for all channels
        actions = []
        for nest in self._channels:
            # Path format: /v1/chat/~host/slug (prefix nest type with /v1/)
            subscribe_path = f"/v1/{nest}"
            sub_id = self._event_id_counter

            actions.append({
                "id": sub_id,
                "action": "subscribe",
                "ship": self._ship_name_no_sig(),
                "app": "channels",
                "path": subscribe_path,
            })
            self._sub_id_to_nest[sub_id] = nest
            self._event_id_counter += 1

        if not actions:
            logger.error("Urbit: no channels to subscribe to")
            return False

        payload = json.dumps(actions)
        channel_url = self._url(f"/~/channel/{self._eyre_channel_id}")
        try:
            async with self._session.post(
                channel_url,
                data=payload,
                headers={
                    "Cookie": self._cookie,
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status not in (200, 204):
                    logger.error("Urbit: failed to open Eyre channel (HTTP %d)", resp.status)
                    return False
        except Exception as e:
            logger.error("Urbit: failed to open Eyre channel — %s", e)
            return False

        logger.info(
            "Urbit: Eyre channel %s opened, %d subscriptions sent",
            self._eyre_channel_id, len(actions),
        )
        return True

    async def _ack_event(self, event_id: int):
        """Acknowledge an SSE event to prevent Eyre from clogging the channel."""
        if self._last_acked_event >= event_id:
            return

        payload = json.dumps([{
            "id": self._event_id_counter,
            "action": "ack",
            "event-id": event_id,
        }])
        self._event_id_counter += 1

        channel_url = self._url(f"/~/channel/{self._eyre_channel_id}")
        try:
            async with self._session.post(
                channel_url,
                data=payload,
                headers={
                    "Cookie": self._cookie,
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status in (200, 204):
                    self._last_acked_event = event_id
        except Exception as e:
            logger.warning("Urbit: failed to ack event %d — %s", event_id, e)

    async def _sse_listener(self):
        """Persistent SSE listener loop.

        Connects to the Eyre channel's SSE stream, parses events,
        and dispatches incoming messages to the gateway.
        Reconnects with backoff on failure.
        """
        backoff = 1
        max_backoff = 60

        while True:
            try:
                await self._sse_read_loop()
                # If we exit cleanly, the channel was likely closed
                logger.warning("Urbit: SSE stream ended, reconnecting...")
            except asyncio.CancelledError:
                logger.info("Urbit: SSE listener cancelled")
                return
            except Exception as e:
                logger.error("Urbit: SSE listener error — %s", e)

            # Backoff before reconnect
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

            # Re-authenticate and re-subscribe to all channels
            try:
                if await self._authenticate():
                    # Re-discover channels (group may have changed)
                    self._channels = await self._discover_all_channels()
                    if self._channels:
                        self._home_channel = self._resolve_home_channel()
                        if await self._open_eyre_channel():
                            backoff = 1  # Reset on success
                            continue
                logger.error("Urbit: reconnection failed, will retry")
            except asyncio.CancelledError:
                return
            except Exception as e:
                logger.error("Urbit: reconnection error — %s", e)

    async def _sse_read_loop(self):
        """Single SSE read session. Returns when the stream closes."""
        channel_url = self._url(f"/~/channel/{self._eyre_channel_id}")

        async with self._session.get(
            channel_url,
            headers={"Cookie": self._cookie},
            timeout=aiohttp.ClientTimeout(total=0),  # No timeout — long-lived
        ) as resp:
            if resp.status != 200:
                raise RuntimeError(f"SSE stream returned HTTP {resp.status}")

            current_event_id = None
            current_data = ""

            async for line_bytes in resp.content:
                line = line_bytes.decode("utf-8", errors="replace").rstrip("\n\r")

                # SSE format: "id: N", "data: {...}", empty line = end of event
                if line.startswith("id:"):
                    current_event_id = int(line[3:].strip())
                elif line.startswith("data:"):
                    current_data = line[5:].strip()
                elif line == "" and current_data:
                    # End of event — process it
                    await self._handle_sse_event(current_event_id, current_data)
                    current_data = ""
                # ":" lines are heartbeat comments — ignore

    async def _handle_sse_event(self, event_id: Optional[int], raw_data: str):
        """Process a single SSE event from the Eyre channel."""
        # Ack the event
        if event_id is not None:
            await self._ack_event(event_id)

        try:
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.warning("Urbit: malformed SSE event data")
            return

        # Check for subscription confirmation
        if data.get("response") == "subscribe":
            sub_id = data.get("id")
            nest = self._sub_id_to_nest.get(sub_id, "?")
            if "ok" in data:
                logger.info("Urbit: subscription confirmed for %s (id=%s)", nest, sub_id)
            elif "err" in data:
                logger.error("Urbit: subscription failed for %s — %s", nest, data.get("err", "")[:200])
            return

        # Check for poke acknowledgment
        if data.get("response") == "poke":
            if "err" in data:
                logger.warning("Urbit: poke error — %s", data.get("err", "")[:200])
            return

        # Channel message event (channel-response-2 diff)
        if data.get("response") == "diff" and "json" in data:
            await self._handle_channel_event(data["json"])

    async def _handle_channel_event(self, event_json: dict):
        """Handle a channel-response-2 event containing a new post.

        The event JSON includes a 'nest' field identifying which channel
        this message belongs to, enabling multi-channel routing.
        """
        # Expected structure:
        # {"nest": "chat/~host/slug", "response": {"post": {"id": "...",
        #   "r-post": {"set": {"essay": {"author": "~ship", "sent": N,
        #   "content": [...], "kind-data": {"chat": null}}, "type": "post"}}}}

        # Extract nest for routing — this identifies the channel
        nest = event_json.get("nest", "")

        # Hot-reload YAML config if the file changed (cheap stat() check)
        self._maybe_reload_yaml()

        response = event_json.get("response", {})
        post_data = response.get("post")
        if not post_data:
            return

        r_post = post_data.get("r-post", {})
        post_set = r_post.get("set")
        if not post_set:
            return

        essay = post_set.get("essay")
        if not essay:
            return

        author = essay.get("author", "")
        sent = essay.get("sent", 0)
        content = essay.get("content", [])
        post_id = post_data.get("id", "")

        # Skip self-messages
        if author == self.ship_name or author == f"~{self._ship_name_no_sig()}":
            return

        # Extract text content from Story format
        text = self._extract_text_from_content(content)
        image_urls = self._extract_image_urls_from_content(content)
        if not text and not image_urls:
            return

        # Handle /rescan command — intercept before reaching the gateway
        if text and text.strip().lower() == "/rescan":
            await self._handle_rescan_command(nest, author)
            return

        # Apply mention triggers filter
        if self.mention_triggers:
            text_lower = (text or "").lower()
            if not any(trigger in text_lower for trigger in self.mention_triggers):
                return

        # Download images to local cache for vision tool access
        media_urls = []
        media_types = []
        for img_url in image_urls:
            try:
                from gateway.platforms.base import cache_image_from_url
                # Preserve original image format from URL
                from urllib.parse import urlsplit
                url_path = urlsplit(img_url).path
                ext = os.path.splitext(url_path)[1] or ".jpg"
                local_path = await cache_image_from_url(img_url, ext=ext)
                media_urls.append(local_path)
                # Map extension to MIME type
                mime_map = {".jpg": "image/jpeg", ".jpeg": "image/jpeg",
                            ".png": "image/png", ".gif": "image/gif",
                            ".webp": "image/webp", ".svg": "image/svg+xml"}
                media_types.append(mime_map.get(ext.lower(), "image/jpeg"))
            except Exception as e:
                logger.warning("Urbit: failed to cache image %s — %s", img_url[:80], e)

        # Look up channel info for this nest
        chan_info = self._channels.get(nest, {})
        chan_title = chan_info.get("title", nest)
        chan_type = chan_info.get("type", "chat")
        chan_config = chan_info.get("config", {})

        # Build message event — chat_id is the nest, giving each channel its own session
        source = self.build_source(
            chat_id=nest or self._home_channel or "",
            chat_name=chan_title,
            chat_type="group",
            user_id=author,
            user_name=author,
        )

        # Resolve per-channel overrides from YAML config
        channel_prompt = (chan_config.get("system_prompt") or "").strip() or None
        skills = chan_config.get("skills")
        auto_skill = skills if isinstance(skills, list) and skills else None

        # Determine message type
        msg_type = MessageType.PHOTO if media_urls else MessageType.TEXT

        event = MessageEvent(
            text=text or "[image]",
            message_type=msg_type,
            message_id=post_id or None,
            source=source,
            raw_message={"post_id": post_id, "essay": essay, "nest": nest},
            media_urls=media_urls,
            media_types=media_types,
            channel_prompt=channel_prompt,
            auto_skill=auto_skill,
        )

        logger.info("Urbit: [%s] message from %s: %s", chan_title or nest, author, (text or "[image]")[:80])
        await self.handle_message(event)

    @staticmethod
    def _extract_text_from_content(content: list) -> str:
        """Extract plain text from Tlon Story format content.

        Story = list of verses. Each verse is either:
        - {"inline": [items...]} where items are strings or rich objects
        - {"block": {...}} for images, code blocks, etc.
        """
        parts = []
        for verse in content:
            if "inline" in verse:
                for item in verse["inline"]:
                    if isinstance(item, str):
                        parts.append(item)
                    elif isinstance(item, dict):
                        # Rich content: mentions, links, formatting
                        if "ship" in item:
                            parts.append(item["ship"])
                        elif "bold" in item:
                            parts.append("".join(item["bold"]))
                        elif "italics" in item:
                            parts.append("".join(item["italics"]))
                        elif "link" in item:
                            link = item["link"]
                            parts.append(link.get("content", link.get("href", "")))
                        elif "code" in item:
                            parts.append(item["code"])
                        elif "break" in item:
                            parts.append("\n")
            elif "block" in verse:
                block = verse["block"]
                if "code" in block:
                    parts.append(f"```\n{block['code'].get('code', '')}\n```")
                # Images are handled separately via _extract_image_urls_from_content
        return "".join(parts).strip()

    @staticmethod
    def _extract_image_urls_from_content(content: list) -> list:
        """Extract image URLs from Tlon Story format content.

        Returns a list of image source URLs found in block.image verses.
        """
        urls = []
        for verse in content:
            if "block" in verse:
                block = verse["block"]
                if "image" in block:
                    src = block["image"].get("src", "")
                    if src:
                        urls.append(src)
        return urls

    # ── Kind-Data Builder ─────────────────────────────────────────────────

    @staticmethod
    def _build_kind_data(
        chan_type: str,
        content: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Build the kind-data object for a channel-action poke (v7 format).

        Chat:  {"chat": null}
        Heap:  {"heap": null}  (or {"heap": "optional title"})
        Diary: {"diary": {"title": "...", "image": ""}}

        For diary, title is pulled from metadata["title"] if present,
        otherwise auto-generated from the first line of content (max 80 chars).
        """
        meta = metadata or {}

        if chan_type == "diary":
            title = meta.get("title", "")
            if not title:
                # Auto-generate from first line of content
                first_line = content.split("\n", 1)[0].strip()
                # Strip markdown heading markers
                first_line = first_line.lstrip("# ").strip()
                if len(first_line) > 80:
                    title = first_line[:77] + "..."
                else:
                    title = first_line or "Untitled"
            image = meta.get("image", "")
            return {"diary": {"title": title, "image": image}}

        elif chan_type == "heap":
            # Heap posts can have an optional title string
            heap_title = meta.get("title")
            return {"heap": heap_title}  # None = no title

        else:
            # Default: chat
            return {"chat": None}

    # ── Commands ────────────────────────────────────────────────────────────

    async def _handle_rescan_command(self, nest: str, author: str):
        """Handle /rescan command — re-discover channels and subscribe to new ones."""
        logger.info("Urbit: /rescan requested by %s in %s", author, nest)

        result = await self.rescan_channels()

        # Build human-readable reply
        parts = ["⛵ **Channel rescan complete**\n"]
        if result.get("error"):
            parts.append(f"⚠️ {result['error']}")
        else:
            if result["added"]:
                for n in result["added"]:
                    info = self._channels.get(n, {})
                    parts.append(f"➕ New: **{info.get('title', n)}** [{info.get('type', '?')}]")
            if result["removed"]:
                for n in result["removed"]:
                    parts.append(f"➖ Removed: {n}")
            if not result["added"] and not result["removed"]:
                parts.append("No changes — all channels up to date.")
            parts.append(f"\n📡 {len(self._channels)} channels active")

        await self.send(nest, "\n".join(parts))

    # ── Sending ─────────────────────────────────────────────────────────────

    @staticmethod
    def _strip_reasoning_block(text: str) -> str:
        """Strip the gateway's 💭 Reasoning block from outbound message text.

        The gateway prepends reasoning as:
            💭 **Reasoning:**
            ```
            <reasoning content>
            ```

            <actual response>

        This method removes that prefix when show_reasoning is false.
        """
        import re
        # Match the reasoning block: 💭 **Reasoning:** followed by a code fence
        stripped = re.sub(
            r'^💭\s*\*{0,2}Reasoning:?\*{0,2}\s*\n```\n.*?\n```\s*\n*',
            '',
            text,
            count=1,
            flags=re.DOTALL,
        )
        return stripped.strip() or text

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to a Urbit channel via JSON poke.

        chat_id is the channel nest (e.g. 'chat/~host/slug').
        Falls back to the home channel if chat_id is not a known nest.
        """
        if not self._cookie or not self._eyre_channel_id:
            return SendResult(success=False, error="Not connected")

        # Resolve the target nest — chat_id should be a nest from build_source
        nest = chat_id
        if nest not in self._channels:
            # Maybe it's a channel title? Try matching.
            resolved = None
            for n, info in self._channels.items():
                if info["title"].lower() == nest.lower():
                    resolved = n
                    break
            if resolved:
                nest = resolved
            elif self._home_channel:
                logger.warning(
                    "Urbit: chat_id '%s' not a known nest, falling back to home channel",
                    chat_id,
                )
                nest = self._home_channel
            else:
                return SendResult(success=False, error=f"Unknown channel: {chat_id}")

        # Per-channel show_reasoning filter
        # The gateway may prepend 💭 **Reasoning:** blocks to the content.
        # Strip them unless the channel's YAML config has show_reasoning: true.
        chan_info = self._channels.get(nest, {})
        chan_config = chan_info.get("config", {})
        show_reasoning = chan_config.get("show_reasoning", False)
        if not show_reasoning:
            content = self._strip_reasoning_block(content)

        # Determine channel type and build appropriate kind-data
        chan_type = chan_info.get("type", "chat")  # chat, heap, or diary

        # Build the channel-action poke payload
        now_ms = int(time.time() * 1000)

        # For diary/heap channels with a reply_to post ID, respond as a
        # comment under the original post rather than creating a new entry.
        # Replies use "memo" (content + author + sent) — no kind-data.
        # Chat channels always create top-level posts (no reply threading).
        use_reply = (
            reply_to
            and chan_type in ("diary", "heap")
        )

        if use_reply:
            # Reply poke: {channel: {nest, action: {post: {reply: {id, action: {add: memo}}}}}}
            poke_json = {
                "channel": {
                    "nest": nest,
                    "action": {
                        "post": {
                            "reply": {
                                "id": reply_to,
                                "action": {
                                    "add": {
                                        "content": [{"inline": [content]}],
                                        "author": self.ship_name,
                                        "sent": now_ms,
                                    }
                                }
                            }
                        }
                    }
                }
            }
        else:
            # Top-level post poke (chat, or diary/heap when creating a new entry)
            kind_data = self._build_kind_data(chan_type, content, metadata)
            poke_json = {
                "channel": {
                    "nest": nest,
                    "action": {
                        "post": {
                            "add": {
                                "author": self.ship_name,
                                "sent": now_ms,
                                "content": [{"inline": [content]}],
                                "kind-data": kind_data,
                            }
                        }
                    }
                }
            }

        # Send via Eyre channel poke
        poke_payload = json.dumps([{
            "id": self._event_id_counter,
            "action": "poke",
            "ship": self._ship_name_no_sig(),
            "app": "channels",
            "mark": "channel-action",
            "json": poke_json,
        }])
        self._event_id_counter += 1

        channel_url = self._url(f"/~/channel/{self._eyre_channel_id}")
        try:
            async with self._session.post(
                channel_url,
                data=poke_payload,
                headers={
                    "Cookie": self._cookie,
                    "Content-Type": "application/json",
                },
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status in (200, 204):
                    action = "reply" if use_reply else "post"
                    logger.info(
                        "Urbit: sent %s %s (%d chars) to %s",
                        chan_type, action, len(content),
                        chan_info.get("title", nest),
                    )
                    return SendResult(success=True)
                else:
                    logger.error("Urbit: send failed (HTTP %d)", resp.status)
                    return SendResult(success=False, error=f"HTTP {resp.status}")
        except Exception as e:
            logger.error("Urbit: send error — %s", e)
            return SendResult(success=False, error=str(e))

    async def send_typing(self, chat_id: str):
        """Urbit has no typing indicator."""
        pass

    async def send_image(self, chat_id: str, image_url: str, caption: str = None, **kwargs) -> SendResult:
        """Send an image (as a link in message content)."""
        msg = f"{caption}\n{image_url}" if caption else image_url
        return await self.send(chat_id, msg)

    async def get_chat_info(self, chat_id: str) -> dict:
        """Return chat info for a given chat_id (nest)."""
        chan_info = self._channels.get(chat_id, {})
        return {
            "name": chan_info.get("title", chat_id),
            "type": chan_info.get("type", "group"),
            "chat_id": chat_id,
        }


# ---------------------------------------------------------------------------
# Plugin Registration Functions
# ---------------------------------------------------------------------------

def check_requirements() -> bool:
    """Check if Urbit adapter can run (aiohttp is always available in Hermes)."""
    try:
        import aiohttp  # noqa: F401
        return True
    except ImportError:
        return False


def validate_config(config: PlatformConfig) -> bool:
    """Validate the Urbit platform config. Returns True if valid."""
    extra = getattr(config, "extra", {}) or {}
    url = os.getenv("URBIT_SHIP_URL") or extra.get("ship_url", "")
    name = os.getenv("URBIT_SHIP_NAME") or extra.get("ship_name", "")
    code = os.getenv("URBIT_ACCESS_CODE") or extra.get("access_code", "")
    owner = os.getenv("URBIT_OWNER_SHIP") or extra.get("owner_ship", "")
    return bool(url and name and code and owner)


def is_connected(config: PlatformConfig) -> bool:
    """Quick check: is the adapter likely configured and connectable?"""
    return validate_config(config)


def interactive_setup(current_config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Interactive setup wizard for Urbit platform (called by hermes gateway setup)."""
    # The plugin env var system handles prompting for requires_env and optional_env.
    # This function is a placeholder for any additional setup logic.
    return None


def _env_enablement() -> Optional[Dict[str, Any]]:
    """Env-driven auto-configuration — seeds PlatformConfig.extra from env vars.

    Called by the plugin system to detect whether the platform should be
    enabled based on environment variables alone.
    """
    url = os.getenv("URBIT_SHIP_URL")
    name = os.getenv("URBIT_SHIP_NAME")
    code = os.getenv("URBIT_ACCESS_CODE")
    owner = os.getenv("URBIT_OWNER_SHIP")

    if not (url and name and code and owner):
        return None

    seed: Dict[str, Any] = {
        "ship_url": url,
        "ship_name": name,
        "access_code": code,
        "owner_ship": owner,
    }

    if os.getenv("URBIT_GROUP_NAME"):
        seed["group_name"] = os.getenv("URBIT_GROUP_NAME")
    if os.getenv("URBIT_MENTION_TRIGGERS"):
        seed["mention_triggers"] = os.getenv("URBIT_MENTION_TRIGGERS")
    if os.getenv("URBIT_HOME_CHANNEL"):
        seed["home_channel"] = os.getenv("URBIT_HOME_CHANNEL")

    # Home channel for cron delivery
    home = os.getenv("URBIT_HOME_CHANNEL") or ""
    if home:
        seed["home_channel_info"] = {
            "chat_id": home,
            "name": home,
        }

    return seed


def register(ctx):
    """Plugin entry point — called by the Hermes plugin system."""
    ctx.register_platform(
        name="urbit",
        label="Urbit",
        adapter_factory=lambda cfg: UrbitAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_connected,
        required_env=["URBIT_SHIP_URL", "URBIT_SHIP_NAME", "URBIT_ACCESS_CODE", "URBIT_OWNER_SHIP"],
        install_hint="No extra packages needed (uses Hermes's bundled aiohttp)",
        setup_fn=interactive_setup,
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="URBIT_HOME_CHANNEL",
        allowed_users_env="URBIT_ALLOWED_USERS",
        allow_all_env="URBIT_ALLOW_ALL_USERS",
        max_message_length=10000,
        emoji="⛵",
        pii_safe=True,  # Urbit @p identities are public
        allow_update_command=True,
        platform_hint=(
            "You are chatting via Urbit (Tlon Messenger group channel). "
            "Messages support basic formatting. Keep responses reasonably concise. "
            "Users are identified by their Urbit @p identity (e.g. ~sampel-palnet). "
            "You can use mentions like ~ship-name to reference users."
        ),
    )

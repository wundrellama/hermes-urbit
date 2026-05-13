"""
Urbit Platform Adapter for Hermes Agent.

A plugin-based gateway adapter that connects to an Urbit ship (moon) via
Eyre HTTP and relays messages between a group chat channel and the Hermes
agent. Uses SSE subscriptions for instant message delivery and JSON pokes
for sending replies.

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

    Connects to an Urbit ship via Eyre HTTP, auto-discovers the group chat
    channel, subscribes via SSE for incoming messages, and sends replies
    via JSON poke with the channel-action mark.
    """

    MAX_MESSAGE_LENGTH = 10000  # Tlon chat chunk limit

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
        self._channel_nest: Optional[str] = None
        self._eyre_channel_id: Optional[str] = None
        self._sse_task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._event_id_counter = 0
        self._last_acked_event = -1

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

    # ── Channel Discovery ──────────────────────────────────────────────────

    async def _discover_channel(self) -> Optional[str]:
        """Auto-discover the group chat channel.

        Logic:
        1. Scry groups/groups/light.json → find groups hosted by owner_ship
        2. If group_name set, match by title; else pick first match
        3. Scry channels/channels.json → find first chat/ channel in that group
        4. Return the channel nest string

        Returns None on failure.
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
                    return None
                groups_data = await resp.json()
        except Exception as e:
            logger.error("Urbit: failed to scry groups — %s", e)
            return None

        # groups_data is {flag: group_info, ...}
        # Filter for groups where flag starts with owner_ship
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
            return None

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

        # Step 2: Find chat channel in this group
        channels_url = self._url("/~/scry/channels/channels.json")
        try:
            async with self._session.get(
                channels_url,
                headers={"Cookie": self._cookie},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    logger.error("Urbit: failed to scry channels (HTTP %d)", resp.status)
                    return None
                channels_data = await resp.json()
        except Exception as e:
            logger.error("Urbit: failed to scry channels — %s", e)
            return None

        # channels_data is {nest: channel_info, ...}
        # Find first chat/ channel whose perms.group matches our target_flag
        for nest, chan_info in channels_data.items():
            if not nest.startswith("chat/"):
                continue
            perms = chan_info.get("perms", {})
            if perms.get("group") == target_flag:
                logger.info("Urbit: discovered channel %s", nest)
                return nest

        # Fallback: find a chat channel hosted by the owner ship
        for nest in channels_data.keys():
            if nest.startswith(f"chat/{self.owner_ship}/"):
                logger.info("Urbit: discovered channel %s (fallback by host)", nest)
                return nest

        logger.error("Urbit: no chat channel found in group %s", target_flag)
        return None

    # ── Connection Lifecycle ───────────────────────────────────────────────

    async def connect(self) -> bool:
        """Connect to Urbit: authenticate, discover channel, start SSE listener."""
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

        # Create aiohttp session
        self._session = aiohttp.ClientSession()

        # Authenticate
        if not await self._authenticate():
            await self._session.close()
            self._set_fatal_error("auth_failed", "Failed to authenticate to Urbit ship", retryable=True)
            return False

        # Discover channel (use override if set)
        if self.home_channel_override:
            self._channel_nest = self.home_channel_override
            logger.info("Urbit: using configured channel %s", self._channel_nest)
        else:
            self._channel_nest = await self._discover_channel()
            if not self._channel_nest:
                await self._session.close()
                self._set_fatal_error(
                    "discovery_failed",
                    "Could not auto-discover group chat channel",
                    retryable=True,
                )
                return False

        logger.info("Urbit: connected — channel %s", self._channel_nest)

        # Open Eyre channel and start SSE listener
        if not await self._open_eyre_channel():
            await self._session.close()
            self._set_fatal_error("sse_failed", "Failed to open Eyre SSE channel", retryable=True)
            return False

        self._sse_task = asyncio.create_task(self._sse_listener())
        logger.info("Urbit: SSE listener started")
        return True

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
        self._channel_nest = None
        self._eyre_channel_id = None
        logger.info("Urbit: disconnected")

    # ── Eyre Channel & SSE ─────────────────────────────────────────────────

    async def _open_eyre_channel(self) -> bool:
        """Open an Eyre channel and subscribe to the group chat channel.

        Returns True on successful subscription, False on failure.
        """
        self._eyre_channel_id = self._new_channel_id()
        self._event_id_counter = 1

        # Subscribe to the channels agent for our specific chat channel
        # Path format: /v1/chat/~host/slug
        # Nest format: chat/~host/slug → path needs /v1/ prefix
        subscribe_path = f"/v1/{self._channel_nest}"

        payload = json.dumps([{
            "id": self._event_id_counter,
            "action": "subscribe",
            "ship": self._ship_name_no_sig(),
            "app": "channels",
            "path": subscribe_path,
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
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status not in (200, 204):
                    logger.error("Urbit: failed to open Eyre channel (HTTP %d)", resp.status)
                    return False
        except Exception as e:
            logger.error("Urbit: failed to open Eyre channel — %s", e)
            return False

        logger.info("Urbit: Eyre channel %s opened, subscribed to %s",
                    self._eyre_channel_id, subscribe_path)
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

            # Re-authenticate and re-subscribe
            try:
                if await self._authenticate():
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
            if "ok" in data:
                logger.info("Urbit: subscription confirmed (id=%s)", data.get("id"))
            elif "err" in data:
                logger.error("Urbit: subscription failed — %s", data.get("err", "")[:200])
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
        """Handle a channel-response-2 event containing a new post."""
        # Expected structure:
        # {"nest": "chat/~host/slug", "response": {"post": {"id": "...",
        #   "r-post": {"set": {"essay": {"author": "~ship", "sent": N,
        #   "content": [...], "kind-data": {"chat": null}}, "type": "post"}}}}}

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
        if not text:
            return

        # Apply mention triggers filter
        if self.mention_triggers:
            text_lower = text.lower()
            if not any(trigger in text_lower for trigger in self.mention_triggers):
                return

        # Build message event and dispatch to gateway
        timestamp = sent / 1000.0 if sent > 1_000_000_000_000 else sent

        source = self.build_source(
            chat_id=self._channel_nest or "",
            chat_name=self._channel_nest or "",
            chat_type="group",
            user_id=author,
            user_name=author,
        )

        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            raw_message={"post_id": post_id, "essay": essay},
        )

        logger.info("Urbit: message from %s: %s", author, text[:80])
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
                elif "image" in block:
                    parts.append(f"[image: {block['image'].get('alt', '')}]")
        return "".join(parts).strip()

    # ── Sending ─────────────────────────────────────────────────────────────

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        """Send a message to the Urbit group chat channel via JSON poke."""
        if not self._cookie or not self._channel_nest:
            return SendResult(success=False, error="Not connected")

        # Use the discovered channel nest (chat_id is usually the nest)
        nest = self._channel_nest

        # Build the channel-action poke payload
        now_ms = int(time.time() * 1000)
        poke_json = {
            "channel": {
                "nest": nest,
                "action": {
                    "post": {
                        "add": {
                            "author": self.ship_name,
                            "sent": now_ms,
                            "content": [{"inline": [content]}],
                            "kind-data": {"chat": None},
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
                    logger.info("Urbit: sent message (%d chars)", len(content))
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
        """Return chat info for a given chat_id."""
        return {
            "name": self._channel_nest or chat_id,
            "type": "group",
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
    return validate_config(config) is None


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

"""Neurora remote view signaling server.

A faithful Python/aiohttp port of the original Node.js `server.js`. The wire
protocol, message validation, room/session semantics and TURN configuration
are identical; only the runtime differs.

Deploy behind a stable HTTPS/WSS hostname. A room supports one publisher and
up to [DEFAULT_MAX_VIEWERS] viewers; a deliberate refresh gets a new
negotiation.

Protocol 4 (was 3):

- A room holds several viewers, each with its own `session`. The session id
  names one publisher<->viewer *link*, not the room: an offer, answer,
  candidate or restart belonging to one viewer can never touch another's.
  The publisher runs one PeerConnection per session.
- Offers may carry audio (`m=audio`). Only `m=application` is refused -
  nothing on either side speaks data channels, and an accidental one would
  negotiate a transport neither peer reads.
- `gaze` (publisher -> every viewer) carries the scene-normalised gaze point
  so the browser can draw the same reticle the phone draws.
- `tag-request` and `stop-request` (any viewer -> publisher) drive the
  phone's own tag and stop controls from the browser.
- No viewer is paired without the publisher's explicit say-so. A viewer
  sends `access-request`; the server relays it to the publisher as
  `{"type": "access-request", "viewer": <id>}`; only once the publisher
  answers with `{"type": "access-response", "viewer": <id>, "accepted":
  true}` does the server mint a session and send `ready`. `accepted: false`
  (or anything else) sends the viewer `access-declined` instead - the
  viewer's socket is left exactly as it was, so it can ask again. This gate
  applies to every new pairing, including a restart and a stale link picked
  back up after the publisher itself resumes - not only a viewer's first
  join. It does not apply to a plain resume of a session whose media
  connection never actually died (the server sends `resumed`, not `ready`,
  for that case): nothing new is being granted there, only a signaling
  socket reconnecting to a link that was never revoked.

Two additive, fully optional wire fields (no version bump - an older client
or server on either end is unaffected either way):

- A publisher may connect with `?deviceName=<name>` on its `/signal/{room}`
  URL (e.g. "Tobii Pro Glasses 3") - if set, every `ready` the server later
  sends a viewer for that room carries `"deviceName"`. Absent entirely if
  the publisher never supplied one.
- `state` (publisher -> every viewer) may carry `"deviceBatteries"`: a list
  of `{"label": <str>, "percent": <0-100>}` for the paired capture device's
  own battery level(s) - NOT the phone's battery. Forwarded as `[]` when
  absent from the publisher's message, same fail-closed validation as
  `tags` when present but malformed.
- A viewer's `view-request` may carry `"viewerName"`/`"role"` (free-text,
  sanitized/truncated the same way as `deviceName`) - if present, the
  `access-request` this raises for the publisher carries them unchanged, so
  its approval prompt can show who's asking. Omitted entirely when the
  viewer didn't send one.
- A publisher may also connect with `?accountName=<name>&accountRole=<role>`
  alongside `?deviceName=`, describing the signed-in capture account rather
  than the glasses. Whenever a viewer's signaling socket opens for a room
  that already has a publisher, its own `joined` reply carries whichever of
  `deviceName`/`accountName`/`accountRole` the publisher supplied - sent
  immediately, independent of `access-request`/`access-response`, since this
  is "who would I be connecting to" information, not media access.

A third additive layer, `view-request`/`view-pending`/`view-approved`/
`view-declined`/`view-expired`: a tracked, user-facing wrapper a viewer may
use instead of the bare `access-request` above. `view-request` (optionally
carrying `"capabilities"`, a list of strings echoed back unchanged on
approval) mints a `requestId` and replies `view-pending` with an
`expiresAt` (epoch ms); it then raises the exact same `access-request` the
publisher already answers with `access-response`. That answer is mirrored
back to the viewer as `view-approved` (carrying the session `pair()`
minted and the echoed capabilities) or `view-declined`, or - if the
publisher never answers within `view_request_ttl_ms` - `view-expired`. A
`requestId` is only ever present for a link a viewer explicitly opened this
way; the `access-request`s this server raises on its own (a restart, a
stale link resuming) stay exactly as before and never produce a `view-*`
message, since the client that already holds a live session isn't watching
for one.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import math
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote

import aiohttp
from aiohttp import WSMsgType, web

PUBLIC_DIR = Path(__file__).resolve().parent / "public"

SIGNAL_PATH = re.compile(r"^/signal/([A-Za-z0-9_-]{3,96})$")
# Node used a /m regex against the SDP; Python needs MULTILINE explicitly.
#
# Audio used to be refused here alongside data channels, back when the
# publisher sent scene video and nothing else. It is now an ordinary second
# m-line (the glasses' own microphone) and must pass. A data channel still
# must
# not: neither client reads one, so an offer carrying it negotiates a
# transport that can only sit there consuming a slot.
DATA_MLINE = re.compile(r"^m=application\s", re.MULTILINE)

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
}
STATIC_FILES = {"/": "index.html", "/viewer.js": "viewer.js", "/viewer.css": "viewer.css"}

# How many viewers one room accepts at once. Every viewer is a separate
# PeerConnection out of the phone - a separate encoder, and a separate copy
# of the stream on the uplink - so this number is a load decision about the
# phone, not about this server, which only ever moves signaling JSON.
# `MAX_VIEWERS` in the environment overrides it.
DEFAULT_MAX_VIEWERS = 5


def _now_ms() -> float:
    """Monotonic milliseconds. The only consumer is the restart debounce, which
    measures an elapsed interval - a clock that cannot jump backwards is the
    right one for that, unlike Date.now()."""
    return time.monotonic() * 1000.0


def _is_int(value: Any) -> bool:
    """Number.isInteger(). `bool` is a subclass of `int` in Python, and `True`
    is not an integer as far as this protocol is concerned."""
    return isinstance(value, int) and not isinstance(value, bool)


def _is_finite_number(value: Any) -> bool:
    """Number.isFinite() - rejects NaN, the infinities, and booleans."""
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def _sanitize_label(value: Any, max_len: int = 100) -> str | None:
    """Fail-closed cleanup for a free-text identity field (viewer name/role,
    capture account name/role) arriving from either a query param or a
    signaling message: non-string, blank, and empty become `None` rather
    than being fabricated or passed through unchecked, and length is capped
    the same way `deviceName` already is."""
    if not isinstance(value, str):
        return None
    return value.strip()[:max_len] or None


# Cloudflare Realtime TURN takes priority when configured (CF_TURN_KEY_ID +
# CF_TURN_API_TOKEN): its own free tier (500GB/month, own account, not a
# shared public one) is far less likely to be rate-limited than the Open
# Relay Project fallback below, which is a single set of credentials shared
# by everyone who's ever copied it from a tutorial. Its response is already
# shaped as {iceServers: [...]} - returned directly, no local TURN math.
async def ice_servers(env: Mapping[str, str] | None = None) -> list[dict[str, Any]]:
    env = os.environ if env is None else env
    if env.get("CF_TURN_KEY_ID") and env.get("CF_TURN_API_TOKEN"):
        url = (
            "https://rtc.live.cloudflare.com/v1/turn/keys/"
            f"{env['CF_TURN_KEY_ID']}/credentials/generate-ice-servers"
        )
        headers = {
            "Authorization": f"Bearer {env['CF_TURN_API_TOKEN']}",
            "Content-Type": "application/json",
        }
        async with aiohttp.ClientSession() as http:
            async with http.post(url, headers=headers, json={"ttl": 86400}) as response:
                if not (200 <= response.status < 300):
                    raise RuntimeError(
                        f"Cloudflare TURN credential request failed ({response.status})"
                    )
                data = await response.json(content_type=None)
                return data["iceServers"]
    # Xirsys next (XIRSYS_IDENT + XIRSYS_SECRET + XIRSYS_CHANNEL) - same
    # reasoning as Cloudflare: a free-tier account of your own, not the
    # shared public Open Relay credentials.
    if env.get("XIRSYS_IDENT") and env.get("XIRSYS_SECRET") and env.get("XIRSYS_CHANNEL"):
        auth = base64.b64encode(
            f"{env['XIRSYS_IDENT']}:{env['XIRSYS_SECRET']}".encode()
        ).decode()
        url = (
            f"https://global.xirsys.net/_turn/{quote(env['XIRSYS_CHANNEL'], safe='')}"
            "?webrtc=1&expire=21600"
        )
        async with aiohttp.ClientSession() as http:
            async with http.put(url, headers={"Authorization": f"Basic {auth}"}) as response:
                if not (200 <= response.status < 300):
                    raise RuntimeError(f"Xirsys TURN credential request failed ({response.status})")
                data = await response.json(content_type=None)
                if data.get("s") != "ok":
                    raise RuntimeError(
                        f"Xirsys TURN credential request failed: {json.dumps(data)}"
                    )
                return data["v"]["iceServers"]
    servers: list[dict[str, Any]] = [{"urls": "stun:stun.l.google.com:19302"}]
    if env.get("TURN_URLS"):
        urls = [s.strip() for s in env["TURN_URLS"].split(",") if s.strip()]
        if env.get("TURN_SHARED_SECRET"):
            username = f"{int(time.time()) + 3600}:neurora"
            credential = base64.b64encode(
                hmac.new(
                    env["TURN_SHARED_SECRET"].encode(), username.encode(), hashlib.sha1
                ).digest()
            ).decode()
            servers.append({"urls": urls, "username": username, "credential": credential})
        elif env.get("TURN_USERNAME") and env.get("TURN_CREDENTIAL"):
            servers.append(
                {
                    "urls": urls,
                    "username": env["TURN_USERNAME"],
                    "credential": env["TURN_CREDENTIAL"],
                }
            )
        else:
            raise RuntimeError(
                "TURN_URLS requires TURN_SHARED_SECRET or TURN_USERNAME and TURN_CREDENTIAL"
            )
    else:
        # Preserve the existing Open Relay configuration; an operator can replace
        # it through environment variables without rebuilding either client.
        #
        # Both UDP ports lead, then TCP, then TLS. ICE prefers the first working
        # candidate it gathers and these are tried in order, so the ordering
        # decides what a phone on a restrictive network ends up on. A UDP relay
        # carries video the way video expects to be carried; a TCP or TLS one
        # wraps it in a reliable ordered stream, where a single lost segment
        # holds up every frame behind it and the retransmissions read to
        # congestion control as available capacity. The result is a connection
        # that "works" while delivering a stuttering, seconds-behind picture no
        # encoder setting can rescue - so TCP and TLS stay in the list as the
        # last resort they are, never as the first thing tried.
        servers.append(
            {
                "urls": [
                    "turn:global.relay.metered.ca:80",
                    "turn:global.relay.metered.ca:443",
                    "turn:global.relay.metered.ca:80?transport=tcp",
                    "turn:global.relay.metered.ca:443?transport=tcp",
                    "turns:global.relay.metered.ca:443?transport=tcp",
                ],
                "username": "openrelayproject",
                "credential": "openrelayproject",
            }
        )
    return servers


def turn_provider(env: Mapping[str, str] | None = None) -> dict[str, Any]:
    """Which TURN credentials [ice_servers] will hand out, and whether they are
    this deployment's own.

    `dedicated: False` means the shared Open Relay Project fallback - one set
    of credentials published in every WebRTC tutorial on the internet and used
    by everyone who has ever copied one. It is fine for a first smoke test and
    unfit for anything else: allocations are rate-limited and frequently
    refused outright, and when they do succeed the relay throttles throughput
    to a fraction of the link's real capacity. A phone measuring "30-120 kbps
    of cellular uplink" through it is measuring the relay, not the radio.

    Reported on /ice-servers and logged at startup because that failure is
    otherwise indistinguishable from a bad network: the call connects, video
    flows, and it is simply terrible.
    """
    env = os.environ if env is None else env
    if env.get("CF_TURN_KEY_ID") and env.get("CF_TURN_API_TOKEN"):
        return {"name": "cloudflare", "dedicated": True}
    if env.get("XIRSYS_IDENT") and env.get("XIRSYS_SECRET") and env.get("XIRSYS_CHANNEL"):
        return {"name": "xirsys", "dedicated": True}
    if env.get("TURN_URLS"):
        return {"name": "self-hosted", "dedicated": True}
    return {"name": "open-relay-shared", "dedicated": False}


# A re-pair mints a new session and costs both peers a full teardown and
# rebuild of their PeerConnection - the viewer's video element goes black
# and re-buffers. Publisher and viewer both watch for a stall, so a single
# bad moment on the network reliably produced two `restart` messages a
# fraction of a second apart, and the second one threw away the connection
# the first had just brought up. Ignore a restart that lands inside this
# window of the last pairing.
RESTART_DEBOUNCE_MS = 3000

# How long a `view-request` waits for the publisher's decision before the
# viewer is told `view-expired`. Independent of the publisher's own socket
# heartbeat - a publisher that is connected but simply hasn't tapped
# Approve/Deny yet is the normal case this bounds, not a failure.
VIEW_REQUEST_TTL_MS = 30000


class ViewerSlot:
    """One viewer's place in a room: its socket, its resume token, and the
    session naming its own link to the publisher.

    The slot outlives the socket. A mobile network drops the signaling
    WebSocket every ~70-90s while the media connection carries on working, so
    a reconnect presenting [token] lands back on this same slot, keeps
    [session], and the publisher is never told anything happened.
    """

    __slots__ = (
        "id",
        "socket",
        "token",
        "session",
        "paired_at",
        "restart_timer",
        "restart_pending",
        "expiry_timer",
        "awaiting_access",
        "view_request_id",
        "view_capabilities",
        "view_request_timer",
        "viewer_name",
        "viewer_role",
    )

    def __init__(self, viewer_id: str, token: str) -> None:
        self.id = viewer_id
        self.token = token
        self.socket: web.WebSocketResponse | None = None
        self.session: str | None = None
        self.paired_at: float | None = None
        self.restart_timer: asyncio.TimerHandle | None = None
        self.restart_pending: bool = False
        # Set only while the slot is being held open for a viewer whose
        # socket dropped abruptly - see the close handler.
        self.expiry_timer: asyncio.TimerHandle | None = None
        # True from the moment this link needs a fresh session until the
        # publisher answers `access-response` for it. See `request_access`.
        self.awaiting_access: bool = False
        # Set only for a link currently inside the `view-request` ->
        # `view-pending` -> {approved|declined|expired} lifecycle - see
        # `start_view_request`. `None` for every `access-request` this
        # server raises on its own (restart, a stale link resuming): those
        # stay on the plain `access-request`/`access-declined` shape a
        # client that already holds a session isn't watching for.
        self.view_request_id: str | None = None
        self.view_capabilities: list[str] = []
        self.view_request_timer: asyncio.TimerHandle | None = None
        # The requesting viewer's own display name/role, as sent on its
        # `view-request` - purely informational, relayed unchanged to the
        # publisher's `access-request` so its approval prompt can show who's
        # asking. `None` for a plain `access-request` this server raises on
        # its own (restart, a stale link resuming), same as view_request_id.
        self.viewer_name: str | None = None
        self.viewer_role: str | None = None

    def cancel_timers(self) -> None:
        for timer in (self.restart_timer, self.expiry_timer, self.view_request_timer):
            if timer:
                timer.cancel()
        self.restart_timer = None
        self.expiry_timer = None
        self.view_request_timer = None


class Room:
    """One publisher and up to `max_viewers` viewers.

    There is no room-wide session any more. Each [ViewerSlot] carries its
    own, which is what lets one viewer renegotiate, stall, resume or leave
    without the others noticing - with a single shared session id, any of
    those would have invalidated every other viewer's in-flight signaling.
    """

    __slots__ = (
        "publisher",
        "publisher_token",
        "publisher_device_name",
        "publisher_account_name",
        "publisher_account_role",
        "viewers",
        "cleanup_timer",
    )

    def __init__(self) -> None:
        self.publisher: web.WebSocketResponse | None = None
        self.publisher_token: str | None = None
        # The paired capture device's own name (e.g. "Tobii Pro Glasses 3"),
        # as the publisher reported it on connect - see `signal_handler`'s
        # publisher branch. `None` until a publisher has ever supplied one.
        self.publisher_device_name: str | None = None
        # The signed-in capture account's own name/role, as the publisher
        # reported it on connect (`?accountName=`/`?accountRole=`) - distinct
        # from publisher_device_name, which describes the glasses, not the
        # person. `None` until a publisher has ever supplied one.
        self.publisher_account_name: str | None = None
        self.publisher_account_role: str | None = None
        self.viewers: dict[str, ViewerSlot] = {}
        self.cleanup_timer: asyncio.TimerHandle | None = None

    def viewer_by_session(self, session: Any) -> ViewerSlot | None:
        if not isinstance(session, str) or not session:
            return None
        for slot in self.viewers.values():
            if slot.session == session:
                return slot
        return None

    def viewer_by_token(self, token: Any) -> ViewerSlot | None:
        if not isinstance(token, str) or not token:
            return None
        for slot in self.viewers.values():
            if slot.token == token:
                return slot
        return None

    @property
    def connected_viewers(self) -> list[ViewerSlot]:
        return [slot for slot in self.viewers.values() if slot.socket is not None]

    @property
    def paired_viewers(self) -> list[ViewerSlot]:
        return [slot for slot in self.connected_viewers if slot.session]


class SignalingServer:
    """The aiohttp application plus the room table, so tests can inspect state
    the way the Node tests read `app.rooms`."""

    def __init__(self, app: web.Application, rooms: dict[str, Room], env: Mapping[str, str]):
        self.app = app
        self.rooms = rooms
        self.env = env


def create_server(
    env: Mapping[str, str] | None = None,
    resume_ttl_ms: int = 120000,
    restart_debounce_ms: int = RESTART_DEBOUNCE_MS,
    max_viewers: int | None = None,
    view_request_ttl_ms: int = VIEW_REQUEST_TTL_MS,
) -> SignalingServer:
    env = os.environ if env is None else env
    if max_viewers is None:
        max_viewers = int(env.get("MAX_VIEWERS") or DEFAULT_MAX_VIEWERS)
    max_viewers = max(1, max_viewers)
    rooms: dict[str, Room] = {}
    sockets: set[web.WebSocketResponse] = set()
    timer_tasks: set[asyncio.Task[Any]] = set()

    async def send(socket: web.WebSocketResponse | None, message: dict[str, Any]) -> None:
        if socket is None or socket.closed:
            return
        try:
            await socket.send_json(message)
        except (ConnectionResetError, RuntimeError):
            pass

    def later(delay_ms: float, factory) -> asyncio.TimerHandle:
        """setTimeout for a coroutine. The task reference is held until it
        finishes so the event loop cannot garbage-collect it mid-flight."""
        loop = asyncio.get_running_loop()

        def fire() -> None:
            task = asyncio.ensure_future(factory())
            timer_tasks.add(task)
            task.add_done_callback(timer_tasks.discard)

        return loop.call_later(max(0.0, delay_ms) / 1000.0, fire)

    # ------------------------------------------------------------------ http
    async def http_handler(request: web.Request) -> web.StreamResponse:
        headers = {
            "Cache-Control": "no-store",
            "X-Content-Type-Options": "nosniff",
            "Referrer-Policy": "no-referrer",
        }
        if request.method != "GET":
            return web.Response(status=405, headers=headers)
        path = request.path
        if path == "/health":
            return web.json_response(
                {"ok": True, "protocol": 4, "media": "video+audio", "maxViewers": max_viewers},
                headers=headers,
            )
        if path == "/ice-servers":
            try:
                servers = await ice_servers(env)
                ice_transport_policy = (
                    "relay" if str(env.get("FORCE_RELAY", "")).lower() == "true" else "all"
                )
                provider = turn_provider(env)
                return web.json_response(
                    {
                        "iceServers": servers,
                        "iceTransportPolicy": ice_transport_policy,
                        "turnProvider": provider["name"],
                        "turnDedicated": provider["dedicated"],
                    },
                    headers=headers,
                )
            except Exception as e:  # noqa: BLE001 - mirrors the Node catch-all
                return web.Response(
                    status=503, text=json.dumps({"error": str(e)}), headers=headers
                )
        name = STATIC_FILES.get(path)
        if not name:
            return web.Response(status=404, text="Not found", headers=headers)
        target = PUBLIC_DIR / name
        try:
            body = target.read_bytes()
        except OSError:
            return web.Response(status=404, text="Not found", headers=headers)
        return web.Response(
            body=body, headers={**headers, "Content-Type": CONTENT_TYPES[target.suffix]}
        )

    # ------------------------------------------------------------- signaling
    async def pair(room: Room, slot: ViewerSlot) -> None:
        """Mint a fresh session for one publisher<->viewer link and tell both
        ends. The publisher's copy carries `viewer`, the slot's stable id
        across re-pairings, so the two sides' logs can be lined up.

        Only ever called once the publisher has granted `access-response`
        for this slot - see `request_access` and its call sites below."""
        if room.publisher is None or slot.socket is None:
            return
        if slot.restart_timer:
            slot.restart_timer.cancel()
            slot.restart_timer = None
        slot.restart_pending = False
        slot.awaiting_access = False
        slot.session = str(uuid.uuid4())
        slot.paired_at = _now_ms()
        ready_for_viewer: dict[str, Any] = {"type": "ready", "session": slot.session}
        if room.publisher_device_name:
            ready_for_viewer["deviceName"] = room.publisher_device_name
        await send(slot.socket, ready_for_viewer)
        await send(room.publisher, {"type": "ready", "session": slot.session, "viewer": slot.id})

    async def request_access(room: Room, slot: ViewerSlot) -> None:
        """Ask the publisher whether this link may be (re)paired, instead of
        minting a session outright.

        Every site that used to call `pair()` directly on its own initiative
        - a fresh viewer joining an already-present publisher, a publisher
        arriving to viewers still waiting, a restart, or a stale link being
        picked back up after the publisher itself resumes - calls this
        instead. `pair()` now only runs from `on_publisher_message`'s
        `access-response` handler, once accepted.

        A no-op if the publisher is not connected yet: the slot is left
        marked, and whichever branch next assigns `room.publisher` relays
        the outstanding request then.
        """
        if slot.socket is None:
            return
        if slot.restart_timer:
            slot.restart_timer.cancel()
            slot.restart_timer = None
        slot.awaiting_access = True
        request_msg: dict[str, Any] = {"type": "access-request", "viewer": slot.id}
        if slot.viewer_name:
            request_msg["viewerName"] = slot.viewer_name
        if slot.viewer_role:
            request_msg["role"] = slot.viewer_role
        await send(room.publisher, request_msg)

    async def start_view_request(room: Room, slot: ViewerSlot, msg: dict[str, Any]) -> None:
        """The tracked, user-facing entry point for a fresh viewer's own
        request: mint a `requestId`, tell the viewer it's `view-pending`,
        then raise the exact same `access-request` [request_access] already
        knows how to relay and have answered.

        Same guard as the legacy `access-request` branch below, and for the
        same reason - a client retrying defensively (or a duplicate tap)
        must not queue a second request for a link already paired or
        already awaiting one.
        """
        if slot.session is not None or slot.awaiting_access:
            return
        if slot.view_request_timer:
            slot.view_request_timer.cancel()
            slot.view_request_timer = None
        request_id = str(uuid.uuid4())
        slot.view_request_id = request_id
        slot.viewer_name = _sanitize_label(msg.get("viewerName"))
        slot.viewer_role = _sanitize_label(msg.get("role"))
        raw_capabilities = msg.get("capabilities")
        slot.view_capabilities = (
            [c for c in raw_capabilities if isinstance(c, str)]
            if isinstance(raw_capabilities, list)
            else []
        )
        expires_at = int(time.time() * 1000) + view_request_ttl_ms

        async def expire_view_request() -> None:
            slot.view_request_timer = None
            # Superseded by a later request, or already resolved - the
            # `awaiting_access` publisher-side guard already ignores a late
            # `access-response` for this same reason.
            if slot.view_request_id != request_id or not slot.awaiting_access:
                return
            slot.awaiting_access = False
            slot.view_request_id = None
            slot.view_capabilities = []
            await send(slot.socket, {"type": "view-expired", "requestId": request_id})

        slot.view_request_timer = later(view_request_ttl_ms, expire_view_request)
        await send(
            slot.socket,
            {"type": "view-pending", "requestId": request_id, "expiresAt": expires_at},
        )
        await request_access(room, slot)

    async def broadcast_viewers(room: Room) -> None:
        """Tell everyone how full the room is. The phone reports it as
        "N watching"; the browser uses it to explain a room-full refusal
        without a second round trip."""
        payload = {
            "type": "viewers",
            "count": len(room.connected_viewers),
            "max": max_viewers,
        }
        await send(room.publisher, payload)
        for slot in room.connected_viewers:
            await send(slot.socket, payload)

    async def signal_handler(request: web.Request) -> web.StreamResponse:
        match = SIGNAL_PATH.match(request.path)
        role = request.query.get("role")
        resume = request.query.get("resume") or None
        device_name_param = (request.query.get("deviceName") or "").strip()[:100] or None
        account_name_param = _sanitize_label(request.query.get("accountName"))
        account_role_param = _sanitize_label(request.query.get("accountRole"))
        if not match or role not in ("publisher", "viewer"):
            return web.Response(status=400, text="Bad Request")
        name = match.group(1)

        socket = web.WebSocketResponse(max_msg_size=128 * 1024, heartbeat=30)
        await socket.prepare(request)
        sockets.add(socket)

        room = rooms.get(name)
        if room is None:
            room = Room()
            rooms[name] = room
        if room.cleanup_timer:
            room.cleanup_timer.cancel()
            room.cleanup_timer = None

        # A mobile network can drop the signaling socket outright every ~70-90s
        # (carrier NAT/radio rebinding) while the actual WebRTC media connection
        # keeps working fine. A reconnect that presents the resume token it was
        # given on first join is treated as the same peer resuming - no fresh
        # offer/answer, so a healthy video connection is left untouched instead
        # of being torn down and rebuilt on every such blip.
        slot: ViewerSlot | None = None
        if role == "publisher":
            is_resume = bool(resume) and resume == room.publisher_token
            if room.publisher is not None and not is_resume:
                await send(
                    socket,
                    {
                        "type": "error",
                        "code": "room-busy",
                        "message": "This room already has a publisher.",
                    },
                )
                await socket.close(code=4009, message=b"Room busy")
                sockets.discard(socket)
                return socket
            if room.publisher is not None and is_resume:
                # The previous socket for this role hasn't fired its close event yet.
                try:
                    await room.publisher.close(code=4000, message=b"Superseded by reconnect")
                except Exception:  # noqa: BLE001
                    pass
            if not is_resume:
                room.publisher_token = str(uuid.uuid4())
            room.publisher = socket
            if device_name_param is not None:
                room.publisher_device_name = device_name_param
            if account_name_param is not None:
                room.publisher_account_name = account_name_param
            if account_role_param is not None:
                room.publisher_account_role = account_role_param
            await send(
                socket,
                {
                    "type": "joined",
                    "role": role,
                    "media": "video+audio",
                    "resumeToken": room.publisher_token,
                    "maxViewers": max_viewers,
                },
            )
            if is_resume:
                # Which sessions the publisher may keep, and which it must
                # renegotiate, in one message. With several viewers there is
                # no single session to resume, and making the phone guess
                # would cost a healthy peer its connection every time some
                # *other* viewer happened to be mid-restart.
                # Deliberately every slot with a session, not only the
                # ones whose socket is up: signaling and media are
                # separate transports, and a viewer whose WebSocket is
                # mid-reconnect still has a working video connection
                # this publisher must not tear down.
                intact = [
                    s.session for s in room.viewers.values()
                    if s.session and not s.restart_pending
                ]
                stale = [s for s in room.connected_viewers if not s.session or s.restart_pending]
                print(
                    f"[{name}] publisher resumed ({len(intact)} session(s) intact, "
                    f"{len(stale)} to renegotiate)",
                    flush=True,
                )
                await send(socket, {"type": "resumed", "sessions": intact})
                for pending in stale:
                    await request_access(room, pending)
            else:
                print(f"[{name}] publisher joined fresh", flush=True)
                for existing in room.connected_viewers:
                    await request_access(room, existing)
        else:
            slot = room.viewer_by_token(resume) if resume else None
            is_resume = slot is not None
            if slot is not None and slot.socket is not None:
                try:
                    await slot.socket.close(code=4000, message=b"Superseded by reconnect")
                except Exception:  # noqa: BLE001
                    pass
            if slot is None:
                if len(room.viewers) >= max_viewers:
                    await send(
                        socket,
                        {
                            "type": "error",
                            "code": "room-full",
                            "message": (
                                f"This session is already being watched by {max_viewers} "
                                "people."
                            ),
                        },
                    )
                    await socket.close(code=4010, message=b"Room full")
                    sockets.discard(socket)
                    return socket
                slot = ViewerSlot(str(uuid.uuid4()), str(uuid.uuid4()))
                room.viewers[slot.id] = slot
            slot.socket = socket
            if slot.expiry_timer:
                slot.expiry_timer.cancel()
                slot.expiry_timer = None
            joined_for_viewer: dict[str, Any] = {
                "type": "joined",
                "role": role,
                "media": "video+audio",
                "resumeToken": slot.token,
                "viewerId": slot.id,
                "maxViewers": max_viewers,
            }
            # "Who would I be connecting to" - sent as soon as the viewer's
            # socket opens, independent of access-request/access-response:
            # this is identity, not media access, so it isn't gated on the
            # publisher's approval the way `ready` is.
            if room.publisher_device_name:
                joined_for_viewer["deviceName"] = room.publisher_device_name
            if room.publisher_account_name:
                joined_for_viewer["accountName"] = room.publisher_account_name
            if room.publisher_account_role:
                joined_for_viewer["accountRole"] = room.publisher_account_role
            await send(socket, joined_for_viewer)
            if is_resume and slot.session and not slot.restart_pending:
                print(f"[{name}] viewer resumed (session {slot.session})", flush=True)
                await send(socket, {"type": "resumed", "session": slot.session})
            else:
                if is_resume:
                    reason = (
                        "resumed into pending renegotiation"
                        if slot.restart_pending
                        else "presented a resume token but no session was live"
                    )
                else:
                    reason = "joined fresh"
                print(
                    f"[{name}] viewer {reason} "
                    f"({len(room.connected_viewers)}/{max_viewers} watching)",
                    flush=True,
                )
                if is_resume:
                    # The link once had the publisher's say-so and is only
                    # being picked back up - the server asks again on the
                    # viewer's behalf rather than making it re-request.
                    await request_access(room, slot)
                # A genuinely fresh viewer has asked for nothing yet: wait
                # for its own explicit `access-request` (see
                # on_viewer_message) before troubling the publisher.
        await broadcast_viewers(room)

        async def request_restart(target: ViewerSlot, who: str) -> None:
            """Re-pair one link, coalescing the duplicate its two ends produce
            off the same stall."""
            since_pair = _now_ms() - (target.paired_at or 0.0)
            if target.paired_at is not None and since_pair < restart_debounce_ms:
                # Deferred, not dropped: a restart this soon is either the second
                # peer echoing the first, or a pairing that failed immediately. It
                # is coalesced into one re-pair once the window closes, so both
                # peers get exactly one rebuild and neither has to retry.
                if not target.restart_timer:

                    async def fire_restart() -> None:
                        target.restart_timer = None
                        if rooms.get(name) is not room or not target.session:
                            return
                        target.restart_pending = True
                        await request_access(room, target)

                    target.restart_timer = later(restart_debounce_ms - since_pair, fire_restart)
                print(
                    f"[{name}] {who} restart coalesced - paired {round(since_pair)}ms ago",
                    flush=True,
                )
                return
            target.restart_pending = True
            await request_access(room, target)

        async def relay_candidate(
            msg: dict[str, Any], destination: web.WebSocketResponse | None, session: str | None
        ) -> None:
            candidate = msg.get("candidate")
            sdp_mline_index = msg.get("sdpMLineIndex")
            if not (
                isinstance(candidate, str)
                and len(candidate) < 4096
                and _is_int(sdp_mline_index)
            ):
                return
            sdp_mid = msg.get("sdpMid")
            await send(
                destination,
                {
                    "type": "candidate",
                    "session": session,
                    "candidate": candidate,
                    "sdpMid": sdp_mid if isinstance(sdp_mid, str) else None,
                    "sdpMLineIndex": sdp_mline_index,
                },
            )

        async def relay_state(msg: dict[str, Any]) -> None:
            """The phone's own recording clock + tag list, mirrored to every
            viewer so each web page shows exactly what the app does.

            Capped and shape-checked like everything else on this socket -
            this is untrusted client input, not merely a size limit.

            elapsedSeconds is a plain COUNT the phone already computed for its
            own on-screen clock, not a timestamp - the viewer just displays it.
            An earlier version sent a wall-clock start time and let the viewer
            compute `now - start`, which assumed the phone's and the browser's
            clocks agreed; they routinely don't, and the displayed clock came
            out wrong by however far the two had drifted.
            """
            recording = msg.get("recording")
            elapsed = msg.get("elapsedSeconds")
            raw_tags = msg.get("tags")
            if (
                not isinstance(recording, bool)
                or not _is_int(elapsed)
                or elapsed < 0
                or not isinstance(raw_tags, list)
                or len(raw_tags) > 500
            ):
                return
            tags = []
            for tag in raw_tags:
                if (
                    not isinstance(tag, dict)
                    or not _is_int(tag.get("id"))
                    or not _is_finite_number(tag.get("tagSeconds"))
                    or not isinstance(tag.get("label"), str)
                    or len(tag["label"]) > 200
                ):
                    return
                tags.append(
                    {"id": tag["id"], "label": tag["label"], "tagSeconds": tag["tagSeconds"]}
                )
            # Optional: the paired capture device's own battery level(s) -
            # e.g. the glasses, NOT the phone's own battery. Absent entirely
            # is fine (forwarded as `[]`, same as today); present-but-
            # malformed drops the whole message, same fail-closed rule as
            # `tags` above rather than silently truncating.
            raw_batteries = msg.get("deviceBatteries")
            batteries: list[dict[str, Any]] = []
            if raw_batteries is not None:
                if not isinstance(raw_batteries, list) or len(raw_batteries) > 5:
                    return
                for battery in raw_batteries:
                    if (
                        not isinstance(battery, dict)
                        or not isinstance(battery.get("label"), str)
                        or len(battery["label"]) > 50
                        or not _is_int(battery.get("percent"))
                        or not (0 <= battery["percent"] <= 100)
                    ):
                        return
                    batteries.append({"label": battery["label"], "percent": battery["percent"]})
            # Stamped with each viewer's OWN session: every client drops a
            # message whose session is not the one it is paired on, so a
            # single shared copy would be discarded by all but at most one.
            for viewer in room.paired_viewers:
                await send(
                    viewer.socket,
                    {
                        "type": "state",
                        "session": viewer.session,
                        "recording": recording,
                        "elapsedSeconds": elapsed,
                        "tags": tags,
                        "deviceBatteries": batteries,
                    },
                )

        async def relay_gaze(msg: dict[str, Any]) -> None:
            """The gaze reticle - the red dot the phone draws - in scene-video
            coordinates.

            (0,0) is the top-left of the scene frame and (1,1) the
            bottom-right: the normalisation both Tobii and Neon report and the
            phone's own overlay draws with, so the browser can place the dot
            over its <video> element without knowing anything about the
            device. Values outside [0,1] are legitimate (gaze off the edge of
            the scene camera) and are passed through for the viewer to clamp
            or hide; only a non-number is dropped.

            Deliberately not a data channel. A data channel would have to be
            negotiated into the media SDP - the one part of this that is
            working and tuned - and a gaze point is ~60 bytes at 25 Hz, which
            costs this socket nothing.
            """
            x = msg.get("x")
            y = msg.get("y")
            if not _is_finite_number(x) or not _is_finite_number(y):
                return
            valid = msg.get("valid")
            for viewer in room.paired_viewers:
                await send(
                    viewer.socket,
                    {
                        "type": "gaze",
                        "session": viewer.session,
                        "x": float(x),
                        "y": float(y),
                        "valid": valid if isinstance(valid, bool) else True,
                    },
                )

        async def on_publisher_message(msg: dict[str, Any]) -> None:
            msg_type = msg.get("type")
            if msg_type == "access-response":
                viewer_id = msg.get("viewer")
                target = room.viewers.get(viewer_id) if isinstance(viewer_id, str) else None
                # Ignoring rather than erroring covers a viewer that left, was
                # already resolved, or was never asked about - a stray or
                # duplicate response should not resurrect a dead link.
                if target is None or not target.awaiting_access:
                    return
                # Only set for a link a viewer opened with its own tracked
                # `view-request` (see `start_view_request`) - an
                # access-request this server raised on its own (restart, a
                # resuming stale link) has no requestId and produces no
                # `view-*` message, same as before this layer existed.
                request_id = target.view_request_id
                capabilities = target.view_capabilities
                if target.view_request_timer:
                    target.view_request_timer.cancel()
                    target.view_request_timer = None
                target.view_request_id = None
                target.view_capabilities = []
                if msg.get("accepted") is True:
                    await pair(room, target)
                    if request_id is not None:
                        await send(
                            target.socket,
                            {
                                "type": "view-approved",
                                "requestId": request_id,
                                "session": target.session,
                                "capabilities": capabilities,
                            },
                        )
                else:
                    target.awaiting_access = False
                    await send(target.socket, {"type": "access-declined"})
                    if request_id is not None:
                        await send(target.socket, {"type": "view-declined", "requestId": request_id})
                return
            if msg_type == "restart":
                target = room.viewer_by_session(msg.get("session"))
                if target is not None:
                    await request_restart(target, "publisher")
                return
            # 'state' and 'gaze' go to every viewer at once and are the only
            # publisher messages not addressed to one link, so they are
            # handled before the session lookup below - the phone sends one
            # copy however many people are watching.
            if msg_type == "state":
                await relay_state(msg)
                return
            if msg_type == "gaze":
                await relay_gaze(msg)
                return
            if msg_type not in ("offer", "candidate"):
                return
            target = room.viewer_by_session(msg.get("session"))
            if target is None or target.socket is None:
                return
            if msg_type == "offer":
                sdp = msg.get("sdp")
                if not isinstance(sdp, str) or len(sdp) > 100000:
                    return
                if DATA_MLINE.search(sdp):
                    await send(
                        socket,
                        {"type": "error", "message": "Remote view does not accept data channels."},
                    )
                    return
                await send(
                    target.socket, {"type": "offer", "session": target.session, "sdp": sdp}
                )
                return
            await relay_candidate(msg, target.socket, target.session)

        async def on_viewer_message(msg: dict[str, Any], mine: ViewerSlot) -> None:
            msg_type = msg.get("type")
            if msg_type == "view-request":
                await start_view_request(room, mine, msg)
                return
            if msg_type == "access-request":
                # A fresh join has nothing yet, so this is the only trigger
                # for it (see signal_handler above); a resume already
                # marked `awaiting_access` itself, so a client that sends
                # this defensively on every `joined` is a harmless no-op
                # here. Already paired - a plain `resumed` session, say -
                # needs no request at all.
                if mine.session is None and not mine.awaiting_access:
                    await request_access(room, mine)
                return
            if msg_type == "restart" and mine.session and msg.get("session") == mine.session:
                await request_restart(mine, "viewer")
                return
            if not mine.session or msg.get("session") != mine.session:
                return
            if msg_type == "answer":
                sdp = msg.get("sdp")
                if not isinstance(sdp, str) or len(sdp) > 100000:
                    return
                await send(
                    room.publisher, {"type": "answer", "session": mine.session, "sdp": sdp}
                )
            elif msg_type == "candidate":
                await relay_candidate(msg, room.publisher, mine.session)
            elif msg_type in ("tag-request", "stop-request"):
                # The web "Tag" and "Stop" buttons asking the phone to do what
                # its own controls do. Any viewer may ask, and the session
                # tells the phone which one did.
                #
                # Relayed, never acted on: this server does not know whether a
                # recording is even running, so a stop for one that already
                # ended has to be the phone's no-op rather than an error here.
                await send(room.publisher, {"type": msg_type, "session": mine.session})

        async def on_message(raw: str | bytes) -> None:
            try:
                msg = json.loads(raw)
            except (ValueError, TypeError):
                await send(socket, {"type": "error", "message": "Invalid JSON"})
                return
            if not isinstance(msg, dict):
                return
            # A socket already superseded by a reconnect stays readable until
            # its close event lands; anything it says now belongs to a
            # connection the room no longer recognises.
            if role == "publisher":
                if room.publisher is socket:
                    await on_publisher_message(msg)
            elif slot is not None and slot.socket is socket:
                await on_viewer_message(msg, slot)

        async def release_room_if_empty() -> None:
            """Drop the room once nothing is connected and nothing is
            resumable.

            A carrier/DNS outage can take every signaling socket down at the
            same moment while the peer connections continue carrying media, so
            a room still holding a resume token or a live session is retained
            for [resume_ttl_ms] rather than deleted the instant its last
            socket disappears.
            """
            if room.publisher is not None or room.connected_viewers:
                return
            resumable = room.publisher_token is not None or any(
                s.session for s in room.viewers.values()
            )
            if not resumable:
                discard_room()
                return
            if room.cleanup_timer:
                return

            async def cleanup() -> None:
                room.cleanup_timer = None
                if (
                    rooms.get(name) is room
                    and room.publisher is None
                    and not room.connected_viewers
                ):
                    discard_room()

            room.cleanup_timer = later(resume_ttl_ms, cleanup)

        def discard_room() -> None:
            if rooms.get(name) is not room:
                return
            for viewer in room.viewers.values():
                viewer.cancel_timers()
            del rooms[name]

        async def on_close(code: int) -> None:
            tail = (
                " - session invalidated"
                if code == 1000
                else " - keeping session for a possible resume"
            )
            if role == "publisher":
                if room.publisher is not socket:
                    return
                print(f"[{name}] publisher closed (code {code}){tail}", flush=True)
                room.publisher = None
                if code == 1000:
                    # A deliberate disconnect (recording stopped): every
                    # viewer's link really is gone, and no resume token is
                    # valid any more.
                    room.publisher_token = None
                    for viewer in room.viewers.values():
                        if viewer.restart_timer:
                            viewer.restart_timer.cancel()
                            viewer.restart_timer = None
                        viewer.session = None
                        viewer.restart_pending = False
                        viewer.paired_at = None
                        viewer.awaiting_access = False
                        await send(viewer.socket, {"type": "peer-left", "role": "publisher"})
            else:
                mine = slot
                if mine is None or mine.socket is not socket:
                    return
                print(f"[{name}] viewer closed (code {code}){tail}", flush=True)
                mine.socket = None
                if code == 1000:
                    mine.cancel_timers()
                    departed = mine.session
                    mine.session = None
                    mine.restart_pending = False
                    mine.paired_at = None
                    # Freeing the slot is what lets someone else take the
                    # place of a viewer who left; a slot held for a resume
                    # that is never coming would keep the room full instead.
                    room.viewers.pop(mine.id, None)
                    if departed:
                        # Named by session so the publisher closes that one
                        # PeerConnection and leaves the other viewers' alone.
                        await send(
                            room.publisher,
                            {"type": "peer-left", "role": "viewer", "session": departed},
                        )
                    await broadcast_viewers(room)
                elif mine.expiry_timer is None:
                    # An abrupt drop keeps the seat so the same viewer
                    # can resume onto its live media connection - but
                    # only for as long as a resume is plausible. A
                    # browser that was killed rather than closed never
                    # sends a 1000, and without this its slot would hold
                    # one of the room's five places for the rest of the
                    # session while the publisher kept encoding for
                    # nobody.
                    async def expire(mine: ViewerSlot = mine) -> None:
                        mine.expiry_timer = None
                        if rooms.get(name) is not room or mine.socket is not None:
                            return
                        if mine.restart_timer:
                            mine.restart_timer.cancel()
                            mine.restart_timer = None
                        expired = mine.session
                        mine.session = None
                        mine.paired_at = None
                        mine.restart_pending = False
                        room.viewers.pop(mine.id, None)
                        print(
                            f"[{name}] viewer seat released - no resume within "
                            f"{resume_ttl_ms}ms",
                            flush=True,
                        )
                        if expired:
                            await send(
                                room.publisher,
                                {"type": "peer-left", "role": "viewer", "session": expired},
                            )
                        await broadcast_viewers(room)
                        await release_room_if_empty()

                    mine.expiry_timer = later(resume_ttl_ms, expire)
            # Any other close code is an abrupt/network-level drop - keep the
            # session and resume token alive so a reconnecting client can resume
            # without forcing the peer to renegotiate its video connection.
            await release_room_if_empty()

        try:
            async for message in socket:
                if message.type in (WSMsgType.TEXT, WSMsgType.BINARY):
                    await on_message(message.data)
                elif message.type == WSMsgType.ERROR:
                    break
        except Exception:  # noqa: BLE001 - socket.on('error', () => {})
            pass
        finally:
            sockets.discard(socket)
            await on_close(socket.close_code if socket.close_code is not None else 1006)
        return socket

    async def on_shutdown(_app: web.Application) -> None:
        for room in list(rooms.values()):
            if room.cleanup_timer:
                room.cleanup_timer.cancel()
            for slot in room.viewers.values():
                slot.cancel_timers()
        for task in list(timer_tasks):
            task.cancel()
        for socket in list(sockets):
            try:
                await socket.close(code=1001, message=b"Server shutting down")
            except Exception:  # noqa: BLE001
                pass

    app = web.Application()
    # The room pattern is checked inside the handler so a malformed room name
    # gets the same 400 the Node upgrade handler returned, not a 404.
    app.router.add_route("*", "/signal{tail:.*}", signal_handler)
    app.router.add_route("*", "/{tail:.*}", http_handler)
    app.on_shutdown.append(on_shutdown)
    return SignalingServer(app, rooms, env)


def load_dotenv(path: str | os.PathLike[str] = ".env") -> None:
    """Load KEY=VALUE lines into os.environ without overriding what is already
    set. The Node launcher did this with `node --env-file-if-exists=.env`;
    Python has no such flag, so the equivalent lives here and is called only
    from main()."""
    file = Path(path)
    if not file.is_file():
        return
    for line in file.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent / ".env")
    port = int(os.environ.get("PORT") or 8787)
    host = os.environ.get("HOST") or "127.0.0.1"
    server = create_server()
    provider = turn_provider()
    max_viewers = int(os.environ.get("MAX_VIEWERS") or DEFAULT_MAX_VIEWERS)
    print(f"Neurora remote view: http://localhost:{port}/ (protocol 4)", flush=True)
    print(f"Viewers per session: up to {max_viewers}", flush=True)
    print(f"TURN provider: {provider['name']}", flush=True)
    if not provider["dedicated"]:
        # Deliberately loud. Publisher and viewer are on different networks in
        # every real session, so TURN is load-bearing rather than a fallback,
        # and the shared credentials below are the most common reason a
        # deployment that "works" delivers an unwatchable picture. See
        # turn_provider() and README.md.
        print(
            "WARNING: no dedicated TURN configured - using the shared public Open Relay "
            "credentials.\n"
            "         Expect refused allocations and heavily throttled video whenever the "
            "two peers\n"
            "         are not on the same network. Set CF_TURN_KEY_ID + CF_TURN_API_TOKEN, "
            "the XIRSYS_*\n"
            "         variables, or TURN_URLS before using this for anything real.",
            file=sys.stderr,
            flush=True,
        )
    web.run_app(server.app, host=host, port=port, print=None)


if __name__ == "__main__":
    main()

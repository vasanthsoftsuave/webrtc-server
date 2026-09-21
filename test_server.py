"""Port of server.test.js, extended for protocol 3 (several viewers, audio,
gaze). Same fixtures and assertion style as the Node original."""

from __future__ import annotations

import asyncio
import json
import re
import time
from typing import Any

import pytest
from aiohttp import ClientSession, WSMsgType, web
from aiohttp.test_utils import TestServer

from server import create_server, ice_servers, turn_provider


class Peer:
    """The Node fixture's `connect()` return value: a socket, the messages it
    has received so far, and `next(type)` which consumes the first message of
    that type (waiting up to 2s for one to arrive)."""

    def __init__(self, session: ClientSession, url: str):
        self._session = session
        self._url = url
        self.socket: Any = None
        self.messages: list[dict[str, Any]] = []
        self._pump: asyncio.Task[None] | None = None

    async def open(self) -> "Peer":
        self.socket = await self._session.ws_connect(self._url, autoclose=False, autoping=True)
        self._pump = asyncio.create_task(self._read())
        return self

    async def _read(self) -> None:
        try:
            async for message in self.socket:
                if message.type == WSMsgType.TEXT:
                    self.messages.append(json.loads(message.data))
                elif message.type in (WSMsgType.CLOSE, WSMsgType.CLOSED, WSMsgType.ERROR):
                    break
        except Exception:  # noqa: BLE001
            pass

    async def next(self, msg_type: str) -> dict[str, Any]:
        deadline = time.monotonic() + 2.0
        while time.monotonic() < deadline:
            for i, msg in enumerate(self.messages):
                if msg.get("type") == msg_type:
                    return self.messages.pop(i)
            await asyncio.sleep(0.01)
        raise AssertionError(f"No {msg_type}; got {json.dumps(self.messages)}")

    async def send(self, msg: dict[str, Any]) -> None:
        await self.socket.send_str(json.dumps(msg))

    async def close(self, code: int = 1000, message: bytes = b"") -> None:
        await self.socket.close(code=code, message=message)

    async def dispose(self) -> None:
        if self._pump:
            self._pump.cancel()
        if self.socket is not None and not self.socket.closed:
            await self.socket.close()


class Fixture:
    def __init__(self, server: Any, http: TestServer, client: ClientSession):
        self.rooms = server.rooms
        self._http = http
        self._client = client
        self.base = str(http.make_url("")).rstrip("/")
        self._peers: list[Peer] = []

    async def connect(
        self, role: str, room: str = "test-room", resume: str | None = None
    ) -> Peer:
        query = f"?role={role}"
        if resume:
            query += f"&resume={resume}"
        ws_base = self.base.replace("http://", "ws://")
        peer = Peer(self._client, f"{ws_base}/signal/{room}{query}")
        self._peers.append(peer)
        return await peer.open()

    async def paired(self) -> tuple[Peer, Peer, str]:
        publisher = await self.connect("publisher")
        viewer = await self.connect("viewer")
        a, b = await asyncio.gather(publisher.next("ready"), viewer.next("ready"))
        assert a["session"] == b["session"]
        return publisher, viewer, a["session"]

    async def join_viewer(self, publisher: Peer, room: str = "test-room") -> tuple[Peer, str]:
        """One more viewer into a room that already has a publisher, returning
        it with the session naming its own link."""
        viewer = await self.connect("viewer", room)
        mine, theirs = await asyncio.gather(viewer.next("ready"), publisher.next("ready"))
        assert mine["session"] == theirs["session"]
        return viewer, mine["session"]

    def slot(self, session: str, room: str = "test-room") -> Any:
        return self.rooms[room].viewer_by_session(session)

    async def get(self, path: str) -> Any:
        return await self._client.get(self.base + path)

    async def teardown(self) -> None:
        for peer in self._peers:
            await peer.dispose()
        await self._client.close()
        await self._http.close()


# restart_debounce_ms is effectively disabled for most tests: they assert the
# *protocol* around restart, and the debounce only decides how long a
# duplicate is coalesced for. It gets its own test below.
async def fixture(**options: Any) -> Fixture:
    options.setdefault("env", {})
    options.setdefault("restart_debounce_ms", 0)
    server = create_server(**options)
    http = TestServer(server.app)
    await http.start_server()
    return Fixture(server, http, ClientSession())


@pytest.fixture
async def app():
    created: list[Fixture] = []

    async def make(**options: Any) -> Fixture:
        f = await fixture(**options)
        created.append(f)
        return f

    yield make
    for f in created:
        await f.teardown()


VIDEO_SDP = "v=0\r\nm=video 9 UDP/TLS/RTP/SAVPF 96\r\na=sendonly\r\n"
AUDIO_SDP = VIDEO_SDP + "m=audio 9 UDP/TLS/RTP/SAVPF 111\r\na=sendonly\r\n"


async def test_serves_viewer_health_and_shared_relay_settings(app):
    f = await app()
    assert re.search("Neurora", await (await f.get("/")).text())
    health = await (await f.get("/health")).json()
    assert health["media"] == "video+audio"
    assert health["maxViewers"] == 5
    config = await (await f.get("/ice-servers")).json()
    assert any("turn:" in json.dumps(s["urls"]) for s in config["iceServers"])
    assert config["iceTransportPolicy"] == "all"
    assert (await f.get("/../server.py")).status == 404


async def test_server_can_require_turn_relay_for_both_clients(app):
    f = await app(env={"FORCE_RELAY": "true"})
    response = await f.get("/ice-servers")
    assert (await response.json())["iceTransportPolicy"] == "relay"


async def test_late_viewer_gets_new_negotiation_and_routed_offer_answer_early_ice(app):
    f = await app()
    p = await f.connect("publisher")
    await p.next("joined")
    assert not any(m["type"] == "ready" for m in p.messages)
    v = await f.connect("viewer")
    ready = await v.next("ready")
    await p.next("ready")
    await p.send(
        {
            "type": "candidate",
            "session": ready["session"],
            "candidate": "candidate:test",
            "sdpMid": "0",
            "sdpMLineIndex": 0,
        }
    )
    assert (await v.next("candidate"))["candidate"] == "candidate:test"
    await p.send({"type": "offer", "session": ready["session"], "sdp": VIDEO_SDP})
    assert (await v.next("offer"))["sdp"] == VIDEO_SDP
    await v.send({"type": "answer", "session": ready["session"], "sdp": VIDEO_SDP})
    assert (await p.next("answer"))["sdp"] == VIDEO_SDP


async def test_viewer_reload_invalidates_old_session_and_does_not_replay_sdp_history(app):
    f = await app()
    p, v, session = await f.paired()
    await p.send({"type": "offer", "session": session, "sdp": VIDEO_SDP})
    await v.next("offer")
    await v.close(1000, b"leave")
    # Named by session, so a publisher holding several peer connections knows
    # which one to close.
    assert (await p.next("peer-left"))["session"] == session
    fresh = await f.connect("viewer")
    nxt = await fresh.next("ready")
    await p.next("ready")
    assert nxt["session"] != session
    await p.send(
        {"type": "candidate", "session": session, "candidate": "stale", "sdpMLineIndex": 0}
    )
    await p.send({"type": "offer", "session": nxt["session"], "sdp": VIDEO_SDP})
    await fresh.next("offer")
    assert not any(m["type"] in ("candidate", "offer") for m in fresh.messages)


async def test_restart_assigns_a_fresh_session_to_both_peers(app):
    f = await app()
    p, v, session = await f.paired()
    await v.send({"type": "restart", "session": session})
    a, b = await asyncio.gather(p.next("ready"), v.next("ready"))
    assert a["session"] == b["session"]
    assert a["session"] != session


async def test_both_peers_restarting_at_once_are_coalesced_into_a_single_repair(app):
    f = await app(restart_debounce_ms=150)
    p, v, session = await f.paired()
    # Publisher and viewer both notice the same stall and both ask for a
    # restart. Exactly one new session must come out of that: a second
    # re-pair would throw away the connection the first one just built.
    await p.send({"type": "restart", "session": session})
    await v.send({"type": "restart", "session": session})
    a, b = await asyncio.gather(p.next("ready"), v.next("ready"))
    assert a["session"] == b["session"]
    assert a["session"] != session
    await asyncio.sleep(0.3)
    assert not any(m["type"] == "ready" for m in p.messages), "publisher was re-paired twice"
    assert not any(m["type"] == "ready" for m in v.messages), "viewer was re-paired twice"
    assert f.slot(a["session"]) is not None


async def test_both_peers_can_resume_the_same_media_session_after_an_abrupt_outage(app):
    f = await app()
    p, v, session = await f.paired()
    publisher_token = (await p.next("joined"))["resumeToken"]
    viewer_token = (await v.next("joined"))["resumeToken"]
    # 1001, not 1000: the Node test used socket.terminate() to produce a 1006.
    # Both land in the server's "any other close code is an abrupt drop" branch,
    # which is the behaviour under test; aiohttp has no public terminate().
    await p.close(1001)
    await v.close(1001)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        room = f.rooms.get("test-room")
        if room and not room.publisher and not room.connected_viewers:
            break
        await asyncio.sleep(0.01)
    assert "test-room" in f.rooms
    resumed_viewer = await f.connect("viewer", "test-room", viewer_token)
    resumed_publisher = await f.connect("publisher", "test-room", publisher_token)
    # The publisher gets the whole set back, not one session: with several
    # viewers there is no single one to name.
    a = await resumed_publisher.next("resumed")
    b = await resumed_viewer.next("resumed")
    assert a["sessions"] == [session]
    assert b["session"] == session


async def test_a_restart_while_the_other_peer_is_offline_runs_after_it_resumes(app):
    f = await app()
    p, v, session = await f.paired()
    viewer_token = (await v.next("joined"))["resumeToken"]
    await v.close(1001)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        slot = f.slot(session)
        if slot is None or slot.socket is None:
            break
        await asyncio.sleep(0.01)
    await p.send({"type": "restart", "session": session})
    while time.monotonic() < deadline and not f.slot(session).restart_pending:
        await asyncio.sleep(0.01)
    assert f.slot(session).restart_pending is True
    resumed_viewer = await f.connect("viewer", "test-room", viewer_token)
    a, b = await asyncio.gather(p.next("ready"), resumed_viewer.next("ready"))
    assert a["session"] == b["session"]
    assert a["session"] != session


async def test_a_second_publisher_is_refused_without_disturbing_existing_peers(app):
    f = await app()
    p, v, session = await f.paired()
    extra = await f.connect("publisher")
    assert (await extra.next("error"))["code"] == "room-busy"
    await p.send({"type": "offer", "session": session, "sdp": VIDEO_SDP})
    assert (await v.next("offer"))["session"] == session


async def test_several_viewers_each_get_their_own_session_and_private_signaling(app):
    f = await app()
    p, first, first_session = await f.paired()
    second, second_session = await f.join_viewer(p)
    assert first_session != second_session
    # An offer addressed to one link must not reach the other: with a single
    # room-wide session id every viewer would have applied every offer.
    await p.send({"type": "offer", "session": second_session, "sdp": AUDIO_SDP})
    assert (await second.next("offer"))["sdp"] == AUDIO_SDP
    await asyncio.sleep(0.05)
    assert not any(m["type"] == "offer" for m in first.messages)
    # And an answer comes back stamped with the session it belongs to, which
    # is how the publisher knows which PeerConnection to feed it to.
    await second.send({"type": "answer", "session": second_session, "sdp": AUDIO_SDP})
    assert (await p.next("answer"))["session"] == second_session


async def test_room_fills_up_and_frees_the_seat_a_viewer_gives_back(app):
    f = await app(max_viewers=2)
    p, first, _first_session = await f.paired()
    second, second_session = await f.join_viewer(p)
    third = await f.connect("viewer")
    error = await third.next("error")
    assert error["code"] == "room-full"
    # The refusal must not touch the two who were already watching.
    assert not any(m["type"] == "peer-left" for m in first.messages)
    await second.close(1000, b"leave")
    assert (await p.next("peer-left"))["session"] == second_session
    replacement, _ = await f.join_viewer(p)
    assert replacement is not None


async def test_viewer_count_is_broadcast_as_people_join_and_leave(app):
    f = await app(max_viewers=5)
    p, first, _ = await f.paired()
    # The publisher is told first, while the room is still empty, then again
    # on each arrival - so its queue reads 0, 1, 2 rather than jumping.
    assert (await p.next("viewers"))["count"] == 0
    count = await first.next("viewers")
    assert (count["count"], count["max"]) == (1, 5)
    assert (await p.next("viewers"))["count"] == 1
    second, _ = await f.join_viewer(p)
    assert (await first.next("viewers"))["count"] == 2
    assert (await p.next("viewers"))["count"] == 2
    await second.close(1000, b"leave")
    assert (await first.next("viewers"))["count"] == 1


async def test_accepts_audio_offers_and_still_rejects_data_channels(app):
    f = await app()
    p, v, session = await f.paired()
    # Audio is the phone's microphone and is expected now.
    await p.send({"type": "offer", "session": session, "sdp": AUDIO_SDP})
    assert (await v.next("offer"))["sdp"] == AUDIO_SDP
    # A data channel is not: nothing on either side reads one.
    await p.send(
        {
            "type": "offer",
            "session": session,
            "sdp": VIDEO_SDP + "m=application 9 UDP/DTLS/SCTP webrtc-datachannel\r\n",
        }
    )
    assert re.search("data channel", (await p.next("error"))["message"])


async def test_recording_state_reaches_every_viewer_stamped_with_its_own_session(app):
    f = await app()
    p, first, first_session = await f.paired()
    second, second_session = await f.join_viewer(p)
    # One message from the phone, however many people are watching.
    await p.send(
        {
            "type": "state",
            "recording": True,
            "elapsedSeconds": 60,
            "tags": [{"id": 1, "label": "", "tagSeconds": 12}],
        }
    )
    for viewer, session in ((first, first_session), (second, second_session)):
        state = await viewer.next("state")
        assert state["session"] == session
        assert state["recording"] is True
        assert state["elapsedSeconds"] == 60
        assert state["tags"] == [{"id": 1, "label": "", "tagSeconds": 12}]
    assert not any(m["type"] == "state" for m in p.messages), "state echoed back to the publisher"


async def test_state_rejects_malformed_payloads_instead_of_relaying_them(app):
    f = await app()
    p, v, session = await f.paired()
    await p.send(
        {"type": "state", "session": session, "recording": "yes", "elapsedSeconds": 1, "tags": []}
    )
    await p.send(
        {"type": "state", "session": session, "recording": True, "elapsedSeconds": -1, "tags": []}
    )
    await p.send(
        {
            "type": "state",
            "session": session,
            "recording": True,
            "elapsedSeconds": 1,
            "tags": [{"id": "x", "label": "", "tagSeconds": 1}],
        }
    )
    await p.send(
        {
            "type": "state",
            "session": session,
            "recording": True,
            "elapsedSeconds": 1,
            "tags": [{"id": 1, "label": "", "tagSeconds": 1}],
        }
    )
    state = await v.next("state")
    assert state["tags"] == [{"id": 1, "label": "", "tagSeconds": 1}]


async def test_gaze_reaches_every_viewer_and_non_numbers_are_dropped(app):
    f = await app()
    p, first, first_session = await f.paired()
    second, second_session = await f.join_viewer(p)
    await p.send({"type": "gaze", "x": "left", "y": 0.5})
    await p.send({"type": "gaze", "x": 0.25, "y": float("nan")})
    await p.send({"type": "gaze", "x": 0.25, "y": 0.75, "valid": True})
    for viewer, session in ((first, first_session), (second, second_session)):
        gaze = await viewer.next("gaze")
        assert (gaze["x"], gaze["y"], gaze["valid"]) == (0.25, 0.75, True)
        assert gaze["session"] == session
        assert not any(m["type"] == "gaze" for m in viewer.messages), "a malformed gaze was relayed"
    assert not any(m["type"] == "gaze" for m in p.messages), "gaze echoed back to the publisher"


async def test_gaze_outside_the_frame_is_passed_through_for_the_viewer_to_hide(app):
    f = await app()
    p, v, _ = await f.paired()
    # Gaze legitimately leaves the scene camera's field of view; clamping or
    # dropping it here would make the dot stick to the edge instead.
    await p.send({"type": "gaze", "x": -0.2, "y": 1.4, "valid": False})
    gaze = await v.next("gaze")
    assert (gaze["x"], gaze["y"], gaze["valid"]) == (-0.2, 1.4, False)


async def test_tag_request_relays_viewer_taps_to_the_publisher_only(app):
    f = await app()
    p, v, session = await f.paired()
    await v.send({"type": "tag-request", "session": session})
    assert (await p.next("tag-request"))["session"] == session
    assert not any(
        m["type"] == "tag-request" for m in v.messages
    ), "tag-request echoed back to the viewer"


async def test_room_isolation_and_sender_roles_prevent_offer_reflection(app):
    f = await app()
    p, v, session = await f.paired()
    other = await f.connect("viewer", "different-room")
    await other.next("joined")
    await v.send({"type": "offer", "session": session, "sdp": VIDEO_SDP})
    await v.send({"type": "candidate", "session": session, "candidate": "valid", "sdpMLineIndex": 0})
    await p.next("candidate")
    assert not any(m["type"] == "offer" for m in p.messages)
    assert not any(m["type"] == "ready" for m in other.messages)


async def test_a_viewer_cannot_restart_or_tag_on_another_viewers_session(app):
    f = await app()
    p, first, first_session = await f.paired()
    second, _second_session = await f.join_viewer(p)
    await second.send({"type": "restart", "session": first_session})
    await second.send({"type": "tag-request", "session": first_session})
    await asyncio.sleep(0.1)
    assert not any(m["type"] == "ready" for m in first.messages), "another viewer forced a re-pair"
    assert not any(m["type"] == "tag-request" for m in p.messages)


async def test_an_abruptly_dropped_viewer_keeps_its_seat_then_gives_it_back(app):
    f = await app(max_viewers=1, resume_ttl_ms=150)
    p, v, session = await f.paired()
    await v.close(1001)
    # The seat is held at first: the same viewer may be reconnecting onto a
    # video connection that never stopped working.
    refused = await f.connect("viewer")
    assert (await refused.next("error"))["code"] == "room-full"
    # ...but not indefinitely. A browser killed rather than closed never
    # sends a 1000, and its slot would otherwise hold one of the room's
    # places for the rest of the session.
    assert (await p.next("peer-left"))["session"] == session
    _replacement, replacement_session = await f.join_viewer(p)
    assert replacement_session != session


async def test_a_publisher_resume_keeps_a_viewer_that_is_itself_reconnecting(app):
    f = await app()
    p, v, session = await f.paired()
    publisher_token = (await p.next("joined"))["resumeToken"]
    viewer_token = (await v.next("joined"))["resumeToken"]
    await v.close(1001)
    await p.close(1001)
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        room = f.rooms.get("test-room")
        if room and room.publisher is None and not room.connected_viewers:
            break
        await asyncio.sleep(0.01)
    resumed_publisher = await f.connect("publisher", "test-room", publisher_token)
    # Signaling and media are separate transports: the viewer's video
    # connection outlived its WebSocket, so the publisher is told to keep
    # that peer rather than rebuild it the moment its own socket returns.
    assert (await resumed_publisher.next("resumed"))["sessions"] == [session]
    resumed_viewer = await f.connect("viewer", "test-room", viewer_token)
    assert (await resumed_viewer.next("resumed"))["session"] == session


async def test_turn_config_supports_expiring_shared_secret_credentials_and_fails_closed():
    config = await ice_servers(
        {"TURN_URLS": "turn:example.test:443?transport=tcp", "TURN_SHARED_SECRET": "test-only"}
    )
    assert re.match(r"^\d+:neurora$", config[1]["username"])
    assert config[1]["credential"]
    with pytest.raises(RuntimeError):
        await ice_servers({"TURN_URLS": "turn:example.test:443"})


async def test_the_shared_public_turn_fallback_is_reported_as_not_dedicated():
    assert turn_provider({}) == {"name": "open-relay-shared", "dedicated": False}
    assert turn_provider({"TURN_URLS": "turn:example.test:3478"})["dedicated"] is True
    # UDP relay candidates must be offered before the TCP/TLS ones: a TCP
    # relay connects and then delivers unwatchable video, so it is the last
    # resort, not the first thing ICE finds.
    servers = await ice_servers({})
    urls = next(s["urls"] for s in servers if "turn:" in str(s["urls"]))
    first_tcp = next(i for i, u in enumerate(urls) if "transport=tcp" in u)
    assert all(
        "transport=tcp" not in u and u.startswith("turn:") for u in urls[:first_tcp]
    )
    assert first_tcp > 0, "no UDP relay offered ahead of the TCP ones"

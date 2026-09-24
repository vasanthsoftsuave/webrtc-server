# Neurora remote view (Python)

This service hosts the browser viewer, relays WebRTC signaling, and gives both
clients the same STUN/TURN configuration. Deploy it behind a stable HTTPS/WSS
hostname; a temporary tunnel hostname is not suitable for recording sessions.

A port of the Node.js implementation onto Python + aiohttp, since extended to
protocol 4. Message validation, resume behaviour and TURN configuration are
unchanged from the Node server; the room model, the accepted media and the set
of relayed messages are not — see **Protocol 4** below.

## Protocol 4

| | Protocol 2 | Protocol 3 | Protocol 4 |
| --- | --- | --- | --- |
| Viewers per room | one | up to `MAX_VIEWERS` (5) | up to `MAX_VIEWERS` (5) |
| `session` | one per room | one per publisher↔viewer link | one per publisher↔viewer link |
| Media | video only | video **and** audio (the glasses' microphone) | video **and** audio |
| Relayed messages | `offer` `answer` `candidate` `state` `tag-request` | … plus `gaze` and `viewers` | … plus `access-request` and `access-response` |
| Pairing | automatic | automatic | gated on the publisher's approval |

**Access control.** No viewer is paired without the publisher explicitly
saying so:

1. A viewer connects to `/signal/<room>?role=viewer`; the server validates the
   room name and, if a resume token is presented, the pairing information it
   names.
2. The viewer sends `{"type": "access-request"}`.
3. The server relays it to the publisher as
   `{"type": "access-request", "viewer": "<id>"}`.
4. The Neurora user (the publisher) accepts or declines from that prompt.
5. Only on `{"type": "access-response", "viewer": "<id>", "accepted": true}`
   does the server mint a session and send `ready`, opening WebRTC
   negotiation. `accepted: false` sends the viewer `access-declined` instead;
   its WebSocket is left exactly as it was, so it can send another
   `access-request` later.

This gate applies to every new pairing, not only a first join — a viewer's own
`restart`, a publisher-initiated one, and a stale link being picked back up
after the publisher itself resumes all go through the same request/response
round trip before a new session is minted. It does **not** apply to a plain
resume of a session whose media connection never actually died (the server
answers `resumed`, not `ready`): nothing new is being granted there, only a
signaling socket reconnecting to a link the publisher never revoked. A
declined or still-pending request does not hold a viewer's seat open in any
special way; the server's normal room-full/seat-release rules apply exactly as
they would to any other connected, unpaired viewer.

**Sessions are links, not rooms.** Each viewer is paired with its own
`session` id, and every `offer`, `answer`, `candidate` and `restart` names
exactly one of them. That is what lets one viewer renegotiate, stall, resume
or leave without touching the others; with the old room-wide id, any of those
invalidated every viewer's in-flight signaling at once.

**The server still relays nothing but JSON.** It is not an SFU, so five
viewers means five PeerConnections out of the phone — five encoders and five
copies of the stream on its uplink. The ceiling that matters in practice is
the handset's concurrent H.264 encoder instances (commonly 2-4 on mid-range
hardware), not this server, which is why `MAX_PEERS` in `RemoteViewPublisher`
caps it independently. If sessions routinely need more viewers than the phone
can encode for, the fix is an SFU between them, not a larger number here.

**Audio** is the glasses' own microphone — what the participant is hearing,
not the room around the phone. The Android side takes it from the same
decoded stream the recording already produces and adds it as a second
m-line on the bundled transport; `m=application` is still refused, because
neither client reads a data channel.

The phone's own microphone is never opened and the app holds no
`RECORD_AUDIO` permission: the glasses' PCM is injected through the WebRTC
audio device module with its recorder disabled. See `GlassesAudioSource` on
the Android side for the mechanism.

**Gaze** — the same reticle the phone draws on its own preview — is relayed
over this WebSocket rather than a data channel, as scene-normalised
coordinates ((0,0) top-left of the scene frame, (1,1) bottom-right) throttled
to 25 Hz by the publisher. A data channel would have to be negotiated into the
media SDP, which is the part of this that is working and tuned, to carry ~60
bytes a frame.

`state`, `gaze` and `viewers` are fanned out by the server: the phone sends
one copy and each viewer receives it stamped with its own session.

A viewer whose socket drops abruptly keeps its seat for `resume_ttl_ms` so it
can resume onto a media connection that never stopped working; a browser that
is closed (a clean 1000) frees its seat immediately.

## Run and test

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements-dev.txt
.\.venv\Scripts\python.exe -m pytest
$env:HOST = "0.0.0.0"
$env:PORT = "8787"
.\.venv\Scripts\python.exe server.py
```

Or just `.\run.cmd`, which creates the virtualenv on first run, starts the
server, opens the public ngrok tunnel the phone connects through, waits for
`/health`, and loads the viewer. Press any key in that window to shut the
server and the tunnel down.

Overridable before launching:

| Variable | Default |
| --- | --- |
| `PORT` | `.env`, else `8787` |
| `MAX_VIEWERS` | `5` |
| `PYTHON` | `py -3`, else `python` on PATH |
| `NGROK_PATH` | `ngrok.exe` on PATH, else the usual install locations |
| `NGROK_DOMAIN` | `cameo-showdown-puppy.ngrok-free.dev` (the one baked into the APK) |
| `SKIP_TUNNEL=1` | unset — set it to run on localhost only |

ngrok is optional: without it the server and the browser viewer still work on
localhost, only the phone cannot reach them. The launcher asks ngrok's local
API what it actually published rather than assuming the reserved domain was
accepted — a domain belonging to another account is rejected and ngrok falls
back to a random hostname.

A tunnel hostname is fine for development. For real recording sessions deploy
behind a stable HTTPS/WSS hostname instead.

`GET /health` is the deployment health check.

`server.py` reads `.env` itself at startup (`load_dotenv` in `main()`), which
is the stand-in for the Node launcher's `node --env-file-if-exists=.env`.
Values already present in the environment win over the file.

## TURN configuration

**A dedicated TURN server is required, not optional.** The phone and the
browser are on different networks in every real session, and mobile carriers
use symmetric NAT, so a large share of sessions can only connect through a
relay. With no TURN configured the server falls back to the Open Relay
Project's shared public credentials — the same ones published in every WebRTC
tutorial. Allocations through them are frequently refused, and the ones that
succeed are throttled to a fraction of the link's real capacity. A session
relayed through it looks like a bad phone or a bad encoder; it is neither.

`GET /ice-servers` reports `turnProvider` and `turnDedicated`, the viewer
shows `Relay/UDP` vs `Relay/TCP` alongside the bitrate, and the server logs a
warning at startup while the shared fallback is in use.

Prefer a provider offering **UDP** relay. A TCP or TLS relay will connect, but
it carries live video inside a reliable ordered stream: one lost segment
stalls every frame behind it, and the retransmissions read to congestion
control as spare capacity. No bitrate setting compensates for this.

Use one of these credential sources:

- `CF_TURN_KEY_ID` and `CF_TURN_API_TOKEN`
- `XIRSYS_IDENT`, `XIRSYS_SECRET`, and `XIRSYS_CHANNEL`
- `TURN_URLS` plus either `TURN_SHARED_SECRET`, or both `TURN_USERNAME` and
  `TURN_CREDENTIAL`

After a dedicated TURN service has been verified, set `FORCE_RELAY=true` to
make both Android and the browser use relay candidates only. Leave it false
while using the public development fallback.

## Point the debug Android app at the deployment

Set `REMOTE_VIEW_BASE_URL` as an environment variable or Gradle property before
building the debug APK:

```powershell
$env:REMOTE_VIEW_BASE_URL = "https://remote-view.example.com"
.\gradlew.bat :app:assembleDebug
```

The URL must use HTTPS. The app derives the corresponding WSS signaling URL.

## Runtime verification

Android logs under `RemoteVideo` report:

- the exact outgoing track and `SEND_ONLY` direction;
- captured and encoded frame counts;
- sent bytes and bitrate;
- selected direct or relay candidate types; and
- automatic recovery when media stops progressing.

The browser displays decoded resolution, frame rate, bitrate, negotiated
codec, whether audio is arriving and unmuted, interval packet loss, freeze
count, whether the route is direct or relayed (and over which transport), and
how many people are watching.

Each Android log line is prefixed with the first eight characters of the
session it belongs to, so several viewers' stats interleave readably.

## Diagnosing a poor picture

Read the Android `RemoteVideo` stats line and the browser's readout together:

| Symptom | Cause |
| --- | --- |
| `path=[... via=tcp]`, or `Relay/TCP` in the browser | No UDP relay reachable. Fix TURN; nothing else will help. |
| `limit=cpu`, encoded fps well under the rung's | The phone cannot decode and re-encode at this size. The ladder steps down on its own. |
| `limit=bandwidth`, loss above ~2% | Genuinely constrained link. The ladder steps down. |
| Resolution never rises over a long session | The ladder is not climbing — check `avail=` against `encoder ceiling` in the log. |
| Quality falls as each extra viewer joins | Expected. Every viewer is another encode and another copy on the uplink; the phone is sharing one radio between them. |
| `limit=cpu` only on the third viewer onward | Past the handset's concurrent hardware encoder instances, the later peers fall back to software. Fewer viewers, or an SFU. |
| Browser says `no audio` | RECORD_AUDIO was declined on the phone — the publisher logs `RECORD_AUDIO not granted` and carries on with video. |
| Browser says `audio muted` | The browser's autoplay policy refused an audible start; the viewer shows a **Tap for sound** button over the picture. |
| No gaze dot | The tracker is reporting no confident 2D point (a blink, glasses off the head), or the **Gaze** checkbox is off. The dot also hides when gaze leaves the scene camera's field of view. |

## Differences from the Node implementation

The room model and the relayed messages have moved on (see **Protocol 3**);
everything below is a mechanical substitution that changed nothing.

| Node | Python |
| --- | --- |
| `node:http` + `ws` | `aiohttp.web` (one app serves HTTP and the WebSocket upgrade) |
| `createServer({env, resumeTtlMs, restartDebounceMs})` | `create_server(env=, resume_ttl_ms=, restart_debounce_ms=)` |
| `iceServers()`, `turnProvider()` | `ice_servers()`, `turn_provider()` |
| `setTimeout` / `clearTimeout` | `loop.call_later` / `TimerHandle.cancel` |
| 30s `ping`/`pong` sweep over `wss.clients` | per-socket `WebSocketResponse(heartbeat=30)` |
| `crypto.randomUUID()` | `uuid.uuid4()` |
| `node --env-file-if-exists=.env` | `load_dotenv()` in `main()` |
| `node --test` (`server.test.js`) | `pytest` (`test_server.py`), 29 tests |

Two details worth knowing:

- `Number.isInteger` / `Number.isFinite` become `_is_int` / `_is_finite_number`,
  which explicitly reject `bool`. In Python `bool` is a subclass of `int`, so
  without that guard a JSON `true` would pass as `elapsedSeconds` or `id`.
- The restart debounce measures with `time.monotonic()` rather than
  `Date.now()`. It is an elapsed-interval measurement, and a monotonic clock
  cannot be dragged backwards by an NTP step mid-session.
#   w e b r t c - s e r v e r  
 
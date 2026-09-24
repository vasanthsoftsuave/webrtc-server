'use strict';
const video = document.querySelector('#video');
const roomInput = document.querySelector('#room');
const statusEl = document.querySelector('#status');
const quality = document.querySelector('#quality');
const empty = document.querySelector('#empty');
// Named screenEl, not screen: `screen` is window.screen, and assigning to a
// property of it here would throw rather than resize anything.
const screenEl = document.querySelector('.screen');
const join = document.querySelector('#join');
const leave = document.querySelector('#leave');
const tagbar = document.querySelector('#tagbar');
const clockEl = document.querySelector('#clock');
const tagBtn = document.querySelector('#tag');
const stopBtn = document.querySelector('#stop');
const tagCountEl = document.querySelector('#tagCount');
const tagListEl = document.querySelector('#tagList');
const gazeEl = document.querySelector('#gaze');
const gazeToggle = document.querySelector('#gazeToggle');
const unmuteBtn = document.querySelector('#unmute');
const viewersEl = document.querySelector('#viewers');
const sound = document.querySelector('#sound');
const liveBadge = document.querySelector('#live');
const muteBtn = document.querySelector('#mute');
let socket, peer, session, pending = [], iceServers = [], iceTransportPolicy = 'all', active = false, retryTimer, retryCount = 0, statsTimer, disconnectTimer, processing = Promise.resolve(), resumeToken = null;
let statsStartedAt = 0, lastInboundAt = 0, lastInboundBytes = 0, lastInboundFrames = 0, mediaStarted = false, restartRequested = false;
let turnProvider = '?', turnDedicated = true, lastLost = 0, lastReceived = 0;
// Video and audio arrive on the same peer connection but are rendered by
// two SEPARATE elements, which is the whole reason sound now starts
// immediately.
//
// A media element does not begin playback until it has media to play, and a
// live video track that has not yet produced a frame does not count. The
// phone deliberately waits ~8s after the stream comes up before it starts
// decoding scene frames at all (HARDWARE_SETTLE_MS in RemoteViewPublisher),
// so a <video> carrying both tracks sat at readyState 0 for that whole
// window — and the audio riding in it was inaudible the entire time, even
// though the packets had been arriving since the moment ICE connected. That
// is the delay this split removes: the <audio> element has nothing to wait
// for.
//
// The cost is that the browser no longer A/V-syncs the two. That costs
// nothing real here: the audio and the video come off the glasses through
// separate decoders and separate clocks, so they were never a lip-synced
// pair to begin with, and hearing the session as it happens is the point.
let videoStream = null, audioStream = null;
const status = text => { statusEl.textContent = text; };
// Must stay above the publisher's own DISCONNECT_GRACE_MS + ICE_RESTART_TIMEOUT_MS
// (RemoteViewPublisher.kt), so the publisher's in-place ICE restart is always
// given its chance before this side escalates to a full re-pair.
const VIEWER_RECOVERY_BACKSTOP_MS = 25000;

// ------------------------------------------------------------------- gaze
// The reticle the phone draws on its own preview, relayed over signaling as
// scene-normalised coordinates: (0,0) top-left of the scene frame, (1,1)
// bottom-right. Nothing here knows or needs to know which eye tracker
// produced it.
//
// Signaling and media travel different paths (the WebSocket goes through the
// server, the video peer-to-peer), so the dot can lead the picture it
// belongs to by a few tens of milliseconds. That is well inside the window
// where a gaze point is still describing the same thing on screen, and the
// alternative — a data channel — would mean renegotiating the media SDP,
// which is the part of this that is working.
let lastGaze = null, gazeStaleTimer;
// Long enough to bridge the gap between samples at the publisher's send rate
// (~25 Hz) plus a slow network moment; short enough that the dot disappears
// rather than sitting frozen somewhere the eye no longer is.
const GAZE_STALE_MS = 700;
function placeGaze() {
  if (!lastGaze || !gazeToggle.checked || !video.videoWidth || !video.videoHeight) {
    gazeEl.hidden = true;
    return;
  }
  // object-fit:contain math, done here rather than assumed: .screen takes the
  // stream's own aspect ratio once the browser reports it, but it is 16/9
  // before that and while the quality ladder is mid-change, and a dot placed
  // by naive percentages would drift off the picture in exactly those moments.
  // clientWidth/clientHeight, not getBoundingClientRect: the dot is
  // absolutely positioned against .screen's padding box, which is
  // exactly the <video> element's box (it is width:100%/height:100% of
  // it, with no padding between them). A bounding rect would include
  // .screen's 1px border and offset every dot by it.
  const boxWidth = video.clientWidth, boxHeight = video.clientHeight;
  const scale = Math.min(boxWidth / video.videoWidth, boxHeight / video.videoHeight);
  const width = video.videoWidth * scale, height = video.videoHeight * scale;
  const inside = lastGaze.valid && lastGaze.x >= 0 && lastGaze.x <= 1 && lastGaze.y >= 0 && lastGaze.y <= 1;
  gazeEl.hidden = !inside;
  if (!inside) return;
  const left = (boxWidth - width) / 2 + lastGaze.x * width;
  const top = (boxHeight - height) / 2 + lastGaze.y * height;
  gazeEl.style.transform = `translate(${left}px, ${top}px) translate(-50%, -50%)`;
}
function applyGaze(msg) {
  lastGaze = { x: msg.x, y: msg.y, valid: msg.valid !== false };
  placeGaze();
  clearTimeout(gazeStaleTimer);
  gazeStaleTimer = setTimeout(() => { lastGaze = null; gazeEl.hidden = true; }, GAZE_STALE_MS);
}
function resetGaze() {
  clearTimeout(gazeStaleTimer);
  lastGaze = null;
  gazeEl.hidden = true;
}
gazeToggle.onchange = placeGaze;
window.addEventListener('resize', placeGaze);

// -------------------------------------------------------------- tags/timing
// Mirrors what the phone's own recording screen shows (RecordingScreen.kt):
// the same mm:ss clock, the same "unnamed tag is red" rule, and per-label
// colors assigned in first-appearance order — see tagColorsFor there.
let remoteState = null; // {recording, elapsedSeconds, tags} from the last 'state' message
const tagColors = new Map(); // lowercased label -> css color
const TAG_UNNAMED_COLOR = 'var(--error)'; // TagRed in light, the dark-theme destructive red in dark — see viewer.css
const TAG_PALETTE = [
  '#FF8D28', '#FFCC00', '#00C8B3', '#3B82F6', '#7C3AED', '#EC4899',
  '#22C55E', '#06B6D4', '#A855F7', '#84CC16', '#6366F1', '#A16207',
];
function paletteColor(index) {
  if (index < TAG_PALETTE.length) return TAG_PALETTE[index];
  const step = index - TAG_PALETTE.length;
  const hue = (40 + step * 137.508) % 300 + 30;
  return `hsl(${hue.toFixed(1)}, 65%, 60%)`;
}
function colorForTag(label) {
  const key = label.trim().toLowerCase();
  if (!tagColors.has(key)) tagColors.set(key, paletteColor(tagColors.size));
  return tagColors.get(key);
}
function formatElapsed(seconds) {
  const total = Math.max(0, Math.floor(seconds));
  return `${String(Math.floor(total / 60)).padStart(2, '0')}:${String(total % 60).padStart(2, '0')}`;
}
function escapeHtml(text) {
  return text.replace(/[&<>"']/g, c => ({ '&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;' }[c]));
}
function renderTags() {
  const tags = [...remoteState.tags].sort((a, b) => a.tagSeconds - b.tagSeconds);
  tagCountEl.textContent = tags.length ? `${tags.length} Tag${tags.length === 1 ? '' : 's'}` : '';
  tagListEl.hidden = tags.length === 0;
  tagListEl.innerHTML = tags.map((tag, i) => {
    const label = tag.label.trim();
    const name = label || `Tag ${i + 1}`;
    const color = label ? colorForTag(label) : TAG_UNNAMED_COLOR;
    return `<li><span class="tag-dot" style="background:${color}"></span>` +
      `<span class="tag-name">${escapeHtml(name)}</span>` +
      `<span class="tag-time">${formatElapsed(tag.tagSeconds)}</span></li>`;
  }).join('');
}
// The phone sends elapsedSeconds — a plain count it already computed for
// its own on-screen clock — once a second while recording (plus on every
// tag and on start/stop), and this just displays it. No local ticking: an
// earlier version derived the clock from a shared wall-clock timestamp and
// assumed the phone's and the browser's clocks agreed, which is why it
// could show a very different time than the app.
function applyState(state) {
  const wasRecording = remoteState?.recording === true;
  remoteState = state;
  tagbar.hidden = false;
  tagBtn.disabled = !state.recording;
  stopBtn.disabled = !state.recording;
  if (!state.recording) disarmStop();
  clockEl.textContent = formatElapsed(state.elapsedSeconds);
  renderTags();
  // The phone sends recording:false the moment it stops, whoever asked
  // for it — its own Stop button or any viewer's. Every viewer lands
  // here and says the same thing, so nobody is left watching a frozen
  // clock wondering whether the feed broke.
  if (wasRecording && !state.recording) status('Recording stopped.');
}
function resetTagState() {
  remoteState = null;
  disarmStop();
  stopBtn.disabled = true;
  tagbar.hidden = true;
  clockEl.textContent = '00:00';
  tagListEl.hidden = true;
  tagListEl.innerHTML = '';
  tagCountEl.textContent = '';
  tagColors.clear();
}
tagBtn.onclick = () => { if (remoteState?.recording) send({ type:'tag-request' }); };

// Stopping ends the recording for the participant and for every other
// viewer, and it cannot be undone from here — so it takes two taps. The
// first arms the button and says so; the second, within
// STOP_ARMED_MS, actually sends. A modal confirm() would do the same job,
// but this keeps the decision on the control itself and cannot be
// dismissed by a stray keypress.
const STOP_ARMED_MS = 4000;
let stopArmedTimer = null;
function disarmStop() {
  clearTimeout(stopArmedTimer);
  stopArmedTimer = null;
  stopBtn.classList.remove('armed');
  stopBtn.textContent = 'Stop recording';
}
stopBtn.onclick = () => {
  if (!remoteState?.recording) return;
  if (!stopArmedTimer) {
    stopBtn.classList.add('armed');
    stopBtn.textContent = 'Tap again to stop';
    stopArmedTimer = setTimeout(disarmStop, STOP_ARMED_MS);
    return;
  }
  disarmStop();
  stopBtn.disabled = true;
  status('Stopping the recording…');
  send({ type:'stop-request' });
};

// ------------------------------------------------------------------ sound
// The video element is permanently muted (its audio lives in #sound), which
// is also what makes its autoplay unconditional — a muted element is never
// refused, so the picture can never be held up by an audio policy.
function startVideo() {
  video.muted = true;
  video.play().catch(() => status('Connected. Waiting for the browser to start the video…'));
}
// Autoplay policy: a page that has never been interacted with may not play
// audible media, and the promise from play() is the only place that failure
// is reported. Joining IS a user gesture, so the unmuted attempt below
// normally succeeds; when it doesn't (the link was opened and left, the
// browser is stricter, the tab was restored) this falls back to a muted
// element and one tap to turn sound on. The picture is unaffected either
// way — it is a different element.
function startSound() {
  sound.muted = false;
  sound.play().then(() => { unmuteBtn.hidden = true; syncMuteButton(); }).catch(() => {
    sound.muted = true;
    unmuteBtn.hidden = false;
    syncMuteButton();
    sound.play().catch(() => {});
  });
}
// With the video element's own controls gone there is nowhere else to turn
// the sound off, so the tag bar carries it. It stays hidden until an audio
// track actually arrives: a session whose glasses sent no audio has nothing
// to mute, and offering the control anyway would suggest the silence was
// this viewer's doing.
function syncMuteButton() {
  muteBtn.hidden = false;
  muteBtn.textContent = sound.muted ? 'Sound off' : 'Sound on';
  muteBtn.setAttribute('aria-pressed', String(sound.muted));
}
muteBtn.onclick = () => {
  sound.muted = !sound.muted;
  if (!sound.muted) { unmuteBtn.hidden = true; sound.play().catch(() => { sound.muted = true; syncMuteButton(); }); }
  syncMuteButton();
};
unmuteBtn.onclick = () => {
  unmuteBtn.hidden = true;
  startSound();
};

const initialRoom = new URLSearchParams(location.hash.slice(1)).get('room');
if (initialRoom && /^[A-Za-z0-9_-]{3,96}$/.test(initialRoom)) roomInput.value = initialRoom;
function send(msg) { if (socket?.readyState === WebSocket.OPEN) socket.send(JSON.stringify({ ...msg, session })); }
function closePeer() {
  clearInterval(statsTimer); clearTimeout(disconnectTimer);
  if (peer) { peer.onconnectionstatechange = null; peer.close(); }
  peer = null; pending = [];
  videoStream = null; audioStream = null;
  video.srcObject = null; sound.srcObject = null;
  empty.hidden = false; liveBadge.hidden = true; muteBtn.hidden = true;
  quality.textContent = 'Waiting for glasses video';
  screenEl.style.aspectRatio = '';
  unmuteBtn.hidden = true;
  statsStartedAt = 0; lastInboundAt = 0; lastInboundBytes = 0; lastInboundFrames = 0; mediaStarted = false; restartRequested = false;
  lastLost = 0; lastReceived = 0;
  resetGaze();
  resetTagState();
}
function stop() {
  active = false; clearTimeout(retryTimer); closePeer();
  if (socket) { socket.onclose = null; socket.close(1000); } socket = null;
  resumeToken = null;
  viewersEl.textContent = '';
  join.disabled = false; roomInput.disabled = false; leave.hidden = true; status('Disconnected.');
}
async function connect() {
  if (!active) return;
  status('Connecting to remote view…');
  try {
    const response = await fetch('/ice-servers', { cache:'no-store', headers:{'ngrok-skip-browser-warning':'true'}, signal:AbortSignal.timeout(10000) });
    if (!response.ok) throw new Error('Relay configuration unavailable');
    const iceConfig = await response.json();
    iceServers = iceConfig.iceServers;
    iceTransportPolicy = iceConfig.iceTransportPolicy === 'relay' ? 'relay' : 'all';
    turnProvider = iceConfig.turnProvider || '?';
    turnDedicated = iceConfig.turnDedicated !== false;
    if (!active) return;
    const resumeParam = resumeToken ? `&resume=${encodeURIComponent(resumeToken)}` : '';
    const ws = new WebSocket(`${location.protocol === 'https:'?'wss:':'ws:'}//${location.host}/signal/${encodeURIComponent(roomInput.value.trim())}?role=viewer${resumeParam}`);
    socket = ws;
    ws.onopen = () => { retryCount = 0; if (!peer) status('Connected. Waiting for the recording phone…'); };
    ws.onmessage = event => {
      processing = processing.then(async () => {
        if (!active || socket !== ws) return;
        await handle(JSON.parse(event.data));
      }).catch(error => { status(`Connection error: ${error.message}`); });
    };
    ws.onclose = event => {
      if (socket !== ws || !active) return;
      // A mobile network / tunnel blip can drop this socket while the
      // actual video connection (a separate transport) keeps working; don't
      // tear the video down here. A resumed reconnect leaves it untouched,
      // and only a fresh "ready" from the server rebuilds it.
      //
      // 4010 (room full) is not a blip and not this viewer's fault: someone
      // else has the last seat. Reconnecting in a loop would only spam the
      // server, so it stops and says so, same as 4009 used to for the
      // single-viewer server.
      if (event.code === 4010) { closePeer(); stop(); status('This session is already full. Ask one of the current viewers to disconnect, or try again shortly.'); return; }
      if (event.code === 4009) { closePeer(); stop(); status('That session code is already publishing from another phone.'); return; }
      if (!peer) status('Connection interrupted. Reconnecting…');
      scheduleReconnect();
    };
    ws.onerror = () => status('Signaling connection interrupted.');
  } catch (e) { if (active) { status(e.message); scheduleReconnect(); } }
}
function scheduleReconnect() {
  clearTimeout(retryTimer); status('Connection interrupted. Reconnecting…');
  retryTimer = setTimeout(connect, Math.min(1000 * 2 ** retryCount++, 10000));
}
async function handle(msg) {
  if (msg.type === 'error') { status(msg.message); return; }
  // Sent on every `joined`, resume or not: a session that is still live
  // answers with `resumed` regardless, and the server otherwise ignores a
  // request it doesn't need (already pending, or one it's about to make
  // itself for a stale/restarting link) - see access-request in README.md.
  if (msg.type === 'joined') { resumeToken = msg.resumeToken || null; send({type:'access-request'}); status('Requesting access from the recording phone…'); return; }
  if (msg.type === 'access-declined') { status('The recording phone declined this request.'); return; }
  if (msg.type === 'viewers') {
    viewersEl.textContent = msg.count ? `${msg.count} of ${msg.max} watching` : '';
    return;
  }
  if (msg.type === 'resumed') {
    session = msg.session;
    if (peer) status('Live glasses video');
    else { status('Reconnecting glasses video…'); send({type:'restart'}); }
    return;
  }
  if (msg.type === 'peer-left') { closePeer(); session = null; status('The phone stopped sharing. Waiting for it to reconnect…'); return; }
  if (msg.type === 'ready') { closePeer(); session = msg.session; status('Phone found. Connecting video…'); return; }
  if (!session || msg.session !== session) return;
  if (msg.type === 'offer') {
    // Audio is expected now (the glasses' microphone). A data channel is
    // not: nothing on this page reads one.
    if (/^m=application\s/m.test(msg.sdp)) throw new Error('This publisher offered a data channel, which remote view does not use.');
    // A second offer inside the SAME session is the publisher performing an
    // ICE restart, not a new pairing. Apply it to the peer connection that
    // is already here: the <video> element keeps its track, so the picture
    // on screen survives the network path being rebuilt underneath it.
    // Tearing down and re-creating the peer for this (as this branch used to
    // do unconditionally) is what made every transient hiccup show up as
    // seconds of black followed by a re-buffer.
    // 'stable' specifically: applying an offer while a negotiation is still
    // in flight throws InvalidStateError, and falling back to a clean
    // rebuild is better than surfacing that.
    if (peer && peer.signalingState === 'stable') {
      const existing = peer;
      clearTimeout(disconnectTimer);
      status('Re-establishing the video path…');
      await existing.setRemoteDescription({type:'offer',sdp:msg.sdp});
      for (const candidate of pending.splice(0)) await existing.addIceCandidate(candidate);
      const renegotiated = await existing.createAnswer(); await existing.setLocalDescription(renegotiated);
      send({type:'answer',sdp:renegotiated.sdp});
      // The stall clock is about this path, which is new again.
      if (peer === existing) startStats(existing);
      return;
    }
    const earlyCandidates = pending;
    closePeer();
    pending = earlyCandidates;
    const pc = new RTCPeerConnection({ iceServers, iceTransportPolicy }); peer = pc;
    const currentSession = session;
    pc.onicecandidate = event => {
      if (peer === pc && currentSession === session && event.candidate) send({type:'candidate',candidate:event.candidate.candidate,sdpMid:event.candidate.sdpMid,sdpMLineIndex:event.candidate.sdpMLineIndex});
    };
    pc.ontrack = event => {
      // Each kind goes to its own element and its own MediaStream — see the
      // note on videoStream/audioStream for why they are not one stream.
      //
      // Everything below is guarded on the track being genuinely new. An ICE
      // restart, and any other renegotiation on a live connection, fires
      // ontrack again for a track already playing; re-assigning srcObject
      // for that tore the element's pipeline down and rebuilt it, so a
      // recovery meant to be invisible flashed black and re-buffered
      // instead — and on the audio side it would have un-muted a viewer
      // who had deliberately silenced the tab.
      if (event.track.kind === 'audio') {
        if (!audioStream) audioStream = new MediaStream();
        if (audioStream.getTracks().includes(event.track)) return;
        audioStream.addTrack(event.track);
        // Assigned after the track is in: a MediaStream that is already
        // attached to an element does not reliably start rendering tracks
        // added to it afterwards.
        sound.srcObject = audioStream;
        startSound();
        return;
      }
      if (!videoStream) videoStream = new MediaStream();
      if (videoStream.getTracks().includes(event.track)) return;
      videoStream.addTrack(event.track);
      video.srcObject = videoStream;
      event.track.onmute = () => { if (peer === pc) status('Connected. Glasses video paused…'); };
      event.track.onunmute = () => { if (peer === pc) status('Receiving glasses video…'); };
      event.track.onended = () => { if (peer === pc) status('The glasses video track ended.'); };
      startVideo();
    };
    video.onplaying = () => { empty.hidden = true; liveBadge.hidden = false; status('Live scene video'); };
    // Size the player to the stream instead of a hardcoded 16:9 box. Neon's
    // scene camera is 4:3 (1600x1200), so a fixed 16:9 frame pillarboxed it
    // and rendered the picture at roughly three quarters of the width the
    // page had available — the stream was fine, the player was showing less
    // of it than it could. The screen now takes the video's own ratio as
    // soon as the browser knows it, and follows a mid-session resolution
    // change (which the quality ladder makes routine) through resize.
    const fitScreen = () => {
      if (video.videoWidth > 0 && video.videoHeight > 0) {
        screenEl.style.aspectRatio = `${video.videoWidth} / ${video.videoHeight}`;
      }
      // The reticle is positioned against the player's box, so it has to be
      // recomputed whenever that box changes — including the resolution
      // steps the quality ladder makes on its own.
      placeGaze();
    };
    video.onloadedmetadata = fitScreen;
    video.onresize = fitScreen;
    pc.onconnectionstatechange = () => {
      if (peer !== pc) return;
      if (pc.connectionState === 'connected') {
        clearTimeout(disconnectTimer);
        status('Connected. Waiting for glasses video frames…');
        startStats(pc);
      }
      if (pc.connectionState === 'failed') { status('Retrying the video connection…'); send({type:'restart'}); }
      if (pc.connectionState === 'disconnected') {
        clearTimeout(disconnectTimer); status('Video connection interrupted…');
        // The publisher owns recovery: it notices the same disconnect, and
        // its first move is an ICE restart that keeps this peer connection
        // and the picture already on screen. This deadline is only a
        // backstop for a publisher that has gone quiet, so it has to
        // outlast the publisher's own attempt — at 5s it was firing first
        // every time, forcing a full re-pair and throwing away the cheap
        // recovery that was already in flight.
        disconnectTimer = setTimeout(() => { if (peer === pc) send({type:'restart'}); }, VIEWER_RECOVERY_BACKSTOP_MS);
      }
    };
    await pc.setRemoteDescription({type:'offer',sdp:msg.sdp});
    for (const candidate of pending.splice(0)) await pc.addIceCandidate(candidate);
    const answer = await pc.createAnswer(); await pc.setLocalDescription(answer);
    send({type:'answer',sdp:answer.sdp});
  } else if (msg.type === 'candidate') {
    const candidate = {candidate:msg.candidate,sdpMid:msg.sdpMid,sdpMLineIndex:msg.sdpMLineIndex};
    if (peer?.remoteDescription) await peer.addIceCandidate(candidate); else pending.push(candidate);
  } else if (msg.type === 'state') {
    applyState(msg);
  } else if (msg.type === 'gaze') {
    applyGaze(msg);
  }
}
function startStats(pc) {
  clearInterval(statsTimer);
  statsStartedAt = lastInboundAt = performance.now();
  lastInboundBytes = 0; lastInboundFrames = 0; mediaStarted = false; restartRequested = false;
  lastLost = 0; lastReceived = 0;
  statsTimer = setInterval(async () => {
    if (peer !== pc) return;
    let stats;
    try { stats = await pc.getStats(); } catch { return; }
    if (peer !== pc) return;
    let inbound, inboundAudio, relay = false, relayProtocol = null;
    stats.forEach(s => {
      if (s.type === 'inbound-rtp' && s.kind === 'video') inbound = s;
      if (s.type === 'inbound-rtp' && s.kind === 'audio') inboundAudio = s;
      if (s.type === 'candidate-pair' && s.state === 'succeeded' && s.nominated) {
        const ends = [stats.get(s.localCandidateId), stats.get(s.remoteCandidateId)];
        relay = ends.some(c => c?.candidateType === 'relay');
        // How the relay itself is reached. A TCP or TLS relay puts live
        // video inside a reliable ordered stream: one lost segment stalls
        // every frame queued behind it, and the retransmissions look to
        // congestion control like the link is fine. It is the one route
        // whose picture no amount of bitrate improves, so it gets named
        // rather than being lumped in with "Relay".
        relayProtocol = ends.find(c => c?.relayProtocol)?.relayProtocol || null;
      }
    });
    if (!inbound) return;
    // Which codec actually got negotiated. Worth showing: the publisher asks
    // for H.264 High because it is hardware-encoded on the phone and
    // compresses this content better than VP8, but that preference is only a
    // preference — if it silently fell back, this is where you see it.
    const codec = (stats.get(inbound.codecId)?.mimeType || '').replace('video/','') || '?';
    const now = performance.now();
    const bytes = Number(inbound.bytesReceived || 0);
    const frames = Number(inbound.framesDecoded || inbound.framesReceived || 0);
    const progressed = bytes > lastInboundBytes || frames > lastInboundFrames;
    const bitrateKbps = Math.max(0, Math.round((bytes - lastInboundBytes) * 8 / Math.max(1, now - lastInboundAt)));
    if (progressed) {
      lastInboundAt = now;
      if (!mediaStarted && bytes > 0 && frames > 0) {
        mediaStarted = true;
        status('Live glasses video');
      }
    }
    // Packet loss over this interval, not the session total: a session-wide
    // figure is dominated by whatever happened during setup and stops moving
    // afterwards, which is exactly when it would be useful.
    const lost = Number(inbound.packetsLost || 0), received = Number(inbound.packetsReceived || 0);
    const deltaLost = Math.max(0, lost - lastLost), deltaTotal = Math.max(0, received - lastReceived) + deltaLost;
    const lossPct = deltaTotal > 0 ? (100 * deltaLost / deltaTotal) : 0;
    lastLost = lost; lastReceived = received;
    const route = relay ? `Relay/${(relayProtocol || '?').toUpperCase()}` : 'Direct';
    // Audio is reported only as present/absent rather than as another
    // bitrate. Opus at this bitrate is a rounding error next to the video,
    // and the question anyone actually has about it is "is there sound?".
    const audio = inboundAudio
      ? (sound.muted ? 'audio muted' : 'audio on')
      : 'no audio';
    quality.textContent = [
      `${inbound.frameWidth || 0} × ${inbound.frameHeight || 0}`,
      `${Math.round(inbound.framesPerSecond || 0)} fps`,
      `${bitrateKbps} kbps`,
      codec,
      audio,
      `${lossPct.toFixed(1)}% loss`,
      // freezeCount is the number the complaint is usually actually about:
      // a stream can report a healthy resolution and frame rate while
      // freezing several times a minute, and nothing else here shows that.
      `${Number(inbound.freezeCount || 0)} freezes`,
      `${route} connection`,
      turnDedicated ? '' : `shared TURN (${turnProvider})`,
    ].filter(Boolean).join(' · ');
    // Both of these produce a bad picture that looks like a bad encoder, so
    // say which it is rather than leaving it to be guessed at.
    if (relay && relayProtocol && relayProtocol !== 'udp') {
      status(`Live glasses video — relayed over ${relayProtocol.toUpperCase()}, which limits quality. A UDP-capable TURN server would fix this.`);
    } else if (relay && !turnDedicated && mediaStarted) {
      status('Live glasses video — running through the shared public TURN relay, which throttles throughput.');
    }
    const startGraceExpired = now - statsStartedAt >= 20000;
    // Same reasoning as VIEWER_RECOVERY_BACKSTOP_MS: the publisher sees a
    // stall on its own outbound stats first and fixes it in place. Only
    // escalate once that has had time to happen and clearly hasn't.
    const stalled = now - lastInboundAt >= VIEWER_RECOVERY_BACKSTOP_MS;
    if (!restartRequested && startGraceExpired && stalled) {
      restartRequested = true;
      status(mediaStarted ? 'Glasses video stalled. Recovering…' : 'Connected, but no glasses frames arrived. Recovering…');
      send({type:'restart'});
    }
    lastInboundBytes = bytes;
    lastInboundFrames = frames;
  },2000);
}
document.querySelector('#join-form').addEventListener('submit', event => {
  event.preventDefault(); if (active) return;
  roomInput.value = roomInput.value.trim(); if (!roomInput.reportValidity()) return;
  active = true; join.disabled = true; roomInput.disabled = true; leave.hidden = false;
  location.hash = new URLSearchParams({room:roomInput.value}).toString(); connect();
});
leave.onclick = stop;
document.querySelector('#copy').onclick = async () => {
  const room = roomInput.value.trim(); if (!/^[A-Za-z0-9_-]{3,96}$/.test(room)) { status('Enter a session code first.'); return; }
  const url = new URL(location.href); url.hash = new URLSearchParams({room}).toString();
  try { await navigator.clipboard.writeText(url.href); status('Viewer link copied.'); }
  catch { status(`Viewer link: ${url.href}`); }
};
window.addEventListener('pagehide', stop);

const devices = document.querySelector('#devices');
const message = document.querySelector('#message');
const cards = new Map();
let live = null;
const say = text => { message.textContent = text; };
async function request(path, body) {
  const response = await fetch(path, { method: 'POST', body });
  if (!response.ok) throw new Error(await response.text());
  return response.json();
}
async function stopLive() {
  if (!live) return;
  const active = live; live = null;
  active.node?.disconnect();
  active.node?.port.close();
  active.source?.disconnect();
  active.stream?.getTracks().forEach(track => track.stop());
  active.socket?.close();
  await active.context?.close();
  say('Browser microphone stopped.');
}
async function startLive(id) {
  await stopLive();
  const active = { id }; live = active;
  try {
    active.stream = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1, echoCancellation: true } });
    if (live !== active) { active.stream.getTracks().forEach(track => track.stop()); return; }
    active.context = new AudioContext({ sampleRate: 16000 });
    if (active.context.sampleRate !== 16000) throw new Error('This browser cannot provide 16 kHz microphone audio.');
    await active.context.audioWorklet.addModule('/worklet.js');
    active.socket = new WebSocket(`${location.protocol === 'https:' ? 'wss' : 'ws'}://${location.host}/api/devices/${id}/live`);
    await new Promise((resolve, reject) => { active.socket.onopen = resolve; active.socket.onerror = () => reject(new Error('Device input is busy or unavailable.')); });
    if (live !== active) return;
    active.node = new AudioWorkletNode(active.context, 'pcm-input');
    active.source = active.context.createMediaStreamSource(active.stream);
    // Keep processing active while muting local monitoring.
    active.mute = active.context.createGain(); active.mute.gain.value = 0;
    active.source.connect(active.node).connect(active.mute).connect(active.context.destination);
    active.node.port.onmessage = ({ data }) => {
      if (active.socket.readyState === WebSocket.OPEN && active.socket.bufferedAmount <= 64000) active.socket.send(data);
      else say('Microphone upload overrun; dropped audio.');
    };
    active.socket.onclose = () => { if (live === active) stopLive().catch(error => say(error.message)); };
    await active.context.resume(); say(`Browser microphone → ${id}`);
  } catch (error) { if (live === active) await stopLive(); throw error; }
}
function buildCard(device) {
  const card = document.createElement('article');
  card.innerHTML = '<h2></h2><small></small><label>Microphone <meter min="0" max="1"></meter></label><label>Speaker <meter min="0" max="1"></meter></label><div class="stats"></div><label>WAV fixture <input type="file" accept=".wav,audio/wav"></label><div class="controls"></div><audio controls></audio><a download>Download recording</a>';
  card.querySelector('h2').textContent = device.id;
  const input = card.querySelector('input'), player = card.querySelector('audio');
  let fixture = null, recordingURL = null;
  input.onchange = () => { fixture = input.files[0] || null; };
  const control = (title, action) => {
    const button = document.createElement('button'); button.textContent = title;
    button.onclick = async () => { button.disabled = true; try { await action(); } catch (error) { say(error.message); } finally { button.disabled = false; } };
    card.querySelector('.controls').append(button);
  };
  const path = `/api/devices/${device.id}`;
  control('Replay WAV', async () => { if (!fixture) throw new Error('Choose a mono PCM16 16 kHz WAV first.'); await request(`${path}/play`, fixture); say(`Playing ${fixture.name} → ${device.id}`); });
  control('Stop input', async () => { if (live?.id === device.id) await stopLive(); await request(`${path}/stop`); });
  control('Use browser mic', () => startLive(device.id));
  control('Disconnect', () => request(`${path}/disconnect`));
  control('Reconnect', () => request(`${path}/reconnect`));
  control('Listen to reply', async () => {
    const response = await fetch(`${path}/recording`); if (!response.ok) throw new Error(await response.text());
    if (recordingURL) URL.revokeObjectURL(recordingURL);
    recordingURL = URL.createObjectURL(await response.blob()); player.src = recordingURL; await player.play();
  });
  control('Clear recording', () => request(`${path}/reset`));
  card.querySelector('a').href = `${path}/recording`;
  devices.append(card); cards.set(device.id, card);
}
async function refresh() {
  try {
    const response = await fetch('/api/devices'); if (!response.ok) throw new Error(await response.text());
    for (const device of await response.json()) {
      if (!cards.has(device.id)) buildCard(device);
      const card = cards.get(device.id), meters = card.querySelectorAll('meter');
      card.querySelector('small').textContent = `${device.profile} · ${device.online ? 'online' : 'offline'} · ${device.connections} RTSP connections`;
      meters[0].value = device.input_level; meters[1].value = device.output_level;
      card.querySelector('.stats').textContent = `Input: ${device.input}\nPackets: mic ${device.mic_packets} / speaker ${device.speaker_packets}\nRecording: ${device.recorded_seconds.toFixed(2)}s\nOverruns: ${device.overruns} / decode errors: ${device.decode_errors}${device.last_error ? `\n${device.last_error}` : ''}`;
    }
  } catch (error) { say(error.message); }
  setTimeout(refresh, 500); // One request at a time, including slow clients.
}
document.querySelector('#run').onclick = async event => {
  event.target.disabled = true;
  try { document.querySelector('#report').textContent = JSON.stringify(await request('/api/scenarios', document.querySelector('#scenario').value), null, 2); }
  catch (error) { say(error.message); } finally { event.target.disabled = false; }
};
window.addEventListener('pagehide', () => { stopLive(); });
refresh();

const http = require('http'), fs = require('fs'), path = require('path'), zlib = require('zlib');
const WebSocket = require('ws');
const BASE = 'livetiming.formula1.com/signalr';
const HUB = encodeURIComponent('[{"name":"Streaming"}]');
let circuit = null, circuitKey = '', state = {}, standings = {}, connected = false, cars = {}, track = [], seen = new Set();

function merge(t, s) {
  for (const k in s) {
    if (s[k] && typeof s[k] === 'object' && !Array.isArray(s[k])) {
      if (!t[k] || typeof t[k] !== 'object') t[k] = {};
      merge(t[k], s[k]);
    } else t[k] = s[k];
  }
  return t;
}
function ingest(topic, v) {
  if (topic.endsWith('.z')) {
    try { v = JSON.parse(zlib.inflateRawSync(Buffer.from(v, 'base64')).toString()); } catch { return; }
    if (topic === 'Position.z') {
      (v.Position || []).forEach(p => {
        for (const n in p.Entries || {}) {
          const e = p.Entries[n];
          if (e.Status === 'OffTrack' || (!e.X && !e.Y)) continue;
          cars[n] = { x: e.X, y: e.Y };
          const k = Math.round(e.X / 180) + ',' + Math.round(e.Y / 180);
          if (!seen.has(k) && track.length < 4000) { seen.add(k); track.push([e.X, e.Y]); }
        }
      });
    }
    return;
  }
  merge(state[topic] = state[topic] || {}, v);
  if (topic === 'SessionInfo') loadCircuit();
}
async function loadCircuit() {
  const s = state.SessionInfo || {}, key = ((s.Meeting || {}).Circuit || {}).Key, yr = (s.StartDate || '').slice(0, 4);
  if (!key || !yr || circuitKey === key + yr) return;
  circuitKey = key + yr;
  try {
    const j = await (await fetch(`https://api.multiviewer.app/api/v1/circuits/${key}/${yr}`, { headers: { 'User-Agent': 'f1-live' } })).json();
    if (j.x && j.y) circuit = j.x.map((x, i) => [x, j.y[i]]);
  } catch (e) { circuitKey = ''; }
}

async function connect() {
  try {
    const r = await fetch(`https://${BASE}/negotiate?connectionData=${HUB}&clientProtocol=1.5`);
    const j = await r.json();
    const cookie = (r.headers.getSetCookie ? r.headers.getSetCookie() : []).map(c => c.split(';')[0]).join('; ');
    const ws = new WebSocket(
      `wss://${BASE}/connect?clientProtocol=1.5&transport=webSockets&connectionToken=${encodeURIComponent(j.ConnectionToken)}&connectionData=${HUB}`,
      { headers: { 'User-Agent': 'BestHTTP', 'Accept-Encoding': 'gzip,identity', Cookie: cookie } });
    ws.on('open', () => {
      connected = true; state = {}; cars = {};
      ws.send(JSON.stringify({ H: 'Streaming', M: 'Subscribe', I: 1,
        A: [['Heartbeat', 'SessionInfo', 'TrackStatus', 'DriverList', 'TimingData', 'TimingAppData', 'LapCount', 'SessionStatus', 'Position.z']] }));
    });
    ws.on('message', m => {
      let d; try { d = JSON.parse(m); } catch { return; }
      if (d.R) for (const k in d.R) ingest(k, d.R[k]);
      (d.M || []).forEach(x => { if (x.M === 'feed') ingest(x.A[0], x.A[1]); });
    });
    ws.on('close', () => { connected = false; setTimeout(connect, 5000); });
    ws.on('error', () => { try { ws.close(); } catch {} });
  } catch (e) { setTimeout(connect, 10000); }
}

async function loadStandings() {
  try {
    const r = await fetch('https://api.jolpi.ca/ergast/f1/current/driverStandings.json');
    const j = await r.json();
    const o = {};
    j.MRData.StandingsTable.StandingsLists[0].DriverStandings.forEach(x => o[x.Driver.code] = +x.points);
    standings = o;
  } catch (e) {}
}

const last = o => { const k = Object.keys(o || {}).sort((a, b) => a - b).pop(); return k === undefined ? {} : o[k]; };
function snapshot() {
  const dl = state.DriverList || {}, tl = (state.TimingData || {}).Lines || {}, ap = (state.TimingAppData || {}).Lines || {}, s = state.SessionInfo || {};
  const rows = Object.entries(tl).map(([n, l]) => {
    const secs = [0, 1, 2].map(i => { const x = (l.Sectors || {})[i] || {}; return { v: x.Value || '', c: x.OverallFastest ? 2 : x.PersonalFastest ? 1 : 0 }; });
    const segs = [0, 1, 2].map(i => { const sg = ((l.Sectors || {})[i] || {}).Segments || {}; return Object.keys(sg).sort((a, b) => a - b).map(k => (sg[k] || {}).Status | 0); });
    const sp = l.Speeds || {};
    return { tla: (dl[n] || {}).Tla || n, color: '#' + ((dl[n] || {}).TeamColour || '888888'), pos: +l.Position || 99,
      gap: l.GapToLeader || l.TimeDiffToFastest || '', int: (l.IntervalToPositionAhead || {}).Value || '',
      last: (l.LastLapTime || {}).Value || '', best: (l.BestLapTime || {}).Value || '', pit: !!l.InPit,
      tyre: (last((ap[n] || {}).Stints).Compound || '')[0] || '', secs, segs, out: !!l.KnockedOut, st: +((sp.ST || {}).Value) || 0 };
  }).sort((a, b) => a.pos - b.pos);
  const cl = Object.entries(cars).map(([n, c]) => ({ tla: (dl[n] || {}).Tla || n, color: '#' + ((dl[n] || {}).TeamColour || '888888'), x: c.x, y: c.y }));
  return { connected, hasData: rows.length > 0, session: s.Name || '', meeting: (s.Meeting || {}).Name || '',
    track: (state.TrackStatus || {}).Message || '', rows, cars: cl, standings,
    part: (state.TimingData || {}).SessionPart || 0, lap: (state.LapCount || {}).CurrentLap || 0,
    total: (state.LapCount || {}).TotalLaps || 0, status: (state.SessionStatus || {}).Status || '' };
}

http.createServer((req, res) => {
  const json = o => { res.writeHead(200, { 'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*' }); res.end(JSON.stringify(o)); };
  if (req.url.startsWith('/state')) return json(snapshot());
  if (req.url.startsWith('/track')) return json(circuit ? { line: true, pts: circuit } : { line: false, pts: track });
  fs.readFile(path.join(__dirname, 'index.html'), (e, b) => {
    res.writeHead(e ? 500 : 200, { 'Content-Type': 'text/html; charset=utf-8' }); res.end(b || 'errore');
  });
}).listen(process.env.PORT || 3000);

connect(); loadStandings(); setInterval(loadStandings, 10 * 60 * 1000);

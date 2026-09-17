/* Relay for Claude Zombies.
 *
 *   npm install          (once)
 *   npm run serve        (or: node server/relay.js [port])
 *
 * Serves game.html and relays messages between the browsers in a room. It runs no game logic
 * at all: one browser in each room is the host and owns the simulation (see MULTIPLAYER in
 * CLAUDE.md for why). That keeps this file small enough to read in one sitting and means
 * there is exactly one copy of the game rules, in game.html.
 *
 * TWO TRANSPORTS, ONE ROUTING TABLE. A WebSocket at /ws is the good path. Plenty of networks
 * break it anyway -- a school or workplace filter that man-in-the-middles TLS very often
 * refuses the Upgrade header, and some proxies accept the upgrade and then cut the connection
 * -- so the same relay also speaks ordinary request/response JSON at /cz/*, which looks like
 * any other web traffic and survives all of that. The client tries the socket first and falls
 * back on its own. Both end up in route() below, so there is one copy of the policy and no
 * transport-specific bugs hiding in it.
 *
 * Behind a host that terminates TLS (Render, Fly, a reverse proxy) this needs no changes: the
 * page is served over https, the socket becomes wss, the polling endpoints are ordinary https
 * POSTs, and all of it is on 443.
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const ROOT = path.join(__dirname, '..');
const PORT = Number(process.argv[2] || process.env.PORT || 8080);
/* tools/nettest.js points this at an instrumented copy of the page; nothing else should. */
const PAGE = process.env.CZ_PAGE || 'game.html';
const MAXMSG = 64 * 1024;
/* A polling client calls in at least three times a second while it is alive, so six seconds
 * of silence is not a slow network, it is a closed tab. It normally does not come to this:
 * the client beacons a 'bye' on the way out and this is only the backstop. */
const POLL_GONE = 6000;
const POLL_HOLD = 500;     // longest a /cz/io call parks waiting for something to say
const OUTBOX_MAX = 600;    // a peer that stops collecting must not grow without limit

/* Which peers a message reaches, and who is allowed to send it. The relay never looks
 * inside the payload; this table is the whole of its policy. */
const ROUTE = {
  me:   { to: 'others' },                 // a player's own transform, 20 Hz
  fx:   { to: 'others' },                 // cosmetic one-shots: fired, reloaded, jumped
  snap: { to: 'others', hostOnly: true }, // the world: zombies, round, doors
  ev:   { to: 'others', hostOnly: true }, // authoritative one-shots: door open, kill, round
  hit:  { to: 'host' },                   // "I hit zombie n for d" — the host decides
  buy:  { to: 'host' },                   // a request to open a shared door
  rev:  { to: 'others' },                 // "I picked player n up" — from whoever did it
  pw:   { to: 'others' },                 // "I took power-up n" — its effect is team-wide
  chat: { to: 'others' }
};

const rooms = new Map();   // code -> { peers: Map<id, Peer>, host: id|null }
const byToken = new Map(); // polling peers only
let nextId = 1;

/* One peer, either kind. A socket peer writes straight out; a polling peer collects into an
 * outbox that its next /cz/io call drains. Nothing above this line cares which it is. */
function Peer(name, ws) {
  this.id = nextId++;
  this.name = String(name || 'Player').slice(0, 16);
  this.ws = ws || null;
  this.out = [];
  this.seen = Date.now();
  this.room = null;
  this.token = ws ? null : crypto.randomBytes(16).toString('hex');
  this.waiter = null;      // a parked /cz/io response, woken by the next message for it
}
Peer.prototype.send = function (obj) {
  if (this.ws) {
    if (this.ws.readyState === 1) { try { this.ws.send(JSON.stringify(obj)); } catch (e) {} }
    return;
  }
  this.out.push(obj);
  if (this.out.length > OUTBOX_MAX) this.out.splice(0, this.out.length - OUTBOX_MAX);
  if (this.waiter) { const w = this.waiter; this.waiter = null; w(); }
};

function room(code) {
  if (!rooms.has(code)) rooms.set(code, { peers: new Map(), host: null });
  return rooms.get(code);
}
function roomCode(raw) {
  return String(raw || 'LAN').toUpperCase().slice(0, 8).replace(/[^A-Z0-9]/g, '') || 'LAN';
}
function roster(r) {
  const list = [];
  r.peers.forEach((p, id) => list.push({ id, name: p.name, host: id === r.host }));
  return list;
}
function announce(r) {
  const list = roster(r);
  r.peers.forEach(p => p.send({ t: 'roster', host: r.host, roster: list }));
}
function join(peer, code) {
  const r = room(code);
  peer.room = code;
  r.peers.set(peer.id, peer);
  if (r.host === null) r.host = peer.id;
  if (peer.token) byToken.set(peer.token, peer);
  const welcome = { t: 'welcome', id: peer.id, room: code, host: r.host, roster: roster(r) };
  announce(r);
  return welcome;
}
function part(peer) {
  const code = peer.room;
  if (peer.token) byToken.delete(peer.token);
  peer.room = null;
  const r = code && rooms.get(code);
  if (!r) return;
  r.peers.delete(peer.id);
  if (!r.peers.size) { rooms.delete(code); return; }
  /* The host left, so the world it was simulating is gone. Promote the peer that has been
   * here longest and tell everyone: the new host restarts the round rather than trying to
   * inherit a simulation it never ran. */
  if (r.host === peer.id) r.host = r.peers.keys().next().value;
  announce(r);
}
/* The only thing that moves a message. Both transports call it; neither adds to it. */
function route(peer, m) {
  if (m.t === 'bye') { part(peer); return; }
  const r = peer.room && rooms.get(peer.room);
  if (!r) return;
  const rule = ROUTE[m.t];
  if (!rule) return;
  if (rule.hostOnly && peer.id !== r.host) return;
  m.from = peer.id;
  if (rule.to === 'host') {
    const h = r.peers.get(r.host);
    if (h && h !== peer) h.send(m);
  } else {
    r.peers.forEach(p => { if (p !== peer) p.send(m); });
  }
}

/* A polling peer has no close event, so silence is the only sign that it has gone. */
setInterval(() => {
  const now = Date.now();
  byToken.forEach(p => { if (now - p.seen > POLL_GONE) part(p); });
}, 2000).unref();

/* game.html loads three.js from a CDN, which is right for the published single-file build and
 * wrong on a LAN with no way out to the internet. The relay serves a local copy and rewrites
 * the tag on the way out, so the file on disk keeps its CDN tag and a game played round a
 * kitchen table does not need the internet at all. */
const CDN = 'https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js';
const THREE_LOCAL = path.join(ROOT, 'node_modules', 'three', 'build', 'three.min.js');

/* Open on purpose. These endpoints carry a room code and a per-peer token and never a cookie
 * or anything else a browser attaches by itself, so there is nothing here for another origin
 * to abuse by calling them. Being reachable cross-origin is what lets a copy of the page
 * served from somewhere else still talk to a relay. */
const CORS = {
  'access-control-allow-origin': '*',
  'access-control-allow-methods': 'GET, POST, OPTIONS',
  'access-control-allow-headers': 'content-type',
  'access-control-max-age': '86400'
};
function json(res, code, obj) {
  res.writeHead(code, Object.assign(
    { 'content-type': 'application/json', 'cache-control': 'no-store' }, CORS));
  res.end(JSON.stringify(obj));
}
function readBody(req, done) {
  let n = 0; const parts = [];
  req.on('data', d => {
    n += d.length;
    if (n > MAXMSG) { req.destroy(); return; }
    parts.push(d);
  });
  req.on('end', () => {
    try { done(JSON.parse(Buffer.concat(parts).toString('utf8') || '{}')); }
    catch (e) { done(null); }
  });
  req.on('error', () => done(null));
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];
  if (req.method === 'OPTIONS') { res.writeHead(204, CORS); return res.end(); }

  /* ---- the polling transport ---- */
  if (url === '/cz/join') {
    return readBody(req, m => {
      if (!m) return json(res, 400, { error: 'bad json' });
      const peer = new Peer(m.name, null);
      const welcome = join(peer, roomCode(m.room));
      // the same welcome a socket gets, so the client feeds both through one handler
      json(res, 200, { token: peer.token, msgs: [welcome] });
    });
  }
  if (url === '/cz/io') {
    return readBody(req, m => {
      if (!m) return json(res, 400, { error: 'bad json' });
      const peer = byToken.get(String(m.token || ''));
      if (!peer || !peer.room) return json(res, 410, { error: 'gone' });
      peer.seen = Date.now();
      const msgs = Array.isArray(m.msgs) ? m.msgs : [];
      for (let i = 0; i < msgs.length; i++)
        if (msgs[i] && typeof msgs[i].t === 'string') route(peer, msgs[i]);
      let timer = null, done = false;
      const flush = () => {
        if (done) return;
        done = true;
        peer.waiter = null;
        if (timer) clearTimeout(timer);
        const out = peer.out; peer.out = [];
        json(res, 200, { msgs: out });
      };
      /* Answer at once when there is anything to say. Otherwise park briefly: long enough
       * that an idle lobby is not hammering, short enough that no proxy decides the response
       * has stalled and cuts it. Never a long poll -- that is the thing these networks break. */
      if (peer.out.length) return flush();
      const wait = Math.min(Math.max(Number(m.wait) || 0, 0), POLL_HOLD);
      if (!wait) return flush();
      timer = setTimeout(flush, wait);
      peer.waiter = flush;
      res.on('close', () => { peer.waiter = null; done = true; if (timer) clearTimeout(timer); });
    });
  }
  if (url === '/cz/health') return json(res, 200, { ok: true, rooms: rooms.size, polling: byToken.size });

  if (url === '/lan') return json(res, 200, { port: PORT, addresses: lanAddresses() });
  if (url === '/three.min.js') {
    return fs.readFile(THREE_LOCAL, (err, buf) => {
      if (err) { res.writeHead(404); return res.end('// run npm install first'); }
      res.writeHead(200, { 'content-type': 'text/javascript; charset=utf-8' });
      res.end(buf);
    });
  }
  // one page, one file: anything else is a typo, so send the game either way
  fs.readFile(path.join(ROOT, PAGE), 'utf8', (err, text) => {
    if (err) { res.writeHead(500); return res.end(PAGE + ' is missing'); }
    const out = fs.existsSync(THREE_LOCAL) ? text.split(CDN).join('/three.min.js') : text;
    res.writeHead(200, { 'content-type': 'text/html; charset=utf-8', 'cache-control': 'no-store' });
    res.end(out);
  });
});

const wss = new WebSocketServer({ server, path: '/ws', maxPayload: MAXMSG });

wss.on('connection', ws => {
  const peer = new Peer('Player', ws);
  ws.on('message', raw => {
    if (raw.length > MAXMSG) return;
    let m; try { m = JSON.parse(raw); } catch (e) { return; }
    if (!m || typeof m.t !== 'string') return;
    if (m.t === 'join') {
      if (peer.room) return;
      peer.name = String(m.name || 'Player').slice(0, 16);
      peer.send(join(peer, roomCode(m.room)));
      return;
    }
    route(peer, m);
  });
  ws.on('close', () => part(peer));
  ws.on('error', () => {});
});

function lanAddresses() {
  const out = [];
  const ifs = os.networkInterfaces();
  for (const name in ifs) for (const a of ifs[name] || [])
    if (a.family === 'IPv4' && !a.internal) out.push(a.address);
  return out;
}

/* A port still held by a previous run is the common way this fails, and "node:events:497"
 * is not a useful thing to print at somebody. */
server.on('error', e => {
  if (e.code === 'EADDRINUSE') {
    console.error('Port ' + PORT + ' is already in use — an earlier relay may still be running.\n' +
      'Stop it, or pick another port:  npm run serve -- 8090');
    process.exit(1);
  }
  throw e;
});
server.listen(PORT, () => {
  const pub = process.env.CZ_PUBLIC_URL || process.env.RENDER_EXTERNAL_URL || '';
  const addrs = lanAddresses();
  console.log('Claude Zombies relay on port ' + PORT);
  if (pub) console.log('  public       : ' + pub);
  console.log('  this machine : http://localhost:' + PORT);
  addrs.forEach(a => console.log('  on the LAN   : http://' + a + ':' + PORT));
  if (!addrs.length && !pub) console.log('  (no LAN address found — others will not be able to reach this)');
});

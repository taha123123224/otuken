// ÖTÜKEN oyun sunucusu — Cloudflare Worker + Durable Object
// Tek bir "Dünya" nesnesi bütün bağlantıları tutar: lobi (kanal sayıları, klan sohbeti, duyurular) ve Kanal 1-4.
// Oyuncular aynı kanaldaki diğer oyuncuların konumunu/hareketini (presence) görür, olay yayınlar (sohbet, grup, grup tecrübesi)
// ve küçük bir belge deposunu kullanır (oyuncu listesi, klanlar, arkadaş listeleri).
// WebSocket Hibernation API kullanılır: kimse yokken nesne uyur ve ücret işlemez.

const ROOMS = new Set(['lobby', 'ch1', 'ch2', 'ch3', 'ch4']);
const MAX_MSG = 16384;             // bytes per incoming message (monster snapshots from the channel host are the largest)
const RATE = 60, BURST = 160;      // messages per second per connection
const UID_RE = /^[A-Za-z0-9_-]{8,48}$/;
const COLS = { players: 300, clans: 300 };

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    if (url.pathname === '/' || url.pathname === '/durum') {
      const id = env.DUNYA.idFromName('dunya');
      const r = await env.DUNYA.get(id).fetch('https://dunya/durum');
      return new Response(await r.text(), { headers: { 'content-type': 'application/json; charset=utf-8', 'access-control-allow-origin': '*' } });
    }
    if (url.pathname === '/ws') {
      if (req.headers.get('Upgrade') !== 'websocket') return new Response('WebSocket bekleniyor', { status: 426 });
      const id = env.DUNYA.idFromName('dunya');
      return env.DUNYA.get(id).fetch(req);
    }
    return new Response('Ötüken sunucusu', { status: 404 });
  }
};

const sha = async s => { const b = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(s)); return [...new Uint8Array(b)].map(x => x.toString(16).padStart(2, '0')).join(''); };
const rid = () => crypto.randomUUID().replace(/-/g, '').slice(0, 20);

export class Dunya {
  constructor(ctx, env) {
    this.ctx = ctx; this.env = env;
    this.conns = new Map();   // peer -> {ws, st}
    this.cache = new Map();   // collection name -> Map(id -> data)
    this.dirty = new Set(); this.flushT = null; this.jseq = Date.now() * 10;
    // wake up after hibernation: restore the connection table from the socket attachments
    for (const ws of ctx.getWebSockets()) { const st = ws.deserializeAttachment(); if (st && st.peer) this.conns.set(st.peer, { ws, st }); }
    ctx.setWebSocketAutoResponse(new WebSocketRequestResponsePair('{"t":"ping"}', '{"t":"pong"}'));
  }

  async fetch(req) {
    const url = new URL(req.url);
    if (url.pathname === '/durum') {
      const ch = { lobby: 0, ch1: 0, ch2: 0, ch3: 0, ch4: 0 };
      for (const { st } of this.conns.values()) for (const r in st.rooms) ch[r] = (ch[r] || 0) + 1;
      return new Response(JSON.stringify({ ok: true, ad: 'Ötüken', baglanti: this.conns.size, kanallar: ch }));
    }
    const pair = new WebSocketPair(); const [client, server] = Object.values(pair);
    const st = { peer: rid(), uid: null, name: '', rooms: {}, subs: {}, tk: BURST, tt: Date.now() };
    this.ctx.acceptWebSocket(server);
    server.serializeAttachment(st);
    this.conns.set(st.peer, { ws: server, st });
    return new Response(null, { status: 101, webSocket: client });
  }

  save(c) { try { c.ws.serializeAttachment(c.st); } catch (e) { } }
  send(ws, o) { try { ws.send(typeof o === 'string' ? o : JSON.stringify(o)); } catch (e) { } }
  conn(ws) { const st = ws.deserializeAttachment(); let c = st && this.conns.get(st.peer); if (!c && st) { c = { ws, st }; this.conns.set(st.peer, c); } return c; }
  inRoom(r) { const out = []; for (const c of this.conns.values()) if (c.st.uid && c.st.rooms[r] !== undefined) out.push(c); return out; }
  bcast(r, o, except) { const s = JSON.stringify(o); for (const c of this.inRoom(r)) if (c.st.peer !== except) this.send(c.ws, s); }
  // js = join order; the oldest peer in a channel simulates its monsters ("host")
  peerOf(c, r) { return { peer: c.st.peer, by: c.st.uid, pr: c.st.rooms[r] || {}, js: (c.st.js && c.st.js[r]) || 0 }; }

  async webSocketMessage(ws, msg) {
    const c = this.conn(ws); if (!c) return;
    if (typeof msg !== 'string' || msg.length > MAX_MSG) return;
    // token bucket rate limit
    const now = Date.now(); c.st.tk = Math.min(BURST, c.st.tk + (now - c.st.tt) / 1000 * RATE); c.st.tt = now;
    if (c.st.tk < 1) return; c.st.tk -= 1;
    let m; try { m = JSON.parse(msg); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    try { await this.handle(c, m); } catch (e) { if (m.q) this.send(c.ws, { t: 'ack', q: m.q, ok: false, code: 'internal' }); }
  }

  async handle(c, m) {
    const st = c.st;
    if (m.t === 'hello') {
      const uid = String(m.uid || ''), key = String(m.key || '');
      if (!UID_RE.test(uid) || key.length < 16 || key.length > 128) return this.send(c.ws, { t: 'err', code: 'bad_hello' });
      const h = await sha(key), k = 'a:' + uid, old = await this.ctx.storage.get(k);
      if (old && old !== h) return this.send(c.ws, { t: 'err', code: 'bad_key' });
      if (!old) await this.ctx.storage.put(k, h);
      st.uid = uid; this.save(c);
      return this.send(c.ws, { t: 'welcome', peer: st.peer, uid });
    }
    if (!st.uid) return;
    switch (m.t) {
      case 'join': {
        const r = String(m.r); if (!ROOMS.has(r)) return;
        if (r !== 'lobby') for (const o of Object.keys(st.rooms)) if (o !== 'lobby' && o !== r) this.leaveRoom(c, o);
        const pr = m.pr && typeof m.pr === 'object' ? m.pr : {};
        st.rooms[r] = pr; st.js = st.js || {}; st.js[r] = ++this.jseq; this.save(c);
        this.send(c.ws, { t: 'peers', r, peers: this.inRoom(r).map(x => this.peerOf(x, r)) });
        this.bcast(r, { t: 'pj', r, ...this.peerOf(c, r) }, st.peer);
        return;
      }
      case 'leave': { const r = String(m.r); if (st.rooms[r] !== undefined) this.leaveRoom(c, r); return; }
      case 'pres': {
        const r = String(m.r); if (st.rooms[r] === undefined || !m.pr || typeof m.pr !== 'object') return;
        const cur = st.rooms[r]; for (const k in m.pr) { if (m.pr[k] === null) delete cur[k]; else cur[k] = m.pr[k]; }
        this.save(c); this.bcast(r, { t: 'pp', r, peer: st.peer, by: st.uid, pr: cur }, st.peer);
        return;
      }
      case 'emit': {
        const r = String(m.r), ev = String(m.ev || '').slice(0, 24); if (st.rooms[r] === undefined || !ev) return;
        this.bcast(r, { t: 'ev', r, ev, d: m.d, peer: st.peer, by: st.uid });
        return;
      }
      case 'sub': {
        const id = String(m.id || '').slice(0, 24), path = String(m.path || ''); if (!id || !path) return;
        st.subs[id] = { path, kind: m.kind === 'col' ? 'col' : 'doc', ob: m.ob ? String(m.ob) : null, dir: m.dir === 'asc' ? 'asc' : 'desc', lim: Math.min(500, m.lim | 0 || 300) };
        this.save(c); await this.pushSub(c, id);
        return;
      }
      case 'unsub': { delete st.subs[String(m.id)]; this.save(c); return; }
      case 'set': case 'update': case 'delete': return this.write(c, m);
      case 'nabiz': return this.nabiz(c, m);
    }
  }

  // heartbeat for the admin panel, relayed to Supabase (the desktop app runs from file:// where direct calls can be blocked)
  async nabiz(c, m) {
    const now = Date.now(); if (now - (c.st.nb || 0) < 20000 || !this.env.SUPA_URL || !this.env.SUPA_KEY) return; c.st.nb = now;
    const b = m.b && typeof m.b === 'object' ? m.b : null; if (!b) return;
    const body = {}; for (const k of ['p_id', 'p_oturum', 'p_ad', 'p_sinif', 'p_sv', 'p_bolge', 'p_durum', 'p_surum', 'p_platform', 'p_sure']) if (b[k] !== undefined) body[k] = b[k];
    try {
      const r = await fetch(this.env.SUPA_URL.replace(/\/$/, '') + '/rest/v1/rpc/nabiz', { method: 'POST', headers: { 'Content-Type': 'application/json', apikey: this.env.SUPA_KEY }, body: JSON.stringify(body) });
      const j = r.ok ? await r.json() : null; this.send(c.ws, { t: 'nabiz', j });
    } catch (e) { this.send(c.ws, { t: 'nabiz', j: null }); }
  }

  leaveRoom(c, r) { delete c.st.rooms[r]; if (c.st.js) delete c.st.js[r]; this.save(c); this.bcast(r, { t: 'pl', r, peer: c.st.peer }); }

  async webSocketClose(ws) { this.drop(ws); }
  async webSocketError(ws) { this.drop(ws); }
  drop(ws) {
    let st; try { st = ws.deserializeAttachment(); } catch (e) { }
    if (!st) return; this.conns.delete(st.peer);
    for (const r of Object.keys(st.rooms || {})) this.bcast(r, { t: 'pl', r, peer: st.peer });
    try { ws.close(1000, 'bye'); } catch (e) { }
  }

  // ---------- document store ----------
  split(path) { const p = String(path).split('/').filter(Boolean); if (!p.length || p.some(s => s.length > 64 || !/^[A-Za-z0-9_-]+$/.test(s))) return null; return p; }
  async col(name) {
    if (this.cache.has(name)) return this.cache.get(name);
    const m = new Map(); const l = await this.ctx.storage.list({ prefix: 'd:' + name + '/' });
    for (const [k, v] of l) { const rest = k.slice(name.length + 3); if (!rest.includes('/')) m.set(rest, v); }
    this.cache.set(name, m); return m;
  }
  async getDoc(path) {
    const p = this.split(path); if (!p) return undefined;
    if (p.length === 2) return (await this.col(p[0])).get(p[1]);
    return this.ctx.storage.get('d:' + p.join('/'));
  }
  allowed(uid, p, before, after) {
    if (p[0] === 'players' && p.length === 2) return p[1] === uid;
    if (p[0] === 'data' && p[1] === 'users' && p.length === 4) return p[2] === uid;
    if (p[0] === 'clans' && p.length === 2) { if (!before) return !!after && after.leader === uid; return before.leader === uid; }
    return false;
  }
  async write(c, m) {
    const ack = (ok, code) => m.q && this.send(c.ws, { t: 'ack', q: m.q, ok, code });
    const p = this.split(m.path); if (!p) return ack(false, 'invalid_argument');
    const before = await this.getDoc(m.path);
    let after = null;
    if (m.t === 'set') after = m.data;
    else if (m.t === 'update') { if (!before) return ack(false, 'not_found'); after = Object.assign({}, before, m.data); }
    if (after !== null && (typeof after !== 'object' || Array.isArray(after) || JSON.stringify(after).length > 2048)) return ack(false, 'invalid_argument');
    if (!this.allowed(c.st.uid, p, before, after)) return ack(false, 'not_permitted');
    if (p.length === 2 && COLS[p[0]] && !before && after) { const col = await this.col(p[0]); if (col.size >= COLS[p[0]] * 3) return ack(false, 'quota_exceeded'); }
    const key = 'd:' + p.join('/');
    if (after) await this.ctx.storage.put(key, after); else await this.ctx.storage.delete(key);
    if (p.length === 2) { const col = await this.col(p[0]); if (after) col.set(p[1], after); else col.delete(p[1]); this.dirty.add('col:' + p[0]); }
    this.dirty.add('doc:' + p.join('/'));
    ack(true);
    if (!this.flushT) this.flushT = setTimeout(() => { this.flushT = null; this.flush().catch(() => { }); }, 400);
  }
  async flush() {
    const d = this.dirty; this.dirty = new Set();
    for (const c of this.conns.values()) for (const id in c.st.subs) { const s = c.st.subs[id]; if (d.has(s.kind + ':' + s.path.split('/').filter(Boolean).join('/'))) await this.pushSub(c, id); }
  }
  async pushSub(c, id) {
    const s = c.st.subs[id]; if (!s) return; const p = this.split(s.path); if (!p) return;
    if (s.kind === 'col') {
      if (p.length !== 1) return;
      let docs = [...(await this.col(p[0])).entries()].map(([id, data]) => ({ id, data }));
      if (s.ob) docs.sort((a, b) => { const x = a.data[s.ob] ?? 0, y = b.data[s.ob] ?? 0; return s.dir === 'asc' ? (x > y ? 1 : x < y ? -1 : 0) : (x < y ? 1 : x > y ? -1 : 0); });
      this.send(c.ws, { t: 'snap', id, docs: docs.slice(0, s.lim) });
    } else {
      const v = await this.getDoc(s.path); this.send(c.ws, { t: 'snap', id, exists: v !== undefined && v !== null, data: v ?? null });
    }
  }
}

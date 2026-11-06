// Marbles relay server: connects players in rooms by code and passes game packets between them.
// No dependencies (plain Node.js). Start with: node server.js   (port from $PORT, default 8080)
'use strict';
const http = require('http');
const crypto = require('crypto');

const PORT = process.env.PORT || 8080;
const MAX_PLAYERS = 4;
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
const MAX_FRAME = 8 * 1024 * 1024;

const rooms = new Map(); // code -> { host: Conn, peers: Map<id, Conn>, created }

// ---------------------------------------------------------------- tiny WebSocket implementation
class Conn {
  constructor(socket) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.frag = null;      // fragmented message in progress
    this.fragOp = 0;
    this.open = true;
    this.room = null;
    this.id = 0;
    this.alive = true;
    socket.setNoDelay(true);
    socket.on('data', d => this.onData(d));
    socket.on('close', () => this.onClose());
    socket.on('error', () => this.onClose());
  }
  onData(d) {
    this.buf = this.buf.length ? Buffer.concat([this.buf, d]) : d;
    for (;;) {
      if (this.buf.length < 2) return;
      const b0 = this.buf[0], b1 = this.buf[1];
      const fin = (b0 & 0x80) !== 0, op = b0 & 0x0f, masked = (b1 & 0x80) !== 0;
      let len = b1 & 0x7f, off = 2;
      if (len === 126) { if (this.buf.length < 4) return; len = this.buf.readUInt16BE(2); off = 4; }
      else if (len === 127) { if (this.buf.length < 10) return; len = Number(this.buf.readBigUInt64BE(2)); off = 10; }
      if (len > MAX_FRAME) return this.close(1009);
      const need = off + (masked ? 4 : 0) + len;
      if (this.buf.length < need) return;
      let payload = this.buf.subarray(off + (masked ? 4 : 0), need);
      if (masked) {
        const m = this.buf.subarray(off, off + 4), out = Buffer.allocUnsafe(len);
        for (let i = 0; i < len; i++) out[i] = payload[i] ^ m[i & 3];
        payload = out;
      } else payload = Buffer.from(payload);
      this.buf = this.buf.subarray(need);
      this.alive = true;
      if (op === 0x8) return this.close(1000);
      if (op === 0x9) { this.sendFrame(0xA, payload); continue; }
      if (op === 0xA) continue;
      if (op === 0x0) {                       // continuation
        if (!this.frag) return this.close(1002);
        this.frag.push(payload);
        if (fin) { const all = Buffer.concat(this.frag); const o = this.fragOp; this.frag = null; this.onMessage(o, all); }
        continue;
      }
      if (!fin) { this.frag = [payload]; this.fragOp = op; continue; }
      this.onMessage(op, payload);
    }
  }
  sendFrame(op, data) {
    if (!this.open) return;
    const len = data.length;
    let head;
    if (len < 126) head = Buffer.from([0x80 | op, len]);
    else if (len < 65536) { head = Buffer.alloc(4); head[0] = 0x80 | op; head[1] = 126; head.writeUInt16BE(len, 2); }
    else { head = Buffer.alloc(10); head[0] = 0x80 | op; head[1] = 127; head.writeBigUInt64BE(BigInt(len), 2); }
    try { this.socket.write(Buffer.concat([head, data])); } catch (e) { this.onClose(); }
  }
  sendJSON(obj) { this.sendFrame(0x1, Buffer.from(JSON.stringify(obj))); }
  sendBinary(buf) { this.sendFrame(0x2, buf); }
  close(code = 1000) {
    if (!this.open) return;
    const b = Buffer.alloc(2); b.writeUInt16BE(code, 0);
    this.sendFrame(0x8, b);
    this.open = false;
    try { this.socket.end(); } catch (e) {}
    setTimeout(() => { try { this.socket.destroy(); } catch (e) {} }, 1000);
    this.leave();
  }
  onClose() {
    this.open = false;
    this.leave();
  }
  // ------------------------------------------------------------ game protocol
  onMessage(op, data) {
    if (op === 0x1) {
      let m; try { m = JSON.parse(data.toString('utf8')); } catch (e) { return; }
      return this.onControl(m);
    }
    if (op !== 0x2 || !this.room || data.length < 4) return;
    const room = this.room;
    const target = data.readInt32LE(0);
    const out = Buffer.allocUnsafe(data.length);
    out.writeInt32LE(this.id, 0);
    data.copy(out, 4, 4);
    if (this !== room.host) { room.host.sendBinary(out); return; }   // clients always talk to the host
    if (target > 0) { const p = room.peers.get(target); if (p) p.sendBinary(out); return; }
    for (const [id, p] of room.peers) if (target === 0 || id !== -target) p.sendBinary(out);
  }
  onControl(m) {
    if (this.room) {
      if (m.op === 'kick' && this === this.room.host) {
        const p = this.room.peers.get(m.id | 0);
        if (p) { p.sendJSON({op: 'kicked'}); p.close(1000); }
      }
      return;
    }
    if (m.op === 'host') {
      let code;
      do { code = Array.from(crypto.randomBytes(6), b => ALPHABET[b & 31]).join(''); } while (rooms.has(code));
      const room = {host: this, peers: new Map(), created: Date.now(), code};
      rooms.set(code, room);
      this.room = room; this.id = 1;
      this.sendJSON({op: 'hosted', code, id: 1});
      log(`room ${code} opened (${rooms.size} rooms)`);
    } else if (m.op === 'join') {
      const code = String(m.code || '').toUpperCase().replace(/[^A-Z0-9]/g, '');
      const room = rooms.get(code);
      if (!room) { this.sendJSON({op: 'error', reason: 'no_room'}); return this.close(1000); }
      if (room.peers.size + 1 >= MAX_PLAYERS) { this.sendJSON({op: 'error', reason: 'full'}); return this.close(1000); }
      let id = m.id | 0;
      if (id < 2 || room.peers.has(id)) do { id = 2 + crypto.randomInt(0x7ffffff0); } while (room.peers.has(id));
      this.room = room; this.id = id;
      room.peers.set(id, this);
      this.sendJSON({op: 'joined', id});
      room.host.sendJSON({op: 'peer_connected', id});
    } else if (m.op === 'ping') {
      this.sendJSON({op: 'pong'});
    }
  }
  leave() {
    const room = this.room;
    if (!room) return;
    this.room = null;
    if (room.host === this) {
      rooms.delete(room.code);
      for (const p of room.peers.values()) { p.room = null; p.sendJSON({op: 'closed'}); p.close(1000); }
      log(`room ${room.code} closed (${rooms.size} rooms)`);
    } else if (room.peers.get(this.id) === this) {
      room.peers.delete(this.id);
      room.host.sendJSON({op: 'peer_disconnected', id: this.id});
    }
  }
}

function log(s) { console.log(new Date().toISOString(), s); }

// ---------------------------------------------------------------- HTTP + upgrade
const server = http.createServer((req, res) => {
  res.writeHead(200, {'Content-Type': 'text/plain; charset=utf-8', 'Access-Control-Allow-Origin': '*'});
  res.end(`Marbles relay OK · ${rooms.size} rooms\n`);
});

server.on('upgrade', (req, socket) => {
  const key = req.headers['sec-websocket-key'];
  if (!key || String(req.headers.upgrade).toLowerCase() !== 'websocket') { socket.destroy(); return; }
  const accept = crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  socket.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
               `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const c = new Conn(socket);
  conns.add(c);
  socket.on('close', () => conns.delete(c));
});

// keep connections alive through proxies; drop dead ones
const conns = new Set();
setInterval(() => {
  for (const c of conns) {
    if (!c.alive) { c.close(1001); conns.delete(c); continue; }
    c.alive = false;
    c.sendFrame(0x9, Buffer.alloc(0));
  }
}, 20000);

server.listen(PORT, () => log(`Marbles relay listening on ${PORT}`));

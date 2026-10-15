#!/usr/bin/env node
// GB Tempo LAN server: built-in Node.js only; no npm packages or internet required.
const http = require('http');
const fs = require('fs');
const path = require('path');
const os = require('os');
const crypto = require('crypto');
const ROOT = __dirname;
const PORT = Number(process.env.PORT || process.argv[2] || 8080);
const rooms = new Map();
const clients = new Set();
const mime = {'.html':'text/html; charset=utf-8','.js':'text/javascript; charset=utf-8','.css':'text/css; charset=utf-8','.json':'application/json; charset=utf-8','.webmanifest':'application/manifest+json','.png':'image/png','.jpg':'image/jpeg','.svg':'image/svg+xml','.ico':'image/x-icon','.md':'text/plain; charset=utf-8'};
function sendFrame(sock, value) {
  if (sock.destroyed || !sock.writable) return;
  const data = Buffer.from(typeof value === 'string' ? value : JSON.stringify(value));
  let head;
  if (data.length < 126) { head = Buffer.from([0x81, data.length]); }
  else if (data.length < 65536) { head = Buffer.alloc(4); head[0]=0x81; head[1]=126; head.writeUInt16BE(data.length,2); }
  else { head = Buffer.alloc(10); head[0]=0x81; head[1]=127; head.writeBigUInt64BE(BigInt(data.length),2); }
  sock.write(Buffer.concat([head,data]));
}
function removeClient(c) {
  if (!clients.has(c)) return;
  clients.delete(c);
  if (c.room && rooms.has(c.room)) {
    const room=rooms.get(c.room); room.members.delete(c);
    for (const member of room.members) sendFrame(member,{type:'members',count:room.members.size});
    if (!room.members.size) rooms.delete(c.room);
  }
}
function joinRoom(c, code) {
  const room=rooms.get(code); if(!room) { sendFrame(c,{type:'error',message:'Session code not found. Ask the host to create a session first.'}); return; }
  c.room=code; room.members.add(c);
  sendFrame(c,{type:'joined',code,name:room.name,members:room.members.size,state:room.state});
  for(const member of room.members) sendFrame(member,{type:'members',count:room.members.size});
}
function processMessage(c, raw) {
  let msg; try { msg=JSON.parse(raw); } catch { return; }
  if(!msg || typeof msg.type!=='string') return;
  if(msg.type==='create') {
    if(c.room) removeClient(c);
    let code; do { code=crypto.randomBytes(3).toString('hex').toUpperCase(); } while(rooms.has(code));
    const room={name:String(msg.name||'GB Tempo rehearsal').slice(0,80),members:new Set([c]),state:{bpm:120,signature:'4/4',subdivision:1}};
    rooms.set(code,room); c.room=code; c.role='host';
    sendFrame(c,{type:'created',code,name:room.name,members:1,state:room.state});
  } else if(msg.type==='join') {
    if(c.room) removeClient(c); c.role='guest'; joinRoom(c,String(msg.code||'').toUpperCase().slice(0,8));
  } else if(c.room && rooms.has(c.room)) {
    const room=rooms.get(c.room);
    if(msg.type==='state') {
      if(Number.isFinite(Number(msg.bpm))) room.state.bpm=Math.max(20,Math.min(300,Math.round(Number(msg.bpm))));
      if(typeof msg.signature==='string' && /^\d+\/\d+$/.test(msg.signature)) room.state.signature=msg.signature;
      if([1,2,3,4].includes(Number(msg.subdivision))) room.state.subdivision=Number(msg.subdivision);
      for(const member of room.members) if(member!==c) sendFrame(member,{type:'state',...room.state});
    } else if(msg.type==='transport' && ['start','stop'].includes(msg.action)) {
      const at=msg.action==='start' ? Math.max(Date.now()+250,Math.min(Date.now()+5000,Number(msg.at)||Date.now()+1200)) : Date.now();
      for(const member of room.members) sendFrame(member,{type:'transport',action:msg.action,at});
    }
  }
}
function decodeFrames(c, chunk) {
  c.buffer=Buffer.concat([c.buffer||Buffer.alloc(0),chunk]);
  while(c.buffer.length>=2) {
    const b0=c.buffer[0], b1=c.buffer[1]; let len=b1&127, offset=2;
    if(len===126) { if(c.buffer.length<4)return; len=c.buffer.readUInt16BE(2); offset=4; }
    else if(len===127) { if(c.buffer.length<10)return; const n=c.buffer.readBigUInt64BE(2); if(n>BigInt(1e7)){c.destroy();return;} len=Number(n); offset=10; }
    const masked=!!(b1&128); let mask;
    if(masked){if(c.buffer.length<offset+4)return;mask=c.buffer.subarray(offset,offset+4);offset+=4;}
    if(c.buffer.length<offset+len)return;
    const payload=Buffer.from(c.buffer.subarray(offset,offset+len));c.buffer=c.buffer.subarray(offset+len);
    if(masked) for(let i=0;i<payload.length;i++) payload[i]^=mask[i%4];
    const opcode=b0&15;
    if(opcode===8){c.end();return;} if(opcode===9){c.write(Buffer.from([0x8A,payload.length,...payload]));continue;}
    if(opcode===1) processMessage(c,payload.toString('utf8'));
  }
}
const server=http.createServer((req,res)=>{
  const url=new URL(req.url,'http://localhost'); let pathname=decodeURIComponent(url.pathname);
  if(pathname==='/' || pathname==='/health') { res.writeHead(200,{'Content-Type':'text/plain; charset=utf-8','Cache-Control':'no-store'}); res.end('GB Tempo WebSocket relay is running. Connect clients to /ws.'); return; }
  if(pathname==='/index.html') pathname='/index.html';
  const file=path.resolve(ROOT,'.'+pathname);
  if(!file.startsWith(ROOT+path.sep) && file!==path.join(ROOT,'index.html')) {res.writeHead(403);res.end('Forbidden');return;}
  fs.stat(file,(err,stat)=>{
    if(err||!stat.isFile()){res.writeHead(404,{'Content-Type':'text/plain'});res.end('Not found');return;}
    res.writeHead(200,{'Content-Type':mime[path.extname(file).toLowerCase()]||'application/octet-stream','Cache-Control':'no-cache','X-Content-Type-Options':'nosniff'});
    fs.createReadStream(file).pipe(res);
  });
});
server.on('upgrade',(req,sock)=>{
  if(req.url.split('?')[0]!=='/ws'){sock.destroy();return;}
  const key=req.headers['sec-websocket-key']; if(!key){sock.destroy();return;}
  const accept=crypto.createHash('sha1').update(key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64');
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: '+accept+'\r\n\r\n');
  clients.add(sock);sock.room=null;sock.role='guest';sock.buffer=Buffer.alloc(0);
  sock.on('data',chunk=>decodeFrames(sock,chunk));sock.on('close',()=>removeClient(sock));sock.on('error',()=>removeClient(sock));
});
server.listen(PORT,'0.0.0.0',()=>{
  console.log('\nGB Tempo LAN server is running (internet not required).');
  console.log(`On this computer: http://localhost:${PORT}`);
  const interfaces=os.networkInterfaces();
  for(const entries of Object.values(interfaces)) for(const item of entries||[]) if(item.family==='IPv4'&&!item.internal) console.log(`On phones/tablets using the same Wi-Fi/hotspot: http://${item.address}:${PORT}`);
  console.log('WebSocket endpoint: ws://<server-ip>:'+PORT+'/ws');
  console.log('Keep this terminal open. If prompted, allow Node.js on private networks.\n');
});

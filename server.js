'use strict';
/**
 * AETHERIA ONLINE — リアルタイム同期サーバー (Phase 1: 移動 + チャット)
 *
 * 設計の要点
 * - WebSocket で常時接続。HTTP のリクエスト/レスポンスでは同期しない。
 * - サーバーが「正解」を持つ (authoritative): クライアントは入力だけ送り、
 *   位置はサーバーが計算する。速度の上限をサーバー側で強制するので、
 *   クライアントを改造しても高速移動チートはできない。
 * - Tick は 20Hz (50ms)。そのたびに「近くにいるプレイヤーだけ」の
 *   スナップショットを送る (AOI = Area of Interest)。
 * - クライアント側はスナップショット間を補間して滑らかに描く
 *   (client/aetheria-net.js 参照)。
 *
 * 起動: npm install && npm start   (PORT 環境変数でポート変更可)
 */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 2567;
const TICK_MS = 50;            // 20Hz
const WORLD = { min: -250, max: 250 }; // プロトタイプのマップ範囲に合わせた仮の境界
const MAX_SPEED = 14;          // units/sec (歩行+ダッシュ込みの上限。これを超える入力は丸める)
const AOI_RADIUS = 120;        // この距離内のプレイヤーだけ互いに見える
const CHAT_MAX_LEN = 200;

/** @type {Map<string, object>} sessionId -> player */
const players = new Map();
let nextId = 1;

function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }

function publicState(p) {
  return {
    id: p.id, name: p.name, cls: p.cls,
    x: +p.x.toFixed(2), z: +p.z.toFixed(2), yaw: +p.yaw.toFixed(3),
    hp: p.hp, maxHp: p.maxHp, level: p.level,
  };
}

function broadcast(obj, exceptId = null) {
  const msg = JSON.stringify(obj);
  for (const p of players.values()) {
    if (p.id === exceptId) continue;
    if (p.ws.readyState === 1) p.ws.send(msg);
  }
}

// ---- HTTP (ヘルスチェック用) ----
const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, players: players.size }));
  } else {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('AETHERIA ONLINE realtime server');
  }
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
  const id = 'p' + (nextId++);
  const player = {
    id, ws,
    name: 'Guest', cls: 'warrior',
    x: 0, z: 10, yaw: 0, pitch: 0,
    input: { dx: 0, dz: 0, sprint: false },
    hp: 100, maxHp: 100, level: 1,
    lastSeen: Date.now(),
  };
  players.set(id, player);

  ws.send(JSON.stringify({ type: 'welcome', id, tickMs: TICK_MS, aoi: AOI_RADIUS }));
  broadcast({ type: 'chat', system: true, text: `${player.name} がログインしました` }, id);
  console.log(`[join] ${id} (online: ${players.size})`);

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    player.lastSeen = Date.now();

    switch (m.type) {
      case 'join': // { name, cls } — キャラ作成画面から送る
        player.name = String(m.name || 'Guest').slice(0, 16);
        player.cls = ['warrior', 'mage', 'archer'].includes(m.cls) ? m.cls : 'warrior';
        broadcast({ type: 'chat', system: true, text: `${player.name} がログインしました` }, id);
        break;

      case 'input': // { dx, dz, yaw, pitch, sprint } — 移動入力だけを受け取る
        player.input.dx = clamp(Number(m.dx) || 0, -1, 1);
        player.input.dz = clamp(Number(m.dz) || 0, -1, 1);
        player.input.sprint = !!m.sprint;
        player.yaw = Number(m.yaw) || 0;
        player.pitch = clamp(Number(m.pitch) || 0, -1.4, 1.4);
        break;

      case 'chat': // { text }
        broadcast({
          type: 'chat', from: player.name,
          text: String(m.text || '').slice(0, CHAT_MAX_LEN),
        });
        break;

      case 'attack': {
        // Phase 1 は「近くの敵/プレイヤーへの攻撃イベント」の受け口だけ用意。
        // 本番 Phase 2 では敵AIとダメージ判定をここ (サーバー側) に移す。
        broadcast({ type: 'attack', from: id, target: String(m.target || '') }, id);
        break;
      }

      case 'ping':
        ws.send(JSON.stringify({ type: 'pong', t: m.t }));
        break;
    }
  });

  const leave = () => {
    if (!players.has(id)) return;
    players.delete(id);
    broadcast({ type: 'leave', id });
    broadcast({ type: 'chat', system: true, text: `${player.name} がログアウトしました` });
    console.log(`[leave] ${id} (online: ${players.size})`);
  };
  ws.on('close', leave);
  ws.on('error', leave);
});

// ---- ゲームループ: 入力を積分して位置を更新し、AOI内の姿を配信 ----
setInterval(() => {
  const dt = TICK_MS / 1000;
  for (const p of players.values()) {
    const { dx, dz, sprint } = p.input;
    const len = Math.hypot(dx, dz);
    if (len > 0.01) {
      const speed = Math.min(MAX_SPEED, (sprint ? 13 : 8));
      // 入力はカメラ向き(yaw)基準でクライアントがワールド座標に変換済みとする
      p.x = clamp(p.x + (dx / len) * speed * dt * Math.min(len, 1), WORLD.min, WORLD.max);
      p.z = clamp(p.z + (dz / len) * speed * dt * Math.min(len, 1), WORLD.min, WORLD.max);
    }
  }
  // スナップショット配信 (相手ごとに「見える範囲」だけ送る)
  for (const p of players.values()) {
    if (p.ws.readyState !== 1) continue;
    const nearby = [];
    for (const other of players.values()) {
      if (other.id === p.id) continue;
      if (Math.hypot(other.x - p.x, other.z - p.z) <= AOI_RADIUS) nearby.push(publicState(other));
    }
    p.ws.send(JSON.stringify({ type: 'snapshot', t: Date.now(), you: publicState(p), players: nearby }));
  }
}, TICK_MS);

// ゾンビ接続の掃除
setInterval(() => {
  const now = Date.now();
  for (const p of players.values()) {
    if (now - p.lastSeen > 30000) { try { p.ws.terminate(); } catch {} }
  }
}, 10000);

httpServer.listen(PORT, () => {
  console.log(`AETHERIA realtime server listening on :${PORT} (tick ${TICK_MS}ms, AOI ${AOI_RADIUS})`);
});

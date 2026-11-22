'use strict';
/**
 * AETHERIA ONLINE — リアルタイム同期サーバー (Phase 1.5: ゲーム画面接続版)
 *
 * 設計の要点
 * - WebSocket で常時接続。HTTP のリクエスト/レスポンスでは同期しない。
 * - 座標系はゲーム本体 (64x64 タイルマップ) と同一。位置はタイル座標 x/y。
 * - クライアントが移動を計算し、位置を約10回/秒で申告する (pos)。
 *   サーバーは「あり得る移動か」を検証する: 前回位置からの距離が
 *   経過時間 × 最大速度 (7 タイル/秒 + 余裕) を超える申告は、
 *   進める分だけ進めた位置に丸める (テレポートチート防止)。
 *   初回位置 (join 時) と復活時の teleport フラグ付き申告はそのまま受理する。
 * - Tick は 20Hz (50ms)。そのたびに「近くにいるプレイヤーだけ」の
 *   スナップショットを送る (AOI = 半径 20 タイル)。
 * - チャットは送信者以外全員へ中継する。
 * - 敵AI・戦闘判定はまだクライアント側 (Phase 2 でサーバーへ移す)。
 *
 * 起動: npm install && npm start   (PORT 環境変数でポート変更可)
 * Render では render.yaml / Dockerfile からそのままデプロイできる。
 */
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 2567;
const TICK_MS = 50;            // 20Hz
const WORLD = { min: 0.5, max: 63.5 }; // 64x64 タイルマップの内側
const MAX_SPEED = 7;           // タイル/秒 (最速クラス 3.7 の約2倍まで許容)
const SPEED_SLACK = 1.6;       // タイル (申告間隔の誤差吸収)
const AOI_RADIUS = 20;         // タイル
const CHAT_MAX_LEN = 200;
const SPAWN = { x: 32.5, y: 54.5 }; // ルミナスの町 (ゲーム側 TOWN_SPAWN と同一)
const CLASSES = ['warrior', 'mage', 'archer'];

/** @type {Map<string, object>} sessionId -> player */
const players = new Map();
let nextId = 1;

function clamp(v, min, max) { return Math.max(min, Math.min(max, v)); }
function num(v, fallback) { const n = Number(v); return Number.isFinite(n) ? n : fallback; }

function publicState(p) {
  return {
    id: p.id, name: p.name, cls: p.cls, level: p.level,
    x: +p.x.toFixed(2), y: +p.y.toFixed(2), angle: +p.angle.toFixed(3),
  };
}

function broadcast(obj, exceptId = null) {
  const msg = JSON.stringify(obj);
  for (const p of players.values()) {
    if (p.id === exceptId) continue;
    if (p.ws.readyState === 1) p.ws.send(msg);
  }
}

// ---- HTTP ----
// /health: ヘルスチェック (ゲーム画面からの疎通確認のため CORS を許可)
// / と /game: ゲーム本体 (game.html) を配信する。Cache-Control: no-store で
// 常に最新を返すので、game.html を差し替えて再デプロイすれば、
// プレイヤーはページ再読み込みだけで最新版になる。
const fs = require('fs');
const path = require('path');
const GAME_FILE = path.join(__dirname, 'game.html');
let gameCache = null;
let gameCacheMtime = 0;
function loadGame() {
  try {
    const st = fs.statSync(GAME_FILE);
    if (!gameCache || st.mtimeMs !== gameCacheMtime) {
      gameCache = fs.readFileSync(GAME_FILE);
      gameCacheMtime = st.mtimeMs;
    }
    return gameCache;
  } catch { return null; }
}
const httpServer = http.createServer((req, res) => {
  res.setHeader('Access-Control-Allow-Origin', '*');
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, players: players.size, protocol: 2 }));
  } else if (url === '/' || url === '/game') {
    const html = loadGame();
    if (html) {
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-store, must-revalidate',
      });
      res.end(html);
    } else {
      res.writeHead(503, { 'Content-Type': 'text/plain; charset=utf-8' });
      res.end('game.html not uploaded yet');
    }
  } else {
    res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('not found');
  }
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws) => {
  const id = 'p' + (nextId++);
  const player = {
    id, ws,
    name: 'Guest', cls: 'warrior', level: 1,
    x: SPAWN.x, y: SPAWN.y, angle: -Math.PI / 2,
    lastPosAt: Date.now(),
    lastSeen: Date.now(),
  };
  players.set(id, player);

  ws.send(JSON.stringify({ type: 'welcome', id, tickMs: TICK_MS, aoi: AOI_RADIUS, protocol: 2 }));

  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    player.lastSeen = Date.now();

    switch (m.type) {
      case 'join': { // { name, cls, level, x, y } — キャラ情報と開始位置
        player.name = String(m.name || 'Guest').slice(0, 16);
        player.cls = CLASSES.includes(m.cls) ? m.cls : 'warrior';
        player.level = clamp(Math.floor(num(m.level, 1)) || 1, 1, 99);
        if (m.x !== undefined && m.y !== undefined) {
          player.x = clamp(num(m.x, SPAWN.x), WORLD.min, WORLD.max);
          player.y = clamp(num(m.y, SPAWN.y), WORLD.min, WORLD.max);
        }
        player.lastPosAt = Date.now();
        broadcast({ type: 'chat', system: true, text: `${player.name} がログインしました` }, id);
        console.log(`[join] ${id} ${player.name} (online: ${players.size})`);
        break;
      }

      case 'pos': { // { x, y, angle, teleport? } — 位置申告 (速度検証つき)
        const tx = clamp(num(m.x, player.x), WORLD.min, WORLD.max);
        const ty = clamp(num(m.y, player.y), WORLD.min, WORLD.max);
        const now = Date.now();
        if (m.teleport) {
          player.x = tx; player.y = ty;
        } else {
          const dt = Math.max(0.001, (now - player.lastPosAt) / 1000);
          const allowed = MAX_SPEED * dt + SPEED_SLACK;
          const dx = tx - player.x;
          const dy = ty - player.y;
          const d = Math.hypot(dx, dy);
          if (d <= allowed) { player.x = tx; player.y = ty; }
          else if (d > 0) { player.x += (dx / d) * allowed; player.y += (dy / d) * allowed; }
        }
        player.lastPosAt = now;
        player.angle = num(m.angle, player.angle);
        break;
      }

      case 'chat': // { text } — 送信者以外へ中継 (送信者は自分の画面に即時表示済み)
        broadcast({
          type: 'chat', from: player.name, cls: player.cls, level: player.level,
          text: String(m.text || '').slice(0, CHAT_MAX_LEN),
        }, id);
        break;

      case 'attack': {
        // 戦闘判定は Phase 2 でサーバー側へ移す。今は演出イベントの中継のみ。
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

// ---- 配信ループ: AOI 内のプレイヤーだけスナップショットを送る ----
setInterval(() => {
  for (const p of players.values()) {
    if (p.ws.readyState !== 1) continue;
    const nearby = [];
    for (const other of players.values()) {
      if (other.id === p.id) continue;
      if (Math.hypot(other.x - p.x, other.y - p.y) <= AOI_RADIUS) nearby.push(publicState(other));
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
  console.log(`AETHERIA realtime server listening on :${PORT} (tick ${TICK_MS}ms, AOI ${AOI_RADIUS} tiles, protocol 2)`);
});

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
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 2567;
const TICK_MS = 50;            // 20Hz
const WORLD = { min: 0.5, max: 511.5 }; // 512x512 tile map inner bounds
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

// ---- アカウント & 自動保存 (Supabase を保存先に使う) ----
// 環境変数 SUPABASE_URL と SUPABASE_SERVICE_KEY が設定されている時だけ有効。
// 未設定なら /api/* は 503 を返し、ゲーム側は従来のファイルセーブで動く。
// パスワードは scrypt ハッシュ (salt つき) でだけ保存し、平文は保持しない。
// ログイン状態は DB に保存したセッショントークンで管理するので、
// サーバーが再起動・スリープ復帰してもログインは維持される。
const SUPABASE_URL = (process.env.SUPABASE_URL || '').replace(/\/+$/, '');
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY || '';
const ACCOUNTS_ENABLED = !!(SUPABASE_URL && SUPABASE_SERVICE_KEY);
const SESSION_TTL_MS = 30 * 24 * 60 * 60 * 1000; // 30日
const CHAR_MAX_BYTES = 8 * 1024 * 1024; /* raised 256KB -> 2MB -> 8MB (2026-11-27): a gamemaster save carries worldEdit terrain/building data on top of the character, and oversized saves were rejected — the client then retried silently forever, so the stored character (including its position) never updated and login kept resuming at an old spot. The real gamemaster save outgrew 2MB as well; 8MB leaves ample headroom while still bounding abuse. */
const loginFails = new Map(); // username -> {count, lockedUntil}

async function sb(pathname, { method = 'GET', body = null, prefer = '' } = {}) {
  const headers = {
    apikey: SUPABASE_SERVICE_KEY,
    Authorization: `Bearer ${SUPABASE_SERVICE_KEY}`,
    'Content-Type': 'application/json',
  };
  if (prefer) headers.Prefer = prefer;
  const r = await fetch(`${SUPABASE_URL}/rest/v1/${pathname}`, {
    method, headers, body: body ? JSON.stringify(body) : undefined,
  });
  const text = await r.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { /* ignore */ }
  if (!r.ok) throw new Error(`supabase ${r.status}: ${text.slice(0, 200)}`);
  return data;
}

function scryptHash(password, salt) {
  return new Promise((resolve, reject) => {
    crypto.scrypt(password, salt, 64, (err, key) => err ? reject(err) : resolve(key));
  });
}
async function makePassHash(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key = await scryptHash(password, salt);
  return `${salt}:${key.toString('hex')}`;
}
async function checkPassHash(password, stored) {
  const [salt, hex] = String(stored || '').split(':');
  if (!salt || !hex) return false;
  const key = await scryptHash(password, salt);
  const expected = Buffer.from(hex, 'hex');
  return key.length === expected.length && crypto.timingSafeEqual(key, expected);
}

function sendJson(res, code, obj) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(obj));
}
function readJsonBody(req) {
  return new Promise((resolve) => {
    const chunks = [];
    let size = 0;
    req.on('data', (c) => {
      size += c.length;
      if (size > 10 * 1024 * 1024) { req.destroy(); resolve(null); return; } /* body cap raised 512KB -> 3MB -> 10MB (2026-11-27): the old 512KB cap destroyed save requests mid-flight (clients saw an empty 502), so any character save over ~512KB — e.g. a gamemaster save carrying worldEdit data — could NEVER be stored, and login kept resuming at a stale position. The cap stays above CHAR_MAX_BYTES (8MB) plus the JSON envelope so the 413 check, not a dead connection, is what rejects oversize characters. */
      chunks.push(c);
    });
    req.on('end', () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString('utf8') || '{}')); }
      catch { resolve(null); }
    });
    req.on('error', () => resolve(null));
  });
}

const USERNAME_RE = /^[a-z0-9_]{3,16}$/;
async function findAccountByName(username) {
  const rows = await sb(`accounts?username=eq.${encodeURIComponent(username)}&select=*`);
  return Array.isArray(rows) && rows.length ? rows[0] : null;
}
async function findAccountByToken(token) {
  if (!token) return null;
  const rows = await sb(`accounts?session_token=eq.${encodeURIComponent(token)}&select=*`);
  const acc = Array.isArray(rows) && rows.length ? rows[0] : null;
  if (!acc) return null;
  if (acc.session_expires && new Date(acc.session_expires).getTime() < Date.now()) return null;
  return acc;
}
async function issueSession(acc) {
  const token = crypto.randomBytes(32).toString('hex');
  const expires = new Date(Date.now() + SESSION_TTL_MS).toISOString();
  await sb(`accounts?id=eq.${acc.id}`, {
    method: 'PATCH',
    body: { session_token: token, session_expires: expires, updated_at: new Date().toISOString() },
  });
  return token;
}
function bearerToken(req) {
  const h = String(req.headers.authorization || '');
  return h.startsWith('Bearer ') ? h.slice(7) : '';
}

async function handleApi(req, res, url) {
  if (url === '/api/diag' && req.method === 'GET') {
    // 接続診断: Supabase へ読み取りだけ試し、失敗理由 (HTTP状態と応答本文) を返す。
    // 鍵そのものは返さない。種類の推定に使う接頭辞と長さだけを返す。
    const keyKind = SUPABASE_SERVICE_KEY.startsWith('sb_secret_') ? 'sb_secret'
      : SUPABASE_SERVICE_KEY.startsWith('sb_publishable_') ? 'sb_publishable'
      : SUPABASE_SERVICE_KEY.startsWith('eyJ') ? 'legacy_jwt' : 'unknown';
    const info = {
      enabled: ACCOUNTS_ENABLED,
      urlHost: SUPABASE_URL.replace(/^https?:\/\//, ''),
      keyKind, keyLen: SUPABASE_SERVICE_KEY.length,
    };
    if (!ACCOUNTS_ENABLED) return sendJson(res, 200, { ...info, ok: false, error: 'accounts_not_configured' });
    try {
      await sb('accounts?select=id&limit=1');
      return sendJson(res, 200, { ...info, ok: true });
    } catch (err) {
      return sendJson(res, 200, { ...info, ok: false, error: String(err.message).slice(0, 300) });
    }
  }
  if (!ACCOUNTS_ENABLED) {
    return sendJson(res, 503, { ok: false, error: 'accounts_not_configured' });
  }
  try {
    if (url === '/api/register' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const username = String((body && body.username) || '').trim().toLowerCase();
      const password = String((body && body.password) || '');
      if (!USERNAME_RE.test(username)) return sendJson(res, 400, { ok: false, error: 'bad_username' });
      if (password.length < 6 || password.length > 64) return sendJson(res, 400, { ok: false, error: 'bad_password' });
      if (await findAccountByName(username)) return sendJson(res, 409, { ok: false, error: 'username_taken' });
      const pass_hash = await makePassHash(password);
      const rows = await sb('accounts', {
        method: 'POST', prefer: 'return=representation',
        body: { username, pass_hash },
      });
      const acc = Array.isArray(rows) ? rows[0] : null;
      if (!acc) return sendJson(res, 500, { ok: false, error: 'register_failed' });
      const token = await issueSession(acc);
      return sendJson(res, 200, { ok: true, token, username, character: null });
    }
    if (url === '/api/login' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const username = String((body && body.username) || '').trim().toLowerCase();
      const password = String((body && body.password) || '');
      const fail = loginFails.get(username);
      if (fail && fail.lockedUntil > Date.now()) return sendJson(res, 429, { ok: false, error: 'too_many_attempts' });
      const acc = await findAccountByName(username);
      const ok = acc && await checkPassHash(password, acc.pass_hash);
      if (!ok) {
        const f = fail || { count: 0, lockedUntil: 0 };
        f.count += 1;
        if (f.count >= 10) { f.lockedUntil = Date.now() + 5 * 60 * 1000; f.count = 0; }
        loginFails.set(username, f);
        return sendJson(res, 401, { ok: false, error: 'wrong_credentials' });
      }
      loginFails.delete(username);
      const token = await issueSession(acc);
      return sendJson(res, 200, { ok: true, token, username, character: acc.char_data || null });
    }
    // ここから先はログイン必須
    const acc = await findAccountByToken(bearerToken(req));
    if (!acc) return sendJson(res, 401, { ok: false, error: 'not_logged_in' });
    if (url === '/api/character' && req.method === 'GET') {
      return sendJson(res, 200, { ok: true, username: acc.username, character: acc.char_data || null });
    }
    if (url === '/api/save' && req.method === 'POST') {
      const body = await readJsonBody(req);
      const character = body && body.character;
      if (!character || typeof character !== 'object') return sendJson(res, 400, { ok: false, error: 'bad_character' });
      if (JSON.stringify(character).length > CHAR_MAX_BYTES) return sendJson(res, 413, { ok: false, error: 'character_too_large' });
      await sb(`accounts?id=eq.${acc.id}`, {
        method: 'PATCH',
        body: { char_data: character, updated_at: new Date().toISOString() },
      });
      return sendJson(res, 200, { ok: true });
    }
    if (url === '/api/logout' && req.method === 'POST') {
      await sb(`accounts?id=eq.${acc.id}`, {
        method: 'PATCH',
        body: { session_token: null, session_expires: null, updated_at: new Date().toISOString() },
      });
      return sendJson(res, 200, { ok: true });
    }
    return sendJson(res, 404, { ok: false, error: 'not_found' });
  } catch (err) {
    console.error('[api]', err.message);
    return sendJson(res, 500, { ok: false, error: 'server_error' });
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
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  const url = (req.url || '/').split('?')[0];
  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
  } else if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ ok: true, players: players.size, protocol: 2, accounts: ACCOUNTS_ENABLED ? 'on' : 'off' }));
  } else if (url.startsWith('/api/')) {
    handleApi(req, res, url);
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
          const allowed = (player.name === 'gamemaster' ? MAX_SPEED * 4 : MAX_SPEED) * dt + SPEED_SLACK;
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

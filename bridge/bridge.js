#!/usr/bin/env node
/**
 * Telegram <-> WorkBuddy 桥接
 *
 * 工作方式：
 *   1. 用长轮询（long polling）从 Telegram 取 Jacky 的私聊讯息；
 *   2. 把讯息投给本机 WorkBuddy 网关（POST /api/v1/webhooks/generic），
 *      由真正的 WorkBuddy Agent（带完整工具与记忆）执行；
 *   3. 订阅该次运行的 SSE 流，取回 Agent 的最终回复；
 *   4. 通过 sendMessage 把回复发回 Telegram。
 *
 * 不使用任何第三方模型。只回复 allow_chats 中的会话，绝不主动发起讯息。
 *
 * 【网关端口】WorkBuddy 每次重启，本机网关(SERVER)端口会随机变化。
 *   不写死端口：启动时 + 投递失败时自动定位
 *   （gwport 提示文件 → config.json → 扫本机监听端口逐个探测 /api/v1/health）。
 *
 * 【待补发队列】网关未就绪 / 密钥过期时，讯息不再丢弃，写入 pending.json，
 *   之后每轮循环自动重投，直到成功或超过时限。
 */

'use strict';

const fs = require('fs');
const os = require('os');
const path = require('path');
const { execFileSync } = require('child_process');

const DIR = __dirname;
const CONFIG = JSON.parse(fs.readFileSync(path.join(DIR, 'config.json'), 'utf8'));
const SECRET_FILE = path.join(DIR, 'secret');
const GW_HINT_FILE = path.join(DIR, 'gwport');
const STATE_FILE = path.join(DIR, 'state.json');
const PENDING_FILE = path.join(DIR, 'pending.json');
const LOG_FILE = path.join(DIR, 'bridge.log');
const STOP_FILE = path.join(DIR, 'STOP');

const BOT = CONFIG.bot_token;
const TG = `https://api.telegram.org/bot${BOT}`;
const ALLOW = new Set((CONFIG.allow_chats || []).map(String));
const REPLY_TIMEOUT = CONFIG.reply_timeout_ms || 600000;

// 待补发讯息最多保留 30 分钟、最多尝试 5 次。
// 说明：只有「讯息压根没投进网关」才需要补发；一旦投进（run 已创建）就绝不重投，
// 否则同一句话会在 WorkBuddy 里被处理多次（就是重复讯息的根源）。
const PENDING_MAX_AGE_MS = 30 * 60 * 1000;
const PENDING_MAX_TRIES = 5;
const RATE_LIMIT_BACKOFF_MS = 60 * 1000;
// 网关端口会随 WorkBuddy 开新窗口/重启而变化 —— 每 5 秒自检 + 每次投递前确认，
// 让「端口漂移」对使用者完全无感（等同固定端口）
const GW_CHECK_MS = 5 * 1000;

// 网关地址：启动后由 resolveGateway() 动态确定
let GW = CONFIG.gateway || 'http://127.0.0.1:52116';

// ---------- 基础工具 ----------

function log(...args) {
  const line = `[${new Date().toISOString()}] ${args.join(' ')}`;
  console.log(line);
  try {
    if (fs.existsSync(LOG_FILE) && fs.statSync(LOG_FILE).size > 2 * 1024 * 1024) {
      fs.writeFileSync(LOG_FILE, '');
    }
    fs.appendFileSync(LOG_FILE, line + '\n');
  } catch (_) { /* 日志失败不影响主流程 */ }
}

function readSecret() {
  const fromEnv = process.env.CODEBUDDY_GATEWAY_PASSWORD;
  if (fromEnv && fromEnv.trim()) return fromEnv.trim();
  try {
    return fs.readFileSync(SECRET_FILE, 'utf8').trim();
  } catch (_) {
    return '';
  }
}

function writeSecretFile(s) {
  try { fs.writeFileSync(SECRET_FILE, s, { mode: 0o600 }); } catch (_) {}
}

// 尝试自愈通道密钥。
// 实测：macOS 不允许同用户进程读取别的进程环境变量（ps eww 拿不到 Electron 的 env），
// 而 WorkBuddy 每次启动重生成的网关密码只存在于其进程环境变量里、不落盘 ——
// 所以桥接（launchd 启动）无法自行取回。可行的自愈路径只有一条：
// 先看自身 env（若桥接是被 WorkBuddy 拉起的就有效），否则等每小时守护刷新写回 secret 文件。
// 关键配套：密钥过期期间讯息会一直排队（BAD_SECRET 退避重试，不丢弃），刷新后自动补发。
function tryRefreshSecret() {
  const envSecret = (process.env.CODEBUDDY_GATEWAY_PASSWORD || '').trim();
  if (envSecret) { writeSecretFile(envSecret); return envSecret; }
  return null;
}

function loadState() {
  try {
    return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8'));
  } catch (_) {
    return { last_update_id: 0 };
  }
}

function saveState(s) {
  try {
    fs.writeFileSync(STATE_FILE, JSON.stringify(s, null, 2));
  } catch (e) {
    log('保存状态失败: ' + e.message);
  }
}

function loadPending() {
  try {
    const arr = JSON.parse(fs.readFileSync(PENDING_FILE, 'utf8'));
    return Array.isArray(arr) ? arr : [];
  } catch (_) {
    return [];
  }
}

function savePending(arr) {
  try {
    fs.writeFileSync(PENDING_FILE, JSON.stringify(arr, null, 2));
  } catch (e) {
    log('保存待补发队列失败: ' + e.message);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// ---------- 网关定位 ----------

// 读 gwport 提示文件（桥接自己写入的上次成功端口）
function readGwHint() {
  try {
    const p = fs.readFileSync(GW_HINT_FILE, 'utf8').trim();
    if (/^\d{2,5}$/.test(p)) return `http://127.0.0.1:${p}`;
  } catch (_) { /* 无提示文件 */ }
  return null;
}

// 探测某地址是否为本机网关（/api/v1/health 用密钥返回 200）
async function probeHealth(base) {
  const secret = readSecret();
  if (!secret) return false;
  try {
    const res = await fetch(`${base}/api/v1/health`, {
      headers: { 'x-access-token': secret, 'x-codebuddy-request': '1' },
      signal: AbortSignal.timeout(3000),
    });
    return res.status === 200;
  } catch (_) {
    return false;
  }
}

// 列出本机 127.0.0.1 上处于 LISTEN 的端口
function listLocalPorts() {
  for (const bin of ['/usr/sbin/lsof', '/usr/bin/lsof']) {
    try {
      const out = execFileSync(bin, ['-nP', '-iTCP', '-sTCP:LISTEN'], {
        encoding: 'utf8', timeout: 5000,
      });
      const ports = new Set();
      for (const m of out.matchAll(/127\.0\.0\.1:(\d+)\s*\(LISTEN\)/g)) ports.add(m[1]);
      return [...ports];
    } catch (_) { /* 换下一个 lsof */ }
  }
  return [];
}

// 定位可用网关：提示文件 → 当前 GW → config → 扫描监听端口
async function resolveGateway() {
  // 密钥过期会让所有探测失败；WorkBuddy 重启后先尝试自愈一次
  if (!(await probeHealth(readGwHint() || GW))) tryRefreshSecret();

  const hint = readGwHint();
  if (hint && await probeHealth(hint)) return hint;

  if (await probeHealth(GW)) return GW;

  const cfg = CONFIG.gateway;
  if (cfg && cfg !== GW && await probeHealth(cfg)) return cfg;

  for (const p of listLocalPorts()) {
    const base = `http://127.0.0.1:${p}`;
    if (base === GW || base === hint) continue;
    if (await probeHealth(base)) return base;
  }
  return null;
}

// 先尝试操作，网络类失败则重新定位网关并重试一次
async function withGateway(fn) {
  try {
    return await fn();
  } catch (e) {
    const msg = (e && e.message) || '';
    if (/fetch failed|ECONN|socket|network|aborted|EHOSTUNREACH/i.test(msg)) {
      const found = await resolveGateway();
      if (found && found !== GW) {
        GW = found;
        try { fs.writeFileSync(GW_HINT_FILE, GW.split(':').pop()); } catch (_) {}
        log('网关端口变化，已重新定位到 ' + GW);
        return await fn();
      }
    }
    throw e;
  }
}

// 哪些错误属于「暂时性」——值得排队重投（网关没起来 / 密钥还没刷新 / 限流）
function isTransientErr(e) {
  // 已成功投进网关的错误（delivered）是「终态」，重试只会造成重复，绝不算可重试
  if (e && e.delivered) return false;
  const m = (e && e.message) || '';
  return m === 'BAD_SECRET' || m === 'NO_SECRET' || m === 'RATE_LIMITED' ||
    /fetch failed|ECONN|socket|network|aborted|ETIMEDOUT|ECONNRESET|网关返回 5\d\d/i.test(m);
}

// ---------- Telegram ----------

async function tgGetUpdates(offset, timeoutSec) {
  const url =
    `${TG}/getUpdates?offset=${offset}&timeout=${timeoutSec}` +
    `&allowed_updates=${encodeURIComponent(JSON.stringify(['message']))}`;
  // 长轮询：服务端最多挂 timeoutSec 秒。给足 3 倍余量，避免正常长轮询被判为超时。
  const res = await fetch(url, { signal: AbortSignal.timeout(Math.max(90, timeoutSec * 3) * 1000) });
  const data = await res.json();
  if (!data.ok) {
    const desc = data.description || JSON.stringify(data).slice(0, 160);
    // 409 = 另一个 getUpdates 正在挂起（多是上一轮尚未释放），当瞬时错误处理
    const err = new Error(`getUpdates 失败(${data.error_code}): ${desc}`);
    err.transient = data.error_code === 409 || data.error_code === 429 || data.error_code >= 500;
    throw err;
  }
  return data.result || [];
}

async function tgSend(chatId, text) {
  const body = new URLSearchParams({ chat_id: String(chatId), text });
  const res = await fetch(`${TG}/sendMessage`, {
    method: 'POST',
    body,
    signal: AbortSignal.timeout(30000),
  });
  const data = await res.json();
  if (!data.ok) throw new Error('sendMessage 失败: ' + JSON.stringify(data).slice(0, 200));
  return data.result;
}

// ---------- WorkBuddy 网关 ----------

async function startRun(chatId, userId, text) {
  const secret = readSecret();
  if (!secret) throw new Error('NO_SECRET');
  const payload = {
    id: `tg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    type: 'message',
    source: {
      platform: 'generic',
      sender: { id: String(userId) },
      conversation: { id: String(chatId), type: 'direct' },
    },
    payload: { text },
    timeoutMs: REPLY_TIMEOUT,
  };
  const res = await fetch(`${GW}/api/v1/webhooks/generic`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'x-access-token': secret,
      'x-codebuddy-request': '1',
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(60000),
  });
  const data = await res.json().catch(() => ({}));
  if (res.status === 401) throw new Error('BAD_SECRET');
  if (res.status === 429) throw new Error('RATE_LIMITED');
  if (!res.ok) throw new Error(`网关返回 ${res.status}: ${JSON.stringify(data).slice(0, 200)}`);
  return data.data.runId;
}

/**
 * 订阅本次运行的 SSE 流，返回 Agent 的最终回复文本。
 * 事件形如： event: message \n data: {"status":"completed","content":{"markdown":"..."}}
 */
async function awaitReply(runId) {
  const secret = readSecret();
  const res = await fetch(`${GW}/api/v1/runs/${runId}/stream`, {
    headers: { 'x-access-token': secret, 'x-codebuddy-request': '1' },
    signal: AbortSignal.timeout(REPLY_TIMEOUT + 60000),
  });
  if (!res.ok) throw new Error(`SSE ${res.status}`);

  let buffer = '';
  let finalText = null;
  let lastPartial = '';

  for await (const chunk of res.body) {
    buffer += Buffer.from(chunk).toString('utf8');
    let idx;
    while ((idx = buffer.indexOf('\n')) >= 0) {
      const line = buffer.slice(0, idx).trim();
      buffer = buffer.slice(idx + 1);
      if (!line.startsWith('data:')) continue;
      const raw = line.slice(5).trim();
      if (!raw || raw === '{}') continue;
      let evt;
      try { evt = JSON.parse(raw); } catch (_) { continue; }
      const st = evt.status;
      if (st === 'completed') {
        finalText = (evt.content && (evt.content.markdown || evt.content.text)) || '';
      } else if (st === 'error') {
        throw new Error('Agent 执行失败: ' + JSON.stringify(evt.error || {}).slice(0, 200));
      } else if (st === 'delta' || st === 'chunk') {
        const t = (evt.content && (evt.content.text || evt.content.markdown)) || '';
        if (t) lastPartial = t;
      }
      if (evt.type === 'done') break;
    }
    if (finalText !== null) break;
  }
  if (finalText === null) finalText = lastPartial;
  return (finalText || '').trim();
}

// 投递一条讯息并取回回复（网关不可用时抛错）
async function deliver(chatId, userId, text) {
  // 投递前先确认端口：WorkBuddy 一开新窗口就换端口，此刻必须是最新的
  if (!(await probeHealth(GW))) {
    const found = await resolveGateway();
    if (found && found !== GW) {
      GW = found;
      try { fs.writeFileSync(GW_HINT_FILE, GW.split(':').pop()); } catch (_) {}
      log('投递前重定位网关 -> ' + GW);
    }
  }
  const runId = await withGateway(() => startRun(chatId, userId, text));
  // 关键：startRun 成功 = 讯息已真正进入 WorkBuddy（run 已创建）。
  // 此后若取回复失败，绝不能重投 —— 那会在 WorkBuddy 里产生重复的同一条讯息。
  // 用 delivered 标记把它和「压根没投进去」区分开。
  try {
    return await awaitReply(runId);
  } catch (e) {
    const err = new Error('回复未取回: ' + ((e && e.message) || ''));
    err.delivered = true;
    throw err;
  }
}

// ---------- 主流程 ----------

async function handleMessage(msg) {
  const chatId = msg.chat && msg.chat.id;
  const userId = (msg.from && msg.from.id) || chatId;
  const text = (msg.text || '').trim();
  if (!text) {
    await tgSend(chatId, '（目前只处理文字讯息。）');
    return;
  }
  log(`收到 ${chatId}: ${text.slice(0, 80)}`);

  await tgSend(chatId, '收到，正在处理…');

  try {
    const reply = await deliver(chatId, userId, text);
    if (!reply) {
      await tgSend(chatId, '（处理完成，但这次没有产出文字回复。）');
      return;
    }
    await tgSend(chatId, reply);
    log(`已回复 ${chatId}: ${reply.slice(0, 80)}`);
  } catch (e) {
    if (e.delivered) {
      // 讯息已真正送达并处理，只是回复没取回 —— 绝不重投（会重复）
      log(`已送达但回复未取回: ${text.slice(0, 40)}`);
      await tgSend(chatId, '这条我已经收到并处理了，只是回复在回传时丢了。不用重发，结果我稍后直接推给你。');
    } else if (isTransientErr(e)) {
      // 网关还没就绪：排队，稍后自动补发，绝不丢弃
      pending.push({ chatId, userId, text, at: Date.now(), tries: 1 });
      savePending(pending);
      log(`投递受阻（${e.message}），已排队待补发: ${text.slice(0, 40)}`);
      const why = e.message === 'BAD_SECRET'
        ? '通道密钥刚好过期（WorkBuddy 重启后会重生成，桥接会自动取回）'
        : e.message === 'NO_SECRET'
          ? '通道密钥还没准备好'
          : e.message === 'RATE_LIMITED'
            ? 'WorkBuddy 正在忙（同一时间只跑一个任务；你正在电脑上跟我对话时就会这样）'
            : '本机网关暂时没就绪（WorkBuddy 刚重启或正在启动）';
      await tgSend(chatId, `提醒：${why}。这条已排队，恢复后会自动补发，不用重发。`);
    } else {
      log('处理失败: ' + e.message);
      await tgSend(chatId, '处理出错：' + e.message);
    }
  }
}

// 尝试补发队列中仍未被回复的讯息
async function flushPending() {
  if (!pending.length) return;
  for (const item of pending.slice()) {
    try {
      const reply = await deliver(item.chatId, item.userId, item.text);
      await tgSend(item.chatId, reply || '（补处理完成，本次没有文字回复。）');
      log(`补发成功 ${item.chatId}: ${item.text.slice(0, 40)}`);
      removePending(item);
    } catch (e) {
      if (e.delivered) {
        // 讯息其实早已投进网关（重投只会造成重复），直接出队，不再重试
        log(`补发时确认讯息已送达（回复未取回），出队不再重投: ${item.text.slice(0, 40)}`);
        removePending(item);
        continue;
      }
      item.tries = (item.tries || 0) + 1;
      const tooOld = Date.now() - (item.at || 0) > PENDING_MAX_AGE_MS;
      if (!isTransientErr(e) || tooOld || item.tries > PENDING_MAX_TRIES) {
        log(`放弃补发（${e.message}）: ${item.text.slice(0, 40)}`);
        await tgSend(item.chatId, `这条始终送不出去，先放弃：「${item.text.slice(0, 50)}」（${e.message}）`);
        removePending(item);
      } else {
        log(`补发仍失败（${e.message}），保留待重试: ${item.text.slice(0, 40)}`);
        savePending(pending);
        // 限流（Jack 正占着 Agent）就退避久一点，别把讯息试到放弃
        await sleep(e.message === 'RATE_LIMITED' ? RATE_LIMIT_BACKOFF_MS : 15000);
        return;
      }
    }
  }
}

function removePending(item) {
  const i = pending.indexOf(item);
  if (i >= 0) pending.splice(i, 1);
  savePending(pending);
}

const pending = loadPending();

async function main() {
  // 启动时先定位网关（端口每次重启会变）
  const found = await resolveGateway();
  if (found) {
    GW = found;
    try { fs.writeFileSync(GW_HINT_FILE, GW.split(':').pop()); } catch (_) {}
  } else {
    log('警告：启动时未能定位本机网关，将在收到讯息时重试定位。');
  }
  log('桥接启动。网关=' + GW + '，允许会话=' + [...ALLOW].join(',') +
      (pending.length ? `，待补发 ${pending.length} 条` : ''));

  const state = loadState();
  let offset = Number(state.last_update_id || 0) + 1;
  let lastErrAt = 0;
  let lastGwCheck = 0;

  while (true) {
    if (fs.existsSync(STOP_FILE)) {
      log('检测到 STOP 文件，退出。');
      return;
    }
    try {
      // 周期性自检网关：WorkBuddy 开新窗口就会换端口，不等投递失败先发现它
      if (Date.now() - lastGwCheck > GW_CHECK_MS) {
        lastGwCheck = Date.now();
        if (!(await probeHealth(GW))) {
          const found = await resolveGateway();
          if (found && found !== GW) {
            GW = found;
            try { fs.writeFileSync(GW_HINT_FILE, GW.split(':').pop()); } catch (_) {}
            log('网关端口变化，已重新定位到 ' + GW);
          }
        }
      }

      // 先把积压的补发掉（优先回复旧讯息）
      if (pending.length) {
        await flushPending();
        if (pending.length) { await sleep(10000); continue; }
      }

      const updates = await tgGetUpdates(offset, 25);

      // 关键：先推进并落盘 offset，再处理。避免处理中途重启导致同一条讯息被重复回复。
      // （投递失败会进 pending 队列，所以不会丢。）
      if (updates.length) {
        for (const u of updates) offset = Math.max(offset, Number(u.update_id) + 1);
        saveState({ last_update_id: offset - 1, updated_at: new Date().toISOString() });
      }

      for (const u of updates) {
        const msg = u.message;
        if (!msg) continue;
        const chatId = msg.chat && msg.chat.id;
        if (!ALLOW.has(String(chatId))) {
          log(`忽略非授权会话 ${chatId}`);
          continue;
        }
        if (msg.from && msg.from.is_bot) continue;
        await handleMessage(msg);
      }
    } catch (e) {
      const now = Date.now();
      const msg = (e && e.message) || String(e);
      // 瞬时错误（含长轮询超时）最多每分钟记一条，避免日志刷屏
      const transient = (e && e.transient) || /timeout|aborted|fetch failed|ECONN|socket hang|network/i.test(msg);
      if (!transient || now - lastErrAt > 60000) {
        log('轮询异常: ' + msg);
        lastErrAt = now;
      }
      await sleep(transient ? 2000 : 5000);
    }
  }
}

process.on('unhandledRejection', (e) => log('未捕获拒绝: ' + (e && e.message)));
process.on('uncaughtException', (e) => log('未捕获异常: ' + (e && e.message)));

main();

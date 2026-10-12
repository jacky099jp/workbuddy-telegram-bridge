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
const HEARTBEAT_FILE = path.join(DIR, 'heartbeat');

// 心跳：供 bridge_ctl.py status 判断「主循环是否还活着」。
// 2026-10-11 教训：进程还活着、但主循环卡死时，status 曾经完全看不出问题
// （所有检查项都正常，实际消息已经收不进来）。
function beat(state) {
  try {
    fs.writeFileSync(HEARTBEAT_FILE, JSON.stringify({ t: Date.now(), state }));
  } catch (_) {}
}

const BOT = CONFIG.bot_token;
const TG = `https://api.telegram.org/bot${BOT}`;
const ALLOW = new Set((CONFIG.allow_chats || []).map(String));
const REPLY_TIMEOUT = CONFIG.reply_timeout_ms || 600000;

// ---------- 云端模式（2026-10-11 新增）----------
// 云端小姚持有 Telegram 的 webhook，本机改成「向云端领消息 / 把回复交给云端发」。
// 目的：本机重启换端口、密钥过期都不再影响收信；本机不在线时云端自己顶上。
// 开关在 config.json 的 cloud 字段；关掉即回到直连 getUpdates，可秒回滚。
const CLOUD = CONFIG.cloud || {};
const CLOUD_MODE = !!CLOUD.enabled && !!CLOUD.url;
const CLOUD_URL = String(CLOUD.url || '').replace(/\/+$/, '');
const CLOUD_KEY = CLOUD.key || '';

// Cloudflare 会拦掉默认的机器人 UA（node/python 一律返回 error 1010），
// 所以每次请求云端都必须自带一个明确标识，否则连健康检查都过不去。
function cloudHeaders(extra) {
  return Object.assign({
    'user-agent': 'yao-bridge/1.0',
    'x-yao-key': CLOUD_KEY,
  }, extra || {});
}

// 排队回执节流。
// 2026-10-11 教训：网关未就绪时「静默排队」会让 Jack 完全不知道消息去哪了 ——
// 他只看到自己一条条发出去、没有任何回应，体感就是「tele 挂了」。
// 现在改为：排队时至少回执一次，让他知道消息已被接住；但 3 分钟内最多一条，不刷屏。
const QUEUE_NOTICE_GAP_MS = 3 * 60 * 1000;
let lastQueueNoticeAt = 0;

// 待补发讯息最多保留 30 分钟、最多尝试 5 次。
// 说明：只有「讯息压根没投进网关」才需要补发；一旦投进（run 已创建）就绝不重投，
// 否则同一句话会在 WorkBuddy 里被处理多次（就是重复讯息的根源）。
const PENDING_MAX_AGE_MS = 30 * 60 * 1000;
const PENDING_MAX_TRIES = 30;
// 网关端口漂移的窗口期约 5~15 秒，用短间隔快试，别让讯息卡上几分钟
const PENDING_RETRY_MS = 5000;
const RATE_LIMIT_BACKOFF_MS = 60 * 1000;

// 云端来的消息：本机投递失败时给几个短窗口再试。
// WorkBuddy 常常只是「正忙一下」，几秒后就有空档 —— 能接就本机答（能力全、答案好），
// 实在接不了才放手让云端兜底。总等待必须明显短于云端的兜底时限（60 秒），
// 否则云端兜底先触发，这条又被本机答一次，Jack 会收到两份回答。
const CLOUD_HANDOFF_RETRIES = 2;
const CLOUD_HANDOFF_GAP_MS = 15 * 1000;
// 网关端口会随 WorkBuddy 开新窗口/重启而变化 —— 每 5 秒自检 + 每次投递前确认，
// 让「端口漂移」对使用者完全无感（等同固定端口）
const GW_CHECK_MS = 5 * 1000;

// 网关地址：启动后由 resolveGateway() 动态确定
let GW = CONFIG.gateway || 'http://127.0.0.1:52116';

// ---------- 基础工具 ----------

function log(...args) {
  const line = `[${new Date().toLocaleString('zh-TW', { timeZone: 'Asia/Taipei', hour12: false, year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', second: '2-digit' })}] ${args.join(' ')}`;
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
// 2026-10-12 修正：实测 `ps eww -p <WorkBuddy主进程>` **可以**读到进程环境变量里的
// CODEBUDDY_GATEWAY_PASSWORD（旧注释说读不到是错的，已用 pid 实测验证）。
// 于是 WorkBuddy 重启 → 密钥更换后，桥接能在下一个巡检周期（≤5 秒）自己收割新密钥，
// 不再依赖每小时守护自动化 —— 之前最长 1 小时的空窗由此消除。
function harvestGatewayEnv() {
  try {
    const pids = execFileSync('/usr/bin/pgrep', ['-f', 'WorkBuddy.app/Contents/MacOS/Electron'],
      { encoding: 'utf8', timeout: 8000 }).split(/\s+/).filter((p) => p);
    for (const pid of pids) {
      try {
        const out = execFileSync('/bin/ps', ['eww', '-p', pid], { encoding: 'utf8', timeout: 8000 });
        const sm = out.match(/CODEBUDDY_GATEWAY_PASSWORD=(\S+)/);
        if (!sm) continue;
        const secret = sm[1].replace(/\0/g, '').trim();
        if (!secret) continue;
        const pm = out.match(/SERVER__PORT=(\d{2,5})/);
        return { secret, port: pm ? pm[1] : null };
      } catch (_) { /* 下一个 pid */ }
    }
  } catch (_) { /* pgrep 失败 */ }
  return null;
}

function tryRefreshSecret() {
  const envSecret = (process.env.CODEBUDDY_GATEWAY_PASSWORD || '').trim();
  if (envSecret) { writeSecretFile(envSecret); return envSecret; }
  const h = harvestGatewayEnv();
  if (h && h.secret) {
    let cur = '';
    try { cur = fs.readFileSync(SECRET_FILE, 'utf8').trim(); } catch (_) {}
    if (h.secret !== cur) {
      writeSecretFile(h.secret);
      log('通道密钥已自动从 WorkBuddy 进程收割（长度 ' + h.secret.length + '，与旧的不同）');
      if (h.port) {
        try { fs.writeFileSync(GW_HINT_FILE, h.port); } catch (_) {}
        log('同时取得网关端口提示: ' + h.port);
      }
    }
    return h.secret;
  }
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

// 云端模式：向云端小姚领消息。
// 同一个 bot 不能同时 webhook + 长轮询，所以上云以后收信统一走云端，本机不再直连。
//
// 【重要】领信之前先确认本机管道是通的。
// 反例（2026-10-11 发现的设计缺陷）：电脑开着、但 WorkBuddy 关掉了 ——
// 桥接照样把消息从云端领走，投给本机网关必然失败，消息进 pending 反复重投直到被丢弃；
// 而云端看到消息已被领走（taken），以为本机在处理，就不介入了。
// 结果：Jack 一条回复都收不到。所以管道不通时，宁可什么都不做，让云端顶上。
let lastSkipLogAt = 0;
// 断线感知：gwDownSince（网关开始不通的时刻，0=通）由下方「通道自养」区块统一持有；
// 这里只负责「恢复后通知用户」，与自养共用同一状态，恢复时清零。
let gwNotifyAt = 0;
const RECOVER_NOTIFY_MIN_MS = 60 * 1000;   // 中断超过 60 秒才值得通知
const RECOVER_NOTIFY_GAP_MS = 10 * 60 * 1000; // 恢复通知至少间隔 10 分钟
async function notifyRecoverIfDue() {
  if (!gwDownSince) return;
  const downMs = Date.now() - gwDownSince;
  gwDownSince = 0;
  if (downMs < RECOVER_NOTIFY_MIN_MS || Date.now() - gwNotifyAt < RECOVER_NOTIFY_GAP_MS) return;
  gwNotifyAt = Date.now();
  const mins = Math.round(downMs / 60000 * 10) / 10;
  for (const chatId of ALLOW) {
    try {
      await tgSend(chatId, `✅ 通道已恢复（刚才断了约 ${mins} 分钟，已自动接回）——这条就是验证，有事直接说。`);
    } catch (_) { /* 通知失败不碍主流程 */ }
  }
  log(`通道已恢复（中断 ${mins} 分钟），已通知用户`);
}
// WorkBuddy 是单任务串行：处理一条消息期间，主循环不会再去 /poll。
// 云端 localOnline() 的判据是「90 秒内有没有 poll 过」，所以本机一忙就会被误判成离线，
// 云端随即把排队中的新消息抢去自己答（2026-10-12 实测：14:13:29 那条就是这样被云端答掉的，
// 云端日志原文「代答檢查: 本機離線，改由雲端回答 4」，而本机那一刻正在跑 14:10:50 那条）。
// 处理期间每 25 秒报到一次，让消息老老实实排队等本机，而不是被轻装版抢着答。
let cloudPingTimer = null;
function startCloudPing() {
  if (cloudPingTimer || !CLOUD_URL) return;
  cloudPingTimer = setInterval(() => {
    fetch(`${CLOUD_URL}/ping`, { headers: cloudHeaders(), signal: AbortSignal.timeout(8000) })
      .catch(() => { /* 报到失败不碍事，主流程照跑 */ });
  }, 25000);
}
function stopCloudPing() {
  if (cloudPingTimer) { clearInterval(cloudPingTimer); cloudPingTimer = null; }
}

async function cloudGetUpdates(timeoutSec) {
  if (!(await probeHealth(GW))) {
    if (!gwDownSince) gwDownSince = Date.now();
    if (Date.now() - lastSkipLogAt > 60000) {
      lastSkipLogAt = Date.now();
      log('本机网关不通，本轮不去云端取信（交给云端自己答）');
    }
    await sleep(15000);
    return [];
  }
  await notifyRecoverIfDue();

  let wait = Number(timeoutSec) || 20;
  wait = Math.max(1, Math.min(wait, 25));
  const res = await fetch(`${CLOUD_URL}/poll?wait=${wait}`, {
    headers: cloudHeaders(),
    signal: AbortSignal.timeout(Math.max(60, wait * 3) * 1000),
  });
  let data;
  try {
    data = await res.json();
  } catch (_) {
    const err = new Error(`云端返回非 JSON（HTTP ${res.status}）`);
    err.transient = true;
    throw err;
  }
  if (!data.ok) {
    const err = new Error('云端取信失败: ' + JSON.stringify(data).slice(0, 160));
    err.transient = true;
    throw err;
  }
  if (!data.msg) return [];
  const m = data.msg;
  // 包装成 Telegram update 的形状，主循环后面的处理逻辑一行都不用改
  return [{
    update_id: 0,
    _cloudId: m.id,
    message: {
      message_id: m.id,
      chat: { id: Number(m.chatId) },
      from: { id: Number(m.userId) },
      text: m.text,
    },
  }];
}

// 处理完通知云端删掉这条（成功接手才该删；接不了就交给 /handoff）
async function cloudAck(id) {
  if (!id) return;
  try {
    await fetch(`${CLOUD_URL}/ack?id=${encodeURIComponent(id)}`, {
      method: 'POST',
      headers: cloudHeaders(),
      signal: AbortSignal.timeout(10000),
    });
  } catch (_) {}
}

// 主动交棒：本机明确接不了这条，请云端立刻代答。
// 以前是「什么都不做，等云端超时兜底」，Jack 要白等一分钟，还可能撞出双份回答。
async function cloudHandoff(id) {
  if (!id) return;
  try {
    const res = await fetch(`${CLOUD_URL}/handoff?id=${encodeURIComponent(id)}`, {
      method: 'POST',
      headers: cloudHeaders(),
      signal: AbortSignal.timeout(90000),
    });
    const d = await res.json().catch(() => ({}));
    log('已交棒给云端：' + JSON.stringify(d).slice(0, 120));
  } catch (e) {
    log('交棒请求失败（云端会自行兜底）: ' + ((e && e.message) || e));
  }
}

// 启动时把云端「已被本机取走、但没处理完」的消息放回队列。
// 本机重启 = 上一次处理必然半途而废，不捞回来它会卡在 taken 里干等 45 分钟。
async function cloudRecover() {
  try {
    const res = await fetch(`${CLOUD_URL}/recover`, {
      method: 'POST',
      headers: cloudHeaders(),
      signal: AbortSignal.timeout(15000),
    });
    const d = await res.json();
    if (d.ok && d.recovered) log(`云端捞回 ${d.recovered} 条中断的消息，稍后重投`);
  } catch (e) {
    log('云端捞回失败（不影响启动）: ' + ((e && e.message) || e));
  }
}

async function tgGetUpdates(offset, timeoutSec) {
  if (CLOUD_MODE) return cloudGetUpdates(timeoutSec);
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

// 云端模式：回复也交给云端发（云端持有 bot token，本机不再直接调 Telegram）
async function cloudSend(chatId, text) {
  const res = await fetch(`${CLOUD_URL}/reply`, {
    method: 'POST',
    headers: cloudHeaders({ 'content-type': 'application/json' }),
    body: JSON.stringify({ chat_id: String(chatId), text }),
    signal: AbortSignal.timeout(30000),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok || !data.ok) {
    const err = new Error('云端 sendMessage 失败: ' + JSON.stringify(data).slice(0, 200));
    err.transient = res.status >= 500 || res.status === 429;
    throw err;
  }
  return data.result;
}

async function tgSend(chatId, text) {
  if (CLOUD_MODE) return cloudSend(chatId, text);
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
async function awaitReplyOnce(runId) {
  const secret = readSecret();
  const res = await fetch(`${GW}/api/v1/runs/${runId}/stream`, {
    headers: { 'x-access-token': secret, 'x-codebuddy-request': '1' },
    signal: AbortSignal.timeout(REPLY_TIMEOUT + 60000),
  });
  if (!res.ok) throw new Error(`SSE ${res.status}`);

  let buffer = '';
  let finalText = null;
  let lastPartial = '';

  try {
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
  } catch (e) {
    // 流被中途掐断时，把已经收到的片段带出去，供上层重连或降级使用
    if (lastPartial) e.partial = lastPartial;
    throw e;
  }
  if (finalText === null) finalText = lastPartial;
  return (finalText || '').trim();
}

// 2026-10-12 实测：这条 SSE 流会被中途掐断 —— 日志里所有「已送达但回复未取回」
// 都发生在收信后 302~303 秒，非常规律（空闲约 300 秒即断）。
// 断流时 run 往往还在跑，绝不能就此放弃：重连同一个 runId 继续等。
// 重连是只读、幂等的，不会在 WorkBuddy 里产生重复消息。
const REPLY_STREAM_RETRIES = 3;
const REPLY_STREAM_GAP_MS = 5000;

async function awaitReply(runId) {
  let lastErr = null;
  let bestPartial = '';
  for (let attempt = 1; attempt <= REPLY_STREAM_RETRIES + 1; attempt++) {
    try {
      const text = await awaitReplyOnce(runId);
      if (attempt > 1) log(`重连第 ${attempt - 1} 次后拿到回复（${text.length} 字）`);
      return text;
    } catch (e) {
      lastErr = e;
      const m = (e && e.message) || '';
      if (e.partial && e.partial.length > bestPartial.length) bestPartial = e.partial;
      // Agent 自身报错是终态，重连没有意义
      if (/Agent 执行失败|BAD_SECRET|NO_SECRET/.test(m)) throw e;
      if (attempt <= REPLY_STREAM_RETRIES) {
        log(`回复流中断（第 ${attempt} 次）：${m}；${REPLY_STREAM_GAP_MS / 1000} 秒后重连拉取`);
        const found = await resolveGateway();
        if (found && found !== GW) {
          GW = found;
          try { fs.writeFileSync(GW_HINT_FILE, GW.split(':').pop()); } catch (_) {}
          log('重连前重定位网关 -> ' + GW);
        }
        await sleep(REPLY_STREAM_GAP_MS);
      }
    }
  }
  if (bestPartial) {
    log(`回复流始终未收全，降级发出已收到的片段（${bestPartial.length} 字）`);
    return bestPartial + '\n\n（这条可能没说完，完整的我稍后补给你。）';
  }
  throw lastErr;
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

// 返回 true = 本机已接手（云端可以删记录了）
// 返回 false = 本机接不了（云端请收回兜底，千万别删）
async function handleMessage(msg, cloudId) {
  const chatId = msg.chat && msg.chat.id;
  const userId = (msg.from && msg.from.id) || chatId;
  const text = (msg.text || '').trim();
  if (!text) {
    await tgSend(chatId, '（目前只处理文字讯息。）');
    return true;
  }
  log(`收到 ${chatId}: ${text.slice(0, 80)}`);

  await tgSend(chatId, '收到，正在处理…');

  try {
    const reply = await deliver(chatId, userId, text);
    if (!reply) {
      await tgSend(chatId, '（处理完成，但这次没有产出文字回复。）');
      return true;
    }
    await tgSend(chatId, reply);
    log(`已回复 ${chatId}: ${reply.slice(0, 80)}`);
    return true;
  } catch (e) {
    if (e.delivered) {
      // 讯息已真正送达并处理，只是回复没取回 —— 绝不重投（会重复）
      log(`已送达但回复未取回: ${text.slice(0, 40)}`);
      await tgSend(chatId, '这条我已经收到并处理了，只是回复在回传时丢了。不用重发，结果我稍后直接推给你。');
      return true;
    } else if (isTransientErr(e)) {
      // 云端来的消息，本机接不了时绝不能自己攥着不放：
      // 攥着的话，云端会以为「本机正在处理」而不再介入，Jack 就永远等不到回复。
      // 一律不 ack，让云端 90 秒后收回自己答（2026-10-11 事故根因）。
      if (cloudId) {
        // 先给本机几个短窗口：WorkBuddy 常常只是「正忙一下」，几秒后就有空档。
        // 能接就本机答（能力全、答案质量高）；接不了就主动交棒给云端，别让 Jack 干等。
        for (let i = 1; i <= CLOUD_HANDOFF_RETRIES; i++) {
          await sleep(CLOUD_HANDOFF_GAP_MS);
          try {
            const r2 = await deliver(chatId, userId, text);
            await tgSend(chatId, r2 || '（处理完成，但这次没有产出文字回复。）');
            log(`第 ${i} 次重试成功，由本机回答: ${text.slice(0, 30)}`);
            return true;
          } catch (e2) {
            if (e2.delivered) {
              log(`重试时确认已送达（回复未取回），不再重投: ${text.slice(0, 40)}`);
              await tgSend(chatId, '这条我已经收到并处理了，只是回复在回传时丢了。不用重发，结果我稍后直接推给你。');
              return true;
            }
            if (!isTransientErr(e2)) return true;
            log(`第 ${i} 次重试仍受阻（${e2.message}）: ${text.slice(0, 30)}`);
          }
        }
        log(`本机接不了，主动交棒给云端: ${text.slice(0, 40)}`);
        await cloudHandoff(cloudId);
        return false;
      }
      // 网关还没就绪：排队，稍后自动补发，绝不丢弃。
      // 同一句话 10 分钟内只留一条：他着急时会连发同样的问话，
      // 不去重的话队列里会有多个副本，WorkBuddy 侧就被重复处理、重复回复。
      const dup = pending.find((p) => p.chatId === chatId && p.text === text &&
                                     Date.now() - (p.at || 0) < 10 * 60 * 1000);
      if (dup) {
        log(`同一条消息已在队列中，不重复排队: ${text.slice(0, 40)}`);
      } else {
        pending.push({ chatId, userId, text, at: Date.now(), tries: 1 });
        savePending(pending);
      }
      log(`投递受阻（${e.message}），已排队待补发: ${text.slice(0, 40)}`);
      const why = e.message === 'BAD_SECRET'
        ? '通道密钥刚好过期（WorkBuddy 重启后会重生成，桥接会自动取回）'
        : e.message === 'NO_SECRET'
          ? '通道密钥还没准备好'
          : e.message === 'RATE_LIMITED'
            ? 'WorkBuddy 正在忙（同一时间只跑一个任务；你正在电脑上跟我对话时就会这样）'
            : '本机网关暂时没就绪（WorkBuddy 刚重启或正在启动）';
      // 不再静默排队：让 Jack 知道消息已被接住（3 分钟最多一条，避免刷屏）
      log(`（排队待补发：${why}）`);
      if (Date.now() - lastQueueNoticeAt > QUEUE_NOTICE_GAP_MS) {
        lastQueueNoticeAt = Date.now();
        await tgSend(chatId, e.message === 'RATE_LIMITED'
          ? '这条我先收着。WorkBuddy 正在忙（同一时间只跑一个任务），空出来就处理。'
          : '这条我先收着。本机通道刚好在重启/抖动，稍后自动补处理。');
      }
      return true;
    } else {
      log('处理失败: ' + e.message);
      await tgSend(chatId, '处理出错：' + e.message);
      return true;
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
        // 退避递增：WorkBuddy 忙（RATE_LIMITED）时投得太密，只会在它那边堆出一串
        // 重复 run（2026-10-11 实测 25 秒一次、堆了 3 个、同样的问题刷了三次）。
        // 改成 60s → 120s → …（上限 5 分钟）：既等它空出来，又不重复。
        // 纯端口抖动仍是 5 秒，秒级自愈。
        await sleep(e.message === 'RATE_LIMITED'
          ? Math.min(RATE_LIMIT_BACKOFF_MS * (item.tries || 1), 300 * 1000)
          : PENDING_RETRY_MS);
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

// ---------- 通道自养：WorkBuddy 死了就自己拉起来 ----------
// 设计目标（Jack 2026-10-12 定下）：他只在 tele 喊我、没反应才去动电脑。
// 所以「电脑没人动」时通道必须能自己活。三层自愈：
//   ① 端口漂移 → 5 秒巡检自动重新定位
//   ② 密钥更换 → 从 WorkBuddy 进程环境自动收割
//   ③ WorkBuddy 本体不在 → 直接 open 拉起（本条）
// 连续 90 秒探不到网关才动手，每 3 分钟最多拉起一次，避免刷屏。
let gwDownSince = 0;
let lastWakeAt = 0;

function workbuddyRunning() {
  try {
    const out = execFileSync('/usr/bin/pgrep', ['-f', 'WorkBuddy.app/Contents/MacOS/Electron'],
      { encoding: 'utf8', timeout: 8000 });
    return out.trim().length > 0;
  } catch (_) {
    return false; // pgrep 无匹配 = 没在跑
  }
}

async function tryWakeWorkBuddy() {
  const now = Date.now();
  if (!gwDownSince) { gwDownSince = now; return; }
  if (now - gwDownSince < 90000 || now - lastWakeAt < 180000) return;
  lastWakeAt = now;
  if (workbuddyRunning()) return; // 在跑只是网关没就绪，等它自己起
  try {
    execFileSync('/usr/bin/open', ['-a', 'WorkBuddy'], { timeout: 15000 });
    log('通道自养：WorkBuddy 不在运行，已自动拉起（等待网关就绪）');
  } catch (e) {
    log('通道自养：拉起 WorkBuddy 失败: ' + ((e && e.message) || e));
  }
}

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
      (pending.length ? `，待补发 ${pending.length} 条` : '') +
      (CLOUD_MODE ? '，收信走云端' : ''));

  // 云端模式：本机刚重启，先让云端把上次「取了没处理完」的消息放回队列
  if (CLOUD_MODE) await cloudRecover();

  const state = loadState();
  let offset = Number(state.last_update_id || 0) + 1;
  let lastErrAt = 0;
  let lastGwCheck = 0;
  let flushing = false;          // 补发是否进行中（防并发重入）
  let lastLoopAt = Date.now();   // 看门狗：主循环最后推进时间
  let busySince = 0;             // 正在处理一条消息的起始时间（0 = 空闲）

  // 看门狗：主循环若长时间无推进（网络挂起等），主动退出让 launchd 重新拉起。
  //
  // 关键修正（2026-10-11）：处理消息时主循环是「故意」停住的 —— 桥接一次只办一件事，
  // await handleMessage 期间 lastLoopAt 不会推进。旧写法不区分这两者，于是
  // 「WorkBuddy 回话慢」被误判成「主循环卡死」→ 240 秒就 process.exit。
  // 而那一刻消息往往已经被云端取走（taken），进程一死就等于把消息丢了，
  // 45 分钟后云端才放回队列重投 —— 表现为「同一条消息被回答两次」或「石沉大海」。
  // 现在：处理中一律豁免，只留一条硬上限（回复超时 + 5 分钟）兜底真卡死。
  setInterval(() => {
    if (busySince) {
      if (Date.now() - busySince <= REPLY_TIMEOUT + 300000) return;
      log('看门狗：单条消息处理已超过硬上限，退出进程等待重启。');
      process.exit(1);
    }
    if (Date.now() - lastLoopAt > 240000) {
      log('看门狗：主循环超过 240 秒未推进，退出进程等待 launchd 重启。');
      process.exit(1);
    }
  }, 30000);

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
            gwDownSince = 0;
            try { fs.writeFileSync(GW_HINT_FILE, GW.split(':').pop()); } catch (_) {}
            log('网关端口变化，已重新定位到 ' + GW);
          } else if (!found) {
            await tryWakeWorkBuddy();
          }
        } else {
          gwDownSince = 0;
        }
      }

      // 先把积压的补发掉 —— 但绝不能阻塞接收新讯息。
      // （旧写法 await flushPending() 后若仍有 pending 就 continue，等于整轮跳过
      //  tgGetUpdates：一条卡住的补发会把整条通道堵死。2026-10-11 实测主循环曾因此
      //  静默停摆 4 分钟，新消息全部收不到。）
      if (pending.length && !flushing) {
        flushing = true;
        // 不要用 Promise.race 截断成 10 秒：那会让 flushing 提前复位，
        // 下一轮循环又启动一个 flushPending，同一个 item 被并发重投。
        // 2026-10-11 实测：25 秒投一次，同一条消息在 WorkBuddy 里堆出 3 个重复 run，
        // 刷屏式重投就是这个并发缺陷造成的。退避 sleep 留在 flushPending 内部，
        // 由 flushing 标志保证同一时间只有一个补发流程在跑。
        flushPending()
          .catch((e) => log('补发流程异常: ' + ((e && e.message) || e)))
          .finally(() => { flushing = false; });
      }

      const updates = await tgGetUpdates(offset, 25);

      // 关键：先推进并落盘 offset，再处理。避免处理中途重启导致同一条讯息被重复回复。
      // （投递失败会进 pending 队列，所以不会丢。）
      // 云端模式下 update_id 是云端队列的序号，跟 Telegram 的 offset 无关，
      // 绝不能拿它推进 offset —— 否则切回直连时会错乱，把真实消息挡在门外。
      if (updates.length && !CLOUD_MODE) {
        for (const u of updates) offset = Math.max(offset, Number(u.update_id) + 1);
        saveState({ last_update_id: offset - 1, updated_at: new Date().toISOString() });
      }

      for (const u of updates) {
        const msg = u.message;
        if (!msg) continue;
        const chatId = msg.chat && msg.chat.id;
        if (!ALLOW.has(String(chatId))) {
          log(`忽略非授权会话 ${chatId}`);
          if (u._cloudId) await cloudAck(u._cloudId);
          continue;
        }
        if (msg.from && msg.from.is_bot) continue;
        beat('busy');   // 让体检知道「正在处理消息」，而不是误判成卡死
        busySince = Date.now();
        startCloudPing();   // 处理期间持续向云端报到，别让它误判离线而抢答
        let handled = true;
        try {
          handled = await handleMessage(msg, u._cloudId);
        } finally {
          stopCloudPing();
          busySince = 0;
          // 只有本机确实接手了才 ack。接不了就必须留住这条记录，
          // 让云端 90 秒后收回自己回答；否则两边互相以为对方在处理，Jack 永远等不到回复。
          if (u._cloudId && handled) await cloudAck(u._cloudId);
          else if (u._cloudId) log('本机未接手，保留给云端兜底（不 ack）');
        }
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
    lastLoopAt = Date.now();
    beat('idle');   // 每轮循环末尾刷新心跳
  }
}

process.on('unhandledRejection', (e) => log('未捕获拒绝: ' + (e && e.message)));
process.on('uncaughtException', (e) => log('未捕获异常: ' + (e && e.message)));

main();

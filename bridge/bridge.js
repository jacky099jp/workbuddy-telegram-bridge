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
// 【2026-10-13】群訊息歸檔的「暫停開關」：這個檔案存在時不歸檔（Jack 說「等全進來再記錄」）。
const ARCHIVE_PAUSE = '/Users/macmini/WorkBuddy/Claw/records/.pause-group-archive';
// 成員名冊：userId → 名字（JSON）。有了它，記錄檔就不必只寫一串 id。
const NAME_BOOK = '/Users/macmini/WorkBuddy/Claw/records/成員名冊.json';
const { execFileSync } = require('child_process');

const DIR = __dirname;
const CONFIG = JSON.parse(fs.readFileSync(path.join(DIR, 'config.json'), 'utf8'));
const SECRET_FILE = path.join(DIR, 'secret');
const GW_HINT_FILE = path.join(DIR, 'gwport');
const STATE_FILE = path.join(DIR, 'state.json');
const PENDING_FILE = path.join(DIR, 'pending.json');
// 【2026-10-15】LINE 回覆「掛起」檔：回得太慢、reply token 過期時，把回覆存這裡，
// 等他下次叫我（有新的免費 reply token）再一起送，不為了快而白扣推播額度。
const LINE_HELD_FILE = path.join(DIR, 'line_held.json');
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

// ---------- LINE 通道（2026-10-12 新增）----------
// 你 → LINE → Cloudflare Worker 入队 → 本机这里取信 → 处理 → Worker 推回 LINE。
//
// 设计上刻意与 tele 完全隔离：LINE 那条线单独 try/catch、单独节流日志，
// LINE 崩了、卡了、乱回消息，都伤不到 tele 主循环（Jack 的铁律：通道稳定优先）。
// 配置在 line.json；文件不存在则整条 LINE 功能不启用，不影响任何既有行为。
let LINE_CFG = null;
try {
  LINE_CFG = JSON.parse(fs.readFileSync(path.join(DIR, 'line.json'), 'utf8'));
} catch (_) { LINE_CFG = null; }
const LINE_URL = LINE_CFG ? String(LINE_CFG.worker_url || '').replace(/\/+$/, '') : '';
const LINE_KEY = LINE_CFG ? LINE_CFG.yao_key : '';
const LINE_POLL_MS = 5000;      // 取信间隔
let lastLineLogAt = 0;          // LINE 侧日志节流，避免刷屏淹没 tele 日志

function lineLog(msg) {
  const now = Date.now();
  // 【2026-10-12】诊断类日志（未取回/受阻/失败…）不能被 60 秒节流吃掉，
  // 否则「回复为什么丢」永远查不出。
  if (now - lastLineLogAt < 60000 && !/收到|已回复|未取回|受阻|异常|失败|重试|告警/.test(msg)) return;
  lastLineLogAt = now;
  log('[LINE] ' + msg);
}

// 【2026-10-12 修】原来是把默认头对象和 opts 一起丢给 Object.assign，
// 而 opts 里若有 headers 键，Object.assign 会用 opts.headers **整体替换**掉默认头，
// x-yao-key 就这样被吃掉了。典型症状：/poll 正常（不带 headers）、/reply 必 401
// （带了 content-type），日志里那句「回推未成功: {"ok":false,"error":"unauthorized"}」就是它。
// 现在改成显式分层合并：默认头 ← opts.headers，密钥永远在。
async function lineApi(pathname, opts) {
  const o = opts || {};
  const res = await fetch(LINE_URL + pathname, Object.assign({}, o, {
    headers: Object.assign({
      'x-yao-key': LINE_KEY,
      'user-agent': 'yao-bridge/1.0',
    }, o.headers || {}),
    signal: AbortSignal.timeout(o.timeout || 25000),
  }));
  return res;
}

// ---------- 语音转写（2026-10-12 新增）----------
// 本机常驻 ASR 服务（launchd 标签 com.jacky.asr-server，端口 8791），
// 模型只在服务启动时加载一次，之后每句转写几秒。
// 设计原则同 LINE：语音这条线整体 try/catch，ASR 挂了只回一句提示，绝不带崩通道。
const ASR_URL = process.env.ASR_URL || 'http://127.0.0.1:8791/transcribe';

async function asrTranscribe(buf) {
  try {
    const res = await fetch(ASR_URL, {
      method: 'POST',
      headers: { 'content-type': 'application/octet-stream' },
      body: buf,
      signal: AbortSignal.timeout(120000),
    });
    const d = await res.json().catch(() => ({}));
    if (!d || !d.ok) {
      log(`[ASR] 转写失败: ${JSON.stringify(d).slice(0, 140)}`);
      return '';
    }
    return String(d.text || '').trim();
  } catch (e) {
    log('[ASR] 服务不可达: ' + ((e && e.message) || e));
    return '';
  }
}

// Telegram 语音/音频：getFile → 下载 → 转写
async function asrFromTelegram(fileId) {
  try {
    const r = await fetch(`${TG}/getFile?file_id=${encodeURIComponent(fileId)}`, {
      signal: AbortSignal.timeout(20000),
    });
    const j = await r.json().catch(() => ({}));
    if (!j || !j.ok || !j.result || !j.result.file_path) {
      log('[ASR] Telegram getFile 失败: ' + JSON.stringify(j).slice(0, 140));
      return '';
    }
    const fr = await fetch(`https://api.telegram.org/file/bot${BOT}/${j.result.file_path}`, {
      signal: AbortSignal.timeout(60000),
    });
    if (!fr.ok) { log(`[ASR] 下载语音失败 HTTP ${fr.status}`); return ''; }
    const buf = Buffer.from(await fr.arrayBuffer());
    return await asrTranscribe(buf);
  } catch (e) {
    log('[ASR] 取语音异常: ' + ((e && e.message) || e));
    return '';
  }
}

// LINE 语音：本机没有 Channel Token，由 Worker 代取音频（/audio?id=…）
async function asrFromLine(messageId) {
  try {
    const res = await lineApi('/audio?id=' + encodeURIComponent(messageId), { timeout: 30000 });
    if (!res.ok) { lineLog(`取音频失败 HTTP ${res.status}`); return ''; }
    const buf = Buffer.from(await res.arrayBuffer());
    if (!buf.length) { lineLog('取到空音频'); return ''; }
    return await asrTranscribe(buf);
  } catch (e) {
    lineLog('取音频异常: ' + ((e && e.message) || e));
    return '';
  }
}

// ---------- 圖片 / 檔案落地（2026-10-14 新增）----------
// 多模態：LINE / Telegram 傳來的圖片或檔案，先落到本機，再把路徑丟給 Agent 讀
// （Agent 用 Read 工具就能看圖、看 PDF）。設計原則同語音：這條線整體 try/catch，
// 壞了只回一句提示，絕不帶崩通道。
const MEDIA_DIR = '/Users/macmini/WorkBuddy/Claw/media';
const MEDIA_KEEP_MS = 7 * 24 * 3600 * 1000;   // 媒體保留 7 天，之後自動清掉
try { fs.mkdirSync(MEDIA_DIR, { recursive: true }); } catch (_) {}

const EXT_BY_MIME = {
  'image/jpeg': '.jpg', 'image/jpg': '.jpg', 'image/png': '.png',
  'image/gif': '.gif', 'image/webp': '.webp', 'image/heic': '.heic',
  'application/pdf': '.pdf',
};

function saveMedia(buf, ext, prefix) {
  const name = `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2, 7)}${ext}`;
  const p = path.join(MEDIA_DIR, name);
  fs.writeFileSync(p, buf);
  return p;
}

function pruneMedia() {
  try {
    const now = Date.now();
    for (const f of fs.readdirSync(MEDIA_DIR)) {
      const p = path.join(MEDIA_DIR, f);
      try { if (now - fs.statSync(p).mtimeMs > MEDIA_KEEP_MS) fs.unlinkSync(p); } catch (_) {}
    }
  } catch (_) {}
}
pruneMedia();

// LINE 圖片/檔案：本机没有 Channel Token，由 Worker 代取（/content?id=…）
async function mediaFromLine(messageId, prefix) {
  try {
    const res = await lineApi('/content?id=' + encodeURIComponent(messageId), { timeout: 60000 });
    if (!res.ok) { lineLog(`取媒體失敗 HTTP ${res.status}`); return null; }
    const ct = String(res.headers.get('content-type') || '').split(';')[0].trim();
    const buf = Buffer.from(await res.arrayBuffer());
    if (!buf.length) { lineLog('取到空媒體'); return null; }
    const ext = EXT_BY_MIME[ct] || '.bin';
    return { path: saveMedia(buf, ext, prefix), bytes: buf.length, mime: ct || 'unknown' };
  } catch (e) {
    lineLog('取媒體異常: ' + ((e && e.message) || e));
    return null;
  }
}

// Telegram 圖片/檔案：getFile → 下載 → 落地
async function mediaFromTelegram(fileId, prefix, fallbackExt) {
  try {
    const r = await fetch(`${TG}/getFile?file_id=${encodeURIComponent(fileId)}`, { signal: AbortSignal.timeout(20000) });
    const j = await r.json().catch(() => ({}));
    if (!j || !j.ok || !j.result || !j.result.file_path) {
      log('[MEDIA] Telegram getFile 失败: ' + JSON.stringify(j).slice(0, 140));
      return null;
    }
    const fr = await fetch(`https://api.telegram.org/file/bot${BOT}/${j.result.file_path}`, { signal: AbortSignal.timeout(60000) });
    if (!fr.ok) { log(`[MEDIA] 下載失敗 HTTP ${fr.status}`); return null; }
    const buf = Buffer.from(await fr.arrayBuffer());
    if (!buf.length) { log('[MEDIA] 下載到空檔案'); return null; }
    const m = /\.([a-z0-9]+)$/i.exec(j.result.file_path);
    const ext = m ? '.' + m[1].toLowerCase() : (fallbackExt || '.jpg');
    return { path: saveMedia(buf, ext, prefix), bytes: buf.length, mime: 'unknown' };
  } catch (e) {
    log('[MEDIA] 取媒體異常: ' + ((e && e.message) || e));
    return null;
  }
}

// 交给 Agent 的指令：Agent 用 Read 工具讀這個路徑就能看圖 / 看檔
function mediaInstruction(kind, p, bytes) {
  return (
    `【${kind === 'image' ? '圖片' : '檔案'}】使用者傳來${kind === 'image' ? '一張圖片' : '一個檔案'}，` +
    `已存到本機：${p}（${Math.max(1, Math.round(bytes / 1024))} KB）\n` +
    '請用 Read 工具讀取這個檔案（支援看圖／看 PDF），再根據內容回答使用者。'
  );
}

// 【2026-10-12 补】投递受阻时不能让 Jack 干等 —— 他只会得出「LINE 又挂了」的结论。
// 这就是 17:17~17:20 他连发三条「情況說明」的原因：本机在忙、投递 fetch failed 却零回执，
// 他看不到任何反应，只能一直重发。与 tele 侧排队回执（QUEUE_NOTICE_GAP_MS）同构：
// 3 分钟最多一条；走 Worker /reply 直推（不经过本机网关，所以本机忙时也发得出去）。
// 注意：**不带 item id**，即不 ack —— 消息必须留在队列里等下一轮真的处理。
// 【2026-10-15】Jack 指示：「卡著可以通知 tele」。LINE 一旦卡住（本機忙 / 回得慢），
// 一律改用 tele 通知他 —— tele 是直連 Bot API，不佔 LINE 每月推播額度。
async function tgNotifyJack(text) {
  for (const chatId of ALLOW) {
    try { await tgSend(chatId, text); }
    catch (e) { lineLog('tele 通知失敗: ' + ((e && e.message) || e)); }
  }
}

let lastLineNoticeAt = 0;
const LINE_NOTICE_GAP_MS = 3 * 60 * 1000;
async function lineQueueNotice(target, err, fallbackUserId) {
  if (Date.now() - lastLineNoticeAt < LINE_NOTICE_GAP_MS) return;
  lastLineNoticeAt = Date.now();
  const m = (err && err.message) || String(err || '');
  const why = m === 'RATE_LIMITED'
    ? '我現在在電腦上忙（這台機器同一時間只跑一件事），空出來就處理你這條，不用重發。'
    : '本機通道剛好在重啟或抖動，你這條我先收著了，稍後會自動補處理，不用重發。';
  // 走 tele 通知，不佔 LINE 推播額度（群組一次就 5 則）
  await tgNotifyJack('【LINE 卡住了】' + why);
  lineLog('已發 tele 排隊回執（不佔 LINE 額度）');
}

// 【2026-10-15】Jack 規則：「LINE 如果來不及回應，可以等下次有叫你時一起回應。」
// 回得太慢 → reply token（約 1 分鐘）已過期 → 老做法會退回 push（群組一次扣 5 則）。
// 新做法：不 push，把這條回覆「掛」在 line_held.json，等他下次叫我時（有新的免費 token）合併送出。
const LINE_HELD_TTL_MS = 6 * 60 * 60 * 1000;   // 掛超過 6 小時就作廢，免得回一句過時的話
let lastHeldNotifyAt = 0;                       // tele 通知別洗版（2 分鐘最多一條）
function readHeld() {
  try {
    const o = JSON.parse(fs.readFileSync(LINE_HELD_FILE, 'utf8'));
    return o && typeof o === 'object' && !Array.isArray(o) ? o : {};
  } catch (_) { return {}; }
}
function writeHeld(o) {
  try { fs.writeFileSync(LINE_HELD_FILE, JSON.stringify(o, null, 2)); } catch (_) {}
}
function peekHeld(target) {
  const o = readHeld();
  const e = o[target];
  if (!e) return '';
  if (Date.now() - Number(e.at || 0) > LINE_HELD_TTL_MS) { delete o[target]; writeHeld(o); return ''; }
  return String(e.text || '');
}
function clearHeld(target) {
  const o = readHeld();
  if (o[target]) { delete o[target]; writeHeld(o); }
}
function addHeld(target, text) {
  const o = readHeld();
  const prev = o[target] && o[target].text ? String(o[target].text) + '\n\n' : '';
  let merged = prev + String(text || '');
  if (merged.length > 3000) merged = merged.slice(-3000);   // 別無限長大
  o[target] = { text: merged, at: Date.now() };
  writeHeld(o);
}

// 取一次 LINE 来信并逐条处理完（串行，与 tele 共用一个 Agent）
async function lineTick() {
  if (!LINE_URL || !LINE_KEY) return;
  let items = [];
  try {
    const res = await lineApi('/poll');
    if (!res.ok) { lineLog(`取信失败 HTTP ${res.status}`); return; }
    const data = await res.json().catch(() => ({}));
    items = (data && data.items) || [];
  } catch (e) {
    lineLog('取信异常: ' + ((e && e.message) || e));
    return;
  }

  for (const it of items) {
    let userText = String(it.text || '').trim();

    // 【2026-10-13】来源判定：群组消息不再当私聊处理。
    //   target = 回覆目标（群 → 群 id）；convId = 该会话在 WorkBuddy 里的独立会话键，
    //   让群里的话题与私聊互不干扰。replyToken 走 LINE 免费回覆，不占每月 200 则推播额度。
    const srcType = String(it.srcType || 'user');
    const isGroup = srcType === 'group' || srcType === 'room';
    const target = isGroup ? (String(it.srcId || '') || String(it.userId)) : String(it.userId);
    const tag = isGroup ? (srcType === 'group' ? '[LINE群] ' : '[LINE房] ') : '';
    const convId = isGroup ? `line-${srcType}:` + target : 'line:' + it.userId;
    const replyBody = (text) => JSON.stringify({
      id: it.id, target, fallbackUserId: it.userId, replyToken: it.replyToken || '', text,
    });

    // 语音消息：由 Worker 代取音频 → 本机转文字 → 走与文字完全相同的路径
    if (!userText && it.kind === 'audio' && it.messageId) {
      lineLog(`收到语音 ${it.userId}，正在转文字…`);
      userText = await asrFromLine(it.messageId);
      if (!userText) {
        try {
          await lineApi('/reply', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: replyBody('（这条语音我没听清，请再发一次，或者直接打字。）'),
          });
        } catch (_) {}
        continue;
      }
      lineLog(`语音转写: ${userText.slice(0, 60)}`);
    }

    // 【2026-10-14】圖片 / 檔案：由 Worker 代取內容 → 落地本機 → 把路徑交給 Agent 讀
    if (!userText && (it.kind === 'image' || it.kind === 'file') && it.messageId) {
      const label = it.kind === 'image' ? '圖片' : '檔案';
      lineLog(`收到${label} ${it.userId}，正在取回…`);
      const m = await mediaFromLine(it.messageId, it.kind === 'image' ? 'line-img' : 'line-file');
      if (!m) {
        try {
          await lineApi('/reply', {
            method: 'POST',
            headers: { 'content-type': 'application/json' },
            body: replyBody(`（這${label}我沒取到，麻煩再傳一次。）`),
          });
        } catch (_) {}
        continue;
      }
      userText = mediaInstruction(it.kind, m.path, m.bytes);
      lineLog(`${label}已存檔: ${m.path} (${Math.round(m.bytes / 1024)} KB)`);
    }

    if (!userText) {
      // 贴图 / 贴纸之类没有文本也没有音频：确认掉，别占着队列
      try {
        await lineApi('/reply', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: replyBody('（这条我看不到内容，请用文字或语音。）'),
        });
      } catch (_) {}
      continue;
    }

    log(`${tag}[LINE] 收到 ${it.userId}→${target}: ${userText.slice(0, 80)}`);

    // 【2026-10-13】群訊息自動歸檔：Jack 把這個群當「記錄事情」的通道。
    // 歸檔放在本機這層（而不是只靠我的會話），就算我這邊忙線或出錯，訊息也不會漏記。
    if (isGroup && !fs.existsSync(ARCHIVE_PAUSE)) {
      try {
        const d = new Date();
        const p2 = (n) => String(n).padStart(2, '0');
        const ym = `${d.getFullYear()}-${p2(d.getMonth() + 1)}`;
        const f = path.join('/Users/macmini/WorkBuddy/Claw/records', `LINE群組-${ym}.md`);
        const stamp =
          `${p2(d.getMonth() + 1)}-${p2(d.getDate())} ${p2(d.getHours())}:` +
          `${p2(d.getMinutes())}:${p2(d.getSeconds())}`;
        let who = '成員 ' + String(it.userId).slice(0, 12) + '…';
        try {
          const book = JSON.parse(fs.readFileSync(NAME_BOOK, 'utf8'));
          if (book && book[it.userId]) who = String(book[it.userId]);
        } catch (_) {}
        if (it.userId === 'Uaa393cb8ded525c0c2c0d395393f1aa6') who = 'Jack';
        if (!fs.existsSync(f)) {
          fs.mkdirSync(path.dirname(f), { recursive: true });
          fs.writeFileSync(f, `# LINE 群組紀錄 · ${ym}\n\n`);
        }
        fs.appendFileSync(f, `- ${stamp} ${who}：${userText.replace(/\s*\n\s*/g, ' ')}`);
        fs.appendFileSync(f, '\n');
      } catch (e) {
        lineLog('群訊息歸檔失敗: ' + ((e && e.message) || e));
      }
    }
    enterBusy();
    let reply = '';
    let giveUp = false;
    try {
      reply = await deliver(convId, it.userId, userText);
    } catch (e) {
      if (e && e.delivered) {
        // 已投进 WorkBuddy，绝不重投（会重复）
        // 【2026-10-12】以前这里只发兜底、不记真因，导致「回复为什么没取回」查不出来。
        // 现在把原始错误落进 line 日志，下次一看便知。
        log(`[LINE] 回复未取回，真因：${((e && e.message) || e).slice(0, 220)}`);
        reply = '（这条我已经收到并处理了，只是结果回传时丢了。不用重发，我稍后补给你。）';
      } else {
        // 没投进去：不 ack，留在队列里下一轮再试（Worker 侧也会把 stale 的退回队列）
        lineLog(`处理受阻，留在队列稍后重试: ${((e && e.message) || e).slice(0, 80)}`);
        await lineQueueNotice(target, e, it.userId);   // 至少让他知道「收到了，在排队」，而不是零反应
        giveUp = true;
      }
    } finally {
      exitBusy();
    }
    if (giveUp) continue;

    if (!reply) reply = '（处理完成，但这次没有产出文字回复。）';

    // 【2026-10-13】靜默約定（Jack：群裡只有被叫「小姚」才回應）。
    // 判斷不該插話時把回覆寫成 [靜默]，本機直接吞掉：不發送、不耗額度，訊息仍照常記錄。
    // （Worker /reply 也有同款判斷，兩邊都生效；本機這層是為了不白跑一趟。）
    if (/^\s*\[(靜默|静默|SILENT|silent)\]\s*$/.test(reply)) {
      log(`${tag}[LINE] 靜默不回應 ${target}: ${userText.slice(0, 40)}`);
      try {
        await lineApi('/reply', {
          method: 'POST',
          headers: { 'content-type': 'application/json' },
          body: replyBody(reply),
        });
      } catch (_) {}
      continue;
    }
    // 【2026-10-15】先把上次「掛著」沒送出去的回覆接回來，跟這次合併成一次送。
    const held = peekHeld(target);
    if (held) reply = held + '\n\n' + reply;

    try {
      const res = await lineApi('/reply', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({
          id: it.id, target, fallbackUserId: it.userId,
          replyToken: it.replyToken || '', text: reply,
          hold: true,   // 回得太慢（token 過期）就不要 push，掛著等下次一起回
        }),
      });
      const d = await res.json().catch(() => ({}));
      if (d && d.ok) {
        if (held) clearHeld(target);
        log(`${tag}[LINE] 已回复 ${target}（${d.viaReplyToken ? '免费回覆' : '推播'}）: ${reply.slice(0, 60)}`);
      } else if (d && d.held) {
        addHeld(target, reply);
        log(`${tag}[LINE] 回得慢（token 已過期），先掛著，等你下次叫我一起回：${reply.slice(0, 50)}`);
        // 【2026-10-15】Jack 指示：卡著就通知 tele，內容一併帶上，免得他以為沒回應。
        if (Date.now() - lastHeldNotifyAt > 2 * 60 * 1000) {
          lastHeldNotifyAt = Date.now();
          await tgNotifyJack(
            '【LINE 回得慢，先掛著】你下次在 LINE 叫我時，我會連這條一起回。\n內容：' + reply.slice(0, 300)
          );
        }
      } else {
        addHeld(target, reply);
        lineLog(`回推未成功（已掛著下一輪再送）: ${JSON.stringify(d).slice(0, 120)}`);
      }
    } catch (e) {
      addHeld(target, reply);
      lineLog('回推异常（已掛著下一輪再送）: ' + ((e && e.message) || e));
    }
  }
}

// 【2026-10-12】LINE 取信从 tele 主循环里彻底摘出来了。
// 原来它挂在主循环里，主循环一忙（在跑长任务）取信就跟着停：Worker 侧 lastPoll 冻住、
// 被判 online:false、消息一直积压（实测 16:16:27~16:22:50 卡了 6 分钟、积压 3 条）。
// 现在改用独立定时器，两边互不阻塞。真正共用的只剩「同一台电脑上只有一个 Agent」，
// 那是平台硬限制，绕不过去；但通道本身不会再因为对方忙而饿死。
// 【2026-10-12 补】LINE 侧同款缺陷：一条消息在跑时整轮 lineTick 都忙（lineBusy 直接 early return），
// 期间不会 /poll，而 worker 的 localOnline() 判据是「90 秒内 poll 过」→ 本机被判成离线。
// 后果：Jack 在这期间再发一条消息，worker 会先回他一句「【雲端】本機現在沒接上…已排進隊伍」——
// 他不过是问了一句话，看起来却像通道坏了（实测 17:11 就是这种状态）。
// 处理期间每 25 秒 /ping 报到（只刷新 poll:last，不动队列），与 tele 侧 startCloudPing 同构。
let linePingTimer = null;
function startLinePing() {
  if (linePingTimer || !LINE_URL || !LINE_KEY) return;
  linePingTimer = setInterval(() => {
    lineApi('/ping').catch(() => { /* 报到失败不碍事，主流程照跑 */ });
  }, 25000);
}
function stopLinePing() {
  if (linePingTimer) { clearInterval(linePingTimer); linePingTimer = null; }
}

let lineBusy = false;   // 防重入：上一轮还没处理完就不开下一轮
setInterval(async () => {
  if (!LINE_URL || !LINE_KEY || lineBusy) return;
  lineBusy = true;
  startLinePing();
  try {
    await lineTick();
  } catch (e) {
    lineLog('取信流程异常（已隔离，不影响 tele）: ' + ((e && e.message) || e));
  } finally {
    stopLinePing();
    lineBusy = false;
  }
}, LINE_POLL_MS);

// 排队回执节流。
// 2026-10-11 教训：网关未就绪时「静默排队」会让 Jack 完全不知道消息去哪了 ——
// 他只看到自己一条条发出去、没有任何回应，体感就是「tele 挂了」。
// 现在改为：排队时至少回执一次，让他知道消息已被接住；但 3 分钟内最多一条，不刷屏。
const QUEUE_NOTICE_GAP_MS = 3 * 60 * 1000;
let lastQueueNoticeAt = 0;

// 「正在处理一条消息」的起始时间（0 = 空闲）。
// 必须放在模块级：tele 主循环与 LINE 取信循环都要读写它，看门狗也靠它豁免误判。
let busySince = 0;

// 【2026-10-12】tee 主循环与 LINE 取信循环现在各自独立、可能同时在处理消息。
// 所以「忙」状态必须用**引用计数**，不能用「置 0 清空」：
// 否则先结束的一方会把还在忙的另一方误标成空闲，看护就会把「正在处理」误判成「卡死」。
let busyDepth = 0;
function enterBusy() {
  busyDepth++;
  busySince = Date.now();
  beat('busy');
  startCloudPing();   // 忙时持续向云端报到，别让它误判本机离线而抢答
}
function exitBusy() {
  busyDepth = Math.max(0, busyDepth - 1);
  if (busyDepth === 0) {
    busySince = 0;
    stopCloudPing();
  }
}

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

// 找出「当前活跃会话」的网关端口：取启动时间最新（etimes 最小）的 codebuddy --serve 进程。
// 【为什么需要】WorkBuddy 每开一个新会话就起一个新的 --serve 进程（随机端口），旧的会被回收。
// 桥接扫描端口时可能抓到「探测瞬间还活着、投递时已被回收」的旧端口 —— 这就是丢消息的主因。
// 把 ps 的 etime 转成秒数：'03:12' / '01:02:03' / '2-01:02:03'（macOS 的 ps 没有 etimes）
function parseEtime(s) {
  const m = String(s).trim().match(/^(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)$/);
  if (!m) return null;
  return (parseInt(m[1] || '0', 10) * 86400) + (parseInt(m[2] || '0', 10) * 3600) +
    (parseInt(m[3], 10) * 60) + parseInt(m[4], 10);
}

function newestServeGateway() {
  try {
    const out = execFileSync('/bin/ps', ['-eo', 'pid=,etime=,command='], {
      encoding: 'utf8', timeout: 5000,
    });
    let bestPid = null;
    let bestAge = Infinity;
    for (const line of out.split('\n')) {
      if (!/codebuddy --serve/.test(line) || /--prewarm/.test(line)) continue;
      const m = line.trim().match(/^(\d+)\s+(\S+)\s+(.*)$/);
      if (!m) continue;
      const age = parseEtime(m[2]);
      if (age === null) continue;
      if (age < bestAge) { bestAge = age; bestPid = m[1]; }
    }
    if (!bestPid) return null;
    const lo = execFileSync('/usr/sbin/lsof', ['-nP', '-a', '-p', bestPid, '-iTCP', '-sTCP:LISTEN'], {
      encoding: 'utf8', timeout: 5000,
    });
    const mm = lo.match(/127\.0\.0\.1:(\d+)\s*\(LISTEN\)/);
    return mm ? `http://127.0.0.1:${mm[1]}` : null;
  } catch (_) {
    return null;
  }
}

// 先尝试操作，网络类失败则遍历「所有健康网关」逐个重试。
// 【2026-10-12 加固】原实现只重定位一次，且 resolveGateway() 可能返回同一个刚失效的端口
// （探测时还活着、投递时已死），导致消息被判 fetch failed 卡在队列里反复重投。
// 现在：优先锁定「最新启动的会话进程端口」，再依次尝试 hint / 当前 GW / config / 全部监听端口。
async function withGateway(fn) {
  try {
    return await fn();
  } catch (e) {
    const isNet = (m) => /fetch failed|ECONN|socket|network|aborted|EHOSTUNREACH/i.test(m || '');
    if (!isNet((e && e.message) || '')) throw e;

    const tried = new Set();
    const candidates = [];
    const fresh = newestServeGateway();
    if (fresh) candidates.push(fresh);
    for (const extra of [readGwHint(), GW, CONFIG.gateway]) if (extra) candidates.push(extra);
    for (const p of listLocalPorts()) candidates.push(`http://127.0.0.1:${p}`);

    let hops = 0;
    for (const base of candidates) {
      if (!base || tried.has(base) || base === GW) continue;
      tried.add(base);
      if (!(await probeHealth(base))) continue;
      GW = base;
      try { fs.writeFileSync(GW_HINT_FILE, base.split(':').pop()); } catch (_) {}
      hops++;
      log(`网关端口变化，重定位到 ${base}，重试投递（第 ${hops} 次）`);
      try {
        return await fn();
      } catch (e2) {
        // 非网络类错误（如网关返回业务报错）说明确实投到了网关 —— 直接抛出，别乱换端口
        if (!isNet((e2 && e2.message) || '')) throw e2;
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
  let text = (msg.text || '').trim();

  // 语音 / 音频消息：先转成文字，再走与文字完全相同的处理路径。
  // voice = 按住说话的语音条；audio = 当文件发来的音频；video_note = 圆形视频留言。
  const media = msg.voice || msg.audio || msg.video_note;
  if (!text && media && (media.file_id || media.file_unique_id)) {
    const dur = media.duration ? `（${media.duration}s）` : '';
    await tgSend(chatId, `收到语音${dur}，正在转成文字…`);
    text = await asrFromTelegram(media.file_id || media.file_unique_id);
    if (!text) {
      await tgSend(chatId, '这条语音我没听清，麻烦再发一次，或者直接打字。');
      return true;
    }
    log(`语音转写: ${text.slice(0, 80)}`);
  }

  // 【2026-10-14】圖片 / 檔案：下載落地，交給 Agent 讀（多模態）
  if (!text) {
    let media = null;
    let kind = '';
    if (Array.isArray(msg.photo) && msg.photo.length) {
      media = msg.photo[msg.photo.length - 1];   // photo 是多种尺寸，取最大那张
      kind = 'image';
    } else if (msg.document) {
      media = msg.document;
      kind = String(msg.document.mime_type || '').startsWith('image/') ? 'image' : 'file';
    }
    if (media && media.file_id) {
      const label = kind === 'image' ? '圖片' : '檔案';
      await tgSend(chatId, `收到${label}，正在處理…`);
      const m = await mediaFromTelegram(
        media.file_id,
        kind === 'image' ? 'tg-img' : 'tg-file',
        kind === 'image' ? '.jpg' : '.bin'
      );
      if (!m) {
        await tgSend(chatId, `這${label}我沒取到，麻煩再傳一次。`);
        return true;
      }
      text = mediaInstruction(kind, m.path, m.bytes);
      log(`${label}已存檔: ${m.path} (${Math.round(m.bytes / 1024)} KB)`);
    }
  }

  if (!text) {
    await tgSend(chatId, '（目前只处理文字、语音、图片和档案讯息。）');
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
      // 【2026-10-12】补上真因，否则永远不知道是超时、断流还是 Agent 自身报错。
      log(`已送达但回复未取回（真因: ${((e && e.message) || e).slice(0, 180)}）: ${text.slice(0, 40)}`);
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
              log(`重试时确认已送达（回复未取回，真因: ${((e2 && e2.message) || e2).slice(0, 180)}），不再重投: ${text.slice(0, 40)}`);
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
  // busySince 已提到模块级（tele 与 LINE 两条线共用）
  busySince = 0;
  let lastLineTick = 0;          // LINE 取信节流计时

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

      // LINE 取信已移出主循环，改由文件末尾的独立定时器驱动（见 lineTick 之后）。
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
        enterBusy();   // 让体检知道「正在处理消息」，而不是误判成卡死
        let handled = true;
        try {
          handled = await handleMessage(msg, u._cloudId);
        } finally {
          exitBusy();
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

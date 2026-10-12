---
name: telegram-bridge
description: |
  管理 Jack 与 WorkBuddy 之间的 Telegram（简称 tele）双向通道。用于：
  ① 把 WorkBuddy 的产出、进度或提醒推送到 Jack 的 tele（直连 Bot API，秒达）；
  ② 体检和修复本机 tele 桥接（"tele 又没回"、"通道断了"、"tele 稳定吗"、
     "怎么不秒回"、"桥接挂了"、"端口"、"密钥"、"刷密钥"）；
  ③ 重新安装/启停开机自启的桥接守护进程；
  ④ 任何需要主动通知 Jack 的任务（定时日报、长任务完成提醒、异常告警）。
  关键词：tele、Telegram、通道、桥接、bridge、推送、发给我、通知我、
  刷新密钥、端口漂移、防睡眠。
agent_created: true
---

# telegram-bridge

管理 WorkBuddy ↔ tele 的双向通道。**通道由两段组成，故障排查时先分清是哪一段：**

```
手机 tele ──①──> 腾讯服务器 ──②──> 本机桥接进程(bridge.js) ──③──> 本机网关 ──④──> Agent(我)
                                                                                      │
手机 tele <──⑦── 腾讯服务器 <──⑥── bridge.js 发回 <──────────────────────────────────⑤
```

- **收（tele → 我）**：由常驻进程 `bridge.js` 负责，靠 `launchd` 保活。这一段出问题表现为
  「我在 tele 发消息没回应」。
- **发（我 → tele）**：由本 skill 的 `tg_send.py` 直连 Telegram Bot API 完成。
  **不要走本机网关转发** —— 网关投递会受 WorkBuddy 单任务限制阻塞。

## 硬规则（务必遵守）

1. **绝不主动给非授权会话发消息。** 只能发到桥接 `config.json` 的 `allow_chats` 里的会话。
2. **不要回复 tele 上用户发来的消息。** 那是 `bridge.js` 的职责，重复回复会导致用户收到两条。
3. **不要读写桥接目录下的 `state.json`**（那是桥接的长轮询游标，手改会导致消息重复或漏收）。
   `bridge.log` 只读，不要清空。
4. **不要用 `rm` 删除桥接目录里任何文件。**
5. 发消息给 Jack 时用**简体中文书面语**，不要用粤语口语（唔/係/嘅/咗），不要夹英文。

## 场景 A：把内容推送到 tele

```bash
T=~/.workbuddy/skills/telegram-bridge/scripts/tg_send.py

python3 $T "文字内容"
python3 $T --title "每日 AI 新闻" "正文……"
python3 $T --file /path/to/report.md          # 从文件读
echo "内容" | python3 $T                       # 管道
python3 $T --silent "夜间通知，不响铃"
python3 $T --dry-run "先预览不真发"
```

脚本自动处理：**超长分段**（Telegram 单条上限 4096 字符，按段落切并加 `(1/3)` 标记）、
**429 限流退避重试**、**网络错误重试 3 次**。成功时输出 `已送达 tele（…message_id=…）`。

需要排版时加 `--html`（脚本会自动转义 `< > &`，不会因格式错误发送失败）。
**不要用 MarkdownV2** —— 转义规则太容易踩坑。

## 场景 B：体检通道

**只要 Jack 说「tele 没回」「tele 稳定吗」「怎么又不回」，第一件事就是跑这个：**

```bash
python3 ~/.workbuddy/skills/telegram-bridge/scripts/bridge_ctl.py status
```

输出涵盖：桥接进程与存活时长、开机自启状态、本机网关健康度（会**自动重新定位端口**）、
通道密钥是否与环境一致、防睡眠是否生效、待补发队列条数、**主循环心跳**、日志尾部、结论。

结论行会直接列出「需要处理」的项；全绿则显示「结论：全链路正常」。

**主循环心跳**（2026-10-11 新增）：桥接每轮循环往 `heartbeat` 写一次
`{"t": 时间戳, "state": "idle"|"busy"}`。专门用来发现「进程活着、主循环却卡死」——
这种故障下其余检查项全是绿的，但消息已经收不进来。判定口径：

- `（正常）` → 空闲且心跳在 90 秒内
- `正在处理消息（已 N 秒）` → 正在跑一条消息，不算故障（直到超过 `reply_timeout_ms`）
- `★ 疑似卡死` → 空闲但心跳超过 90 秒 → **跑 `refresh`**

桥接自身另有一道看门狗：主循环若 240 秒没推进就主动退出，由 `launchd`
自动拉起，无需人工干预。

## 场景 C：修复

按 `status` 报出的问题对症下药：

| 症状 | 命令 | 说明 |
|---|---|---|
| 密钥标记「需刷新」 | `bridge_ctl.py refresh` | **必须在 WorkBuddy 会话内跑**才能读到新密钥 |
| 网关不通、端口漂移 | `bridge_ctl.py refresh` | 会自动扫描本机监听端口重新定位 |
| 桥接进程没在跑 | `bridge_ctl.py start` | 会先清掉 STOP 文件再拉起 |
| 开机自启丢失 | `bridge_ctl.py install` | 重写 `launchd` plist 并加载（桥接 + 防睡眠） |
| 要彻底停掉 | `bridge_ctl.py stop` | 写 STOP 文件，桥接会自行退出 |
| 看详细日志 | `bridge_ctl.py logs 80` | 桥接日志 + launchd 错误日志 |
| 队列积压 | `bridge_ctl.py pending` / `pending clear` | 查 / 清待补发队列 |
| 主循环疑似卡死 | `bridge_ctl.py refresh` | 心跳超 90 秒无动静；重启桥接即可恢复 |
| 会话卡在「等高危命令批准」 | 自动（看护会直连 tele 提醒） | 无提示死锁：通道全堵，只有点掉批准框才能恢复 |

**WorkBuddy 重启后的标准动作**：跑一次 `bridge_ctl.py refresh`，把新生成的网关密钥写回
`secret` 文件，否则最长有 1 小时窗口 tele 的消息只能排队。
（2026-10-11 起**看护已能自动做这件事**：最长 5 分钟自愈，无需人工；手动跑只是更快。）

## 场景 C+：看护（cn.yaowork.bridge-guard）

`scripts/bridge_guard.py` 由 launchd 每 **5 分钟**拉起一次，跑完即退出（不常驻，避免自身卡死）。
它**独立于 WorkBuddy Agent** —— Agent 忙或卡住时，Agent 自己也跑不了定时任务，看护必须站在 Agent 之外。

做五件事：

1. 桥接进程掉线 → 自动 `bridge_ctl.py start`
2. 主循环心跳停摆（idle 且超 150 秒）→ 自动 `bridge_ctl.py refresh`
3. **网关密钥与 WorkBuddy 当前密钥不一致 → 自动 refresh 写回**（见下）
4. **有会话卡在「等高危命令批准」→ 直连 tele 提醒 Jack 去点批准框**（同一会话只提醒一次，恢复后可再提醒）
5. 网关不通 / 待补发积压超 5 分钟 → 直连 tele 告警（同类 1 小时最多一条，静音送达）

**第 3 条是 2026-10-11 新增的自动收割**：`refresh` 依赖环境变量 `CODEBUDDY_GATEWAY_PASSWORD`，
而看护是 launchd 拉起的、环境里没有这个值 —— 所以以前 WorkBuddy 一重启，看护也修不了，
只能干等 Jack 在会话里说句话。现在看护会直接 `ps eww` 去 WorkBuddy 主进程的环境里把密钥捞出来
（实测可读），补进子进程环境再跑 refresh，于是**开机后能自愈，不需要人工介入**。
捞不到就静默跳过，维持旧行为，绝不报错刷屏。密钥只在比对时使用，不写日志。

```bash
python3 ~/.workbuddy/skills/telegram-bridge/scripts/bridge_guard.py --dry-run   # 只判断，不修复不发消息
```

日志 `~/.workbuddy/skills/telegram-bridge/guard.log`；状态 `guard_state.json`。
卸载：`launchctl bootout gui/$(id -u)/cn.yaowork.bridge-guard && rm ~/Library/LaunchAgents/cn.yaowork.bridge-guard.plist`

## 场景 D：让任务主动通知 Jack

在任务收尾时用场景 A 的脚本推送结果。定时任务（自动化）里也一样，直接调用绝对路径。

## 场景 E：云端小姚（Cloudflare Worker）

手机侧的收信入口在云端，云端当「总机」：本机在线就转给本机（能力全），本机离线就自己用 AI 答。
地址：`https://yao-cloud.jacky099jp.workers.dev`（Cloudflare 免费额度内，0 元）

**一个控制台管全部**（切错了也能秒回滚）：

```bash
python3 scripts/cloud_ctl.py status        # 体检：云端队列 / webhook / 本机是否在线
python3 scripts/cloud_ctl.py on            # 切到云端模式
python3 scripts/cloud_ctl.py off           # 回滚到直连模式（上云前的样子）
python3 scripts/cloud_ctl.py test "你好"    # 让云端模型直接答一句
python3 scripts/cloud_ctl.py deploy        # 改完 cloud/src/index.js 后重新部署
python3 scripts/cloud_ctl.py info          # 看当前配置（不连网）
```

**硬规则**

- 同一个 bot **不能**同时 webhook + 长轮询 → 云端模式必须停用 `getUpdates`；`config.json` 的 `cloud.enabled` 是总开关。
- 访问 `*.workers.dev` 的请求**必须自带 user-agent**，否则被 Cloudflare 拦成 403（error 1010）。默认的 Python-urllib 必被拦，curl/node 默认 UA 反而能过。
- Workers AI 的模型名会下线，别写死单个模型 —— 用 `cloud/src/index.js` 里的 `CANDIDATE_MODELS` 候选链依次尝试。
- `wrangler login` 的 OAuth 回调只等 2 分钟，超时得重来；用 `open -a "Google Chrome"` 明确拉 Chrome。
- 桥接**领信前必须先确认本机网关是通的**：电脑开着但 WorkBuddy 关掉时，若照样把消息从云端领走，投递必然失败、消息进 pending 反复重投后被丢弃，而云端以为「本机在处理」不会介入 —— 结果一条回复都收不到。

**接不了必须交棒（2026-10-11 事故，务必牢记）**

这是 Jack 反复「消息没回音」的真正根因，链路是这样叠死的：

1. 手机发消息 → 云端收下 → 本机抢先领走（WorkBuddy 网关端口活着，健康检查过得去）
2. 本机把消息投给 WorkBuddy → Jack 正在电脑上对话 / WorkBuddy 半天 → **被拒（RATE_LIMITED）**
3. 本机把消息塞进自己的 pending 等重投，**却没告诉云端**
4. 云端一看「消息已被领走」，判定本机在处理 → 不介入
5. → 两边互相等，Jack 什么都收不到

三道防线（缺一不可）：

- **本机侧**：`handleMessage(msg, cloudId)` 返回 `true/false`。返回 `false` → 主循环**不 ack**，
  并调 `/handoff` 主动交棒。云端来的消息**先重试 2 次（间隔 15 秒）**，本机只是「忙一下」时仍由本机答。
- **云端侧**：`/handoff?id=` → 立即用 AI 代答（不等超时，避免和本机重试撞车）。
- **保险**：`/poll` 里同步跑 `reviveStale()`（节流 15 秒），把 `taken` 超 90 秒仍未 ack 的收回云端答。
  ⚠️ **不要用 `ctx.waitUntil()`** —— 实测根本没跑起来，兜底必须挂在主链路上。
  ⚠️ 收回时状态改成 `cloud`（`/take` 只取 `new`），否则本机会再抢一遍，形成「抢→失败→放回→再抢」死循环。
  ⚠️ 本机重试总时长（2×15 秒）必须明显短于 `STALE_MS`（90 秒），否则双份回答。

**验证手法**（没人手也能自测）：伪造 Telegram update 直接打 `/tg`，header 带
`x-telegram-bot-api-secret-token`（值在 `cloud/.secrets.json` 的 `TG_HOOK_SECRET`），
再看本机日志是否出现「第 N 次重试仍受阻」→「本机接不了，主动交棒给云端」→「已交棒给云端：{"ok":true,"answered":N}」，
以及 `/queue` 是否清空。手动救火：`POST /revive`（带 `x-yao-key`）立刻收回所有卡住的消息。

**手机为主的三件套（2026-10-11 按 Jack 实际用法新增）**

Jack 明确说过：**他主要用手机，几乎不用电脑**（除非这边没回应才去开电脑查）。
所以一切优化以手机侧为先：

1. **手机发「状态 / 狀態 / 体检 / 體檢」→ 秒回体检报告**
   `handleWebhook()` 里关键词前置拦截（**不进队列、不走 AI**）+ `buildStatusText()`。
   报告含：本机在线否（多少秒前取的信）、积压条数、最近 3 条。
   → 让他不必为了查状态而开电脑。
2. **本机掉线 → 云端主动喊人**
   `wrangler.toml` 的 `[triggers] crons = ["*/10 * * * *"]` + `scheduled()` + `checkLocalAlive()`。
   20 分钟没取信 = 离线 → 发 tele 提醒他开 WorkBuddy；10 分钟内取信 = 已恢复（清 `offline_alerted`）；
   **同类提醒 4 小时冷却**（别刷屏）。DO 侧新增 `/setmeta` 批量写 meta。
3. **本机优先接手机消息**：他不碰电脑时电脑是空的 → 本机直接接（能力全）。实测 19 秒接走。
   这才是他该有的常态 —— 判据是他不在电脑前开对话。

**开机自启（2026-10-11 已办好）**：登录项里 **QClaw 已取消**，换成 **WorkBuddy**
（`/Applications/WorkBuddy.app`，hidden=false）；pCloud Drive 保留不动。
配合看护的密钥自动收割，开机后本机小姚会自己回来并自动接上 tele，**不需要人工 refresh**。
QClaw.app 仍留在 `/Applications`（只是不再自启，没卸载）。
改登录项的命令：
```bash
osascript -e 'tell application "System Events" to delete login item "QClaw"'
osascript -e 'tell application "System Events" to make login item at end with properties {path:"/Applications/WorkBuddy.app", hidden:false}'
osascript -e 'tell application "System Events" to get the name of every login item'   # 复查
```


**云端不可达怎么办**：看护每 5 分钟探一次，持续 ≥10 分钟自动 `cloud_ctl.py off` 切回直连并告警。
（云端挂 = Telegram 入口挂 = 彻底失联，所以必须有这条兜底；降级之后告警才发得出去。）

**项目位置**：`~/WorkBuddy/<时间戳>/telegram-bridge/cloud/`（`wrangler.toml` + `src/index.js`）

## 环境依赖

- 脚本用系统 `python3`，只用标准库（urllib / json / subprocess），**无需装任何包**。
- 桥接目录通过 `TELEGRAM_BRIDGE_DIR` 环境变量、skill 内的 `bridge_dir` 记录文件、
  或扫描 `~/WorkBuddy/*/telegram-bridge/config.json` 依次定位，**不写死路径**。

## 场景 F：开机自检（line + tele）

**Jack 的要求（2026-10-12）：「以後重新開機後先測試 line 和 tele 是否正常」。**

开机登录后 75 秒，launchd 自动跑 `boot_check.py --notify`，把结果同时推到 tele 与 LINE。
三路各探一次，并把结论**实发**出去（不是只打印）：

```bash
python3 ~/.workbuddy/skills/telegram-bridge/scripts/boot_check.py --notify    # 体检 + 双通道推送
python3 ~/.workbuddy/skills/telegram-bridge/scripts/boot_check.py             # 只体检，不发送
python3 ~/.workbuddy/skills/telegram-bridge/scripts/boot_check.py --wait 60   # 先等 60 秒再查
python3 ~/.workbuddy/skills/telegram-bridge/scripts/boot_check.py --no-fix    # 只诊断，不自动修
```

| 检查项 | 判据 | 自愈动作 |
|---|---|---|
| tele | 桥接进程在、网关 200、密钥长度、主循环心跳新鲜度 | 网关端口重定位；进程不在则 kickstart |
| LINE | worker `/status` 200、本机取信 `lastPoll` 在 60 秒内、`hasToken` | 取信循环停摆 → kickstart 桥接，等 12 秒复查 |
| 云端中继 | `http://127.0.0.1:8787/health` 的 yao-hub ok | 隧道进程不在 → kickstart `com.jacky.yao-tunnel` |

- 退出码 0 = line 与 tele 都通；1 = 有未通项。
- 最后一次结果落盘 `logs/boot-check-last.json`（含每项证据数字），做汇报时直接读它。
- 开机自启：`~/Library/LaunchAgents/com.jacky.boot-check.plist`（RunAtLoad，`--notify --wait 75`）；
  日志 `logs/boot-check.log`。卸载：`launchctl bootout gui/$(id -u)/com.jacky.boot-check`。
- ⚠️ 两个坑：① 访问 LINE worker 必须带自定义 User-Agent，`Python-urllib` 会被 Cloudflare 403（error 1010）；
  ② launchd 用户级 agent 在**登录之后**才跑，若重启后停在登录界面则不会自检。
- LINE 通知对象取自 `line.json` 的 `owner_uid`（Jack 的 LINE userId）。

## 深入资料

排障细节、历史踩过的坑与原理（端口漂移、密钥 1 小时盲区、RATE_LIMITED 排队、
Mac 睡眠、长任务截断），见 `references/troubleshooting.md`。

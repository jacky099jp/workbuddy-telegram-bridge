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
通道密钥是否与环境一致、防睡眠是否生效、待补发队列条数、日志尾部、结论。

结论行会直接列出「需要处理」的项；全绿则显示「结论：全链路正常」。

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

**WorkBuddy 重启后的标准动作**：跑一次 `bridge_ctl.py refresh`，把新生成的网关密钥写回
`secret` 文件，否则最长有 1 小时窗口 tele 的消息只能排队。

## 场景 D：让任务主动通知 Jack

在任务收尾时用场景 A 的脚本推送结果。定时任务（自动化）里也一样，直接调用绝对路径。

## 环境依赖

- 脚本用系统 `python3`，只用标准库（urllib / json / subprocess），**无需装任何包**。
- 桥接目录通过 `TELEGRAM_BRIDGE_DIR` 环境变量、skill 内的 `bridge_dir` 记录文件、
  或扫描 `~/WorkBuddy/*/telegram-bridge/config.json` 依次定位，**不写死路径**。

## 深入资料

排障细节、历史踩过的坑与原理（端口漂移、密钥 1 小时盲区、RATE_LIMITED 排队、
Mac 睡眠、长任务截断），见 `references/troubleshooting.md`。

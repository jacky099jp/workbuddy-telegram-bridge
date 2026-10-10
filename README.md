# workbuddy-telegram-bridge

让 [WorkBuddy](https://www.workbuddy.cn) 在 Telegram 上「活过来」：你在 Telegram 私聊里发讯息，**由 WorkBuddy 本人（带完整工具与记忆的 Agent）回复**，不经过任何第三方模型。

## 它是怎么跑的

```
Telegram 私聊
  → bridge.js（本机常驻，长轮询 getUpdates）
  → POST http://127.0.0.1:<gateway>/api/v1/webhooks/generic   （WorkBuddy 本机网关）
  → WorkBuddy Agent 真正执行（可查资料、读写文件、跑命令）
  → GET /api/v1/runs/:runId/stream（SSE）取回最终回复
  → Telegram sendMessage 把回复发回你
```

反方向（Agent 主动推消息给你）走 `skill/scripts/tg_send.py`，直连 Telegram Bot API，不经过网关，秒达。

## 特性

- **零依赖**：`bridge.js` 只用 Node 内置模块；脚本只用 Python 标准库。
- **只回白名单**：仅回复 `config.json` 里 `allow_chats` 指定的会话，绝不主动发起讯息。
- **端口自愈**：WorkBuddy 每次重启本机网关端口都会变，桥接会自动扫描本机端口、用密钥探活、重新定位。
- **待补发队列**：投递失败的消息落盘排队，网关恢复后自动补发，不丢消息。
- **开机自启 + 防睡眠**：附 `launchd` 模板，登录即拉起，并阻止系统睡眠导致断线。

## 环境要求

- macOS（用到 `launchd`；Linux 可自行改用 systemd）
- Node.js ≥ 18
- Python 3（脚本只用标准库，无需 pip 安装任何包）
- 一个 Telegram Bot（找 [@BotFather](https://t.me/BotFather) 建，拿 `bot_token`）
- 你的 Telegram chat id（可从 `https://api.telegram.org/bot<token>/getUpdates` 看到）

## 快速开始

```bash
git clone https://github.com/jacky099jp/workbuddy-telegram-bridge.git
cd workbuddy-telegram-bridge

# 1) 准备配置
cd bridge
cp config.example.json config.json
# 编辑 config.json，填上 bot_token 与 allow_chats

# 2) 启动桥接
python3 start_bridge.py

# 3) 查看日志
tail -f bridge.log
```

然后把 `skill/` 整个目录放到 `~/.workbuddy/skills/telegram-bridge/`，就能在 WorkBuddy 里用下面这些命令。

## 常用命令（skill）

```bash
S=~/.workbuddy/skills/telegram-bridge/scripts

python3 $S/bridge_ctl.py status      # 体检：进程、网关、密钥、队列
python3 $S/bridge_ctl.py refresh     # 刷新网关密钥并重新定位端口（WorkBuddy 重启后必跑）
python3 $S/bridge_ctl.py start       # 拉起桥接
python3 $S/bridge_ctl.py logs 80     # 看日志
python3 $S/bridge_ctl.py pending     # 查待补发队列
python3 $S/bridge_ctl.py pending clear

python3 $S/tg_send.py "要让 Agent 主动推给你的文字"   # 直连 Telegram 推送
```

## 目录结构

```
bridge/
  bridge.js                 # 桥接主程序（Node，无依赖）
  start_bridge.py           # 以脱离方式启动 bridge.js
  config.example.json       # 配置模板（复制成 config.json 使用）
skill/
  SKILL.md                  # WorkBuddy 技能说明
  references/troubleshooting.md
  scripts/tg_send.py        # 主动推送
  scripts/bridge_ctl.py     # 体检 / 修复
  scripts/_common.py        # 公共函数
```

## 已知限制

- **慢回合的回答可能丢失**：桥接订阅 Agent 回复有超时（默认 30 分钟，实测可能几分钟就断）。若 Agent 那一轮耗时过长，回复会取不回，消息转入待补发队列。缓解办法：把 `reply_timeout_ms` 调大，或让 Agent 尽量简短作答。
- **单 Agent 限制**：你正在 WorkBuddy 窗口里对话时，Telegram 消息会排队，不会秒回。
- **WorkBuddy 密钥轮换**：每次重启网关密钥都会重生成，需跑一次 `refresh`，否则最长有 1 小时窗口消息只能排队。

## 许可

MIT，见 [LICENSE](LICENSE)。

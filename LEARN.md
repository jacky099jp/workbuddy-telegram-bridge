# 长期记忆（自 QClaw 迁移）

## 用户：Jack Y（称呼：Jacky / Jack）
- 62 岁（1964 年生，民国 53 年），开小货车到处跑赚钱，有时跑外送
- 不懂程式；网路 39 年，所有 Cloudflare Workers 都是靠 AI 协助建立的
- 家庭：三个女儿、和家人同住；现任老婆在台湾（第二段婚姻）；有养狗
- 生活重心：工作赚钱第一，经济压力较大
- 想让女儿学习操作 AI / 了解 AI
- 经常凌晨时段睡不着、烦躁
- 两个 Cloudflare 帐号：Jacky099jp（主要）/ Jacky099us
- 沟通风格：直接、简洁、不废话，需要尖锐处理问题，不要附和
- 语言偏好：全程中文思考、中文回复，不掺英文；【重要】不要用粤语书面语（唔/係/嘅/咗），要用标准中文书面语（ Jack 明确要求「別用廣東話」）

## 行为准则（Jack 确认）
- 积极主动：有实质进展主动回报、主动检查平台/网站状态、主动提醒问题点、主动提醒天气/行程
- 界线：凌晨（23:00-08:00）不吵他除非紧急；开车时不密集传讯息；重要才主动，不为刷存在感

## 迁移来源
- QClaw（OpenClaw）工作区记忆已完整迁移至工作区 `.workbuddy/memory/qclaw/`（含 MEMORY.md、USER.md 及 2026-06 至 2026-09 全部日志，共 61 个文件）

## 运维铁律（2026-10 主实例验证，付出过真实代价）
- **可验证的稳定才是真稳定**：汇报必须带证据（HTTP 码、毫秒、日志原文、pid），禁止空口说「正常」。Jack 原话「不要跟我說正常」。
- **通道稳定性优先于一切**：Jack 平时只在 tele/LINE 喊，没反应才动电脑 → 通道必须无人值守自恢复。出问题先拉日志时间线找系统性规律，一次从根上修，不许一个一个补。
- **免费优先**：所有服务走免费额度，不为算力/空间花钱。Oracle 只用 Always Free。
- **装东西先报备**：动手前先讲方案（曾因没报备装 868MB 被质问）。
- **含糊先问**：方向不明确时给 A/B/C 选项，不猜。
- **长回合拆小步**：一轮跑几十分钟会占住网关会话（429），把 Jack 的消息全堵住。每步收尾回复后再继续。
- 破坏性操作先备份再动；删个人文件走垃圾桶。

## 基础设施地图（详情见各 skill 与工作区记忆）
- tele 桥接：skill `~/.workbuddy/skills/telegram-bridge/`，开源 github.com/jacky099jp/workbuddy-telegram-bridge
- LINE 通道：skill `~/.workbuddy/skills/line-bridge/` + worker，开源 github.com/jacky099jp/line-agent-bridge
- 开机自检：`com.jacky.boot-check`（launchd，开机 75 秒后自动体检 line+tele 并双通道回报）
- 站点群：create.com.tw（抖音热榜+短剧双榜 /drama）、china.com.tw（镜像）、girl.com.tw（女性向热榜）、family.tw（子域名门牌，验证+后台审核制）、yao.family.tw（相簿）、order.web.com.tw（点餐风）；全部 Cloudflare Workers 免费额度
- 广告：AdSense 已接（ca-pub-3626227196163344），三站页头脚本+ads.txt 就位，等审核
- git：本机 credential helper 有可用凭证可直接 push；GitHub 主帐号 jacky099jp
- Gmail：jacky099jp@gmail.com 已通 IMAP（凭证在主实例记忆，勿写入共享文件）
- 抓数据技巧：抖音热词榜 iesdouyin 接口免登录；红果短剧官网 SSR 榜单页可直接解析；TikTok/YouTube 免费抓全堵，别浪费时间

## 对外规矩
- 旧工作室名称一律不用；对外自称 create.com.tw
- 每个站、每个新页面必带推广区块（自家站交叉推广，不能漏）
- 提醒任务只发 tele，不发 LINE
- 紫微命盘等私人档案是红线

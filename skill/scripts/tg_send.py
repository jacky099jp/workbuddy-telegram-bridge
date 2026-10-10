#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""WorkBuddy → tele 单向推送（直连 Telegram Bot API）。

为什么直连而不走本机网关：
  走网关的投递会被 WorkBuddy 的单任务限制挡住（Jack 正用 WorkBuddy 对话时必然排队）。
  直连 Telegram 不受影响，永远秒达。

用法：
  python3 tg_send.py "文字"                 # 直接发
  python3 tg_send.py --stdin                # 从管道读
  python3 tg_send.py --file report.md       # 从文件读
  echo "内容" | python3 tg_send.py          # 管道简写
  python3 tg_send.py --title "体检报告" "正文"
  python3 tg_send.py --silent "夜间通知"
  python3 tg_send.py --dry-run "先看看"

参数：
  --title T     在正文前加一行标题
  --chat-id ID  指定接收会话（默认取桥接 config 的 allow_chats 第一个）
  --silent      静音送达（不响铃）
  --no-chunk    超长不自动分段（Telegram 会拒绝，仅调试用）
  --html        按 HTML 解析（本脚本会转义，安全）
  --dry-run     只打印将发送的内容，不真发

退出码：0 成功；1 失败。
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _common as C  # noqa: E402


def read_input(args):
    if args.get("stdin"):
        return sys.stdin.read()
    if args.get("file"):
        with open(os.path.expanduser(args["file"]), "r", encoding="utf-8") as fh:
            return fh.read()
    if args.get("text"):
        return args["text"]
    if not sys.stdin.isatty():
        return sys.stdin.read()
    return ""


def parse_args(argv):
    a = {"stdin": False, "silent": False, "dry_run": False,
         "html": False, "no_chunk": False, "title": None,
         "file": None, "chat_id": None, "text": None}
    i = 0
    while i < len(argv):
        tok = argv[i]
        if tok == "--stdin":
            a["stdin"] = True
        elif tok == "--silent":
            a["silent"] = True
        elif tok == "--dry-run":
            a["dry_run"] = True
        elif tok == "--html":
            a["html"] = True
        elif tok == "--no-chunk":
            a["no_chunk"] = True
        elif tok in ("--title", "--file", "--chat-id"):
            i += 1
            if i >= len(argv):
                C.die("参数 %s 缺少值" % tok)
            a[tok.lstrip("-").replace("-", "_")] = argv[i]
        elif tok in ("-h", "--help"):
            print(__doc__)
            sys.exit(0)
        elif tok.startswith("--"):
            C.die("未知参数：%s（用 --help 看用法）" % tok)
        else:
            a["text"] = (a["text"] + "\n" + tok) if a["text"] else tok
        i += 1
    return a


def main():
    a = parse_args(sys.argv[1:])
    body = read_input(a).strip()
    if not body:
        C.die("没有内容可发。（用 --help 看用法）")

    text = body
    if a["title"]:
        text = "%s\n\n%s" % (a["title"], body)

    bridge = C.find_bridge_dir()
    C.remember_bridge_dir(bridge)
    cfg = C.load_config(bridge)
    token = cfg.get("bot_token")
    if not token:
        C.die("桥接 config.json 里没有 bot_token")

    chat_id = a["chat_id"] or (cfg.get("allow_chats") or [None])[0]
    if not chat_id:
        C.die("没有可用的 chat_id（config.json 的 allow_chats 为空）")

    parts = [text] if a["no_chunk"] else C.chunk_text(text)

    if a["dry_run"]:
        C.ok("[dry-run] 目标 chat_id=%s，共 %d 条，%d 字符" % (chat_id, len(parts), len(text)))
        for p in parts:
            C.ok("----\n" + p)
        return

    sent = []
    for p in parts:
        payload = {
            "chat_id": str(chat_id),
            "text": C.html_escape(p) if a["html"] else p,
            "disable_notification": "true" if a["silent"] else "false",
        }
        if a["html"]:
            payload["parse_mode"] = "HTML"
        res = C.tg_call(token, "sendMessage", payload)
        sent.append(res.get("message_id"))

    C.ok("已送达 tele（chat_id=%s，%d 条，message_id=%s）"
         % (chat_id, len(sent), ",".join(str(x) for x in sent)))


if __name__ == "__main__":
    C.main_guard()
    try:
        main()
    except C.TgError as e:
        C.die("发送失败：%s" % e)
    except KeyboardInterrupt:
        C.die("已中断")

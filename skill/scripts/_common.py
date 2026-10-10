#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""telegram-bridge skill 共用工具。

职责：
  1. 定位本机 Telegram 桥接目录（token / chat_id / 网关密钥的唯一真相来源）；
  2. 封装 Telegram Bot API 调用（含 429 退避重试）；
  3. 长文本分段。

设计原则：
  - 桥接目录不写死。Workspace 目录名带时间戳、会变，所以按优先级动态解析。
  - Telegram 凭据只从桥接 config.json 读，skill 自己不另存一份，避免两处不一致。
"""

import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HOME = os.path.expanduser("~")
SKILL_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SKILL_CONFIG = os.path.join(SKILL_DIR, "bridge_dir")

# Telegram 单条讯息上限 4096 字符，留余量给分段标记
CHUNK_LIMIT = 3800


class TgError(Exception):
    pass


# ---------------------------------------------------------------- 路径解析

def _bridge_dir_candidates():
    """按可靠性从高到低给出候选桥接目录。"""
    env = os.environ.get("TELEGRAM_BRIDGE_DIR", "").strip()
    if env:
        yield os.path.expanduser(env)

    # skill 自己记录的路径（install / refresh 成功后写入）
    try:
        with open(SKILL_CONFIG, "r", encoding="utf-8") as fh:
            p = fh.read().strip()
        if p:
            yield os.path.expanduser(p)
    except OSError:
        pass

    # 全局扫描：任何 WorkBuddy 工作区下的 telegram-bridge
    root = os.path.join(HOME, "WorkBuddy")
    if os.path.isdir(root):
        found = []
        for name in os.listdir(root):
            cand = os.path.join(root, name, "telegram-bridge")
            cfg = os.path.join(cand, "config.json")
            if os.path.isfile(cfg):
                try:
                    found.append((os.path.getmtime(cfg), cand))
                except OSError:
                    pass
        # config.json 越新越可能是当前在用的那一个
        for _, cand in sorted(found, reverse=True):
            yield cand


def find_bridge_dir(required=True):
    """找到第一个含 config.json 的桥接目录。"""
    for cand in _bridge_dir_candidates():
        if cand and os.path.isfile(os.path.join(cand, "config.json")):
            return cand
    if required:
        die("找不到桥接目录（没有任何 telegram-bridge/config.json）。\n"
            "如果桥接还没部署，先执行： python3 bridge_ctl.py install")
    return None


def remember_bridge_dir(path):
    """把解析到的桥接目录写进 skill，供下次直接命中。"""
    try:
        with open(SKILL_CONFIG, "w", encoding="utf-8") as fh:
            fh.write(path)
    except OSError:
        pass


def load_config(bridge_dir):
    with open(os.path.join(bridge_dir, "config.json"), "r", encoding="utf-8") as fh:
        return json.load(fh)


def load_secret(bridge_dir):
    """网关密钥：优先环境变量（WorkBuddy 会话内），其次 secret 文件。"""
    env = (os.environ.get("CODEBUDDY_GATEWAY_PASSWORD") or "").strip()
    if env:
        return env
    try:
        with open(os.path.join(bridge_dir, "secret"), "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return ""


def read_gwport(bridge_dir):
    try:
        with open(os.path.join(bridge_dir, "gwport"), "r", encoding="utf-8") as fh:
            return fh.read().strip()
    except OSError:
        return ""


# ---------------------------------------------------------------- Telegram

def tg_call(token, method, params, timeout=30, retries=3):
    """调用 Telegram Bot API，遇 429 / 5xx / 网络错误自动退避重试。"""
    url = "https://api.telegram.org/bot%s/%s" % (token, method)
    data = urllib.parse.urlencode(params).encode("utf-8")
    last = None
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, data=data)
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                body = json.load(resp)
            if body.get("ok"):
                return body.get("result")
            # 429：按 Telegram 指定的秒数等待
            if body.get("error_code") == 429:
                wait = (body.get("parameters") or {}).get("retry_after", 3)
                last = TgError("429 限流，等待 %ss" % wait)
                time.sleep(min(int(wait) + 1, 30))
                continue
            raise TgError("Telegram 返回错误：%s" % json.dumps(body, ensure_ascii=False)[:200])
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = e.read().decode("utf-8", "ignore")[:200]
            except Exception:
                pass
            last = TgError("HTTP %s %s" % (e.code, detail))
            if e.code in (429, 500, 502, 503, 504):
                time.sleep(2 * (attempt + 1))
                continue
            raise last
        except Exception as e:  # 网络类
            last = TgError("网络错误：%s" % e)
            time.sleep(2 * (attempt + 1))
    raise last or TgError("未知错误")


def chunk_text(text, limit=CHUNK_LIMIT):
    """按段落优先、其次硬切，把长文分段（Telegram 单条上限 4096）。"""
    text = text or ""
    if len(text) <= limit:
        return [text]
    parts = []
    rest = text
    while len(rest) > limit:
        window = rest[:limit]
        cut = window.rfind("\n\n")
        if cut < limit // 3:
            cut = window.rfind("\n")
        if cut < limit // 3:
            cut = limit
        parts.append(rest[:cut].rstrip())
        rest = rest[cut:].lstrip()
    if rest:
        parts.append(rest)
    total = len(parts)
    if total == 1:
        return parts
    return ["(%d/%d)\n%s" % (i + 1, total, p) for i, p in enumerate(parts)]


def html_escape(text):
    return (text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


# ---------------------------------------------------------------- 小工具

def die(msg, code=1):
    sys.stderr.write(msg.rstrip() + "\n")
    sys.exit(code)


def ok(msg):
    sys.stdout.write(msg.rstrip() + "\n")


def http_get(url, headers=None, timeout=5):
    req = urllib.request.Request(url, headers=headers or {})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.status, resp.read().decode("utf-8", "ignore")


def gateway_health(bridge_dir, port=None):
    """探测本机网关是否就绪，返回 (是否健康, 端口)。"""
    port = port or read_gwport(bridge_dir)
    secret = load_secret(bridge_dir)
    if not port or not secret:
        return False, port
    url = "http://127.0.0.1:%s/api/v1/health" % port
    try:
        status, _ = http_get(url, {
            "x-access-token": secret,
            "x-codebuddy-request": "1",
        }, timeout=4)
        return status == 200, port
    except Exception:
        return False, port


def scan_gateway_port(bridge_dir):
    """扫描本机 127.0.0.1 监听端口，找出真正的网关。"""
    import subprocess
    try:
        out = subprocess.run(["/usr/sbin/lsof", "-nP", "-iTCP", "-sTCP:LISTEN"],
                             capture_output=True, text=True, timeout=8).stdout
    except Exception:
        return None
    ports = sorted(set(re.findall(r"127\.0\.0\.1:(\d+)\s*\(LISTEN\)", out)),
                   key=lambda x: int(x))
    for p in ports:
        healthy, _ = gateway_health(bridge_dir, port=p)
        if healthy:
            return p
    return None


def main_guard():
    """给各脚本统一入口用的最小依赖检查。"""
    if sys.version_info < (3, 6):
        die("需要 Python 3.6+")


if __name__ == "__main__":
    d = find_bridge_dir(required=False)
    print("桥接目录：", d or "（未找到）")
    if d:
        cfg = load_config(d)
        print("网关：", cfg.get("gateway"))
        print("允许会话：", cfg.get("allow_chats"))
        print("密钥长度：", len(load_secret(d)))
        print("网关健康：", gateway_health(d))

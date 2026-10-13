#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""开机/重启后自检：tele + LINE + 云端中继。

背景（Jack 的要求，2026-10-12）：
  「以後重新開機後先測試 line 和 tele 是否正常」
  Mac 重启后三条链路都靠 launchd 自启，但「进程起来了」不等于「可用」：
  网关端口会漂、密钥会换、worker 可能被限流、SSH 隧道可能没握上手。
  所以开机后必须真探一次，并把结果主动推给他看（可验证，不空口说正常）。

用法：
  python3 boot_check.py                # 只体检，结果打到 stdout
  python3 boot_check.py --notify       # 体检 + 结果推 tele 与 LINE
  python3 boot_check.py --wait 75      # 先等 75 秒（等网络与守护进程就绪）
  python3 boot_check.py --json         # 输出机器可读结果
  python3 boot_check.py --no-fix       # 只诊断，不尝试自愈

退出码：0 全通；1 有失败项。
"""

import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _common as C  # noqa: E402

SKILL_DIR = C.SKILL_DIR
LOG_DIR = os.path.join(SKILL_DIR, "logs")
LAST_JSON = os.path.join(LOG_DIR, "boot-check-last.json")

BRIDGE_LABEL = "cn.yaowork.telegram-bridge"
TUNNEL_LABEL = "com.jacky.yao-tunnel"
HUB_URL = "http://127.0.0.1:8787"      # SSH 隧道 → Oracle 大阪 yao-hub
HEARTBEAT_STALE = 120                  # 心跳超过这么久没动 → 视为主循环卡死
LINE_POLL_STALE = 60                   # worker 侧 lastPoll 超过这么久没更新 → 本机取信循环没在跑

_results = {}


def sh(cmd, timeout=20):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except Exception as e:
        return 999, "", str(e)


UA = "yao-boot-check/1.0 (+mac-mini)"   # 必须自定义：Cloudflare 会拦 Python-urllib 默认 UA（error 1010 → 403）


def http_json(url, headers=None, timeout=8, method="GET", body=None):
    """返回 (status_code, dict_or_text)，异常返回 (0, 错误字符串)。"""
    data = None
    hdrs = {"User-Agent": UA}
    hdrs.update(headers or {})
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        hdrs["content-type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=hdrs, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", "ignore")
            try:
                return resp.status, json.loads(raw)
            except ValueError:
                return resp.status, raw
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode("utf-8", "ignore"))
        except Exception:
            return e.code, "HTTP %s" % e.code
    except Exception as e:
        return 0, str(e)


# ---------------------------------------------------------------- tele 段

def check_tele(bridge, fix=True):
    """桥接进程 + 网关 + 心跳，必要时自愈。"""
    r = {"name": "tele", "ok": False, "notes": [], "fixed": [], "evidence": {}}

    # 1) 桥接进程
    rc, out, _ = sh(["/usr/bin/pgrep", "-f", "telegram-bridge/bridge.js"])
    pids = [p for p in out.split("\n") if p.strip()]
    if not pids:
        r["notes"].append("桥接进程不在")
        if fix:
            sh(["/bin/launchctl", "kickstart", "-k", "gui/%d/%s" % (os.getuid(), BRIDGE_LABEL)])
            time.sleep(6)
            rc, out, _ = sh(["/usr/bin/pgrep", "-f", "telegram-bridge/bridge.js"])
            pids = [p for p in out.split("\n") if p.strip()]
            if pids:
                r["fixed"].append("已拉起桥接进程")
    pid = pids[0] if pids else None
    r["evidence"]["pid"] = pid
    if pid:
        rc, out, _ = sh(["/bin/ps", "-o", "etime=", "-p", pid])
        r["evidence"]["uptime"] = out.strip() or "?"

    # 2) 网关健康（不健康则重定位端口）
    port = C.read_gwport(bridge)
    healthy, _ = C.gateway_health(bridge, port)
    if not healthy:
        found = C.scan_gateway_port(bridge)
        if found:
            try:
                with open(os.path.join(bridge, "gwport"), "w", encoding="utf-8") as fh:
                    fh.write(str(found))
            except OSError:
                pass
            healthy, _ = C.gateway_health(bridge, found)
            port = found
            if healthy:
                r["fixed"].append("网关端口重定位到 %s" % port)
    r["evidence"]["gateway_port"] = port
    r["evidence"]["gateway_ok"] = healthy
    if not healthy:
        r["notes"].append("本机网关不通（端口 %s）" % (port or "无"))

    # 3) 密钥
    secret = C.load_secret(bridge)
    r["evidence"]["secret_len"] = len(secret)
    if len(secret) < 10:
        r["notes"].append("通道密钥缺失")

    # 4) 心跳（进程活着但主循环卡死，靠这个才发现得了）
    try:
        with open(os.path.join(bridge, "heartbeat"), "r", encoding="utf-8") as fh:
            hb = json.load(fh)
        age = int(time.time() - int(hb.get("t", 0)) / 1000)
        r["evidence"]["heartbeat_age"] = age
        r["evidence"]["heartbeat_state"] = hb.get("state")
        if age > HEARTBEAT_STALE:
            r["notes"].append("主循环心跳停在 %d 秒前（疑似卡死）" % age)
    except Exception as e:
        r["notes"].append("读不到心跳文件：%s" % e)

    r["ok"] = bool(pid) and healthy and len(secret) >= 10
    return r


def send_tele(bridge, text):
    """出向实发：直连 Telegram Bot API（不走网关，避免排队）。"""
    try:
        cfg = C.load_config(bridge)
        token = cfg.get("bot_token")
        chat_id = (cfg.get("allow_chats") or [None])[0]
        if not token or not chat_id:
            return False, "config 缺 bot_token / allow_chats"
        t0 = time.time()
        res = C.tg_call(token, "sendMessage", {"chat_id": str(chat_id), "text": text})
        return True, "message_id=%s/%dms" % (res.get("message_id"), int((time.time() - t0) * 1000))
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------- LINE 段

def load_line_cfg(bridge):
    try:
        with open(os.path.join(bridge, "line.json"), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return None


def owner_uid_from_log(bridge):
    """桥接日志里捞最近的 LINE userId（作为通知对象）。"""
    path = os.path.join(bridge, "bridge.log")
    try:
        rc, out, _ = sh(["/usr/bin/grep", "-o", "-E", r"\[LINE\] 收到 (U[0-9a-f]+)", path])
        ids = [ln.split()[-1] for ln in out.split("\n") if ln.strip()]
        if ids:
            return ids[-1]
        rc, out, _ = sh(["/usr/bin/grep", "-o", "-E", r"U[0-9a-f]{32}", path])
        for ln in reversed([x for x in out.split("\n") if x.strip()]):
            return ln.strip()
    except Exception:
        pass
    return None


def check_line(bridge, fix=True):
    """worker 可达 + 本机取信循环在跑（用 lastPoll 新鲜度判定）。"""
    r = {"name": "LINE", "ok": False, "notes": [], "fixed": [], "evidence": {}}
    cfg = load_line_cfg(bridge)
    if not cfg:
        r["notes"].append("line.json 不存在（LINE 通道未启用）")
        return r

    url = str(cfg.get("worker_url") or "").rstrip("/")
    key = cfg.get("yao_key") or ""
    hdrs = {"x-yao-key": key}

    t0 = time.time()
    st, body = http_json(url + "/status", headers=hdrs, timeout=12)
    ms = int((time.time() - t0) * 1000)
    r["evidence"]["worker_http"] = st
    r["evidence"]["worker_ms"] = ms
    if st != 200 or not isinstance(body, dict) or not body.get("ok"):
        # worker 侧不可达：可能是网络、CF 拦截或部署问题——本机重启桥接救不了，只报不修
        r["notes"].append("worker /status 异常：HTTP %s %s" % (st, str(body)[:120]))
        r["evidence"]["owner_uid"] = cfg.get("owner_uid")
        return r

    r["evidence"]["worker_online"] = body.get("online")
    r["evidence"]["queue"] = body.get("queue")
    r["evidence"]["has_secret"] = body.get("hasSecret")
    r["evidence"]["has_token"] = body.get("hasToken")
    last_poll = body.get("lastPoll")
    age = None
    if last_poll:
        try:
            import datetime
            dt = datetime.datetime.strptime(last_poll, "%Y-%m-%dT%H:%M:%S.%fZ")
            age = max(0, int((datetime.datetime.utcnow() - dt).total_seconds()))
            r["evidence"]["last_poll_age"] = age
        except Exception:
            pass

    if not body.get("hasToken"):
        r["notes"].append("worker 侧 LINE_CHANNEL_TOKEN 缺失（无法回推）")

    # 【2026-10-13】额度也一起报：LINE 按收件人数计费（群推 1 条 = 群人数），
    # 额度耗尽会让「主动推」全部静默失败，属最危险的隐形故障，所以要主动盯。
    try:
        stq, bq = http_json(url + "/quota", headers=hdrs, timeout=12)
        if stq == 200 and isinstance(bq, dict) and bq.get("ok"):
            total = (bq.get("quota") or {}).get("value")
            used = (bq.get("consumption") or {}).get("totalUsage")
            if total is not None and used is not None:
                total, used = int(total), int(used)
                r["evidence"]["quota_total"] = total
                r["evidence"]["quota_used"] = used
                r["evidence"]["quota_left"] = total - used
                if total - used <= 40:
                    r["notes"].append("LINE 推播额度只剩 %d 则（<40），主动推要节制" % (total - used))
    except Exception:
        pass

    poll_ok = age is not None and age <= LINE_POLL_STALE
    if not poll_ok:
        r["notes"].append("本机取信循环没在跑（lastPoll %s）" % (last_poll or "空"))
        if fix:
            sh(["/bin/launchctl", "kickstart", "-k", "gui/%d/%s" % (os.getuid(), BRIDGE_LABEL)])
            time.sleep(12)
            st2, body2 = http_json(url + "/status", headers=hdrs, timeout=12)
            if st2 == 200 and isinstance(body2, dict) and body2.get("lastPoll"):
                try:
                    import datetime
                    dt = datetime.datetime.strptime(body2["lastPoll"], "%Y-%m-%dT%H:%M:%S.%fZ")
                    age2 = max(0, int((datetime.datetime.utcnow() - dt).total_seconds()))
                except Exception:
                    age2 = None
                if age2 is not None and age2 <= LINE_POLL_STALE:
                    r["fixed"].append("已重启桥接，取信循环恢复（%s 秒前）" % age2)
                    r["evidence"]["last_poll_age"] = age2
                    poll_ok = True
                    r["notes"] = [n for n in r["notes"] if not n.startswith("本机取信循环")]

    r["evidence"]["owner_uid"] = cfg.get("owner_uid") or owner_uid_from_log(bridge)
    r["evidence"]["worker_url"] = url
    r["ok"] = bool(body.get("hasToken")) and poll_ok
    return r


def send_line(bridge, text):
    cfg = load_line_cfg(bridge) or {}
    url = str(cfg.get("worker_url") or "").rstrip("/")
    key = cfg.get("yao_key") or ""
    uid = cfg.get("owner_uid") or owner_uid_from_log(bridge)
    if not (url and key and uid):
        return False, "缺 worker_url / yao_key / owner_uid"
    t0 = time.time()
    st, body = http_json(url + "/reply", headers={"x-yao-key": key}, timeout=15,
                         method="POST", body={"userId": uid, "text": text[:4500]})
    ok = st == 200 and isinstance(body, dict) and body.get("ok")
    return ok, "HTTP %s/%dms" % (st, int((time.time() - t0) * 1000))


# ---------------------------------------------------------------- 云端中继段

def check_hub(fix=True):
    r = {"name": "云端中继", "ok": False, "notes": [], "fixed": [], "evidence": {}}
    t0 = time.time()
    st, body = http_json(HUB_URL + "/health", timeout=8)
    r["evidence"]["http"] = st
    r["evidence"]["ms"] = int((time.time() - t0) * 1000)
    if st == 200 and isinstance(body, dict) and body.get("ok"):
        r["evidence"]["service"] = body.get("service")
        r["evidence"]["mode"] = body.get("mode")
        r["ok"] = True
    else:
        r["notes"].append("yao-hub 不通（HTTP %s）" % st)
        # 隧道断了 → 重启隧道（launchd KeepAlive 保活）
        rc, out, _ = sh(["/usr/bin/pgrep", "-f", "8787:127.0.0.1:8787"])
        if not out.strip():
            r["notes"].append("SSH 隧道进程不在")
            if fix:
                sh(["/bin/launchctl", "kickstart", "-k", "gui/%d/%s" % (os.getuid(), TUNNEL_LABEL)])
                time.sleep(5)
                st2, body2 = http_json(HUB_URL + "/health", timeout=8)
                if st2 == 200 and isinstance(body2, dict) and body2.get("ok"):
                    r["ok"] = True
                    r["fixed"].append("已重启 SSH 隧道")
    return r


# ---------------------------------------------------------------- 报告

def build_report(res, waited):
    from datetime import datetime
    now = datetime.now().strftime("%m-%d %H:%M:%S")
    lines = ["🔧 開機自檢 %s" % now]

    t = res["tele"]
    ev = t["evidence"]
    if t["ok"]:
        lines.append("tele ✅ 橋接 pid %s（已跑 %s）｜網關 %s 健康｜心跳 %s 秒前"
                     % (ev.get("pid"), ev.get("uptime"), ev.get("gateway_port"),
                        ev.get("heartbeat_age")))
    else:
        lines.append("tele ❌ %s" % "；".join(t["notes"] or ["未知异常"]))

    l = res["LINE"]
    ev = l["evidence"]
    if l["ok"]:
        lines.append("LINE ✅ worker 200（%sms）｜本機取信 %s 秒前｜佇列 %s"
                     % (ev.get("worker_ms"), ev.get("last_poll_age"), ev.get("queue")))
        if ev.get("quota_left") is not None:
            lines.append("LINE 推播額度 %d/%d（剩 %d 則；群推 1 條＝群人數，回覆免費）"
                         % (ev.get("quota_used"), ev.get("quota_total"), ev.get("quota_left")))
    else:
        lines.append("LINE ❌ %s" % "；".join(l["notes"] or ["未知异常"]))

    h = res["hub"]
    if h["ok"]:
        lines.append("雲端 ✅ %s（mode=%s，隧道 %sms）"
                     % (h["evidence"].get("service"), h["evidence"].get("mode"), h["evidence"].get("ms")))
    else:
        lines.append("雲端 ⚠️ %s" % "；".join(h["notes"] or ["未知异常"]))

    fixed = [x for k in ("tele", "LINE", "hub") for x in res[k].get("fixed", [])]
    if fixed:
        lines.append("已自愈：" + "；".join(fixed))

    allok = res["tele"]["ok"] and res["LINE"]["ok"]
    lines.append("結論：%s" % ("line 與 tele 全通 ✅" if allok else "有通道未通，需要處理 ⚠️"))
    if res.get("tele_send"):
        lines.append("（tele 實發：%s）" % res["tele_send"])
    if res.get("line_send"):
        lines.append("（LINE 實發：%s）" % res["line_send"])
    return "\n".join(lines)


def main():
    args = sys.argv[1:]
    wait = 0
    if "--wait" in args:
        i = args.index("--wait")
        try:
            wait = int(args[i + 1])
        except Exception:
            wait = 0
    notify = "--notify" in args
    as_json = "--json" in args
    fix = "--no-fix" not in args

    if wait > 0:
        time.sleep(wait)

    bridge = C.find_bridge_dir(required=False)
    if not bridge:
        sys.stderr.write("找不到桥接目录，无法自检\n")
        sys.exit(1)
    C.remember_bridge_dir(bridge)

    res = {"at": int(time.time()), "waited": wait}
    res["tele"] = check_tele(bridge, fix=fix)
    res["hub"] = check_hub(fix=fix)
    res["LINE"] = check_line(bridge, fix=fix)

    # 出向实发（这也正是通知本身，一次完成「测试」与「报平安」）
    if notify:
        txt = build_report(res, wait)
        ok, detail = send_tele(bridge, txt)
        res["tele_send"] = ("成功 " + detail) if ok else ("失败 " + detail)

        # 【2026-10-13】LINE 实发要花额度（私聊 1 则），额度紧张时跳过：
        # 自检的结论 tele 已经送到，LINE 这一跳只是「验证能推」，不值得为它烧额度。
        left = res["LINE"]["evidence"].get("quota_left")
        if left is not None and left <= 40:
            res["line_send"] = "已跳过（LINE 剩额 %s 则，省额度）" % left
        else:
            ok2, detail2 = send_line(bridge, txt)
            res["line_send"] = ("成功 " + detail2) if ok2 else ("失败 " + detail2)
        txt = build_report(res, wait)   # 带上实发结果再落盘/打印

    out = json.dumps(res, ensure_ascii=False, indent=1) if as_json else build_report(res, wait)
    print(out)

    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        with open(LAST_JSON, "w", encoding="utf-8") as fh:
            json.dump(res, fh, ensure_ascii=False, indent=1)
    except OSError:
        pass

    sys.exit(0 if (res["tele"]["ok"] and res["LINE"]["ok"]) else 1)


if __name__ == "__main__":
    C.main_guard()
    main()

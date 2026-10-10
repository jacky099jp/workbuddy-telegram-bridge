#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""telegram-bridge 控制台：安装 / 启停 / 体检 / 刷新密钥 / 看日志 / 清队列。

用法：
  python3 bridge_ctl.py status          # 全链路体检（最常用）
  python3 bridge_ctl.py install [--force]  # 安装/修复开机自启（桥接 + 防睡眠）
                                           # 已加载且配置未变时不会重启，避免打断
                                           # 正在处理的 tele 消息；--force 强制重载
  python3 bridge_ctl.py start|stop|restart
  python3 bridge_ctl.py refresh         # 刷新网关密钥 + 重新定位端口
  python3 bridge_ctl.py logs [N]        # 看桥接日志（默认 30 行）
  python3 bridge_ctl.py pending [clear] # 看待补发队列 / 清空
  python3 bridge_ctl.py uninstall       # 卸载开机自启

为什么需要 refresh：
  WorkBuddy 每次重启会重新生成网关密钥（只存在于进程环境变量、不落盘），
  桥接读的是 secret 文件。所以在 WorkBuddy 重启后：
    - 若在 WorkBuddy 的会话里跑本脚本 → 直接读到环境变量，1 秒刷新完；
    - 否则只能等每小时守护任务刷新。
"""

import json
import os
import platform
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _common as C  # noqa: E402

LABEL_BRIDGE = "cn.yaowork.telegram-bridge"
LABEL_AWAKE = "cn.yaowork.keepawake"
AGENTS_DIR = os.path.join(C.HOME, "Library", "LaunchAgents")


# ---------------------------------------------------------------- 进程/服务

def pgrep_bridge():
    try:
        out = subprocess.run(["pgrep", "-fl", "telegram-bridge/bridge.js"],
                             capture_output=True, text=True, timeout=6).stdout.strip()
    except Exception:
        return None
    for line in out.splitlines():
        if "bridge.js" in line and "pgrep" not in line:
            return line.strip()
    return None


def launchctl(*args, check=False):
    return subprocess.run(["launchctl"] + list(args),
                          capture_output=True, text=True, timeout=15, check=check)


def service_state(label):
    r = launchctl("print", "gui/%d/%s" % (os.getuid(), label))
    if r.returncode != 0:
        return None
    state, pid = "?", "?"
    for line in r.stdout.splitlines():
        s = line.strip()
        if s.startswith("state ="):
            state = s.split("=", 1)[1].strip()
        elif s.startswith("pid ="):
            pid = s.split("=", 1)[1].strip()
    return {"state": state, "pid": pid}


def find_node():
    cands = [os.environ.get("WORKBUDDY_NODE", "")]
    cands += ["/opt/homebrew/bin/node", "/usr/local/bin/node"]
    base = os.path.expanduser("~/.workbuddy/binaries/node/versions")
    if os.path.isdir(base):
        for v in sorted(os.listdir(base), reverse=True):
            cands.append(os.path.join(base, v, "bin", "node"))
    for cand in cands:
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    for p in os.environ.get("PATH", "").split(":"):
        cand = os.path.join(p, "node")
        if os.path.isfile(cand) and os.access(cand, os.X_OK):
            return cand
    return None


def process_etime(pidline):
    pid = pidline.split()[0] if pidline else None
    if not pid:
        return "?"
    try:
        return subprocess.run(["ps", "-o", "etime=", "-p", pid],
                              capture_output=True, text=True, timeout=5).stdout.strip() or "?"
    except Exception:
        return "?"


# ---------------------------------------------------------------- 各项体检

def check_gateway(bridge):
    port = C.read_gwport(bridge)
    healthy, _ = C.gateway_health(bridge, port)
    detail = "端口 %s" % (port or "未知")
    if not healthy:
        found = C.scan_gateway_port(bridge)
        if found:
            with open(os.path.join(bridge, "gwport"), "w", encoding="utf-8") as fh:
                fh.write(found)
            port = found
            healthy, _ = C.gateway_health(bridge, port)
            detail = "端口已重新定位到 %s" % port
    return healthy, detail


def check_secret(bridge):
    env = (os.environ.get("CODEBUDDY_GATEWAY_PASSWORD") or "").strip()
    file_secret = C.load_secret(bridge) if not env else env
    try:
        with open(os.path.join(bridge, "secret"), "r", encoding="utf-8") as fh:
            on_disk = fh.read().strip()
    except OSError:
        on_disk = ""
    if env and on_disk and env != on_disk:
        return "stale", "环境里的密钥与 secret 文件不一致（需要 refresh）"
    if not file_secret:
        return "missing", "secret 文件为空"
    return "ok", "%d 字符%s" % (len(file_secret), "，与环境一致" if env else "")


def check_awake():
    r = subprocess.run(["pgrep", "-fl", "caffeinate"], capture_output=True, text=True)
    alive = bool(r.stdout.strip())
    gap = "-n" if "caffeinate -i -s" not in r.stdout else ""
    return alive, ("运行中" if alive else "未运行（Mac 会睡，tele 会断）")


def check_pending(bridge):
    p = os.path.join(bridge, "pending.json")
    try:
        with open(p, "r", encoding="utf-8") as fh:
            arr = json.load(fh)
    except Exception:
        return 0, []
    return len(arr), [str(x.get("text", ""))[:30] for x in arr if isinstance(x, dict)]


def cmd_status(bridge):
    print("=" * 46)
    print(" tele 通道体检")
    print("=" * 46)
    print("桥接目录    %s" % bridge)

    line = pgrep_bridge()
    etime = process_etime(line)
    print("桥接进程    %s" % ("运行中 (pid %s, 已跑 %s)" % (line.split()[0], etime) if line else "★ 未运行"))

    st = service_state(LABEL_BRIDGE)
    print("开机自启    %s" % (("已加载，state=%s pid=%s" % (st["state"], st["pid"])) if st
                             else "★ 未安装（跑 install）"))

    healthy, detail = check_gateway(bridge)
    print("本机网关    %s（%s）" % ("健康 200" if healthy else "★ 不通", detail))

    sev, sdet = check_secret(bridge)
    mark = {"ok": "正常", "stale": "★ 需刷新", "missing": "★ 缺失"}[sev]
    print("通道密钥    %s（%s）" % (mark, sdet))

    alive, adet = check_awake()
    print("防睡眠      %s" % adet)

    n, items = check_pending(bridge)
    print("待补发      %d 条%s" % (n, ("：" + " / ".join(items)) if items else ""))

    logs = os.path.join(bridge, "bridge.log")
    if os.path.isfile(logs):
        try:
            with open(logs, "r", encoding="utf-8", errors="ignore") as fh:
                tail = [x for x in fh.read().splitlines() if x.strip()][-3:]
            print("-- 日志尾 --")
            for t in tail:
                print("  " + t[:150])
        except Exception:
            pass

    cfg = C.load_config(bridge)
    print("-- 配置 --")
    print("  网关        %s" % cfg.get("gateway"))
    print("  允许会话    %s" % cfg.get("allow_chats"))
    print("  回复超时    %s ms" % cfg.get("reply_timeout_ms"))
    print("=" * 46)

    problems = []
    if not line:
        problems.append("桥接进程没在跑")
    if not healthy:
        problems.append("网关不通")
    if sev != "ok":
        problems.append("密钥%s" % ("需刷新" if sev == "stale" else "缺失"))
    if not alive:
        problems.append("防睡眠未开")
    if problems:
        print("需要处理：" + "、".join(problems))
        return 1
    print("结论：全链路正常。")
    return 0


# ---------------------------------------------------------------- launchd

BRIDGE_PLIST = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>{node}</string>
    <string>{bridge_js}</string>
  </array>
  <key>WorkingDirectory</key><string>{dir}</string>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
  <key>StandardOutPath</key><string>{dir}/launchd.out.log</string>
  <key>StandardErrorPath</key><string>{dir}/launchd.err.log</string>
  <key>EnvironmentVariables</key>
  <dict><key>PATH</key><string>/usr/bin:/bin:/usr/sbin:/sbin</string></dict>
</dict>
</plist>
"""

AWAKE_PLIST = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key><string>{label}</string>
  <key>ProgramArguments</key>
  <array>
    <string>/usr/bin/caffeinate</string>
    <string>-i</string>
    <string>-s</string>
  </array>
  <key>RunAtLoad</key><true/>
  <key>KeepAlive</key><true/>
  <key>ThrottleInterval</key><integer>10</integer>
</dict>
</plist>
"""


def write_plist(label, content):
    os.makedirs(AGENTS_DIR, exist_ok=True)
    path = os.path.join(AGENTS_DIR, label + ".plist")
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(content)
    return path


def plist_changed(path, content):
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return fh.read() != content
    except OSError:
        return True


def bootstrap(label, path, content=None, force=False):
    """加载服务。

    关键：**已加载且 plist 内容没变就不动它** —— 重启桥接会打断正在处理中的
    tele 消息，而那条消息的游标已推进、不会自动补发，等于永久丢失。
    只有首次安装、配置有变、或显式 --force 时才重载。

    返回 'unchanged' / 'reloaded' / 'failed'。
    """
    if content is not None and not force:
        st = service_state(label)
        if st is not None and not plist_changed(path, content):
            return "unchanged"

    launchctl("bootout", "gui/%d/%s" % (os.getuid(), label))
    r = launchctl("bootstrap", "gui/%d" % os.getuid(), path)
    if r.returncode != 0:
        r = launchctl("load", "-w", path)
    return "reloaded" if r.returncode == 0 else "failed"


def ensure_bridge_plist(bridge):
    """按当前桥接目录生成（或刷新）桥接 plist，返回 (path, content)。"""
    node = find_node()
    bridge_js = os.path.join(bridge, "bridge.js")
    if not node:
        C.die("找不到 node 可执行文件。请先安装 Node.js。")
    if not os.path.isfile(bridge_js):
        C.die("找不到 %s" % bridge_js)
    content = BRIDGE_PLIST.format(label=LABEL_BRIDGE, node=node,
                                  bridge_js=bridge_js, dir=bridge)
    return write_plist(LABEL_BRIDGE, content), content


def cmd_install(bridge, force=False):
    p1, c1 = ensure_bridge_plist(bridge)
    c2 = AWAKE_PLIST.format(label=LABEL_AWAKE)
    p2 = write_plist(LABEL_AWAKE, c2)
    print("已写入：%s" % p1)
    print("已写入：%s" % p2)

    label_of = {"unchanged": "已加载且配置未变，跳过重启（避免打断处理中的消息）",
                "reloaded": "已重新加载",
                "failed": "★ 加载失败"}
    print("桥接服务：  %s" % label_of[bootstrap(LABEL_BRIDGE, p1, c1, force)])
    print("防睡眠：    %s" % label_of[bootstrap(LABEL_AWAKE, p2, c2, force)])
    C.remember_bridge_dir(bridge)
    print("已记录桥接目录到 skill。")
    time.sleep(2)
    return cmd_status(bridge)


def cmd_uninstall(bridge):
    for label in (LABEL_BRIDGE, LABEL_AWAKE):
        launchctl("bootout", "gui/%d/%s" % (os.getuid(), label))
        path = os.path.join(AGENTS_DIR, label + ".plist")
        if os.path.isfile(path):
            os.remove(path)
        print("已卸载：%s" % label)
    return 0


# ---------------------------------------------------------------- 启停

def cmd_start(bridge):
    stop_file = os.path.join(bridge, "STOP")
    if os.path.isfile(stop_file):
        os.remove(stop_file)
    if pgrep_bridge():
        print("桥接已在运行。")
        return 0

    if service_state(LABEL_BRIDGE):
        launchctl("kickstart", "-k", "gui/%d/%s" % (os.getuid(), LABEL_BRIDGE))
    else:
        # 服务未被加载（例如刚被 stop 卸载，或开机自启丢了）→ 重新注册服务，
        # 保持「开机自启」这项能力，不要退化成跑一个孤儿进程。
        path, content = ensure_bridge_plist(bridge)
        if bootstrap(LABEL_BRIDGE, path, content, force=True) == "failed":
            node = find_node()
            subprocess.Popen([node, os.path.join(bridge, "bridge.js")],
                             cwd=bridge,
                             stdout=open(os.path.join(bridge, "bridge.out.log"), "a"),
                             stderr=subprocess.STDOUT, start_new_session=True)
    time.sleep(3)
    line = pgrep_bridge()
    print("桥接已启动：%s" % (line or "★ 启动失败"))
    return 0 if line else 1


def cmd_stop(bridge):
    # 先写 STOP 让桥接自行优雅退出（它每轮循环顶部会检查）
    with open(os.path.join(bridge, "STOP"), "w") as fh:
        fh.write("")
    for _ in range(10):
        if not pgrep_bridge():
            break
        time.sleep(1)
    # 还活着就卸载服务再强杀
    launchctl("bootout", "gui/%d/%s" % (os.getuid(), LABEL_BRIDGE))
    if pgrep_bridge():
        subprocess.run(["pkill", "-f", "telegram-bridge/bridge.js"], capture_output=True)
        time.sleep(1)
    print("桥接已停止。" + ("（仍有残留进程）" if pgrep_bridge() else ""))
    return 0


def cmd_restart(bridge):
    cmd_stop(bridge)
    return cmd_start(bridge)


# ---------------------------------------------------------------- 刷新/日志/队列

def cmd_refresh(bridge):
    env = (os.environ.get("CODEBUDDY_GATEWAY_PASSWORD") or "").strip()
    if not env:
        print("环境变量 CODEBUDDY_GATEWAY_PASSWORD 为空 —— 当前不在 WorkBuddy 会话内，"
              "只能等每小时守护任务刷新。")
    else:
        path = os.path.join(bridge, "secret")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(env)
        os.chmod(path, 0o600)
        print("密钥已刷新（%d 字符）。" % len(env))

    port = C.scan_gateway_port(bridge)
    if port:
        with open(os.path.join(bridge, "gwport"), "w", encoding="utf-8") as fh:
            fh.write(port)
        cfg = C.load_config(bridge)
        cfg["gateway"] = "http://127.0.0.1:%s" % port
        with open(os.path.join(bridge, "config.json"), "w", encoding="utf-8") as fh:
            json.dump(cfg, fh, ensure_ascii=False, indent=2)
        print("网关端口已定位：%s" % port)
    else:
        print("★ 未能定位本机网关（WorkBuddy 可能没开）。")

    C.remember_bridge_dir(bridge)
    if pgrep_bridge():
        launchctl("kickstart", "-k", "gui/%d/%s" % (os.getuid(), LABEL_BRIDGE))
        print("已重启桥接。")
    healthy, detail = check_gateway(bridge)
    print("刷新后网关：%s（%s）" % ("健康" if healthy else "仍不通", detail))
    return 0 if healthy else 1


def cmd_logs(bridge, n=30):
    for name in ("bridge.log", "launchd.err.log"):
        path = os.path.join(bridge, name)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8", errors="ignore") as fh:
            lines = [x for x in fh.read().splitlines() if x.strip()][-n:]
        print("--- %s（末 %d 行）---" % (name, len(lines)))
        for x in lines:
            print(x[:200])
    return 0


def cmd_pending(bridge, action=None):
    path = os.path.join(bridge, "pending.json")
    if action == "clear":
        with open(path, "w", encoding="utf-8") as fh:
            fh.write("[]")
        print("待补发队列已清空。")
        return 0
    n, items = check_pending(bridge)
    print("待补发 %d 条" % n)
    for t in items:
        print("  - " + t)
    return 0


# ---------------------------------------------------------------- 入口

def main():
    argv = sys.argv[1:]
    cmd = argv[0] if argv else "status"
    rest = [x for x in argv[1:] if not x.startswith("-")]
    force = "--force" in argv

    bridge = C.find_bridge_dir(required=True)
    C.remember_bridge_dir(bridge)

    if cmd == "install":
        return cmd_install(bridge, force)
    if cmd == "uninstall":
        return cmd_uninstall(bridge)
    if cmd == "status":
        return cmd_status(bridge)
    if cmd == "start":
        return cmd_start(bridge)
    if cmd == "stop":
        return cmd_stop(bridge)
    if cmd == "restart":
        return cmd_restart(bridge)
    if cmd == "refresh":
        return cmd_refresh(bridge)
    if cmd == "logs":
        return cmd_logs(bridge, int(rest[0]) if rest and rest[0].isdigit() else 30)
    if cmd == "pending":
        return cmd_pending(bridge, rest[0] if rest else None)

    C.die("未知命令：%s（用 --help 看用法）" % cmd)


if __name__ == "__main__":
    C.main_guard()
    if len(sys.argv) > 1 and sys.argv[1] in ("-h", "--help"):
        print(__doc__)
        sys.exit(0)
    sys.exit(main())

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""把 bridge.js 以完全脱离的方式启动（双 fork + setsid），使其在调用方退出后仍存活。

用法：  python3 start_bridge.py
停止：  touch STOP  或  kill $(cat bridge.pid)
"""
import os
import sys
import time

BASE = os.path.dirname(os.path.abspath(__file__))
BRIDGE = os.path.join(BASE, "bridge.js")
PIDFILE = os.path.join(BASE, "bridge.pid")


def find_node():
    """找一个可用的 node：优先环境变量 WORKBUDDY_NODE，其次常见安装位置，
    再扫 WorkBuddy 自带的 node，最后回落到 PATH。"""
    cands = [os.environ.get("WORKBUDDY_NODE", "")]
    cands += ["/opt/homebrew/bin/node", "/usr/local/bin/node"]
    base = os.path.expanduser("~/.workbuddy/binaries/node/versions")
    if os.path.isdir(base):
        for v in sorted(os.listdir(base), reverse=True):
            cands.append(os.path.join(base, v, "bin", "node"))
    for p in os.environ.get("PATH", "").split(":"):
        cands.append(os.path.join(p, "node"))
    for c in cands:
        if c and os.path.isfile(c) and os.access(c, os.X_OK):
            return c
    return None


NODE = find_node()


def alive(pid):
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def current_pid():
    if not os.path.exists(PIDFILE):
        return None
    try:
        pid = int(open(PIDFILE).read().strip())
    except Exception:
        return None
    return pid if alive(pid) else None


def main():
    existing = current_pid()
    if existing:
        print("已在运行，pid=%d" % existing)
        return 0

    if not NODE:
        print("找不到 node。请先安装 Node.js，或设置环境变量 WORKBUDDY_NODE 指向 node 可执行文件。")
        return 1

    if os.path.exists(os.path.join(BASE, "STOP")):
        os.remove(os.path.join(BASE, "STOP"))

    # 第一次 fork
    if os.fork() > 0:
        # 父进程等待子进程写下 pid 文件
        for _ in range(30):
            time.sleep(0.1)
            pid = current_pid()
            if pid:
                print("已启动，pid=%d" % pid)
                return 0
        print("启动超时，请查看 bridge.log")
        return 1

    os.setsid()
    # 第二次 fork
    if os.fork() > 0:
        os._exit(0)

    os.chdir(BASE)
    os.umask(0o077)

    devnull = os.open(os.devnull, os.O_RDONLY)
    os.dup2(devnull, 0)
    logfd = os.open(os.path.join(BASE, "bridge.out.log"),
                    os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
    os.dup2(logfd, 1)
    os.dup2(logfd, 2)

    # 清理可能残留的代理变量，避免影响出网
    env = dict(os.environ)
    for k in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"):
        env.pop(k, None)
    env["PYTHONUNBUFFERED"] = "1"

    pid = os.fork()
    if pid == 0:
        os.execve(NODE, [NODE, BRIDGE], env)

    with open(PIDFILE, "w") as f:
        f.write(str(pid))
    os._exit(0)


if __name__ == "__main__":
    sys.exit(main())

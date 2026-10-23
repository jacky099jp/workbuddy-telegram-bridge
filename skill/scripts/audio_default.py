#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""默认音频输出设备：查询 / 切换 / 开机确保走内建扬声器。

背景（2026-10-23）：Mac mini（2014）用 HDMI 接 BenQ GW2455 显示器，
该型号**没有内置喇叭**。每次重启后 macOS 会把默认输出挑到 HDMI，
结果就是「整台电脑没声音」。这里用 CoreAudio 系统 API 直接处理，装任何第三方都不用。

用法：
  python3 audio_default.py list      # 列出所有输出设备与当前默认
  python3 audio_default.py ensure    # 若默认不是内建扬声器 → 切回内建（返回是否有动作）
  python3 audio_default.py set NAME  # 按名字片段切换（如 "Built-in"）

退出码：0 正常；1 失败。
"""

import ctypes
import sys

CA = ctypes.CDLL("/System/Library/Frameworks/CoreAudio.framework/CoreAudio")
CF = ctypes.CDLL("/System/Library/Frameworks/CoreFoundation.framework/CoreFoundation")

SYS_OBJECT = 1
SCOPE_GLOBAL = int.from_bytes(b"glob", "big")
SCOPE_OUTPUT = int.from_bytes(b"outp", "big")
ELEM_MAIN = 0


def _cc(s):
    return int.from_bytes(s.encode(), "big")


class Addr(ctypes.Structure):
    _fields_ = [("mSelector", ctypes.c_uint32),
                ("mScope", ctypes.c_uint32),
                ("mElement", ctypes.c_uint32)]


def _addr(sel, scope=SCOPE_GLOBAL):
    return Addr(_cc(sel), scope, ELEM_MAIN)


def _u32(obj, sel, scope=SCOPE_GLOBAL, default=0):
    a = _addr(sel, scope)
    size = ctypes.c_uint32(4)
    out = ctypes.c_uint32(0)
    st = CA.AudioObjectGetPropertyData(ctypes.c_uint32(obj), ctypes.byref(a), 0, None,
                                       ctypes.byref(size), ctypes.byref(out))
    return (out.value if st == 0 else default), st


def device_ids():
    a = _addr("dev#")
    size = ctypes.c_uint32(0)
    CA.AudioObjectGetPropertyDataSize(ctypes.c_uint32(SYS_OBJECT), ctypes.byref(a), 0, None,
                                      ctypes.byref(size))
    n = size.value // 4
    if n <= 0:
        return []
    buf = (ctypes.c_uint32 * n)()
    CA.AudioObjectGetPropertyData(ctypes.c_uint32(SYS_OBJECT), ctypes.byref(a), 0, None,
                                  ctypes.byref(size), buf)
    return list(buf)


def device_name(did):
    a = _addr("lnam")
    size = ctypes.c_uint32(ctypes.sizeof(ctypes.c_void_p))
    out = ctypes.c_void_p()
    st = CA.AudioObjectGetPropertyData(ctypes.c_uint32(did), ctypes.byref(a), 0, None,
                                       ctypes.byref(size), ctypes.byref(out))
    if st != 0 or not out.value:
        return "?"
    buf = ctypes.create_string_buffer(256)
    CF.CFStringGetCString(ctypes.c_void_p(out.value), buf, 256, 0x08000100)
    return buf.value.decode("utf-8", "ignore")


def is_output(did):
    a = _addr("stm#", SCOPE_OUTPUT)
    size = ctypes.c_uint32(0)
    st = CA.AudioObjectGetPropertyDataSize(ctypes.c_uint32(did), ctypes.byref(a), 0, None,
                                           ctypes.byref(size))
    return st == 0 and size.value > 0


def transport(did):
    """'bltn' 内建 / 'hdmi' HDMI / 'usb ' / 'blue' 蓝牙 …"""
    val, st = _u32(did, "tran")
    if st != 0:
        return ""
    return val.to_bytes(4, "big").decode("ascii", "ignore")


def get_default():
    return _u32(SYS_OBJECT, "dOut")[0]


def set_default(did):
    ok = True
    for sel in ("dOut", "sOut"):          # 默认输出 + 系统提示音输出
        a = _addr(sel)
        v = ctypes.c_uint32(did)
        st = CA.AudioObjectSetPropertyData(ctypes.c_uint32(SYS_OBJECT), ctypes.byref(a), 0, None,
                                           ctypes.c_uint32(4), ctypes.byref(v))
        ok = ok and st == 0
    return ok


def snapshot():
    cur = get_default()
    out = []
    for d in device_ids():
        if not is_output(d):
            continue
        out.append({"id": d, "name": device_name(d), "transport": transport(d),
                    "default": d == cur})
    return out


def builtin_output():
    for d in snapshot():
        if d["transport"] == "bltn" or "built-in" in d["name"].lower():
            return d
    return None


def ensure_builtin():
    """默认不在内建扬声器时切回去。返回 (是否切换, 前, 后, 说明)"""
    devs = snapshot()
    cur = next((d for d in devs if d["default"]), None)
    bi = builtin_output()
    if bi is None:
        return False, cur, cur, "找不到内建扬声器"
    if cur and cur["id"] == bi["id"]:
        return False, cur, cur, "已是内建扬声器"
    if not set_default(bi["id"]):
        return False, cur, cur, "切换失败"
    return True, cur, builtin_output(), "已切回内建扬声器"


def _main():
    arg = sys.argv[1] if len(sys.argv) > 1 else "list"
    if arg == "list":
        for d in snapshot():
            mark = "  ← 当前默认" if d["default"] else ""
            print("%3d  %-14s %s%s" % (d["id"], d["transport"], d["name"], mark))
        return 0
    if arg == "ensure":
        changed, before, after, msg = ensure_builtin()
        b = "%s(%s)" % (before["name"], before["transport"]) if before else "无"
        a = "%s(%s)" % (after["name"], after["transport"]) if after else "无"
        print("切换=%s 前=%s 后=%s %s" % ("是" if changed else "否", b, a, msg))
        return 0
    if arg == "set":
        want = " ".join(sys.argv[2:]).strip().lower()
        for d in snapshot():
            if want and want in d["name"].lower():
                print("已切换 %s" % d["name"] if set_default(d["id"]) else "切换失败 %s" % d["name"])
                return 0
        print("找不到输出设备：%s" % want)
        return 1
    print(__doc__)
    return 1


if __name__ == "__main__":
    sys.exit(_main())

# Graph Report - workbuddy-telegram-bridge  (2026-10-24)

## Corpus Check
- cluster-only mode — file stats not available

## Summary
- 205 nodes · 502 edges · 12 communities
- Extraction: 100% EXTRACTED · 0% INFERRED · 0% AMBIGUOUS
- Token cost: 0 input · 0 output

## Graph Freshness
- Built from commit: `fdc99820`
- Run `git rev-parse HEAD` and compare to check if the graph is stale.
- Run `graphify update .` after code changes (no API cost).

## Community Hubs (Navigation)
- bridge.js
- bridge_ctl.py
- _common.py
- boot_check.py
- audio_default.py
- lineTick
- sys
- deliver
- main
- log
- handleMessage
- json

## God Nodes (most connected - your core abstractions)
1. `log()` - 23 edges
2. `main()` - 19 edges
3. `lineTick()` - 17 edges
4. `handleMessage()` - 13 edges
5. `cmd_start()` - 12 edges
6. `deliver()` - 12 edges
7. `cmd_status()` - 11 edges
8. `main()` - 11 edges
9. `main()` - 10 edges
10. `flushPending()` - 9 edges

## Surprising Connections (you probably didn't know these)
- `lineTick()` --calls--> `exitBusy()`  [EXTRACTED]
  bridge/bridge.js → bridge/bridge.js  _Bridges community 0 → community 5_
- `main()` --calls--> `exitBusy()`  [EXTRACTED]
  bridge/bridge.js → bridge/bridge.js  _Bridges community 0 → community 8_
- `withGateway()` --calls--> `newestServeGateway()`  [EXTRACTED]
  bridge/bridge.js → bridge/bridge.js  _Bridges community 0 → community 7_
- `startRun()` --calls--> `readSecret()`  [EXTRACTED]
  bridge/bridge.js → bridge/bridge.js  _Bridges community 0 → community 9_
- `check_pending()` --calls--> `json`  [EXTRACTED]
  skill/scripts/bridge_ctl.py →   _Bridges community 1 → community 11_

## Import Cycles
- None detected.

## Communities (12 total, 0 thin omitted)

### Community 0 - "bridge.js"
Cohesion: 0.07
Nodes (24): ALLOW, CLOUD_URL, CONFIG, exitBusy(), EXT_BY_MIME, GW_HINT_FILE, harvestGatewayEnv(), HEARTBEAT_FILE (+16 more)

### Community 1 - "bridge_ctl.py"
Cohesion: 0.19
Nodes (23): bootstrap(), check_awake(), check_gateway(), check_pending(), check_secret(), cmd_install(), cmd_logs(), cmd_pending() (+15 more)

### Community 2 - "_common.py"
Cohesion: 0.11
Nodes (11): _bridge_dir_candidates(), chunk_text(), die(), find_bridge_dir(), gateway_health(), http_get(), load_secret(), main_guard() (+3 more)

### Community 3 - "boot_check.py"
Cohesion: 0.20
Nodes (12): build_report(), check_audio(), check_hub(), check_line(), check_tele(), http_json(), load_line_cfg(), main() (+4 more)

### Community 4 - "audio_default.py"
Cohesion: 0.26
Nodes (14): Addr, _addr(), builtin_output(), _cc(), device_ids(), device_name(), ensure_builtin(), get_default() (+6 more)

### Community 5 - "lineTick"
Cohesion: 0.20
Nodes (17): addHeld(), asrFromLine(), clearHeld(), groupWho(), isCallingXiaoyao(), lineApi(), lineLog(), lineQueueNotice() (+9 more)

### Community 6 - "sys"
Cohesion: 0.22
Nodes (7): alive(), current_pid(), find_node(), main(), main(), parse_args(), read_input()

### Community 7 - "deliver"
Cohesion: 0.26
Nodes (12): awaitReply(), awaitReplyOnce(), clearRun(), cloudGetUpdates(), deliver(), listLocalPorts(), probeHealth(), readGwHint() (+4 more)

### Community 8 - "main"
Cohesion: 0.27
Nodes (10): beat(), cloudAck(), cloudHandoff(), cloudHeaders(), cloudRecover(), enterBusy(), loadState(), main() (+2 more)

### Community 9 - "log"
Cohesion: 0.33
Nodes (9): asrFromTelegram(), asrTranscribe(), cancelRun(), clearStaleOn429(), log(), readSecret(), sweepOrphanRun(), tryWakeWorkBuddy() (+1 more)

### Community 10 - "handleMessage"
Cohesion: 0.31
Nodes (9): cloudSend(), flushPending(), handleMessage(), isTransientErr(), mediaFromTelegram(), notifyRecoverIfDue(), removePending(), savePending() (+1 more)

### Community 11 - "json"
Cohesion: 0.25
Nodes (4): check_heartbeat(), load_config(), tg_call(), TgError

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `main()` connect `boot_check.py` to `bridge_ctl.py`, `json`, `sys`?**
  _High betweenness centrality (0.010) - this node is a cross-community bridge._
- **Should `bridge.js` be split into smaller, more focused modules?**
  _Cohesion score 0.07258064516129033 - nodes in this community are weakly interconnected._
- **Why does `check_line()` connect `boot_check.py` to `bridge_ctl.py`?**
  _High betweenness centrality (0.008) - this node is a cross-community bridge._
- **Should `_common.py` be split into smaller, more focused modules?**
  _Cohesion score 0.10666666666666667 - nodes in this community are weakly interconnected._
- **Why does `tg_call()` connect `json` to `_common.py`, `boot_check.py`?**
  _High betweenness centrality (0.006) - this node is a cross-community bridge._
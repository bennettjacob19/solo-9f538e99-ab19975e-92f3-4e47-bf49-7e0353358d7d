# GSB 网关维护演练台（Flask + SQLite + 本地卷）

一个可交互的“配置网关 + 分布式租约接管 + fencing token + 发布暂停 / 中断恢复 +
幂等回执”演练环境。所有题目要求的关键行为都有真实进程 / 真实文件 / 真实锁支撑，
不是纯前端动画。

---

## 1. 它能演示什么

| 题目要求 | 实现方式 |
| --- | --- |
| Flask 网关从本地卷读 JSON | `GET /gw/<route>` 以 `current.json` 的 `stat` 指纹（inode/mtime/size）判切换，带 `X-Config-Version`；正文经原子 `os.replace` 发布 |
| **万条路由下单请求不重读整份配置** | 路由目录 `drill/catalog.py`：内存不可变快照 + 按内容指纹共享的路由索引；请求热路径只做 1 次 `stat` + 两次 O(1) 集合判定，不 `open`/`json.load` 整份配置、不遍历路由表与停用清单 |
| **切换后首个请求也不遍历 / 不等索引重建** | 发布端在 `os.replace` **之前**先把路由目录侧车 `route-<指纹>.json` 与代际指针 `active.json` 原子写到 `STATE_DIR/catalog/`；切换后首个请求只 `stat` + 读有界文件头确认版本 + 按指纹 O(1) 装载，自证指标 `sidecar_adoptions`，全程无全表扫描 |
| **只改停用清单复用路由目录** | 路由集合做规范化 SHA-256 指纹；指纹不变即复用同一 `RouteIndex`（`route_index_reuses`），只替换 disabled 集合 |
| 停用路由返回 503，其余照常 | 命中 `disabled` → **503** + `Retry-After`；启用路由由 mock 上游回 200；未知路由 404；状态码与版本头取自**同一个不可变快照**，不可能跨版本 |
| 网页试发网关请求 | 卡片①既可下拉选路由，也可**直接输入任意路径**（含未知路径）发送，展示原始状态码、`X-Config-Version`、索引来源与 JSON 响应 |
| 暂停 / 写失败仍用旧版 | 一切以 `current.json` 的实际切换为准：暂停在切换前或写入失败时正文未换，网关继续按旧快照响应；侧车缺失 / 指针损坏只触发一次性全量解析降级，正确性不变 |
| 恢复 / 重置后首请求即正确（即使版号复用） | recover 补切换时同步装载内存快照并补指针；重置清空侧车与内存后重新播种，首个请求即按实际生效配置响应，版号复用也不认旧记忆 |
| 推进模拟时钟 | 时钟存在 SQLite `kv` 表（初始 t=1000，TTL=30 模拟秒），支持 +10/+30/+60、自定义、一键“推进到租约过期” |
| 两个独立 worker 申请 / 续租 / 提交停用清单 | 页面两张独立的 A / B 卡片，各自保存“自认 token”和勾选集合；每次发布另起一个**独立 OS 子进程** |
| SQLite 保存租约和回执 | `lease` / `worker_state` / `receipts` / `versions` / `publish_runs` / `events` 等表，WAL + `busy_timeout` |
| 并发申请只准一个成功 | `BEGIN IMMEDIATE` 事务内“读租约→判定→发 token→写回”，写锁串行化竞争；败者 409 并带持有者 / 期限原因 |
| 新租约发递增 fencing token | `kv.next_token` 单调递增；只有“无租约 / 旧租约过期后接管”才发新号 |
| 三个暂停点 | 发布子进程在 ① 暂存写一半且 **fd 不关闭** ② 暂存完整、`os.replace` 前（切换已发起未生效）③ 已 replace 且回执已落库、返回前，把任务置 `paused` 并真实 `SIGSTOP` 自己 |
| 过期 + 旧进程暂停时，备用进程连续发布两版 | 推进时钟 → B 申请得 token+1 → B 连续发布 v1、v2，自动化用例完整跑通 |
| 旧进程恢复后的过期写入被拒绝，配置 / 版号不变 | 恢复后发布端在 SQLite 事务里再次 fencing，旧 token 被拒（`status=rejected`），不写版本 / 回执 / 文件；当前版仍停在 B 的 v2 |
| 磁盘最多 current + 一份 staging；已删但仍打开也算 | 固定两个正文文件名；新发布先 `unlink staging.json` 再建新 inode。磁盘面板通过 `/proc/<pid>/fd` 真实扫描“`(deleted)` 但仍打开”的幽灵文件、大小和归属，并按唯一 inode 统计占用。**路由侧车写在 state 目录，不占用配置卷槽位** |
| 暂停 / 写失败 / 重启时网关按最近完整配置响应 | 网关只认 `current.json`；写失败保留半成品 staging 但不碰 current；进程 / 服务重启后 current 与 SQLite 都在；另有进程内 last-good 兜底（响应头 `X-Config-Source: last-good-cache`） |
| 生效未回包时中断，重启恢复租约和令牌 | 生效点之后回执与版本已在同一 SQLite 事务落库；可 `SIGKILL` 发布端，`/api/publisher/recover`（以及 Flask 启动钩子）读回 holder/token，补完 `applied=0` 的切换，标记失联任务 |
| 同号重发返回原回执、不升版（即使原进程仍暂停） | `receipts.request_id` 主键幂等；`POST /api/publish` 先查回执，命中直接 `replayed:true` 返回原回执，不校验 token、不起进程、不升版 |
| 页面显示版号 / 持有者 / 令牌 / 期限 / 拒绝原因 | 顶部租约卡、两张 worker 卡、任务表、版本表、回执表、事件 / 拒绝原因流，每秒轮询；大表时 `/api/state` 路由列表截断到 200 条并标注总数，全量仍在网关目录中 |
| docker compose 启动 | `docker compose up --build` → http://localhost:8000 ，数据在命名卷 `gsb-drill-volume:/drill` |

---

## 2. 进程与存储拓扑

```
浏览器 (templates/index.html + static/app.js)
   │  /api/state 轮询、/api/lease/*、/api/publish、/api/runs/.../{resume,kill}
   ▼
Flask（app.py，threaded，单进程；演练台 + 网关 /gw/...）
   │  subprocess.Popen([python, -m, drill.publisher, --run-id ...],
   │                   start_new_session=True)，SIGCONT / SIGKILL
   ├─ publisher A（独立 PID / fd，可被 SIGSTOP 暂停）
   ├─ publisher B …
   ├─ drill/catalog.py 路由目录（网关热路径）
   │    内存：不可变 Snapshot（版本 + 停用 frozenset + 按指纹共享的 RouteIndex）
   │    巡检：守护线程每 0.1s stat 一次 current.json，主动预热新代际
   │    侧车：STATE_DIR/catalog/route-<指纹>.json（不可变路由目录）
   │          STATE_DIR/catalog/active.json（代际指针，原子 replace）
   └─ SQLite (DRILL_STATE_DIR/drill.db, WAL)
        租约 / fencing 序号 / 版本 / 幂等回执 / 任务 / 事件
本地卷 DRILL_DATA_DIR/
   ├─ current.json   ← 网关唯一读取的完整配置（os.replace 原子换入）
   └─ staging.json   ← 全局至多一份暂存正文（失败 / 半写时可能残留）
```

网关 `/gw/<path>` 的热路径（无切换时）：

```
请求 → catalog.decide(path)
        ├─ stat(current.json)               # 1 次系统调用，O(1)
        ├─ 指纹 == 当前快照？ 是 → 直接返回（hot_path_hits++）
        └─ 否（发生切换）：
             ├─ 读 current.json 头部 8KB 正则确认 version（与表大小无关）
             ├─ 读 active.json 指针（小文件）并校验 size+version
             ├─ 按 route_fingerprint 取内存 RouteIndex（没有就读对应侧车）
             └─ 两次 O(1) 集合查询：path in disabled? / path in routes?
```

关键正确性设计：

1. **fencing 有两道卡**：Flask 接单时预校验一次（快速 409）；发布端子进程在
   “切换前的提交事务”里用 `BEGIN IMMEDIATE` 再校验一次 holder + token + 模拟期限。
   因此旧进程即使在暂停期间经历了过期和接管，恢复后也过不了第二道卡。
2. **版本 + 回执一个事务**：`versions` 与 `receipts` 在同一 `IMMEDIATE` 事务提交，
   然后才 `os.replace(staging, current)`。回执先于“回包暂停点”存在，所以原进程
   还 `T (stopped)` 时，同号重发就能从 SQLite 拿到原回执。
3. **崩溃恢复**：若死在“事务已提交、文件还没换”之间，`versions.applied=0`，
   recover 会用库里的 `config_json` 经 staging 原子补切换，并同步装载目录快照。
4. **磁盘核算不做假账**：Linux 下直接扫 `/proc/[0-9]*/fd/*` 的 readlink 与 fstat，
   `.../staging.json (deleted)` 且能 fstat 到大小的，就是“已删除但仍打开”的 inode。
5. **侧车先于正文**：发布端顺序是「写不可变路由侧车 → 原子代际指针 →
   os.replace 正文」。新正文可见时指针必然已就绪；任一步崩溃只会让网关走
   一次全量解析降级（计入 `full_parse_fallbacks`），应答仍然正确。
6. **状态码 / 版本头同源**：一次请求的决策结果和 `X-Config-Version` 取自同一个
   不可变 `Snapshot` 对象，从结构上杜绝“头是新版、判定按旧版”的错配。

---

## 3. 目录结构

```
app.py                 Flask：页面 / 网关 / 全部演练 API
drill/config.py        路径、TTL、配额、内置路由、大表播种（环境变量可覆盖）
drill/db.py            SQLite schema、连接（WAL/busy_timeout）、模拟时钟、事件
drill/lease.py         并发安全的 acquire / renew / fence_check
drill/configstore.py   current.json 播种 / 读取（含 last-good 兜底）
drill/catalog.py       路由目录：不可变快照、指纹共享索引、侧车 / 指针、O(1) 判定与指标
drill/disk.py          卷文件 + /proc 已删打开句柄扫描与唯一 inode 占用
drill/runner.py        发布子进程拉起 / SIGCONT / SIGKILL / 回收 / 重置 / 恢复
drill/publisher.py     发布端状态机（可独立成进程：暂停点、fencing、提交、切换、侧车预制）
templates/index.html   演练页（含任意路径试发输入框）
static/app.js          原生 JS（轮询、两 worker、任务操作、网关试发、目录指标）
static/style.css       暗色控制台样式
tests/selftest.py      100 项断言的端到端自测（stdlib client，自起真实服务）
tests/selftest_bigtable.py  万条路由专项自测（默认 12000 条，73 项断言）
Dockerfile
docker-compose.yml
requirements.txt
```

---

## 4. 本地运行

需要 Python 3.11+ 与 Flask：

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python app.py
# 打开 http://127.0.0.1:8000
```

可用环境变量：`DRILL_PORT`（默认 8000）、`DRILL_DATA_DIR`、`DRILL_STATE_DIR`、
`DRILL_LEASE_TTL`（默认 30 模拟秒）、`DRILL_DISK_QUOTA`、
`DRILL_ROUTE_COUNT`（播种路由总数，大于内置 6 条时自动补足生成路由，用于万条
路由演练）、`DRILL_CATALOG_NO_WATCHER`（设为 1 关闭后台巡检，自测用来确定性
验证“切换后首个请求自身”完成装载）。

### docker compose

```bash
docker compose up --build
# http://localhost:8000
# 查看卷内文件：
docker compose exec gateway ls -la /drill/config
# 看某个暂停发布端的真实进程状态（T = stopped）：
docker compose exec gateway ps -ef   # 或直接看页面上的 /proc 状态
```

compose 里 `init: true` 负责回收发布端僵尸进程；数据持久化在命名卷
`gsb-drill-volume`（JSON 在 `/drill/config`，SQLite 在 `/drill/state`）。

---

## 5. 推荐手动演练剧本

### A. 并发申请 + 接管两版 + 旧进程恢复被拒

1. 点 **「⚡ A / B 同时申请租约（并发）」**：恰好一个成功，另一个红色提示
   `held_by_other`（含持有者、token、到期 t）。
2. 若 B 不是胜方，可先「重置演练」，让 A 申请成功。
3. A 勾选 `/api/checkout`，暂停点选 **「暂存写一半（fd 保持打开）」**，发布。
   任务表出现 `paused / stage_half`，`/proc=T`；磁盘面板能看到半写的
   `staging.json`。
4. 连点时钟 **+30s / 「推进到租约过期」**，直到租约变红“已过期”。
5. 到 B 卡片点 **申请租约**：B 拿到 **token=2**。
6. B 勾选 `/api/checkout` + `/admin/reload`，暂停点“不暂停”，发布 → **v1**；
   再只勾 `/api/checkout` 发布一次 → **v2**。
   - 磁盘面板此时出现红色 **「已删除·仍打开」** 幽灵行（A 的 pid，大小为半写字节）。
   - 网关卡片试发 `/api/checkout` = 503（`X-Config-Version: 2`），
     `/admin/reload` = 200。
7. 回任务表找到 A 的任务，点 **▶ 恢复**。A 很快变 `rejected`，原因是
   `fencing 拒绝：持有者已变更为 B（现行 token=2）…`。
8. 页面顶部版号仍是 **v2**、停用清单仍是 B 第二版的 `["/api/checkout"]`；
   A 的请求没有版本 / 回执；A 退出后磁盘幽灵行消失，只剩 `current.json`。

`before_switch` 暂停点可以同样再跑一遍（该点 fd 已正常 close，所以不会有幽灵
inode，但恢复后一样被 fencing 拒绝）。

### B. 生效未回包 → 同号重发 → 强杀 → 重启恢复

1. 重置；A 申请；暂停点选 **「已生效但未回包」**，发布。
2. 任务 `paused / effective_no_response` 时，回执表里已经有原回执（v1）。
3. 点该行 **⟳ 同号重发**：提示“返回原回执 v1（replayed=true），未升版”，
   版本表仍只有 v1。原进程在 `/proc` 里仍是 `T`。
4. 点 **✕ 强杀**（SIGKILL），再点顶部 **「🔄 重启发布端并恢复租约 / 令牌」**：
   报告里 `lease = {holder: A, token: 1, ...}`。
5. 再点一次 **同号重发**：仍返回同一份回执，不升版。
6. （可选）直接重启整个服务 / 容器：`docker compose restart gateway`。
   启动钩子自动 recover，租约、token、回执、版本都在；同号仍回放。

### C. 写入失败

租约有效时点 **「下一次写入触发 ENOSPC」**（或 EIO），再发布：任务 `failed`，
原因含 `write_failed` 与 `[ENOSPC] ...（演练注入）`；网关仍按 current.json 的
旧完整版响应；故障是一次性的，再发一次即成功。

---

## 6. 实测结果（本仓库交付前真实执行）

沙箱里没有 docker / 浏览器，因此：

- 用 pip（user site）安装 `Flask 3.1.3`，真实起服务 + 真实 publisher 子进程，
  运行两套自测（自找空闲端口、临时卷、支持硬重启服务）：

```text
$ python3 tests/selftest.py
...
全部通过：100 项断言

$ python3 tests/selftest_bigtable.py        # 默认 DRILL_ROUTE_COUNT=12000
...
大路由表自测全部通过：73 项断言
```

  原 100 项回归全绿，覆盖：网关 200/503/404 与版本头；8 线程并发申请恰一胜 +
  A/B 并发；续租 / 错 token / 临界过期 / 过期后 token 递增；正常发布；
  `stage_half` 与 `before_switch` 两条完整接管剧本；生效未回包同号回放、强杀、
  recover、**SIGKILL 整个 Flask 进程组后用同一卷硬重启**；ENOSPC/EIO 写失败。

  大路由表专项（12000 条，关闭后台巡检排除预热）实测：

  - 冷启动仅 1 次全量解析建立首代目录，之后所有请求 `full_parse_fallbacks`
    不再增加；热路径单请求 p50 ≈ **1.0 ms**，与 6 条小表（p50 ≈ 1.1 ms）同量级，
    不随路由表规模增长；
  - 切换后**首个请求自身**恰好完成 1 次代际检测 + 1 次侧车 O(1) 装载
    （`sig_changes_detected +1`、`sidecar_adoptions +1`、无全量解析），
    含装载耗时 ≈ 2.0 ms，随即返回正确 503 与 `X-Config-Version: 1`；
  - 只改停用清单时路由指纹不变、`route_index_reuses` 增长、目录不重建；
  - 暂停在切换前 / ENOSPC 写入失败时正文未换，网关持续按旧版响应；
  - 重置后版号复用（回到 v0 再发 v1），首个请求即按实际生效配置响应；
  - SIGKILL 硬重启 + 同一卷后首个请求经侧车冷装载即正确，回执仍可回放。

- 故障注入补充验证（多进程冷启动）：指针文件损坏、路由侧车缺失时自动降级为
  一次性全量解析且应答仍正确（`pointer_rejected` / `full_parse_fallbacks`
  如实计数）；`current.json` 缺失时用 last-good 快照继续服务并带
  `X-Config-Source: last-good-cache` 头。

- 用 jsdom 加载真实页面并执行真实 `static/app.js`（注入 fetch polyfill），
  对运行中的服务做交互冒烟：

```text
14/14 DOM checks passed
```

  包括目录指标渲染、路由下拉、在输入框直接键入 `/totally/unknown/path`
  得到 `HTTP 404` 与版本头、回车发送 `/health` 得到 `HTTP 200`，且无 JS
  运行时错误。`node --check` 通过。

- `docker-compose.yml` 已做 YAML 解析 / 结构静态校验（服务 / 端口 / 命名卷 /
  init 正确）。

---

## 7. 未完成项 / 已知边界（如实说明）

1. **沙箱无 docker 可执行文件**，所以镜像构建与 `docker compose up` 没有在本机
   实跑，只做了 YAML 校验和 Dockerfile / compose 的静态核对；容器路径
   （`/drill/...`、`/proc` 扫描、SIGSTOP/SIGCONT）均按 Linux 常规环境编写，
   但仍建议在有 Docker 的机器上首跑确认一次。
2. **没有真实浏览器 / 截图**：前端只通过 jsdom（无布局渲染）+ `node --check`
   验证；视觉效果与窄屏适配未做像素级检查。
3. Flask 用内置 dev server、**单进程 + threaded** 运行（演练工具定位）。
   子进程句柄表在进程内存里；若用多 worker gunicorn，信号管理会退化到仅靠
   SQLite 中的 pid（状态 / 恢复仍正确，但本仓库没有做多进程句柄共享）。
   路由目录的内存快照同样是**每进程一份**：多 worker 部署时各进程独立经
   侧车冷装载（侧车文件在共享卷上，正确性不受影响，只是首请求各自装载一次），
   compose 因此直接 `python app.py`。
4. 时钟是 **SQLite 里的模拟时钟**，不会随真实时间自动过期——这是题目要求的
   “可推进模拟时钟”；生产租约需要真实时间 / TTL 线程。
5. “切换已发起但未生效”建模为 **原子 `rename` 前的屏障**（rename 本身原子，
   不存在可观察的中间态）；没有实现两阶段提交。
6. 网关对启用路由返回的是 **mock 上游应答**，没有反代真实服务。
7. 磁盘配额条是 **统计 / 展示口径**（含已删打开 inode），真正的“写满”通过
   ENOSPC/EIO 故障注入演练，没有挂载小容量 loop 盘或用 quota 在内核层强制。
   路由侧车放在 state 目录而非配置卷，因此不进入“一正文一暂存”的槽位核算。
8. fencing 由发布端自觉执行 + SQLite 事务保证；它能挡住“本演练里恢复的旧
   发布进程”，但不是对任意恶意 / 绕过发布器的写入者做存储侧硬隔离。
9. 路由目录的代际校验依赖 current.json 的 **size + 头部版本号** 与指针匹配；
   极端情况下两份不同配置恰好等长且版本号相同（本演练版号单调递增，不会发生），
   校验会失效——此时最坏结果是读到旧代际的停用集合，下一次 stat 指纹变化即纠正。
10. 没有加登录鉴权（本地演练台），请勿直接暴露到公网。

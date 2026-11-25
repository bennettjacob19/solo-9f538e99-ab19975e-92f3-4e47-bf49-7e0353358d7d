# GSB 网关维护演练台（Flask + SQLite + 本地卷）

一个可交互的“配置网关 + 分布式租约接管 + fencing token + 发布暂停 / 中断恢复 +
幂等回执”演练环境。所有题目要求的关键行为都有真实进程 / 真实文件 / 真实锁支撑，
不是纯前端动画。

---

## 1. 它能演示什么

| 题目要求 | 实现方式 |
| --- | --- |
| Flask 网关从本地卷读 JSON | `GET /gw/<route>` 每次请求读 `DRILL_DATA_DIR/current.json`（原子 `os.replace` 发布），带 `X-Config-Version` |
| 停用路由返回 503，其余照常 | 命中 `disabled` → **503** + `Retry-After`；启用路由由 mock 上游回 200；未知路由 404 |
| 网页试发网关请求 | 左上卡片选路由 → 发送，展示状态码 / 版本头 / JSON 响应 |
| 推进模拟时钟 | 时钟存在 SQLite `kv` 表（初始 t=1000，TTL=30 模拟秒），支持 +10/+30/+60、自定义、一键“推进到租约过期” |
| 两个独立 worker 申请 / 续租 / 提交停用清单 | 页面两张独立的 A / B 卡片，各自保存“自认 token”和勾选集合；每次发布另起一个**独立 OS 子进程** |
| SQLite 保存租约和回执 | `lease` / `worker_state` / `receipts` / `versions` / `publish_runs` / `events` 等表，WAL + `busy_timeout` |
| 并发申请只准一个成功 | `BEGIN IMMEDIATE` 事务内“读租约→判定→发 token→写回”，写锁串行化竞争；败者 409 并带持有者 / 期限原因 |
| 新租约发递增 fencing token | `kv.next_token` 单调递增；只有“无租约 / 旧租约过期后接管”才发新号 |
| 三个暂停点 | 发布子进程在 ① 暂存写一半且 **fd 不关闭** ② 暂存完整、`os.replace` 前（切换已发起未生效）③ 已 replace 且回执已落库、返回前，把任务置 `paused` 并真实 `SIGSTOP` 自己 |
| 过期 + 旧进程暂停时，备用进程连续发布两版 | 推进时钟 → B 申请得 token+1 → B 连续发布 v1、v2，自动化用例完整跑通 |
| 旧进程恢复后的过期写入被拒绝，配置 / 版号不变 | 恢复后发布端在 SQLite 事务里再次 fencing，旧 token 被拒（`status=rejected`），不写版本 / 回执 / 文件；当前版仍停在 B 的 v2 |
| 磁盘最多 current + 一份 staging；已删但仍打开也算 | 固定两个正文文件名；新发布先 `unlink staging.json` 再建新 inode。磁盘面板通过 `/proc/<pid>/fd` 真实扫描“`(deleted)` 但仍打开”的幽灵文件、大小和归属，并按唯一 inode 统计占用 |
| 暂停 / 写失败 / 重启时网关按最近完整配置响应 | 网关只读 `current.json`；写失败保留半成品 staging 但不碰 current；进程 / 服务重启后 current 与 SQLite 都在；另有进程内 last-good 兜底 |
| 生效未回包时中断，重启恢复租约和令牌 | 生效点之后回执与版本已在同一 SQLite 事务落库；可 `SIGKILL` 发布端，`/api/publisher/recover`（以及 Flask 启动钩子）读回 holder/token，补完 `applied=0` 的切换，标记失联任务 |
| 同号重发返回原回执、不升版（即使原进程仍暂停） | `receipts.request_id` 主键幂等；`POST /api/publish` 先查回执，命中直接 `replayed:true` 返回原回执，不校验 token、不起进程、不升版 |
| 页面显示版号 / 持有者 / 令牌 / 期限 / 拒绝原因 | 顶部租约卡、两张 worker 卡、任务表、版本表、回执表、事件 / 拒绝原因流，每秒轮询 |
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
   └─ SQLite (DRILL_STATE_DIR/drill.db, WAL)
        租约 / fencing 序号 / 版本 / 幂等回执 / 任务 / 事件
本地卷 DRILL_DATA_DIR/
   ├─ current.json   ← 网关唯一读取的完整配置（os.replace 原子换入）
   └─ staging.json   ← 全局至多一份暂存正文（失败 / 半写时可能残留）
```

关键正确性设计：

1. **fencing 有两道卡**：Flask 接单时预校验一次（快速 409）；发布端子进程在
   “切换前的提交事务”里用 `BEGIN IMMEDIATE` 再校验一次 holder + token + 模拟期限。
   因此旧进程即使在暂停期间经历了过期和接管，恢复后也过不了第二道卡。
2. **版本 + 回执一个事务**：`versions` 与 `receipts` 在同一 `IMMEDIATE` 事务提交，
   然后才 `os.replace(staging, current)`。回执先于“回包暂停点”存在，所以原进程
   还 `T (stopped)` 时，同号重发就能从 SQLite 拿到原回执。
3. **崩溃恢复**：若死在“事务已提交、文件还没换”之间，`versions.applied=0`，
   recover 会用库里的 `config_json` 经 staging 原子补切换。
4. **磁盘核算不做假账**：Linux 下直接扫 `/proc/[0-9]*/fd/*` 的 readlink 与 fstat，
   `.../staging.json (deleted)` 且能 fstat 到大小的，就是“已删除但仍打开”的 inode。

---

## 3. 目录结构

```
app.py                 Flask：页面 / 网关 / 全部演练 API
drill/config.py        路径、TTL、配额、内置路由（环境变量可覆盖）
drill/db.py            SQLite schema、连接（WAL/busy_timeout）、模拟时钟、事件
drill/lease.py         并发安全的 acquire / renew / fence_check
drill/configstore.py   current.json 播种 / 读取（含 last-good 兜底）
drill/disk.py          卷文件 + /proc 已删打开句柄扫描与唯一 inode 占用
drill/runner.py        发布子进程拉起 / SIGCONT / SIGKILL / 回收 / 重置 / 恢复
drill/publisher.py     发布端状态机（可独立成进程：暂停点、fencing、提交、切换）
templates/index.html   演练页
static/app.js          原生 JS（轮询、两 worker、任务操作、网关试发）
static/style.css       暗色控制台样式
tests/selftest.py      100 项断言的端到端自测（stdlib client，自起真实服务）
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
`DRILL_LEASE_TTL`（默认 30 模拟秒）、`DRILL_DISK_QUOTA`。

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

- 用 `.venv` 安装 `Flask 3.1.3`，真实起服务 + 真实 publisher 子进程，运行
  `tests/selftest.py`（自找空闲端口、临时卷、支持硬重启服务）：

```text
$ .venv/bin/python tests/selftest.py
...
全部通过：100 项断言
```

  连续 3 轮均全绿。覆盖：网关 200/503/404 与版本头；8 线程并发申请恰一胜 +
  A/B 并发；续租 / 错 token / 临界过期 / 过期后 token 递增；正常发布；
  `stage_half` 与 `before_switch` 两条完整接管剧本（T 状态、半写文件、
  token=2、B 两版、`/proc` 幽灵 inode、恢复被 fencing 拒、版号配置不变、
  幽灵释放、槽位约束）；生效未回包同号回放、强杀、recover、**SIGKILL 整个
  Flask 进程组后用同一卷硬重启**仍保留租约 / token / v3 配置 / 回执且同号回放；
  ENOSPC/EIO 写失败时网关不受影响。

- 用 jsdom 加载真实页面并执行真实 `static/app.js`，对运行中的服务做交互冒烟：

```text
18/18 DOM checks passed
```

  包括租约 / token / 勾选框 / 任务 / 回执 / 磁盘渲染，真实点击“同号重发”
  出现回放提示且不升版、点 +10s 时钟变 t=1010、试发 `/api/checkout`
  渲染出 `HTTP 503` 与 `route_disabled`，且无 JS 运行时错误。`node --check` 通过。

- `docker-compose.yml` 已用 PyYAML 解析校验（服务 / 端口 / 命名卷 / init 正确）。

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
   compose 因此直接 `python app.py`。
4. 时钟是 **SQLite 里的模拟时钟**，不会随真实时间自动过期——这是题目要求的
   “可推进模拟时钟”；生产租约需要真实时间 / TTL 线程。
5. “切换已发起但未生效”建模为 **原子 `rename` 前的屏障**（rename 本身原子，
   不存在可观察的中间态）；没有实现两阶段提交。
6. 网关对启用路由返回的是 **mock 上游应答**，没有反代真实服务。
7. 磁盘配额条是 **统计 / 展示口径**（含已删打开 inode），真正的“写满”通过
   ENOSPC/EIO 故障注入演练，没有挂载小容量 loop 盘或用 quota 在内核层强制。
8. fencing 由发布端自觉执行 + SQLite 事务保证；它能挡住“本演练里恢复的旧
   发布进程”，但不是对任意恶意 / 绕过发布器的写入者做存储侧硬隔离。
9. 没有加登录鉴权（本地演练台），请勿直接暴露到公网。

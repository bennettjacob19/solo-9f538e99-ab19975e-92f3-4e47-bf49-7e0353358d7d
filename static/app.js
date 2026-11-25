/* GSB 演练台前端：纯原生 JS，每秒轮询 /api/state，操作后立即刷新。 */
"use strict";

const $ = (id) => document.getElementById(id);

function esc(v) {
  if (v === null || v === undefined) return "";
  return String(v).replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}

async function api(path, body, method = "POST") {
  try {
    const res = await fetch(path, {
      method,
      headers: { "Content-Type": "application/json" },
      body: body !== undefined ? JSON.stringify(body) : undefined,
    });
    const text = await res.text();
    let data = null;
    try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
    return { status: res.status, ok: res.ok, data };
  } catch (e) {
    return { status: 0, ok: false, data: { error: "网络错误: " + e } };
  }
}

function notice(kind, text, ms = 6500) {
  const box = $("notices");
  const div = document.createElement("div");
  div.className = "notice " + (kind === "ok" ? "ok" : kind === "err" ? "err" : "");
  div.textContent = text;
  box.appendChild(div);
  setTimeout(() => div.remove(), ms);
}

/* 每个 worker 各自维护勾选集合（两个独立 worker 看到 / 提交的清单可以不同） */
const checks = { A: new Set(), B: new Set() };
let checksInitialized = false;
let lastRoutesSig = "";

function workerTokenInput(h) {
  // 提交时使用该 worker “自认”的最近 token（可能已是过期 fencing token）
  return state?.worker_tokens?.[h] ?? null;
}

let state = null;

/* ---------------- 渲染 ---------------- */

function renderAll() {
  if (!state) return;
  $("sim-now").textContent = "t=" + state.sim_now;
  $("ttl").textContent = state.lease_ttl;
  $("data-dir").textContent = state.disk?.data_dir || "–";
  renderLease();
  renderWorkers();
  ensureChecks();
  renderGatewayOptions();
  renderConfig();
  renderDisk();
  renderRuns();
  renderReceipts();
  renderVersions();
  renderEvents();
  $("fault-state").textContent = state.fault_next_write
    ? "已武装：" + state.fault_next_write
    : "无故障";
  $("fault-state").className = "badge " + (state.fault_next_write ? "bad" : "");
}

function leaseBadge(lease) {
  if (!lease) return '<span class="badge bad">无有效租约</span>';
  if (lease.active) return '<span class="badge ok">有效</span>';
  return '<span class="badge bad">已过期</span>';
}

function renderLease() {
  const l = state.lease;
  if (!l) {
    $("lease-panel").innerHTML =
      '<span class="badge bad">无租约</span> ' +
      `<span class="muted">下一份新租约将发放 fencing token=${state.next_token}</span>`;
  } else {
    const remain = l.active ? `剩余 <b style="color:${l.remaining < 10 ? "var(--red)" : "var(--green)"}">${l.remaining}s</b>（模拟时间）`
      : `<b style="color:var(--red)">已过期 ${state.sim_now - l.expires_at}s</b>，可被接管`;
    $("lease-panel").innerHTML = `
      <div class="lease-big">
        <span>${leaseBadge(l)}</span>
        <span>持有者 <b style="font-size:16px">${esc(l.holder)}</b></span>
        <span class="token-pill">🎟 token ${esc(l.token)}</span>
        <span class="mono muted">期限 t=${esc(l.expires_at)}</span>
        <span>${remain}</span>
      </div>
      <div class="muted" style="font-size:12px">下一个新 token 序号：${state.next_token}（仅全新租约 / 过期后接管时递增发放）</div>`;
  }

  for (const h of ["A", "B"]) {
    $("who-" + h).textContent = `last-token=${state.worker_tokens?.[h] ?? "–"}`;
    const box = $("worker-lease-" + h);
    const mine = state.worker_tokens?.[h];
    if (!l) {
      box.innerHTML = `无有效租约；点「申请租约」获取 <b>token ${state.next_token}</b>` +
        (mine ? `<br><span class="muted">本 worker 仍保留旧 token=${mine}</span>` : "");
    } else if (l.holder === h && l.active) {
      box.innerHTML = `${leaseBadge(l)} 本 worker 持有中 · token=<b>${l.token}</b> · ` +
        `到期 t=${l.expires_at} · 剩余 ${l.remaining}s`;
    } else if (l.active) {
      box.innerHTML = `${leaseBadge(l)} 现由 <b>${esc(l.holder)}</b> 持有 · token=${l.token} · 剩余 ${l.remaining}s` +
        (mine !== undefined ? `<br><span style="color:#fca5a5">本 worker 自认 token=${mine}（提交 / 恢复时会被 fencing 校验）</span>` : "");
    } else {
      box.innerHTML = `${leaseBadge(l)} 旧持有者 <b>${esc(l.holder)}</b> token=${l.token} 已到期 t=${l.expires_at}` +
        (mine !== undefined ? `<br><span style="color:#fca5a5">${h} 保留的 token=${mine} 现已失效，恢复旧发布进程将被拒绝</span>` : "");
    }
    renderReject(h);
  }
}

function latestRejection(h) {
  // runs 已按开始时间倒序：优先展示该 worker 最近一次被 fencing 拒绝 / 失败的具体原因
  const rejectRun = (state.runs || []).find(
    (r) => r.holder === h && (r.status === "rejected" || r.status === "failed"));
  if (rejectRun?.reason_detail) return rejectRun.reason_detail;
  if (rejectRun?.reason_code) return rejectRun.reason_code;
  const denyEvt = (state.events || []).find(
    (e) => e.holder === h && (e.level === "deny" || e.level === "error"));
  return denyEvt ? denyEvt.message : "";
}

function renderReject(h) {
  const box = $("reject-" + h);
  const msg = latestRejection(h);
  if (msg) {
    box.classList.add("show");
    box.textContent = "⛔ 最近拒绝 / 失败原因：" + msg;
  } else {
    box.classList.remove("show");
    box.textContent = "";
  }
}

function renderWorkers() {
  // 勾选框只在路由集合变化时重建，避免轮询打断用户勾选
  ensureChecks();
}

function ensureChecks() {
  const routes = state?.config?.routes || [];
  const sig = routes.map((r) => r.path).join("|");
  if (sig !== lastRoutesSig) {
    lastRoutesSig = sig;
    for (const h of ["A", "B"]) {
      // 首次见到路由：用当前已发布停用清单初始化
      if (!checksInitialized) {
        checks[h] = new Set(state?.config?.disabled || []);
      }
      const wrap = $("checks-" + h);
      wrap.innerHTML = routes.map((r) => `
        <label title="${esc(r.note || "")}">
          <input type="checkbox" data-holder="${h}" data-path="${esc(r.path)}"
            ${checks[h].has(r.path) ? "checked" : ""}>
          <span class="mono">${esc(r.path)}</span>
          <span class="muted">${esc(r.note || "")}</span>
        </label>`).join("");
      wrap.querySelectorAll('input[type=checkbox]').forEach((cb) => {
        cb.addEventListener("change", () => {
          const p = cb.getAttribute("data-path");
          if (cb.checked) checks[h].add(p); else checks[h].delete(p);
        });
      });
    }
    checksInitialized = true;
  }
}

function renderGatewayOptions() {
  const sel = $("gw-route");
  const cur = sel.value;
  const routes = state?.config?.routes || [];
  const opts = ['<option value="/">/ （网关索引）</option>']
    .concat(routes.map((r) => `<option value="${esc(r.path)}">${esc(r.path)} — ${esc(r.note || "")}</option>`));
  sel.innerHTML = opts.join("");
  if (cur && [...sel.options].some((o) => o.value === cur)) sel.value = cur;
}

function renderConfig() {
  const c = state.config || {};
  const disabled = new Set(c.disabled || []);
  $("config-summary").innerHTML = `
    <div class="lease-big">
      <span class="badge info">version ${esc(c.version)}</span>
      <span class="muted">request_id <span class="mono">${esc(c.request_id)}</span></span>
      ${state.config_from_cache ? '<span class="badge warn">读取失败，使用内存 last-good 缓存</span>' : ""}
      <span>停用：${disabled.size ? [...disabled].map((d) => `<span class="badge bad">${esc(d)}</span>`).join(" ") : '<span class="badge ok">无</span>'}</span>
    </div>`;
  const rows = (c.routes || []).map((r) => {
    const off = disabled.has(r.path);
    return `<tr>
      <td class="mono">${esc(r.path)}</td>
      <td class="muted">${esc(r.note || "")}</td>
      <td>${off ? '<span class="badge bad">停用 → 503</span>' : '<span class="badge ok">正常 → 200</span>'}</td>
    </tr>`;
  }).join("");
  $("route-table").innerHTML = `<table class="data"><thead><tr><th>路由</th><th>说明</th><th>网关行为</th></tr></thead><tbody>${rows}</tbody></table>`;
}

function fmtBytes(n) {
  if (n < 1024) return n + " B";
  return (n / 1024).toFixed(1) + " KiB";
}

function renderDisk() {
  const d = state.disk || {};
  if (d.error) { $("disk-panel").textContent = "磁盘扫描失败：" + d.error; return; }
  const pct = Math.min(100, d.quota_used_pct || 0);
  const files = (d.named_files || []).map((f) => `
    <tr>
      <td class="mono">${esc(f.name)} ${f.is_body ? "" : '<span class="badge warn">非正文</span>'}</td>
      <td>${fmtBytes(f.size)}</td>
      <td class="mono muted">ino ${f.inode}</td>
    </tr>`).join("");
  const fds = (d.open_files || []).length ? (d.open_files || []).map((f) => `
    <tr style="${f.deleted_open ? "color:#fca5a5" : ""}">
      <td class="mono">pid ${f.pid}:${esc(f.fd)}</td>
      <td class="mono">${esc(f.path)}${f.deleted_open ? ' <span class="badge ghost">已删除·仍打开</span>' : ""}</td>
      <td>${fmtBytes(f.size)}</td>
      <td>${f.holder ? `${esc(f.holder)} · token ${f.token} · <span class="mono">${esc(f.request_id)}</span>` : '<span class="muted">–</span>'}</td>
    </tr>`).join("") : '<tr><td colspan="4" class="empty">data 卷内没有被进程打开的 fd</td></tr>';

  $("disk-panel").innerHTML = `
    <div class="row" style="justify-content:space-between">
      <span>${d.slot_invariant_ok ? '<span class="badge ok">正文槽位合规</span>' : '<span class="badge bad">槽位异常</span>'}
        正文文件 ${d.body_file_count}/2（current + staging）</span>
      <span class="mono muted">${fmtBytes(d.total_bytes)} / 配额 ${fmtBytes(d.quota_bytes)}（含幽灵 ${fmtBytes(d.ghost_bytes)}）</span>
    </div>
    <div class="disk-bar"><i class="${pct > 80 ? "hot" : ""}" style="width:${Math.max(1, pct)}%"></i></div>
    ${(d.unexpected_files || []).length ? `<div class="reject show">意外文件：${esc(d.unexpected_files.join(", "))}</div>` : ""}
    <div class="scroll" style="max-height:150px"><table class="data">
      <thead><tr><th>卷内目录项</th><th>大小</th><th>inode</th></tr></thead><tbody>${files || '<tr><td colspan=3 class=empty>空卷</td></tr>'}</tbody>
    </table></div>
    <label class="field-label">打开的卷内文件句柄（/proc 实测，含已删除未释放 inode）</label>
    <div class="scroll" style="max-height:170px"><table class="data">
      <thead><tr><th>pid:fd</th><th>路径</th><th>大小</th><th>归属任务</th></tr></thead><tbody>${fds}</tbody>
    </table></div>`;
}

function statusClass(s) { return "st-" + esc(s); }

function renderRuns() {
  const runs = state.runs || [];
  if (!runs.length) { $("runs-panel").innerHTML = '<div class="empty">还没有发布任务。</div>'; return; }
  const rows = runs.map((r) => {
    const canResume = r.status === "paused";
    const canKill = r.status === "paused" || r.status === "running" || r.status === "queued";
    const disabled = JSON.parse(r.disabled_json || "[]");
    const btns = `
      ${canResume ? `<button class="act resume" data-act="resume" data-run="${r.run_id}">▶ 恢复</button>` : ""}
      ${canKill ? `<button class="act kill" data-act="kill" data-run="${r.run_id}">✕ 强杀</button>` : ""}
      <button class="act resend" data-act="resend" data-run="${r.run_id}">⟳ 同号重发</button>`;
    return `<tr>
      <td>
        <div class="mono"><b>${esc(r.request_id)}</b></div>
        <div class="muted" style="font-size:10.5px">${esc(r.run_id.slice(0, 8))} · 暂停点 ${esc(r.pause_at)}</div>
        <div style="margin-top:2px">${disabled.map((d) => `<span class="badge bad" style="font-size:10px">${esc(d)}</span>`).join(" ") || '<span class="muted">不停用</span>'}</div>
      </td>
      <td>${esc(r.holder)}<br><span class="mono muted">tok ${r.token}</span></td>
      <td>${r.pid ? `<span class="mono">${r.pid}</span>` : "–"}
        ${r.proc_state ? `<br><span class="proc-state">/proc=${esc(r.proc_state)}${r.proc_state === "T" ? " (stopped)" : ""}</span>` : ""}</td>
      <td><span class="status-pill ${statusClass(r.status)}">${esc(r.status)}</span>
          <div class="muted" style="font-size:10.5px;margin-top:2px">${esc(r.phase)}</div>
          ${r.bytes_total ? `<div class="mono muted" style="font-size:10.5px">${r.bytes_written || 0}/${r.bytes_total} B</div>` : ""}
      </td>
      <td style="max-width:260px">${r.reason_detail ? `<div class="reason-detail">${esc(r.reason_detail)}</div>` : (r.reason_code ? `<span class="muted">${esc(r.reason_code)}</span>` : "")}</td>
      <td style="white-space:nowrap">${btns}</td>
    </tr>`;
  }).join("");
  $("runs-panel").innerHTML = `<div class="scroll"><table class="data">
    <thead><tr><th>请求 / 停用清单</th><th>worker</th><th>pid</th><th>状态 / 阶段</th><th>拒绝 / 原因</th><th>操作</th></tr></thead>
    <tbody>${rows}</tbody></table></div>`;
}

function renderReceipts() {
  const rs = state.receipts || [];
  if (!rs.length) { $("receipts-panel").innerHTML = '<div class="empty">暂无回执（发布生效后写入 SQLite）。</div>'; return; }
  $("receipts-panel").innerHTML = `<div class="scroll"><table class="data">
    <thead><tr><th>请求号</th><th>版本</th><th>持有者 / token</th><th>来源</th></tr></thead>
    <tbody>${rs.map((r) => `<tr>
      <td class="mono">${esc(r.request_id)}</td>
      <td><span class="badge info">v${r.version}</span></td>
      <td>${esc(r.holder)} / <span class="mono">${r.token}</span></td>
      <td>${r.recovered ? '<span class="badge warn">恢复补录</span>' : '<span class="muted">正常提交</span>'}<div class="muted mono" style="font-size:10px">sim t=${r.created_sim}</div></td>
    </tr>`).join("")}</tbody></table></div>`;
}

function renderVersions() {
  const vs = state.versions || [];
  if (!vs.length) { $("versions-panel").innerHTML = '<div class="empty">暂无版本。</div>'; return; }
  $("versions-panel").innerHTML = `<div class="scroll" style="max-height:240px"><table class="data">
    <thead><tr><th>版号</th><th>请求号</th><th>持有者 / token</th><th>文件切换</th><th>sim</th></tr></thead>
    <tbody>${vs.map((v) => `<tr>
      <td><b>v${v.version}</b></td>
      <td class="mono">${esc(v.request_id)}</td>
      <td>${esc(v.holder)} / ${v.token}</td>
      <td>${v.applied ? '<span class="badge ok">applied</span>' : '<span class="badge warn">pending</span>'}</td>
      <td class="mono muted">${v.created_sim}</td>
    </tr>`).join("")}</tbody></table></div>`;
}

function renderEvents() {
  const es = state.events || [];
  $("events-panel").innerHTML = es.map((e) => `
    <div class="event ${esc(e.level)}">
      <span class="meta">#${e.id} · sim t=${e.sim_now} · ${esc(e.kind)}${e.holder ? " · " + esc(e.holder) : ""}${e.request_id ? " · " + esc(e.request_id) : ""}</span>
      <span class="msg">${esc(e.message)}</span>
    </div>`).join("");
}

/* ---------------- 数据刷新 ---------------- */

async function refresh() {
  const res = await api("/api/state", undefined, "GET");
  if (res.ok) {
    state = res.data;
    renderAll();
  }
}

/* ---------------- 操作 ---------------- */

async function doAcquire(h) {
  const r = await api("/api/lease/acquire", { holder: h });
  if (r.ok) notice("ok", `✅ ${h}: ${r.data.status === "granted" ? "获得新租约" : "已是持有者"}，token=${r.data.lease.token}，期限 t=${r.data.lease.expires_at}`);
  else notice("err", "⛔ " + (r.data?.error || r.status));
  refresh();
}

async function doRenew(h) {
  const token = workerTokenInput(h);
  const r = await api("/api/lease/renew", { holder: h, token });
  if (r.ok) notice("ok", `🔄 ${h} 续租成功，新期限 t=${r.data.lease.expires_at}`);
  else notice("err", "⛔ " + (r.data?.error || r.status));
  refresh();
}

function nextReqId(h) {
  const input = $("reqid-" + h);
  const m = input.value.match(/^(.*?)(\d+)$/);
  if (m) input.value = m[1] + String(Number(m[2]) + 1).padStart(m[2].length, "0");
  else input.value = input.value + "-2";
}

async function doPublish(h) {
  const request_id = $("reqid-" + h).value.trim();
  const pause_at = $("pause-" + h).value;
  const token = workerTokenInput(h);
  const disabled = [...checks[h]];
  if (token === null || token === undefined) {
    notice("err", `${h} 还没有申请过租约（无自认 token），请先申请`);
    return;
  }
  const r = await api("/api/publish", { holder: h, request_id, token, disabled, pause_at });
  if (r.status === 202) {
    notice("ok", `🚀 发布端已启动 pid=${r.data.pid} run=${r.data.run_id.slice(0, 8)}` +
      (pause_at !== "none" ? `，将在「${pause_at}」暂停` : ""));
    nextReqId(h);
  } else if (r.ok && r.data?.replayed) {
    notice("ok", `↩️ 同号重发放回原回执：${request_id} → v${r.data.receipt.version}，未升版`);
  } else {
    notice("err", "⛔ " + (r.data?.error || ("HTTP " + r.status)));
  }
  refresh();
}

async function raceAB() {
  notice("", "⚡ 已让 A / B 同时发出申请…");
  const [ra, rb] = await Promise.all([
    api("/api/lease/acquire", { holder: "A" }),
    api("/api/lease/acquire", { holder: "B" }),
  ]);
  const fmt = (h, r) => r.ok
    ? `${h} ✅ ${r.data.status} token=${r.data.lease.token}`
    : `${h} ⛔ ${r.data?.error_code || r.status}`;
  const aWin = ra.ok, bWin = rb.ok;
  notice(aWin !== bWin ? "ok" : "err",
    `并发结果（恰好一个成功）：\n${fmt("A", ra)}\n${fmt("B", rb)}`, 9000);
  refresh();
}

async function runAction(act, runId, btn) {
  if (act === "resume") {
    const r = await api(`/api/runs/${runId}/resume`, {});
    if (!r.ok) notice("err", r.data?.error || "恢复失败");
    else notice("ok", "▶ 已发送 SIGCONT");
  } else if (act === "kill") {
    const r = await api(`/api/runs/${runId}/kill`, {});
    if (!r.ok) notice("err", r.data?.error || "强杀失败");
    else notice("ok", "✕ 已发送 SIGKILL，发布端中断（租约 / 回执 / token 仍在 SQLite）");
  } else if (act === "resend") {
    const run = state.runs.find((x) => x.run_id === runId);
    const r = await api("/api/publish", {
      request_id: run.request_id, holder: run.holder, token: run.token,
      disabled: JSON.parse(run.disabled_json || "[]"), pause_at: "none",
    });
    if (r.ok && r.data?.replayed) {
      notice("ok", `↩️ 同号 ${run.request_id} 返回原回执 v${r.data.receipt.version}（replayed=true），版号不变`, 9000);
    } else if (r.status === 202) {
      notice("ok", `🔁 原请求未生成回执，已用同号重新启动发布 run=${r.data.run_id.slice(0, 8)}`);
    } else {
      notice("err", "⛔ 重发被拒：" + (r.data?.error || r.status), 9000);
    }
  }
  refresh();
}

async function sendGateway() {
  const path = $("gw-route").value;
  const url = "/gw" + (path === "/" ? "/" : path);
  const box = $("gw-result");
  box.textContent = "请求中…";
  try {
    const res = await fetch(url, { headers: { Accept: "application/json" } });
    const text = await res.text();
    let pretty = text;
    try { pretty = JSON.stringify(JSON.parse(text), null, 2); } catch { /* plain */ }
    const cls = res.status < 300 ? "s-2" : res.status < 500 ? "s-4" : "s-5";
    box.innerHTML =
      `<div><span class="gw-status ${cls}">HTTP ${res.status}</span>` +
      `<span class="muted">X-Config-Version: ${esc(res.headers.get("X-Config-Version"))}</span>` +
      (res.headers.get("X-Config-Source") ? ` <span class="badge warn">${esc(res.headers.get("X-Config-Source"))}</span>` : "") +
      `</div><pre style="margin:8px 0 0;white-space:pre-wrap">${esc(pretty)}</pre>`;
  } catch (e) {
    box.textContent = "请求失败：" + e;
  }
}

/* ---------------- 事件绑定 ---------------- */

document.addEventListener("click", async (ev) => {
  const clockBtn = ev.target.closest("[data-clock]");
  if (clockBtn) {
    await api("/api/clock/advance", { seconds: Number(clockBtn.getAttribute("data-clock")) });
    return refresh();
  }
  const faultBtn = ev.target.closest("[data-fault]");
  if (faultBtn) {
    const mode = faultBtn.getAttribute("data-fault");
    const r = await api("/api/fault", { mode });
    if (r.ok) notice("ok", mode ? "已武装下一次写入故障：" + mode : "已清除故障注入");
    return refresh();
  }
  const actionBtn = ev.target.closest("[data-action]");
  if (actionBtn) {
    const h = actionBtn.getAttribute("data-holder");
    const act = actionBtn.getAttribute("data-action");
    if (act === "acquire") return doAcquire(h);
    if (act === "renew") return doRenew(h);
    if (act === "publish") return doPublish(h);
    if (act === "new-reqid") return nextReqId(h);
    if (act === "sync-checks") {
      checks[h] = new Set(state?.config?.disabled || []);
      lastRoutesSig = "";  // 强制用新集合重建勾选框
      ensureChecks();
      notice("ok", `${h} 的勾选已同步为当前已发布停用清单`);
      return;
    }
  }
  const runBtn = ev.target.closest("[data-act]");
  if (runBtn && $("runs-panel").contains(runBtn)) {
    return runAction(runBtn.getAttribute("data-act"), runBtn.getAttribute("data-run"), runBtn);
  }
});

$("clock-expire").addEventListener("click", async () => {
  const r = await api("/api/clock/advance", { to_expire: true });
  if (!r.ok) notice("err", r.data?.error || "无法推进到过期");
  refresh();
});
$("clock-custom-btn").addEventListener("click", async () => {
  const v = Number($("clock-custom").value);
  if (!v || v <= 0) return notice("err", "请输入正的秒数");
  const r = await api("/api/clock/advance", { seconds: v });
  if (!r.ok) notice("err", r.data?.error);
  refresh();
});
$("reset-btn").addEventListener("click", async () => {
  if (!confirm("确定重置整个演练？所有发布进程会被杀死，SQLite 与卷配置清空后重新播种。")) return;
  await api("/api/admin/reset", {});
  location.reload();
});
$("race-ab").addEventListener("click", raceAB);
$("recover-btn").addEventListener("click", async () => {
  const r = await api("/api/publisher/recover", {});
  if (r.ok) {
    const rep = r.data.report;
    notice("ok", "🔄 恢复报告：\n" + JSON.stringify({
      lease: rep.lease ? { holder: rep.lease.holder, token: rep.lease.token, expires_at: rep.lease.expires_at, active: rep.lease.active } : null,
      pending_switches: rep.pending_switches,
      recovered_runs: rep.recovered_runs.length,
    }, null, 2), 10000);
  } else notice("err", r.data?.error || "恢复失败");
  refresh();
});
$("gw-send").addEventListener("click", sendGateway);
$("gw-route").addEventListener("change", () => {});

refresh();
setInterval(refresh, 1000);

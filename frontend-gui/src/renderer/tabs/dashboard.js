import * as api from "../api.js";
import { UI_POLL_INTERVAL_MS, UI_TEXT } from "../constants.js";
import { formatAgentStageLabel, formatDurationMs, formatStatusLabel, statusToTagClass } from "../agent_ui.js";
import { createEventManager, debounce } from "../utils.js";
import { PollManager } from "../poll_manager.js";

export function bind(section, onStatusChange) {
  const eventManager = createEventManager();

  const refreshBtn = section.querySelector("#dashboard-refresh-btn");
  const metaEl = section.querySelector("#dashboard-agent-meta");
  const planEl = section.querySelector("#dashboard-agent-plan");
  const thoughtsEl = section.querySelector("#dashboard-agent-thoughts");
  const activityEl = section.querySelector("#dashboard-activity");

  const pollManager = new PollManager();
  const DASHBOARD_POLL_KEY = "dashboard";

  function setListEmpty(el, text) {
    if (!el) return;
    el.innerHTML = `<div class="panel-empty-text">${text}</div>`;
  }

  function renderPlan(planObj) {
    if (!planEl) return;
    const items = planObj?.items;
    if (!Array.isArray(items) || items.length === 0) {
      setListEmpty(planEl, UI_TEXT.DASH || "-");
      return;
    }
    planEl.innerHTML = "";
    items.forEach((item) => {
      const li = document.createElement("div");
      li.className = "panel-list-item";

      const text = document.createElement("div");
      text.className = "panel-list-item-content";
      text.textContent = String(item?.brief || item?.title || "").trim() || UI_TEXT.DASH;

      const tag = document.createElement("span");
      tag.className = `panel-tag ${statusToTagClass(item?.status)}`.trim();
      tag.textContent = formatStatusLabel(item?.status);

      li.appendChild(text);
      li.appendChild(tag);
      planEl.appendChild(li);
    });
  }

  function renderThoughts(stateObj) {
    if (!thoughtsEl) return;

    const lines = [];
    const paused = stateObj?.paused;
    if (paused && typeof paused === "object") {
      const q = String(paused.question || "").trim();
      if (q) lines.push(`【等待输入】${q}`);
    }
    const obs = stateObj?.observations;
    if (Array.isArray(obs) && obs.length) {
      // 只显示最近若干条，避免撑爆主面板
      const tail = obs.slice(Math.max(0, obs.length - 10));
      tail.forEach((o) => {
        const t = String(o || "").trim();
        if (t) lines.push(t);
      });
    }

    if (lines.length === 0) {
      setListEmpty(thoughtsEl, UI_TEXT.DASH || "-");
      return;
    }

    thoughtsEl.innerHTML = "";
    lines.forEach((line) => {
      const li = document.createElement("div");
      li.className = "panel-list-item";
      li.textContent = line;
      thoughtsEl.appendChild(li);
    });
  }

  function truncateInline(text, maxLen = 160) {
    const value = String(text || "").replace(/\s+/g, " ").trim();
    if (!value) return "";
    if (!Number.isFinite(maxLen) || maxLen <= 0) return value;
    if (value.length <= maxLen) return value;
    return `${value.slice(0, maxLen).trimEnd()}…`;
  }

  function firstLine(text) {
    const raw = String(text || "");
    const idx = raw.indexOf("\n");
    const line = (idx >= 0 ? raw.slice(0, idx) : raw).trim();
    return line;
  }

  function formatTimeForActivity(rawTs) {
    const ts = String(rawTs || "").trim();
    if (!ts) return "";
    const parsed = Date.parse(ts);
    if (!Number.isFinite(parsed)) return ts;
    const d = new Date(parsed);
    const yyyy = String(d.getFullYear());
    const mm = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    const hh = String(d.getHours()).padStart(2, "0");
    const mi = String(d.getMinutes()).padStart(2, "0");
    const ss = String(d.getSeconds()).padStart(2, "0");
    return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
  }

  function normalizeRecordType(type) {
    return String(type || "").trim().toLowerCase();
  }

  function formatRecordTypeLabel(type) {
    switch (normalizeRecordType(type)) {
      case "run":
        return UI_TEXT.RECORD_TYPE_RUN_LABEL || "run";
      case "step":
        return UI_TEXT.RECORD_TYPE_STEP_LABEL || "step";
      case "output":
        return UI_TEXT.RECORD_TYPE_OUTPUT_LABEL || "output";
      case "llm":
        return UI_TEXT.RECORD_TYPE_LLM_LABEL || "llm";
      case "tool":
        return UI_TEXT.RECORD_TYPE_TOOL_LABEL || "tool";
      case "memory":
        return UI_TEXT.RECORD_TYPE_MEMORY_LABEL || "memory";
      case "skill":
        return UI_TEXT.RECORD_TYPE_SKILL_LABEL || "skill";
      case "agent_review":
        return UI_TEXT.RECORD_TYPE_AGENT_REVIEW_LABEL || "agent_review";
      default:
        return String(type || "").trim() || (UI_TEXT.DASH || "-");
    }
  }

  function reviewStatusToTagClass(status) {
    const v = String(status || "").trim().toLowerCase();
    switch (v) {
      case "pass":
      case "done":
      case "success":
        return "panel-tag--success";
      case "needs_changes":
      case "warning":
        return "panel-tag--warning";
      case "fail":
      case "failed":
      case "error":
        return "panel-tag--error";
      default:
        return "panel-tag--accent";
    }
  }

  function toolReuseStatusToTagClass(status) {
    const v = String(status || "").trim().toLowerCase();
    if (v === "pass" || v === "ok" || v === "success") return "panel-tag--success";
    if (v === "fail" || v === "failed" || v === "error") return "panel-tag--error";
    return "";
  }

  function debugLevelToTagClass(level) {
    const v = String(level || "").trim().toLowerCase();
    if (v === "error" || v === "failed" || v === "fail") return "panel-tag--error";
    if (v === "warning" || v === "warn") return "panel-tag--warning";
    if (v === "info") return "panel-tag--accent";
    if (v === "debug") return "";
    return "";
  }

  function prettyDetailText(text) {
    const raw = String(text || "").trim();
    if (!raw) return "";
    const looksLikeJson = raw.startsWith("{") || raw.startsWith("[");
    if (looksLikeJson) {
      try {
        return JSON.stringify(JSON.parse(raw), null, 2);
      } catch (e) {}
    }
    return raw;
  }

  function buildActivityItemModel(item) {
    const it = item && typeof item === "object" ? item : {};
    const type = normalizeRecordType(it.type);

    const tsRaw = String(it.timestamp || "").trim();
    const tsText = formatTimeForActivity(tsRaw);
    const taskId = it.task_id != null ? Number(it.task_id) : null;
    const runId = it.run_id != null ? Number(it.run_id) : null;
    const taskTitle = truncateInline(String(it.task_title || ""), 72);

    const status = String(it.status || "").trim();
    const title = String(it.title || "").trim();
    const summary = String(it.summary || "").trim();
    const detailRaw = String(it.detail || "").trim();
    const detail = prettyDetailText(detailRaw);

    const metaParts = [];
    if (Number.isFinite(taskId) && taskId > 0) metaParts.push(`task#${taskId}`);
    if (Number.isFinite(runId) && runId > 0) metaParts.push(`run#${runId}`);
    if (type === "tool" && it.ref_id != null) metaParts.push(`tool#${it.ref_id}`);
    if (taskTitle && type !== "run") metaParts.push(taskTitle);

    let primary = "";
    let secondary = "";
    let tagText = "";
    let tagClass = "";

    if (type === "run") {
      primary = taskTitle || truncateInline(summary, 72) || `run#${Number.isFinite(runId) ? runId : "-"}`;
      secondary = summary;
      tagText = formatStatusLabel(status);
      tagClass = statusToTagClass(status);
    } else if (type === "step") {
      primary = title || UI_TEXT.DASH || "-";
      secondary = "";
      tagText = formatStatusLabel(status);
      tagClass = statusToTagClass(status);
    } else if (type === "output") {
      const outType = String(summary || "").trim();
      const outTypeNorm = outType.toLowerCase();
      if (outTypeNorm === "debug") {
        let parsed = null;
        try {
          parsed = JSON.parse(detailRaw);
        } catch (e) {}
        const debugObj = parsed && typeof parsed === "object" ? parsed : null;
        const debugMsg = debugObj ? String(debugObj.message || "").trim() : "";
        const debugLevel = debugObj ? String(debugObj.level || "").trim() : "";
        primary = debugMsg || truncateInline(firstLine(detailRaw) || detailRaw, 160) || UI_TEXT.DASH || "-";
        secondary = "";
        tagText = debugLevel || outType || (UI_TEXT.DASH || "-");
        tagClass = debugLevelToTagClass(debugLevel);
      } else {
        primary = truncateInline(firstLine(detailRaw) || detailRaw, 160) || UI_TEXT.DASH || "-";
        secondary = "";
        tagText = outType || (UI_TEXT.DASH || "-");
        tagClass = "";
      }
    } else if (type === "llm") {
      primary = summary || UI_TEXT.RECORD_LLM_DEFAULT_LABEL || "LLM";
      const promptPreview = truncateInline(firstLine(title) || title, 160);
      secondary = promptPreview || truncateInline(firstLine(detailRaw), 160);
      tagText = formatStatusLabel(status);
      tagClass = statusToTagClass(status);
    } else if (type === "tool") {
      primary = title || (it.ref_id != null ? `tool#${it.ref_id}` : (UI_TEXT.DASH || "-"));
      secondary = truncateInline(summary, 160);
      tagText = status || (UI_TEXT.DASH || "-");
      tagClass = toolReuseStatusToTagClass(status);
    } else if (type === "memory") {
      primary = truncateInline(firstLine(detailRaw) || detailRaw, 160) || UI_TEXT.DASH || "-";
      secondary = "";
      tagText = status || (UI_TEXT.DASH || "-");
      tagClass = "";
    } else if (type === "skill") {
      primary = title || UI_TEXT.DASH || "-";
      const parts = [];
      if (status) parts.push(status);
      if (summary) parts.push(`v${summary}`);
      secondary = parts.join(" ");
      tagText = status || (UI_TEXT.DASH || "-");
      tagClass = "";
    } else if (type === "agent_review") {
      primary = summary || `review#${it.id != null ? it.id : "-"}`;
      secondary = taskTitle ? taskTitle : "";
      tagText = status || (UI_TEXT.DASH || "-");
      tagClass = reviewStatusToTagClass(status);
    } else {
      primary = title || summary || truncateInline(firstLine(detailRaw) || detailRaw, 160) || UI_TEXT.DASH || "-";
      secondary = "";
      tagText = status || (UI_TEXT.DASH || "-");
      tagClass = "";
    }

    return {
      type: type || String(it.type || "").trim(),
      typeLabel: formatRecordTypeLabel(type || it.type),
      tsRaw,
      tsText: tsText || tsRaw || (UI_TEXT.DASH || "-"),
      metaText: metaParts.join(" / "),
      primary,
      secondary,
      detail,
      tagText,
      tagClass,
    };
  }

  function createActivityItemEl(item) {
    const model = buildActivityItemModel(item);

    const row = document.createElement("div");
    row.className = "panel-list-item dashboard-activity-item";

    const main = document.createElement("div");
    main.className = "dashboard-activity-main";

    const meta = document.createElement("div");
    meta.className = "dashboard-activity-meta";

    const timeEl = document.createElement("span");
    timeEl.className = "dashboard-activity-time";
    timeEl.textContent = model.tsText || (UI_TEXT.DASH || "-");
    if (model.tsRaw) timeEl.title = model.tsRaw;

    const typeTag = document.createElement("span");
    typeTag.className = "panel-tag dashboard-activity-type-tag";
    typeTag.textContent = model.typeLabel || (UI_TEXT.DASH || "-");

    const ctx = document.createElement("span");
    ctx.className = "dashboard-activity-context";
    ctx.textContent = model.metaText || "";
    if (model.metaText) ctx.title = model.metaText;

    meta.appendChild(timeEl);
    meta.appendChild(typeTag);
    if (model.metaText) meta.appendChild(ctx);

    const title = document.createElement("div");
    title.className = "dashboard-activity-title";
    title.textContent = model.primary || (UI_TEXT.DASH || "-");

    const sub = document.createElement("div");
    sub.className = "dashboard-activity-sub";
    sub.textContent = model.secondary || "";
    if (!model.secondary) sub.classList.add("is-hidden");

    const detail = document.createElement("pre");
    detail.className = "dashboard-activity-detail";
    detail.textContent = model.detail || "";

    const hasDetail = !!String(model.detail || "").trim();
    if (!hasDetail) detail.classList.add("is-hidden");

    const isLong = hasDetail && (String(model.detail || "").length > 220 || String(model.detail || "").includes("\n"));
    if (hasDetail && isLong) {
      detail.classList.add("is-collapsed");

      const toggleBtn = document.createElement("button");
      toggleBtn.type = "button";
      toggleBtn.className = "panel-button panel-button--small dashboard-activity-toggle";
      toggleBtn.textContent = UI_TEXT.BUTTON_EXPAND || "展开";
      toggleBtn.addEventListener("click", (e) => {
        e.preventDefault();
        e.stopPropagation();
        const expanded = detail.classList.toggle("is-expanded");
        toggleBtn.textContent = expanded ? (UI_TEXT.BUTTON_COLLAPSE || "收起") : (UI_TEXT.BUTTON_EXPAND || "展开");
      });
      meta.appendChild(toggleBtn);
    }

    const tags = document.createElement("div");
    tags.className = "dashboard-activity-tags";

    const tag = document.createElement("span");
    tag.className = `panel-tag ${model.tagClass || ""}`.trim();
    tag.textContent = model.tagText || (UI_TEXT.DASH || "-");
    tags.appendChild(tag);

    main.appendChild(meta);
    main.appendChild(title);
    main.appendChild(sub);
    main.appendChild(detail);

    row.appendChild(main);
    row.appendChild(tags);
    return row;
  }

  async function updateActivity(options = {}) {
    if (!activityEl) return;
    // 避免轮询时频繁清空导致闪烁：仅在首次为空时显示 loading
    if (!String(activityEl.textContent || "").trim()) {
      setListEmpty(activityEl, UI_TEXT.LOADING || "...");
    }
    try {
      const opt = options && typeof options === "object" ? options : {};
      const runId = Number(opt.runId);
      const runIdValue = Number.isFinite(runId) && runId > 0 ? runId : null;
      const result = await api.fetchRecentRecords({ limit: 20, offset: 0, run_id: runIdValue });
      const items = result.items || [];
      if (!items.length) {
        setListEmpty(activityEl, UI_TEXT.NO_DATA || "-");
        return;
      }
      const signature = `${runIdValue || ""}|` + items.map((it) => `${String(it?.type || "")}:${String(it?.id || "")}`).join("|");
      if (String(activityEl.dataset.signature || "") === signature && activityEl.children.length) return;
      activityEl.dataset.signature = signature;

      activityEl.innerHTML = "";
      items.forEach((it) => activityEl.appendChild(createActivityItemEl(it)));
    } catch (e) {
      setListEmpty(activityEl, UI_TEXT.UNAVAILABLE || "-");
    }
  }

  async function updateBrain() {
    // 避免轮询时频繁清空导致闪烁：仅在首次为空时显示 loading/placeholder
    if (metaEl && !String(metaEl.textContent || "").trim()) {
      metaEl.textContent = UI_TEXT.DASHBOARD_AGENT_BRAIN_PLACEHOLDER || "...";
    }
    if (planEl && !String(planEl.textContent || "").trim()) {
      setListEmpty(planEl, UI_TEXT.LOADING || "...");
    }
    if (thoughtsEl && !String(thoughtsEl.textContent || "").trim()) {
      setListEmpty(thoughtsEl, UI_TEXT.LOADING || "...");
    }
    if (activityEl && !String(activityEl.textContent || "").trim()) {
      setListEmpty(activityEl, UI_TEXT.LOADING || "...");
    }

    try {
      const current = await api.fetchCurrentAgentRun();
      const run = current?.run;
      if (!run) {
        if (metaEl) metaEl.textContent = UI_TEXT.DASHBOARD_AGENT_EMPTY || "当前没有可展示的 Agent 任务。";
        setListEmpty(planEl, UI_TEXT.DASH || "-");
        setListEmpty(thoughtsEl, UI_TEXT.DASH || "-");
        updateActivity({ runId: null });
        return;
      }

      const title = String(run.task_title || run.title || "").trim();
      const stateLabel = run.is_current ? (UI_TEXT.DASHBOARD_AGENT_CURRENT || "进行中") : (UI_TEXT.DASHBOARD_AGENT_LATEST || "最近一次");

      // 细节数据：仅在 run_id 或 updated_at 变化时刷新，避免 1s 轮询导致 DB 压力与 UI 闪烁
      const runId = Number(run.run_id);
      const updatedAt = String(run.updated_at || "").trim();
      const prev = section.__agentBrainCache || {};
      const needFetchDetail = prev.runId !== runId || String(prev.updatedAt || "") !== updatedAt || !prev.detail;

      let detail = prev.detail;
      if (needFetchDetail) {
        detail = await api.fetchAgentRunDetail(runId);
        section.__agentBrainCache = { runId, updatedAt, detail };
      }

      const snapshot = detail?.snapshot && typeof detail.snapshot === "object" ? detail.snapshot : null;
      const stage = snapshot ? formatAgentStageLabel(snapshot.stage) : "";
      const elapsed = snapshot?.elapsed_ms != null ? formatDurationMs(snapshot.elapsed_ms) : "";
      const plan = snapshot?.plan && typeof snapshot.plan === "object" ? snapshot.plan : null;
      const progressText = plan && plan.total > 0 ? `${plan.done || 0}/${plan.total}` : "";
      const counters = snapshot?.counters && typeof snapshot.counters === "object" ? snapshot.counters : null;
      const llmCalls = counters?.llm?.calls != null ? Number(counters.llm.calls) : null;
      const tokensTotal = counters?.llm?.tokens_total != null ? Number(counters.llm.tokens_total) : null;
      const toolCalls = counters?.tools?.calls != null ? Number(counters.tools.calls) : null;

      if (metaEl) {
        const parts = [];
        parts.push(`${stateLabel}：task#${run.task_id} / run#${run.run_id}`);
        parts.push(`${UI_TEXT.STATUS_LABEL || "状态"}：${formatStatusLabel(run.status)}`);
        if (stage) parts.push(`${UI_TEXT.STAGE_LABEL || "阶段"}：${stage}`);
        if (elapsed) parts.push(`${UI_TEXT.ELAPSED_LABEL || "用时"}：${elapsed}`);
        if (progressText) parts.push(`${UI_TEXT.PROGRESS_LABEL || "进度"}：${progressText}`);
        const costParts = [];
        if (Number.isFinite(llmCalls)) costParts.push(`LLM ${llmCalls}`);
        if (Number.isFinite(tokensTotal)) costParts.push(`tokens ${tokensTotal}`);
        if (Number.isFinite(toolCalls)) costParts.push(`${UI_TEXT.WORLD_THOUGHTS_LABEL_TOOLS || "工具"} ${toolCalls}`);
        if (costParts.length) parts.push(`${UI_TEXT.CALLS_LABEL || "调用"}：${costParts.join(" | ")}`);
        if (title) parts.push(`${UI_TEXT.TITLE_LABEL || "任务"}：${title}`);
        metaEl.textContent = parts.join(" / ");
      }

      renderPlan(detail?.agent_plan || {});
      renderThoughts(detail?.agent_state || {});
      updateActivity({ runId });
    } catch (e) {
      if (metaEl) metaEl.textContent = UI_TEXT.UNAVAILABLE || "无法连接后端";
      setListEmpty(planEl, UI_TEXT.UNAVAILABLE || "-");
      setListEmpty(thoughtsEl, UI_TEXT.UNAVAILABLE || "-");
      setListEmpty(activityEl, UI_TEXT.UNAVAILABLE || "-");
    }
  }

  if (refreshBtn) {
    eventManager.add(
      refreshBtn,
      "click",
      debounce(() => {
        if (onStatusChange) onStatusChange();
        updateBrain();
      }, 300)
    );
  }

  // 初次进入自动刷新一次（状态栏 + Agent 大脑）
  if (onStatusChange) onStatusChange();
  updateBrain();

  // Dashboard 轮询：保证用户“随时看到进度”（不依赖桌宠窗口是否打开）
  pollManager.start(DASHBOARD_POLL_KEY, updateBrain, UI_POLL_INTERVAL_MS, { runImmediately: false });

  return {
    removeAll: () => {
      pollManager.stop(DASHBOARD_POLL_KEY);
      eventManager.removeAll();
    }
  };
}

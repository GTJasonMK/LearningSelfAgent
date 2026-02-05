import * as api from "../api.js";
import { UI_POLL_INTERVAL_MS, UI_TEXT } from "../constants.js";
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

  function statusToTagClass(status) {
    switch (String(status || "").toLowerCase()) {
      case "done":
        return "panel-tag--success";
      case "running":
        return "panel-tag--accent";
      case "failed":
        return "panel-tag--error";
      case "pending":
        return "panel-tag--warning";
      default:
        return "";
    }
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
      tag.textContent = String(item?.status || "pending");

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

  function formatActivityLine(item) {
    const type = String(item?.type || "").trim();
    const ts = String(item?.timestamp || "").trim();
    const taskId = item?.task_id != null ? `task#${item.task_id}` : "";
    const runId = item?.run_id != null ? `run#${item.run_id}` : "";
    const status = String(item?.status || "").trim();
    const title = String(item?.title || "").trim();
    const summary = String(item?.summary || "").trim();
    const detail = String(item?.detail || "").trim();

    const parts = [];
    if (ts) parts.push(ts);
    if (type) parts.push(type);
    if (taskId) parts.push(taskId);
    if (runId) parts.push(runId);
    if (status) parts.push(status);
    if (title) parts.push(title);
    if (summary) parts.push(summary);
    if (detail) parts.push(detail);
    return parts.join(" | ");
  }

  async function updateActivity() {
    if (!activityEl) return;
    // 避免轮询时频繁清空导致闪烁：仅在首次为空时显示 loading
    if (!String(activityEl.textContent || "").trim()) {
      setListEmpty(activityEl, UI_TEXT.LOADING || "...");
    }
    try {
      const result = await api.fetchRecentRecords({ limit: 20, offset: 0 });
      const items = result.items || [];
      if (!items.length) {
        setListEmpty(activityEl, UI_TEXT.NO_DATA || "-");
        return;
      }
      activityEl.innerHTML = "";
      items.forEach((it) => {
        const li = document.createElement("div");
        li.className = "panel-list-item";
        li.textContent = formatActivityLine(it);
        activityEl.appendChild(li);
      });
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
        if (metaEl) metaEl.textContent = "当前没有可展示的 Agent 任务。";
        setListEmpty(planEl, UI_TEXT.DASH || "-");
        setListEmpty(thoughtsEl, UI_TEXT.DASH || "-");
        updateActivity();
        return;
      }

      const title = String(run.task_title || run.title || "").trim();
      const stateLabel = run.is_current ? "进行中" : "最近一次";
      if (metaEl) {
        metaEl.textContent = `${stateLabel}：task#${run.task_id} / run#${run.run_id} / ${run.status}${title ? ` / ${title}` : ""}`;
      }

      const detail = await api.fetchAgentRunDetail(run.run_id);
      renderPlan(detail?.agent_plan || {});
      renderThoughts(detail?.agent_state || {});
      updateActivity();
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

// 评估标签页模块（只读展示：Agent 自动复盘记录）

import * as api from "../api.js";
import { UI_TEXT } from "../constants.js";
import { createEventManager, debounce } from "../utils.js";
import { setListLoading, setListError, renderList } from "../list-utils.js";
import { PollManager } from "../poll_manager.js";

function normalizeStatus(status) {
  return String(status || "").trim();
}

function statusToTagClass(status) {
  const v = normalizeStatus(status).toLowerCase();
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

function formatBulletList(title, items) {
  const lines = [];
  lines.push(title);
  if (!items || !Array.isArray(items) || items.length === 0) {
    lines.push("  - （无）");
    return lines.join("\n");
  }
  for (const it of items) {
    if (typeof it === "string" || typeof it === "number" || typeof it === "boolean") {
      const text = String(it).trim();
      if (text) lines.push(`  - ${text}`);
      continue;
    }
    if (it && typeof it === "object") {
      try {
        lines.push(`  - ${JSON.stringify(it, null, 2).replace(/\n/g, "\n    ")}`);
      } catch (e) {
        lines.push("  - [object]");
      }
      continue;
    }
    lines.push("  - -");
  }
  return lines.join("\n");
}

function formatSkills(items) {
  const title = UI_TEXT.AGENT_REVIEW_SKILLS_LABEL || "技能沉淀：";
  const lines = [];
  lines.push(title);
  if (!items || !Array.isArray(items) || items.length === 0) {
    lines.push("  - （无）");
    return lines.join("\n");
  }
  for (const it of items) {
    if (!it || typeof it !== "object") {
      lines.push(`  - ${String(it)}`);
      continue;
    }
    const status = String(it.status || "").trim();
    const sid = it.skill_id != null ? `skill#${it.skill_id}` : "skill#-";
    const name = String(it.name || "").trim();
    const source = String(it.source_path || "").trim();
    const err = String(it.error || "").trim();
    const parts = [];
    if (status) parts.push(`[${status}]`);
    parts.push(sid);
    if (name) parts.push(name);
    if (source) parts.push(`(${source})`);
    if (err) parts.push(`ERROR: ${err}`);
    lines.push(`  - ${parts.join(" ")}`);
  }
  return lines.join("\n");
}

function formatReviewDetail(review) {
  const rid = review?.id != null ? `#${review.id}` : "#-";
  const status = normalizeStatus(review?.status);
  const summary = String(review?.summary || "").trim();
  const created = String(review?.created_at || "").trim();
  const runId = review?.run_id != null ? Number(review.run_id) : null;
  const taskId = review?.task_id != null ? Number(review.task_id) : null;

  const lines = [];
  lines.push(`评估 ${rid}`);
  const meta = [];
  if (status) meta.push(`状态：${status}`);
  if (created) meta.push(`时间：${created}`);
  if (taskId) meta.push(`task#${taskId}`);
  if (runId) meta.push(`run#${runId}`);
  if (meta.length) lines.push(meta.join(" | "));
  lines.push("");
  if (summary) {
    lines.push("摘要");
    lines.push(`  ${summary}`);
    lines.push("");
  }

  lines.push(formatBulletList("问题：", review?.issues));
  lines.push("");
  lines.push(formatBulletList("下一步建议：", review?.next_actions));
  lines.push("");
  lines.push(formatSkills(review?.skills));
  return lines.join("\n");
}

export function bind(section) {
  const eventManager = createEventManager();

  const refreshBtn = section.querySelector("#agent-review-refresh");
  const listEl = section.querySelector("#agent-review-list");
  const detailEl = section.querySelector("#agent-review-detail");

  let selectedReviewId = null;
  const pollManager = new PollManager();
  const AUTO_REFRESH_KEY = "eval_auto_refresh";
  let loading = false;

  function setDetailText(text) {
    if (!detailEl) return;
    detailEl.textContent = text || UI_TEXT.DASH || "-";
  }

  function markSelected(id) {
    if (!listEl) return;
    listEl.querySelectorAll(".panel-list-item").forEach((li) => {
      li.classList.toggle("is-active", Number(li.dataset.id) === Number(id));
    });
  }

  async function loadDetail(reviewId) {
    if (!reviewId) return;
    try {
      const result = await api.fetchAgentReview(Number(reviewId));
      const review = result?.review || null;
      if (!review) {
        setDetailText(UI_TEXT.NO_DATA || "-");
        return;
      }
      setDetailText(formatReviewDetail(review));
    } catch (e) {
      setDetailText(UI_TEXT.LOAD_FAIL || "加载失败");
    }
  }

  async function loadList({ keepSelection = true } = {}) {
    if (!listEl || loading) return;
    loading = true;
    setListLoading(listEl, UI_TEXT.LOAD_LIST || UI_TEXT.LOADING || "...");
    try {
      const result = await api.fetchAgentReviews({ offset: 0, limit: 50 });
      const items = result?.items || [];
      renderList(
        listEl,
        items,
        (li, item) => {
          const content = document.createElement("span");
          content.className = "panel-list-item-content";
          const rid = item?.id != null ? `#${item.id}` : "#-";
          const run = item?.run_id != null ? `run#${item.run_id}` : "run#-";
          const summary = String(item?.summary || "").trim();
          content.textContent = `${rid} ${run} ${summary}`.trim();

          const tag = document.createElement("span");
          const st = normalizeStatus(item?.status) || UI_TEXT.DASH;
          tag.className = `panel-tag ${statusToTagClass(st)}`.trim();
          tag.textContent = st;

          li.title = String(item?.created_at || "");
          li.appendChild(content);
          li.appendChild(tag);
        },
        UI_TEXT.NO_DATA || "-"
      );

      if (!items.length) {
        selectedReviewId = null;
        setDetailText(UI_TEXT.NO_DATA || "-");
        return;
      }

      const exists = keepSelection && selectedReviewId && items.some((it) => Number(it.id) === Number(selectedReviewId));
      const nextId = exists ? selectedReviewId : Number(items[0].id);
      selectedReviewId = nextId;
      markSelected(selectedReviewId);
      await loadDetail(selectedReviewId);
    } catch (e) {
      setListError(listEl, UI_TEXT.LOAD_FAIL || "加载失败");
      setDetailText(UI_TEXT.LOAD_FAIL || "加载失败");
    } finally {
      loading = false;
    }
  }

  if (listEl) {
    eventManager.add(listEl, "click", (e) => {
      const li = e.target?.closest ? e.target.closest(".panel-list-item") : null;
      if (!li || !li.dataset.id) return;
      const id = Number(li.dataset.id);
      if (!id) return;
      selectedReviewId = id;
      markSelected(selectedReviewId);
      loadDetail(selectedReviewId);
    });
  }

  if (refreshBtn) {
    eventManager.add(
      refreshBtn,
      "click",
      debounce(() => loadList({ keepSelection: true }), 250)
    );
  }

  // 初始化
  setDetailText(UI_TEXT.DASH || "-");
  loadList({ keepSelection: true });

  // 轻量自动刷新：展示最新评估状态
  pollManager.start(
    AUTO_REFRESH_KEY,
    async () => {
      await loadList({ keepSelection: true });
    },
    4500,
    { runImmediately: false }
  );

  return {
    ...eventManager,
    removeAll: () => {
      pollManager.stop(AUTO_REFRESH_KEY);
      eventManager.removeAll();
    }
  };
}

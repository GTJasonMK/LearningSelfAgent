// 主面板模块 - 标签页切换与状态栏

import * as api from "./api.js";
import { UI_POLL_INTERVAL_MS, UI_TEXT } from "./constants.js";
import { applyText } from "./ui.js";
import { debounce } from "./utils.js";
import {
  extractResultPayloadText,
  parseSlashCommand
} from "./agent_text.js";
import { createStore } from "./store.js";
import { streamSse } from "./streaming.js";
import { abortStream, createStreamController, isStreamActive, startStream, stopStream } from "./stream_controller.js";
import { PollManager } from "./poll_manager.js";
import { buildInputHistoryFromChatItems, createInputHistoryManager, mergeInputHistoryWithBackend } from "./input_history.js";
import { getPanelDomRefs } from "./dom_refs.js";
import { setMarkdownContent } from "./markdown.js";
import { emitAgentEvent, initAgentEventBridge } from "./agent_events.js";

// 标签页模块导入
import * as dashboardTab from "./tabs/dashboard.js";
import * as tasksTab from "./tabs/tasks.js";
import * as memoryTab from "./tabs/memory.js";
import * as skillsTab from "./tabs/skills.js";
import * as graphTab from "./tabs/graph.js";
import * as evalTab from "./tabs/eval.js";
import * as systemTab from "./tabs/system.js";
import * as settingsTab from "./tabs/settings.js";

// 通过 window.require 获取 Electron API（ESM 环境下避免 import 中断）
const ipcRenderer = window?.require ? window.require("electron").ipcRenderer : null;
// 接收桌宠窗口转发的结构化事件（memory/skills/graph...）
initAgentEventBridge();

// 获取元素（集中在 dom_refs.js，减少散落的 querySelector）
const {
  tabs,
  sections,
  topbarTabsEl,
  pageHomeEl,
  pageStateEl,
  pageWorldEl,
  pageTabWorldBtn,
  pageTabStateBtn,
  panelEnterMainBtn,
  worldTaskPlanEl,
  worldEvalPlanEl,
  worldResultEl,
  worldChatEl,
  worldThoughtsEl,
  worldUploadBtn,
  worldInputEl,
  worldSendBtn
} = getPanelDomRefs();

// 标签页事件管理器跟踪
const tabEventManagers = new Map();

// 标签页绑定函数映射
const TAB_BINDERS = {
  "tab-dashboard": dashboardTab.bind,
  "tab-tasks": tasksTab.bind,
  "tab-memory": memoryTab.bind,
  "tab-skills": skillsTab.bind,
  "tab-graph": graphTab.bind,
  "tab-eval": evalTab.bind,
  "tab-system": systemTab.bind,
  "tab-settings": settingsTab.bind
};

// -----------------------------
// 世界页：数据绑定与交互
// -----------------------------

const pollManager = new PollManager();
const WORLD_POLL_KEY = "world_poll";
const worldStream = createStreamController();
// “任务执行结果”在评估完成前不落库（避免先输出最终结论，再补评估 plan-list）
const WORLD_EVAL_GATE_TIMEOUT_MS = 90000;
// 世界页关键会话状态：集中管理，避免散落的全局变量在并发/切页时漂移
const worldSession = createStore({
  streaming: false,
  streamingMode: "",
  // do 模式流式时的“思考状态行”（只显示在右上角思考框，不进入中间对话）
  streamingStatus: "",
  pendingResume: null, // {runId, taskId?, question}
  // 等待评估完成后再落库的最终回答（只用于世界页 do/resume）
  pendingFinal: null, // {tmpKey, content, run_id, task_id, metadata, startedAt}
  // 当前 run 元信息（来自 /agent/runs/current），用于“世界页”统一展示（不区分来源：桌宠/世界页）。
  currentRun: null,
  // 当前 run 的 agent_plan（来自 /agent/runs/{run_id}），用于在思考框中展示“当前步骤标题”等信息。
  currentAgentPlan: null,
  // 当前 run 的 agent_state（来自 /agent/runs/{run_id}）
  currentAgentState: null,
  lastRunMeta: null, // {run_id, task_id, updated_at, status}（兼容旧字段）
  // 右上角“思考轨迹”附加信息：来自 /records/recent 的筛选结果（用于展示 tool/skill/memory/debug 等）
  traceLines: [],
  traceFetchedAt: 0,
  inputHistory: [],
  historyCursor: null, // null 表示不在历史模式；否则是 [0..inputHistory.length]，len 表示草稿位
  historyDraft: "",
  chat: {
    timeline: [], // {id?, role, content, created_at?, task_id?, run_id?, metadata?}
    maxId: 0,
    minId: 0,
    initialized: false,
    tempSeq: 0,
    loadingOlder: false,
    lastSyncAt: 0
  }
});

function getWorldChatState() {
  return worldSession.getState().chat;
}

function setWorldChatState(patch, meta) {
  worldSession.setState((prev) => {
    const prevChat = prev?.chat || {};
    const nextChat = typeof patch === "function"
      ? patch(prevChat)
      : { ...prevChat, ...(patch || {}) };
    return { ...prev, chat: nextChat };
  }, meta);
}
// 单会话聊天时间线（桌宠/世界页共用同一份记录，落库到后端）
const WORLD_CHAT_CONTEXT_LIMIT = 16;
const WORLD_INPUT_HISTORY_LIMIT = 50;
const worldChatDomByKey = new Map();

// 输入历史（上下键回溯）
const worldInputHistory = createInputHistoryManager({
  getState: worldSession.getState,
  setState: worldSession.setState,
  limit: WORLD_INPUT_HISTORY_LIMIT
});

function bootstrapWorldInputHistoryFromChatItems(items) {
  const backendHistory = buildInputHistoryFromChatItems(items, WORLD_INPUT_HISTORY_LIMIT);
  if (!backendHistory.length) return;
  worldSession.setState(
    (prev) => ({
      ...prev,
      inputHistory: mergeInputHistoryWithBackend(prev?.inputHistory, backendHistory, WORLD_INPUT_HISTORY_LIMIT),
      historyCursor: null,
      historyDraft: ""
    }),
    { reason: "input_history_bootstrap" }
  );
}

function appendWorldInputHistoryFromChatItems(items) {
  const newInputs = buildInputHistoryFromChatItems(items, WORLD_INPUT_HISTORY_LIMIT);
  if (!newInputs.length) return;

  worldSession.setState(
    (prev) => {
      const history = Array.isArray(prev?.inputHistory) ? prev.inputHistory.slice() : [];
      const oldLen = history.length;
      let cursor = prev?.historyCursor;

      for (const raw of newInputs) {
        const text = String(raw || "").trim();
        if (!text) continue;
        const last = history.length ? history[history.length - 1] : "";
        if (text === last) continue;
        history.push(text);
      }

      const beforeTrimLen = history.length;
      if (history.length > WORLD_INPUT_HISTORY_LIMIT) {
        history.splice(0, history.length - WORLD_INPUT_HISTORY_LIMIT);
      }
      const dropped = Math.max(0, beforeTrimLen - history.length);

      if (cursor !== null && cursor !== undefined) {
        if (cursor === oldLen) {
          // 草稿位：保持在末尾
          cursor = history.length;
        } else {
          cursor = Math.max(0, Number(cursor) - dropped);
          cursor = Math.min(history.length, Number(cursor));
        }
      }

      return { ...prev, inputHistory: history, historyCursor: cursor };
    },
    { reason: "input_history_sync" }
  );
}

let worldLastReviewId = null;
let worldLastReviewSignature = "";

function statusToTagClass(status) {
  switch (String(status || "").toLowerCase()) {
    case "done":
      return "panel-tag--success";
    case "running":
      return "panel-tag--accent";
    case "failed":
      return "panel-tag--error";
    case "waiting":
      return "panel-tag--warning";
    default:
      return "";
  }
}

function setEmptyText(el, text) {
  if (!el) return;
  el.innerHTML = `<div class="panel-empty-text">${text}</div>`;
}

function ensurePre(el) {
  if (!el) return null;
  let pre = el.querySelector(".world-pre");
  if (pre) return pre;
  pre = document.createElement("pre");
  pre.className = "world-pre";
  el.innerHTML = "";
  el.appendChild(pre);
  return pre;
}

function setPreText(el, text) {
  const pre = ensurePre(el);
  if (!pre) return;
  const container = pre.parentElement;
  const shouldStickToBottom = container
    ? container.scrollTop + container.clientHeight >= container.scrollHeight - 48
    : false;
  pre.textContent = String(text || "");
  if (container && shouldStickToBottom) {
    container.scrollTop = container.scrollHeight;
  }
}

function appendPreText(el, delta) {
  const pre = ensurePre(el);
  if (!pre) return;
  const container = pre.parentElement;
  const shouldStickToBottom = container
    ? container.scrollTop + container.clientHeight >= container.scrollHeight - 48
    : false;
  pre.textContent += String(delta || "");
  if (container && shouldStickToBottom) {
    container.scrollTop = container.scrollHeight;
  }
}

function renderPlanList(el, items) {
  if (!el) return;
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    setEmptyText(el, UI_TEXT.NO_DATA || "-");
    return;
  }

  const container = el;
  const prevScrollTop = container.scrollTop;
  const shouldStickToBottom =
    container.scrollTop + container.clientHeight >= container.scrollHeight - 48;

  el.innerHTML = "";
  list.forEach((item) => {
    const row = document.createElement("div");
    row.className = "panel-list-item";

    const text = document.createElement("div");
    text.className = "panel-list-item-content";
    text.textContent = String(item?.brief || item?.title || "").trim() || UI_TEXT.DASH;

    const tag = document.createElement("span");
    tag.className = `panel-tag ${statusToTagClass(item?.status)}`.trim();
    tag.textContent = String(item?.status || "").trim() || "planned";

    row.appendChild(text);
    row.appendChild(tag);
    el.appendChild(row);
  });

  // 只有用户已经在底部时才自动跟随滚动；否则保持用户正在查看的历史位置。
  if (shouldStickToBottom) {
    container.scrollTop = container.scrollHeight;
  } else {
    container.scrollTop = prevScrollTop;
  }
}

function worldChatScrollContainer() {
  // world-result 是滚动容器，world-chat 是列表容器
  return worldResultEl || null;
}

function worldChatIsNearBottom(container) {
  if (!container) return false;
  return container.scrollTop + container.clientHeight >= container.scrollHeight - 64;
}

function worldChatScrollToBottom() {
  const container = worldChatScrollContainer();
  if (!container) return;
  container.scrollTop = container.scrollHeight;
}

function worldChatKey(msg) {
  if (!msg) return "";
  if (msg.id != null) return String(msg.id);
  if (msg._tmpKey) return String(msg._tmpKey);
  return "";
}

function runAssistantPlaceholderKey(runId) {
  const rid = Number(runId);
  if (!Number.isFinite(rid) || rid <= 0) return "";
  return `run-${rid}-assistant-placeholder`;
}

function removeWorldChatMessageByKey(key) {
  const k = String(key || "");
  if (!k) return;
  const item = worldChatDomByKey.get(k);
  if (item?.row) {
    try { item.row.remove(); } catch (e) {}
  }
  worldChatDomByKey.delete(k);
  setWorldChatState(
    (prev) => {
      const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
      const next = timeline.filter((m) => worldChatKey(m) !== k);
      return { ...prev, timeline: next };
    },
    { reason: "chat_remove_by_key" }
  );
  ensureWorldChatEmpty();
}

function ensureRunAssistantPlaceholder(runMeta) {
  const runId = Number(runMeta?.run_id);
  const taskId = Number(runMeta?.task_id);
  const status = String(runMeta?.status || "").trim().toLowerCase();
  const key = runAssistantPlaceholderKey(runId);
  if (!key) return;

  const chat = getWorldChatState();
  if (!chat?.initialized) return;

  // 仅在“本页未发起流式请求 + 后端 run 仍在 running”时补一个占位，
  // 这样从桌宠发起任务后打开世界页，看到的聊天区也会呈现“有一条进行中的回复”。
  const session = worldSession.getState();
  if (session.streaming) return;
  if (status !== "running") {
    removeWorldChatMessageByKey(key);
    return;
  }

  const timeline = Array.isArray(chat?.timeline) ? chat.timeline : [];
  const hasRealAssistant = timeline.some((m) => {
    const role = String(m?.role || "").trim().toLowerCase();
    if (role !== "assistant") return false;
    const rid = m?.run_id != null ? Number(m.run_id) : null;
    return rid === runId && m?.id != null;
  });
  if (hasRealAssistant) {
    removeWorldChatMessageByKey(key);
    return;
  }
  const hasPlaceholder = timeline.some((m) => String(m?._tmpKey || "") === key);
  if (hasPlaceholder) return;

  // 新增占位消息（仅 UI，不落库）
  const msg = {
    role: "assistant",
    content: UI_TEXT.PET_CHAT_SENDING || "…",
    created_at: "",
    run_id: Number.isFinite(runId) ? runId : null,
    task_id: Number.isFinite(taskId) ? taskId : null,
    _tmpKey: key,
    metadata: { source: "panel", placeholder: true, mode: "do" }
  };
  setWorldChatState(
    (prev) => {
      const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
      timeline.push(msg);
      return { ...prev, timeline };
    },
    { reason: "chat_placeholder_add" }
  );
  appendWorldChatMessage(msg);
}

function ensureWorldChatEmpty() {
  if (!worldChatEl) return;
  const chat = getWorldChatState();
  if (!chat?.timeline?.length) {
    worldChatEl.innerHTML = `<div class="world-chat-empty">${UI_TEXT.NO_DATA || "-"}</div>`;
  }
}

function createWorldChatItemEl(msg) {
  const role = String(msg?.role || "").trim().toLowerCase();
  const isUser = role === "user";

  const row = document.createElement("div");
  row.className = `world-chat-item ${isUser ? "world-chat-item--user" : "world-chat-item--assistant"}`.trim();

  const bubble = document.createElement("div");
  bubble.className = `world-chat-bubble ${isUser ? "world-chat-bubble--user" : "world-chat-bubble--assistant"}`.trim();
  bubble.dataset.role = role || (isUser ? "user" : "assistant");
  if (isUser) {
    bubble.textContent = String(msg?.content || "");
  } else {
    setMarkdownContent(bubble, String(msg?.content || ""));
  }

  const createdAt = String(msg?.created_at || "").trim();
  const runId = msg?.run_id != null ? Number(msg.run_id) : null;
  const taskId = msg?.task_id != null ? Number(msg.task_id) : null;
  const metaParts = [];
  if (createdAt) metaParts.push(createdAt);
  if (taskId) metaParts.push(`task#${taskId}`);
  if (runId) metaParts.push(`run#${runId}`);
  if (metaParts.length) bubble.title = metaParts.join(" | ");

  row.appendChild(bubble);
  return { row, bubble };
}

function renderWorldChatAll() {
  if (!worldChatEl) return;
  worldChatDomByKey.clear();
  worldChatEl.innerHTML = "";
  const chat = getWorldChatState();
  const timeline = Array.isArray(chat?.timeline) ? chat.timeline : [];
  if (!timeline.length) {
    ensureWorldChatEmpty();
    return;
  }
  timeline.forEach((msg) => {
    const key = worldChatKey(msg);
    const { row, bubble } = createWorldChatItemEl(msg);
    if (key) worldChatDomByKey.set(key, { row, bubble });
    worldChatEl.appendChild(row);
  });
  worldChatScrollToBottom();
}

function appendWorldChatMessage(msg) {
  if (!worldChatEl) return;
  const container = worldChatScrollContainer();
  const stick = worldChatIsNearBottom(container);
  // 若当前是空态占位，先清空
  if (worldChatEl.querySelector(".world-chat-empty")) {
    worldChatEl.innerHTML = "";
  }
  const key = worldChatKey(msg);
  const { row, bubble } = createWorldChatItemEl(msg);
  if (key) worldChatDomByKey.set(key, { row, bubble });
  worldChatEl.appendChild(row);
  if (stick) worldChatScrollToBottom();
}

function prependWorldChatMessages(items) {
  if (!worldChatEl) return;
  const list = Array.isArray(items) ? items : [];
  if (!list.length) return;
  if (worldChatEl.querySelector(".world-chat-empty")) {
    worldChatEl.innerHTML = "";
  }
  // 注意：insertBefore 会把节点插到最前面。为了保持 list 的顺序（时间正序），这里倒序插入。
  for (let i = list.length - 1; i >= 0; i--) {
    const msg = list[i];
    const key = worldChatKey(msg);
    const { row, bubble } = createWorldChatItemEl(msg);
    if (key) worldChatDomByKey.set(key, { row, bubble });
    worldChatEl.insertBefore(row, worldChatEl.firstChild);
  }
}

function updateWorldChatMessageContent(msgKey, nextContent) {
  const key = String(msgKey || "");
  if (!key) return;
  const item = worldChatDomByKey.get(key);
  if (!item || !item.bubble) return;
  if (item.bubble.classList.contains("world-chat-bubble--assistant")) {
    setMarkdownContent(item.bubble, String(nextContent || ""));
  } else {
    item.bubble.textContent = String(nextContent || "");
  }
  const container = worldChatScrollContainer();
  if (worldChatIsNearBottom(container)) worldChatScrollToBottom();
}

let worldTaskPlanItems = [];
function renderWorldPlan(planObj) {
  if (!worldTaskPlanEl) return;
  const items = planObj?.items;
  worldTaskPlanItems = Array.isArray(items) ? items.map((it) => (it && typeof it === "object" ? { ...it } : {})) : [];
  renderPlanList(worldTaskPlanEl, worldTaskPlanItems);
}

function applyWorldPlanDelta(deltaObj) {
  const changes = Array.isArray(deltaObj?.changes) ? deltaObj.changes : [];
  if (!changes.length) return;
  if (!Array.isArray(worldTaskPlanItems) || !worldTaskPlanItems.length) return;

  for (const raw of changes) {
    const ch = raw && typeof raw === "object" ? raw : {};
    const id = Number(ch.id);
    const stepOrder = Number(ch.step_order);

    let idx = -1;
    if (Number.isFinite(id) && id > 0) {
      idx = worldTaskPlanItems.findIndex((it) => Number(it?.id) === id);
    }
    if (idx === -1 && Number.isFinite(stepOrder) && stepOrder > 0) {
      idx = stepOrder - 1;
    }
    if (idx < 0 || idx >= worldTaskPlanItems.length) continue;

    const base = worldTaskPlanItems[idx] && typeof worldTaskPlanItems[idx] === "object" ? worldTaskPlanItems[idx] : {};
    if (ch.status != null) base.status = ch.status;
    if (ch.brief != null) base.brief = ch.brief;
    if (ch.title != null) base.title = ch.title;
    if (!base.id && (Number.isFinite(id) && id > 0)) base.id = id;
    worldTaskPlanItems[idx] = base;
  }

  renderPlanList(worldTaskPlanEl, worldTaskPlanItems);
}

function normalizeInlineText(text) {
  return String(text || "").replace(/\s+/g, " ").trim();
}

function truncateInline(text, maxLen = 220) {
  const value = normalizeInlineText(text);
  if (!value) return "";
  if (!Number.isFinite(maxLen) || maxLen <= 0) return value;
  if (value.length <= maxLen) return value;
  return `${value.slice(0, maxLen).trimEnd()}…`;
}

function formatObservationForThoughts(rawLine) {
  const line = String(rawLine || "").trim();
  if (!line) return "";

  // 结构化提示（非步骤）：保持可见但做简化
  if (line.startsWith("artifacts_missing_autofix:")) {
    return `[WARN] ${truncateInline(line.replace("artifacts_missing_autofix:", "缺失文件自动补救:"), 260)}`;
  }

  const sep = line.indexOf(": ");
  if (sep === -1) return truncateInline(line, 280);

  const title = truncateInline(line.slice(0, sep), 140);
  const restRaw = line.slice(sep + 2).trim();
  const rest = normalizeInlineText(restRaw);

  if (rest.startsWith("FAIL ")) {
    return `[FAIL] ${title} | ${truncateInline(rest.slice(5), 260)}`;
  }
  if (rest === "ok") return `[OK] ${title}`;
  if (rest.startsWith("shell ")) return `[OK] ${title} | ${truncateInline(rest.replace(/^shell\s+/, ""), 260)}`;
  if (rest.startsWith("llm=")) return `[OK] ${title} | llm: ${truncateInline(rest.slice(4), 180)}`;
  if (rest.startsWith("output=")) return `[OK] ${title} | 输出: ${truncateInline(rest.slice(7), 180)}`;
  if (rest.startsWith("file_write ")) return `[OK] ${title} | ${truncateInline(rest.replace(/^file_write\s+/, "写文件 "), 220)}`;
  if (rest.startsWith("tool#")) return `[OK] ${title} | ${truncateInline(rest, 220)}`;
  if (rest.startsWith("memory#")) return `[OK] ${title} | ${truncateInline(rest, 220)}`;
  return `[OK] ${title} | ${truncateInline(rest, 220)}`;
}

function renderWorldThoughts(runMeta, stateObj) {
  if (!worldThoughtsEl) return;
  const session = worldSession.getState();
  const lines = [];

  // do 模式流式时：把状态行固定显示在思考框顶部（中间对话区不显示这些“思考过程”）
  const streamingStatus = String(session?.streamingStatus || "").trim();
  if (session?.streaming && session?.streamingMode === "do" && streamingStatus) {
    lines.push("【状态】");
    lines.push(`- ${truncateInline(streamingStatus, 240)}`);
    lines.push("");
  }

  const taskId = runMeta?.task_id;
  const runId = runMeta?.run_id;
  const status = String(runMeta?.status || "").trim();
  const taskTitle = truncateInline(runMeta?.task_title || runMeta?.title || "", 120);
  if (taskId && runId) {
    if (!lines.length) lines.push("【状态】");
    lines.push(`- task#${taskId} / run#${runId} / ${status || "-"}${taskTitle ? ` | ${taskTitle}` : ""}`);
  }

  const stepOrder = stateObj?.step_order;
  const plan = session?.currentAgentPlan;
  const planTitles = Array.isArray(plan?.titles) ? plan.titles : [];
  const planItems = Array.isArray(plan?.items) ? plan.items : [];
  if (stepOrder != null) {
    const stepNum = Number(stepOrder);
    const total = planTitles.length ? planTitles.length : null;
    const idx = Number.isFinite(stepNum) ? stepNum - 1 : -1;
    const stepTitle = idx >= 0 && idx < planTitles.length ? String(planTitles[idx] || "").trim() : "";
    const stepBrief = idx >= 0 && idx < planItems.length ? String(planItems[idx]?.brief || "").trim() : "";
    const stepStatus = idx >= 0 && idx < planItems.length ? String(planItems[idx]?.status || "").trim() : "";

    if (!lines.length) lines.push("【状态】");
    lines.push("");
    lines.push("【步骤】");
    const head = total ? `#${stepNum}/${total}` : `#${stepNum}`;
    lines.push(`- 当前：${head}${stepBrief ? ` ${truncateInline(stepBrief, 24)}` : ""}${stepStatus ? ` (${stepStatus})` : ""}`);
    if (stepTitle) lines.push(`  - ${truncateInline(stepTitle, 220)}`);
  }

  const paused = stateObj?.paused;
  const question = paused && typeof paused === "object" ? String(paused.question || "").trim() : "";
  if (question) {
    const stepTitle = paused && typeof paused === "object" ? String(paused.step_title || "").trim() : "";
    lines.push("");
    lines.push("【等待输入】");
    if (stepTitle) lines.push(`- 步骤：${truncateInline(stepTitle, 220)}`);
    lines.push(`- 问题：${truncateInline(question, 260)}`);
  }

  const obs = stateObj?.observations;
  if (Array.isArray(obs) && obs.length) {
    const tail = obs.slice(Math.max(0, obs.length - 10));
    lines.push("");
    lines.push("【观测】");
    tail.forEach((o) => {
      const t = formatObservationForThoughts(o);
      if (t) lines.push(`- ${t}`);
    });
    if (obs.length > tail.length) {
      lines.push(`- …（共 ${obs.length} 条，仅展示最近 ${tail.length} 条）`);
    }
  }

  const traceLines = Array.isArray(session?.traceLines) ? session.traceLines : [];
  if (traceLines.length) {
    lines.push("");
    lines.push("【轨迹】");
    traceLines.slice(0, 20).forEach((t) => {
      const text = String(t || "").trim();
      if (text) lines.push(`- ${truncateInline(text, 240)}`);
    });
  }

  if (!lines.length) {
    setEmptyText(worldThoughtsEl, UI_TEXT.NO_DATA || "-");
    return;
  }
  setPreText(worldThoughtsEl, lines.join("\n"));
}

function rerenderWorldThoughtsFromState() {
  const session = worldSession.getState();
  renderWorldThoughts(session?.currentRun || null, session?.currentAgentState || null);
}

function isEvalFinalStatus(status) {
  const s = String(status || "").trim().toLowerCase();
  return s === "pass" || s === "needs_changes" || s === "fail" || s === "done" || s === "failed";
}

function renderWorldEvalPlan(review) {
  if (!worldEvalPlanEl) return;
  if (!review) {
    setEmptyText(worldEvalPlanEl, UI_TEXT.NO_DATA || "-");
    return;
  }
  const status = String(review.status || "").trim() || UI_TEXT.DASH;
  const summary = String(review.summary || "").trim();
  const nextActions = Array.isArray(review.next_actions) ? review.next_actions : [];

  const statusLower = String(status || "").toLowerCase();
  const stageSteps = ["读取记录", "评估分析", "落库沉淀", "输出结果"];

  const isFinalEval = statusLower === "pass" || statusLower === "needs_changes" || statusLower === "fail";
  const finalStageStatus =
    statusLower === "pass" ? "done" : statusLower === "needs_changes" ? "waiting" : statusLower === "fail" ? "failed" : "";

  let stageIdx = 0;
  // running 状态下，summary 会是“评估中：读取记录…”这类文本，可用于定位当前阶段
  if (!isFinalEval) {
    for (let i = 0; i < stageSteps.length; i++) {
      if (summary.includes(stageSteps[i])) {
        stageIdx = i;
        break;
      }
    }
  }

  const items = [];
  stageSteps.forEach((name, idx) => {
    let st = "pending";
    if (isFinalEval) {
      st = idx < stageSteps.length - 1 ? "done" : (finalStageStatus || "done");
    } else if (statusLower === "running") {
      st = idx < stageIdx ? "done" : idx === stageIdx ? "running" : "pending";
    } else if (statusLower === "done") {
      st = "done";
    } else if (statusLower === "failed") {
      st = idx < stageIdx ? "done" : idx === stageIdx ? "failed" : "pending";
    } else if (statusLower === "waiting") {
      st = idx < stageIdx ? "done" : idx === stageIdx ? "waiting" : "pending";
    } else {
      st = idx < stageIdx ? "done" : "pending";
    }
    items.push({ brief: name, status: st });
  });

  // next_actions 也按“计划项”方式平铺（保持与任务 plan-list 一致）
  nextActions.slice(0, 6).forEach((a) => {
    if (a && typeof a === "object") {
      const title = String(a.title || "").trim();
      const details = String(a.details || "").trim();
      const t = title || details;
      if (t) items.push({ brief: t, status: "planned" });
      return;
    }
    const t = String(a || "").trim();
    if (t) items.push({ brief: t, status: "planned" });
  });

  renderPlanList(worldEvalPlanEl, items);
}

function formatRecentRecordLine(item) {
  const it = item && typeof item === "object" ? item : {};
  const type = String(it.type || "").trim().toLowerCase();
  const title = String(it.title || "").trim();
  const status = String(it.status || "").trim();
  const summary = String(it.summary || "").trim();
  const detail = String(it.detail || "").trim();

  if (type === "step") {
    return `${title || "step"}${status ? ` (${status})` : ""}`;
  }
  if (type === "tool") {
    const head = `tool:${title || it.ref_id || "-"}`;
    const reuse = status ? ` reuse=${status}` : "";
    const input = summary ? ` in=${summary}` : "";
    return `${head}${reuse}${input}`.trim();
  }
  if (type === "output") {
    const kind = summary || "output";
    return `${kind}: ${detail || "-"}`.trim();
  }
  if (type === "llm") {
    const head = `llm:${summary || "-"}`;
    return `${head}${status ? ` (${status})` : ""}`.trim();
  }
  if (type === "memory") {
    return `memory: ${detail || "-"}`.trim();
  }
  if (type === "skill") {
    return `skill: ${title || "-"}${status ? ` (${status})` : ""}`.trim();
  }
  if (type === "agent_review") {
    const head = `review: ${status || "-"}`;
    return `${head}${summary ? ` | ${summary}` : ""}`.trim();
  }
  if (type === "run") {
    return `run: ${status || "-"}`.trim();
  }
  return `${type || "event"}: ${title || summary || detail || "-"}`.trim();
}

async function updateWorldTrace(taskId, runId, force = false) {
  const tid = Number(taskId);
  const rid = Number(runId);
  if (!Number.isFinite(tid) || tid <= 0) return;
  if (!Number.isFinite(rid) || rid <= 0) return;

  const now = Date.now();
  const session = worldSession.getState();
  if (!force && now - Number(session.traceFetchedAt || 0) < 900) return;
  worldSession.setState({ traceFetchedAt: now }, { reason: "trace_throttle" });

  try {
    const resp = await api.fetchRecentRecords({ limit: 120, offset: 0 });
    const items = Array.isArray(resp?.items) ? resp.items : [];

    const filtered = items.filter((raw) => {
      const it = raw && typeof raw === "object" ? raw : {};
      if (Number(it.task_id) !== tid) return false;
      // 有 run_id 的记录：严格按当前 run 过滤
      if (it.run_id != null) return Number(it.run_id) === rid;
      // 没有 run_id 的：仅收进对“Agent 思考轨迹”有价值的（记忆/技能）
      const t = String(it.type || "").trim().toLowerCase();
      return t === "memory" || t === "skill";
    });

    const lines = [];
    filtered.slice(0, 24).forEach((it) => {
      const line = formatRecentRecordLine(it);
      if (line) lines.push(line);
    });

    worldSession.setState({ traceLines: lines }, { reason: "trace_update" });
    rerenderWorldThoughtsFromState();
  } catch (e) {
    // 忽略：后端可能暂时不可用
  }
}

async function loadWorldChatHistory(force = false) {
  const chat = getWorldChatState();
  if (chat?.initialized && !force) return;
  try {
    const resp = await api.fetchChatMessages({ limit: 80 });
    const items = Array.isArray(resp?.items) ? resp.items : [];
    const first = items.length ? items[0] : null;
    const last = items.length ? items[items.length - 1] : null;
    setWorldChatState(
      {
        timeline: items,
        initialized: true,
        maxId: last?.id != null ? Number(last.id) : 0,
        minId: first?.id != null ? Number(first.id) : 0,
      },
      { reason: "chat_init" }
    );
    renderWorldChatAll();
    // 输入历史：从后端聊天记录中抽取 user 输入，确保“桌宠/世界页”共享上下键历史
    bootstrapWorldInputHistoryFromChatItems(items);
  } catch (e) {
    setWorldChatState({ initialized: false }, { reason: "chat_init_fail" });
    // 后端不可用时保留现有 UI，不要抛错卡死
    ensureWorldChatEmpty();
  }
}

async function syncWorldChatNew() {
  const chat = getWorldChatState();
  if (!chat?.initialized) return;
  const now = Date.now();
  // 轮询频率与世界页 run 轮询一致时，额外做一次轻量节流
  if (now - Number(chat.lastSyncAt || 0) < 900) return;
  setWorldChatState({ lastSyncAt: now }, { reason: "chat_sync_throttle" });
  if (worldSession.getState().streaming) return;
  try {
    const resp = await api.fetchChatMessages({ after_id: Number(chat.maxId || 0), limit: 200 });
    const items = Array.isArray(resp?.items) ? resp.items : [];
    if (!items.length) return;
    // 输入历史：同步新到达的 user 输入（可能来自桌宠窗口）
    appendWorldInputHistoryFromChatItems(items);
    items.forEach((m) => {
      // 若从桌宠发起的 do 任务已落库最终回复：移除“进行中占位”
      const role = String(m?.role || "").trim().toLowerCase();
      if (role === "assistant" && m?.run_id != null) {
        removeWorldChatMessageByKey(runAssistantPlaceholderKey(Number(m.run_id)));
      }
      appendWorldChatMessage(m);
    });
    setWorldChatState(
      (prev) => {
        const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
        let maxId = Number(prev?.maxId || 0);
        let minId = Number(prev?.minId || 0);
        items.forEach((m) => {
          timeline.push(m);
          const id = Number(m?.id);
          if (Number.isFinite(id) && id > maxId) maxId = id;
          if (Number.isFinite(id) && id > 0 && (!minId || minId <= 0)) minId = id;
        });
        return { ...prev, timeline, maxId, minId };
      },
      { reason: "chat_sync_new" }
    );
  } catch (e) {
    // 忽略：后端可能暂时不可用
  }
}

async function loadOlderWorldChatHistory() {
  const chat = getWorldChatState();
  if (!chat?.initialized) return;
  if (chat.loadingOlder) return;
  if (!chat.minId || chat.minId <= 1) return;
  if (!worldResultEl) return;
  setWorldChatState({ loadingOlder: true }, { reason: "chat_load_older_start" });

  const prevScrollHeight = worldResultEl.scrollHeight;
  const prevScrollTop = worldResultEl.scrollTop;

  try {
    const resp = await api.fetchChatMessages({ before_id: Number(chat.minId || 0), limit: 80 });
    const items = Array.isArray(resp?.items) ? resp.items : [];
    if (!items.length) {
      setWorldChatState({ minId: 1 }, { reason: "chat_no_older" });
      return;
    }
    // 追加到 timeline 头部（保持时间正序）
    setWorldChatState(
      (prev) => {
        const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
        const first = items[0];
        return {
          ...prev,
          timeline: [...items, ...timeline],
          minId: first?.id != null ? Number(first.id) : Number(prev?.minId || 0)
        };
      },
      { reason: "chat_load_older_ok" }
    );
    prependWorldChatMessages(items);

    // 保持视口稳定：插入历史后把滚动条拉回原来的内容位置
    const nextScrollHeight = worldResultEl.scrollHeight;
    worldResultEl.scrollTop = nextScrollHeight - prevScrollHeight + prevScrollTop;
  } catch (e) {
    // 忽略
  } finally {
    setWorldChatState({ loadingOlder: false }, { reason: "chat_load_older_end" });
  }
}

async function createAndAppendChatMessage(payload) {
  try {
    const resp = await api.createChatMessage(payload);
    const msg = resp?.message;
    if (!msg) return null;
    const id = Number(msg?.id);
    setWorldChatState(
      (prev) => {
        const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
        timeline.push(msg);
        const maxId = Number.isFinite(id) && id > Number(prev?.maxId || 0) ? id : Number(prev?.maxId || 0);
        const minId = Number.isFinite(id) && id > 0 && (!prev?.minId || prev.minId <= 0)
          ? id
          : Number(prev?.minId || 0);
        return { ...prev, timeline, maxId, minId };
      },
      { reason: "chat_create_ok" }
    );
    appendWorldChatMessage(msg);
    return msg;
  } catch (e) {
    // 后端不可用时仍要在 UI 中显示用户输入（单会话体验不中断）
    const role = String(payload?.role || "").trim().toLowerCase() || "user";
    const content = String(payload?.content || "");
    if (!content.trim()) return null;
    return createTempWorldChatMessage(role, content, {
      task_id: payload?.task_id || null,
      run_id: payload?.run_id || null,
      metadata: payload?.metadata || null
    });
  }
}

function commitTempChatMessage(tempKey, savedMsg) {
  if (!savedMsg) return;
  const newKey = savedMsg?.id != null ? String(savedMsg.id) : "";
  if (!newKey) return;

  // 更新 timeline：把临时消息替换为落库后的正式消息
  setWorldChatState(
    (prev) => {
      const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
      const idx = timeline.findIndex((m) => String(m?._tmpKey || "") === String(tempKey || ""));
      if (idx >= 0) {
        timeline[idx] = savedMsg;
      } else {
        timeline.push(savedMsg);
      }
      const id = Number(savedMsg?.id);
      const maxId = Number.isFinite(id) && id > Number(prev?.maxId || 0) ? id : Number(prev?.maxId || 0);
      const minId = Number.isFinite(id) && id > 0 && (!prev?.minId || prev.minId <= 0)
        ? id
        : Number(prev?.minId || 0);
      return { ...prev, timeline, maxId, minId };
    },
    { reason: "chat_commit_temp" }
  );

  // 更新 DOM 映射：保持同一个气泡节点，避免闪烁
  const item = worldChatDomByKey.get(String(tempKey || ""));
  if (item) {
    worldChatDomByKey.delete(String(tempKey || ""));
    worldChatDomByKey.set(newKey, item);
    if (item.bubble) {
      const createdAt = String(savedMsg?.created_at || "").trim();
      const runId = savedMsg?.run_id != null ? Number(savedMsg.run_id) : null;
      const taskId = savedMsg?.task_id != null ? Number(savedMsg.task_id) : null;
      const metaParts = [];
      if (createdAt) metaParts.push(createdAt);
      if (taskId) metaParts.push(`task#${taskId}`);
      if (runId) metaParts.push(`run#${runId}`);
      if (metaParts.length) item.bubble.title = metaParts.join(" | ");
    }
  } else if (worldChatEl) {
    // 兜底：没找到临时节点就直接 append
    appendWorldChatMessage(savedMsg);
  }
}

async function _updateWorldFromBackendImpl(force = false) {
  // chat 流式时不需要刷新“Agent 进度”，避免左侧内容频繁变化；do 流式时允许刷新 plan/state。
  const session = worldSession.getState();
  if (session.streaming && session.streamingMode === "chat" && !force) return;
  if (!pageWorldEl?.classList.contains("is-visible") && !force) return;
  try {
    const current = await api.fetchCurrentAgentRun();
    const run = current?.run;
    if (!run) {
      renderWorldPlan({ items: [] });
      worldSession.setState(
        { currentRun: null, currentAgentPlan: null, currentAgentState: null, traceLines: [], streamingStatus: "" },
        { reason: "no_run" }
      );
      renderWorldThoughts(null, null);
      renderWorldEvalPlan(null);
      worldSession.setState({ lastRunMeta: null }, { reason: "no_run" });
      worldLastReviewId = null;
      worldLastReviewSignature = "";
      return;
    }

    const runId = Number(run.run_id);
    const taskId = Number(run.task_id);
    const updatedAt = String(run.updated_at || "").trim();
    worldSession.setState({ currentRun: run }, { reason: "current_run" });

    const lastRunMeta = worldSession.getState().lastRunMeta;
    const shouldRefreshDetail = force
      || !lastRunMeta
      || lastRunMeta.run_id !== runId
      || String(lastRunMeta.updated_at || "") !== updatedAt;

    if (shouldRefreshDetail) {
      const detail = await api.fetchAgentRunDetail(runId);
      renderWorldPlan(detail?.agent_plan || {});
      worldSession.setState(
        { currentAgentPlan: detail?.agent_plan || null, currentAgentState: detail?.agent_state || null },
        { reason: "agent_detail" }
      );
      rerenderWorldThoughtsFromState();
      worldSession.setState(
        { lastRunMeta: { run_id: runId, task_id: taskId, updated_at: updatedAt, status: run.status } },
        { reason: "refresh_detail" }
      );
    }

    // 思考轨迹：从最近动态里提取当前任务/run 的关键信息（tool/skill/memory/debug 等）
    updateWorldTrace(taskId, runId, shouldRefreshDetail || force);

    // 评估计划：取该 run 最新一次 review
    let hasReview = false;
    let evalFinal = false;
    try {
      const list = await api.fetchAgentReviews({ run_id: runId, limit: 1, offset: 0 });
      const reviewMeta = Array.isArray(list?.items) && list.items.length ? list.items[0] : null;
      const reviewId = reviewMeta ? Number(reviewMeta.id) : null;
      if (!reviewId) {
        worldLastReviewId = null;
        worldLastReviewSignature = "";
        renderWorldEvalPlan(null);
      } else {
        hasReview = true;
        evalFinal = isEvalFinalStatus(reviewMeta?.status);
        // 注意：评估记录可能是“先插入 running 占位 -> 再 update 成最终状态”，id 不变但 status/summary 会变。
        // 因此不能只用 id 判断是否需要刷新。
        const signature = [
          reviewId,
          String(reviewMeta?.status || ""),
          String(reviewMeta?.summary || ""),
          String(reviewMeta?.created_at || "")
        ].join("|");

        const shouldRefresh = force || worldLastReviewId !== reviewId || worldLastReviewSignature !== signature;
        if (shouldRefresh) {
          const detail = await api.fetchAgentReview(reviewId);
          worldLastReviewId = reviewId;
          worldLastReviewSignature = signature;
          renderWorldEvalPlan(detail?.review || null);
        }
      }
    } catch (e) {
      // 忽略：评估模块可能未启用/后端不可用
    }

    // 任务执行 + 评估均完成后，才把“最终回答”落库并展示
    const pendingFinal = worldSession.getState().pendingFinal;
    if (pendingFinal && String(pendingFinal.tmpKey || "") && Number(pendingFinal.run_id) === runId) {
      const runStatusLower = String(run.status || "").trim().toLowerCase();
      const age = Date.now() - Number(pendingFinal.startedAt || 0);
      const timeout = age > WORLD_EVAL_GATE_TIMEOUT_MS;

      // 默认严格等评估完成；若评估未启用/未触发，则超时后放行（避免卡死在“评估中”）
      const shouldFinalize = evalFinal || (!hasReview && runStatusLower !== "running" && timeout);
      if (shouldFinalize) {
        const finalText = String(pendingFinal.content || "").trim()
          || "任务已结束，但未产出可展示结果（缺少【结果】输出）。";
        updateWorldChatMessageContent(pendingFinal.tmpKey, finalText);
        await saveAndCommitWorldAssistantMessage(pendingFinal.tmpKey, {
          role: "assistant",
          content: finalText,
          run_id: pendingFinal.run_id,
          task_id: pendingFinal.task_id,
          metadata: pendingFinal.metadata || { source: "panel", mode: "do" }
        });
        worldSession.setState({ pendingFinal: null }, { reason: "pending_final_done" });
      }
    }

    // 同步聊天时间线（桌宠/面板都可能产生新消息）
    syncWorldChatNew();
    // 若任务来自桌宠：世界页补一个“进行中占位”，让两种形态看到的聊天区表现一致
    ensureRunAssistantPlaceholder(run);
  } catch (e) {
    // 兜底：后端不可用时不要抛到全局（避免 unhandled rejection 导致 UI 卡死）
    renderWorldPlan({ items: [] });
    renderWorldEvalPlan(null);
    if (worldThoughtsEl) setPreText(worldThoughtsEl, UI_TEXT.UNAVAILABLE || "不可用");
  } finally {}
}

async function updateWorldFromBackend(force = false) {
  return pollManager.run(WORLD_POLL_KEY, () => _updateWorldFromBackendImpl(force));
}

function startWorldPolling() {
  if (pollManager.isRunning(WORLD_POLL_KEY)) return;
  loadWorldChatHistory(false).catch(() => {});
  updateWorldFromBackend(true).catch(() => {});
  pollManager.start(WORLD_POLL_KEY, () => _updateWorldFromBackendImpl(false), UI_POLL_INTERVAL_MS, {
    runImmediately: false
  });
}

function stopWorldPolling() {
  pollManager.stop(WORLD_POLL_KEY);
}

function buildWorldChatContextMessages() {
  const system = String(UI_TEXT.PET_SYSTEM_PROMPT || "").trim();
  const chat = getWorldChatState();
  const timeline = Array.isArray(chat?.timeline) ? chat.timeline : [];
  const tail = timeline
    .filter((m) => {
      const role = String(m?.role || "").trim().toLowerCase();
      if (role !== "user" && role !== "assistant") return false;
      return !!String(m?.content || "").trim();
    })
    .slice(-WORLD_CHAT_CONTEXT_LIMIT)
    .map((m) => ({ role: String(m.role), content: String(m.content) }));

  const messages = [];
  if (system) messages.push({ role: "system", content: system });
  messages.push(...tail);
  return messages;
}

function createTempWorldChatMessage(role, content, extra = {}) {
  const chat = getWorldChatState();
  const nextSeq = Number(chat?.tempSeq || 0) + 1;
  const msg = {
    role,
    content,
    created_at: "",
    _tmpKey: `tmp-${Date.now()}-${nextSeq}`,
    ...extra
  };
  setWorldChatState(
    (prev) => {
      const timeline = Array.isArray(prev?.timeline) ? prev.timeline.slice() : [];
      timeline.push(msg);
      return { ...prev, timeline, tempSeq: nextSeq };
    },
    { reason: "chat_temp_add" }
  );
  appendWorldChatMessage(msg);
  return msg;
}

async function streamToWorld(makeRequest, options = {}) {
  const mode = String(options.mode || "").trim().toLowerCase();
  const displayMode = String(options.displayMode || "full").trim().toLowerCase();
  const assistantKey = String(options.assistantKey || "").trim();

  // 取消上一次请求（并发保护：旧请求的 finally 不应覆盖新请求状态）
  const { seq: mySeq, controller } = startStream(worldStream);
  worldSession.setState(
    { streaming: true, streamingMode: mode, pendingResume: null, streamingStatus: "" },
    { reason: "stream_start" }
  );

  const { transcript, hadError } = await streamSse(
    (signal) => makeRequest(signal),
    {
      signal: controller.signal,
      displayMode,
      shouldPauseUpdates: () => !isStreamActive(worldStream, mySeq) || !!worldSession.getState().pendingResume,
      onUpdate: (text) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        // chat 模式：中间对话区可流式更新（这是最终回答的一部分）
        if (displayMode === "full") {
          if (!assistantKey) return;
          updateWorldChatMessageContent(assistantKey, String(text || ""));
          return;
        }

        // do 模式：status 是“思考/执行状态”，只写入右上角思考框，不进入中间对话区
        worldSession.setState({ streamingStatus: String(text || "") }, { reason: "stream_status" });
        rerenderWorldThoughtsFromState();
      },
      onError: (msg) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        const text = String(msg || "请求失败");
        if (assistantKey) updateWorldChatMessageContent(assistantKey, text);
        worldSession.setState({ streamingStatus: text }, { reason: "stream_error" });
        rerenderWorldThoughtsFromState();
      },
      onRunCreated: (obj) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        worldSession.setState(
          { lastRunMeta: { run_id: obj.run_id, task_id: obj.task_id, updated_at: "", status: "running" } },
          { reason: "run_created" }
        );
        updateWorldFromBackend(true);
      },
      onNeedInput: (obj) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        const q = String(obj?.question || "").trim();
        worldSession.setState(
          { pendingResume: { runId: Number(obj.run_id), taskId: Number(obj.task_id) || null, question: q } },
          { reason: "need_input" }
        );
        updateWorldFromBackend(true);
        if (q && assistantKey) updateWorldChatMessageContent(assistantKey, q);
      },
      onPlan: (obj) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        renderWorldPlan({ items: obj.items });
      },
      onPlanDelta: (obj) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        applyWorldPlanDelta(obj);
      },
      onEvent: (obj) => {
        // 世界页发起的 SSE 事件本就在当前窗口：直接派发本地事件给各标签页订阅
        if (!isStreamActive(worldStream, mySeq)) return;
        if (obj?.type === "memory_item") {
          emitAgentEvent(obj, { broadcast: false });
        }
      },
      onReviewDelta: () => "" // 世界页不展开评估明细，避免刷屏
    }
  );

  if (stopStream(worldStream, mySeq)) {
    worldSession.setState({ streaming: false, streamingMode: "", streamingStatus: "" }, { reason: "stream_stop" });
    updateWorldFromBackend(true);
  }

  return { transcript, hadError };
}

function extractVisibleResultText(transcript) {
  const payload = extractResultPayloadText(transcript || "");
  return String(payload || "").replace(/^【结果】/g, "").trim();
}

async function saveAndCommitWorldAssistantMessage(tempKey, payload) {
  const saved = await api.createChatMessage(payload).catch(() => null);
  commitTempChatMessage(tempKey, saved?.message || null);
}

async function runWorldChatMode(userText) {
  const msg = String(userText || "").trim();
  if (!msg) return;

  await createAndAppendChatMessage({
    role: "user",
    content: msg,
    metadata: { source: "panel", mode: "chat" }
  });
  const ctx = buildWorldChatContextMessages();
  const assistantTemp = createTempWorldChatMessage(
    "assistant",
    UI_TEXT.PET_CHAT_SENDING || "…",
    { metadata: { source: "panel", mode: "chat" } }
  );
  const { transcript, hadError } = await streamToWorld(
    (signal) => api.streamPetChat({ messages: ctx }, signal),
    { mode: "chat", displayMode: "full", assistantKey: assistantTemp._tmpKey }
  );
  if (hadError) return;
  await saveAndCommitWorldAssistantMessage(assistantTemp._tmpKey, {
    role: "assistant",
    content: transcript,
    metadata: { source: "panel", mode: "chat" }
  });
}

async function runWorldDoMode(userText, options = {}) {
  const msg = String(userText || "").trim();
  if (!msg) return;

  const resumeRunId = options.resumeRunId != null ? Number(options.resumeRunId) : null;
  const resumeTaskId = options.resumeTaskId != null ? Number(options.resumeTaskId) : null;
  const isResume = !!(resumeRunId && resumeRunId > 0);

  await createAndAppendChatMessage({
    role: "user",
    content: msg,
    task_id: isResume ? resumeTaskId : null,
    run_id: isResume ? resumeRunId : null,
    metadata: { source: "panel", mode: isResume ? "resume" : "do" }
  });

  const assistantTemp = createTempWorldChatMessage(
    "assistant",
    UI_TEXT.PET_CHAT_SENDING || "…",
    isResume ? { run_id: resumeRunId, task_id: resumeTaskId } : { metadata: { source: "panel", mode: "do" } }
  );

  const { transcript, hadError } = await streamToWorld(
    (signal) => isResume
      ? api.streamAgentResume({ run_id: resumeRunId, message: msg }, signal)
      : api.streamAgentCommand({ message: msg }, signal),
    { mode: "do", displayMode: "status", assistantKey: assistantTemp._tmpKey }
  );
  if (hadError) return;

  // 若进入等待用户输入：把提问作为“assistant 消息”落库（不把规划/执行状态当作最终回答）
  const pending = worldSession.getState().pendingResume;
  if (pending && pending.runId) {
    const q = String(pending.question || "").trim() || "需要你补充信息后才能继续执行。";
    updateWorldChatMessageContent(assistantTemp._tmpKey, q);
    await saveAndCommitWorldAssistantMessage(assistantTemp._tmpKey, {
      role: "assistant",
      content: q,
      run_id: pending.runId,
      task_id: pending.taskId || null,
      metadata: { source: "panel", mode: "need_input" }
    });
    return;
  }

  const visible = extractVisibleResultText(transcript);
  const finalVisible = visible || "任务已结束，但未产出可展示结果（缺少【结果】输出）。";

  const lastRunMeta = worldSession.getState().lastRunMeta;
  const runId = isResume ? resumeRunId : (lastRunMeta?.run_id || null);
  const taskId = isResume ? resumeTaskId : (lastRunMeta?.task_id || null);

  // 关键体验：在评估 plan-list 完成前，不输出最终总结（避免用户看到“结果”但评估仍在跑）
  worldSession.setState(
    {
      pendingFinal: {
        tmpKey: assistantTemp._tmpKey,
        content: finalVisible,
        run_id: runId,
        task_id: taskId,
        metadata: { source: "panel", mode: isResume ? "resume" : "do" },
        startedAt: Date.now()
      }
    },
    { reason: "pending_final_set" }
  );
  updateWorldChatMessageContent(assistantTemp._tmpKey, "任务执行完成，评估中…");
  // 主动刷新一次，减少用户等待一个 polling interval 才看到“评估计划”的延迟
  updateWorldFromBackend(true).catch(() => {});
}

async function submitWorldInput() {
  const text = String(worldInputEl?.value || "").trim();
  if (!text) return;
  if (worldInputEl) worldInputEl.value = "";

  // 等待输入态：自动 resume
  const pending = worldSession.getState().pendingResume;
  if (pending && pending.runId) {
    worldSession.setState({ pendingResume: null }, { reason: "resume_send" });
    await runWorldDoMode(text, { resumeRunId: pending.runId, resumeTaskId: pending.taskId });
    return;
  }

  const cmd = parseSlashCommand(text);
  if (cmd?.cmd === "help") {
    const tip = "可用命令：/chat <内容>、/do <指令>。不带 / 会让后端自动判断 chat/do。";
    createTempWorldChatMessage("assistant", tip, { metadata: { source: "panel" } });
    return;
  }

  if (cmd?.cmd === "chat") {
    const msg = String(cmd.args || "").trim();
    if (!msg) return;
    await runWorldChatMode(msg);
    return;
  }

  if (cmd?.cmd === "do") {
    const msg = String(cmd.args || "").trim();
    if (!msg) return;
    await runWorldDoMode(msg);
    return;
  }

  // 默认：后端路由 chat/do
  let mode = "chat";
  try {
    const route = await api.routeAgentMode({ message: text });
    const m = String(route?.mode || "").trim().toLowerCase();
    if (m === "do" || m === "chat") mode = m;
  } catch (e) {}

  if (mode === "chat") {
    await runWorldChatMode(text);
    return;
  }
  await runWorldDoMode(text);
}

function submitWorldInputWithHistory() {
  if (!worldInputEl) {
    submitWorldInput();
    return;
  }
  const value = String(worldInputEl.value || "");
  if (!worldInputHistory.record(value)) return;
  submitWorldInput();
}

function bindWorldInteractions() {
  if (worldResultEl) {
    worldResultEl.addEventListener("scroll", () => {
      // 顶部触发“加载更早的聊天记录”
      if (worldResultEl.scrollTop <= 24) {
        loadOlderWorldChatHistory();
      }
    });
  }
  if (worldSendBtn) {
    worldSendBtn.addEventListener("click", () => {
      submitWorldInputWithHistory();
      try { worldInputEl?.focus?.(); } catch (e) {}
    });
  }
  if (worldInputEl) {
    worldInputEl.addEventListener("keydown", (event) => {
      // 输入历史：上下键回溯之前的输入
      if (worldInputHistory.handleKeyDown(event, worldInputEl)) return;

      if (event.key === "Enter") {
        event.preventDefault();
        submitWorldInputWithHistory();
        return;
      }
    });
  }
  if (worldUploadBtn) {
    // 先做占位：后续可接入实际文件注入/上传链路
    worldUploadBtn.addEventListener("click", () => {
      createTempWorldChatMessage(
        "assistant",
        "(文件上传：暂未接入后端，仅保留按钮占位)",
        { metadata: { source: "panel" } }
      );
    });
  }
}

function bindWindowControls() {
  if (!ipcRenderer) return;
  const minBtn = document.getElementById("panel-win-minimize");
  const maxBtn = document.getElementById("panel-win-maximize");
  const closeBtn = document.getElementById("panel-win-close");

  if (minBtn) {
    minBtn.addEventListener("click", () => {
      ipcRenderer.send("panel-window-control", { action: "minimize" });
    });
  }
  if (maxBtn) {
    maxBtn.addEventListener("click", () => {
      ipcRenderer.send("panel-window-control", { action: "toggle-maximize" });
    });
  }
  if (closeBtn) {
    closeBtn.addEventListener("click", () => {
      ipcRenderer.send("panel-window-control", { action: "close" });
    });
  }
}

/**
 * 绑定状态栏 (同时更新顶部 Header 和 Dashboard)
 */
async function bindStatusBar() {
  // Header Elements
  const backendEl = document.getElementById("status-backend");
  const taskCountEl = document.getElementById("status-task-count");
  const taskCurrentEl = document.getElementById("status-task-current");
  const refreshBtn = document.getElementById("status-refresh");

  // Dashboard Elements (可能不存在，取决于当前是否加载了 Dashboard)
  const getDashboardEls = () => ({
    backend: document.getElementById("dashboard-backend-status"),
    count: document.getElementById("dashboard-task-count"),
    current: document.getElementById("dashboard-task-current")
  });

  async function updateStatus() {
    // Set loading state
    const loadingText = UI_TEXT.LOADING || "...";
    if (backendEl) backendEl.textContent = loadingText;
    if (taskCountEl) taskCountEl.textContent = loadingText;
    if (taskCurrentEl) taskCurrentEl.textContent = loadingText;

    const dbEls = getDashboardEls();
    if (dbEls.backend) dbEls.backend.textContent = loadingText;
    if (dbEls.count) dbEls.count.textContent = "-";
    if (dbEls.current) dbEls.current.textContent = "-";

    try {
      const [health, summary] = await Promise.all([
        api.fetchHealth(),
        api.fetchTasksSummary()
      ]);

      const statusText = health.status || UI_TEXT.BACKEND_STATUS;
      const countText = summary.count ?? 0;
      const currentText = summary.current || UI_TEXT.NONE;

      // Update Header
      if (backendEl) backendEl.textContent = statusText;
      if (taskCountEl) taskCountEl.textContent = countText;
      if (taskCurrentEl) taskCurrentEl.textContent = currentText;

      // Update Dashboard (Re-fetch elements as they might have just rendered)
      const freshDbEls = getDashboardEls();
      if (freshDbEls.backend) {
         freshDbEls.backend.textContent = statusText;
         // Toggle styling for status
         if (statusText === 'online' || statusText === 'ok') {
            freshDbEls.backend.classList.remove('panel-tag--accent');
            freshDbEls.backend.style.backgroundColor = '#e8f5e9';
            freshDbEls.backend.style.color = '#2e7d32';
            freshDbEls.backend.style.borderColor = '#81c784';
         }
      }
      if (freshDbEls.count) freshDbEls.count.textContent = countText;
      if (freshDbEls.current) freshDbEls.current.textContent = currentText;

    } catch (error) {
      const errorText = UI_TEXT.UNAVAILABLE || "Offline";
      if (backendEl) backendEl.textContent = errorText;
      
      const freshDbEls = getDashboardEls();
      if (freshDbEls.backend) {
        freshDbEls.backend.textContent = errorText;
        freshDbEls.backend.classList.add('panel-tag--accent'); // Red/Orange
      }
    }
  }

  if (refreshBtn) {
    refreshBtn.addEventListener("click", debounce(updateStatus, 300));
  }
  
  // Initial call
  updateStatus();

  // 返回更新函数供标签页调用
  return updateStatus;
}

/**
 * 加载标签页内容
 * @param {HTMLElement} section - 标签页容器
 * @param {Function} onStatusChange - 状态变化回调
 */
async function loadSection(section, onStatusChange) {
  const src = section.dataset.src;
  // Dashboard 特殊处理：每次激活可能需要刷新数据，但 HTML 结构只加载一次
  const isLoaded = section.dataset.loaded === "true";
  
  if (!src) return;

  try {
    if (!isLoaded) {
        const response = await fetch(src);
        section.innerHTML = await response.text();
        applyText(section);
        section.dataset.loaded = "true";
    }

    // 绑定标签页逻辑
    const binder = TAB_BINDERS[section.id];
    if (binder) {
      // 清理旧的事件管理器 (Dashboard 不需要频繁清理，但为了保持一致性)
      const oldManager = tabEventManagers.get(section.id);
      if (oldManager) {
        oldManager.removeAll();
      }

      // 所有标签页现在都可以访问 onStatusChange
      const eventManager = binder(section, onStatusChange);
      tabEventManagers.set(section.id, eventManager);
    }
  } catch (error) {
    console.error(error);
    section.innerHTML = `<div class="panel-error">${UI_TEXT.LOAD_FAIL}</div>`;
  }
}

/**
 * 激活指定标签页
 * @param {string} targetId - 目标标签页 ID
 * @param {Function} onStatusChange - 状态变化回调
 */
function activateTab(targetId, onStatusChange) {
  sections.forEach((section) => {
    section.classList.toggle("is-visible", section.id === targetId);
  });
  tabs.forEach((tab) => {
    tab.classList.toggle("is-active", tab.dataset.target === targetId);
  });
  const active = document.getElementById(targetId);
  if (active) {
    loadSection(active, onStatusChange);
  }
}

function activatePage(pageId) {
  const isHome = pageId === "home";
  const isWorld = pageId === "world";
  const isState = pageId === "state";

  if (pageHomeEl) pageHomeEl.classList.toggle("is-visible", isHome);
  if (pageWorldEl) pageWorldEl.classList.toggle("is-visible", isWorld);
  if (pageStateEl) pageStateEl.classList.toggle("is-visible", isState);

  if (topbarTabsEl) topbarTabsEl.classList.toggle("is-hidden", isHome);
  if (pageTabWorldBtn) pageTabWorldBtn.classList.toggle("is-active", isWorld);
  if (pageTabStateBtn) pageTabStateBtn.classList.toggle("is-active", isState);

  // 世界页需要持续轮询 Agent run 进度；隐藏时停止轮询避免浪费
  if (isWorld) {
    startWorldPolling();
  } else {
    stopWorldPolling();
    // 离开世界页时中断可能仍在进行的流式请求，避免后台持续占用连接/写 DOM
    abortStream(worldStream);
    worldSession.setState(
      { streaming: false, streamingMode: "", pendingResume: null, streamingStatus: "" },
      { reason: "page_hide" }
    );
  }
}

/**
 * 初始化面板
 */
async function init() {
  // 应用文本
  applyText();
  document.title = UI_TEXT.PANEL_TITLE || "Agent Panel";

  // 绑定窗口按钮（无边框窗口）
  bindWindowControls();
  // 绑定世界页交互
  bindWorldInteractions();

  // 绑定状态栏并获取更新函数
  const updateStatusBar = await bindStatusBar();

  // 绑定标签页切换
  tabs.forEach((tab) => {
    tab.addEventListener("click", () => {
      activateTab(tab.dataset.target, updateStatusBar);
    });
  });

  // 绑定顶层页面切换
  if (pageTabWorldBtn) {
    pageTabWorldBtn.addEventListener("click", () => activatePage("world"));
  }
  if (pageTabStateBtn) {
    pageTabStateBtn.addEventListener("click", () => activatePage("state"));
  }
  if (panelEnterMainBtn) {
    panelEnterMainBtn.addEventListener("click", () => {
      try {
        localStorage.setItem("panel_entered_main", "1");
      } catch (e) {}
      activatePage("world");
    });
  }

  // 加载默认标签页 (Dashboard)
  const defaultTab = document.getElementById("tab-dashboard");
  if (defaultTab) {
    loadSection(defaultTab, updateStatusBar);
  } else {
     // Fallback to tasks if dashboard missing
     const tasksTab = document.getElementById("tab-tasks");
     if (tasksTab) loadSection(tasksTab, updateStatusBar);
  }

  // 默认落在“首页”：只展示“进入主页面”，避免一进来就看到世界/状态两个标签
  activatePage("home");
}

// 启动
init();

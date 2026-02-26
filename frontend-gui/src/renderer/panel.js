// 主面板模块 - 标签页切换与状态栏

import * as api from "./api.js";
import { UI_POLL_INTERVAL_MS, UI_TEXT } from "./constants.js";
import { applyText } from "./ui.js";
import { debounce } from "./utils.js";
import {
  buildNoVisibleResultText,
  extractVisibleResultText as extractVisibleResultTextFromTranscript,
  parseSlashCommand
} from "./agent_text.js";
import { createStore } from "./store.js";
import { streamSse } from "./streaming.js";
import { abortStream, createStreamController, isStreamActive, startStream, stopStream } from "./stream_controller.js";
import { PollManager } from "./poll_manager.js";
import { buildInputHistoryFromChatItems, createInputHistoryManager, mergeInputHistoryWithBackend } from "./input_history.js";
import { getPanelDomRefs } from "./dom_refs.js";
import { setMarkdownContent } from "./markdown.js";
import { AGENT_EVENT_NAME, emitAgentEvent, initAgentEventBridge } from "./agent_events.js";
import { bindPetImageFallback } from "./pet_image.js";
import { isTerminalRunStatus, normalizeRunStatusValue } from "./run_status.js";
import { buildTaskFeedbackAckText, extractRunLastError } from "./run_messages.js";
import { shouldFinalizePendingFinal } from "./pending_final.js";
import {
  inferNeedInputChoices,
  markNeedInputPromptHandled,
  NEED_INPUT_CHOICES_LIMIT_DEFAULT,
  normalizeNeedInputChoices as normalizeSharedNeedInputChoices,
  pruneNeedInputRecentRecords,
  resolvePendingResumeFromRunDetail,
  shouldSuppressNeedInputPrompt
} from "./need_input.js";
import {
  formatAgentStageLabel,
  formatDurationMs,
  formatStatusLabel,
  statusToTagClass
} from "./agent_ui.js";

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
  worldChoicesEl,
  worldUploadBtn,
  worldInputEl,
  worldSendBtn
} = getPanelDomRefs();

// 世界页桌宠图标兜底：避免某一张资源异常导致“形态区域空白”。
bindPetImageFallback(document.querySelector(".world-pet-image"));

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
let worldResumeInFlight = false;
// “任务执行结果”在评估完成前不落库（避免先输出最终结论，再补评估 plan-list）
const WORLD_EVAL_GATE_TIMEOUT_MS = 90000;
const WORLD_NEED_INPUT_RECENT_TTL_MS = 20000;
const AGENT_EVENT_SOURCE_PANEL = "panel";
// 世界页关键会话状态：集中管理，避免散落的全局变量在并发/切页时漂移
const worldSession = createStore({
  streaming: false,
  streamingMode: "",
  // 执行模式流式时（do/think/resume）的“思考状态行”（只显示在右上角思考框，不进入中间对话）
  streamingStatus: "",
  pendingResume: null, // {runId, taskId?, question, kind?, choices?, promptToken?, sessionKey?}
  needInputRecentRecords: [], // [{runId, fingerprint, at}]，抑制已处理提示被回放/轮询重复展示
  // 等待评估完成后再落库的最终回答（只用于世界页执行链路：do/think/resume）
  pendingFinal: null, // {tmpKey, content, run_id, task_id, metadata, startedAt}
  // 当前 run 元信息（来自 /agent/runs/current），用于“世界页”统一展示（不区分来源：桌宠/世界页）。
  currentRun: null,
  // 当前 run 的 agent_plan（来自 /agent/runs/{run_id}），用于在思考框中展示“当前步骤标题”等信息。
  currentAgentPlan: null,
  // 当前 run 的 agent_state（来自 /agent/runs/{run_id}）
  currentAgentState: null,
  // 当前 run 的 snapshot（stage/计数器/进度），用于“可观测性”展示
  currentAgentSnapshot: null,
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

function normalizeNeedInputChoices(rawChoices) {
  return normalizeSharedNeedInputChoices(rawChoices, { limit: NEED_INPUT_CHOICES_LIMIT_DEFAULT });
}

function markWorldNeedInputHandled(payload) {
  worldSession.setState(
    (prev) => ({
      ...prev,
      needInputRecentRecords: markNeedInputPromptHandled(
        payload,
        prev?.needInputRecentRecords,
        { ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS }
      )
    }),
    { reason: "need_input_handled" }
  );
}

function emitWorldNeedInputResolved(pending, reason = "resolved") {
  const runId = Number(pending?.runId);
  if (!Number.isFinite(runId) || runId <= 0) return;
  emitAgentEvent(
    {
      type: "need_input_resolved",
      run_id: runId,
      task_id: Number(pending?.taskId) || null,
      prompt_token: String(pending?.promptToken || "").trim() || null,
      session_key: String(pending?.sessionKey || "").trim() || null,
      question: String(pending?.question || "").trim() || null,
      reason: String(reason || "").trim() || null,
      _source: AGENT_EVENT_SOURCE_PANEL
    },
    { broadcast: true }
  );
}

function clearWorldPendingResume(options = {}) {
  const current = worldSession.getState().pendingResume;
  if (!current) {
    renderWorldNeedInputChoicesUi(null);
    return;
  }
  const targetRunId = Number(options?.runId);
  if (Number.isFinite(targetRunId) && targetRunId > 0 && Number(current?.runId) !== targetRunId) return;

  worldSession.setState(
    (prev) => ({
      ...prev,
      pendingResume: null,
      needInputRecentRecords: markNeedInputPromptHandled(
        current,
        prev?.needInputRecentRecords,
        { ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS }
      )
    }),
    { reason: options?.reason || "need_input_clear" }
  );
  renderWorldNeedInputChoicesUi(null);
  if (options?.emit !== false) {
    emitWorldNeedInputResolved(current, options?.reason || "need_input_clear");
  }
}

function applyRemoteRunCreatedToWorld(obj) {
  const rid = Number(obj?.run_id);
  const tid = Number(obj?.task_id);
  if (!Number.isFinite(rid) || rid <= 0) return;

  worldSession.setState(
    (prev) => ({
      ...prev,
      currentRun: {
        ...(prev?.currentRun || {}),
        run_id: rid,
        task_id: Number.isFinite(tid) && tid > 0 ? tid : null,
        status: "running",
        is_current: true
      },
      lastRunMeta: {
        ...(prev?.lastRunMeta || {}),
        run_id: rid,
        task_id: Number.isFinite(tid) && tid > 0 ? tid : null,
        updated_at: String(prev?.lastRunMeta?.updated_at || ""),
        status: "running"
      }
    }),
    { reason: "run_created_remote" }
  );
}

function applyRemoteRunStatusToWorld(obj) {
  const status = normalizeRunStatusValue(obj?.status);
  if (!status) return;
  const rid = Number(obj?.run_id);
  const tid = Number(obj?.task_id);

  worldSession.setState(
    (prev) => {
      const prevRun = prev?.currentRun && typeof prev.currentRun === "object" ? prev.currentRun : null;
      const prevRunId = Number(prevRun?.run_id);
      const prevTaskId = Number(prevRun?.task_id);
      const nextRunId = Number.isFinite(rid) && rid > 0
        ? rid
        : (Number.isFinite(prevRunId) && prevRunId > 0 ? prevRunId : null);
      const nextTaskId = Number.isFinite(tid) && tid > 0
        ? tid
        : (Number.isFinite(prevTaskId) && prevTaskId > 0 ? prevTaskId : null);
      const nextStatus = status || normalizeRunStatusValue(prevRun?.status) || "running";

      let nextCurrentRun = prevRun;
      if (nextRunId && (!Number.isFinite(prevRunId) || prevRunId <= 0 || prevRunId === nextRunId)) {
        nextCurrentRun = {
          ...(prevRun || {
            task_title: "",
            summary: null,
            mode: null,
            started_at: "",
            finished_at: "",
            created_at: "",
            updated_at: "",
            is_current: true
          }),
          run_id: nextRunId,
          task_id: nextTaskId || null,
          status: nextStatus
        };
      }

      const prevMeta = prev?.lastRunMeta && typeof prev.lastRunMeta === "object" ? prev.lastRunMeta : {};
      let nextLastRunMeta = prev.lastRunMeta;
      if (nextRunId) {
        nextLastRunMeta = {
          ...prevMeta,
          run_id: nextRunId,
          task_id: nextTaskId || null,
          status: nextStatus,
          updated_at: String(prevMeta.updated_at || "")
        };
      }

      return {
        ...prev,
        currentRun: nextCurrentRun,
        lastRunMeta: nextLastRunMeta
      };
    },
    { reason: "run_status_remote" }
  );

  if (status !== "waiting") {
    if (Number.isFinite(rid) && rid > 0) {
      clearWorldPendingResume({ runId: rid, reason: "run_status_remote_continue", emit: false });
    } else if (isTerminalRunStatus(status)) {
      clearWorldPendingResume({ reason: "run_status_remote_terminal", emit: false });
    }
  }

  if (pageWorldEl?.classList?.contains("is-visible")) {
    rerenderWorldThoughtsFromState();
  }
}

function applyRemoteNeedInputToWorld(obj) {
  const rid = Number(obj?.run_id);
  const tid = Number(obj?.task_id);
  const q = String(obj?.question || "").trim();
  if (!Number.isFinite(rid) || rid <= 0) return;
  if (!q) return;

  const pending = {
    runId: rid,
    taskId: Number.isFinite(tid) && tid > 0 ? tid : null,
    question: q,
    kind: String(obj?.kind || "").trim() || null,
    choices: normalizeNeedInputChoices(obj?.choices),
    promptToken: String(obj?.prompt_token || obj?.promptToken || "").trim() || null,
    sessionKey: String(obj?.session_key || obj?.sessionKey || "").trim() || null
  };

  const state = worldSession.getState();
  const recentRecords = pruneNeedInputRecentRecords(state?.needInputRecentRecords, {
    ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS
  });
  if (shouldSuppressNeedInputPrompt(pending, recentRecords, { ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS })) {
    worldSession.setState(
      { needInputRecentRecords: recentRecords },
      { reason: "need_input_remote_suppressed" }
    );
    clearWorldPendingResume({ runId: rid, reason: "need_input_remote_suppressed", emit: false });
    return;
  }

  const currentPending = state?.pendingResume;
  const isSamePending = !!currentPending
    && Number(currentPending.runId) === Number(pending.runId)
    && Number(currentPending.taskId || 0) === Number(pending.taskId || 0)
    && String(currentPending.question || "") === String(pending.question || "")
    && String(currentPending.kind || "") === String(pending.kind || "")
    && String(currentPending.promptToken || "") === String(pending.promptToken || "")
    && String(currentPending.sessionKey || "") === String(pending.sessionKey || "")
    && JSON.stringify(Array.isArray(currentPending.choices) ? currentPending.choices : [])
      === JSON.stringify(Array.isArray(pending.choices) ? pending.choices : []);
  if (isSamePending) return;

  worldSession.setState(
    (prev) => ({
      ...prev,
      pendingResume: pending,
      needInputRecentRecords: recentRecords,
      currentRun: {
        ...(prev?.currentRun || {}),
        run_id: rid,
        task_id: Number.isFinite(tid) && tid > 0 ? tid : null,
        status: "waiting",
        is_current: true
      },
      lastRunMeta: {
        ...(prev?.lastRunMeta || {}),
        run_id: rid,
        task_id: Number.isFinite(tid) && tid > 0 ? tid : null,
        updated_at: String(prev?.lastRunMeta?.updated_at || ""),
        status: "waiting"
      }
    }),
    { reason: "need_input_remote" }
  );
  renderWorldNeedInputChoicesUi(pending);
}

function renderWorldNeedInputChoicesUi(pending) {
  if (!worldChoicesEl) return;
  worldChoicesEl.innerHTML = "";

  const runId = Number(pending?.runId);
  if (!Number.isFinite(runId) || runId <= 0) {
    worldChoicesEl.classList.add("is-hidden");
    return;
  }

  const kind = String(pending?.kind || "").trim();
  const normalized = normalizeNeedInputChoices(pending?.choices);
  const choices = normalized.length
    ? normalized
    : inferNeedInputChoices(String(pending?.question || ""), kind);

  worldChoicesEl.classList.remove("is-hidden");

  function focusCustomInput() {
    try { worldInputEl?.focus?.(); } catch (e) {}
  }

  let choiceSubmitting = false;
  async function resumeWithChoice(value) {
    if (choiceSubmitting || worldResumeInFlight) return;
    const current = worldSession.getState().pendingResume;
    if (!current?.runId) return;
    const msg = String(value || "").trim();
    if (!msg) {
      focusCustomInput();
      return;
    }
    choiceSubmitting = true;
    for (const node of Array.from(worldChoicesEl.querySelectorAll("button"))) {
      try { node.disabled = true; } catch (e) {}
    }
    clearWorldPendingResume({ runId: Number(current.runId), reason: "resume_choice" });
    try {
      await runWorldDoMode(msg, {
        resumeRunId: Number(current.runId),
        resumeTaskId: Number(current.taskId) || null,
        resumePromptToken: String(current.promptToken || "").trim() || null,
        resumeSessionKey: String(current.sessionKey || "").trim() || null
      });
    } finally {
      choiceSubmitting = false;
    }
  }

  for (const c of choices) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "world-choice-btn";
    btn.textContent = String(c?.label || "").trim() || "选项";
    btn.onclick = () => resumeWithChoice(String(c?.value || "").trim());
    worldChoicesEl.appendChild(btn);
  }

  const customBtn = document.createElement("button");
  customBtn.type = "button";
  customBtn.className = "world-choice-btn";
  customBtn.textContent = "自定义输入";
  customBtn.onclick = () => focusCustomInput();
  worldChoicesEl.appendChild(customBtn);
}

function applyAgentStageEventToWorld(payload) {
  const obj = payload && typeof payload === "object" ? payload : null;
  if (!obj) return;
  const stage = String(obj.stage || "").trim();
  if (!stage) return;
  const runId = Number(obj.run_id);
  const taskId = Number(obj.task_id);
  if (!Number.isFinite(runId) || runId <= 0) return;

  const session = worldSession.getState();
  const currentRunId = Number(session?.currentRun?.run_id);
  if (Number.isFinite(currentRunId) && currentRunId > 0 && currentRunId !== runId) return;

  worldSession.setState(
    (prev) => {
      const prevSnapshot = prev?.currentAgentSnapshot && typeof prev.currentAgentSnapshot === "object"
        ? prev.currentAgentSnapshot
        : {};
      const nextSnapshot = { ...prevSnapshot, stage };

      // 若世界页尚未轮询到 currentRun，则先用最小元信息占位，保证“阶段”可立即渲染。
      const prevRunId = Number(prev?.currentRun?.run_id);
      let nextRun = prev?.currentRun;
      if (!Number.isFinite(prevRunId) || prevRunId <= 0) {
        nextRun = {
          run_id: runId,
          task_id: Number.isFinite(taskId) && taskId > 0 ? taskId : null,
          task_title: "",
          status: "running",
          summary: null,
          mode: null,
          started_at: "",
          finished_at: "",
          created_at: "",
          updated_at: "",
          is_current: true
        };
      }

      return { ...prev, currentRun: nextRun, currentAgentSnapshot: nextSnapshot };
    },
    { reason: "agent_stage_event" }
  );

  // 仅在世界页可见时才写 DOM，避免后台窗口频繁重渲染影响性能。
  if (pageWorldEl?.classList?.contains("is-visible")) {
    rerenderWorldThoughtsFromState();
  }
}

function computePlanSnapshotFromItems(items, agentState, snapshot) {
  const list = Array.isArray(items) ? items : [];
  const byStatus = {};
  for (const raw of list) {
    const it = raw && typeof raw === "object" ? raw : {};
    const st = normalizePlanItemStatus(it?.status);
    byStatus[st] = (byStatus[st] || 0) + 1;
  }

  const total = list.length;
  const done = Number(byStatus.done || 0);
  const running = Number(byStatus.running || 0);
  const waiting = Number(byStatus.waiting || 0);
  const failed = Number(byStatus.failed || 0);
  const skipped = Number(byStatus.skipped || 0);
  const pending = Number(byStatus.pending || 0);
  const progress = total > 0 ? Math.round((done / total) * 10000) / 10000 : 0;

  const stepOrder = inferPlanCurrentStepOrder(list, agentState, snapshot);
  let currentStep = null;
  if (Number.isFinite(stepOrder) && stepOrder > 0) {
    const idx = stepOrder - 1;
    const it = idx >= 0 && idx < list.length && typeof list[idx] === "object" ? list[idx] : null;
    if (it) {
      const brief = String(it.brief || "").trim();
      const title = String(it.title || "").trim();
      const status = String(it.status || "").trim();
      currentStep = {
        step_order: stepOrder,
        brief: brief || null,
        title: title || null,
        status: status || null,
        allow: Array.isArray(it.allow) ? it.allow.slice() : [],
        executor: null
      };
    } else {
      currentStep = { step_order: stepOrder, brief: null, title: null, status: null, allow: [], executor: null };
    }
  }

  return {
    total,
    done,
    running,
    waiting,
    failed,
    skipped,
    pending,
    by_status: byStatus,
    progress,
    current_step: currentStep
  };
}

function applyAgentPlanEventToWorld(payload) {
  const obj = payload && typeof payload === "object" ? payload : null;
  if (!obj) return;
  const items = Array.isArray(obj.items) ? obj.items : null;
  if (!items) return;
  const runId = Number(obj.run_id);
  const taskId = Number(obj.task_id);
  if (!Number.isFinite(runId) || runId <= 0) return;

  const session = worldSession.getState();
  const currentRunId = Number(session?.currentRun?.run_id);
  if (Number.isFinite(currentRunId) && currentRunId > 0 && currentRunId !== runId) return;

  const normalizedItems = items.map((it) => (it && typeof it === "object" ? { ...it } : {}));
  const planSnapshot = computePlanSnapshotFromItems(normalizedItems, session?.currentAgentState, session?.currentAgentSnapshot);

  worldSession.setState(
    (prev) => {
      const prevRunId = Number(prev?.currentRun?.run_id);
      let nextRun = prev?.currentRun;
      if (!Number.isFinite(prevRunId) || prevRunId <= 0) {
        nextRun = {
          run_id: runId,
          task_id: Number.isFinite(taskId) && taskId > 0 ? taskId : null,
          task_title: "",
          status: "running",
          summary: null,
          mode: null,
          started_at: "",
          finished_at: "",
          created_at: "",
          updated_at: "",
          is_current: true
        };
      }

      const prevPlan = prev?.currentAgentPlan && typeof prev.currentAgentPlan === "object" ? prev.currentAgentPlan : {};
      const nextPlan = { ...prevPlan, items: normalizedItems };

      const prevSnapshot = prev?.currentAgentSnapshot && typeof prev.currentAgentSnapshot === "object"
        ? prev.currentAgentSnapshot
        : {};
      const nextSnapshot = { ...prevSnapshot, plan: planSnapshot };

      return { ...prev, currentRun: nextRun, currentAgentPlan: nextPlan, currentAgentSnapshot: nextSnapshot };
    },
    { reason: "agent_plan_event" }
  );

  if (pageWorldEl?.classList?.contains("is-visible")) {
    renderWorldPlan({ items: normalizedItems });
    rerenderWorldThoughtsFromState();
  }
}

function applyAgentPlanDeltaEventToWorld(payload) {
  const obj = payload && typeof payload === "object" ? payload : null;
  if (!obj) return;
  const changes = Array.isArray(obj.changes) ? obj.changes : null;
  if (!changes || changes.length === 0) return;
  const runId = Number(obj.run_id);
  if (!Number.isFinite(runId) || runId <= 0) return;

  const session = worldSession.getState();
  const currentRunId = Number(session?.currentRun?.run_id);
  if (Number.isFinite(currentRunId) && currentRunId > 0 && currentRunId !== runId) return;

  // 若还没收到全量 plan，则忽略 plan_delta（按契约等待下一次 plan 或轮询）
  if (!Array.isArray(worldTaskPlanItems) || worldTaskPlanItems.length === 0) return;

  applyWorldPlanDelta(obj);

  const nextPlanSnapshot = computePlanSnapshotFromItems(
    worldTaskPlanItems,
    worldSession.getState()?.currentAgentState,
    worldSession.getState()?.currentAgentSnapshot
  );

  worldSession.setState(
    (prev) => {
      const prevPlan = prev?.currentAgentPlan && typeof prev.currentAgentPlan === "object" ? prev.currentAgentPlan : {};
      const nextPlan = { ...prevPlan, items: Array.isArray(worldTaskPlanItems) ? worldTaskPlanItems.map((it) => ({ ...(it || {}) })) : [] };

      const prevSnapshot = prev?.currentAgentSnapshot && typeof prev.currentAgentSnapshot === "object"
        ? prev.currentAgentSnapshot
        : {};
      const nextSnapshot = { ...prevSnapshot, plan: nextPlanSnapshot };

      return { ...prev, currentAgentPlan: nextPlan, currentAgentSnapshot: nextSnapshot };
    },
    { reason: "agent_plan_delta_event" }
  );

  if (pageWorldEl?.classList?.contains("is-visible")) {
    rerenderWorldThoughtsFromState();
  }
}

try {
  // 订阅跨窗口 Agent 事件（桌宠窗口的 SSE 可实时驱动世界页的“阶段”展示）
  window.addEventListener(
    AGENT_EVENT_NAME,
    (event) => {
      const obj = event?.detail;
      const source = String(obj?._source || "").trim();
      if (source === AGENT_EVENT_SOURCE_PANEL) return;
      if (obj?.type === "run_created") applyRemoteRunCreatedToWorld(obj);
      if (obj?.type === "run_status") applyRemoteRunStatusToWorld(obj);
      if (obj?.type === "need_input") applyRemoteNeedInputToWorld(obj);
      if (obj?.type === "agent_stage") applyAgentStageEventToWorld(obj);
      if (obj?.type === "plan") applyAgentPlanEventToWorld(obj);
      if (obj?.type === "plan_delta") applyAgentPlanDeltaEventToWorld(obj);
      if (obj?.type === "need_input_resolved") {
        const runId = Number(obj?.run_id);
        if (Number.isFinite(runId) && runId > 0) {
          markWorldNeedInputHandled({
            runId,
            question: obj?.question,
            kind: obj?.kind,
            promptToken: obj?.prompt_token,
            sessionKey: obj?.session_key
          });
          clearWorldPendingResume({ runId, reason: "need_input_remote_resolved", emit: false });
        }
      }
    },
    { passive: true }
  );
} catch (e) {}

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

function normalizeThoughtSectionTitle(raw) {
  const text = String(raw || "").trim();
  if (!text) return "";
  const matched = text.match(/^【(.+?)】$/);
  if (matched) return String(matched[1] || "").trim();
  return text;
}

function tagClassFromThoughtBadge(rawBadge) {
  const badge = String(rawBadge || "").trim().toUpperCase();
  if (!badge) return "panel-tag";
  if (badge === "OK" || badge === "DONE" || badge === "PASS") return "panel-tag panel-tag--success";
  if (badge === "WARN" || badge === "WAIT" || badge === "WAITING") return "panel-tag panel-tag--warning";
  if (badge === "FAIL" || badge === "ERROR" || badge === "ERR") return "panel-tag panel-tag--error";
  return "panel-tag panel-tag--accent";
}

function splitThoughtBadge(text) {
  const raw = String(text || "").trim();
  if (!raw) return { badge: "", content: "" };
  const matched = raw.match(/^\[([A-Za-z_]+)\]\s*(.*)$/);
  if (!matched) return { badge: "", content: raw };
  return {
    badge: String(matched[1] || "").trim().toUpperCase(),
    content: String(matched[2] || "").trim() || raw
  };
}

function renderThoughtItemContent(container, content) {
  const text = String(content || "").trim();
  if (!text) return;

  const sepCandidates = [text.indexOf("："), text.indexOf(":")].filter((idx) => idx > 0);
  const sep = sepCandidates.length ? Math.min(...sepCandidates) : -1;
  if (sep > 0 && sep <= 24) {
    const label = String(text.slice(0, sep) || "").trim();
    const value = String(text.slice(sep + 1) || "").trim();
    if (label && value && !/[\/\\]/.test(label)) {
      const labelEl = document.createElement("span");
      labelEl.className = "world-thoughts-label";
      labelEl.textContent = `${label}：`;

      const valueEl = document.createElement("span");
      valueEl.className = "world-thoughts-value";
      valueEl.textContent = value;

      container.appendChild(labelEl);
      container.appendChild(valueEl);
      return;
    }
  }

  const textEl = document.createElement("span");
  textEl.className = "world-thoughts-text";
  textEl.textContent = text;
  container.appendChild(textEl);
}

function renderThoughtLinesStructured(el, lines) {
  if (!el) return;
  const list = Array.isArray(lines) ? lines : [];
  const normalized = list.map((line) => String(line || "").trimEnd()).filter((line) => String(line || "").trim());
  if (!normalized.length) {
    setEmptyText(el, UI_TEXT.NO_DATA || "-");
    return;
  }

  const container = el;
  const prevScrollTop = container.scrollTop;
  const shouldStickToBottom = container.scrollTop + container.clientHeight >= container.scrollHeight - 48;

  const root = document.createElement("div");
  root.className = "world-thoughts";

  let currentList = null;
  const ensureSection = (title) => {
    const section = document.createElement("section");
    section.className = "world-thoughts-section";

    const titleEl = document.createElement("div");
    titleEl.className = "world-thoughts-section-title";
    titleEl.textContent = normalizeThoughtSectionTitle(title) || (UI_TEXT.DASH || "-");

    const listEl = document.createElement("div");
    listEl.className = "world-thoughts-list";

    section.appendChild(titleEl);
    section.appendChild(listEl);
    root.appendChild(section);
    currentList = listEl;
    return listEl;
  };

  normalized.forEach((rawLine) => {
    const sectionMatched = String(rawLine || "").trim().match(/^【(.+?)】$/);
    if (sectionMatched) {
      ensureSection(sectionMatched[0]);
      return;
    }

    if (!currentList) {
      ensureSection(UI_TEXT.WORLD_THOUGHTS_SECTION_STATUS || "【状态】");
    }

    const bulletMatched = String(rawLine || "").match(/^(\s*)-\s+(.*)$/);
    const indentSpaces = bulletMatched ? String(bulletMatched[1] || "").length : 0;
    const contentText = bulletMatched ? String(bulletMatched[2] || "").trim() : String(rawLine || "").trim();
    if (!contentText) return;

    const item = document.createElement("div");
    item.className = "world-thoughts-item";
    if (indentSpaces >= 2) item.classList.add("is-sub");

    const { badge, content } = splitThoughtBadge(contentText);
    if (badge) {
      const tagEl = document.createElement("span");
      tagEl.className = tagClassFromThoughtBadge(badge);
      tagEl.textContent = badge;
      item.appendChild(tagEl);
    }

    const contentEl = document.createElement("div");
    contentEl.className = "world-thoughts-item-main";
    renderThoughtItemContent(contentEl, content);
    item.appendChild(contentEl);

    currentList.appendChild(item);
  });

  el.innerHTML = "";
  el.appendChild(root);

  if (shouldStickToBottom) {
    container.scrollTop = container.scrollHeight;
  } else {
    container.scrollTop = prevScrollTop;
  }
}

function renderPlanList(el, items, options = {}) {
  if (!el) return;
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    setEmptyText(el, UI_TEXT.NO_DATA || "-");
    return;
  }

  const currentStepOrder = Number(options?.currentStepOrder);
  const titles = Array.isArray(options?.titles) ? options.titles : [];

  const container = el;
  const prevScrollTop = container.scrollTop;
  const shouldStickToBottom =
    container.scrollTop + container.clientHeight >= container.scrollHeight - 48;

  el.innerHTML = "";
  list.forEach((item, idx) => {
    const row = document.createElement("div");
    row.className = "panel-list-item";
    const id = Number(item?.id);
    const stepId = Number.isFinite(id) && id > 0 ? id : (idx + 1);
    if (Number.isFinite(currentStepOrder) && currentStepOrder > 0 && stepId === currentStepOrder) {
      row.classList.add("is-current");
    }
    if (Number.isFinite(stepId) && stepId > 0 && stepId <= titles.length) {
      const title = String(titles[stepId - 1] || "").trim();
      if (title) row.title = title;
    }

    const text = document.createElement("div");
    text.className = "panel-list-item-content";
    text.textContent = String(item?.brief || item?.title || "").trim() || UI_TEXT.DASH;

    const tag = document.createElement("span");
    tag.className = `panel-tag ${statusToTagClass(item?.status)}`.trim();
    tag.textContent = formatStatusLabel(item?.status);

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

function upsertTimelineMessageById(timeline, msg) {
  const list = Array.isArray(timeline) ? timeline : [];
  const id = Number(msg?.id);
  if (!Number.isFinite(id) || id <= 0) {
    list.push(msg);
    return list;
  }
  const idx = list.findIndex((item) => Number(item?.id) === id);
  if (idx >= 0) {
    list[idx] = msg;
    return list;
  }
  list.push(msg);
  return list;
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
  const runMode = String(runMeta?.mode || "").trim().toLowerCase();
  const msg = {
    role: "assistant",
    content: UI_TEXT.PET_CHAT_SENDING || "…",
    created_at: "",
    run_id: Number.isFinite(runId) ? runId : null,
    task_id: Number.isFinite(taskId) ? taskId : null,
    _tmpKey: key,
    metadata: { source: "panel", placeholder: true, mode: runMode || "do" }
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
  const key = worldChatKey(msg);
  const existing = key ? worldChatDomByKey.get(key) : null;
  if (existing?.bubble) {
    const role = String(msg?.role || "").trim().toLowerCase();
    const content = String(msg?.content || "");
    if (role === "assistant") {
      setMarkdownContent(existing.bubble, content);
    } else {
      existing.bubble.textContent = content;
    }

    const createdAt = String(msg?.created_at || "").trim();
    const runId = msg?.run_id != null ? Number(msg.run_id) : null;
    const taskId = msg?.task_id != null ? Number(msg.task_id) : null;
    const metaParts = [];
    if (createdAt) metaParts.push(createdAt);
    if (taskId) metaParts.push(`task#${taskId}`);
    if (runId) metaParts.push(`run#${runId}`);
    existing.bubble.title = metaParts.join(" | ");

    if (stick) worldChatScrollToBottom();
    return;
  }

  // 若当前是空态占位，先清空
  if (worldChatEl.querySelector(".world-chat-empty")) {
    worldChatEl.innerHTML = "";
  }
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
function normalizePlanItemStatus(status) {
  const raw = String(status || "").trim().toLowerCase();
  if (!raw) return "pending";
  if (raw === "planned") return "pending";
  if (raw === "queued") return "pending";
  return raw;
}

function inferPlanCurrentStepOrder(items, agentState, snapshot) {
  const fromState = Number(agentState?.step_order);
  if (Number.isFinite(fromState) && fromState > 0) return fromState;

  const fromSnapshot = Number(snapshot?.plan?.current_step?.step_order);
  if (Number.isFinite(fromSnapshot) && fromSnapshot > 0) return fromSnapshot;

  const list = Array.isArray(items) ? items : [];

  const pickFirstByStatus = (wanted) => {
    const idx = list.findIndex((it) => normalizePlanItemStatus(it?.status) === wanted);
    if (idx < 0) return null;
    const id = Number(list[idx]?.id);
    if (Number.isFinite(id) && id > 0) return id;
    return idx + 1;
  };

  const running = pickFirstByStatus("running");
  if (running != null) return running;
  const waiting = pickFirstByStatus("waiting");
  if (waiting != null) return waiting;

  // 找到第一个“非终态”步骤作为当前步骤（用于刚规划完成/尚未写 step_order 的早期阶段）
  for (let i = 0; i < list.length; i++) {
    const st = normalizePlanItemStatus(list[i]?.status);
    if (st === "done" || st === "failed" || st === "skipped" || st === "stopped" || st === "cancelled") continue;
    const id = Number(list[i]?.id);
    if (Number.isFinite(id) && id > 0) return id;
    return i + 1;
  }

  return null;
}

function renderWorldPlan(planObj) {
  if (!worldTaskPlanEl) return;
  const items = planObj?.items;
  worldTaskPlanItems = Array.isArray(items) ? items.map((it) => (it && typeof it === "object" ? { ...it } : {})) : [];
  const session = worldSession.getState();
  const titles = Array.isArray(planObj?.titles)
    ? planObj.titles
    : (Array.isArray(session?.currentAgentPlan?.titles) ? session.currentAgentPlan.titles : []);
  const stepOrder = inferPlanCurrentStepOrder(worldTaskPlanItems, session?.currentAgentState, session?.currentAgentSnapshot);
  renderPlanList(worldTaskPlanEl, worldTaskPlanItems, { currentStepOrder: stepOrder, titles });
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

    const orig = worldTaskPlanItems[idx] && typeof worldTaskPlanItems[idx] === "object" ? worldTaskPlanItems[idx] : {};
    const base = { ...orig };
    if (ch.status != null) base.status = ch.status;
    if (ch.brief != null) base.brief = ch.brief;
    if (ch.title != null) base.title = ch.title;
    if (!base.id && (Number.isFinite(id) && id > 0)) base.id = id;
    worldTaskPlanItems[idx] = base;
  }

  const session = worldSession.getState();
  const currentStepOrder = inferPlanCurrentStepOrder(worldTaskPlanItems, session?.currentAgentState, session?.currentAgentSnapshot);
  const titles = Array.isArray(session?.currentAgentPlan?.titles) ? session.currentAgentPlan.titles : [];
  renderPlanList(worldTaskPlanEl, worldTaskPlanItems, { currentStepOrder, titles });
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

  // 执行模式流式时（do/think/resume）：把状态行固定显示在思考框顶部（中间对话区不显示这些“思考过程”）
  const streamingStatus = String(session?.streamingStatus || "").trim();
  if (session?.streaming && String(session?.streamingMode || "") !== "chat" && streamingStatus) {
    lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_STATUS || "【状态】");
    lines.push(`- ${truncateInline(streamingStatus, 240)}`);
    lines.push("");
  }

  const taskId = runMeta?.task_id;
  const runId = runMeta?.run_id;
  const status = String(runMeta?.status || "").trim();
  const taskTitle = truncateInline(runMeta?.task_title || runMeta?.title || "", 120);
  if (taskId && runId) {
    if (!lines.length) lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_STATUS || "【状态】");
    lines.push(`- task#${taskId} / run#${runId} / ${status || "-"}${taskTitle ? ` | ${taskTitle}` : ""}`);
  }

  // P3：可观测性 - 快照指标（stage/进度/计数器）
  const snapshot = session?.currentAgentSnapshot;
  if (snapshot && typeof snapshot === "object") {
    const stage = formatAgentStageLabel(snapshot?.stage);
    const elapsed = snapshot?.elapsed_ms != null ? formatDurationMs(snapshot.elapsed_ms) : (UI_TEXT.DASH || "-");
    const plan = snapshot?.plan && typeof snapshot.plan === "object" ? snapshot.plan : {};
    const total = Number(plan?.total);
    const done = Number(plan?.done);
    const progress = Number(plan?.progress);
    const counters = snapshot?.counters && typeof snapshot.counters === "object" ? snapshot.counters : {};
    const llm = counters?.llm && typeof counters.llm === "object" ? counters.llm : {};
    const tools = counters?.tools && typeof counters.tools === "object" ? counters.tools : {};
    const lastErr = counters?.last_error && typeof counters.last_error === "object" ? counters.last_error : null;

    lines.push("");
    lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_METRICS || "【指标】");
    lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_STAGE || "阶段"}：${stage || (UI_TEXT.DASH || "-")}`);
    lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_ELAPSED || "用时"}：${elapsed}`);
    if (Number.isFinite(total) && total > 0) {
      const pct = Number.isFinite(progress) ? Math.round(progress * 100) : null;
      const pctText = pct != null ? ` (${pct}%)` : "";
      lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_PROGRESS || "进度"}：${Number.isFinite(done) ? done : "-"} / ${total}${pctText}`);
    }
    const llmCalls = Number(llm?.calls);
    const tokensTotal = Number(llm?.tokens_total);
    const toolCalls = Number(tools?.calls);
    const reuseCalls = Number(tools?.reuse_calls);
    const reusePassRate = Number(tools?.reuse_pass_rate);
    const parts = [];
    if (Number.isFinite(llmCalls)) parts.push(`${UI_TEXT.WORLD_THOUGHTS_LABEL_LLM || "LLM"} ${llmCalls}`);
    if (Number.isFinite(tokensTotal)) parts.push(`${UI_TEXT.WORLD_THOUGHTS_LABEL_TOKENS || "tokens"} ${tokensTotal}`);
    if (Number.isFinite(toolCalls)) parts.push(`${UI_TEXT.WORLD_THOUGHTS_LABEL_TOOLS || "工具"} ${toolCalls}`);
    if (Number.isFinite(reuseCalls)) parts.push(`${UI_TEXT.WORLD_THOUGHTS_LABEL_REUSE || "复用"} ${reuseCalls}`);
    if (Number.isFinite(reusePassRate)) parts.push(`${UI_TEXT.WORLD_THOUGHTS_LABEL_PASS_RATE || "通过率"} ${Math.round(reusePassRate * 100)}%`);
    if (parts.length) lines.push(`- ${parts.join(" | ")}`);
    if (lastErr && (lastErr.error || lastErr.title)) {
      const head = lastErr.step_order ? `#${lastErr.step_order} ` : "";
      const title = String(lastErr.title || "").trim();
      const err = String(lastErr.error || "").trim();
      lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_LAST_ERROR || "最近错误"}：${head}${truncateInline(title || err, 220)}`);
    }
  }

  const plan = session?.currentAgentPlan;
  const planTitles = Array.isArray(plan?.titles) ? plan.titles : [];
  const planItems = Array.isArray(plan?.items) ? plan.items : (Array.isArray(worldTaskPlanItems) ? worldTaskPlanItems : []);

  let stepOrder = stateObj?.step_order;
  if (stepOrder == null) {
    stepOrder = inferPlanCurrentStepOrder(planItems, stateObj, session?.currentAgentSnapshot);
  }
  if (stepOrder != null) {
    const stepNum = Number(stepOrder);
    const total = planTitles.length ? planTitles.length : null;
    const idx = Number.isFinite(stepNum) ? stepNum - 1 : -1;
    const stepTitle = idx >= 0 && idx < planTitles.length ? String(planTitles[idx] || "").trim() : "";
    const stepBrief = idx >= 0 && idx < planItems.length ? String(planItems[idx]?.brief || "").trim() : "";
    const stepStatus = idx >= 0 && idx < planItems.length ? String(planItems[idx]?.status || "").trim() : "";

    if (!lines.length) lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_STATUS || "【状态】");
    lines.push("");
    lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_STEP || "【步骤】");
    const head = total ? `#${stepNum}/${total}` : `#${stepNum}`;
    lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_CURRENT || "当前"}：${head}${stepBrief ? ` ${truncateInline(stepBrief, 24)}` : ""}${stepStatus ? ` (${formatStatusLabel(stepStatus)})` : ""}`);
    if (stepTitle) lines.push(`  - ${truncateInline(stepTitle, 220)}`);
  }

  const paused = stateObj?.paused;
  const question = paused && typeof paused === "object" ? String(paused.question || "").trim() : "";
  if (question) {
    const stepTitle = paused && typeof paused === "object" ? String(paused.step_title || "").trim() : "";
    lines.push("");
    lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_WAITING || "【等待输入】");
    if (stepTitle) lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_STEP || "步骤"}：${truncateInline(stepTitle, 220)}`);
    lines.push(`- ${UI_TEXT.WORLD_THOUGHTS_LABEL_QUESTION || "问题"}：${truncateInline(question, 260)}`);
  }

  const obs = stateObj?.observations;
  if (Array.isArray(obs) && obs.length) {
    const tail = obs.slice(Math.max(0, obs.length - 10));
    lines.push("");
    lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_OBSERVATION || "【观测】");
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
    lines.push(UI_TEXT.WORLD_THOUGHTS_SECTION_TRACE || "【轨迹】");
    traceLines.slice(0, 20).forEach((t) => {
      const text = String(t || "").trim();
      if (text) lines.push(`- ${truncateInline(text, 240)}`);
    });
  }

  if (!lines.length) {
    setEmptyText(worldThoughtsEl, UI_TEXT.NO_DATA || "-");
    return;
  }
  renderThoughtLinesStructured(worldThoughtsEl, lines);
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
          upsertTimelineMessageById(timeline, m);
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
        upsertTimelineMessageById(timeline, msg);
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
      const tmpKey = String(tempKey || "");
      for (let i = timeline.length - 1; i >= 0; i--) {
        if (String(timeline[i]?._tmpKey || "") === tmpKey) {
          timeline.splice(i, 1);
        }
      }
      upsertTimelineMessageById(timeline, savedMsg);

      const savedId = Number(savedMsg?.id);
      if (Number.isFinite(savedId) && savedId > 0) {
        let seen = false;
        for (let i = timeline.length - 1; i >= 0; i--) {
          if (Number(timeline[i]?.id) !== savedId) continue;
          if (!seen) {
            timeline[i] = savedMsg;
            seen = true;
            continue;
          }
          timeline.splice(i, 1);
        }
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
    const existed = worldChatDomByKey.get(newKey);
    if (existed && existed !== item) {
      try { existed.row?.remove(); } catch (e) {}
    }
    worldChatDomByKey.delete(String(tempKey || ""));
    worldChatDomByKey.set(newKey, item);
    if (item.bubble) {
      const role = String(savedMsg?.role || "").trim().toLowerCase();
      const content = String(savedMsg?.content || "");
      if (role === "assistant") {
        setMarkdownContent(item.bubble, content);
      } else {
        item.bubble.textContent = content;
      }
      const createdAt = String(savedMsg?.created_at || "").trim();
      const runId = savedMsg?.run_id != null ? Number(savedMsg.run_id) : null;
      const taskId = savedMsg?.task_id != null ? Number(savedMsg.task_id) : null;
      const metaParts = [];
      if (createdAt) metaParts.push(createdAt);
      if (taskId) metaParts.push(`task#${taskId}`);
      if (runId) metaParts.push(`run#${runId}`);
      item.bubble.title = metaParts.join(" | ");
    }
  } else if (worldChatEl) {
    // 兜底：没找到临时节点就直接 upsert 到 DOM（appendWorldChatMessage 内部已做幂等）
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
        { currentRun: null, currentAgentPlan: null, currentAgentState: null, currentAgentSnapshot: null, traceLines: [], streamingStatus: "" },
        { reason: "no_run" }
      );
      clearWorldPendingResume({ reason: "no_run", emit: false });
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
      worldSession.setState(
        { currentAgentPlan: detail?.agent_plan || null, currentAgentState: detail?.agent_state || null, currentAgentSnapshot: detail?.snapshot || null },
        { reason: "agent_detail" }
      );
      renderWorldPlan(detail?.agent_plan || {});
      rerenderWorldThoughtsFromState();
      worldSession.setState(
        { lastRunMeta: { run_id: runId, task_id: taskId, updated_at: updatedAt, status: run.status } },
        { reason: "refresh_detail" }
      );
    }

    // 思考轨迹：从最近动态里提取当前任务/run 的关键信息（tool/skill/memory/debug 等）
    updateWorldTrace(taskId, runId, shouldRefreshDetail || force);

    // waiting -> 允许世界页接管交互（页面刷新/错过 SSE 的兜底）
    const latest = worldSession.getState();
    if (!latest.streaming) {
      const statusLower = String(run.status || "").trim().toLowerCase();
      const paused = latest?.currentAgentState?.paused;
      const question = String(paused?.question || "").trim();
      if (statusLower === "waiting" && question) {
        const existing = latest.pendingResume;
        const nextPending = {
          runId,
          taskId: Number.isFinite(taskId) && taskId > 0 ? taskId : null,
          question,
          kind: String(paused?.kind || "").trim() || null,
          choices: normalizeNeedInputChoices(paused?.choices),
          promptToken: String(paused?.prompt_token || "").trim() || null,
          sessionKey: String(paused?.session_key || latest?.currentAgentState?.session_key || "").trim() || null
        };
        const recentRecords = pruneNeedInputRecentRecords(latest?.needInputRecentRecords, {
          ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS
        });
        const suppressed = shouldSuppressNeedInputPrompt(nextPending, recentRecords, {
          ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS
        });
        const shouldSet = !existing
          || Number(existing.runId) !== runId
          || String(existing.question || "") !== question
          || String(existing.kind || "") !== String(nextPending.kind || "")
          || String(existing.promptToken || "") !== String(nextPending.promptToken || "")
          || String(existing.sessionKey || "") !== String(nextPending.sessionKey || "");
        if (suppressed) {
          worldSession.setState({ needInputRecentRecords: recentRecords }, { reason: "need_input_poll_suppressed" });
          clearWorldPendingResume({ runId, reason: "need_input_poll_suppressed", emit: false });
        } else if (shouldSet) {
          worldSession.setState(
            { pendingResume: nextPending, needInputRecentRecords: recentRecords },
            { reason: "need_input_poll" }
          );
          renderWorldNeedInputChoicesUi(nextPending);
        } else {
          renderWorldNeedInputChoicesUi(existing);
        }
      } else {
        const existing = latest.pendingResume;
        if (existing) {
          clearWorldPendingResume({ runId, reason: "need_input_clear", emit: false });
        }
      }
    }

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
      const terminal = runStatusLower === "done" || runStatusLower === "failed" || runStatusLower === "stopped";

      // 默认优先等评估完成；但只要 run 已终态且超过门限，也要放行，
      // 避免“review 记录存在但长期非终态”导致对话一直停在“评估中...”。
      const shouldFinalize = shouldFinalizePendingFinal({
        evalFinal,
        hasReview,
        terminal,
        ageMs: age,
        timeoutMs: WORLD_EVAL_GATE_TIMEOUT_MS
      });
      if (shouldFinalize) {
        const fallbackLastError = extractRunLastError({
          snapshot: worldSession.getState()?.currentAgentSnapshot || null
        });
        const finalText = String(pendingFinal.content || "").trim()
          || await buildWorldNoVisibleResultText(runStatusLower, pendingFinal.run_id, fallbackLastError);
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
    if (worldThoughtsEl) {
      renderThoughtLinesStructured(worldThoughtsEl, [
        UI_TEXT.WORLD_THOUGHTS_SECTION_STATUS || "【状态】",
        `- ${UI_TEXT.UNAVAILABLE || "不可用"}`
      ]);
    }
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
  const enableAgentReplay = options.enableAgentReplay === true;
  const expectedRunIdValue = Number(options.expectedRunId);
  const expectedRunId = Number.isFinite(expectedRunIdValue) && expectedRunIdValue > 0
    ? expectedRunIdValue
    : null;
  let streamRunId = null;
  let streamTaskId = null;
  let streamRunStatus = "";

  // 取消上一次请求（并发保护：旧请求的 finally 不应覆盖新请求状态）
  const { seq: mySeq, controller } = startStream(worldStream);
  worldSession.setState(
    (prev) => ({
      ...prev,
      streaming: true,
      streamingMode: mode,
      pendingResume: null,
      streamingStatus: "",
      needInputRecentRecords: pruneNeedInputRecentRecords(
        prev?.needInputRecentRecords,
        { ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS }
      )
    }),
    { reason: "stream_start" }
  );
  renderWorldNeedInputChoicesUi(null);

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
        emitAgentEvent(
          { ...(obj || {}), type: "run_created", _source: AGENT_EVENT_SOURCE_PANEL },
          { broadcast: true }
        );
        const rid = Number(obj?.run_id);
        const tid = Number(obj?.task_id);
        if (Number.isFinite(rid) && rid > 0) streamRunId = rid;
        if (Number.isFinite(tid) && tid > 0) streamTaskId = tid;
        streamRunStatus = "running";
        const runMeta = {
          run_id: Number(obj.run_id),
          task_id: Number(obj.task_id),
          task_title: "",
          status: "running",
          summary: null,
          mode: null,
          started_at: "",
          finished_at: "",
          created_at: "",
          updated_at: "",
          is_current: true
        };
        worldSession.setState(
          { currentRun: runMeta, lastRunMeta: { run_id: obj.run_id, task_id: obj.task_id, updated_at: "", status: "running" } },
          { reason: "run_created" }
        );
        updateWorldFromBackend(true);
      },
      onRunStatus: (obj) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        emitAgentEvent(
          { ...(obj || {}), type: "run_status", _source: AGENT_EVENT_SOURCE_PANEL },
          { broadcast: true }
        );
        const status = normalizeRunStatusValue(obj?.status);
        const rid = Number(obj?.run_id);
        const tid = Number(obj?.task_id);
        if (status) streamRunStatus = status;
        if (Number.isFinite(rid) && rid > 0) streamRunId = rid;
        if (Number.isFinite(tid) && tid > 0) streamTaskId = tid;

        const state = worldSession.getState();
        const pending = state?.pendingResume;
        const shouldClearPending = !!(
          status
          && status !== "waiting"
          && Number.isFinite(rid)
          && rid > 0
          && Number(pending?.runId) === rid
        );

        worldSession.setState(
          (prev) => {
            const prevRun = prev?.currentRun && typeof prev.currentRun === "object" ? prev.currentRun : null;
            const prevRunId = Number(prevRun?.run_id);
            const prevTaskId = Number(prevRun?.task_id);
            const nextRunId = Number.isFinite(rid) && rid > 0
              ? rid
              : (Number.isFinite(prevRunId) && prevRunId > 0 ? prevRunId : null);
            const nextTaskId = Number.isFinite(tid) && tid > 0
              ? tid
              : (Number.isFinite(prevTaskId) && prevTaskId > 0 ? prevTaskId : null);
            const nextStatus = status || normalizeRunStatusValue(prevRun?.status) || "running";

            let nextCurrentRun = prevRun;
            if (nextRunId && (!Number.isFinite(prevRunId) || prevRunId <= 0 || prevRunId === nextRunId)) {
              nextCurrentRun = {
                ...(prevRun || {
                  task_title: "",
                  summary: null,
                  mode: null,
                  started_at: "",
                  finished_at: "",
                  created_at: "",
                  updated_at: "",
                  is_current: true
                }),
                run_id: nextRunId,
                task_id: nextTaskId || null,
                status: nextStatus
              };
            }

            const prevMeta = prev?.lastRunMeta && typeof prev.lastRunMeta === "object" ? prev.lastRunMeta : {};
            let nextLastRunMeta = prev.lastRunMeta;
            if (nextRunId) {
              nextLastRunMeta = {
                ...prevMeta,
                run_id: nextRunId,
                task_id: nextTaskId || null,
                status: nextStatus,
                updated_at: String(prevMeta.updated_at || "")
              };
            }

            return {
              ...prev,
              currentRun: nextCurrentRun,
              lastRunMeta: nextLastRunMeta
            };
          },
          { reason: "run_status_event" }
        );
        if (shouldClearPending) {
          clearWorldPendingResume({ runId: rid, reason: "run_status_continue" });
        } else if (status && status !== "waiting" && isTerminalRunStatus(status)) {
          clearWorldPendingResume({ reason: "run_status_terminal_fallback" });
        }
        if (pageWorldEl?.classList?.contains("is-visible")) {
          rerenderWorldThoughtsFromState();
        }
      },
      onNeedInput: (obj) => {
        if (!isStreamActive(worldStream, mySeq)) return;
        emitAgentEvent(
          { ...(obj || {}), type: "need_input", _source: AGENT_EVENT_SOURCE_PANEL },
          { broadcast: true }
        );
        const rid = Number(obj?.run_id);
        const tid = Number(obj?.task_id);
        if (Number.isFinite(rid) && rid > 0) streamRunId = rid;
        if (Number.isFinite(tid) && tid > 0) streamTaskId = tid;
        streamRunStatus = "waiting";
        const q = String(obj?.question || "").trim();
        const pending = {
          runId: rid,
          taskId: tid || null,
          question: q,
          kind: String(obj?.kind || "").trim() || null,
          choices: normalizeNeedInputChoices(obj?.choices),
          promptToken: String(obj?.prompt_token || obj?.promptToken || "").trim() || null,
          sessionKey: String(obj?.session_key || obj?.sessionKey || "").trim() || null
        };

        const state = worldSession.getState();
        const recentRecords = pruneNeedInputRecentRecords(state?.needInputRecentRecords, {
          ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS
        });
        if (shouldSuppressNeedInputPrompt(pending, recentRecords, { ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS })) {
          worldSession.setState(
            { needInputRecentRecords: recentRecords },
            { reason: "need_input_suppressed" }
          );
          return;
        }
        const currentPending = state?.pendingResume;
        const isSamePending = !!currentPending
          && Number(currentPending.runId) === Number(pending.runId)
          && Number(currentPending.taskId || 0) === Number(pending.taskId || 0)
          && String(currentPending.question || "") === String(pending.question || "")
          && String(currentPending.kind || "") === String(pending.kind || "")
          && String(currentPending.promptToken || "") === String(pending.promptToken || "")
          && String(currentPending.sessionKey || "") === String(pending.sessionKey || "")
          && JSON.stringify(Array.isArray(currentPending.choices) ? currentPending.choices : [])
            === JSON.stringify(Array.isArray(pending.choices) ? pending.choices : []);
        if (isSamePending) return;

        worldSession.setState(
          (prev) => {
            const prevRun = prev?.currentRun && typeof prev.currentRun === "object" ? prev.currentRun : null;
            const prevRunId = Number(prevRun?.run_id);
            const nextCurrentRun = Number.isFinite(rid) && rid > 0 && (!Number.isFinite(prevRunId) || prevRunId <= 0 || prevRunId === rid)
              ? { ...(prevRun || {}), run_id: rid, task_id: tid || null, status: "waiting" }
              : prevRun;
            return {
              ...prev,
              pendingResume: pending,
              needInputRecentRecords: recentRecords,
              currentRun: nextCurrentRun,
              lastRunMeta: {
                ...(prev?.lastRunMeta || {}),
                run_id: Number.isFinite(rid) && rid > 0 ? rid : (prev?.lastRunMeta?.run_id || null),
                task_id: tid || null,
                updated_at: String(prev?.lastRunMeta?.updated_at || ""),
                status: "waiting"
              }
            };
          },
          { reason: "need_input" }
        );
        renderWorldNeedInputChoicesUi(pending);
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
        if (obj?.type === "replay_applied") {
          const n = Number(obj?.applied);
          if (Number.isFinite(n) && n > 0) {
            worldSession.setState(
              { streamingStatus: `连接已恢复，已从事件日志补齐 ${n} 条事件。` },
              { reason: "stream_replay_applied" }
            );
            rerenderWorldThoughtsFromState();
          }
          return;
        }
        if (obj?.type === "memory_item") {
          emitAgentEvent(obj, { broadcast: false });
          return;
        }
        if (obj?.type === "agent_stage") {
          emitAgentEvent(obj, { broadcast: false });
        }
      },
      onReviewDelta: () => "", // 世界页不展开评估明细，避免刷屏
      replayFetch: enableAgentReplay
        ? (runId, afterEventId, signal) => api.fetchAgentRunEvents(
          runId,
          {
            after_event_id: afterEventId || undefined,
            limit: 200
          },
          signal
        )
        : undefined,
      getReplayRunId: enableAgentReplay
        ? () => {
          if (Number.isFinite(expectedRunId) && expectedRunId > 0) return expectedRunId;
          const ridFromStream = Number(streamRunId);
          return Number.isFinite(ridFromStream) && ridFromStream > 0 ? ridFromStream : null;
        }
        : undefined
    }
  );

  if (stopStream(worldStream, mySeq)) {
    worldSession.setState({ streaming: false, streamingMode: "", streamingStatus: "" }, { reason: "stream_stop" });
    updateWorldFromBackend(true);
    renderWorldNeedInputChoicesUi(worldSession.getState().pendingResume);
  }

  return { transcript, hadError, runId: streamRunId, taskId: streamTaskId, runStatus: streamRunStatus };
}

function extractVisibleResultText(transcript) {
  return extractVisibleResultTextFromTranscript(transcript || "");
}

async function buildWorldNoVisibleResultText(status, runId, fallbackLastError = null) {
  const rid = Number(runId);
  let normalizedStatus = normalizeRunStatusValue(status);
  let lastError = fallbackLastError || null;
  if (Number.isFinite(rid) && rid > 0 && (!lastError || !normalizedStatus)) {
    const detail = await api.fetchAgentRunDetail(rid).catch(() => null);
    if (!normalizedStatus) {
      normalizedStatus = normalizeRunStatusValue(detail?.run?.status);
    }
    if (!lastError) {
      lastError = extractRunLastError(detail);
    }
  }
  return buildNoVisibleResultText(normalizedStatus, {
    runId: Number.isFinite(rid) && rid > 0 ? rid : null,
    lastError
  });
}

function buildPendingFinalPlaceholderText(status) {
  const normalized = normalizeRunStatusValue(status);
  if (normalized === "failed") return "任务执行失败，评估中…";
  if (normalized === "stopped" || normalized === "cancelled") return "任务已中止，评估中…";
  return "任务执行完成，评估中…";
}

async function ensureWorldPendingResumeFromBackend(runId, fallbackTaskId = null) {
  const rid = Number(runId);
  if (!Number.isFinite(rid) || rid <= 0) return { waiting: false, question: "", taskId: null, status: "" };

  const detail = await api.fetchAgentRunDetail(rid).catch(() => null);
  const resolved = resolvePendingResumeFromRunDetail(rid, detail, {
    fallbackTaskId,
    normalizeChoices: normalizeNeedInputChoices
  });
  if (!resolved.waiting || !resolved.pending) {
    return { waiting: false, question: "", taskId: null, status: resolved.status };
  }

  const state = worldSession.getState();
  const current = state.pendingResume;
  const recentRecords = pruneNeedInputRecentRecords(state.needInputRecentRecords, {
    ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS
  });
  if (shouldSuppressNeedInputPrompt(resolved.pending, recentRecords, { ttlMs: WORLD_NEED_INPUT_RECENT_TTL_MS })) {
    worldSession.setState({ needInputRecentRecords: recentRecords }, { reason: "need_input_recover_suppressed" });
    clearWorldPendingResume({ runId: rid, reason: "need_input_recover_suppressed", emit: false });
    return { waiting: false, question: "", taskId: null, status: resolved.status };
  }

  const currentRunId = Number(current?.runId);
  const shouldRefreshPending = !Number.isFinite(currentRunId)
    || currentRunId !== rid
    || String(current?.question || "").trim() !== String(resolved.pending.question || "").trim()
    || String(current?.kind || "").trim() !== String(resolved.pending.kind || "").trim()
    || String(current?.promptToken || "").trim() !== String(resolved.pending.promptToken || "").trim()
    || String(current?.sessionKey || "").trim() !== String(resolved.pending.sessionKey || "").trim();
  if (shouldRefreshPending) {
    worldSession.setState(
      { pendingResume: resolved.pending, needInputRecentRecords: recentRecords },
      { reason: "need_input_recover" }
    );
    renderWorldNeedInputChoicesUi(resolved.pending);
  }

  return {
    waiting: true,
    question: resolved.question,
    taskId: resolved.taskId,
    status: resolved.status
  };
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
  const requestedMode = String(options.mode || "do").trim().toLowerCase();
  const agentMode = requestedMode === "think" ? "think" : "do";

  const resumeRunId = options.resumeRunId != null ? Number(options.resumeRunId) : null;
  const resumeTaskId = options.resumeTaskId != null ? Number(options.resumeTaskId) : null;
  const resumePromptToken = String(options.resumePromptToken || "").trim() || null;
  const resumeSessionKey = String(options.resumeSessionKey || "").trim() || null;
  const resumeKind = String(options.resumeKind || "").trim();
  const isResume = !!(resumeRunId && resumeRunId > 0);
  const isTaskFeedbackResume = isResume && resumeKind === "task_feedback";
  const currentRunMode = String(worldSession.getState()?.currentRun?.mode || "").trim().toLowerCase();
  const executionMode = isResume
    ? ((requestedMode === "think" || currentRunMode === "think") ? "think" : "do")
    : agentMode;
  let acquiredResumeLock = false;
  if (isResume) {
    if (worldResumeInFlight) return;
    worldResumeInFlight = true;
    acquiredResumeLock = true;
  }

  try {
    await createAndAppendChatMessage({
      role: "user",
      content: msg,
      task_id: isResume ? resumeTaskId : null,
      run_id: isResume ? resumeRunId : null,
      metadata: { source: "panel", mode: isResume ? "resume" : agentMode }
    });

    const assistantTemp = createTempWorldChatMessage(
      "assistant",
      UI_TEXT.PET_CHAT_SENDING || "…",
      isResume
        ? { run_id: resumeRunId, task_id: resumeTaskId }
        : { metadata: { source: "panel", mode: agentMode } }
    );

    const streamResult = await streamToWorld(
      (signal) => isResume
        ? api.streamAgentResume(
          {
            run_id: resumeRunId,
            message: msg,
            prompt_token: resumePromptToken || undefined,
            session_key: resumeSessionKey || undefined
          },
          signal
        )
        : api.streamAgentCommand({ message: msg, mode: agentMode }, signal),
      {
        mode: executionMode,
        displayMode: "status",
        assistantKey: assistantTemp._tmpKey,
        enableAgentReplay: true,
        expectedRunId: isResume ? resumeRunId : null
      }
    );
    const { transcript, hadError } = streamResult;
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

    const lastRunMeta = worldSession.getState().lastRunMeta;
    const streamRunId = Number(streamResult?.runId);
    const streamTaskId = Number(streamResult?.taskId);
    const runId = isResume
      ? resumeRunId
      : (Number.isFinite(streamRunId) && streamRunId > 0 ? streamRunId : (lastRunMeta?.run_id || null));
    const taskId = isResume
      ? resumeTaskId
      : (Number.isFinite(streamTaskId) && streamTaskId > 0 ? streamTaskId : (lastRunMeta?.task_id || null));
    let runStatus = normalizeRunStatusValue(streamResult?.runStatus);
    if (runStatus === "waiting") {
      await ensureWorldPendingResumeFromBackend(runId, taskId);
      return;
    }
    if (!runStatus) {
      const runState = await ensureWorldPendingResumeFromBackend(runId, taskId);
      if (runState.waiting) return;
      runStatus = normalizeRunStatusValue(runState.status);
    }
    if (!isTerminalRunStatus(runStatus)) return;

    // 与桌宠页保持一致：task_feedback 的 resume 只展示反馈闭环结果，
    // 不走“缺少可见结果”兜底，避免出现误导性提示。
    if (isTaskFeedbackResume) {
      const doneText = buildTaskFeedbackAckText(runStatus, runId);
      updateWorldChatMessageContent(assistantTemp._tmpKey, doneText);
      await saveAndCommitWorldAssistantMessage(assistantTemp._tmpKey, {
        role: "assistant",
        content: doneText,
        run_id: runId,
        task_id: taskId || null,
        metadata: { source: "panel", mode: "resume", kind: "task_feedback" }
      });
      return;
    }

    const visible = extractVisibleResultText(transcript);
    if (!visible) {
      // 兜底：本地 pendingResume 丢失但 run 实际仍在 waiting 时，恢复等待输入态并避免写入错误 fallback。
      const waitingRecovered = await ensureWorldPendingResumeFromBackend(runId, taskId);
      if (waitingRecovered.waiting) {
        const q = String(waitingRecovered.question || "").trim() || "需要你补充信息后才能继续执行。";
        updateWorldChatMessageContent(assistantTemp._tmpKey, q);
        await saveAndCommitWorldAssistantMessage(assistantTemp._tmpKey, {
          role: "assistant",
          content: q,
          run_id: runId,
          task_id: waitingRecovered.taskId || taskId || null,
          metadata: { source: "panel", mode: "need_input" }
        });
        return;
      }
      runStatus = normalizeRunStatusValue(waitingRecovered.status) || runStatus;
    }
    const statusFallback = runStatus || String(worldSession.getState().currentRun?.status || "").trim().toLowerCase();
    const finalVisible = visible || await buildWorldNoVisibleResultText(statusFallback, runId);

    // 关键体验：在评估 plan-list 完成前，不输出最终总结（避免用户看到“结果”但评估仍在跑）
    worldSession.setState(
      {
        pendingFinal: {
          tmpKey: assistantTemp._tmpKey,
          content: finalVisible,
          run_id: runId,
          task_id: taskId,
          metadata: { source: "panel", mode: isResume ? "resume" : agentMode },
          startedAt: Date.now()
        }
      },
      { reason: "pending_final_set" }
    );
    updateWorldChatMessageContent(assistantTemp._tmpKey, buildPendingFinalPlaceholderText(statusFallback));
    // 主动刷新一次，减少用户等待一个 polling interval 才看到“评估计划”的延迟
    updateWorldFromBackend(true).catch(() => {});
  } finally {
    if (acquiredResumeLock) {
      worldResumeInFlight = false;
    }
  }
}

async function submitWorldInput() {
  const text = String(worldInputEl?.value || "").trim();
  if (!text) return;
  if (worldInputEl) worldInputEl.value = "";

  // 等待输入态：自动 resume
  const pending = worldSession.getState().pendingResume;
  if (pending && pending.runId) {
    if (worldResumeInFlight) return;
    clearWorldPendingResume({ runId: Number(pending.runId), reason: "resume_send" });
    await runWorldDoMode(text, {
      resumeRunId: pending.runId,
      resumeTaskId: pending.taskId,
      resumePromptToken: pending.promptToken,
      resumeSessionKey: pending.sessionKey,
      resumeKind: pending.kind
    });
    return;
  }

  const cmd = parseSlashCommand(text);
  if (cmd?.cmd === "help") {
    const tip = "可用命令：/chat <内容>、/do <指令>、/think <指令>。不带 / 会让后端自动判断 chat/do/think。";
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
    await runWorldDoMode(msg, { mode: "do" });
    return;
  }

  if (cmd?.cmd === "think") {
    const msg = String(cmd.args || "").trim();
    if (!msg) return;
    await runWorldDoMode(msg, { mode: "think" });
    return;
  }

  // 默认：后端路由 chat/do/think
  let mode = "chat";
  try {
    const route = await api.routeAgentMode({ message: text });
    const m = String(route?.mode || "").trim().toLowerCase();
    if (m === "do" || m === "chat" || m === "think") mode = m;
  } catch (e) {}

  if (mode === "chat") {
    await runWorldChatMode(text);
    return;
  }
  await runWorldDoMode(text, { mode });
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
    const session = worldSession.getState();
    const keepExecutionStream = !!session.streaming && String(session.streamingMode || "") !== "chat";
    if (!keepExecutionStream) {
      // chat/空闲态仍按原逻辑中断，避免无意义连接占用。
      abortStream(worldStream);
      clearWorldPendingResume({ reason: "page_hide", emit: false });
      worldSession.setState(
        { streaming: false, streamingMode: "", pendingResume: null, streamingStatus: "" },
        { reason: "page_hide" }
      );
    }
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

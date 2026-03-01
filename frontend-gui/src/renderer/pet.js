// 桌宠主模块

// 通过 window.require 获取 Electron API，避免 ESM 导入失败导致脚本中断
const ipcRenderer = window?.require ? window.require("electron").ipcRenderer : null;
import { applyText } from "./ui.js";
import { PET_INPUT_HISTORY_LIMIT, PET_PLAN_HIDE_DELAY_MS, UI_POLL_INTERVAL_MS, UI_TEXT } from "./constants.js";
import * as api from "./api.js";
import {
  buildNoVisibleResultText,
  extractVisibleResultText,
  normalizeResultText,
  parseSlashCommand
} from "./agent_text.js";
import { createStore } from "./store.js";
import { buildPetChatContextMessages, writePetChatMessage } from "./pet_chat_store.js";
import { PetAnimator, PET_STATES } from "./pet-animator.js";
import { BUBBLE_TYPES, PetBubble } from "./pet-bubble.js";
import { streamSse } from "./streaming.js";
import { createStreamController, isStreamActive, startStream, stopStream, abortStream } from "./stream_controller.js";
import { PollManager } from "./poll_manager.js";
import { buildInputHistoryFromChatItems, createInputHistoryManager, mergeInputHistoryWithBackend } from "./input_history.js";
import { getPetDomRefs } from "./dom_refs.js";
import { AGENT_EVENT_NAME, emitAgentEvent, initAgentEventBridge } from "./agent_events.js";
import { bindPetImageFallback } from "./pet_image.js";
import { isTerminalRunStatus, normalizeRunStatusValue } from "./run_status.js";
import {
  buildRunSyncingHint,
  buildTaskFeedbackAckText,
  extractRunLastError,
  formatRunStatusDebugLabel
} from "./run_messages.js";
import { pickLatestTaskRunMeta } from "./run_fallback.js";
import {
  computePendingResumeTransition,
  markNeedInputPromptHandled,
  normalizeNeedInputChoices,
  renderNeedInputQuestionThenChoices,
  resolvePendingResumeFromRunDetail,
  shouldClearPendingResumeOnRunStatus,
} from "./need_input.js";

// 初始化
document.title = UI_TEXT.PET_TITLE;
applyText();
// 接收主进程转发的跨窗口结构化事件（panel <-> pet 双向同步）
initAgentEventBridge();

// 获取元素（集中在 dom_refs.js，减少散落的 querySelector）
const {
  petEl,
  imageEl,
  bubbleEl,
  bubbleContentEl,
  bubbleActionsEl,
  bubbleYesEl,
  bubbleNoEl,
  chatEl,
  chatInputEl,
  chatSendEl,
  planEl,
  planSlotEls
} = getPetDomRefs();

// 图片加载兜底：主图失效时自动回退到其他可用形态，避免桌宠整块消失。
bindPetImageFallback(imageEl);

// 说明：用户要求把规划栏/气泡/输入框放回同一个渲染窗口。
// 透明区域通过“像素级命中检测”继续保持穿透，避免错误遮挡鼠标点击。
const bubble = new PetBubble(bubbleEl);
const NEED_INPUT_RECENT_TTL_MS = 20000;
const AGENT_EVENT_SOURCE_PET = "pet";
// 桌宠关键会话状态：集中管理，避免散落的全局变量在并发/拖拽时漂移
const petSession = createStore({
  pendingResume: null, // {runId, taskId?, question, kind?, choices?, promptToken?, sessionKey?}
  needInputRecentRecords: [], // [{runId, fingerprint, at}]，用于抑制已处理提示的回放/轮询抖动
  lastRun: null, // {runId, taskId?}
  // P1：把关键状态从“顶层全局变量”收敛进 store，减少并发/拖拽/输入历史造成的隐式耦合
  streaming: false,
  inputHistory: [],
  historyCursor: null, // null 表示不在历史模式；否则是 [0..inputHistory.length]，len 表示草稿位
  historyDraft: "",
  taskFeedbackPending: false,
  suppressAutoCompletionBubble: false,
  lastStatus: null,
  debugPlanEnabled: false
});
const chatInputHistory = createInputHistoryManager({
  getState: petSession.getState,
  setState: petSession.setState,
  limit: PET_INPUT_HISTORY_LIMIT
});

let lastPetHistorySyncAt = 0;

async function refreshPetInputHistoryFromBackend(throttleMs = 3000) {
  const now = Date.now();
  const throttle = Number(throttleMs) > 0 ? Number(throttleMs) : 0;
  if (throttle && now - lastPetHistorySyncAt < throttle) return;
  lastPetHistorySyncAt = now;

  try {
    const resp = await api.fetchChatMessages({ limit: 80 });
    const items = Array.isArray(resp?.items) ? resp.items : [];
    const backendHistory = buildInputHistoryFromChatItems(items, PET_INPUT_HISTORY_LIMIT);
    if (!backendHistory.length) return;
    petSession.setState(
      (prev) => ({
        ...prev,
        inputHistory: mergeInputHistoryWithBackend(prev?.inputHistory, backendHistory, PET_INPUT_HISTORY_LIMIT),
        historyCursor: null,
        historyDraft: ""
      }),
      { reason: "input_history_bootstrap" }
    );
  } catch (e) {
    // 后端不可用：保留本地历史（避免影响桌宠交互）
  }
}

function setPendingAgentResume(payload) {
  const session = petSession.getState();
  const transition = computePendingResumeTransition({
    payload,
    currentPending: session.pendingResume,
    recentRecords: session.needInputRecentRecords,
    ttlMs: NEED_INPUT_RECENT_TTL_MS,
    normalizeChoices: normalizeNeedInputChoices,
    defaultQuestion: "",
    requireQuestion: false
  });
  if (!transition.valid || !transition.pending) {
    return { pending: null, changed: false };
  }
  if (transition.suppressed) {
    petSession.setState(
      { needInputRecentRecords: transition.recentRecords },
      { reason: "need_input_suppress_refresh" }
    );
    return {
      pending: session.pendingResume || null,
      changed: false,
      suppressed: true
    };
  }
  if (!transition.changed) return { pending: session.pendingResume, changed: false, suppressed: false };

  petSession.setState(
    {
      pendingResume: transition.pending,
      needInputRecentRecords: transition.recentRecords
    },
    { reason: "need_input" }
  );
  return { pending: transition.pending, changed: true, suppressed: false };
}

function markNeedInputHandled(payload) {
  petSession.setState(
    (prev) => ({
      ...prev,
      needInputRecentRecords: markNeedInputPromptHandled(
        payload,
        prev?.needInputRecentRecords,
        { ttlMs: NEED_INPUT_RECENT_TTL_MS }
      )
    }),
    { reason: "need_input_handled" }
  );
}

function emitNeedInputResolvedEvent(pending, reason = "resolved") {
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
      _source: AGENT_EVENT_SOURCE_PET
    },
    { broadcast: true }
  );
}

function hideNeedInputChoicesUi() {
  // 清理 need_input/pendingResume 时必须强制收敛全部按钮态（包含 task_feedback 的是/否按钮）。
  // 否则在跨窗口 resume 或 run_status 继续事件下，可能出现“pending 已清空但选择框残留”。
  resetTaskFeedbackUi();
}

function clearPendingAgentResume(options = {}) {
  const pending = petSession.getState().pendingResume;
  petSession.setState(
    (prev) => ({
      ...prev,
      pendingResume: null,
      needInputRecentRecords: markNeedInputPromptHandled(
        pending,
        prev?.needInputRecentRecords,
        { ttlMs: NEED_INPUT_RECENT_TTL_MS }
      )
    }),
    { reason: options?.reason || "clear_need_input" }
  );
  hideNeedInputChoicesUi();
  if (pending && options?.emit !== false) {
    emitNeedInputResolvedEvent(pending, options?.reason || "clear_need_input");
  }
  refreshHitAfterUiChange();
}

function clearPendingAgentResumeForRun(runId, options = {}) {
  const pending = petSession.getState().pendingResume;
  if (!pending) return;
  const targetRunId = Number(runId);
  if (Number.isFinite(targetRunId) && targetRunId > 0 && Number(pending.runId) !== targetRunId) return;
  clearPendingAgentResume(options);
}

function clearPetPendingOnRunStatus(status, runId, options = {}) {
  const pendingRunId = Number(petSession.getState().pendingResume?.runId);
  if (!shouldClearPendingResumeOnRunStatus(status, runId, pendingRunId)) return false;
  const rid = Number(runId);
  const continueReason = String(options?.continueReason || "run_status_continue");
  const terminalReason = String(options?.terminalReason || "run_status_terminal_fallback");
  const emit = options?.emit;
  if (Number.isFinite(rid) && rid > 0) {
    if (options?.forRun === true) {
      clearPendingAgentResumeForRun(rid, { reason: continueReason, emit });
    } else {
      clearPendingAgentResume({ reason: continueReason, emit });
    }
    return true;
  }
  clearPendingAgentResume({ reason: terminalReason, emit });
  return true;
}

function applyRemoteRunCreated(obj) {
  const runId = Number(obj?.run_id);
  const taskId = Number(obj?.task_id);
  if (!Number.isFinite(runId) || runId <= 0) return;
  setLastAgentRun({
    run_id: runId,
    task_id: Number.isFinite(taskId) && taskId > 0 ? taskId : null
  });
}

function applyRemoteRunStatus(obj) {
  const status = normalizeRunStatusValue(obj?.status);
  const runId = Number(obj?.run_id);
  const taskId = Number(obj?.task_id);
  if (Number.isFinite(runId) && runId > 0) {
    setLastAgentRun({
      run_id: runId,
      task_id: Number.isFinite(taskId) && taskId > 0 ? taskId : null
    });
  }
  clearPetPendingOnRunStatus(status, runId, {
    forRun: true,
    emit: false,
    continueReason: "run_status_remote_continue",
    terminalReason: "run_status_remote_terminal"
  });
}

function applyRemoteNeedInput(obj) {
  const applied = setPendingAgentResume(obj);
  if (applied?.suppressed) {
    const runId = Number(obj?.run_id);
    if (Number.isFinite(runId) && runId > 0) {
      clearPendingAgentResumeForRun(runId, { reason: "need_input_remote_suppressed", emit: false });
    }
    return;
  }
  if (!applied?.changed) return;

  setLastAgentRun(obj);
  const question = String(obj?.question || "").trim();
  if (question) {
    renderNeedInputQuestionThenChoices({
      question,
      payload: {
        ...obj,
        choices: applied?.pending?.choices || obj?.choices
      },
      renderQuestion: (text) => bubbleSet(text, BUBBLE_TYPES.INFO),
      renderChoices: (payload) => showNeedInputChoicesUi(payload),
    });
  } else {
    bubbleSet("需要你补充信息后才能继续执行。", BUBBLE_TYPES.WARNING);
  }
  showChat();
  schedulePetContentSizeReport();
}

try {
  window.addEventListener(
    AGENT_EVENT_NAME,
    (event) => {
      const obj = event?.detail;
      const source = String(obj?._source || "").trim();
      if (source === AGENT_EVENT_SOURCE_PET) return;
      if (!obj || typeof obj !== "object") return;
      if (obj.type === "run_created") {
        applyRemoteRunCreated(obj);
        return;
      }
      if (obj.type === "run_status") {
        applyRemoteRunStatus(obj);
        return;
      }
      if (obj.type === "need_input") {
        applyRemoteNeedInput(obj);
        return;
      }
      if (obj.type !== "need_input_resolved") return;
      const runId = Number(obj.run_id);
      if (!Number.isFinite(runId) || runId <= 0) return;
      markNeedInputHandled({
        runId,
        question: obj.question,
        kind: obj.kind,
        promptToken: obj.prompt_token,
        sessionKey: obj.session_key
      });
      clearPendingAgentResumeForRun(runId, { reason: "need_input_remote_resolved", emit: false });
    },
    { passive: true }
  );
} catch (e) {}

function setLastAgentRun(payload) {
  const runId = Number(payload?.run_id);
  if (!Number.isFinite(runId) || runId <= 0) return;
  petSession.setState(
    { lastRun: { runId, taskId: Number(payload?.task_id) || null } },
    { reason: "run_created" }
  );
}

function resetTaskFeedbackUi() {
  if (petSession.getState().taskFeedbackPending) {
    petSession.setState({ taskFeedbackPending: false }, { reason: "task_feedback_reset" });
  }
  if (bubbleActionsEl) {
    const nodes = bubbleActionsEl.querySelectorAll(".pet-bubble-action-dynamic");
    for (const el of Array.from(nodes)) {
      try { el.remove(); } catch (e) {}
    }
  }
  if (bubbleActionsEl) bubbleActionsEl.classList.add("is-hidden");
  if (bubbleYesEl) bubbleYesEl.classList.add("is-hidden");
  if (bubbleNoEl) bubbleNoEl.classList.add("is-hidden");
  if (bubbleEl) bubbleEl.classList.remove("has-actions");
  if (bubbleYesEl) bubbleYesEl.onclick = null;
  if (bubbleNoEl) bubbleNoEl.onclick = null;
}

function bubbleSet(message, type = BUBBLE_TYPES.INFO) {
  resetTaskFeedbackUi();
  bubble.set(String(message || ""), type);
}

function bubbleShow(message, type = BUBBLE_TYPES.INFO, duration = 0) {
  resetTaskFeedbackUi();
  // duration<=0 表示常驻：使用 set 覆盖（避免队列堆积）
  if (Number(duration) > 0) {
    bubble.show(String(message || ""), type, Number(duration));
    return;
  }
  bubble.set(String(message || ""), type);
}

function bubbleClear() {
  resetTaskFeedbackUi();
  bubble.clear();
}

function showTaskFeedbackUi() {
  if (!bubbleActionsEl || !bubbleYesEl || !bubbleNoEl) return;
  petSession.setState({ taskFeedbackPending: true }, { reason: "task_feedback_show" });
  if (bubbleEl) bubbleEl.classList.add("has-actions");
  bubbleActionsEl.classList.remove("is-hidden");
  bubbleYesEl.classList.remove("is-hidden");
  bubbleNoEl.classList.remove("is-hidden");

  bubbleYesEl.onclick = async () => {
    if (resumeInFlight) return;
    // 若后端正在 waiting（need_input），则“是”会触发一次 resume，用于真正结束 run
    const pending = petSession.getState().pendingResume;
    if (pending?.runId) {
      resetTaskFeedbackUi();
      bubbleSet("收到：满意。正在确认并结束任务…", BUBBLE_TYPES.INFO);
      try {
        await handleResumeMode("是");
      } catch (e) {
        bubbleSet("确认失败：请稍后重试或在输入框回复“是”。", BUBBLE_TYPES.ERROR);
      }
      refreshHitAfterUiChange();
      return;
    }

    const fallbackRunId = Number(petSession.getState().lastRun?.runId);
    if (Number.isFinite(fallbackRunId) && fallbackRunId > 0) {
      const recovered = await ensurePendingResumeFromBackend(fallbackRunId);
      if (recovered.waiting) {
        refreshHitAfterUiChange();
        return;
      }
      resetTaskFeedbackUi();
      bubbleSet(
        `无法确认满意度：run#${fallbackRunId} 当前状态为${formatRunStatusDebugLabel(recovered.status)}。请等待进入确认阶段后再提交。`,
        BUBBLE_TYPES.WARNING
      );
      refreshHitAfterUiChange();
      return;
    }

    resetTaskFeedbackUi();
    bubbleSet("无法确认满意度：当前没有处于等待确认的任务。", BUBBLE_TYPES.WARNING);
    refreshHitAfterUiChange();
  };

  bubbleNoEl.onclick = () => {
    resetTaskFeedbackUi();
    // 仅预填，不自动发送；若用户已有草稿则不覆盖，只做前缀补充
    const prefix = "这些方面与我预期不符：";
    showChat();
    if (chatInputEl) {
      const old = String(chatInputEl.value || "");
      const next = old.trim() ? (old.includes(prefix) ? old : `${prefix}\n${old}`) : prefix;
      chatInputEl.value = next;
      try {
        chatInputEl.focus();
        chatInputEl.setSelectionRange(chatInputEl.value.length, chatInputEl.value.length);
      } catch (e) {}
    }
    refreshHitAfterUiChange();
  };
  refreshHitAfterUiChange();
}

function showNeedInputChoicesUi(payload) {
  if (!bubbleActionsEl) return;

  const kind = String(payload?.kind || "").trim();
  if (kind === "task_feedback") {
    showTaskFeedbackUi();
    return;
  }

  const choices = normalizeNeedInputChoices(payload?.choices);

  // 从 task_feedback 切到普通 need_input 时，先收敛反馈态，避免残留按钮/点击回调叠在新选项上。
  if (petSession.getState().taskFeedbackPending) {
    petSession.setState({ taskFeedbackPending: false }, { reason: "task_feedback_leave" });
  }
  if (bubbleYesEl) bubbleYesEl.onclick = null;
  if (bubbleNoEl) bubbleNoEl.onclick = null;

  // 清空旧的动态按钮
  const nodes = bubbleActionsEl.querySelectorAll(".pet-bubble-action-dynamic");
  for (const el of Array.from(nodes)) {
    try { el.remove(); } catch (e) {}
  }

  bubbleActionsEl.classList.remove("is-hidden");
  if (bubbleEl) bubbleEl.classList.add("has-actions");
  if (bubbleYesEl) bubbleYesEl.classList.add("is-hidden");
  if (bubbleNoEl) bubbleNoEl.classList.add("is-hidden");

  function focusCustomInput() {
    showChat();
    if (chatInputEl) {
      try {
        chatInputEl.focus();
        chatInputEl.setSelectionRange(chatInputEl.value.length, chatInputEl.value.length);
      } catch (e) {}
    }
  }

  let choiceSubmitting = false;
  for (const c of choices) {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "pet-bubble-action pet-bubble-action-dynamic";
    btn.textContent = String(c?.label || "").trim() || "选项";
    btn.onclick = async () => {
      if (choiceSubmitting || resumeInFlight) return;
      const pending = petSession.getState().pendingResume;
      if (!pending?.runId) return;
      choiceSubmitting = true;
      // 禁用所有动态按钮，避免重复提交
      for (const node of Array.from(bubbleActionsEl.querySelectorAll(".pet-bubble-action-dynamic"))) {
        try { node.disabled = true; } catch (e) {}
      }
      bubbleSet(`收到：${btn.textContent}。正在继续执行…`, BUBBLE_TYPES.INFO);
      try {
        await handleResumeMode(String(c?.value || "").trim());
      } catch (e) {
        bubbleSet("继续执行失败：请稍后重试或在输入框手动回复。", BUBBLE_TYPES.ERROR);
        focusCustomInput();
      } finally {
        choiceSubmitting = false;
        refreshHitAfterUiChange();
      }
    };
    bubbleActionsEl.appendChild(btn);
  }

  // “自定义输入”：只聚焦输入框，不自动发送（符合用户期望）
  const customBtn = document.createElement("button");
  customBtn.type = "button";
  customBtn.className = "pet-bubble-action pet-bubble-action-dynamic";
  customBtn.textContent = "自定义输入";
  customBtn.onclick = () => {
    focusCustomInput();
    refreshHitAfterUiChange();
  };
  bubbleActionsEl.appendChild(customBtn);

  refreshHitAfterUiChange();
}

let planHideTimer = null;
let lastPlanEventAt = 0;
let lastPolledPlanRunId = null;
let lastPolledPlanSignature = "";
let currentPlanItems = [];
function setPlanSlot(index, text, status) {
  const el = planSlotEls[index];
  if (!el) return;
  el.textContent = String(text || "").trim();
  if (status) {
    el.dataset.status = status;
  } else {
    delete el.dataset.status;
  }
}

function hidePlan() {
  if (!planEl) return;
  planEl.classList.add("is-hidden");
  setPlanSlot(0, "", "");
  setPlanSlot(1, "", "");
  setPlanSlot(2, "", "");
  setPlanSlot(3, "", "");
  currentPlanItems = [];
}

function renderPlan(items) {
  const list = Array.isArray(items) ? items : [];
  if (!list.length) {
    hidePlan();
    return;
  }
  planEl?.classList.remove("is-hidden");

  const normalized = list.map((raw) => {
    const it = raw && typeof raw === "object" ? raw : {};
    const brief = String(it.brief || it.title || "").trim();
    let status = String(it.status || "pending").trim().toLowerCase();
    // 兼容：后端/历史 run 可能会用 planned/queued 等状态名，桌宠侧统一按 pending 渲染
    if (status === "planned" || status === "queued") status = "pending";
    // waiting（等待用户输入）在 UI 上按 running 展示，避免焦点丢失/样式缺失
    if (status === "waiting") status = "running";
    return { brief, status };
  });

  // 计划栏来源与世界页一致：items 全量由后端提供；桌宠仅“挑 4 条展示”。
  // 注意：failed 不等于“当前正在执行”。执行器允许“失败继续”，因此 focus 必须优先选择 running/waiting，
  // 否则会被早期的 failed 步骤“吸走焦点”，与世界页不一致。
  const runningIdx = normalized.findIndex((it) => it.status === "running");
  const firstPendingIdx = normalized.findIndex((it) => it.status === "pending");
  const focusIdx = runningIdx !== -1 ? runningIdx : firstPendingIdx !== -1 ? firstPendingIdx : Math.max(0, normalized.length - 1);

  let doneItem = null;
  for (let i = focusIdx - 1; i >= 0; i--) {
    if (normalized[i].status === "done") {
      doneItem = normalized[i];
      break;
    }
  }
  if (!doneItem) {
    for (let i = normalized.length - 1; i >= 0; i--) {
      if (normalized[i].status === "done") {
        doneItem = normalized[i];
        break;
      }
    }
  }

  const currentItem = normalized[focusIdx] || null;
  const nextItems = normalized.slice(focusIdx + 1).filter((it) => it.status === "pending");

  setPlanSlot(0, doneItem?.brief || "", doneItem ? "done" : "");
  setPlanSlot(1, currentItem?.brief || "", currentItem ? (currentItem.status || "pending") : "");
  setPlanSlot(2, nextItems[0]?.brief || "", nextItems[0] ? "pending" : "");
  setPlanSlot(3, nextItems[1]?.brief || "", nextItems[1] ? "pending" : "");
}

function planUpdate(payload) {
  lastPlanEventAt = Date.now();
  const items = Array.isArray(payload?.items) ? payload.items : [];
  currentPlanItems = items.map((it) => (it && typeof it === "object" ? { ...it } : {}));
  renderPlan(currentPlanItems);
}

function planApplyDelta(payload) {
  lastPlanEventAt = Date.now();
  const changes = Array.isArray(payload?.changes) ? payload.changes : [];
  if (!changes.length) return;
  if (!Array.isArray(currentPlanItems) || !currentPlanItems.length) return;

  for (const raw of changes) {
    const ch = raw && typeof raw === "object" ? raw : {};
    const id = Number(ch.id);
    const stepOrder = Number(ch.step_order);

    let idx = -1;
    if (Number.isFinite(id) && id > 0) {
      idx = currentPlanItems.findIndex((it) => Number(it?.id) === id);
    }
    if (idx === -1 && Number.isFinite(stepOrder) && stepOrder > 0) {
      idx = stepOrder - 1;
    }
    if (idx < 0 || idx >= currentPlanItems.length) continue;

    const orig = currentPlanItems[idx] && typeof currentPlanItems[idx] === "object" ? currentPlanItems[idx] : {};
    const base = { ...orig };
    if (ch.status != null) base.status = ch.status;
    if (ch.brief != null) base.brief = ch.brief;
    if (ch.title != null) base.title = ch.title;
    if (!base.id && (Number.isFinite(id) && id > 0)) base.id = id;
    currentPlanItems[idx] = base;
  }

  renderPlan(currentPlanItems);
}

function scheduleHidePlan(delayMs = PET_PLAN_HIDE_DELAY_MS) {
  if (planHideTimer) clearTimeout(planHideTimer);
  planHideTimer = setTimeout(() => hidePlan(), delayMs);
}

// 固定桌宠宽度，避免尺寸随资源变化
if (imageEl) {
  imageEl.style.width = "150px";
  imageEl.style.height = "auto";
  // 禁用浏览器默认拖拽（会触发“禁止符号”并抢占拖拽逻辑）
  imageEl.addEventListener("dragstart", (event) => event.preventDefault());
}

// 初始化动画器和气泡
// 动画尽量作用在图片本身，避免输入框出现时整体容器移动
const animator = new PetAnimator(imageEl || petEl);

// 桌宠对话上下文（保留少量轮次即可）
const MAX_CHAT_MESSAGES = 12;
const chatStream = createStreamController();
let resumeInFlight = false;
function setChatStreaming(next) {
  petSession.setState({ streaming: !!next }, { reason: next ? "stream_start" : "stream_end" });
}

// 桌宠窗口保持固定尺寸：不做内容自适应缩放，避免对话框/气泡导致桌宠位置抖动
function schedulePetContentSizeReport() {}

// 像素级命中检测：按图像 alpha 判断是否可交互
const mask = {
  ready: false,
  width: 0,
  height: 0,
  data: null
};

// 拖拽移动：仅在按下雷姆像素区域且移动超过阈值时进入拖拽
// 该阈值用于避免“单击”时轻微抖动被误判为拖拽，从而导致窗口位置变化
const DRAG_START_DISTANCE = 6;
let pressState = null;
let isDragging = false;
let suppressClickOnce = false;

// 禁用页面级拖拽（会触发 Windows “禁止符号”并打断 pointer 事件链）
window.addEventListener(
  "dragstart",
  (event) => event.preventDefault(),
  { capture: true }
);

function buildMask(img) {
  if (!img || !img.naturalWidth || !img.naturalHeight) return;
  const canvas = document.createElement("canvas");
  canvas.width = img.naturalWidth;
  canvas.height = img.naturalHeight;
  const ctx = canvas.getContext("2d");
  ctx.drawImage(img, 0, 0);
  const imageData = ctx.getImageData(0, 0, canvas.width, canvas.height);
  mask.ready = true;
  mask.width = canvas.width;
  mask.height = canvas.height;
  mask.data = imageData.data;
  schedulePetContentSizeReport();
}

function isHitOnPetImage(clientX, clientY) {
  if (!imageEl) return false;
  const rect = imageEl.getBoundingClientRect();
  if (clientX < rect.left || clientX > rect.right || clientY < rect.top || clientY > rect.bottom) {
    return false;
  }
  if (!mask.ready) {
    // 兜底：遮罩尚未构建时允许按矩形命中，避免桌宠在资源加载失败/延迟时完全不可点击。
    return true;
  }
  const x = Math.floor((clientX - rect.left) / rect.width * mask.width);
  const y = Math.floor((clientY - rect.top) / rect.height * mask.height);
  if (x < 0 || y < 0 || x >= mask.width || y >= mask.height) return false;
  const alpha = mask.data[(y * mask.width + x) * 4 + 3];
  return alpha > 10;
}

function isElementVisible(el) {
  if (!el) return false;
  if (el.classList.contains("is-hidden")) return false;
  const rect = el.getBoundingClientRect();
  return rect.width > 0 && rect.height > 0;
}

function isHitOnElement(el, clientX, clientY) {
  if (!el) return false;
  const rect = el.getBoundingClientRect();
  return (
    clientX >= rect.left
    && clientX <= rect.right
    && clientY >= rect.top
    && clientY <= rect.bottom
  );
}

function isHitInteractive(clientX, clientY) {
  // 交互区域 = 雷姆像素区域 +（可见）气泡 +（可见）输入框
  // 计划栏为纯展示（pointer-events:none），不计入命中，避免遮挡点击。
  if (isElementVisible(chatEl) && isHitOnElement(chatEl, clientX, clientY)) return true;
  if (isElementVisible(bubbleEl) && isHitOnElement(bubbleEl, clientX, clientY)) return true;
  return isHitOnPetImage(clientX, clientY);
}

let hitTestPending = false;
let lastHit = null;
let latestMouseEvent = null;

function sendHitResult(hit) {
  if (hit === lastHit) return;
  lastHit = hit;
  ipcRenderer?.send("pet-hit-test-result", hit);
}

function refreshHitAfterUiChange() {
  // 按住/拖拽期间禁止切回穿透，否则会导致 pointerup 丢失
  if (pressState || isDragging) return;
  if (latestMouseEvent) {
    const hit = isHitInteractive(latestMouseEvent.clientX, latestMouseEvent.clientY);
    sendHitResult(hit);
    return;
  }
  sendHitResult(false);
}

function handleHitTest(event) {
  latestMouseEvent = event;
  // 按住/拖拽期间锁定命中为 true，避免主进程切回穿透导致事件链断裂
  if (pressState || isDragging) return;
  if (hitTestPending) return;
  hitTestPending = true;
  requestAnimationFrame(() => {
    hitTestPending = false;
    if (pressState || isDragging) return;
    if (!latestMouseEvent) return;
    const hit = isHitInteractive(latestMouseEvent.clientX, latestMouseEvent.clientY);
    sendHitResult(hit);
  });
}

window.addEventListener("mousemove", handleHitTest);
window.addEventListener("mouseleave", () => {
  if (pressState || isDragging) return;
  sendHitResult(false);
});
window.addEventListener("blur", () => {
  lastHit = null;
  sendHitResult(false);
});

ipcRenderer?.on("pet-hit-test-point", (event, payload) => {
  if (!payload?.cursor || !payload?.bounds) return;
  // 按住/拖拽期间禁止切回穿透，否则会导致 pointerup 丢失，出现“粘住/遮挡”问题
  if (pressState || isDragging) return;
  const clientX = payload.cursor.x - payload.bounds.x;
  const clientY = payload.cursor.y - payload.bounds.y;
  const hit = isHitInteractive(clientX, clientY);
  sendHitResult(hit);
});

if (imageEl) {
  if (imageEl.complete) {
    buildMask(imageEl);
  } else {
    imageEl.addEventListener("load", () => buildMask(imageEl), { once: true });
  }
}

// 当气泡自动隐藏时刷新命中检测，避免出现“气泡没了但窗口仍拦截点击”的问题
bubble.onHide = refreshHitAfterUiChange;

// 状态轮询间隔（毫秒）
const POLL_INTERVAL = UI_POLL_INTERVAL_MS;
const pollManager = new PollManager();
async function updateAgentPlanFromBackend() {
  const session = petSession.getState();
  const now = Date.now();
  const recentPlanEvent = lastPlanEventAt && now - lastPlanEventAt < Math.max(800, Math.floor(POLL_INTERVAL / 2));

  // 若当前正在走桌宠发起的 SSE 且 plan 事件持续到达，则不额外轮询，避免“倒退闪回”。
  if (session.streaming && recentPlanEvent) return;

  const current = await api.fetchCurrentAgentRun().catch(() => null);
  const run = current?.run || null;
  const isCurrent = !!run?.is_current;
  const runId = Number(run?.run_id);
  if (!isCurrent || !Number.isFinite(runId) || runId <= 0) {
    // 非当前 run：仅在“非调试模式且桌宠未在流式”时收起规划栏
    if (!session.debugPlanEnabled && !session.streaming) hidePlan();
    // 清理可能残留的等待输入状态（避免用户看到旧问题但无法 resume）
    // 注意：流式过程中由 SSE 自己维护 need_input，不应被轮询覆盖。
    if (!session.streaming && session.pendingResume) clearPendingAgentResume({ reason: "run_not_current" });
    lastPolledPlanRunId = null;
    lastPolledPlanSignature = "";
    return;
  }

  // run 变化：重置签名，确保首次必定更新
  if (lastPolledPlanRunId !== runId) {
    lastPolledPlanRunId = runId;
    lastPolledPlanSignature = "";
  }

  const detail = await api.fetchAgentRunDetail(runId).catch(() => null);
  const items = detail?.agent_plan?.items;
  if (Array.isArray(items) && items.length) {
    let sig = "";
    try {
      sig = JSON.stringify(items);
    } catch (e) {
      sig = String(items.length);
    }
    if (sig !== lastPolledPlanSignature) {
      lastPolledPlanSignature = sig;
      planUpdate({ task_id: detail?.run?.task_id, items });
    }
  }

  // waiting -> 允许桌宠接管交互（即便该 run 是从世界页发起）
  // 注意：流式过程中由 SSE 驱动 need_input，不要在这里抢占/覆盖 pendingResume。
  if (session.streaming) return;
  const status = String(detail?.run?.status || run?.status || "").trim().toLowerCase();
  const paused = detail?.agent_state?.paused;
  const question = String(paused?.question || "").trim();
  if (status === "waiting" && question) {
    const payload = {
      run_id: runId,
      task_id: detail?.run?.task_id,
      question,
      kind: paused?.kind,
      choices: paused?.choices,
      prompt_token: paused?.prompt_token,
      session_key: paused?.session_key || detail?.agent_state?.session_key
    };
    const applied = setPendingAgentResume(payload);
    if (applied.changed) {
      bubbleSet(question, BUBBLE_TYPES.INFO);
      showNeedInputChoicesUi({
        ...payload,
        choices: applied?.pending?.choices
      });
      showChat();
    }
  } else {
    const pending = session.pendingResume;
    if (pending && Number(pending.runId) === runId) {
      clearPendingAgentResume({ reason: "run_status_poll_continue" });
    }
  }
}

/**
 * 根据后端状态更新桌宠
 */
async function updateFromBackend() {
  try {
    const [health, summary, currentRunResp] = await Promise.all([
      api.fetchHealth(),
      api.fetchTasksSummary(),
      api.fetchCurrentAgentRun().catch(() => null)
    ]);

    const session = petSession.getState();
    const lastStatus = session.lastStatus;
    const isStreaming = !!session.streaming;
    const taskFeedbackPending = !!session.taskFeedbackPending;
    const suppressAutoCompletionBubble = !!session.suppressAutoCompletionBubble;

    const backendOk = health.status === "ok";
    const hasRunningTask = summary.current && summary.current !== UI_TEXT.NONE;
    const hasPendingResume = !!session.pendingResume;
    const latestRun = currentRunResp?.run || null;
    const latestRunStatus = normalizeRunStatusValue(latestRun?.status);
    const latestRunId = Number(latestRun?.run_id);
    const runSuffix = Number.isFinite(latestRunId) && latestRunId > 0 ? `（run#${latestRunId}）` : "";

    // 确定新状态
    let newStatus;
    if (!backendOk) {
      newStatus = "error";
    } else if (hasRunningTask) {
      newStatus = "working";
    } else {
      newStatus = "idle";
    }

    // 状态变化时更新动画和气泡
    if (newStatus !== lastStatus) {
      switch (newStatus) {
        case "working":
          animator.setState(PET_STATES.WORKING);
          // 新任务开始：允许后续由轮询补充“完成”提示（若用户未开流式对话）
          if (session.suppressAutoCompletionBubble) {
            petSession.setState({ suppressAutoCompletionBubble: false }, { reason: "task_started" });
          }
          if (!isStreaming && !hasPendingResume) {
            bubbleShow(`正在执行: ${summary.current}`, BUBBLE_TYPES.INFO, 0);
          }
          break;
        case "error":
          animator.setState(PET_STATES.ERROR);
          if (!isStreaming && !hasPendingResume) {
            bubbleShow("后端连接异常", BUBBLE_TYPES.ERROR, 0);
          }
          break;
        case "idle":
          animator.setState(PET_STATES.IDLE);
          if (lastStatus === "working") {
            if (!isStreaming && !taskFeedbackPending && !suppressAutoCompletionBubble && !hasPendingResume) {
              if (latestRunStatus === "failed") {
                animator.playAnimation(PET_STATES.ERROR);
                bubbleShow(`任务失败${runSuffix}：请查看执行日志并修复后重试。`, BUBBLE_TYPES.ERROR, 0);
              } else if (latestRunStatus === "stopped") {
                bubbleShow(`任务已停止${runSuffix}。`, BUBBLE_TYPES.WARNING, 0);
              } else if (latestRunStatus === "done") {
                animator.playAnimation(PET_STATES.SUCCESS);
                bubbleShow(`任务完成${runSuffix}!`, BUBBLE_TYPES.SUCCESS, 0);
              } else {
                bubbleShow(
                  `任务已结束${runSuffix}${latestRunStatus ? `（${formatRunStatusDebugLabel(latestRunStatus)}）` : ""}。`,
                  BUBBLE_TYPES.INFO,
                  0
                );
              }
            }
            // 无论是否展示“任务完成”，到 idle 后都清掉一次性抑制标记
            if (session.suppressAutoCompletionBubble) {
              petSession.setState({ suppressAutoCompletionBubble: false }, { reason: "idle_clear_completion_suppress" });
            }
          }
          break;
      }
      petSession.setState({ lastStatus: newStatus }, { reason: "pet_status_change" });
    }
  } catch (error) {
    const session = petSession.getState();
    if (session.lastStatus !== "error") {
      animator.setState(PET_STATES.ERROR);
      if (!session.streaming) {
        bubbleShow("无法连接后端", BUBBLE_TYPES.ERROR, 0);
      }
      petSession.setState({ lastStatus: "error" }, { reason: "pet_status_error" });
    }
  }
}

/**
 * 开始状态轮询
 */
function startPolling() {
  pollManager.start("pet_status", updateFromBackend, POLL_INTERVAL, { runImmediately: true });
  pollManager.start("pet_agent_plan", updateAgentPlanFromBackend, POLL_INTERVAL, { runImmediately: true });
}

/**
 * 停止状态轮询
 */
function stopPolling() {
  pollManager.stop("pet_status");
  pollManager.stop("pet_agent_plan");
}

function onPointerDown(event) {
  if (event.button !== 0) return;
  // 仅允许在雷姆图像像素区域按下触发拖拽（输入框区域不触发拖拽）
  if (!isHitOnPetImage(event.clientX, event.clientY)) return;
  event.preventDefault();
  pressState = {
    pointerId: event.pointerId,
    pointerType: event.pointerType,
    startScreenX: event.screenX,
    startScreenY: event.screenY
  };
  isDragging = false;
  petEl?.setPointerCapture?.(event.pointerId);
  sendHitResult(true);
}

function onPointerMove(event) {
  if (!pressState) return;
  if (pressState.pointerId != null && event.pointerId !== pressState.pointerId) return;
  if (isDragging) return;
  if (event.pointerType !== pressState.pointerType) return;
  const dx = event.screenX - pressState.startScreenX;
  const dy = event.screenY - pressState.startScreenY;
  if (Math.hypot(dx, dy) < DRAG_START_DISTANCE) return;
  isDragging = true;
  suppressClickOnce = true;
  petEl?.classList.add("is-dragging");
  ipcRenderer?.send("pet-drag-start");
}

function clearDragState() {
  pressState = null;
  isDragging = false;
  petEl?.classList.remove("is-dragging");
}

function onPointerUp(event) {
  if (!pressState) return;
  if (pressState.pointerId != null && event.pointerId !== pressState.pointerId) return;
  if (event.pointerType !== pressState.pointerType) return;
  if (isDragging) {
    ipcRenderer?.send("pet-drag-end");
    // 拖拽结束后强制恢复穿透，避免停在雷姆像素上导致透明区域遮挡
    sendHitResult(false);
    clearDragState();
    return;
  }
  const hit = isHitOnPetImage(event.clientX, event.clientY);
  sendHitResult(hit);
  clearDragState();
}

function cancelDrag() {
  if (!pressState) return;
  if (isDragging) {
    ipcRenderer?.send("pet-drag-end");
  }
  sendHitResult(false);
  clearDragState();
}

petEl?.addEventListener("pointerdown", onPointerDown);
petEl?.addEventListener("lostpointercapture", cancelDrag);
window.addEventListener("pointermove", onPointerMove);
window.addEventListener("pointerup", onPointerUp);
window.addEventListener("pointercancel", cancelDrag);
window.addEventListener("blur", cancelDrag);

function isChatVisible() {
  return !!chatEl && !chatEl.classList.contains("is-hidden");
}

function showChat() {
  if (!chatEl) return;
  // 输入历史：从后端聊天记录抽取 user 输入，确保桌宠/世界页共享上下键历史
  refreshPetInputHistoryFromBackend(1500).catch(() => {});
  chatEl.classList.remove("is-hidden");
  // 输入框出现后立即允许交互（避免命中检测轮询带来的“点不到/延迟”体感）
  sendHitResult(true);
  requestAnimationFrame(() => chatInputEl?.focus?.());
}

function hideChat() {
  if (!chatEl) return;
  chatEl.classList.add("is-hidden");
  if (document.activeElement === chatInputEl) {
    try { chatInputEl?.blur?.(); } catch (e) {}
  }
  // 让命中检测重新接管穿透状态
  lastHit = null;
  sendHitResult(false);
}

function toggleChat() {
  if (isChatVisible()) {
    hideChat();
    return;
  }
  showChat();
}

// 单击显示/隐藏对话框
petEl?.addEventListener("click", async (event) => {
  // 拖拽结束会触发 click，这里拦截一次避免误触
  if (suppressClickOnce) {
    suppressClickOnce = false;
    return;
  }
  // 防止拖拽时触发
  if (petEl.classList.contains("is-dragging")) return;
  // 穿透模式下仍会收到 click（forward: true），这里仅允许点击雷姆像素区域触发交互
  if (!isHitOnPetImage(event.clientX, event.clientY)) return;
  toggleChat();
});

function petHelpText() {
  return [
    "桌宠命令：",
    "/help - 显示帮助",
    "/chat <内容> - 纯聊天（仅调用 LLM）",
    "/do <指令> - 指令执行（生成任务 steps 并执行）",
    "/think <指令> - 深度编排执行（多模型协作规划）",
    "/eval [run_id] [补充说明] - 评估 Agent（默认评估最近一次 /do 的 run）",
    "/task <标题> - 创建任务",
    "/run <任务ID> - 流式执行任务 steps",
    "/memory <内容> - 写入记忆",
    "/search <关键词> - 统一检索",
    "/panel - 打开主面板",
    "/debugplan - 切换“规划栏示例”调试模式（用于调位置/样式）",
    "",
    "不带 / 默认让 LLM 自动判断：普通对话走 /chat；执行类任务走 /do 或 /think。",
    "想强制指定：用 /chat、/do 或 /think。"
  ].join("\n");
}

function formatReviewToText(review) {
  const status = String(review?.status || "").trim();
  const summary = String(review?.summary || "").trim();
  const issues = Array.isArray(review?.issues) ? review.issues : [];
  const nextActions = Array.isArray(review?.next_actions) ? review.next_actions : [];
  const skills = Array.isArray(review?.skills) ? review.skills : [];

  const lines = [];
  lines.push("【评估】");
  if (status) lines.push(`状态：${status}`);
  if (summary) lines.push(summary);

  if (issues.length) {
    lines.push("");
    lines.push("问题：");
    for (const raw of issues.slice(0, 5)) {
      const it = raw && typeof raw === "object" ? raw : {};
      const sev = String(it.severity || "").trim();
      const title = String(it.title || "").trim();
      if (!title) continue;
      lines.push(`- ${sev ? `[${sev}] ` : ""}${title}`);
    }
  }

  if (nextActions.length) {
    lines.push("");
    lines.push("建议：");
    for (const raw of nextActions.slice(0, 5)) {
      const it = raw && typeof raw === "object" ? raw : {};
      const title = String(it.title || "").trim();
      if (!title) continue;
      lines.push(`- ${title}`);
    }
  }

  if (skills.length) {
    lines.push("");
    lines.push("技能沉淀：");
    for (const raw of skills.slice(0, 5)) {
      const it = raw && typeof raw === "object" ? raw : {};
      const name = String(it.name || "").trim();
      const st = String(it.status || "").trim();
      const source = String(it.source_path || "").trim();
      const label = name || source || String(it.skill_id || "").trim();
      if (!label) continue;
      lines.push(`- ${label}${st ? ` (${st})` : ""}`);
    }
  }

  return lines.join("\n").trim();
}

const DEBUG_PLAN_STORAGE_KEY = "LSA_PET_DEBUG_PLAN";

function setDebugPlanEnabled(next) {
  const debugPlanEnabled = !!next;
  petSession.setState({ debugPlanEnabled }, { reason: "debug_plan_toggle" });
  try {
    window.localStorage.setItem(DEBUG_PLAN_STORAGE_KEY, debugPlanEnabled ? "1" : "0");
  } catch (e) {}

  if (debugPlanEnabled) {
    // 仅用于调 UI：展示一个固定示例，不依赖后端
    renderPlan([
      { brief: "已完成", status: "done" },
      { brief: "执行中", status: "running" },
      { brief: "待执行", status: "pending" },
      { brief: "待执行2", status: "pending" }
    ]);
    planEl?.classList.remove("is-hidden");
    bubbleShow("调试模式：规划栏示例已开启", BUBBLE_TYPES.INFO, 1400);
    return;
  }
  hidePlan();
  bubbleShow("调试模式：规划栏示例已关闭", BUBBLE_TYPES.INFO, 1200);
}

function initDebugPlanFromUrlOrStorage() {
  let enabled = false;
  try {
    const params = new URLSearchParams(window.location.search || "");
    const flag = String(params.get("debug") || "").trim().toLowerCase();
    if (flag === "1" || flag === "true" || flag === "yes") enabled = true;
  } catch (e) {}
  if (!enabled) {
    try {
      enabled = window.localStorage.getItem(DEBUG_PLAN_STORAGE_KEY) === "1";
    } catch (e) {}
  }
  if (enabled) setDebugPlanEnabled(true);
}

async function streamAndShow(makeRequest, options = {}) {
  const displayMode = String(options.displayMode || "full").trim().toLowerCase();
  const enableAgentReplay = options.enableAgentReplay === true;
  const expectedRunIdValue = Number(options.expectedRunId);
  const expectedRunId = Number.isFinite(expectedRunIdValue) && expectedRunIdValue > 0
    ? expectedRunIdValue
    : null;
  let streamRunId = null;
  let streamTaskId = null;
  let streamRunStatus = "";

  // 取消上一次流式请求（并发保护：旧请求的 finally 不应覆盖新请求状态）
  const { seq: mySeq, controller } = startStream(chatStream);
  setChatStreaming(true);

  // 新的执行链路开始：清空“等待用户输入”的续跑状态（后续若需要会由 need_input 事件重新设置）
  clearPendingAgentResume({ reason: "stream_start" });

  // 新的一次流式会话开始：清空规划栏（由后端 plan 事件重新填充）
  if (planHideTimer) {
    clearTimeout(planHideTimer);
    planHideTimer = null;
  }
  hidePlan();

  bubbleSet(UI_TEXT.PET_CHAT_SENDING || "...", BUBBLE_TYPES.INFO);
  schedulePetContentSizeReport();

  const { transcript, hadError } = await streamSse(
    (signal) => makeRequest(signal),
    {
      signal: controller.signal,
      displayMode,
      shouldPauseUpdates: () => !isStreamActive(chatStream, mySeq) || !!petSession.getState().pendingResume,
      onUpdate: (text) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        bubbleSet(text, BUBBLE_TYPES.INFO);
        schedulePetContentSizeReport();
      },
      onError: (msg) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        bubbleSet(String(msg || "请求失败"), BUBBLE_TYPES.ERROR);
      },
      onRunCreated: (obj) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        emitAgentEvent(
          { ...(obj || {}), type: "run_created", _source: AGENT_EVENT_SOURCE_PET },
          { broadcast: true }
        );
        setLastAgentRun(obj);
        const rid = Number(obj?.run_id);
        if (Number.isFinite(rid) && rid > 0) streamRunId = rid;
        const tid = Number(obj?.task_id);
        if (Number.isFinite(tid) && tid > 0) streamTaskId = tid;
      },
      onRunStatus: (obj) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        emitAgentEvent(
          { ...(obj || {}), type: "run_status", _source: AGENT_EVENT_SOURCE_PET },
          { broadcast: true }
        );
        const status = normalizeRunStatusValue(obj?.status);
        if (status) streamRunStatus = status;
        const rid = Number(obj?.run_id);
        const tid = Number(obj?.task_id);
        if (Number.isFinite(rid) && rid > 0) {
          streamRunId = rid;
          setLastAgentRun({ run_id: rid, task_id: Number.isFinite(tid) && tid > 0 ? tid : null });
        }
        clearPetPendingOnRunStatus(status, rid, {
          forRun: false,
          continueReason: "run_status_continue",
          // 兜底：极少数情况下 run_status 事件缺少 run_id，终态时仍应清理残留选择框。
          terminalReason: "run_status_terminal_fallback"
        });
      },
      onNeedInput: (obj) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        emitAgentEvent(
          { ...(obj || {}), type: "need_input", _source: AGENT_EVENT_SOURCE_PET },
          { broadcast: true }
        );
        const applied = setPendingAgentResume(obj);
        if (applied?.suppressed || !applied?.changed) return;
        setLastAgentRun(obj);
        const rid = Number(obj?.run_id);
        if (Number.isFinite(rid) && rid > 0) streamRunId = rid;
        const tid = Number(obj?.task_id);
        if (Number.isFinite(tid) && tid > 0) streamTaskId = tid;
        const question = String(obj?.question || "").trim();
        if (question) {
          renderNeedInputQuestionThenChoices({
            question,
            payload: {
              ...obj,
              choices: applied?.pending?.choices || obj?.choices
            },
            renderQuestion: (text) => bubbleSet(text, BUBBLE_TYPES.INFO),
            renderChoices: (payload) => showNeedInputChoicesUi(payload),
          });
          // 需要用户补充信息属于“对话的一部分”：落库后世界页/状态页可一致回放。
          if (applied.changed) {
            writePetChatMessage("assistant", question, {
              task_id: Number(obj?.task_id) || null,
              run_id: Number(obj?.run_id) || null,
              metadata: { mode: "need_input" }
            }).catch(() => {});
          }
        } else {
          bubbleSet("需要你补充信息后才能继续执行。", BUBBLE_TYPES.WARNING);
        }
        showChat();
        schedulePetContentSizeReport();
      },
      onPlan: (obj) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        planUpdate({ task_id: obj.task_id, items: obj.items });
      },
      onPlanDelta: (obj) => {
        if (!isStreamActive(chatStream, mySeq)) return;
        planApplyDelta(obj);
      },
      onEvent: (obj) => {
        // 复用现有 SSE：把关键结构化事件转发给主面板（跨窗口同步）
        if (!isStreamActive(chatStream, mySeq)) return;
        if (obj?.type === "replay_applied") {
          const n = Number(obj?.applied);
          if (Number.isFinite(n) && n > 0) {
            bubbleShow(`连接已恢复，已从事件日志补齐 ${n} 条事件。`, BUBBLE_TYPES.INFO, 1500);
          }
          return;
        }
        if (obj?.type === "memory_item" || obj?.type === "agent_stage" || obj?.type === "plan" || obj?.type === "plan_delta") {
          emitAgentEvent(obj, { broadcast: true });
        }
      },
      onReviewDelta: (obj) => {
        const reviewText = formatReviewToText(obj);
        return reviewText ? `${reviewText}\n` : "";
      },
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

  // 仅最新会话允许收敛状态（避免 abort 旧会话导致“卡住/闪回”）
  if (stopStream(chatStream, mySeq)) {
    setChatStreaming(false);
    if (!petSession.getState().pendingResume) scheduleHidePlan();
  }

  if (hadError) {
    return { transcript: "", hadError: true, runId: streamRunId, taskId: streamTaskId, runStatus: streamRunStatus };
  }
  return { transcript, hadError: false, runId: streamRunId, taskId: streamTaskId, runStatus: streamRunStatus };
}

function resolveRunIdForPostRun(streamResult) {
  const ridFromStream = Number(streamResult?.runId);
  if (Number.isFinite(ridFromStream) && ridFromStream > 0) return ridFromStream;
  const ridFromState = Number(petSession.getState().lastRun?.runId);
  if (Number.isFinite(ridFromState) && ridFromState > 0) return ridFromState;
  return null;
}

async function resolveLatestRunForTask(taskId) {
  const tid = Number(taskId);
  if (!Number.isFinite(tid) || tid <= 0) {
    return { runId: null, taskId: null, status: "" };
  }
  try {
    const resp = await api.fetchTaskRuns(tid);
    const latest = pickLatestTaskRunMeta(resp?.items);
    if (!Number.isFinite(Number(latest?.runId)) || Number(latest?.runId) <= 0) {
      return { runId: null, taskId: tid, status: "" };
    }
    return {
      runId: Number(latest.runId),
      taskId: Number(latest.taskId) || tid,
      status: String(latest.status || "").trim()
    };
  } catch (error) {
    return { runId: null, taskId: tid, status: "" };
  }
}

async function showRunSyncingHintFromBackend() {
  const current = await api.fetchCurrentAgentRun().catch(() => null);
  const run = current?.run || null;
  bubbleSet(buildRunSyncingHint(run), BUBBLE_TYPES.INFO);
}

async function buildNoVisibleResultWithDebug(runStatus, runId) {
  const rid = Number(runId);
  let status = normalizeRunStatusValue(runStatus);
  let lastError = null;

  if (Number.isFinite(rid) && rid > 0) {
    const detail = await api.fetchAgentRunDetail(rid).catch(() => null);
    if (!status) {
      status = normalizeRunStatusValue(detail?.run?.status);
    }
    lastError = extractRunLastError(detail);
  }

  return buildNoVisibleResultText(status, {
    runId: Number.isFinite(rid) && rid > 0 ? rid : null,
    lastError
  });
}

function feedbackAckBubbleType(status) {
  const normalized = normalizeRunStatusValue(status);
  if (normalized === "done") return BUBBLE_TYPES.SUCCESS;
  if (normalized === "failed") return BUBBLE_TYPES.ERROR;
  if (normalized === "stopped" || normalized === "cancelled") return BUBBLE_TYPES.WARNING;
  return BUBBLE_TYPES.INFO;
}

async function handleChatMode(message) {
  const text = String(message || "").trim();
  if (!text) return;

  await writePetChatMessage("user", text, { metadata: { mode: "chat" } });

  const ctx = await buildPetChatContextMessages(
    UI_TEXT.PET_SYSTEM_PROMPT || "",
    MAX_CHAT_MESSAGES
  );
  const streamResult = await streamAndShow(
    (signal) => api.streamPetChat({ messages: ctx }, signal),
    { displayMode: "full" }
  );
  const finalText = String(streamResult?.transcript || "");
  if (!finalText) return;

  bubbleSet(finalText, BUBBLE_TYPES.INFO);
  await writePetChatMessage("assistant", finalText, { metadata: { mode: "chat" } });
}

function showDoResultUi(visibleText) {
  // 任务结果已展示：抑制轮询的“任务完成!”提示，避免覆盖最终回答
  petSession.setState({ suppressAutoCompletionBubble: true }, { reason: "do_result_show" });
  bubbleSet(String(visibleText || "").trim(), BUBBLE_TYPES.INFO);
}

async function persistDoAssistantMessage(visible, extra = {}) {
  const visibleForStore = normalizeResultText(visible);
  const content = visibleForStore || visible;
  if (!String(content || "").trim()) return;

  await writePetChatMessage("assistant", content, {
    task_id: extra.task_id || null,
    run_id: extra.run_id || null,
    metadata: { mode: "do", ...(extra.metadata || {}) }
  });
}

async function ensurePendingResumeFromBackend(runId) {
  const rid = Number(runId);
  if (!Number.isFinite(rid) || rid <= 0) return { waiting: false, question: "", status: "" };

  const detail = await api.fetchAgentRunDetail(rid).catch(() => null);
  const resolved = resolvePendingResumeFromRunDetail(rid, detail, { requireQuestionForWaiting: false });
  if (!resolved.waiting) {
    return { waiting: false, question: "", status: resolved.status };
  }
  if (!resolved.pending) {
    return { waiting: true, question: "", status: resolved.status };
  }

  const pending = petSession.getState().pendingResume;
  const pendingRunId = Number(pending?.runId);
  const shouldRefreshPending = !Number.isFinite(pendingRunId)
    || pendingRunId !== rid
    || String(pending?.question || "").trim() !== String(resolved.pending.question || "").trim()
    || String(pending?.kind || "").trim() !== String(resolved.pending.kind || "").trim()
    || String(pending?.promptToken || "").trim() !== String(resolved.pending.promptToken || "").trim()
    || String(pending?.sessionKey || "").trim() !== String(resolved.pending.sessionKey || "").trim();
  if (shouldRefreshPending) {
    const payload = {
      run_id: resolved.pending.runId,
      task_id: resolved.pending.taskId,
      question: resolved.pending.question,
      kind: resolved.pending.kind,
      choices: resolved.pending.choices,
      prompt_token: resolved.pending.promptToken,
      session_key: resolved.pending.sessionKey
    };
    const applied = setPendingAgentResume(payload);
    if (applied?.suppressed || !applied?.changed) {
      return { waiting: true, question: resolved.question, status: resolved.status };
    }
    renderNeedInputQuestionThenChoices({
      question: String(resolved.question || ""),
      payload: {
        ...payload,
        choices: applied?.pending?.choices || payload.choices
      },
      renderQuestion: (text) => bubbleSet(text, BUBBLE_TYPES.INFO),
      renderChoices: (data) => showNeedInputChoicesUi(data),
    });
    showChat();
    schedulePetContentSizeReport();
  }
  return { waiting: true, question: resolved.question, status: resolved.status };
}

async function handleDoMode(message, options = {}) {
  const text = String(message || "").trim();
  if (!text) return;
  const requestedMode = String(options?.mode || "do").trim().toLowerCase();
  const agentMode = requestedMode === "think" ? "think" : "do";

  await writePetChatMessage("user", text, { metadata: { mode: agentMode } });
  // 每次 /do 前清空 lastRun，避免缺失 run_created 事件时误用旧 run 触发错误反馈提示。
  petSession.setState({ lastRun: null }, { reason: "do_reset_last_run" });
  const streamResult = await streamAndShow(
    (signal) => api.streamAgentCommand({ message: text, mode: agentMode }, signal),
    { displayMode: "status", enableAgentReplay: true }
  );
  const finalText = String(streamResult?.transcript || "");
  const runId = resolveRunIdForPostRun(streamResult);
  if (!Number.isFinite(runId) || runId <= 0) {
    await showRunSyncingHintFromBackend();
    return;
  }
  let runStatus = normalizeRunStatusValue(streamResult?.runStatus);
  if (runStatus === "waiting") {
    await ensurePendingResumeFromBackend(runId);
    return;
  }
  if (!runStatus) {
    const runState = await ensurePendingResumeFromBackend(runId);
    if (runState.waiting) return;
    runStatus = normalizeRunStatusValue(runState.status);
  }
  if (!isTerminalRunStatus(runStatus)) return;
  clearPendingAgentResumeForRun(runId, { reason: "run_terminal" });

  const lastRun = petSession.getState().lastRun;
  const visible = extractVisibleResultText(finalText);
  if (!visible) {
    const warn = await buildNoVisibleResultWithDebug(runStatus, runId);
    bubbleSet(warn, BUBBLE_TYPES.WARNING);
    // 与世界页保持一致：异常也落库，便于回放与调试
    await writePetChatMessage("assistant", warn, {
      task_id: lastRun?.taskId || null,
      run_id: lastRun?.runId || null,
      metadata: { mode: agentMode }
    });
    return;
  }

  await persistDoAssistantMessage(visible, {
    task_id: lastRun?.taskId,
    run_id: lastRun?.runId,
    metadata: { mode: agentMode }
  });
  showDoResultUi(visible);
}

async function handleResumeMode(message) {
  const text = String(message || "").trim();
  if (!text) return;
  if (resumeInFlight) return;

  const pending = petSession.getState().pendingResume;
  if (!pending || !pending.runId) return;
  const pendingKind = String(pending.kind || "").trim();
  const isTaskFeedbackResume = pendingKind === "task_feedback";

  const resumeRunId = pending.runId;
  const resumeTaskId = pending.taskId;
  resumeInFlight = true;
  try {
    clearPendingAgentResume({ reason: "resume_start" });

    await writePetChatMessage("user", text, {
      task_id: resumeTaskId,
      run_id: resumeRunId,
      metadata: { mode: "resume" }
    });
    const streamResult = await streamAndShow(
      (signal) => api.streamAgentResume(
        {
          run_id: resumeRunId,
          message: text,
          prompt_token: pending.promptToken || undefined,
          session_key: pending.sessionKey || undefined
        },
        signal
      ),
      { displayMode: "status", enableAgentReplay: true, expectedRunId: resumeRunId }
    );
    const finalText = String(streamResult?.transcript || "");
    let runStatus = normalizeRunStatusValue(streamResult?.runStatus);
    if (runStatus === "waiting") {
      await ensurePendingResumeFromBackend(resumeRunId);
      return;
    }
    if (!runStatus) {
      const runState = await ensurePendingResumeFromBackend(resumeRunId);
      if (runState.waiting) return;
      runStatus = normalizeRunStatusValue(runState.status);
    }
    if (!isTerminalRunStatus(runStatus)) return;
    clearPendingAgentResumeForRun(resumeRunId, { reason: "run_terminal" });

    // 反馈确认步骤由后端 need_input(task_feedback) 驱动；
    // 这里不再二次弹“满意度选择”，避免状态漂移和重复消息。
    if (isTaskFeedbackResume) {
      const doneText = buildTaskFeedbackAckText(runStatus, resumeRunId);
      bubbleSet(doneText, feedbackAckBubbleType(runStatus));
      await writePetChatMessage("assistant", doneText, {
        task_id: resumeTaskId || null,
        run_id: resumeRunId || null,
        metadata: { mode: "resume", kind: "task_feedback" }
      });
      return;
    }

    const visible = extractVisibleResultText(finalText);
    if (!visible) {
      const fallback = await buildNoVisibleResultWithDebug(runStatus, resumeRunId);
      bubbleSet(fallback, BUBBLE_TYPES.WARNING);
      await writePetChatMessage("assistant", fallback, {
        task_id: resumeTaskId || null,
        run_id: resumeRunId || null,
        metadata: { mode: "resume" }
      });
      return;
    }

    await persistDoAssistantMessage(visible, { task_id: resumeTaskId, run_id: resumeRunId });
    showDoResultUi(visible);
  } finally {
    resumeInFlight = false;
  }
}

function parseEvalArgsOrShowUsage(args) {
  // 用法：
  // - /eval            -> 评估最近一次 /do 的 run
  // - /eval 123        -> 评估指定 run_id
  // - /eval 123 说明   -> 评估指定 run_id 并附带补充说明
  let runId = null;
  let note = "";
  const raw = String(args || "").trim();
  if (raw) {
    const parts = raw.split(/\s+/);
    const maybe = Number(parts[0]);
    if (Number.isFinite(maybe) && maybe > 0) {
      runId = maybe;
      note = parts.slice(1).join(" ").trim();
    } else {
      note = raw;
    }
  }
  if (!runId) runId = Number(petSession.getState().lastRun?.runId);
  if (!Number.isFinite(runId) || runId <= 0) {
    bubbleSet("用法：/eval [run_id] [补充说明]（当前没有可评估的 run）", BUBBLE_TYPES.WARNING);
    return null;
  }
  return { runId, note };
}

const SLASH_COMMAND_HANDLERS = {
  help: async () => bubbleSet(petHelpText(), BUBBLE_TYPES.INFO),
  debugplan: async () => setDebugPlanEnabled(!petSession.getState().debugPlanEnabled),
  panel: async () => {
    if (ipcRenderer) {
      ipcRenderer.send("toggle-panel");
      bubbleShow("已打开主面板", BUBBLE_TYPES.SUCCESS, 1600);
      return;
    }
    bubbleShow("主进程 IPC 不可用", BUBBLE_TYPES.ERROR, 2000);
  },
  chat: async (args) => {
    if (!args) {
      bubbleSet("用法：/chat 你好", BUBBLE_TYPES.WARNING);
      return;
    }
    await handleChatMode(args);
  },
  do: async (args) => {
    if (!args) {
      bubbleSet("用法：/do 帮我写一条记忆：xxx", BUBBLE_TYPES.WARNING);
      return;
    }
    await handleDoMode(args, { mode: "do" });
  },
  think: async (args) => {
    if (!args) {
      bubbleSet("用法：/think 先分析再执行 xxx", BUBBLE_TYPES.WARNING);
      return;
    }
    await handleDoMode(args, { mode: "think" });
  },
  eval: async (args) => {
    const parsed = parseEvalArgsOrShowUsage(args);
    if (!parsed) return;
    const streamResult = await streamAndShow(
      (signal) => api.streamAgentEvaluate({ run_id: parsed.runId, message: parsed.note }, signal),
      { displayMode: "full" }
    );
    const finalText = String(streamResult?.transcript || "");
    if (finalText) bubbleSet(finalText, BUBBLE_TYPES.INFO);
  },
  task: async (args) => {
    if (!args) {
      bubbleSet("用法：/task 任务标题", BUBBLE_TYPES.WARNING);
      return;
    }
    try {
      const resp = await api.createTask(args);
      const id = resp?.task?.id;
      bubbleSet(`已创建任务 #${id}: ${args}`, BUBBLE_TYPES.SUCCESS);
    } catch (e) {
      bubbleSet("创建任务失败", BUBBLE_TYPES.ERROR);
    }
  },
  run: async (args) => {
    const id = Number(args);
    if (!Number.isFinite(id) || id <= 0) {
      bubbleSet("用法：/run 任务ID（数字）", BUBBLE_TYPES.WARNING);
      return;
    }
    // 每次 /run 前清空 lastRun，避免 run_created 丢失时误判到历史 run。
    petSession.setState({ lastRun: null }, { reason: "run_reset_last_run" });
    const streamResult = await streamAndShow(
      (signal) => api.streamExecuteTask(id, {}, signal),
      { displayMode: "status", enableAgentReplay: true }
    );
    const finalText = String(streamResult?.transcript || "");
    let runId = resolveRunIdForPostRun(streamResult);
    let runStatus = normalizeRunStatusValue(streamResult?.runStatus);
    if ((!Number.isFinite(runId) || runId <= 0) || !runStatus) {
      const fallback = await resolveLatestRunForTask(id);
      if (!Number.isFinite(runId) || runId <= 0) {
        runId = Number(fallback?.runId) || null;
      }
      if (!runStatus) {
        runStatus = normalizeRunStatusValue(fallback?.status);
      }
      if (Number.isFinite(Number(runId)) && Number(runId) > 0) {
        setLastAgentRun({ run_id: Number(runId), task_id: Number(fallback?.taskId) || id });
      }
    }
    if (!Number.isFinite(runId) || runId <= 0) {
      await showRunSyncingHintFromBackend();
      return;
    }
    if (runStatus === "waiting") {
      await ensurePendingResumeFromBackend(runId);
      return;
    }
    if (!runStatus) {
      const runState = await ensurePendingResumeFromBackend(runId);
      if (runState.waiting) return;
      runStatus = normalizeRunStatusValue(runState.status);
    }
    if (!isTerminalRunStatus(runStatus)) return;
    clearPendingAgentResumeForRun(runId, { reason: "run_terminal" });

    const visible = extractVisibleResultText(finalText);
    if (!visible) {
      const fallback = await buildNoVisibleResultWithDebug(runStatus, runId);
      bubbleSet(fallback, BUBBLE_TYPES.WARNING);
      return;
    }
    showDoResultUi(visible);
  },
  memory: async (args) => {
    if (!args) {
      bubbleSet("用法：/memory 记忆内容", BUBBLE_TYPES.WARNING);
      return;
    }
    try {
      const resp = await api.createMemoryItem(args);
      bubbleSet(`已写入记忆 #${resp?.item?.id}`, BUBBLE_TYPES.SUCCESS);
    } catch (e) {
      bubbleSet("写入记忆失败", BUBBLE_TYPES.ERROR);
    }
  },
  search: async (args) => {
    if (!args) {
      bubbleSet("用法：/search 关键词", BUBBLE_TYPES.WARNING);
      return;
    }
    try {
      const resp = await api.searchUnified(args, 3);
      const mem = resp?.memory?.length || 0;
      const skills = resp?.skills?.length || 0;
      const nodes = resp?.graph?.nodes?.length || 0;
      const top = [];
      if (resp?.memory?.[0]?.content) top.push(`记忆: ${resp.memory[0].content}`);
      if (resp?.skills?.[0]?.name) top.push(`技能: ${resp.skills[0].name}`);
      if (resp?.graph?.nodes?.[0]?.label) top.push(`图谱: ${resp.graph.nodes[0].label}`);
      const head = `检索结果 memory:${mem} skills:${skills} graph:${nodes}`;
      bubbleSet([head, ...top].join("\n"), BUBBLE_TYPES.INFO);
    } catch (e) {
      bubbleSet("检索失败", BUBBLE_TYPES.ERROR);
    }
  }
};

async function dispatchSlashCommand(cmdObj) {
  const cmd = String(cmdObj?.cmd || "").trim().toLowerCase();
  const args = cmdObj?.args;
  const handler = SLASH_COMMAND_HANDLERS[cmd];
  if (!handler) {
    bubbleSet(`未知命令：/${cmd}\n\n${petHelpText()}`, BUBBLE_TYPES.WARNING);
    return;
  }
  await handler(args);
}

async function resolveRouteModeForMessage(text) {
  let mode = "";
  try {
    const route = await api.routeAgentMode({ message: text });
    mode = String(route?.mode || "").trim().toLowerCase();
  } catch (e) {
    mode = "";
  }
  if (mode !== "chat" && mode !== "do" && mode !== "think") {
    mode = "chat";
  }
  return mode;
}

async function sendChatMessage(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return;

  const cmd = parseSlashCommand(text);
  if (cmd) {
    await dispatchSlashCommand(cmd);
    return;
  }

  // 若当前 run 正在等待用户输入：把本次输入当作“回答”，调用 resume 继续执行
  if (petSession.getState().pendingResume) {
    await handleResumeMode(text);
    return;
  }

  // 默认：让后端 LLM 做一次“chat/do/think”路由。
  // 路由失败时默认走 chat（用户仍可用 /do 或 /think 强制）。
  bubbleSet(UI_TEXT.PET_CHAT_SENDING || "...", BUBBLE_TYPES.INFO);
  const mode = await resolveRouteModeForMessage(text);

  if (mode === "chat") {
    await handleChatMode(text);
    return;
  }

  await handleDoMode(text, { mode });
}

// 双击打开面板（通过 IPC）
petEl?.addEventListener("dblclick", (event) => {
  if (suppressClickOnce) {
    suppressClickOnce = false;
    return;
  }
  // 仅允许双击雷姆像素区域触发（避免透明区域误触）
  if (!isHitOnPetImage(event.clientX, event.clientY)) return;
  if (ipcRenderer) {
    ipcRenderer.send("toggle-panel");
  }
});

function submitChatInput() {
  const raw = String(chatInputEl?.value || "");
  const text = raw.trim();
  if (!text) return;
  // 写入历史（去重：连续相同不重复记录）
  chatInputHistory.record(text);
  if (chatInputEl) chatInputEl.value = "";
  sendChatMessage(text);
}

chatSendEl?.addEventListener("click", () => {
  submitChatInput();
  try { chatInputEl?.focus?.(); } catch (e) {}
});
chatInputEl?.addEventListener("keydown", (event) => {
  // 输入历史：上下键回溯之前的输入（类似 shell history）
  if (chatInputHistory.handleKeyDown(event, chatInputEl)) return;
  if (event.key === "Enter" && !event.shiftKey) {
    event.preventDefault();
    submitChatInput();
    return;
  }
  if (event.key === "Escape") {
    event.preventDefault();
    chatInputHistory.resetNavigation({ reason: "input_escape" });
    hideChat();
  }
});


// 初始化显示
animator.setState(PET_STATES.IDLE);
// 默认清空气泡/规划栏
bubbleClear();
hidePlan();
// 启动后立刻尝试同步一次输入历史（后端不可用时自动忽略）
refreshPetInputHistoryFromBackend(0).catch(() => {});

// 调试：支持通过 URL 参数或 localStorage 开启规划栏示例
// - URL: pet.html?debug=1
// - 命令: /debugplan
initDebugPlanFromUrlOrStorage();

// 启动轮询
startPolling();

// 页面卸载时清理资源：停止轮询、中断流式连接、清除定时器
window.addEventListener("beforeunload", () => {
  stopPolling();
  abortStream(chatStream);
  if (planHideTimer) {
    clearTimeout(planHideTimer);
    planHideTimer = null;
  }
});

import { isTerminalRunStatus, normalizeRunStatusValue } from "./run_status.js";

export const NEED_INPUT_CHOICES_LIMIT_DEFAULT = 12;

function normalizeChoiceText(raw) {
  return String(raw == null ? "" : raw).replace(/\s+/g, " ").trim();
}

function toChoiceLabel(text) {
  return normalizeChoiceText(text);
}

export function normalizeNeedInputChoices(rawChoices, options = {}) {
  if (!Array.isArray(rawChoices)) return [];
  const limit = Number.isFinite(options?.limit)
    ? Math.max(1, Number(options.limit))
    : NEED_INPUT_CHOICES_LIMIT_DEFAULT;
  const out = [];
  const seen = new Set();
  for (const rawItem of rawChoices) {
    if (typeof rawItem === "string") {
      const value = normalizeChoiceText(rawItem);
      if (!value || seen.has(value)) continue;
      seen.add(value);
      out.push({ label: toChoiceLabel(value), value });
      if (out.length >= limit) break;
      continue;
    }
    if (!rawItem || typeof rawItem !== "object") continue;
    const fallback = normalizeChoiceText(rawItem?.label || rawItem?.value || "");
    const value = normalizeChoiceText(rawItem?.value != null ? rawItem.value : fallback);
    if (!value || seen.has(value)) continue;
    seen.add(value);
    const label = toChoiceLabel(rawItem?.label || fallback || value);
    out.push({ label: label || toChoiceLabel(value), value });
    if (out.length >= limit) break;
  }
  return out;
}

export function inferNeedInputChoices(question, kind) {
  const normalizedKind = String(kind || "").trim().toLowerCase();
  if (normalizedKind === "task_feedback") {
    return [{ label: "是", value: "是" }, { label: "否", value: "否" }];
  }

  const text = String(question || "").trim();
  if (!text) return [];
  const looksLikeYesNo = (
    text.includes("是否")
    || text.includes("可否")
    || text.includes("能否")
    || text.includes("要不要")
    || text.includes("确认")
    || text.endsWith("吗")
    || text.endsWith("吗？")
    || text.endsWith("吗?")
  );
  if (!looksLikeYesNo) return [];
  return [{ label: "是", value: "是" }, { label: "否", value: "否" }];
}

function normalizePromptToken(raw) {
  const text = String(raw == null ? "" : raw).trim();
  if (!text) return "";
  const safe = text.replace(/[^a-zA-Z0-9._-]/g, "");
  return safe.slice(0, 96);
}

function normalizeSessionKey(raw) {
  const text = String(raw == null ? "" : raw).trim();
  if (!text) return "";
  return text.replace(/[^a-zA-Z0-9._:-]/g, "").slice(0, 128);
}

function normalizeQuestionText(raw) {
  return normalizeChoiceText(raw).slice(0, 240);
}

function normalizeNeedInputKind(raw) {
  return String(raw == null ? "" : raw).trim().toLowerCase().slice(0, 64);
}

function normalizeNeedInputRunId(raw) {
  const id = Number(raw);
  return Number.isFinite(id) && id > 0 ? id : 0;
}

export function buildNeedInputFingerprint(payload) {
  const runId = normalizeNeedInputRunId(payload?.runId ?? payload?.run_id);
  if (!runId) return "";

  const kind = normalizeNeedInputKind(payload?.kind);
  const promptToken = normalizePromptToken(payload?.promptToken ?? payload?.prompt_token);
  const sessionKey = normalizeSessionKey(payload?.sessionKey ?? payload?.session_key);
  const question = normalizeQuestionText(payload?.question);

  const anchor = promptToken || sessionKey || question;
  if (!anchor) return "";
  return `${runId}:${kind || "-"}:${anchor}`;
}

function normalizeNeedInputRecentRecord(raw) {
  if (!raw || typeof raw !== "object") return null;
  const runId = normalizeNeedInputRunId(raw.runId ?? raw.run_id);
  const fingerprint = String(raw.fingerprint || "").trim();
  const at = Number(raw.at);
  if (!runId || !fingerprint || !Number.isFinite(at) || at <= 0) return null;
  return { runId, fingerprint, at };
}

export function pruneNeedInputRecentRecords(rawRecords, options = {}) {
  const nowMs = Number.isFinite(options?.nowMs) ? Number(options.nowMs) : Date.now();
  const ttlMs = Number.isFinite(options?.ttlMs) ? Math.max(1000, Number(options.ttlMs)) : 20000;
  const limit = Number.isFinite(options?.limit) ? Math.max(1, Number(options.limit)) : 64;

  const records = Array.isArray(rawRecords) ? rawRecords : [];
  const normalized = [];
  const dedup = new Set();
  for (let i = records.length - 1; i >= 0; i -= 1) {
    const item = normalizeNeedInputRecentRecord(records[i]);
    if (!item) continue;
    if (nowMs - item.at > ttlMs) continue;
    const key = `${item.fingerprint}@${item.at}`;
    if (dedup.has(key)) continue;
    dedup.add(key);
    normalized.push(item);
    if (normalized.length >= limit) break;
  }
  normalized.reverse();
  return normalized;
}

export function shouldSuppressNeedInputPrompt(payload, rawRecords, options = {}) {
  const fingerprint = buildNeedInputFingerprint(payload);
  if (!fingerprint) return false;
  const records = pruneNeedInputRecentRecords(rawRecords, options);
  return records.some((item) => item.fingerprint === fingerprint);
}

export function markNeedInputPromptHandled(payload, rawRecords, options = {}) {
  const nowMs = Number.isFinite(options?.nowMs) ? Number(options.nowMs) : Date.now();
  const fingerprint = buildNeedInputFingerprint(payload);
  const runId = normalizeNeedInputRunId(payload?.runId ?? payload?.run_id);
  let next = pruneNeedInputRecentRecords(rawRecords, { ...options, nowMs });
  if (!fingerprint || !runId) return next;
  next = next.filter((item) => item.fingerprint !== fingerprint);
  next.push({ runId, fingerprint, at: nowMs });
  return pruneNeedInputRecentRecords(next, { ...options, nowMs });
}

/**
 * 统一计算 pendingResume 的更新结果：
 * - 构建 pending
 * - recentRecords 归一化
 * - suppress 判定
 * - changed 判定
 */
export function computePendingResumeTransition(params = {}) {
  const ttlMs = Number.isFinite(params?.ttlMs) ? Number(params.ttlMs) : 20000;
  const pending = buildPendingResumeFromPayload(params?.payload, {
    normalizeChoices: params?.normalizeChoices,
    defaultQuestion: params?.defaultQuestion,
    requireQuestion: params?.requireQuestion === true,
    limit: params?.limit
  });
  const recentRecords = pruneNeedInputRecentRecords(params?.recentRecords, { ttlMs });
  if (!pending) {
    return {
      pending: null,
      recentRecords,
      suppressed: false,
      changed: false,
      valid: false
    };
  }

  if (shouldSuppressNeedInputPrompt(pending, recentRecords, { ttlMs })) {
    return {
      pending,
      recentRecords,
      suppressed: true,
      changed: false,
      valid: true
    };
  }

  const changed = !isSamePendingResume(params?.currentPending, pending);
  return {
    pending,
    recentRecords,
    suppressed: false,
    changed,
    valid: true
  };
}

export function resolvePendingResumeFromRunDetail(runId, detail, options = {}) {
  const rid = Number(runId);
  if (!Number.isFinite(rid) || rid <= 0) {
    return { waiting: false, status: "", question: "", taskId: null, pending: null };
  }
  const status = normalizeRunStatusValue(detail?.run?.status);
  const paused = detail?.agent_state?.paused;
  const question = String(paused?.question || "").trim();
  const taskId = Number(detail?.run?.task_id || options?.fallbackTaskId) || null;
  const promptToken = normalizePromptToken(paused?.prompt_token || paused?.promptToken);
  const sessionKey = normalizeSessionKey(
    paused?.session_key || detail?.agent_state?.session_key || detail?.snapshot?.session_key
  );
  const defaultQuestion = normalizeQuestionText(options?.defaultQuestion)
    || "需要你补充信息后才能继续执行。";

  if (status !== "waiting") {
    return { waiting: false, status, question: "", taskId: null, pending: null };
  }

  const waiting = true;
  const effectiveQuestion = question || defaultQuestion;
  let pending = null;
  if (waiting && effectiveQuestion) {
    const normalizeChoices = typeof options?.normalizeChoices === "function"
      ? options.normalizeChoices
      : (raw) => normalizeNeedInputChoices(raw, { limit: options?.limit });
    pending = {
      runId: rid,
      taskId,
      question: effectiveQuestion,
      kind: String(paused?.kind || "").trim() || null,
      choices: normalizeChoices(paused?.choices),
      promptToken: promptToken || null,
      sessionKey: sessionKey || null
    };
  }

  return { waiting, status, question: effectiveQuestion, taskId, pending };
}

export function renderNeedInputQuestionThenChoices(options = {}) {
  const renderQuestion = typeof options?.renderQuestion === "function" ? options.renderQuestion : null;
  const renderChoices = typeof options?.renderChoices === "function" ? options.renderChoices : null;
  const question = String(options?.question || "");
  if (renderQuestion) {
    renderQuestion(question);
  }
  if (renderChoices) {
    renderChoices(options?.payload);
  }
}

function normalizeNeedInputTaskId(raw) {
  const id = Number(raw);
  return Number.isFinite(id) && id > 0 ? id : null;
}

/**
 * 统一构建 pendingResume 数据结构，避免 pet/panel 双实现漂移。
 */
export function buildPendingResumeFromPayload(payload, options = {}) {
  const runId = normalizeNeedInputRunId(payload?.runId ?? payload?.run_id);
  if (!runId) return null;

  const normalizeChoices = typeof options?.normalizeChoices === "function"
    ? options.normalizeChoices
    : (raw) => normalizeNeedInputChoices(raw, { limit: options?.limit });
  const rawQuestion = payload?.question;
  const defaultQuestion = options?.defaultQuestion;
  const question = normalizeQuestionText(rawQuestion || defaultQuestion || "");
  if (options?.requireQuestion === true && !question) return null;

  const pending = {
    runId,
    taskId: normalizeNeedInputTaskId(payload?.taskId ?? payload?.task_id),
    question,
    kind: String(payload?.kind || "").trim() || null,
    choices: normalizeChoices(payload?.choices),
    promptToken: normalizePromptToken(payload?.promptToken ?? payload?.prompt_token) || null,
    sessionKey: normalizeSessionKey(payload?.sessionKey ?? payload?.session_key) || null
  };
  return pending;
}

/**
 * pendingResume 等价性判断（对 choices 做值比较）。
 */
export function isSamePendingResume(a, b) {
  if (!a || !b) return false;
  return (
    Number(a.runId) === Number(b.runId)
    && Number(a.taskId || 0) === Number(b.taskId || 0)
    && String(a.question || "") === String(b.question || "")
    && String(a.kind || "") === String(b.kind || "")
    && String(a.promptToken || "") === String(b.promptToken || "")
    && String(a.sessionKey || "") === String(b.sessionKey || "")
    && JSON.stringify(Array.isArray(a.choices) ? a.choices : [])
      === JSON.stringify(Array.isArray(b.choices) ? b.choices : [])
  );
}

/**
 * run_status 到来时，是否应清理 pendingResume。
 */
export function shouldClearPendingResumeOnRunStatus(status, eventRunId, pendingRunId) {
  const normalized = normalizeRunStatusValue(status);
  if (!normalized || normalized === "waiting") return false;

  const rid = Number(eventRunId);
  const pid = Number(pendingRunId);
  if (!Number.isFinite(pid) || pid <= 0) return false;

  if (Number.isFinite(rid) && rid > 0) {
    return rid === pid;
  }
  return isTerminalRunStatus(normalized);
}

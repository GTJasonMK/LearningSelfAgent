import { normalizeRunStatusValue } from "./run_status.js";

function toPositiveInt(value) {
  const n = Number(value);
  return Number.isFinite(n) && n > 0 ? n : 0;
}

function isObject(value) {
  return !!value && typeof value === "object" && !Array.isArray(value);
}

function buildEmptyRun() {
  return {
    task_title: "",
    summary: null,
    mode: null,
    started_at: "",
    finished_at: "",
    created_at: "",
    updated_at: "",
    is_current: true
  };
}

function buildRunPlaceholder(runId, taskId, status = "running") {
  const rid = toPositiveInt(runId);
  if (!rid) return null;
  const tid = toPositiveInt(taskId);
  return {
    ...buildEmptyRun(),
    run_id: rid,
    task_id: tid || null,
    status: String(status || "").trim() || "running"
  };
}

export function applyWorldRunStatusState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const prevRun = isObject(prevState.currentRun) ? prevState.currentRun : null;
  const prevRunId = toPositiveInt(prevRun?.run_id);
  const prevTaskId = toPositiveInt(prevRun?.task_id);

  const eventRunId = toPositiveInt(payload.runId);
  const eventTaskId = toPositiveInt(payload.taskId);
  const nextRunId = eventRunId || prevRunId || 0;
  const nextTaskId = eventTaskId || prevTaskId || 0;
  const nextStatus = String(payload.status || "").trim()
    || normalizeRunStatusValue(prevRun?.status)
    || "running";

  let nextCurrentRun = prevRun;
  if (nextRunId && (!prevRunId || prevRunId === nextRunId)) {
    nextCurrentRun = {
      ...(prevRun || buildEmptyRun()),
      run_id: nextRunId,
      task_id: nextTaskId || null,
      status: nextStatus
    };
  }

  const prevMeta = isObject(prevState.lastRunMeta) ? prevState.lastRunMeta : {};
  let nextLastRunMeta = prevState.lastRunMeta;
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
    ...prevState,
    currentRun: nextCurrentRun,
    lastRunMeta: nextLastRunMeta
  };
}

export function applyWorldPendingResumeState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const pending = payload.pending;
  const recentRecords = Array.isArray(payload.recentRecords) ? payload.recentRecords : [];

  const rid = toPositiveInt(pending?.runId);
  const tid = toPositiveInt(pending?.taskId);
  const prevRun = isObject(prevState.currentRun) ? prevState.currentRun : null;
  const prevRunId = toPositiveInt(prevRun?.run_id);
  const allowUpdateRun = !!rid && (!prevRunId || prevRunId === rid);
  const nextCurrentRun = allowUpdateRun
    ? {
      ...(prevRun || {}),
      run_id: rid,
      task_id: tid || null,
      status: "waiting",
      is_current: true
    }
    : prevRun;

  const prevMeta = isObject(prevState.lastRunMeta) ? prevState.lastRunMeta : {};
  const nextLastRunMeta = {
    ...prevMeta,
    run_id: rid || toPositiveInt(prevMeta.run_id) || null,
    task_id: tid || null,
    updated_at: String(prevMeta.updated_at || ""),
    status: "waiting"
  };

  return {
    ...prevState,
    pendingResume: pending,
    needInputRecentRecords: recentRecords,
    currentRun: nextCurrentRun,
    lastRunMeta: nextLastRunMeta
  };
}

export function applyWorldAgentStageState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const runId = toPositiveInt(payload.runId);
  if (!runId) return prevState;
  const taskId = toPositiveInt(payload.taskId);
  const stage = String(payload.stage || "").trim();

  const prevRun = isObject(prevState.currentRun) ? prevState.currentRun : null;
  const prevRunId = toPositiveInt(prevRun?.run_id);
  const nextRun = prevRunId ? prevRun : buildRunPlaceholder(runId, taskId, "running");

  const prevSnapshot = isObject(prevState.currentAgentSnapshot) ? prevState.currentAgentSnapshot : {};
  const nextSnapshot = stage
    ? { ...prevSnapshot, stage }
    : prevSnapshot;

  return {
    ...prevState,
    currentRun: nextRun,
    currentAgentSnapshot: nextSnapshot
  };
}

export function applyWorldAgentPlanState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const runId = toPositiveInt(payload.runId);
  const taskId = toPositiveInt(payload.taskId);
  const status = String(payload.status || "").trim() || "running";
  const items = Array.isArray(payload.items)
    ? payload.items.map((it) => (isObject(it) ? { ...it } : {}))
    : [];
  const planSnapshot = payload.planSnapshot;

  const prevRun = isObject(prevState.currentRun) ? prevState.currentRun : null;
  const prevRunId = toPositiveInt(prevRun?.run_id);
  const nextRun = (!prevRunId && runId)
    ? buildRunPlaceholder(runId, taskId, status)
    : prevRun;

  const prevPlan = isObject(prevState.currentAgentPlan) ? prevState.currentAgentPlan : {};
  const nextPlan = { ...prevPlan, items };

  const prevSnapshot = isObject(prevState.currentAgentSnapshot) ? prevState.currentAgentSnapshot : {};
  const nextSnapshot = isObject(planSnapshot)
    ? { ...prevSnapshot, plan: { ...planSnapshot } }
    : prevSnapshot;

  return {
    ...prevState,
    currentRun: nextRun,
    currentAgentPlan: nextPlan,
    currentAgentSnapshot: nextSnapshot
  };
}

export function applyWorldAgentPlanDeltaState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const items = Array.isArray(payload.items)
    ? payload.items.map((it) => (isObject(it) ? { ...it } : {}))
    : [];
  const planSnapshot = payload.planSnapshot;

  const prevPlan = isObject(prevState.currentAgentPlan) ? prevState.currentAgentPlan : {};
  const nextPlan = { ...prevPlan, items };

  const prevSnapshot = isObject(prevState.currentAgentSnapshot) ? prevState.currentAgentSnapshot : {};
  const nextSnapshot = isObject(planSnapshot)
    ? { ...prevSnapshot, plan: { ...planSnapshot } }
    : prevSnapshot;

  return {
    ...prevState,
    currentAgentPlan: nextPlan,
    currentAgentSnapshot: nextSnapshot
  };
}

export function applyWorldCurrentRunState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const run = isObject(payload.run) ? { ...payload.run } : null;
  return {
    ...prevState,
    currentRun: run
  };
}

export function applyWorldRunDetailState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const detail = isObject(payload.detail) ? payload.detail : {};
  const meta = isObject(payload.lastRunMeta) ? payload.lastRunMeta : null;
  return {
    ...prevState,
    currentAgentPlan: detail?.agent_plan || null,
    currentAgentState: detail?.agent_state || null,
    currentAgentSnapshot: detail?.snapshot || null,
    lastRunMeta: meta || prevState.lastRunMeta
  };
}

export function applyWorldNoRunState(prev) {
  const prevState = isObject(prev) ? prev : {};
  return {
    ...prevState,
    currentRun: null,
    currentAgentPlan: null,
    currentAgentState: null,
    currentAgentSnapshot: null,
    traceLines: [],
    streamingStatus: "",
    lastRunMeta: null
  };
}

export function applyWorldNeedInputRecentRecordsState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const recentRecords = Array.isArray(payload.recentRecords) ? payload.recentRecords : [];
  return {
    ...prevState,
    needInputRecentRecords: recentRecords
  };
}

export function applyWorldPendingResumeClearedState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const recentRecords = Array.isArray(payload.recentRecords)
    ? payload.recentRecords
    : (Array.isArray(prevState.needInputRecentRecords) ? prevState.needInputRecentRecords : []);
  return {
    ...prevState,
    pendingResume: null,
    needInputRecentRecords: recentRecords
  };
}

export function applyWorldStreamStartState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const mode = String(payload.mode || "").trim().toLowerCase();
  const recentRecords = Array.isArray(payload.recentRecords)
    ? payload.recentRecords
    : (Array.isArray(prevState.needInputRecentRecords) ? prevState.needInputRecentRecords : []);
  return {
    ...prevState,
    streaming: true,
    streamingMode: mode,
    pendingResume: null,
    streamingStatus: "",
    needInputRecentRecords: recentRecords
  };
}

export function applyWorldStreamingStatusState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  return {
    ...prevState,
    streamingStatus: String(payload.statusText || "")
  };
}

export function applyWorldStreamStopState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const clearPendingResume = payload.clearPendingResume === true;
  return {
    ...prevState,
    streaming: false,
    streamingMode: "",
    streamingStatus: "",
    ...(clearPendingResume ? { pendingResume: null } : {})
  };
}

export function applyWorldPendingFinalState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  return {
    ...prevState,
    pendingFinal: payload.pendingFinal || null
  };
}

export function applyWorldTraceFetchedAtState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const at = Number(payload.at);
  if (!Number.isFinite(at) || at <= 0) return prevState;
  return {
    ...prevState,
    traceFetchedAt: at
  };
}

export function applyWorldTraceLinesState(prev, payload = {}) {
  const prevState = isObject(prev) ? prev : {};
  const lines = Array.isArray(payload.lines)
    ? payload.lines.map((line) => String(line || ""))
    : [];
  return {
    ...prevState,
    traceLines: lines
  };
}

export function applyWorldPageHideState(prev) {
  const prevState = isObject(prev) ? prev : {};
  return {
    ...prevState,
    streaming: false,
    streamingMode: "",
    pendingResume: null,
    streamingStatus: ""
  };
}

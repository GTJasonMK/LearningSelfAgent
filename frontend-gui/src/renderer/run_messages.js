import { UI_TEXT } from "./constants.js";
import { normalizeRunStatusValue } from "./run_status.js";

function normalizeRunId(runId) {
  const id = Number(runId);
  return Number.isFinite(id) && id > 0 ? id : null;
}

function formatRunIdSuffix(runId) {
  const id = normalizeRunId(runId);
  return id ? `（run#${id}）` : "";
}

export function formatRunStatusDebugLabel(status) {
  const normalized = normalizeRunStatusValue(status);
  switch (normalized) {
    case "running":
      return UI_TEXT.STATUS_RUNNING_LABEL || "执行中";
    case "waiting":
      return UI_TEXT.STATUS_WAITING_LABEL || "等待输入";
    case "done":
      return UI_TEXT.STATUS_DONE_LABEL || "完成";
    case "failed":
      return UI_TEXT.STATUS_FAILED_LABEL || "失败";
    case "stopped":
      return UI_TEXT.STATUS_STOPPED_LABEL || "已中断";
    case "cancelled":
      return UI_TEXT.STATUS_CANCELLED_LABEL || "已取消";
    default:
      return UI_TEXT.STATUS_UNKNOWN_LABEL || "未知";
  }
}

export function buildRunSyncingHint(run) {
  const rid = normalizeRunId(run?.run_id || run?.runId);
  const status = normalizeRunStatusValue(run?.status);
  if (!rid && !status) {
    return "任务状态同步中，请稍后查看执行结果。";
  }
  if (rid && status) {
    return `任务状态同步中：run#${rid}（${formatRunStatusDebugLabel(status)}）。请稍后查看执行结果。`;
  }
  if (rid) {
    return `任务状态同步中：run#${rid}。请稍后查看执行结果。`;
  }
  return `任务状态同步中：${formatRunStatusDebugLabel(status)}。请稍后查看执行结果。`;
}

export function buildTaskFeedbackAckText(status, runId) {
  const normalized = normalizeRunStatusValue(status);
  const suffix = formatRunIdSuffix(runId);
  if (normalized === "done") return "已确认满意，任务已完成。";
  if (normalized === "failed") return `已记录反馈，但任务执行失败${suffix}。`;
  if (normalized === "stopped" || normalized === "cancelled") {
    return `已记录反馈，但任务已中止${suffix}。`;
  }
  if (normalized === "running") return `已记录反馈，任务仍在执行中${suffix}。`;
  if (normalized === "waiting") return `已记录反馈，任务仍在等待输入${suffix}。`;
  return `已记录反馈，任务状态为${formatRunStatusDebugLabel(normalized)}${suffix}。`;
}

export function extractRunLastError(detail) {
  const raw = detail?.snapshot?.counters?.last_error
    || detail?.snapshot?.last_error
    || detail?.last_error
    || null;
  if (!raw || typeof raw !== "object") return null;
  const stepOrder = Number(raw.step_order ?? raw.stepOrder);
  const title = String(raw.title || "").trim();
  const error = String(raw.error || raw.message || "").trim();
  return {
    stepOrder: Number.isFinite(stepOrder) && stepOrder > 0 ? stepOrder : null,
    title: title || null,
    error: error || null
  };
}

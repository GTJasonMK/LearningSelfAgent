import { UI_TEXT } from "./constants.js";

function normalizeStatus(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (!raw) return "pending";
  if (raw === "planned") return "pending";
  if (raw === "pending") return "pending";
  if (raw === "queued") return "pending";
  if (raw === "running") return "running";
  if (raw === "waiting") return "waiting";
  if (raw === "done") return "done";
  if (raw === "failed") return "failed";
  if (raw === "stopped") return "stopped";
  if (raw === "cancelled") return "cancelled";
  if (raw === "skipped") return "skipped";
  return raw;
}

export function statusToTagClass(status) {
  switch (normalizeStatus(status)) {
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

export function formatStatusLabel(status) {
  switch (normalizeStatus(status)) {
    case "pending":
      return UI_TEXT.STATUS_PENDING_LABEL || "pending";
    case "running":
      return UI_TEXT.STATUS_RUNNING_LABEL || "running";
    case "waiting":
      return UI_TEXT.STATUS_WAITING_LABEL || "waiting";
    case "done":
      return UI_TEXT.STATUS_DONE_LABEL || "done";
    case "failed":
      return UI_TEXT.STATUS_FAILED_LABEL || "failed";
    case "skipped":
      return UI_TEXT.STATUS_SKIPPED_LABEL || "skipped";
    case "stopped":
      return UI_TEXT.STATUS_STOPPED_LABEL || "stopped";
    case "cancelled":
      return UI_TEXT.STATUS_CANCELLED_LABEL || "cancelled";
    default:
      return String(status || "").trim() || (UI_TEXT.STATUS_UNKNOWN_LABEL || "unknown");
  }
}

export function normalizeAgentStage(stage) {
  const raw = String(stage || "").trim().toLowerCase();
  if (!raw) return "unknown";
  if (raw === "retrieval") return "retrieval";
  if (raw.includes("retrieval")) return "retrieval";
  if (raw === "planning") return "planning";
  if (raw.includes("planning")) return "planning";
  if (raw === "planned") return "planned";
  if (raw === "execute" || raw.includes("execute")) return "execute";
  if (raw === "finalize" || raw.includes("finalize")) return "finalize";
  if (raw === "review" || raw.includes("review")) return "review";
  if (raw === "postprocess" || raw.includes("postprocess")) return "postprocess";
  if (raw === "waiting_input" || raw.includes("waiting")) return "waiting_input";
  if (raw === "done") return "done";
  if (raw === "failed") return "failed";
  if (raw === "stopped") return "stopped";
  return raw;
}

export function formatAgentStageLabel(stage) {
  switch (normalizeAgentStage(stage)) {
    case "retrieval":
      return UI_TEXT.AGENT_STAGE_RETRIEVAL_LABEL || "retrieval";
    case "planning":
      return UI_TEXT.AGENT_STAGE_PLANNING_LABEL || "planning";
    case "planned":
      return UI_TEXT.AGENT_STAGE_PLANNED_LABEL || "planned";
    case "execute":
      return UI_TEXT.AGENT_STAGE_EXECUTE_LABEL || "execute";
    case "finalize":
      return UI_TEXT.AGENT_STAGE_FINALIZE_LABEL || "finalize";
    case "review":
      return UI_TEXT.AGENT_STAGE_REVIEW_LABEL || "review";
    case "postprocess":
      return UI_TEXT.AGENT_STAGE_POSTPROCESS_LABEL || "postprocess";
    case "waiting_input":
      return UI_TEXT.AGENT_STAGE_WAITING_INPUT_LABEL || "waiting";
    case "done":
      return UI_TEXT.AGENT_STAGE_DONE_LABEL || "done";
    case "failed":
      return UI_TEXT.AGENT_STAGE_FAILED_LABEL || "failed";
    case "stopped":
      return UI_TEXT.AGENT_STAGE_STOPPED_LABEL || "stopped";
    default:
      return UI_TEXT.AGENT_STAGE_UNKNOWN_LABEL || "unknown";
  }
}

export function formatDurationMs(ms) {
  const value = Number(ms);
  if (!Number.isFinite(value) || value < 0) return UI_TEXT.DASH || "-";
  const totalSeconds = Math.floor(value / 1000);
  const seconds = totalSeconds % 60;
  const totalMinutes = Math.floor(totalSeconds / 60);
  const minutes = totalMinutes % 60;
  const hours = Math.floor(totalMinutes / 60);
  if (hours > 0) return `${hours}h${String(minutes).padStart(2, "0")}m`;
  if (minutes > 0) return `${minutes}m${String(seconds).padStart(2, "0")}s`;
  // < 1 min：保留 1 位小数，提升“感知进度”
  const short = Math.round((value / 1000) * 10) / 10;
  return `${short}s`;
}


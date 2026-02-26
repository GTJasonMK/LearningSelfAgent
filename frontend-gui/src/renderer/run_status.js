const TERMINAL_RUN_STATUSES = new Set(["done", "failed", "stopped", "cancelled"]);

export function normalizeRunStatusValue(status) {
  const normalized = String(status || "").trim().toLowerCase();
  if (!normalized) return "";
  if (normalized === "canceled") return "cancelled";
  return normalized;
}

export function isTerminalRunStatus(status) {
  return TERMINAL_RUN_STATUSES.has(normalizeRunStatusValue(status));
}

export function isWaitingRunStatus(status) {
  return normalizeRunStatusValue(status) === "waiting";
}

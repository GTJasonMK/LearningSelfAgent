export function pickLatestTaskRunMeta(rawItems) {
  const items = Array.isArray(rawItems) ? rawItems : [];
  let best = null;

  for (const item of items) {
    if (!item || typeof item !== "object") continue;
    const runId = Number(item.id ?? item.run_id);
    if (!Number.isFinite(runId) || runId <= 0) continue;
    if (!best || runId > best.runId) {
      best = {
        runId,
        taskId: Number(item.task_id) || null,
        status: String(item.status || "").trim()
      };
    }
  }

  if (best) return best;
  return { runId: null, taskId: null, status: "" };
}

import test from "node:test";
import assert from "node:assert/strict";

import { pickLatestTaskRunMeta } from "../src/renderer/run_fallback.js";

test("pickLatestTaskRunMeta selects the max run id from mixed order", () => {
  const out = pickLatestTaskRunMeta([
    { id: 2, task_id: 10, status: "running" },
    { id: 5, task_id: 10, status: "waiting" },
    { id: 3, task_id: 10, status: "done" }
  ]);
  assert.deepEqual(out, { runId: 5, taskId: 10, status: "waiting" });
});

test("pickLatestTaskRunMeta supports run_id field and skips invalid rows", () => {
  const out = pickLatestTaskRunMeta([
    null,
    { id: 0, task_id: 3, status: "failed" },
    { run_id: 8, task_id: 3, status: "stopped" },
    { run_id: "x", task_id: 3, status: "running" }
  ]);
  assert.deepEqual(out, { runId: 8, taskId: 3, status: "stopped" });
});

test("pickLatestTaskRunMeta returns empty meta when no valid run", () => {
  const out = pickLatestTaskRunMeta([{ id: -1 }, {}, null]);
  assert.deepEqual(out, { runId: null, taskId: null, status: "" });
});

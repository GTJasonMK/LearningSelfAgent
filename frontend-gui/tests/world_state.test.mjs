import test from "node:test";
import assert from "node:assert/strict";

import {
  applyWorldAgentPlanDeltaState,
  applyWorldAgentPlanState,
  applyWorldAgentStageState,
  applyWorldCurrentRunState,
  applyWorldNeedInputRecentRecordsState,
  applyWorldNoRunState,
  applyWorldPageHideState,
  applyWorldPendingFinalState,
  applyWorldPendingResumeClearedState,
  applyWorldPendingResumeState,
  applyWorldRunDetailState,
  applyWorldRunStatusState,
  applyWorldStreamStartState,
  applyWorldStreamStopState,
  applyWorldStreamingStatusState,
  applyWorldTraceFetchedAtState,
  applyWorldTraceLinesState
} from "../src/renderer/world_state.js";

test("applyWorldRunStatusState should create currentRun and lastRunMeta", () => {
  const next = applyWorldRunStatusState({}, { status: "running", runId: 42, taskId: 9 });
  assert.equal(next.currentRun.run_id, 42);
  assert.equal(next.currentRun.task_id, 9);
  assert.equal(next.currentRun.status, "running");
  assert.equal(next.lastRunMeta.run_id, 42);
  assert.equal(next.lastRunMeta.task_id, 9);
  assert.equal(next.lastRunMeta.status, "running");
});

test("applyWorldPendingResumeState should not overwrite mismatched currentRun", () => {
  const prev = {
    currentRun: { run_id: 100, task_id: 8, status: "running" },
    lastRunMeta: { run_id: 100, task_id: 8, status: "running", updated_at: "x" }
  };
  const pending = { runId: 200, taskId: 20, question: "请补充" };
  const next = applyWorldPendingResumeState(prev, { pending, recentRecords: [] });
  assert.equal(next.currentRun.run_id, 100);
  assert.equal(next.currentRun.status, "running");
  assert.equal(next.pendingResume.runId, 200);
  assert.equal(next.lastRunMeta.run_id, 200);
  assert.equal(next.lastRunMeta.status, "waiting");
});

test("applyWorldAgentStageState should bootstrap minimal run when absent", () => {
  const next = applyWorldAgentStageState(
    { currentAgentSnapshot: { progress: 0.3 } },
    { runId: 7, taskId: 3, stage: "planning" }
  );
  assert.equal(next.currentRun.run_id, 7);
  assert.equal(next.currentRun.task_id, 3);
  assert.equal(next.currentRun.status, "running");
  assert.equal(next.currentAgentSnapshot.stage, "planning");
  assert.equal(next.currentAgentSnapshot.progress, 0.3);
});

test("applyWorldAgentPlanState should merge plan items and plan snapshot", () => {
  const prev = {
    currentAgentPlan: { source: "sse", items: [] },
    currentAgentSnapshot: { stage: "planning" }
  };
  const next = applyWorldAgentPlanState(prev, {
    runId: 11,
    taskId: 5,
    items: [{ title: "A", status: "running" }],
    planSnapshot: { total: 1, done: 0, running: 1 }
  });
  assert.equal(next.currentRun.run_id, 11);
  assert.equal(next.currentAgentPlan.source, "sse");
  assert.equal(Array.isArray(next.currentAgentPlan.items), true);
  assert.equal(next.currentAgentPlan.items.length, 1);
  assert.equal(next.currentAgentSnapshot.stage, "planning");
  assert.deepEqual(next.currentAgentSnapshot.plan, { total: 1, done: 0, running: 1 });
});

test("applyWorldAgentPlanDeltaState should overwrite items while keeping other plan fields", () => {
  const prev = {
    currentAgentPlan: { source: "poll", items: [{ title: "old" }] },
    currentAgentSnapshot: { stage: "running", plan: { total: 1 } }
  };
  const next = applyWorldAgentPlanDeltaState(prev, {
    items: [{ title: "new-1" }, { title: "new-2" }],
    planSnapshot: { total: 2, done: 1, running: 1 }
  });
  assert.equal(next.currentAgentPlan.source, "poll");
  assert.equal(next.currentAgentPlan.items.length, 2);
  assert.equal(next.currentAgentPlan.items[0].title, "new-1");
  assert.equal(next.currentAgentSnapshot.stage, "running");
  assert.deepEqual(next.currentAgentSnapshot.plan, { total: 2, done: 1, running: 1 });
});

test("applyWorldRunDetailState should set detail fields and meta", () => {
  const next = applyWorldRunDetailState(
    {},
    {
      detail: {
        agent_plan: { items: [{ title: "x" }] },
        agent_state: { paused: null },
        snapshot: { stage: "done" }
      },
      lastRunMeta: { run_id: 9, task_id: 2, updated_at: "2026-02-27", status: "done" }
    }
  );
  assert.equal(next.currentAgentPlan.items.length, 1);
  assert.equal(next.currentAgentState.paused, null);
  assert.equal(next.currentAgentSnapshot.stage, "done");
  assert.equal(next.lastRunMeta.status, "done");
});

test("applyWorldNoRunState should clear run-related state and keep unrelated data", () => {
  const prev = {
    currentRun: { run_id: 1 },
    currentAgentPlan: { items: [1] },
    currentAgentState: { paused: {} },
    currentAgentSnapshot: { stage: "running" },
    lastRunMeta: { run_id: 1 },
    traceLines: ["x"],
    streamingStatus: "busy",
    chat: { timeline: [{ id: 1 }] }
  };
  const next = applyWorldNoRunState(prev);
  assert.equal(next.currentRun, null);
  assert.equal(next.currentAgentPlan, null);
  assert.equal(next.currentAgentState, null);
  assert.equal(next.currentAgentSnapshot, null);
  assert.equal(next.lastRunMeta, null);
  assert.deepEqual(next.traceLines, []);
  assert.equal(next.streamingStatus, "");
  assert.deepEqual(next.chat, { timeline: [{ id: 1 }] });
});

test("applyWorldCurrentRunState should replace currentRun only", () => {
  const prev = {
    currentRun: { run_id: 1, status: "running" },
    currentAgentSnapshot: { stage: "planning" }
  };
  const next = applyWorldCurrentRunState(prev, { run: { run_id: 2, status: "waiting" } });
  assert.equal(next.currentRun.run_id, 2);
  assert.equal(next.currentRun.status, "waiting");
  assert.deepEqual(next.currentAgentSnapshot, { stage: "planning" });
});

test("applyWorldNeedInputRecentRecordsState and clear should update pending state atomically", () => {
  const prev = {
    pendingResume: { runId: 7, question: "q" },
    needInputRecentRecords: [{ runId: 7, fingerprint: "x", at: 1 }]
  };
  const recent = [{ runId: 7, fingerprint: "y", at: 2 }];
  const patched = applyWorldNeedInputRecentRecordsState(prev, { recentRecords: recent });
  assert.deepEqual(patched.needInputRecentRecords, recent);
  assert.equal(patched.pendingResume.runId, 7);

  const cleared = applyWorldPendingResumeClearedState(patched, { recentRecords: recent });
  assert.equal(cleared.pendingResume, null);
  assert.deepEqual(cleared.needInputRecentRecords, recent);
});

test("applyWorldStreamStartState and stop should keep stream lifecycle consistent", () => {
  const prev = {
    streaming: false,
    streamingMode: "",
    pendingResume: { runId: 10, question: "q" },
    streamingStatus: "old",
    needInputRecentRecords: [{ runId: 10, fingerprint: "a", at: 1 }]
  };
  const next = applyWorldStreamStartState(prev, {
    mode: "do",
    recentRecords: [{ runId: 10, fingerprint: "b", at: 2 }]
  });
  assert.equal(next.streaming, true);
  assert.equal(next.streamingMode, "do");
  assert.equal(next.pendingResume, null);
  assert.equal(next.streamingStatus, "");
  assert.deepEqual(next.needInputRecentRecords, [{ runId: 10, fingerprint: "b", at: 2 }]);

  const stopped = applyWorldStreamStopState(next);
  assert.equal(stopped.streaming, false);
  assert.equal(stopped.streamingMode, "");
  assert.equal(stopped.streamingStatus, "");
});

test("applyWorldStreamingStatusState and pendingFinal should update independent slices", () => {
  const prev = { streamingStatus: "", pendingFinal: null, currentRun: { run_id: 1 } };
  const withStatus = applyWorldStreamingStatusState(prev, { statusText: "running..." });
  assert.equal(withStatus.streamingStatus, "running...");
  assert.equal(withStatus.currentRun.run_id, 1);

  const pendingFinal = { tmpKey: "tmp-1", content: "done" };
  const withFinal = applyWorldPendingFinalState(withStatus, { pendingFinal });
  assert.deepEqual(withFinal.pendingFinal, pendingFinal);
  const cleared = applyWorldPendingFinalState(withFinal, { pendingFinal: null });
  assert.equal(cleared.pendingFinal, null);
});

test("applyWorldTrace reducers and page hide reducer should keep state deterministic", () => {
  const prev = {
    traceFetchedAt: 0,
    traceLines: [],
    streaming: true,
    streamingMode: "chat",
    pendingResume: { runId: 1, question: "q" },
    streamingStatus: "busy"
  };
  const withAt = applyWorldTraceFetchedAtState(prev, { at: 123 });
  assert.equal(withAt.traceFetchedAt, 123);
  const withLines = applyWorldTraceLinesState(withAt, { lines: ["a", "b"] });
  assert.deepEqual(withLines.traceLines, ["a", "b"]);

  const hidden = applyWorldPageHideState(withLines);
  assert.equal(hidden.streaming, false);
  assert.equal(hidden.streamingMode, "");
  assert.equal(hidden.streamingStatus, "");
  assert.equal(hidden.pendingResume, null);
});

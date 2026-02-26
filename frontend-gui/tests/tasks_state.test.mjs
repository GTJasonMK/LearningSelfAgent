import test from "node:test";
import assert from "node:assert/strict";

import { UI_TEXT } from "../src/renderer/constants.js";
import { deriveTaskResumeActionState } from "../src/renderer/tabs/tasks.js";

test("deriveTaskResumeActionState default should be enabled", () => {
  const state = deriveTaskResumeActionState();
  assert.equal(state.disabled, false);
  assert.equal(state.title, "");
});

test("deriveTaskResumeActionState disables when waiting for need_input", () => {
  const state = deriveTaskResumeActionState({
    pendingNeedInput: { runId: 10, question: "q" }
  });
  assert.equal(state.disabled, true);
  assert.equal(state.title, UI_TEXT.TASK_RESUME_DISABLED_WAITING);
});

test("deriveTaskResumeActionState disables while resuming", () => {
  const state = deriveTaskResumeActionState({ resumingNeedInput: true });
  assert.equal(state.disabled, true);
  assert.equal(state.title, "");
});

test("deriveTaskResumeActionState keeps waiting title when states overlap", () => {
  const state = deriveTaskResumeActionState({
    executingTask: true,
    resumingNeedInput: true,
    pendingNeedInput: { runId: 11 }
  });
  assert.equal(state.disabled, true);
  assert.equal(state.title, UI_TEXT.TASK_RESUME_DISABLED_WAITING);
});

import test from "node:test";
import assert from "node:assert/strict";

import {
  buildNeedInputFingerprint,
  markNeedInputPromptHandled,
  pruneNeedInputRecentRecords,
  renderNeedInputQuestionThenChoices,
  resolvePendingResumeFromRunDetail,
  shouldSuppressNeedInputPrompt
} from "../src/renderer/need_input.js";

test("buildNeedInputFingerprint uses run + token/session/question", () => {
  const withToken = buildNeedInputFingerprint({
    run_id: 12,
    kind: "task_feedback",
    prompt_token: "tok-1",
    question: "ignored when token exists"
  });
  assert.equal(withToken, "12:task_feedback:tok-1");

  const withSession = buildNeedInputFingerprint({
    runId: 9,
    kind: "user_prompt",
    sessionKey: "sess_a"
  });
  assert.equal(withSession, "9:user_prompt:sess_a");

  const withQuestion = buildNeedInputFingerprint({
    run_id: 7,
    question: "请确认是否继续"
  });
  assert.equal(withQuestion, "7:-:请确认是否继续");
});

test("markNeedInputPromptHandled + shouldSuppressNeedInputPrompt obey ttl", () => {
  const now = 1_000_000;
  const payload = {
    run_id: 3,
    kind: "task_feedback",
    prompt_token: "pt_3",
    question: "满意吗"
  };
  const records = markNeedInputPromptHandled(payload, [], { nowMs: now, ttlMs: 20_000 });
  assert.equal(records.length, 1);
  assert.equal(
    shouldSuppressNeedInputPrompt(payload, records, { nowMs: now + 1_000, ttlMs: 20_000 }),
    true
  );
  assert.equal(
    shouldSuppressNeedInputPrompt(payload, records, { nowMs: now + 21_000, ttlMs: 20_000 }),
    false
  );
});

test("pruneNeedInputRecentRecords keeps recent unique entries only", () => {
  const now = 2_000_000;
  const rows = [
    { runId: 1, fingerprint: "a", at: now - 1_000 },
    { runId: 1, fingerprint: "a", at: now - 1_000 }, // duplicate
    { runId: 2, fingerprint: "b", at: now - 50_000 }, // expired
    { runId: 3, fingerprint: "c", at: now - 500 }
  ];
  const out = pruneNeedInputRecentRecords(rows, { nowMs: now, ttlMs: 20_000 });
  assert.deepEqual(out.map((it) => it.fingerprint), ["a", "c"]);
});

test("renderNeedInputQuestionThenChoices renders question before choices", () => {
  const calls = [];
  renderNeedInputQuestionThenChoices({
    question: "请补充信息",
    payload: { kind: "user_prompt" },
    renderQuestion: (text) => calls.push(`q:${text}`),
    renderChoices: (payload) => calls.push(`c:${payload?.kind || ""}`),
  });
  assert.deepEqual(calls, ["q:请补充信息", "c:user_prompt"]);
});

test("resolvePendingResumeFromRunDetail builds pending for waiting run even without question", () => {
  const resolved = resolvePendingResumeFromRunDetail(
    23,
    {
      run: { status: "waiting", task_id: 9 },
      agent_state: {
        paused: {
          kind: "user_prompt",
          choices: [{ label: "A", value: "A" }],
          prompt_token: "pt_23",
        },
        session_key: "sess_23"
      }
    }
  );

  assert.equal(resolved.waiting, true);
  assert.equal(resolved.status, "waiting");
  assert.equal(resolved.question, "需要你补充信息后才能继续执行。");
  assert.ok(resolved.pending);
  assert.equal(resolved.pending.runId, 23);
  assert.equal(resolved.pending.taskId, 9);
  assert.equal(resolved.pending.promptToken, "pt_23");
  assert.equal(resolved.pending.sessionKey, "sess_23");
  assert.equal(resolved.pending.question, "需要你补充信息后才能继续执行。");
});

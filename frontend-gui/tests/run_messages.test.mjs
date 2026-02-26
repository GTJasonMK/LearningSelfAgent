import test from "node:test";
import assert from "node:assert/strict";

import {
  buildRunSyncingHint,
  buildTaskFeedbackAckText,
  extractRunLastError
} from "../src/renderer/run_messages.js";

test("buildRunSyncingHint includes run id and status label", () => {
  const text = buildRunSyncingHint({ run_id: 7, status: "failed" });
  assert.equal(text, "任务状态同步中：run#7（失败）。请稍后查看执行结果。");
});

test("buildTaskFeedbackAckText reflects failed terminal state", () => {
  const text = buildTaskFeedbackAckText("failed", 11);
  assert.equal(text, "已记录反馈，但任务执行失败（run#11）。");
});

test("extractRunLastError reads from run detail snapshot counters", () => {
  const detail = {
    snapshot: {
      counters: {
        last_error: {
          step_order: 5,
          title: "写入文件",
          error: "Permission denied"
        }
      }
    }
  };
  assert.deepEqual(extractRunLastError(detail), {
    stepOrder: 5,
    title: "写入文件",
    error: "Permission denied"
  });
});

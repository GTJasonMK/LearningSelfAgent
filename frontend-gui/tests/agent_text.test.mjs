import test from "node:test";
import assert from "node:assert/strict";

import {
  buildNoVisibleResultText,
  extractVisibleResultText,
  normalizeThinkingStatus,
} from "../src/renderer/agent_text.js";

test("normalizeThinkingStatus suppresses numbered plan rows", () => {
  assert.equal(normalizeThinkingStatus("1. 先收集信息"), "");
  assert.equal(normalizeThinkingStatus("2. 再执行步骤"), "");
});

test("extractVisibleResultText ignores pure status/plan transcripts without result tag", () => {
  const transcript = [
    "【规划】开始拆解任务",
    "1. 收集信息",
    "2. 执行操作",
    "3. 汇总结果",
  ].join("\n");
  assert.equal(extractVisibleResultText(transcript), "");
});

test("extractVisibleResultText keeps plain fallback text when available", () => {
  const transcript = [
    "【执行】开始执行",
    "工具执行完成",
    "文件已生成在 output/result.json",
  ].join("\n");
  assert.equal(extractVisibleResultText(transcript), "文件已生成在 output/result.json");
});

test("buildNoVisibleResultText keeps compatibility without debug options", () => {
  assert.equal(buildNoVisibleResultText("failed"), "任务执行失败，未产出可展示结果。");
});

test("buildNoVisibleResultText appends run and error context when provided", () => {
  const text = buildNoVisibleResultText("failed", {
    runId: 42,
    lastError: { stepOrder: 3, title: "调用模型", error: "401 无效 API Key" }
  });
  assert.equal(text, "任务执行失败，未产出可展示结果。（run#42 | step#3 | 调用模型 | 401 无效 API Key）");
});

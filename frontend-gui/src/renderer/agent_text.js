import { PET_STREAM_RESULT_TAG } from "./constants.js";

export function consumeCompletedSentences(text) {
  const endChars = new Set(["。", "！", "？", "!", "?", "."]);
  const sentences = [];
  let last = 0;
  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (!endChars.has(ch)) continue;
    const seg = text.slice(last, i + 1);
    if (seg.trim()) sentences.push(seg);
    last = i + 1;
  }
  return { sentences, rest: text.slice(last) };
}

export function parseSlashCommand(text) {
  const trimmed = String(text || "").trim();
  if (!trimmed.startsWith("/")) return null;
  const body = trimmed.slice(1).trim();
  if (!body) return { cmd: "help", args: "" };
  const firstSpace = body.search(/\s/);
  if (firstSpace === -1) return { cmd: body.toLowerCase(), args: "" };
  const cmd = body.slice(0, firstSpace).toLowerCase();
  const args = body.slice(firstSpace + 1).trim();
  return { cmd, args };
}

export function extractFinalResultText(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";
  const idx = text.lastIndexOf(PET_STREAM_RESULT_TAG);
  if (idx === -1) return text;
  return text.slice(idx).trim();
}

export function normalizeResultText(text) {
  return String(text || "").replace(/^【结果】/g, "").trim();
}

// 仅提取“【结果】”所在段落（用于任务/行动类指令的最终可见输出）。
// 与 extractFinalResultText 的区别：
// - 若没有结果标签，则返回空字符串（避免把“规划/执行状态”当作最终回答落库/展示）。
export function extractResultPayloadText(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";
  const idx = text.lastIndexOf(PET_STREAM_RESULT_TAG);
  if (idx === -1) return "";
  return text.slice(idx).trim();
}

function extractFallbackVisibleTextWithoutResultTag(rawText) {
  const text = String(rawText || "").trim();
  if (!text) return "";
  const lines = text
    .split(/\r?\n/)
    .map((line) => String(line || "").trim())
    .filter(Boolean);
  if (!lines.length) return "";

  const statusPrefixPattern = /^【(?:技能|记忆|步骤|规划|执行|任务|完成|失败|跳过|思考|规划者|检索|工具|评估|领域|图谱|方案|知识)】/;
  const filtered = lines.filter((line) => !statusPrefixPattern.test(line) && !/^\d+\.\s+/.test(line));
  if (!filtered.length) return "";
  const tail = filtered;
  const last = String(tail[tail.length - 1] || "").trim();
  if (!last) return "";
  if (last === "…" || last === "...") return "";
  return normalizeResultText(last);
}

export function extractVisibleResultText(rawText) {
  const payload = extractResultPayloadText(rawText);
  const tagged = normalizeResultText(payload);
  if (tagged) return tagged;
  return extractFallbackVisibleTextWithoutResultTag(rawText);
}

function buildLastErrorDebugText(lastError) {
  if (!lastError || typeof lastError !== "object") return "";
  const stepOrder = Number(lastError.stepOrder ?? lastError.step_order);
  const title = String(lastError.title || "").trim();
  const error = String(lastError.error || lastError.message || "").trim();
  const parts = [];
  if (Number.isFinite(stepOrder) && stepOrder > 0) parts.push(`step#${stepOrder}`);
  if (title) parts.push(title);
  if (error) parts.push(error);
  return parts.join(" | ");
}

export function buildNoVisibleResultText(status, options = {}) {
  const state = String(status || "").trim().toLowerCase();
  let base = "任务已结束，但未产出可展示结果。";
  if (state === "failed") base = "任务执行失败，未产出可展示结果。";
  else if (state === "stopped" || state === "cancelled") base = "任务已中止，未产出可展示结果。";
  else if (state === "waiting") base = "任务等待补充信息，尚未产出可展示结果。";

  const runId = Number(options?.runId);
  const debugParts = [];
  if (Number.isFinite(runId) && runId > 0) debugParts.push(`run#${runId}`);
  const lastErrorText = buildLastErrorDebugText(options?.lastError);
  if (lastErrorText) debugParts.push(lastErrorText);
  if (!debugParts.length) return base;
  return `${base}（${debugParts.join(" | ")}）`;
}

export function normalizeThinkingStatus(line) {
  const raw = String(line || "").trim();
  if (!raw) return "";
  if (raw.startsWith("【思考】")) {
    const rest = raw.replace("【思考】", "").trim();
    return rest ? `思考：${rest}` : "思考中…";
  }
  if (raw.startsWith("【规划者】")) {
    const rest = raw.replace("【规划者】", "").trim();
    return rest ? `规划者：${rest}` : "规划者处理中…";
  }
  if (raw.startsWith("【技能】")) {
    const rest = raw.replace("【技能】", "").trim();
    return rest ? `技能：${rest}` : "检索技能…";
  }
  if (raw.startsWith("【领域】")) {
    const rest = raw.replace("【领域】", "").trim();
    return rest ? `领域：${rest}` : "选择领域…";
  }
  if (raw.startsWith("【图谱】")) {
    const rest = raw.replace("【图谱】", "").trim();
    return rest ? `图谱：${rest}` : "检索图谱…";
  }
  if (raw.startsWith("【记忆】")) {
    const rest = raw.replace("【记忆】", "").trim();
    return rest ? `记忆：${rest}` : "检索记忆…";
  }
  if (raw.startsWith("【方案】")) {
    const rest = raw.replace("【方案】", "").trim();
    return rest ? `方案：${rest}` : "检索方案…";
  }
  if (raw.startsWith("【知识】")) {
    const rest = raw.replace("【知识】", "").trim();
    return rest ? `知识：${rest}` : "整合知识…";
  }
  if (raw.startsWith("【检索】")) {
    const rest = raw.replace("【检索】", "").trim();
    return rest ? `检索：${rest}` : "检索中…";
  }
  if (raw.startsWith("【步骤】")) {
    const title = raw.replace("【步骤】", "").trim();
    return title ? `正在执行：${title}` : "正在执行…";
  }
  if (raw.startsWith("【规划】")) {
    const rest = raw.replace("【规划】", "").trim();
    return rest ? `正在规划：${rest}` : "正在规划…";
  }
  if (raw.startsWith("【执行】")) {
    const rest = raw.replace("【执行】", "").trim();
    return rest ? `执行中：${rest}` : "执行中…";
  }
  if (raw.startsWith("【工具】")) {
    const rest = raw.replace("【工具】", "").trim();
    return rest ? `工具：${rest}` : "调用工具中…";
  }
  if (raw.startsWith("【评估】")) {
    const rest = raw.replace("【评估】", "").trim();
    return rest ? `评估：${rest}` : "评估中…";
  }
  if (raw.startsWith("【任务】")) {
    const rest = raw.replace("【任务】", "").trim();
    return rest ? `任务：${rest}` : "任务处理中…";
  }
  if (raw.startsWith("【完成】")) {
    const rest = raw.replace("【完成】", "").trim();
    return rest ? `完成：${rest}` : "完成";
  }
  if (raw.startsWith("【失败】")) {
    const rest = raw.replace("【失败】", "").trim();
    return rest ? `失败：${rest}` : "失败";
  }
  if (raw.startsWith("【跳过】")) {
    const rest = raw.replace("【跳过】", "").trim();
    return rest ? `跳过：${rest}` : "跳过";
  }
  if (raw.startsWith(PET_STREAM_RESULT_TAG)) {
    return "已生成结果，正在整理…";
  }
  // 过滤掉计划列表的序号行（避免刷屏）
  if (/^\d+\.\s+/.test(raw)) return "";
  // 思考模式只展示“状态”，不展示普通内容行（避免长输出撑爆气泡）
  return "";
}

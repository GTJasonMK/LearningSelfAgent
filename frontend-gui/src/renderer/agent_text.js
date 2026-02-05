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

export function normalizeThinkingStatus(line) {
  const raw = String(line || "").trim();
  if (!raw) return "";
  if (raw.startsWith("【技能】")) {
    const rest = raw.replace("【技能】", "").trim();
    return rest ? `技能：${rest}` : "检索技能…";
  }
  if (raw.startsWith("【记忆】")) {
    const rest = raw.replace("【记忆】", "").trim();
    return rest ? `记忆：${rest}` : "检索记忆…";
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
  if (/^\\d+\\.\\s+/.test(raw)) return "";
  // 思考模式只展示“状态”，不展示普通内容行（避免长输出撑爆气泡）
  return "";
}

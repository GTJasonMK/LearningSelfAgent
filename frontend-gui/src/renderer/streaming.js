// 通用 SSE 流式读取器（桌宠/世界页共用）
//
// 目标：
// - 消除 pet.js 与 panel.js 中高度重复的 SSE 解析/节流/逐句渲染逻辑；
// - 保持“看起来像真流式”：合并事件时也能逐帧刷新（requestAnimationFrame 让出主线程）；
// - 兼容 Agent 执行链路的结构化事件：run_created / need_input / plan / review / done / error。

import { consumeCompletedSentences, normalizeThinkingStatus } from "./agent_text.js";

function parseSseEventBlock(rawEvent) {
  let eventName = "message";
  const dataLines = [];
  for (const line of String(rawEvent || "").split("\n")) {
    if (line.startsWith("event:")) eventName = line.slice(6).trim();
    if (line.startsWith("data:")) dataLines.push(line.slice(5).trimStart());
  }
  return { eventName, dataStr: dataLines.join("\n") };
}

function extractErrorMessage(dataStr, fallback) {
  let msg = String(dataStr || "").trim() || String(fallback || "").trim();
  try {
    const obj = JSON.parse(String(dataStr || ""));
    msg = obj?.message || obj?.error?.message || msg;
  } catch (e) {}
  return msg || String(fallback || "");
}

/**
 * 读取 text/event-stream 并按 displayMode 流式更新 UI。
 *
 * @param {(signal: AbortSignal)=>Promise<Response>} makeRequest
 * @param {object} options
 * @param {AbortSignal} options.signal
 * @param {"full"|"status"} [options.displayMode]
 * @param {(text:string)=>void} [options.onUpdate]      - 用于 full/status 两种模式的“可见文本更新”
 * @param {(msg:string)=>void} [options.onError]        - 错误展示
 * @param {(obj:any)=>void}    [options.onRunCreated]
 * @param {(obj:any)=>void}    [options.onNeedInput]
 * @param {(obj:any)=>void}    [options.onPlan]
 * @param {(obj:any)=>void}    [options.onPlanDelta]
 * @param {(obj:any)=>void}    [options.onEvent]       - 结构化事件回调（例如 memory_item/skills 等），用于事件驱动更新
 * @param {(obj:any)=>string}  [options.onReviewDelta]  - 将 review 事件转换为 delta 文本（不想显示则返回 ""）
 * @param {()=>boolean}        [options.shouldPauseUpdates] - 例如进入 need_input 后，避免后续 delta 覆盖提问气泡
 * @param {number} [options.uiMinInterval]              - UI 节流间隔（ms）
 * @param {number} [options.yieldMinInterval]           - 让出渲染的最小间隔（ms）
 */
export async function streamSse(makeRequest, options = {}) {
  const signal = options.signal;
  const displayMode = String(options.displayMode || "full").trim().toLowerCase();
  const uiMinInterval = Number.isFinite(options.uiMinInterval) ? Number(options.uiMinInterval) : 80;
  const yieldMinInterval = Number.isFinite(options.yieldMinInterval) ? Number(options.yieldMinInterval) : 40;

  let transcript = "";
  let completed = "";
  let pending = "";
  let streamDone = false;
  let hadError = false;

  let statusLine = "";
  let statusDots = "";

  let lastUiAt = 0;
  let lastYieldAt = 0;

  function statusTextNow() {
    const base = String(statusLine || "").trim();
    if (!base) return "";
    const normalized = base.replace(/…+$/g, "");
    return `${normalized}${statusDots}`;
  }

  function canUpdateUi(force) {
    if (typeof options.shouldPauseUpdates === "function" && options.shouldPauseUpdates()) return false;
    if (typeof options.onUpdate !== "function") return false;
    const now = performance.now();
    if (!force && now - lastUiAt < uiMinInterval) return false;
    lastUiAt = now;
    return true;
  }

  async function maybeYieldToPaint(force) {
    const now = performance.now();
    if (!force && now - lastYieldAt < yieldMinInterval) return;
    lastYieldAt = now;
    await new Promise((resolve) => requestAnimationFrame(resolve));
  }

  function updateUi(text, force) {
    if (!canUpdateUi(force)) return;
    const value = String(text || "").trim();
    if (!value) return;
    try {
      options.onUpdate(value);
    } catch (e) {}
  }

  try {
    const response = await makeRequest(signal);
    if (!response?.ok) {
      let msg = `请求失败: ${response?.status || 0}`;
      try {
        const payload = await response.json();
        msg = payload?.error?.message || msg;
      } catch (e) {}
      if (typeof options.onError === "function") options.onError(msg);
      return { transcript: "", hadError: true };
    }

    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("text/event-stream") || !response.body) {
      if (typeof options.onError === "function") options.onError("返回格式错误：非流式输出");
      return { transcript: "", hadError: true };
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (!streamDone) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let idx;
      while ((idx = buffer.indexOf("\n\n")) >= 0) {
        const rawEvent = buffer.slice(0, idx);
        buffer = buffer.slice(idx + 2);

        const { eventName, dataStr } = parseSseEventBlock(rawEvent);
        if (eventName === "done") {
          streamDone = true;
          break;
        }
        if (eventName === "error") {
          const errMsg = extractErrorMessage(dataStr, "流式输出失败");
          if (typeof options.onError === "function") options.onError(errMsg);
          hadError = true;
          streamDone = true;
          break;
        }

        let obj = null;
        try {
          obj = JSON.parse(dataStr);
        } catch (e) {
          obj = null;
        }

        // 结构化事件：优先处理并决定是否继续产生 delta
        if (obj && typeof obj === "object") {
          // 通用事件回调：让调用方按需订阅（例如 memory_item 触发 UI 更新）
          if (typeof obj.type === "string") {
            try { options.onEvent?.(obj); } catch (e) {}
          }
          if (obj.type === "done") {
            streamDone = true;
            break;
          }
          if (obj.type === "run_created") {
            try { options.onRunCreated?.(obj); } catch (e) {}
            continue;
          }
          if (obj.type === "need_input") {
            try { options.onNeedInput?.(obj); } catch (e) {}
            await maybeYieldToPaint(true);
            continue;
          }
          if (obj.type === "plan" && Array.isArray(obj.items)) {
            try { options.onPlan?.(obj); } catch (e) {}
            continue;
          }
          if (obj.type === "plan_delta" && Array.isArray(obj.changes)) {
            try { options.onPlanDelta?.(obj); } catch (e) {}
            continue;
          }
        }

        // delta 文本：用于 transcript 与 UI 流式显示
        let delta = "";
        if (obj && typeof obj === "object") {
          if (obj.type === "review") {
            try {
              delta = typeof options.onReviewDelta === "function" ? String(options.onReviewDelta(obj) || "") : "";
            } catch (e) {
              delta = "";
            }
          } else if (typeof obj.delta === "string") {
            delta = obj.delta;
          } else {
            delta = "";
          }
        } else {
          delta = dataStr;
        }

        if (!delta) continue;
        transcript += delta;

        if (displayMode === "status") {
          const chunks = String(delta || "").split("\n");
          for (const rawLine of chunks) {
            const line = String(rawLine || "").trim();
            if (!line) continue;
            if (line === "…" || line === "...") {
              statusDots = statusDots.length >= 3 ? "" : `${statusDots}…`;
              continue;
            }
            const normalized = normalizeThinkingStatus(line);
            if (!normalized) continue;
            if (normalized !== statusLine) {
              statusLine = normalized;
              statusDots = "";
            }
          }
          updateUi(statusTextNow(), true);
          await maybeYieldToPaint(true);
          continue;
        }

        pending += delta;
        const { sentences, rest } = consumeCompletedSentences(pending);
        if (sentences.length) {
          completed += sentences.join("");
          pending = rest;
        }
        updateUi((completed + pending).trim(), sentences.length > 0);
        await maybeYieldToPaint(sentences.length > 0);
      }
    }
  } catch (e) {
    if (String(e?.name || "") !== "AbortError") {
      hadError = true;
      if (typeof options.onError === "function") {
        options.onError("对话中断：网络/后端异常");
      }
    }
  }

  if (hadError) return { transcript: "", hadError: true };

  const finalText =
    displayMode === "status" ? transcript.trim() : (completed + pending).trim();
  return { transcript: finalText, hadError: false };
}

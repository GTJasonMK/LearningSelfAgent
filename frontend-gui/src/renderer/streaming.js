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
  const normalized = String(rawEvent || "").replace(/\r/g, "");
  for (const line of normalized.split("\n")) {
    if (line.startsWith(":")) continue;
    if (line.startsWith("event:")) eventName = line.slice(6).trim();
    if (line.startsWith("data:")) {
      let data = line.slice(5);
      // SSE 规范允许 data: 后跟一个可选空格；只移除一个，避免吞掉代码缩进。
      if (data.startsWith(" ")) data = data.slice(1);
      dataLines.push(data);
    }
  }
  return { eventName, dataStr: dataLines.join("\n") };
}

function consumeNextSseEventBlock(buffer) {
  const text = String(buffer || "");
  if (!text) return null;
  const match = /\r?\n\r?\n/.exec(text);
  if (!match) return null;
  const idx = Number(match.index);
  const sep = String(match[0] || "\n\n");
  return {
    rawEvent: text.slice(0, idx),
    rest: text.slice(idx + sep.length),
  };
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
 * @param {(obj:any)=>void}    [options.onRunStatus]
 * @param {(obj:any)=>void}    [options.onNeedInput]
 * @param {(obj:any)=>void}    [options.onPlan]
 * @param {(obj:any)=>void}    [options.onPlanDelta]
 * @param {(obj:any)=>void}    [options.onEvent]       - 结构化事件回调（例如 memory_item/skills 等），用于事件驱动更新
 * @param {(obj:any)=>string}  [options.onReviewDelta]  - 将 review 事件转换为 delta 文本（不想显示则返回 ""）
 * @param {(runId:number, afterEventId:string|null, signal:AbortSignal)=>Promise<any>} [options.replayFetch]
 * @param {()=>number|null}    [options.getReplayRunId]
 * @param {number}             [options.replayBatchLimit]
 * @param {number}             [options.replayMaxBatches]
 * @param {()=>boolean}        [options.shouldPauseUpdates] - 例如进入 need_input 后，避免后续 delta 覆盖提问气泡
 * @param {number} [options.uiMinInterval]              - UI 节流间隔（ms）
 * @param {number} [options.yieldMinInterval]           - 让出渲染的最小间隔（ms）
 */
export async function streamSse(makeRequest, options = {}) {
  const signal = options.signal;
  const displayMode = String(options.displayMode || "full").trim().toLowerCase();
  const uiMinInterval = Number.isFinite(options.uiMinInterval) ? Number(options.uiMinInterval) : 80;
  const yieldMinInterval = Number.isFinite(options.yieldMinInterval) ? Number(options.yieldMinInterval) : 40;
  const replayBatchLimit = Number.isFinite(options.replayBatchLimit)
    ? Math.max(1, Math.min(500, Number(options.replayBatchLimit)))
    : 200;
  const replayMaxBatches = Number.isFinite(options.replayMaxBatches)
    ? Math.max(1, Math.min(20, Number(options.replayMaxBatches)))
    : 4;

  let transcript = "";
  let completed = "";
  let pending = "";
  let streamDone = false;
  let streamCompletedByDoneEvent = false;
  let sawBusinessStateEvent = false;
  let hadError = false;
  let wasAborted = false;
  let deferredErrorMessage = "";
  let replayApplied = 0;

  let statusLine = "";
  let statusDots = "";
  const seenEventIds = new Set();
  const seenEventIdQueue = [];
  const seenStructuralKeys = new Set();
  const seenStructuralQueue = [];
  const lastRunStatusByRun = new Map();
  const lastNeedInputByRun = new Map();
  let lastSeenEventId = "";
  let lastKnownRunId = null;
  let lastKnownTaskId = null;

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

  function buildStructuralDedupKey(obj) {
    if (!obj || typeof obj !== "object") return "";
    const type = String(obj.type || "").trim();
    if (!type) return "";
    const runId = Number(obj.run_id || obj.runId);
    const taskId = Number(obj.task_id || obj.taskId);
    const runPart = Number.isFinite(runId) && runId > 0 ? String(runId) : "0";
    const taskPart = Number.isFinite(taskId) && taskId > 0 ? String(taskId) : "0";

    if (type === "run_created") return `k:${type}:${taskPart}:${runPart}`;
    if (type === "done" || type === "stream_end") return `k:${type}:${taskPart}:${runPart}`;
    return "";
  }

  function rememberStructuralKey(key) {
    const k = String(key || "").trim();
    if (!k) return;
    if (seenStructuralKeys.has(k)) return;
    seenStructuralKeys.add(k);
    seenStructuralQueue.push(k);
    if (seenStructuralQueue.length > 2000) {
      const removeCount = seenStructuralQueue.length - 2000;
      for (let i = 0; i < removeCount; i += 1) {
        const removed = seenStructuralQueue.shift();
        if (removed) seenStructuralKeys.delete(removed);
      }
    }
  }

  function shouldProcessRunStatus(obj) {
    const runId = Number(obj?.run_id || obj?.runId);
    const taskId = Number(obj?.task_id || obj?.taskId);
    const status = String(obj?.status || "").trim().toLowerCase();
    if (!status) return true;
    const runPart = Number.isFinite(runId) && runId > 0 ? String(runId) : "0";
    const taskPart = Number.isFinite(taskId) && taskId > 0 ? String(taskId) : "0";
    const runKey = `${taskPart}:${runPart}`;
    const previous = String(lastRunStatusByRun.get(runKey) || "").trim().toLowerCase();
    if (previous && previous === status) return false;
    lastRunStatusByRun.set(runKey, status);
    return true;
  }

  function shouldProcessNeedInput(obj) {
    const runId = Number(obj?.run_id || obj?.runId);
    const taskId = Number(obj?.task_id || obj?.taskId);
    const runPart = Number.isFinite(runId) && runId > 0 ? String(runId) : "0";
    const taskPart = Number.isFinite(taskId) && taskId > 0 ? String(taskId) : "0";
    const runKey = `${taskPart}:${runPart}`;
    const kind = String(obj?.kind || obj?.payload?.kind || "").trim();
    const promptToken = String(obj?.prompt_token || obj?.promptToken || obj?.payload?.prompt_token || "").trim();
    const question = String(obj?.question || obj?.payload?.question || "").trim();
    const marker = promptToken || question;
    if (!marker) return true;
    const next = `${kind}:${marker}`;
    const prev = String(lastNeedInputByRun.get(runKey) || "");
    if (prev && prev === next) return false;
    lastNeedInputByRun.set(runKey, next);
    return true;
  }

  function shouldProcessEvent(obj) {
    if (!obj || typeof obj !== "object") return true;
    const eventId = String(obj.event_id || obj.eventId || "").trim();
    if (eventId) {
      const idKey = `id:${eventId}`;
      if (seenEventIds.has(idKey)) return false;
      seenEventIds.add(idKey);
      seenEventIdQueue.push(idKey);
      if (seenEventIdQueue.length > 2000) {
        const removeCount = seenEventIdQueue.length - 2000;
        for (let i = 0; i < removeCount; i += 1) {
          const removed = seenEventIdQueue.shift();
          if (removed) seenEventIds.delete(removed);
        }
      }
    }

    const type = String(obj.type || "").trim();
    if (type === "run_status") {
      return shouldProcessRunStatus(obj);
    }
    if (type === "need_input") {
      return shouldProcessNeedInput(obj);
    }

    const structuralKey = buildStructuralDedupKey(obj);
    if (structuralKey) {
      if (seenStructuralKeys.has(structuralKey)) return false;
      rememberStructuralKey(structuralKey);
    }
    return true;
  }

  function rememberRunTask(obj) {
    if (!obj || typeof obj !== "object") return;
    const runId = Number(obj.run_id || obj.runId);
    const taskId = Number(obj.task_id || obj.taskId);
    if (Number.isFinite(runId) && runId > 0) lastKnownRunId = runId;
    if (Number.isFinite(taskId) && taskId > 0) lastKnownTaskId = taskId;
  }

  function rememberEventId(obj) {
    if (!obj || typeof obj !== "object") return;
    const eventId = String(obj.event_id || obj.eventId || "").trim();
    if (eventId) lastSeenEventId = eventId;
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

  async function applyDelta(delta) {
    const text = String(delta || "");
    if (!text) return;
    transcript += text;

    if (displayMode === "status") {
      const chunks = text.split("\n");
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
      return;
    }

    pending += text;
    const { sentences, rest } = consumeCompletedSentences(pending);
    if (sentences.length) {
      completed += sentences.join("");
      pending = rest;
    }
    updateUi((completed + pending).trim(), sentences.length > 0);
    await maybeYieldToPaint(sentences.length > 0);
  }

  async function processStructuredObject(obj) {
    if (!obj || typeof obj !== "object") return { consumed: false, delta: "" };
    rememberRunTask(obj);
    if (!shouldProcessEvent(obj)) return { consumed: true, delta: "", skipped: true };
    rememberEventId(obj);

    if (typeof obj.type === "string") {
      try { options.onEvent?.(obj); } catch (e) {}
    }

    if (obj.type === "done" || obj.type === "stream_end") {
      if (String(obj.run_status || "").trim()) sawBusinessStateEvent = true;
      streamDone = true;
      streamCompletedByDoneEvent = true;
      return { consumed: true, delta: "", stop: true };
    }
    if (obj.type === "run_created") {
      try { options.onRunCreated?.(obj); } catch (e) {}
      return { consumed: true, delta: "" };
    }
    if (obj.type === "run_status") {
      sawBusinessStateEvent = true;
      try { options.onRunStatus?.(obj); } catch (e) {}
      return { consumed: true, delta: "" };
    }
    if (obj.type === "need_input") {
      sawBusinessStateEvent = true;
      try { options.onNeedInput?.(obj); } catch (e) {}
      await maybeYieldToPaint(true);
      return { consumed: true, delta: "" };
    }
    if (obj.type === "plan" && Array.isArray(obj.items)) {
      try { options.onPlan?.(obj); } catch (e) {}
      return { consumed: true, delta: "" };
    }
    if (obj.type === "plan_delta" && Array.isArray(obj.changes)) {
      try { options.onPlanDelta?.(obj); } catch (e) {}
      return { consumed: true, delta: "" };
    }
    if (obj.type === "review") {
      try {
        const delta = typeof options.onReviewDelta === "function" ? String(options.onReviewDelta(obj) || "") : "";
        return { consumed: true, delta };
      } catch (e) {
        return { consumed: true, delta: "" };
      }
    }
    if (typeof obj.delta === "string") {
      return { consumed: false, delta: obj.delta };
    }
    return { consumed: false, delta: "" };
  }

  async function replayEventsIfNeeded(reason) {
    if (wasAborted) return { applied: 0, error: "" };
    if (typeof options.replayFetch !== "function") return { applied: 0, error: "" };

    const hintedRunId = Number(typeof options.getReplayRunId === "function" ? options.getReplayRunId() : 0);
    const runId = Number.isFinite(hintedRunId) && hintedRunId > 0
      ? hintedRunId
      : (Number.isFinite(lastKnownRunId) && Number(lastKnownRunId) > 0 ? Number(lastKnownRunId) : 0);
    if (!Number.isFinite(runId) || runId <= 0) return { applied: 0, error: "" };

    let cursor = String(lastSeenEventId || "").trim() || null;
    let applied = 0;
    let errorMessage = "";

    for (let round = 0; round < replayMaxBatches; round += 1) {
      if (signal?.aborted) break;
      let response = null;
      try {
        response = await options.replayFetch(runId, cursor, signal);
      } catch (err) {
        errorMessage = String(err?.message || err || "事件回放失败");
        break;
      }
      const items = Array.isArray(response?.items) ? response.items : [];
      if (!items.length) break;

      let progressedInRound = false;
      for (const item of items) {
        const obj = item && typeof item === "object" ? item.payload : null;
        if (!obj || typeof obj !== "object") continue;
        const result = await processStructuredObject(obj);
        if (result?.delta) await applyDelta(result.delta);
        if (!result?.skipped) applied += 1;
        const rowEventId = String(item?.event_id || "").trim();
        const objEventId = String(obj?.event_id || obj?.eventId || "").trim();
        const nextCursor = objEventId || rowEventId;
        if (nextCursor && nextCursor !== cursor) {
          cursor = nextCursor;
          progressedInRound = true;
        }
        if (result?.stop) {
          streamDone = true;
          streamCompletedByDoneEvent = true;
          break;
        }
      }
      if (streamDone || streamCompletedByDoneEvent) break;
      if (!progressedInRound && items.length >= replayBatchLimit) break;
      if (items.length < replayBatchLimit) break;
    }

    if (applied > 0) {
      try {
        options.onEvent?.({ type: "replay_applied", reason, run_id: runId, applied });
      } catch (e) {}
    }
    return { applied, error: errorMessage };
  }

  let reader = null;
  try {
    const response = await makeRequest(signal);
    if (!response?.ok) {
      let msg = `请求失败: ${response?.status || 0}`;
      try {
        const payload = await response.json();
        msg = payload?.error?.message || msg;
      } catch (e) {}
      if (typeof options.onError === "function") options.onError(msg);
      return { transcript: "", hadError: true, wasAborted: false, replayApplied: 0 };
    }

    const contentType = response.headers.get("content-type") || "";
    if (!contentType.includes("text/event-stream") || !response.body) {
      if (typeof options.onError === "function") options.onError("返回格式错误：非流式输出");
      return { transcript: "", hadError: true, wasAborted: false, replayApplied: 0 };
    }

    reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (!streamDone) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      let consumed;
      while ((consumed = consumeNextSseEventBlock(buffer))) {
        const rawEvent = consumed.rawEvent;
        buffer = consumed.rest;

        const { eventName, dataStr } = parseSseEventBlock(rawEvent);
        if (eventName === "done") {
          streamDone = true;
          streamCompletedByDoneEvent = true;
          break;
        }
        if (eventName === "error") {
          deferredErrorMessage = extractErrorMessage(dataStr, "流式输出失败");
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

        let delta = "";
        if (obj && typeof obj === "object") {
          const result = await processStructuredObject(obj);
          if (result?.stop) break;
          delta = String(result?.delta || "");
          if (result?.consumed && !delta) continue;
        } else {
          delta = String(dataStr || "");
        }
        if (!delta) continue;
        await applyDelta(delta);
      }
    }
  } catch (e) {
    if (String(e?.name || "") === "AbortError") {
      // 主动中断（页面切换/新请求抢占）不应继续走“正常完成”分支，
      // 否则上层会把空 transcript 误判为“任务结束但无结果”。
      hadError = true;
      wasAborted = true;
    } else {
      hadError = true;
      deferredErrorMessage = "对话中断：网络/后端异常";
    }
  } finally {
    // 确保 reader 被释放，避免 ReadableStream 和 TextDecoder 内部缓冲区泄漏
    if (reader) {
      try { reader.cancel(); } catch (e) {}
      reader = null;
    }
  }

  const needReplay = !wasAborted
    && (
      hadError
      || !streamCompletedByDoneEvent
      // 仅收到 done 但未看到 run_status/need_input 时，不足以判定业务链路已收敛。
      || (streamCompletedByDoneEvent && !sawBusinessStateEvent)
    );
  if (needReplay) {
    const replay = await replayEventsIfNeeded(hadError ? "stream_error" : "stream_closed");
    replayApplied = Number(replay?.applied || 0);
    if (replayApplied > 0) hadError = false;
    if (!deferredErrorMessage && replay?.error) deferredErrorMessage = String(replay.error || "");
  }

  if (hadError) {
    if (!wasAborted && typeof options.onError === "function") {
      options.onError(deferredErrorMessage || "对话中断：网络/后端异常");
    }
    return { transcript: "", hadError: true, wasAborted, replayApplied };
  }

  const finalText =
    displayMode === "status" ? transcript.trim() : (completed + pending).trim();
  return { transcript: finalText, hadError: false, wasAborted: false, replayApplied };
}

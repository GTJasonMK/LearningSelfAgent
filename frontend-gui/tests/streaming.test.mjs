import test from "node:test";
import assert from "node:assert/strict";

import { streamSse } from "../src/renderer/streaming.js";

function buildSseResponse(chunks) {
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(String(chunk || "")));
      }
      controller.close();
    },
  });
  return new Response(stream, {
    status: 200,
    headers: { "content-type": "text/event-stream; charset=utf-8" },
  });
}

test("streamSse supports CRLF event separators", async () => {
  const response = buildSseResponse([
    "data: {\"delta\":\"第一段\"}\r\n\r\n",
    "data: {\"delta\":\"第二段\"}\r\n\r\n",
    "event: done\r\ndata: {\"type\":\"done\"}\r\n\r\n",
  ]);
  const updates = [];
  const previousRaf = globalThis.requestAnimationFrame;
  globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0);
  try {
    const result = await streamSse(
      async () => response,
      {
        displayMode: "full",
        onUpdate: (text) => updates.push(String(text || "")),
      }
    );
    assert.equal(result.hadError, false);
    assert.equal(result.transcript, "第一段第二段");
    assert.ok(updates.length >= 1);
  } finally {
    if (typeof previousRaf === "function") {
      globalThis.requestAnimationFrame = previousRaf;
    } else {
      delete globalThis.requestAnimationFrame;
    }
  }
});

test("streamSse preserves indentation after SSE data prefix", async () => {
  const response = buildSseResponse([
    "data: {\"delta\":\"```python\\n\"}\n\n",
    "data: {\"delta\":\"    print('ok')\\n\"}\n\n",
    "data: {\"delta\":\"```\"}\n\n",
    "event: done\ndata: {\"type\":\"done\"}\n\n",
  ]);
  const updates = [];
  const previousRaf = globalThis.requestAnimationFrame;
  globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0);
  try {
    const result = await streamSse(
      async () => response,
      {
        displayMode: "full",
        onUpdate: (text) => updates.push(String(text || "")),
      }
    );
    assert.equal(result.hadError, false);
    assert.equal(result.transcript.includes("    print('ok')"), true);
    assert.equal(result.transcript.includes("print('ok')"), true);
    assert.equal(result.transcript.includes("\n    print('ok')\n"), true);
    assert.ok(updates.length >= 1);
  } finally {
    if (typeof previousRaf === "function") {
      globalThis.requestAnimationFrame = previousRaf;
    } else {
      delete globalThis.requestAnimationFrame;
    }
  }
});

test("streamSse deduplicates run_status events without event_id", async () => {
  const response = buildSseResponse([
    "data: {\"type\":\"run_status\",\"task_id\":8,\"run_id\":11,\"status\":\"running\"}\n\n",
    "data: {\"type\":\"run_status\",\"task_id\":8,\"run_id\":11,\"status\":\"running\"}\n\n",
    "event: done\ndata: {\"type\":\"done\"}\n\n",
  ]);
  let runStatusCalls = 0;
  const previousRaf = globalThis.requestAnimationFrame;
  globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0);
  try {
    const result = await streamSse(
      async () => response,
      {
        displayMode: "status",
        onRunStatus: () => {
          runStatusCalls += 1;
        },
      }
    );
    assert.equal(result.hadError, false);
    assert.equal(runStatusCalls, 1);
  } finally {
    if (typeof previousRaf === "function") {
      globalThis.requestAnimationFrame = previousRaf;
    } else {
      delete globalThis.requestAnimationFrame;
    }
  }
});

test("streamSse keeps run_status transitions for same run", async () => {
  const response = buildSseResponse([
    "data: {\"type\":\"run_status\",\"task_id\":8,\"run_id\":11,\"status\":\"running\"}\n\n",
    "data: {\"type\":\"run_status\",\"task_id\":8,\"run_id\":11,\"status\":\"running\"}\n\n",
    "data: {\"type\":\"run_status\",\"task_id\":8,\"run_id\":11,\"status\":\"waiting\"}\n\n",
    "data: {\"type\":\"run_status\",\"task_id\":8,\"run_id\":11,\"status\":\"running\"}\n\n",
    "event: done\ndata: {\"type\":\"done\"}\n\n",
  ]);
  const statuses = [];
  const previousRaf = globalThis.requestAnimationFrame;
  globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0);
  try {
    const result = await streamSse(
      async () => response,
      {
        displayMode: "status",
        onRunStatus: (obj) => {
          statuses.push(String(obj?.status || "").trim().toLowerCase());
        },
      }
    );
    assert.equal(result.hadError, false);
    assert.deepEqual(statuses, ["running", "waiting", "running"]);
  } finally {
    if (typeof previousRaf === "function") {
      globalThis.requestAnimationFrame = previousRaf;
    } else {
      delete globalThis.requestAnimationFrame;
    }
  }
});

test("streamSse deduplicates need_input events without event_id", async () => {
  const response = buildSseResponse([
    "data: {\"type\":\"need_input\",\"task_id\":8,\"run_id\":11,\"kind\":\"task_feedback\",\"question\":\"确认满意度\"}\n\n",
    "data: {\"type\":\"need_input\",\"task_id\":8,\"run_id\":11,\"kind\":\"task_feedback\",\"question\":\"确认满意度\"}\n\n",
    "event: done\ndata: {\"type\":\"done\"}\n\n",
  ]);
  let needInputCalls = 0;
  const previousRaf = globalThis.requestAnimationFrame;
  globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0);
  try {
    const result = await streamSse(
      async () => response,
      {
        displayMode: "status",
        onNeedInput: () => {
          needInputCalls += 1;
        },
      }
    );
    assert.equal(result.hadError, false);
    assert.equal(needInputCalls, 1);
  } finally {
    if (typeof previousRaf === "function") {
      globalThis.requestAnimationFrame = previousRaf;
    } else {
      delete globalThis.requestAnimationFrame;
    }
  }
});

test("streamSse keeps need_input when prompt token changes", async () => {
  const response = buildSseResponse([
    "data: {\"type\":\"need_input\",\"task_id\":8,\"run_id\":11,\"kind\":\"task_feedback\",\"prompt_token\":\"t1\",\"question\":\"确认满意度\"}\n\n",
    "data: {\"type\":\"need_input\",\"task_id\":8,\"run_id\":11,\"kind\":\"task_feedback\",\"prompt_token\":\"t1\",\"question\":\"确认满意度\"}\n\n",
    "data: {\"type\":\"need_input\",\"task_id\":8,\"run_id\":11,\"kind\":\"task_feedback\",\"prompt_token\":\"t2\",\"question\":\"确认满意度\"}\n\n",
    "event: done\ndata: {\"type\":\"done\"}\n\n",
  ]);
  let needInputCalls = 0;
  const previousRaf = globalThis.requestAnimationFrame;
  globalThis.requestAnimationFrame = (cb) => setTimeout(() => cb(Date.now()), 0);
  try {
    const result = await streamSse(
      async () => response,
      {
        displayMode: "status",
        onNeedInput: () => {
          needInputCalls += 1;
        },
      }
    );
    assert.equal(result.hadError, false);
    assert.equal(needInputCalls, 2);
  } finally {
    if (typeof previousRaf === "function") {
      globalThis.requestAnimationFrame = previousRaf;
    } else {
      delete globalThis.requestAnimationFrame;
    }
  }
});

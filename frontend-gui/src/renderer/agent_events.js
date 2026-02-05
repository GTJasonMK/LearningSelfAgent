// Agent 事件总线：用于“复用现有 SSE”把结构化事件分发到各个 UI 组件。
//
// 设计目标：
// - 不新增新的后端 SSE 通道：只消费现有 /agent/*/stream 的结构化事件；
// - 桌宠窗口收到事件后，通过 IPC 转发给主面板窗口（同一份大脑数据，跨窗口同步）；
// - 前端内部用 CustomEvent 做低耦合分发，memory/skills/graph 等页面可按需订阅。

// ESM 环境下避免 import 中断：通过 window.require 获取 Electron API
const ipcRenderer = window?.require ? window.require("electron").ipcRenderer : null;

// 统一事件名，避免与第三方或浏览器事件冲突
export const AGENT_EVENT_NAME = "lsa-agent-event";

let bridgeInited = false;

function dispatchLocal(payload) {
  const obj = payload && typeof payload === "object" ? payload : null;
  if (!obj) return;
  try {
    window.dispatchEvent(new CustomEvent(AGENT_EVENT_NAME, { detail: obj }));
  } catch (e) {}
}

/**
 * 初始化跨窗口事件桥：
 * - 主面板窗口接收主进程转发的 "agent-event" 后，再派发为本地 CustomEvent。
 */
export function initAgentEventBridge() {
  if (bridgeInited) return false;
  bridgeInited = true;
  if (!ipcRenderer) return false;
  ipcRenderer.on("agent-event", (_event, payload) => {
    dispatchLocal(payload);
  });
  return true;
}

/**
 * 派发 Agent 事件（本地 + 可选跨窗口广播）
 */
export function emitAgentEvent(payload, options = {}) {
  dispatchLocal(payload);

  const broadcast = options && options.broadcast === true;
  if (!broadcast) return;
  if (!ipcRenderer) return;
  try {
    ipcRenderer.send("agent-event", payload);
  } catch (e) {}
}


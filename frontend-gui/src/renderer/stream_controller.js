// 轻量流式控制器：管理 AbortController + 并发序号（P2：状态管理）
//
// 设计目标：
// - 同一 UI 内只允许“最后一次”流式请求更新界面；
// - 新请求启动时自动 abort 旧请求，避免旧请求 finally 覆盖新状态；
// - pet.js 与 panel.js 共用，减少重复实现与全局变量耦合。

export function createStreamController() {
  return {
    abortController: null,
    seq: 0
  };
}

export function startStream(ctrl) {
  if (!ctrl) throw new Error("stream_controller: ctrl missing");
  if (ctrl.abortController) {
    try { ctrl.abortController.abort(); } catch (e) {}
  }
  ctrl.seq = Number(ctrl.seq || 0) + 1;
  const controller = new AbortController();
  ctrl.abortController = controller;
  return { seq: ctrl.seq, controller };
}

export function isStreamActive(ctrl, seq) {
  if (!ctrl) return false;
  return Number(seq) === Number(ctrl.seq);
}

export function stopStream(ctrl, seq) {
  // 仅允许“当前活跃 seq”收敛状态
  if (!isStreamActive(ctrl, seq)) return false;
  ctrl.abortController = null;
  return true;
}

export function abortStream(ctrl) {
  if (!ctrl) return;
  if (ctrl.abortController) {
    try { ctrl.abortController.abort(); } catch (e) {}
  }
  // bump seq：让旧回调立刻失效（避免隐藏页面后仍然写 DOM）
  ctrl.seq = Number(ctrl.seq || 0) + 1;
  ctrl.abortController = null;
}

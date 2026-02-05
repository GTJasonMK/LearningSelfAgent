// 轻量状态仓库（P2：前端状态管理）
//
// 目标：
// - 把关键状态从“散落的全局变量”收敛到一个可订阅的 store；
// - 方便排查“旧请求回写/状态漂移/并发覆盖”等复杂问题；
// - 不引入额外依赖，保持实现简单可读。

/**
 * @template T
 * @param {T} initialState
 * @returns {{getState: () => T, setState: (patch: Partial<T> | ((prev: T) => T), meta?: any) => void, subscribe: (fn: (next: T, prev: T, meta?: any) => void) => () => void}}
 */
export function createStore(initialState) {
  let state = { ...(initialState || {}) };
  const listeners = new Set();

  function getState() {
    return state;
  }

  function setState(patch, meta) {
    const prev = state;
    const next = typeof patch === "function"
      ? patch(prev)
      : { ...prev, ...(patch || {}) };
    state = next;
    for (const fn of listeners) {
      try { fn(next, prev, meta); } catch (e) {}
    }
  }

  function subscribe(fn) {
    if (typeof fn !== "function") return () => {};
    listeners.add(fn);
    return () => listeners.delete(fn);
  }

  return { getState, setState, subscribe };
}


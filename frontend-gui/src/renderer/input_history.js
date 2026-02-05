// 输入历史管理器：用于“上下键回溯历史输入”（类似 shell history）。
//
// 设计目标：
// - pet/panel 共用同一套逻辑，避免两处实现漂移；
// - 历史状态可落入 store（由调用方提供 getState/setState），减少全局变量耦合；
// - 只管理输入历史相关字段：inputHistory/historyCursor/historyDraft。

/**
 * @param {{getState: Function, setState: Function, limit?: number}} options
 * @returns {{record: (value: string) => boolean, resetNavigation: (meta?: any) => void, handleKeyDown: (event: any, inputEl: any) => boolean}}
 */
export function createInputHistoryManager(options) {
  const getState = options?.getState;
  const setState = options?.setState;
  const limit = Number(options?.limit) > 0 ? Number(options.limit) : 50;

  function _state() {
    try { return (typeof getState === "function" ? getState() : {}) || {}; } catch (e) { return {}; }
  }

  function _set(patch, meta) {
    try { if (typeof setState === "function") setState(patch, meta); } catch (e) {}
  }

  function record(value) {
    const text = String(value || "").trim();
    if (!text) return false;
    _set(
      (prev) => {
        const history = Array.isArray(prev?.inputHistory) ? prev.inputHistory.slice() : [];
        const last = history.length ? history[history.length - 1] : "";
        if (text !== last) {
          history.push(text);
          if (history.length > limit) {
            history.splice(0, history.length - limit);
          }
        }
        return { ...prev, inputHistory: history, historyCursor: null, historyDraft: "" };
      },
      { reason: "input_submit" }
    );
    return true;
  }

  function resetNavigation(meta) {
    _set({ historyCursor: null, historyDraft: "" }, meta || { reason: "input_history_reset" });
  }

  function handleKeyDown(event, inputEl) {
    if (!event || !inputEl) return false;
    if (event.key !== "ArrowUp" && event.key !== "ArrowDown") return false;
    if (event.isComposing || event.altKey || event.ctrlKey || event.metaKey) return false;

    const state = _state();
    const history = Array.isArray(state?.inputHistory) ? state.inputHistory : [];
    if (!history.length) return false;

    event.preventDefault();

    let cursor = state?.historyCursor;
    let draft = String(state?.historyDraft || "");
    if (cursor === null || cursor === undefined) {
      draft = String(inputEl.value || "");
      cursor = history.length; // 从“草稿位”开始往上
    }

    if (event.key === "ArrowUp") {
      cursor = Math.max(0, Number(cursor) - 1);
    } else {
      cursor = Math.min(history.length, Number(cursor) + 1);
    }

    _set({ historyCursor: cursor, historyDraft: draft }, { reason: "input_history" });

    if (cursor === history.length) {
      inputEl.value = draft;
    } else {
      inputEl.value = history[cursor] || "";
    }

    try {
      inputEl.setSelectionRange(inputEl.value.length, inputEl.value.length);
    } catch (e) {}
    return true;
  }

  return { record, resetNavigation, handleKeyDown };
}

/**
 * 从后端 chat_messages 的 items 中提取“用户输入历史”（仅 role=user）。
 *
 * @param {any[]} items
 * @param {number} limit
 * @returns {string[]}
 */
export function buildInputHistoryFromChatItems(items, limit = 50) {
  const list = Array.isArray(items) ? items : [];
  const max = Number(limit) > 0 ? Number(limit) : 50;

  const out = [];
  let last = "";

  for (const raw of list) {
    const it = raw && typeof raw === "object" ? raw : {};
    const role = String(it?.role || "").trim().toLowerCase();
    if (role !== "user") continue;
    const text = String(it?.content || "").trim();
    if (!text) continue;
    // 去重：连续重复的输入只保留一次（符合 record() 的语义）
    if (text === last) continue;
    out.push(text);
    last = text;
  }

  return out.length > max ? out.slice(out.length - max) : out;
}

/**
 * 以“后端历史”为主合并输入历史：保持顺序，并尽量保留本地未落库的输入。
 *
 * @param {any[]} prevHistory
 * @param {any[]} backendHistory
 * @param {number} limit
 * @returns {string[]}
 */
export function mergeInputHistoryWithBackend(prevHistory, backendHistory, limit = 50) {
  const max = Number(limit) > 0 ? Number(limit) : 50;

  const backend = Array.isArray(backendHistory)
    ? backendHistory.map((x) => String(x || "").trim()).filter(Boolean)
    : [];
  const local = Array.isArray(prevHistory)
    ? prevHistory.map((x) => String(x || "").trim()).filter(Boolean)
    : [];

  // 后端为空：保留本地（例如后端暂不可用，但本地已记录了输入）
  if (!backend.length) {
    return local.length > max ? local.slice(local.length - max) : local;
  }
  // 本地为空：直接使用后端（跨窗口共享历史）
  if (!local.length) {
    return backend.length > max ? backend.slice(backend.length - max) : backend;
  }

  const out = backend.slice();
  for (const text of local) {
    // 后端已存在该文本：跳过，避免重复
    if (backend.includes(text)) continue;
    const last = out.length ? out[out.length - 1] : "";
    if (text === last) continue;
    out.push(text);
  }

  return out.length > max ? out.slice(out.length - max) : out;
}

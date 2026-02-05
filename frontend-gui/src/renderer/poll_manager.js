// 轮询管理器：统一 setInterval/clearInterval，并避免 async 任务重入。
//
// 设计目标：
// - 让 pet/panel/dashboard 等模块不再各自维护 timerId/inFlight；
// - 定时任务执行时间超过间隔时，默认跳过重入，避免并发导致 UI 状态漂移；
// - 支持手动触发 run(key, fn) 与定时触发共用同一把“inFlight 锁”。

export class PollManager {
  constructor() {
    /** @type {Map<string, { timerId: any, intervalMs: number, inFlight: boolean, handler: Function | null }>} */
    this._items = new Map();
  }

  _key(value) {
    return String(value || "").trim();
  }

  _ensureItem(key) {
    const k = this._key(key);
    if (!k) throw new Error("PollManager key is empty");
    const existing = this._items.get(k);
    if (existing) return { k, item: existing };
    const item = { timerId: null, intervalMs: 0, inFlight: false, handler: null };
    this._items.set(k, item);
    return { k, item };
  }

  isRunning(key) {
    const k = this._key(key);
    if (!k) return false;
    const item = this._items.get(k);
    return !!(item && item.timerId);
  }

  start(key, handler, intervalMs, options = {}) {
    const { k, item } = this._ensureItem(key);
    if (item.timerId) return false;

    item.handler = typeof handler === "function" ? handler : null;
    item.intervalMs = Number(intervalMs) > 0 ? Number(intervalMs) : 1000;

    const runImmediately = options.runImmediately !== false;
    if (runImmediately) {
      this.run(k).catch(() => {});
    }

    item.timerId = setInterval(() => {
      this.run(k).catch(() => {});
    }, item.intervalMs);

    return true;
  }

  stop(key) {
    const k = this._key(key);
    if (!k) return;
    const item = this._items.get(k);
    if (!item || !item.timerId) return;
    clearInterval(item.timerId);
    item.timerId = null;
  }

  stopAll() {
    for (const k of Array.from(this._items.keys())) {
      this.stop(k);
    }
  }

  async run(key, handlerOverride) {
    const { k, item } = this._ensureItem(key);
    const handler = typeof handlerOverride === "function" ? handlerOverride : item.handler;
    if (!handler) return false;
    if (item.inFlight) return false;

    item.inFlight = true;
    try {
      await handler();
      return true;
    } catch (e) {
      // 轮询失败不应抛到全局，避免 UI 被 unhandled rejection 卡死。
      return false;
    } finally {
      item.inFlight = false;
    }
  }
}


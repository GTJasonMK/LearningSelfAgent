// 通用工具函数模块
// 提供防抖、节流、按钮状态管理、事件管理器

/**
 * 防抖函数
 * @param {Function} fn - 要防抖的函数
 * @param {number} delay - 延迟时间（毫秒）
 * @returns {Function} 防抖后的函数
 */
export function debounce(fn, delay = 300) {
  let timeoutId = null;
  return function (...args) {
    if (timeoutId) {
      clearTimeout(timeoutId);
    }
    timeoutId = setTimeout(() => {
      fn.apply(this, args);
      timeoutId = null;
    }, delay);
  };
}

/**
 * 节流函数
 * @param {Function} fn - 要节流的函数
 * @param {number} limit - 时间限制（毫秒）
 * @returns {Function} 节流后的函数
 */
export function throttle(fn, limit = 100) {
  let lastTime = 0;
  let timeoutId = null;
  return function (...args) {
    const now = Date.now();
    const remaining = limit - (now - lastTime);
    if (remaining <= 0) {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      lastTime = now;
      fn.apply(this, args);
    } else if (!timeoutId) {
      timeoutId = setTimeout(() => {
        lastTime = Date.now();
        timeoutId = null;
        fn.apply(this, args);
      }, remaining);
    }
  };
}

/**
 * 设置按钮加载状态
 * @param {HTMLButtonElement} btn - 按钮元素
 * @param {string} loadingText - 加载中显示的文本
 * @returns {string} 原始文本（用于恢复）
 */
export function setButtonLoading(btn, loadingText) {
  if (!btn) return "";
  const originalText = btn.textContent;
  btn.textContent = loadingText;
  btn.disabled = true;
  btn.classList.add("panel-button-loading");
  return originalText;
}

/**
 * 清除按钮加载状态
 * @param {HTMLButtonElement} btn - 按钮元素
 * @param {string} originalText - 原始文本
 */
export function clearButtonLoading(btn, originalText) {
  if (!btn) return;
  btn.textContent = originalText;
  btn.disabled = false;
  btn.classList.remove("panel-button-loading");
}

/**
 * 创建带加载状态的异步处理包装器
 * @param {HTMLButtonElement} btn - 按钮元素
 * @param {string} loadingText - 加载中显示的文本
 * @param {Function} asyncFn - 异步函数
 * @returns {Function} 包装后的函数
 */
export function withLoading(btn, loadingText, asyncFn) {
  let isLoading = false;
  return async function (...args) {
    if (isLoading) return;
    isLoading = true;
    const originalText = setButtonLoading(btn, loadingText);
    try {
      return await asyncFn.apply(this, args);
    } finally {
      clearButtonLoading(btn, originalText);
      isLoading = false;
    }
  };
}

/**
 * 创建事件管理器
 * 用于管理事件监听器的生命周期，防止内存泄漏
 * @returns {{add: Function, remove: Function, removeAll: Function}}
 */
export function createEventManager() {
  const listeners = [];

  return {
    /**
     * 添加事件监听器
     * @param {EventTarget} target - 事件目标
     * @param {string} type - 事件类型
     * @param {Function} handler - 事件处理函数
     * @param {Object} options - 事件选项
     */
    add(target, type, handler, options) {
      if (!target) return;
      target.addEventListener(type, handler, options);
      listeners.push({ target, type, handler, options });
    },

    /**
     * 移除指定事件监听器
     * @param {EventTarget} target - 事件目标
     * @param {string} type - 事件类型
     * @param {Function} handler - 事件处理函数
     */
    remove(target, type, handler) {
      const index = listeners.findIndex(
        (l) => l.target === target && l.type === type && l.handler === handler
      );
      if (index !== -1) {
        const { target: t, type: ty, handler: h, options } = listeners[index];
        t.removeEventListener(ty, h, options);
        listeners.splice(index, 1);
      }
    },

    /**
     * 移除所有事件监听器
     */
    removeAll() {
      for (const { target, type, handler, options } of listeners) {
        target.removeEventListener(type, handler, options);
      }
      listeners.length = 0;
    },

    /**
     * 获取当前监听器数量
     * @returns {number}
     */
    count() {
      return listeners.length;
    }
  };
}

/**
 * 安全地解析 JSON
 * @param {string} str - JSON 字符串
 * @param {*} defaultValue - 解析失败时的默认值
 * @returns {*}
 */
export function safeParseJson(str, defaultValue = null) {
  if (!str) return defaultValue;
  try {
    return JSON.parse(str);
  } catch {
    return defaultValue;
  }
}

/**
 * 格式化模板字符串
 * @param {string} template - 模板字符串，使用 {key} 作为占位符
 * @param {Object} data - 数据对象
 * @returns {string}
 */
export function formatTemplate(template, data) {
  if (!template) return "";
  return template.replace(/\{(\w+)\}/g, (_, key) => {
    const value = data[key];
    return value === undefined || value === null ? "" : String(value);
  });
}

/**
 * 延迟执行
 * @param {number} ms - 延迟毫秒数
 * @returns {Promise<void>}
 */
export function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * 生成唯一ID
 * @param {string} prefix - ID前缀
 * @returns {string}
 */
export function generateId(prefix = "id") {
  return `${prefix}_${Date.now()}_${Math.random().toString(36).slice(2, 9)}`;
}

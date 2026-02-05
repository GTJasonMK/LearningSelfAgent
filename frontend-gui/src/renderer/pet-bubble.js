// 桌宠气泡消息系统

import { setMarkdownContent } from "./markdown.js";

export const BUBBLE_TYPES = {
  INFO: "info",
  WARNING: "warning",
  ERROR: "error",
  SUCCESS: "success"
};

/**
 * 气泡消息管理器
 */
export class PetBubble {
  /**
   * @param {HTMLElement} bubbleEl - 气泡元素
   */
  constructor(bubbleEl) {
    this.element = bubbleEl;
    // 兼容：气泡内若有结构（content/actions），只更新 content 区域，避免覆盖按钮等 DOM
    this.contentEl = bubbleEl?.querySelector?.(".pet-bubble-content") || bubbleEl;
    this.queue = [];
    this.isShowing = false;
    this.hideTimer = null;
    this.defaultDuration = 3000;
    // 可选回调：当气泡隐藏/清空时触发（用于命中检测刷新，避免透明区域误遮挡）
    this.onHide = null;
  }

  /**
   * 显示消息
   * @param {string} message - 消息内容
   * @param {string} type - 消息类型
   * @param {number} duration - 显示时长（毫秒）
   */
  show(message, type = BUBBLE_TYPES.INFO, duration = this.defaultDuration) {
    this.queue.push({ message, type, duration });
    if (!this.isShowing) {
      this._showNext();
    }
  }

  /**
   * 立即覆盖当前气泡内容（用于流式输出，避免队列导致延迟/堆积）
   * @param {string} message - 消息内容
   * @param {string} type - 消息类型
   */
  set(message, type = BUBBLE_TYPES.INFO) {
    if (!this.element || !this.contentEl) return;
    this.queue = [];
    this.isShowing = true;
    if (this.hideTimer) {
      clearTimeout(this.hideTimer);
      this.hideTimer = null;
    }
    setMarkdownContent(this.contentEl, message);
    this._setType(type);
    this.element.classList.remove("is-hidden");
    this.element.classList.add("is-visible");
    // 跟随最新输出，避免长文本被底部截断（配合 CSS overflow:auto）
    try {
      this.contentEl.scrollTop = this.contentEl.scrollHeight;
    } catch (e) {}
  }

  /**
   * 显示队列中的下一条消息
   */
  showNext() {
    this._showNext();
  }

  /**
   * 清空队列并隐藏气泡
   */
  clear() {
    this.queue = [];
    this._hide();
  }

  /**
   * 内部：显示下一条消息
   * @private
   */
  _showNext() {
    if (!this.element || !this.contentEl || this.queue.length === 0) {
      this.isShowing = false;
      return;
    }

    const { message, type, duration } = this.queue.shift();
    this.isShowing = true;

    // 清除之前的定时器
    if (this.hideTimer) {
      clearTimeout(this.hideTimer);
    }

    // 设置内容和样式
    setMarkdownContent(this.contentEl, message);
    this._setType(type);

    // 显示动画
    this.element.classList.remove("is-hidden");
    this.element.classList.add("is-visible");
    try {
      this.contentEl.scrollTop = this.contentEl.scrollHeight;
    } catch (e) {}

    // 设置自动隐藏
    if (duration > 0) {
      this.hideTimer = setTimeout(() => {
        this._hide();
        // 延迟后显示下一条
        setTimeout(() => this._showNext(), 300);
      }, duration);
    }
  }

  /**
   * 内部：隐藏气泡
   * @private
   */
  _hide() {
    if (!this.element) return;
    this.element.classList.remove("is-visible");
    this.element.classList.add("is-hidden");
    this.isShowing = false;
    try {
      if (typeof this.onHide === "function") this.onHide();
    } catch (e) {}
  }

  /**
   * 内部：设置气泡类型样式
   * @private
   */
  _setType(type) {
    if (!this.element) return;
    // 移除所有类型类
    this.element.classList.remove(
      "pet-bubble-info",
      "pet-bubble-warning",
      "pet-bubble-error",
      "pet-bubble-success"
    );
    // 添加当前类型类
    this.element.classList.add(`pet-bubble-${type}`);
  }
}

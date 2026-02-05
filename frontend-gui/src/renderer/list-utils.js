// 列表渲染工具模块
// 提供统一的列表渲染、加载状态、操作按钮等功能

import { UI_TEXT } from "./constants.js";
import { formatTemplate } from "./utils.js";

/**
 * 设置列表加载状态
 * @param {HTMLElement} listEl - 列表元素
 * @param {string} message - 加载提示信息
 */
export function setListLoading(listEl, message) {
  if (!listEl) return;
  listEl.innerHTML = `<li class="panel-list-item panel-list-loading">${message}</li>`;
}

/**
 * 设置列表为空状态
 * @param {HTMLElement} listEl - 列表元素
 * @param {string} message - 空状态提示信息
 */
export function setListEmpty(listEl, message) {
  if (!listEl) return;
  listEl.innerHTML = `<li class="panel-list-item panel-list-empty">${message}</li>`;
}

/**
 * 设置列表错误状态
 * @param {HTMLElement} listEl - 列表元素
 * @param {string} message - 错误提示信息
 */
export function setListError(listEl, message) {
  if (!listEl) return;
  listEl.innerHTML = `<li class="panel-list-item panel-list-error">${message}</li>`;
}

/**
 * 渲染列表
 * @param {HTMLElement} listEl - 列表元素
 * @param {Array} items - 数据项数组
 * @param {Function} renderItem - 渲染函数，接收 (li, item, index)
 * @param {string} emptyMessage - 空状态提示信息
 */
export function renderList(listEl, items, renderItem, emptyMessage) {
  if (!listEl) return;
  if (!items || items.length === 0) {
    setListEmpty(listEl, emptyMessage);
    return;
  }
  listEl.innerHTML = "";
  items.forEach((item, index) => {
    const li = document.createElement("li");
    li.className = "panel-list-item";
    li.dataset.index = String(index);
    if (item.id !== undefined) {
      li.dataset.id = String(item.id);
    }
    renderItem(li, item, index);
    listEl.appendChild(li);
  });
}

/**
 * 创建操作按钮
 * @param {string} text - 按钮文本
 * @param {string} action - 操作名称
 * @param {string} className - 额外的类名
 * @returns {HTMLButtonElement}
 */
export function createActionButton(text, action, className = "") {
  const btn = document.createElement("button");
  btn.className = `panel-button panel-list-action ${className}`.trim();
  btn.textContent = text;
  btn.dataset.action = action;
  return btn;
}

/**
 * 创建操作按钮组
 * @param {Array<{text: string, action: string, className?: string}>} actions - 操作配置数组
 * @returns {HTMLElement}
 */
export function createActionButtons(actions) {
  const container = document.createElement("span");
  container.className = "panel-list-actions";
  for (const { text, action, className = "" } of actions) {
    container.appendChild(createActionButton(text, action, className));
  }
  return container;
}

/**
 * 列表渲染器类
 */
export class ListRenderer {
  /**
   * @param {HTMLElement} listEl - 列表元素
   * @param {Object} options - 配置选项
   * @param {string} options.loadingMessage - 加载提示
   * @param {string} options.emptyMessage - 空状态提示
   * @param {string} options.errorMessage - 错误提示
   */
  constructor(listEl, options = {}) {
    this.listEl = listEl;
    this.loadingMessage = options.loadingMessage || UI_TEXT.LIST_LOADING;
    this.emptyMessage = options.emptyMessage || UI_TEXT.LIST_EMPTY;
    this.errorMessage = options.errorMessage || UI_TEXT.LIST_ERROR;
    this.items = [];
    this.renderItemFn = null;
    this.actionHandlers = new Map();
  }

  /**
   * 设置加载状态
   * @returns {ListRenderer}
   */
  setLoading() {
    setListLoading(this.listEl, this.loadingMessage);
    return this;
  }

  /**
   * 设置空状态
   * @returns {ListRenderer}
   */
  setEmpty() {
    setListEmpty(this.listEl, this.emptyMessage);
    return this;
  }

  /**
   * 设置错误状态
   * @param {string} message - 错误信息（可选）
   * @returns {ListRenderer}
   */
  setError(message) {
    setListError(this.listEl, message || this.errorMessage);
    return this;
  }

  /**
   * 设置渲染函数
   * @param {Function} fn - 渲染函数，接收 (li, item, index)
   * @returns {ListRenderer}
   */
  setRenderItem(fn) {
    this.renderItemFn = fn;
    return this;
  }

  /**
   * 设置使用模板渲染
   * @param {string} template - 模板字符串
   * @param {Array<{text: string, action: string}>} actions - 操作按钮配置
   * @returns {ListRenderer}
   */
  setTemplate(template, actions = []) {
    this.renderItemFn = (li, item) => {
      const content = document.createElement("span");
      content.className = "panel-list-content";
      content.textContent = formatTemplate(template, item);
      li.appendChild(content);
      if (actions.length > 0) {
        li.appendChild(createActionButtons(actions));
      }
    };
    return this;
  }

  /**
   * 注册操作处理函数
   * @param {string} action - 操作名称
   * @param {Function} handler - 处理函数，接收 (item, index, event)
   * @returns {ListRenderer}
   */
  onAction(action, handler) {
    this.actionHandlers.set(action, handler);
    return this;
  }

  /**
   * 渲染列表
   * @param {Array} items - 数据项数组
   * @returns {ListRenderer}
   */
  render(items) {
    this.items = items || [];
    if (!this.listEl) return this;

    if (this.items.length === 0) {
      this.setEmpty();
      return this;
    }

    this.listEl.innerHTML = "";
    this.items.forEach((item, index) => {
      const li = document.createElement("li");
      li.className = "panel-list-item";
      li.dataset.index = String(index);
      if (item.id !== undefined) {
        li.dataset.id = String(item.id);
      }
      if (this.renderItemFn) {
        this.renderItemFn(li, item, index);
      }
      this.listEl.appendChild(li);
    });

    // 绑定操作按钮事件（使用事件委托）
    this.bindActionEvents();
    return this;
  }

  /**
   * 绑定操作按钮事件
   */
  bindActionEvents() {
    // 移除旧的监听器
    if (this._actionHandler) {
      this.listEl.removeEventListener("click", this._actionHandler);
    }

    this._actionHandler = (e) => {
      const btn = e.target.closest("[data-action]");
      if (!btn) return;

      const action = btn.dataset.action;
      const li = btn.closest(".panel-list-item");
      const index = li ? parseInt(li.dataset.index, 10) : -1;
      const item = index >= 0 ? this.items[index] : null;

      const handler = this.actionHandlers.get(action);
      if (handler && item) {
        handler(item, index, e);
      }
    };

    this.listEl.addEventListener("click", this._actionHandler);
  }

  /**
   * 清理资源
   */
  destroy() {
    if (this._actionHandler) {
      this.listEl?.removeEventListener("click", this._actionHandler);
      this._actionHandler = null;
    }
    this.actionHandlers.clear();
    this.items = [];
  }

  /**
   * 获取当前数据
   * @returns {Array}
   */
  getItems() {
    return this.items;
  }

  /**
   * 根据ID查找项
   * @param {*} id - 项ID
   * @returns {*}
   */
  findById(id) {
    return this.items.find((item) => item.id === id);
  }

  /**
   * 根据索引获取项
   * @param {number} index - 索引
   * @returns {*}
   */
  getItem(index) {
    return this.items[index];
  }
}

/**
 * 创建可展开的列表项
 * @param {Object} options - 配置选项
 * @param {string} options.title - 标题
 * @param {string} options.content - 内容
 * @param {boolean} options.expanded - 是否展开
 * @returns {HTMLElement}
 */
export function createExpandableItem(options) {
  const { title, content, expanded = false } = options;
  const details = document.createElement("details");
  details.className = "panel-list-expandable";
  if (expanded) details.open = true;

  const summary = document.createElement("summary");
  summary.className = "panel-list-summary";
  summary.textContent = title;
  details.appendChild(summary);

  const contentEl = document.createElement("div");
  contentEl.className = "panel-list-expand-content";
  contentEl.innerHTML = content;
  details.appendChild(contentEl);

  return details;
}

/**
 * 创建带状态标签的列表项内容
 * @param {string} text - 主要文本
 * @param {string} status - 状态文本
 * @param {string} statusClass - 状态类名
 * @returns {HTMLElement}
 */
export function createStatusItem(text, status, statusClass = "") {
  const container = document.createElement("span");
  container.className = "panel-list-content";

  const textSpan = document.createElement("span");
  textSpan.className = "panel-list-text";
  textSpan.textContent = text;
  container.appendChild(textSpan);

  if (status) {
    const statusSpan = document.createElement("span");
    statusSpan.className = `panel-list-status ${statusClass}`.trim();
    statusSpan.textContent = status;
    container.appendChild(statusSpan);
  }

  return container;
}

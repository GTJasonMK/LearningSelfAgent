// 表单工具模块
// 提供表单验证、错误处理、提交绑定等功能

import { UI_TEXT, INPUT_ATTRS } from "./constants.js";
import { createEventManager, withLoading } from "./utils.js";

/**
 * 获取或创建表单错误元素
 * @param {HTMLFormElement} formEl - 表单元素
 * @returns {HTMLElement|null}
 */
export function getFormErrorEl(formEl) {
  if (!formEl) return null;
  let errorEl = formEl.querySelector(".panel-form-error");
  if (!errorEl) {
    errorEl = document.createElement("div");
    errorEl.className = "panel-error panel-form-error";
    formEl.appendChild(errorEl);
  }
  return errorEl;
}

/**
 * 清除表单错误
 * @param {HTMLFormElement} formEl - 表单元素
 */
export function clearFormError(formEl) {
  const errorEl = formEl?.querySelector(".panel-form-error");
  if (errorEl) errorEl.remove();
}

/**
 * 显示表单错误
 * @param {HTMLFormElement} formEl - 表单元素
 * @param {string} message - 错误信息
 */
export function showFormError(formEl, message) {
  const errorEl = getFormErrorEl(formEl);
  if (errorEl) {
    errorEl.textContent = message;
  }
}

/**
 * 显示表单成功提示
 * @param {HTMLFormElement} formEl - 表单元素
 * @param {string} message - 成功信息
 * @param {number} duration - 显示时长（毫秒）
 */
export function showFormSuccess(formEl, message, duration = 3000) {
  if (!formEl) return;
  let successEl = formEl.querySelector(".panel-form-success");
  if (!successEl) {
    successEl = document.createElement("div");
    successEl.className = "panel-success panel-form-success";
    formEl.appendChild(successEl);
  }
  successEl.textContent = message;
  if (duration > 0) {
    setTimeout(() => successEl.remove(), duration);
  }
}

/**
 * 检查数字是否在范围内
 * @param {*} value - 要检查的值
 * @param {Object} attrs - 属性对象，包含 min 和 max
 * @returns {boolean}
 */
export function isNumberInRange(value, attrs) {
  if (value === "" || value === null || value === undefined) return false;
  const num = Number(value);
  if (Number.isNaN(num)) return false;
  if (attrs?.min !== undefined && num < attrs.min) return false;
  if (attrs?.max !== undefined && num > attrs.max) return false;
  return true;
}

/**
 * 验证必填文本
 * @param {HTMLFormElement} formEl - 表单元素
 * @param {string} value - 要验证的值
 * @returns {boolean}
 */
export function validateRequiredText(formEl, value) {
  if (!value || value.trim() === "") {
    showFormError(formEl, UI_TEXT.VALIDATION_REQUIRED);
    return false;
  }
  return true;
}

/**
 * 验证可选数字
 * @param {HTMLFormElement} formEl - 表单元素
 * @param {*} value - 要验证的值
 * @param {Object} attrs - 属性对象，包含 min 和 max
 * @returns {boolean}
 */
export function validateOptionalNumber(formEl, value, attrs) {
  if (value === "" || value === null || value === undefined) return true;
  if (!isNumberInRange(value, attrs)) {
    const message = Number.isNaN(Number(value))
      ? UI_TEXT.VALIDATION_NUMBER
      : UI_TEXT.VALIDATION_RANGE;
    showFormError(formEl, message);
    return false;
  }
  return true;
}

/**
 * 验证必填数字
 * @param {HTMLFormElement} formEl - 表单元素
 * @param {*} value - 要验证的值
 * @param {Object} attrs - 属性对象，包含 min 和 max
 * @returns {boolean}
 */
export function validateRequiredNumber(formEl, value, attrs) {
  if (value === "" || value === null || value === undefined) {
    showFormError(formEl, UI_TEXT.VALIDATION_REQUIRED);
    return false;
  }
  if (!isNumberInRange(value, attrs)) {
    const message = Number.isNaN(Number(value))
      ? UI_TEXT.VALIDATION_NUMBER
      : UI_TEXT.VALIDATION_RANGE;
    showFormError(formEl, message);
    return false;
  }
  return true;
}

/**
 * 表单验证器类
 */
export class FormValidator {
  constructor(formEl) {
    this.formEl = formEl;
    this.rules = [];
  }

  /**
   * 添加必填文本验证规则
   * @param {string} fieldId - 字段ID
   * @returns {FormValidator}
   */
  requireText(fieldId) {
    this.rules.push({
      fieldId,
      validate: (value) => {
        if (!value || value.trim() === "") {
          return UI_TEXT.VALIDATION_REQUIRED;
        }
        return null;
      }
    });
    return this;
  }

  /**
   * 添加可选数字验证规则
   * @param {string} fieldId - 字段ID
   * @param {string} attrsKey - INPUT_ATTRS 中的键名
   * @returns {FormValidator}
   */
  optionalNumber(fieldId, attrsKey) {
    const attrs = INPUT_ATTRS[attrsKey] || {};
    this.rules.push({
      fieldId,
      validate: (value) => {
        if (value === "" || value === null || value === undefined) return null;
        const num = Number(value);
        if (Number.isNaN(num)) return UI_TEXT.VALIDATION_NUMBER;
        if (attrs.min !== undefined && num < attrs.min) return UI_TEXT.VALIDATION_RANGE;
        if (attrs.max !== undefined && num > attrs.max) return UI_TEXT.VALIDATION_RANGE;
        return null;
      }
    });
    return this;
  }

  /**
   * 添加必填数字验证规则
   * @param {string} fieldId - 字段ID
   * @param {string} attrsKey - INPUT_ATTRS 中的键名
   * @returns {FormValidator}
   */
  requireNumber(fieldId, attrsKey) {
    const attrs = INPUT_ATTRS[attrsKey] || {};
    this.rules.push({
      fieldId,
      validate: (value) => {
        if (value === "" || value === null || value === undefined) {
          return UI_TEXT.VALIDATION_REQUIRED;
        }
        const num = Number(value);
        if (Number.isNaN(num)) return UI_TEXT.VALIDATION_NUMBER;
        if (attrs.min !== undefined && num < attrs.min) return UI_TEXT.VALIDATION_RANGE;
        if (attrs.max !== undefined && num > attrs.max) return UI_TEXT.VALIDATION_RANGE;
        return null;
      }
    });
    return this;
  }

  /**
   * 添加自定义验证规则
   * @param {string} fieldId - 字段ID
   * @param {Function} validateFn - 验证函数，返回错误信息或 null
   * @returns {FormValidator}
   */
  custom(fieldId, validateFn) {
    this.rules.push({ fieldId, validate: validateFn });
    return this;
  }

  /**
   * 执行验证
   * @returns {boolean}
   */
  validate() {
    clearFormError(this.formEl);
    for (const rule of this.rules) {
      const field = this.formEl.querySelector(`#${rule.fieldId}`);
      const value = field?.value;
      const error = rule.validate(value);
      if (error) {
        showFormError(this.formEl, error);
        field?.focus();
        return false;
      }
    }
    return true;
  }

  /**
   * 获取表单数据
   * @returns {Object}
   */
  getData() {
    const data = {};
    for (const rule of this.rules) {
      const field = this.formEl.querySelector(`#${rule.fieldId}`);
      if (field) {
        data[rule.fieldId] = field.value;
      }
    }
    return data;
  }
}

/**
 * 绑定表单提交处理
 * @param {HTMLFormElement} formEl - 表单元素
 * @param {Function} handler - 提交处理函数
 * @param {Object} options - 选项
 * @param {string} options.loadingText - 加载中文本
 * @param {HTMLButtonElement} options.submitBtn - 提交按钮
 * @param {Function} options.onSuccess - 成功回调
 * @param {Function} options.onError - 错误回调
 * @returns {Object} 事件管理器
 */
export function attachFormSubmit(formEl, handler, options = {}) {
  const eventManager = createEventManager();
  const {
    loadingText = UI_TEXT.LOADING_DEFAULT,
    submitBtn = formEl?.querySelector('button[type="submit"]'),
    onSuccess,
    onError
  } = options;

  if (!formEl) return eventManager;

  // 绑定输入时清除错误
  const clearHandler = () => clearFormError(formEl);
  eventManager.add(formEl, "input", clearHandler);
  eventManager.add(formEl, "change", clearHandler);

  // 绑定提交处理
  const submitHandler = async (e) => {
    e.preventDefault();
    clearFormError(formEl);

    let originalText = "";
    if (submitBtn) {
      originalText = submitBtn.textContent;
      submitBtn.textContent = loadingText;
      submitBtn.disabled = true;
    }

    try {
      const result = await handler(e);
      if (onSuccess) onSuccess(result);
    } catch (err) {
      const message = err?.message || UI_TEXT.ERROR_UNKNOWN;
      showFormError(formEl, message);
      if (onError) onError(err);
    } finally {
      if (submitBtn) {
        submitBtn.textContent = originalText;
        submitBtn.disabled = false;
      }
    }
  };

  eventManager.add(formEl, "submit", submitHandler);
  return eventManager;
}

/**
 * 绑定表单清除错误（简易版本，用于兼容旧代码）
 * @param {HTMLFormElement} formEl - 表单元素
 */
export function attachFormClear(formEl) {
  if (!formEl) return;
  formEl.addEventListener("input", () => clearFormError(formEl));
  formEl.addEventListener("change", () => clearFormError(formEl));
}

/**
 * 重置表单
 * @param {HTMLFormElement} formEl - 表单元素
 */
export function resetForm(formEl) {
  if (!formEl) return;
  formEl.reset();
  clearFormError(formEl);
}

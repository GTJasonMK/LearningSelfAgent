// 设置标签页模块（LLM 配置）

import * as api from "../api.js";
import { UI_TEXT } from "../constants.js";
import { createEventManager } from "../utils.js";
import { clearFormError, attachFormClear } from "../form-utils.js";

export function bind(section) {
  const eventManager = createEventManager();

  const formEl = section.querySelector("#llm-config-form");
  const providerEl = section.querySelector("#llm-config-provider");
  const apiKeyEl = section.querySelector("#llm-config-api-key");
  const baseUrlEl = section.querySelector("#llm-config-base-url");
  const modelEl = section.querySelector("#llm-config-model");
  const providerClearEl = section.querySelector("#llm-config-provider-clear");
  const apiKeyClearEl = section.querySelector("#llm-config-api-key-clear");
  const baseUrlClearEl = section.querySelector("#llm-config-base-url-clear");
  const modelClearEl = section.querySelector("#llm-config-model-clear");
  const loadBtn = section.querySelector("#llm-config-load");
  const statusEl = section.querySelector("#llm-config-status");

  async function loadConfig() {
    if (statusEl) statusEl.textContent = UI_TEXT.LOADING || "...";
    try {
      const result = await api.fetchLlmConfig();
      if (providerEl) providerEl.value = result.provider || "";
      if (baseUrlEl) baseUrlEl.value = result.base_url || "";
      if (modelEl) modelEl.value = result.model || "";
      if (providerClearEl) providerClearEl.checked = false;
      if (apiKeyClearEl) apiKeyClearEl.checked = false;
      if (baseUrlClearEl) baseUrlClearEl.checked = false;
      if (modelClearEl) modelClearEl.checked = false;
      // 不回显明文 key，避免误展示；用 placeholder 提示是否已配置
      if (apiKeyEl) {
        apiKeyEl.value = "";
        apiKeyEl.setAttribute(
          "placeholder",
          result.api_key_set
            ? (UI_TEXT.LLM_CONFIG_API_KEY_PLACEHOLDER_SET || "")
            : (UI_TEXT.LLM_CONFIG_API_KEY_PLACEHOLDER || "")
        );
      }
      if (statusEl) statusEl.textContent = UI_TEXT.LLM_CONFIG_STATUS_LOADED || UI_TEXT.OK;
    } catch (e) {
      if (statusEl) statusEl.textContent = UI_TEXT.LLM_CONFIG_STATUS_FAIL || UI_TEXT.LOAD_FAIL;
    }
  }

  async function saveConfig(event) {
    event.preventDefault();
    if (!formEl) return;
    clearFormError(formEl);

    const payload = {
      provider: undefined,
      api_key: undefined,
      base_url: undefined,
      model: undefined
    };
    const providerValue = providerEl ? providerEl.value.trim() : "";
    const apiKeyValue = apiKeyEl ? apiKeyEl.value.trim() : "";
    const baseUrlValue = baseUrlEl ? baseUrlEl.value.trim() : "";
    const modelValue = modelEl ? modelEl.value.trim() : "";

    if (providerClearEl?.checked) {
      payload.provider = "";
    } else if (providerValue) {
      payload.provider = providerValue;
    }

    if (apiKeyClearEl?.checked) {
      payload.api_key = "";
    } else if (apiKeyValue) {
      payload.api_key = apiKeyValue;
    }

    if (baseUrlClearEl?.checked) {
      payload.base_url = "";
    } else if (baseUrlValue) {
      payload.base_url = baseUrlValue;
    }

    if (modelClearEl?.checked) {
      payload.model = "";
    } else if (modelValue) {
      payload.model = modelValue;
    }

    if (statusEl) statusEl.textContent = UI_TEXT.LOADING || "...";
    try {
      await api.updateLlmConfig(payload);
      if (apiKeyEl) apiKeyEl.value = "";
      if (providerClearEl) providerClearEl.checked = false;
      if (apiKeyClearEl) apiKeyClearEl.checked = false;
      if (baseUrlClearEl) baseUrlClearEl.checked = false;
      if (modelClearEl) modelClearEl.checked = false;
      if (statusEl) statusEl.textContent = UI_TEXT.LLM_CONFIG_STATUS_OK || UI_TEXT.OK;
      // 保存后顺便刷新一次状态
      loadConfig();
    } catch (e) {
      if (statusEl) statusEl.textContent = UI_TEXT.LLM_CONFIG_STATUS_FAIL || UI_TEXT.LOAD_FAIL;
    }
  }

  if (formEl) {
    attachFormClear(formEl);
    eventManager.add(formEl, "submit", saveConfig);
  }
  if (loadBtn) {
    eventManager.add(loadBtn, "click", loadConfig);
  }

  // 首次进入自动加载
  loadConfig();

  return eventManager;
}

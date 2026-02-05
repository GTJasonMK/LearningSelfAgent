// 系统标签页模块

import * as api from "../api.js";
import { UI_TEXT, INPUT_ATTRS, REUSE_OPTIONS } from "../constants.js";
import { createEventManager, formatTemplate, debounce } from "../utils.js";
import {
  clearFormError,
  showFormError,
  validateRequiredText,
  validateOptionalNumber,
  isNumberInRange,
  attachFormClear
} from "../form-utils.js";
import { setListLoading, renderList } from "../list-utils.js";

/**
 * 绑定系统标签页
 * @param {HTMLElement} section - 标签页容器
 * @returns {Object} 事件管理器
 */
export function bind(section) {
  const eventManager = createEventManager();

  // 系统页内子标签（系统 -> 多页面）
  const subnavItems = Array.from(section.querySelectorAll(".system-subnav-item"));
  const pages = Array.from(section.querySelectorAll(".system-page"));

  // 获取搜索相关元素
  const searchFormEl = section.querySelector("#search-form");
  const searchQueryEl = section.querySelector("#search-query");
  const searchLimitEl = section.querySelector("#search-limit");
  const searchResultEl = section.querySelector("#search-result");
  const searchResultListEl = section.querySelector("#search-result-list");
  const searchInjectionListEl = section.querySelector("#search-injection-list");

  // 获取工具相关元素
  const toolFormEl = section.querySelector("#tool-form");
  const toolNameEl = section.querySelector("#tool-name");
  const toolDescEl = section.querySelector("#tool-desc");
  const toolVersionEl = section.querySelector("#tool-version");
  const toolListEl = section.querySelector("#tool-list");
  const toolListRefreshBtn = section.querySelector("#tool-list-refresh");

  // 获取LLM记录相关元素
  const llmFormEl = section.querySelector("#llm-record-form");
  const llmPromptEl = section.querySelector("#llm-prompt");
  const llmResponseEl = section.querySelector("#llm-response");
  const llmTaskIdEl = section.querySelector("#llm-task-id");
  const llmListEl = section.querySelector("#llm-record-list");
  const llmRefreshBtn = section.querySelector("#llm-record-refresh");

  // 获取工具记录相关元素
  const toolRecordFormEl = section.querySelector("#tool-record-form");
  const toolRecordToolEl = section.querySelector("#tool-record-tool");
  const toolRecordTaskEl = section.querySelector("#tool-record-task");
  const toolRecordSkillEl = section.querySelector("#tool-record-skill");
  const toolRecordReuseEl = section.querySelector("#tool-record-reuse");
  const toolRecordInputEl = section.querySelector("#tool-record-input");
  const toolRecordOutputEl = section.querySelector("#tool-record-output");
  const toolRecordListEl = section.querySelector("#tool-record-list");
  const toolRecordRefreshBtn = section.querySelector("#tool-record-refresh");

  // 获取配置相关元素
  const configFormEl = section.querySelector("#config-form");
  const configTrayEl = section.querySelector("#config-tray");
  const configPetEl = section.querySelector("#config-pet");
  const configPanelEl = section.querySelector("#config-panel");
  const configLoadBtn = section.querySelector("#config-load");
  const configStatusEl = section.querySelector("#config-status");

  // 获取权限相关元素
  const permissionsFormEl = section.querySelector("#permissions-form");
  const permissionsPathsEl = section.querySelector("#permissions-paths");
  const permissionsReadEl = section.querySelector("#permissions-read");
  const permissionsWriteEl = section.querySelector("#permissions-write");
  const permissionsExecEl = section.querySelector("#permissions-exec");
  const permissionsLoadBtn = section.querySelector("#permissions-load");
  const permissionsStatusEl = section.querySelector("#permissions-status");
  const permissionsActionsEl = section.querySelector("#permissions-actions");
  const permissionsToolsEl = section.querySelector("#permissions-tools");

  let permissionsState = {
    allowed_paths: [],
    allowed_ops: [],
    disabled_actions: [],
    disabled_tools: []
  };

  // 加载工具列表
  async function loadTools() {
    if (toolListEl) setListLoading(toolListEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchTools();
      renderList(toolListEl, result.items, (li, item) => {
        li.textContent = formatTemplate(UI_TEXT.LIST_ITEM_TOOL, {
          id: item.id,
          name: item.name,
          version: item.version
        });
      }, UI_TEXT.NO_DATA);
    } catch (error) {
      if (toolListEl) setListLoading(toolListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  // 加载LLM记录列表
  async function loadLlmRecords() {
    if (llmListEl) setListLoading(llmListEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchLlmRecords();
      renderList(llmListEl, result.items, (li, item) => {
        li.textContent = formatTemplate(UI_TEXT.LIST_ITEM_LLM, {
          id: item.id,
          task: item.task_id ?? UI_TEXT.DASH,
          prompt: item.prompt
        });
      }, UI_TEXT.NO_DATA);
    } catch (error) {
      if (llmListEl) setListLoading(llmListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  // 加载工具记录列表
  async function loadToolRecords() {
    if (toolRecordListEl) setListLoading(toolRecordListEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchToolRecords();
      renderList(toolRecordListEl, result.items, (li, item) => {
        li.textContent = formatTemplate(UI_TEXT.LIST_ITEM_TOOL_RECORD, {
          id: item.id,
          tool: item.tool_id,
          task: item.task_id ?? UI_TEXT.DASH,
          reuse: item.reuse ?? UI_TEXT.DASH
        });
      }, UI_TEXT.NO_DATA);
    } catch (error) {
      if (toolRecordListEl) setListLoading(toolRecordListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  function activateSystemPage(targetId, refresh = true) {
    if (!targetId) return;

    pages.forEach((page) => {
      page.classList.toggle("is-visible", page.id === targetId);
    });
    subnavItems.forEach((btn) => {
      btn.classList.toggle("is-active", btn.dataset.systemTarget === targetId);
    });

    if (!refresh) return;
    // 进入对应子页时，做一次“该页所需数据”的刷新（避免多卡片长页面造成的心智负担）
    switch (targetId) {
      case "system-page-tools":
        loadTools();
        break;
      case "system-page-records":
        loadLlmRecords();
        loadToolRecords();
        break;
      case "system-page-config":
        loadConfig();
        break;
      case "system-page-permissions":
        loadPermissions();
        break;
      default:
        break;
    }
  }

  // 绑定子标签切换
  subnavItems.forEach((btn) => {
    const targetId = btn.dataset.systemTarget;
    eventManager.add(btn, "click", () => activateSystemPage(targetId, true));
  });

  // 路径规范化
  function normalizePaths(value) {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
  }

  // 加载配置
  async function loadConfig() {
    if (configStatusEl) configStatusEl.textContent = UI_TEXT.LOADING;
    try {
      const result = await api.fetchConfig();
      if (configTrayEl) configTrayEl.checked = !!result.tray_enabled;
      if (configPetEl) configPetEl.checked = !!result.pet_enabled;
      if (configPanelEl) configPanelEl.checked = !!result.panel_enabled;
      if (configStatusEl) configStatusEl.textContent = UI_TEXT.CONFIG_STATUS_OK;
    } catch (error) {
      if (configStatusEl) configStatusEl.textContent = UI_TEXT.CONFIG_STATUS_FAIL;
    }
  }

  // 加载权限
  async function loadPermissions() {
    if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.LOADING;
    try {
      const result = await api.fetchPermissions();
      permissionsState = {
        allowed_paths: Array.isArray(result.allowed_paths) ? result.allowed_paths : [],
        allowed_ops: Array.isArray(result.allowed_ops) ? result.allowed_ops : [],
        disabled_actions: Array.isArray(result.disabled_actions) ? result.disabled_actions : [],
        disabled_tools: Array.isArray(result.disabled_tools) ? result.disabled_tools : []
      };
      if (permissionsPathsEl) {
        permissionsPathsEl.value = permissionsState.allowed_paths.join(", ");
      }
      if (permissionsReadEl) {
        permissionsReadEl.checked = permissionsState.allowed_ops.includes("read");
      }
      if (permissionsWriteEl) {
        permissionsWriteEl.checked = permissionsState.allowed_ops.includes("write");
      }
      if (permissionsExecEl) {
        permissionsExecEl.checked = permissionsState.allowed_ops.includes("execute");
      }
      renderActionToggles();
      renderToolToggles();
      if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_OK;
    } catch (error) {
      if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_FAIL;
    }
  }

  function normalizeDisabledList(value) {
    return Array.isArray(value) ? value.map((item) => String(item)) : [];
  }

  const savePermissionsDebounced = debounce(async () => {
    try {
      await api.updatePermissions({
        allowed_paths: permissionsState.allowed_paths,
        allowed_ops: permissionsState.allowed_ops,
        disabled_actions: permissionsState.disabled_actions,
        disabled_tools: permissionsState.disabled_tools
      });
      if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_OK;
    } catch (error) {
      if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_FAIL;
    }
  }, 400);

  function updateDisabledActions(actionName, enabled) {
    const disabled = new Set(normalizeDisabledList(permissionsState.disabled_actions));
    const key = String(actionName || "");
    if (!key) return;
    if (enabled) {
      disabled.delete(key);
    } else {
      disabled.add(key);
    }
    permissionsState.disabled_actions = Array.from(disabled);
    savePermissionsDebounced();
  }

  function updateDisabledTools(toolName, enabled) {
    const disabled = new Set(normalizeDisabledList(permissionsState.disabled_tools));
    const key = String(toolName || "");
    if (!key) return;
    if (enabled) {
      disabled.delete(key);
    } else {
      disabled.add(key);
    }
    permissionsState.disabled_tools = Array.from(disabled);
    savePermissionsDebounced();
  }

  async function renderActionToggles() {
    if (!permissionsActionsEl) return;
    setListLoading(permissionsActionsEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchActions();
      const actions = Array.isArray(result.items) ? result.items : [];
      const disabled = new Set(normalizeDisabledList(permissionsState.disabled_actions));
      renderList(permissionsActionsEl, actions, (li, item) => {
        const name = String(item || "");
        const content = document.createElement("div");
        content.className = "panel-list-item-content";
        content.textContent = name;
        const label = document.createElement("label");
        label.className = "panel-inline";
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = !disabled.has(name);
        const text = document.createElement("span");
        text.textContent = UI_TEXT.PERMISSIONS_TOGGLE_LABEL;
        label.appendChild(checkbox);
        label.appendChild(text);
        li.appendChild(content);
        li.appendChild(label);
        eventManager.add(checkbox, "change", () => updateDisabledActions(name, checkbox.checked));
      }, UI_TEXT.NO_DATA);
    } catch (error) {
      setListLoading(permissionsActionsEl, UI_TEXT.LOAD_FAIL);
    }
  }

  async function renderToolToggles() {
    if (!permissionsToolsEl) return;
    setListLoading(permissionsToolsEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchTools();
      const tools = Array.isArray(result.items) ? result.items : [];
      const disabled = new Set(normalizeDisabledList(permissionsState.disabled_tools));
      renderList(permissionsToolsEl, tools, (li, item) => {
        const name = String(item?.name || "");
        const version = String(item?.version || UI_TEXT.DASH);
        const content = document.createElement("div");
        content.className = "panel-list-item-content";
        content.textContent = `${name} (${version})`;
        const label = document.createElement("label");
        label.className = "panel-inline";
        const checkbox = document.createElement("input");
        checkbox.type = "checkbox";
        checkbox.checked = !disabled.has(name);
        const text = document.createElement("span");
        text.textContent = UI_TEXT.PERMISSIONS_TOGGLE_LABEL;
        label.appendChild(checkbox);
        label.appendChild(text);
        li.appendChild(content);
        li.appendChild(label);
        eventManager.add(checkbox, "change", () => updateDisabledTools(name, checkbox.checked));
      }, UI_TEXT.NO_DATA);
    } catch (error) {
      setListLoading(permissionsToolsEl, UI_TEXT.LOAD_FAIL);
    }
  }

  // 绑定搜索表单
  if (searchFormEl && searchQueryEl && searchResultEl) {
    attachFormClear(searchFormEl);
    const searchHandler = async (event) => {
      event.preventDefault();
      clearFormError(searchFormEl);
      const q = searchQueryEl.value.trim();
      if (!validateRequiredText(searchFormEl, q)) return;
      if (!validateOptionalNumber(searchFormEl, searchLimitEl?.value, INPUT_ATTRS.SEARCH_LIMIT)) {
        return;
      }
      const limit = searchLimitEl?.value ? Number(searchLimitEl.value) : null;
      searchResultEl.textContent = UI_TEXT.SEARCHING;
      try {
        const result = await api.searchUnified(q, limit);
        const memoryCount = result.memory?.length ?? 0;
        const skillsCount = result.skills?.length ?? 0;
        const graphCount = result.graph?.nodes?.length ?? 0;
        searchResultEl.textContent = formatTemplate(UI_TEXT.SEARCH_SUMMARY_TEMPLATE, {
          memory: memoryCount,
          skills: skillsCount,
          graph: graphCount
        });
        const searchItems = [];
        (result.memory || []).forEach((item) => {
          searchItems.push({
            type: UI_TEXT.SEARCH_TYPE_MEMORY_LABEL,
            text: item.content
          });
        });
        (result.skills || []).forEach((item) => {
          searchItems.push({
            type: UI_TEXT.SEARCH_TYPE_SKILLS_LABEL,
            text: item.name
          });
        });
        (result.graph?.nodes || []).forEach((item) => {
          searchItems.push({
            type: UI_TEXT.SEARCH_TYPE_GRAPH_LABEL,
            text: item.label
          });
        });
        renderList(
          searchResultListEl,
          searchItems,
          (li, item) => {
            li.textContent = formatTemplate(UI_TEXT.LIST_ITEM_SEARCH_RESULT, item);
          },
          UI_TEXT.NO_DATA
        );
        renderList(
          searchInjectionListEl,
          result.injection || [],
          (li, item) => {
            li.textContent = formatTemplate(UI_TEXT.LIST_ITEM_INJECTION, {
              type: item.type,
              weight: item.weight,
              snippet: item.snippet
            });
          },
          UI_TEXT.NO_DATA
        );
      } catch (error) {
        searchResultEl.textContent = UI_TEXT.SEARCH_FAIL;
        if (searchResultListEl) setListLoading(searchResultListEl, UI_TEXT.SEARCH_FAIL);
        if (searchInjectionListEl) setListLoading(searchInjectionListEl, UI_TEXT.SEARCH_FAIL);
      }
    };
    eventManager.add(searchFormEl, "submit", searchHandler);
  }

  // 绑定工具创建表单
  if (toolFormEl && toolNameEl && toolDescEl && toolVersionEl) {
    attachFormClear(toolFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(toolFormEl);
      const name = toolNameEl.value.trim();
      const description = toolDescEl.value.trim();
      const version = toolVersionEl.value.trim();
      if (!validateRequiredText(toolFormEl, name)) return;
      if (!validateRequiredText(toolFormEl, description)) return;
      if (!validateRequiredText(toolFormEl, version)) return;
      try {
        await api.createTool({ name, description, version });
        toolNameEl.value = "";
        toolDescEl.value = "";
        toolVersionEl.value = "";
        loadTools();
      } catch (error) {
        showFormError(toolFormEl, UI_TEXT.TOOL_CREATE_FAIL);
      }
    };
    eventManager.add(toolFormEl, "submit", submitHandler);
  }

  // 绑定工具刷新按钮
  if (toolListRefreshBtn) {
    eventManager.add(toolListRefreshBtn, "click", debounce(loadTools, 300));
  }

  // 绑定LLM记录表单
  if (llmFormEl && llmPromptEl && llmResponseEl) {
    attachFormClear(llmFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(llmFormEl);
      const payload = {
        prompt: llmPromptEl.value.trim(),
        response: llmResponseEl.value.trim()
      };
      if (!validateRequiredText(llmFormEl, payload.prompt)) return;
      if (!validateRequiredText(llmFormEl, payload.response)) return;
      if (llmTaskIdEl?.value) {
        if (!isNumberInRange(llmTaskIdEl.value, INPUT_ATTRS.LLM_TASK_ID)) {
          showFormError(llmFormEl, UI_TEXT.VALIDATION_NUMBER);
          return;
        }
        payload.task_id = Number(llmTaskIdEl.value);
      }
      try {
        await api.createLlmRecord(payload);
        llmPromptEl.value = "";
        llmResponseEl.value = "";
        if (llmTaskIdEl) llmTaskIdEl.value = "";
        loadLlmRecords();
      } catch (error) {
        showFormError(llmFormEl, UI_TEXT.LLM_RECORD_FAIL);
      }
    };
    eventManager.add(llmFormEl, "submit", submitHandler);
  }

  // 绑定LLM刷新按钮
  if (llmRefreshBtn) {
    eventManager.add(llmRefreshBtn, "click", debounce(loadLlmRecords, 300));
  }

  // 初始化复用选择器
  if (toolRecordReuseEl && toolRecordReuseEl.options.length === 0) {
    REUSE_OPTIONS.forEach((opt) => {
      const option = document.createElement("option");
      option.value = opt.value;
      option.textContent = opt.label;
      toolRecordReuseEl.appendChild(option);
    });
  }

  // 绑定工具记录表单
  if (toolRecordFormEl && toolRecordToolEl && toolRecordInputEl && toolRecordOutputEl) {
    attachFormClear(toolRecordFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(toolRecordFormEl);
      const toolId = Number(toolRecordToolEl.value);
      const input = toolRecordInputEl.value.trim();
      const output = toolRecordOutputEl.value.trim();
      if (!isNumberInRange(toolRecordToolEl.value, INPUT_ATTRS.TOOL_RECORD_TOOL)) {
        showFormError(toolRecordFormEl, UI_TEXT.VALIDATION_NUMBER);
        return;
      }
      if (!validateRequiredText(toolRecordFormEl, input)) return;
      if (!validateRequiredText(toolRecordFormEl, output)) return;
      const payload = { tool_id: toolId, input, output };
      if (toolRecordTaskEl?.value) {
        if (!isNumberInRange(toolRecordTaskEl.value, INPUT_ATTRS.TOOL_RECORD_TASK)) {
          showFormError(toolRecordFormEl, UI_TEXT.VALIDATION_NUMBER);
          return;
        }
        payload.task_id = Number(toolRecordTaskEl.value);
      }
      if (toolRecordSkillEl?.value) {
        if (!isNumberInRange(toolRecordSkillEl.value, INPUT_ATTRS.TOOL_RECORD_SKILL)) {
          showFormError(toolRecordFormEl, UI_TEXT.VALIDATION_NUMBER);
          return;
        }
        payload.skill_id = Number(toolRecordSkillEl.value);
      }
      if (toolRecordReuseEl?.value) {
        payload.reuse = toolRecordReuseEl.value === "true";
      }
      try {
        await api.createToolRecord(payload);
        toolRecordToolEl.value = "";
        if (toolRecordTaskEl) toolRecordTaskEl.value = "";
        if (toolRecordSkillEl) toolRecordSkillEl.value = "";
        if (toolRecordReuseEl) toolRecordReuseEl.value = "";
        toolRecordInputEl.value = "";
        toolRecordOutputEl.value = "";
        loadToolRecords();
      } catch (error) {
        showFormError(toolRecordFormEl, UI_TEXT.TOOL_RECORD_FAIL);
      }
    };
    eventManager.add(toolRecordFormEl, "submit", submitHandler);
  }

  // 绑定工具记录刷新按钮
  if (toolRecordRefreshBtn) {
    eventManager.add(toolRecordRefreshBtn, "click", debounce(loadToolRecords, 300));
  }

  // 绑定配置表单
  if (configFormEl) {
    attachFormClear(configFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(configFormEl);
      try {
        await api.updateConfig({
          tray_enabled: !!configTrayEl?.checked,
          pet_enabled: !!configPetEl?.checked,
          panel_enabled: !!configPanelEl?.checked
        });
        loadConfig();
      } catch (error) {
        if (configStatusEl) configStatusEl.textContent = UI_TEXT.CONFIG_STATUS_FAIL;
      }
    };
    eventManager.add(configFormEl, "submit", submitHandler);
  }

  // 绑定配置加载按钮
  if (configLoadBtn) {
    eventManager.add(configLoadBtn, "click", debounce(loadConfig, 300));
  }

  // 绑定权限表单
  if (permissionsFormEl) {
    attachFormClear(permissionsFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(permissionsFormEl);
      const ops = [];
      if (permissionsReadEl?.checked) ops.push("read");
      if (permissionsWriteEl?.checked) ops.push("write");
      if (permissionsExecEl?.checked) ops.push("execute");
      try {
        permissionsState.allowed_paths = permissionsPathsEl?.value
          ? normalizePaths(permissionsPathsEl.value)
          : [];
        permissionsState.allowed_ops = ops;
        await api.updatePermissions({
          allowed_paths: permissionsState.allowed_paths,
          allowed_ops: permissionsState.allowed_ops,
          disabled_actions: permissionsState.disabled_actions,
          disabled_tools: permissionsState.disabled_tools
        });
        loadPermissions();
      } catch (error) {
        if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_FAIL;
      }
    };
    eventManager.add(permissionsFormEl, "submit", submitHandler);
  }

  // 绑定权限加载按钮
  if (permissionsLoadBtn) {
    eventManager.add(permissionsLoadBtn, "click", debounce(loadPermissions, 300));
  }

  // 初始化加载
  const initialPage =
    pages.find((page) => page.classList.contains("is-visible"))?.id ||
    (pages[0] ? pages[0].id : null);
  if (initialPage) {
    activateSystemPage(initialPage, true);
  }

  return eventManager;
}

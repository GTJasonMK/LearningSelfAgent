// 系统标签页模块（资源中心：只读展示为主，写入操作收起到“高级”）

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
import { setListLoading, setListError, renderList } from "../list-utils.js";

function safeNumber(value, fallback = null) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function truncateText(value, maxChars = 140) {
  const text = String(value || "").trim();
  if (!text) return "";
  if (text.length <= maxChars) return text;
  return `${text.slice(0, maxChars).trimEnd()}...`;
}

function formatPercent(rate) {
  const n = safeNumber(rate, 0) || 0;
  return `${Math.round(n * 1000) / 10}%`;
}

function normalizeString(value) {
  return String(value || "").trim().toLowerCase();
}

function normalizeStringList(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function normalizeDisabledList(value) {
  return normalizeStringList(value);
}

/**
 * 绑定系统标签页
 * @param {HTMLElement} section - 标签页容器
 * @returns {Object} 事件管理器
 */
export function bind(section) {
  const eventManager = createEventManager();

  // 系统页内子标签（系统 -> 多页面）
  const subnavItems = Array.from(section.querySelectorAll(".system-subnav-item"));
  const pages = Array.from(section.querySelectorAll(":scope > .system-root .system-pages > .system-page"));

  // --- Resources ---
  const resourcesRefreshBtn = section.querySelector("#system-resources-refresh");

  const countDomainsEl = section.querySelector("#system-count-domains");
  const countSkillsEl = section.querySelector("#system-count-skills");
  const countSolutionsEl = section.querySelector("#system-count-solutions");
  const countToolsEl = section.querySelector("#system-count-tools");
  const countMemoryEl = section.querySelector("#system-count-memory");
  const countGraphNodesEl = section.querySelector("#system-count-graph-nodes");
  const countGraphEdgesEl = section.querySelector("#system-count-graph-edges");

  const domainsRefreshBtn = section.querySelector("#system-domains-refresh");
  const domainsStatusEl = section.querySelector("#system-domains-status");
  const domainsTreeEl = section.querySelector("#system-domains-tree");

  const solutionsRefreshBtn = section.querySelector("#system-solutions-refresh");
  const solutionsFormEl = section.querySelector("#system-solutions-form");
  const solutionsQueryEl = section.querySelector("#system-solutions-query");
  const solutionsStatusEl = section.querySelector("#system-solutions-status");
  const solutionsListEl = section.querySelector("#system-solutions-list");

  const toolsRefreshBtn = section.querySelector("#system-tools-refresh");
  const toolsFilterEl = section.querySelector("#system-tools-filter");
  const toolsListEl = section.querySelector("#system-tools-list");

  // --- Search ---
  const searchFormEl = section.querySelector("#search-form");
  const searchQueryEl = section.querySelector("#search-query");
  const searchLimitEl = section.querySelector("#search-limit");
  const searchResultEl = section.querySelector("#search-result");
  const searchResultListEl = section.querySelector("#search-result-list");
  const searchInjectionListEl = section.querySelector("#search-injection-list");

  // --- Governance ---
  const reuseRefreshBtn = section.querySelector("#system-reuse-refresh");
  const toolReuseSummaryEl = section.querySelector("#system-tool-reuse-summary");
  const toolReuseListEl = section.querySelector("#system-tool-reuse-list");
  const skillReuseSummaryEl = section.querySelector("#system-skill-reuse-summary");
  const skillReuseListEl = section.querySelector("#system-skill-reuse-list");

  // --- Advanced (Runtime) / Config ---
  const configFormEl = section.querySelector("#config-form");
  const configTrayEl = section.querySelector("#config-tray");
  const configPetEl = section.querySelector("#config-pet");
  const configPanelEl = section.querySelector("#config-panel");
  const configLoadBtn = section.querySelector("#config-load");
  const configStatusEl = section.querySelector("#config-status");

  // --- Advanced (Runtime) / Tool register ---
  const toolFormEl = section.querySelector("#tool-form");
  const toolNameEl = section.querySelector("#tool-name");
  const toolDescEl = section.querySelector("#tool-desc");
  const toolVersionEl = section.querySelector("#tool-version");

  // --- Advanced (Runtime) / Records (LLM + Tool calls) ---
  const llmFormEl = section.querySelector("#llm-record-form");
  const llmPromptEl = section.querySelector("#llm-prompt");
  const llmResponseEl = section.querySelector("#llm-response");
  const llmTaskIdEl = section.querySelector("#llm-task-id");
  const llmListEl = section.querySelector("#llm-record-list");
  const llmRefreshBtn = section.querySelector("#llm-record-refresh");

  const toolRecordFormEl = section.querySelector("#tool-record-form");
  const toolRecordToolEl = section.querySelector("#tool-record-tool");
  const toolRecordTaskEl = section.querySelector("#tool-record-task");
  const toolRecordSkillEl = section.querySelector("#tool-record-skill");
  const toolRecordReuseEl = section.querySelector("#tool-record-reuse");
  const toolRecordInputEl = section.querySelector("#tool-record-input");
  const toolRecordOutputEl = section.querySelector("#tool-record-output");
  const toolRecordListEl = section.querySelector("#tool-record-list");
  const toolRecordRefreshBtn = section.querySelector("#tool-record-refresh");

  // --- Permissions (Governance) ---
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

  let cachedTools = [];

  function normalizePermissionsConfigPayload(payload) {
    const data = payload && typeof payload === "object" ? payload : {};
    return {
      allowed_paths: normalizeStringList(data.allowed_paths),
      allowed_ops: normalizeStringList(data.allowed_ops),
      disabled_actions: normalizeDisabledList(data.disabled_actions),
      disabled_tools: normalizeDisabledList(data.disabled_tools)
    };
  }

  function normalizePermissionsMatrixPayload(payload) {
    const root = payload && typeof payload === "object" ? payload : {};
    const matrix = root.matrix && typeof root.matrix === "object" ? root.matrix : root;
    const ops = matrix.ops && typeof matrix.ops === "object" ? matrix.ops : {};
    return {
      allowed_paths: normalizeStringList(matrix.allowed_paths),
      disabled_actions: normalizeDisabledList(matrix.disabled_actions),
      disabled_tools: normalizeDisabledList(matrix.disabled_tools),
      ops: {
        write: Boolean(ops.write),
        execute: Boolean(ops.execute)
      }
    };
  }

  function mergeAllowedOpsWithMatrix(baseAllowedOps, matrixOps) {
    const set = new Set(normalizeStringList(baseAllowedOps));
    if (matrixOps && typeof matrixOps === "object") {
      if (matrixOps.write) {
        set.add("write");
      } else {
        set.delete("write");
      }
      if (matrixOps.execute) {
        set.add("execute");
      } else {
        set.delete("execute");
      }
    }
    return Array.from(set);
  }

  async function fetchMergedPermissionsState() {
    let baseConfig = null;
    let matrixConfig = null;

    await Promise.all([
      api
        .fetchPermissions()
        .then((result) => {
          baseConfig = normalizePermissionsConfigPayload(result);
        })
        .catch(() => {}),
      api
        .fetchPermissionsMatrix()
        .then((result) => {
          matrixConfig = normalizePermissionsMatrixPayload(result);
        })
        .catch(() => {})
    ]);

    if (!baseConfig && !matrixConfig) {
      throw new Error("load permissions failed");
    }

    const fallbackBase = baseConfig || {
      allowed_paths: [],
      allowed_ops: [],
      disabled_actions: [],
      disabled_tools: []
    };
    const fallbackMatrix = matrixConfig || {
      allowed_paths: null,
      disabled_actions: null,
      disabled_tools: null,
      ops: null
    };

    return {
      allowed_paths: Array.isArray(fallbackMatrix.allowed_paths)
        ? fallbackMatrix.allowed_paths
        : fallbackBase.allowed_paths,
      allowed_ops: mergeAllowedOpsWithMatrix(fallbackBase.allowed_ops, fallbackMatrix.ops),
      disabled_actions: Array.isArray(fallbackMatrix.disabled_actions)
        ? fallbackMatrix.disabled_actions
        : fallbackBase.disabled_actions,
      disabled_tools: Array.isArray(fallbackMatrix.disabled_tools)
        ? fallbackMatrix.disabled_tools
        : fallbackBase.disabled_tools
    };
  }

  // --- 页面切换 ---

  function activateSystemPage(targetId, refresh = true) {
    if (!targetId) return;

    pages.forEach((page) => {
      page.classList.toggle("is-visible", page.id === targetId);
    });
    subnavItems.forEach((btn) => {
      btn.classList.toggle("is-active", btn.dataset.systemTarget === targetId);
    });

    if (!refresh) return;
    switch (targetId) {
      case "system-page-resources":
        loadResources();
        break;
      case "system-page-governance":
        loadGovernance();
        break;
      case "system-page-runtime":
        loadConfig();
        break;
      default:
        break;
    }
  }

  subnavItems.forEach((btn) => {
    const targetId = btn.dataset.systemTarget;
    eventManager.add(btn, "click", () => activateSystemPage(targetId, true));
  });

  // --- Resources：概览 ---

  function setMetric(el, text) {
    if (!el) return;
    el.textContent = text == null ? UI_TEXT.DASH : String(text);
  }

  async function loadResourceOverview() {
    setMetric(countDomainsEl, UI_TEXT.LOADING);
    setMetric(countSkillsEl, UI_TEXT.LOADING);
    setMetric(countSolutionsEl, UI_TEXT.LOADING);
    setMetric(countToolsEl, UI_TEXT.LOADING);
    setMetric(countMemoryEl, UI_TEXT.LOADING);
    setMetric(countGraphNodesEl, UI_TEXT.LOADING);
    setMetric(countGraphEdgesEl, UI_TEXT.LOADING);

    try {
      const [domainsStats, skillsCatalog, memorySummary, graphSummary] = await Promise.all([
        api.fetchDomainsStats(),
        api.fetchSkillsCatalog(),
        api.fetchMemorySummary(),
        api.fetchGraph()
      ]);

      const domainsTotal = safeNumber(domainsStats?.total, null);
      setMetric(countDomainsEl, domainsTotal != null ? domainsTotal : UI_TEXT.DASH);

      const memoryCount = safeNumber(memorySummary?.items, null);
      setMetric(countMemoryEl, memoryCount != null ? memoryCount : UI_TEXT.DASH);

      const graphNodes = safeNumber(graphSummary?.nodes, null);
      const graphEdges = safeNumber(graphSummary?.edges, null);
      setMetric(countGraphNodesEl, graphNodes != null ? graphNodes : UI_TEXT.DASH);
      setMetric(countGraphEdgesEl, graphEdges != null ? graphEdges : UI_TEXT.DASH);

      const skillTypes = Array.isArray(skillsCatalog?.skill_types) ? skillsCatalog.skill_types : [];
      const typeMap = new Map();
      skillTypes.forEach((it) => {
        const key = String(it?.skill_type || "").trim();
        const count = safeNumber(it?.count, null);
        if (!key || count == null) return;
        typeMap.set(key, count);
      });

      const skillsCount = typeMap.has("methodology")
        ? typeMap.get("methodology")
        : safeNumber(skillsCatalog?.count, null);
      const solutionsCount = typeMap.has("solution") ? typeMap.get("solution") : null;

      setMetric(countSkillsEl, skillsCount != null ? skillsCount : UI_TEXT.DASH);
      setMetric(countSolutionsEl, solutionsCount != null ? solutionsCount : UI_TEXT.DASH);
    } catch (error) {
      setMetric(countDomainsEl, UI_TEXT.DASH);
      setMetric(countSkillsEl, UI_TEXT.DASH);
      setMetric(countSolutionsEl, UI_TEXT.DASH);
      setMetric(countMemoryEl, UI_TEXT.DASH);
      setMetric(countGraphNodesEl, UI_TEXT.DASH);
      setMetric(countGraphEdgesEl, UI_TEXT.DASH);
    }
  }

  // --- Resources：领域树 ---

  function renderDomainsTree(items) {
    if (!domainsTreeEl) return;
    const list = Array.isArray(items) ? items : [];
    if (list.length === 0) {
      domainsTreeEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.NO_DATA}</div>`;
      return;
    }
    domainsTreeEl.innerHTML = "";

    list.forEach((domain, idx) => {
      const details = document.createElement("details");
      details.className = "system-domain-node";
      details.open = idx === 0;

      const summary = document.createElement("summary");
      summary.className = "system-domain-summary";

      const title = document.createElement("span");
      title.className = "system-domain-title";
      const name = String(domain?.name || "").trim();
      const did = String(domain?.domain_id || "").trim();
      title.textContent = name || did || UI_TEXT.DASH;

      const right = document.createElement("span");
      right.className = "system-domain-meta";

      if (did) {
        const tag = document.createElement("span");
        tag.className = "panel-tag";
        tag.textContent = did;
        right.appendChild(tag);
      }

      const count = safeNumber(domain?.skill_count, 0) || 0;
      const countTag = document.createElement("span");
      countTag.className = "panel-tag";
      countTag.textContent = String(count);
      right.appendChild(countTag);

      const status = String(domain?.status || "").trim();
      if (status) {
        const statusTag = document.createElement("span");
        statusTag.className = `panel-tag ${status === "deprecated" ? "panel-tag--warning" : "panel-tag--accent"}`.trim();
        statusTag.textContent = status;
        right.appendChild(statusTag);
      }

      summary.appendChild(title);
      summary.appendChild(right);
      details.appendChild(summary);

      const desc = String(domain?.description || "").trim();
      if (desc) {
        const p = document.createElement("div");
        p.className = "system-domain-desc";
        p.textContent = truncateText(desc, 220);
        details.appendChild(p);
      }

      const children = Array.isArray(domain?.children) ? domain.children : [];
      if (children.length) {
        const ul = document.createElement("ul");
        ul.className = "system-domain-children";
        children.forEach((child) => {
          const li = document.createElement("li");
          li.className = "system-domain-child";
          const cname = String(child?.name || "").trim();
          const cdid = String(child?.domain_id || "").trim();
          const ccount = safeNumber(child?.skill_count, 0) || 0;
          li.textContent = `${cname || cdid || UI_TEXT.DASH} (${ccount})`;
          ul.appendChild(li);
        });
        details.appendChild(ul);
      }

      domainsTreeEl.appendChild(details);
    });
  }

  async function loadDomainsTree() {
    if (domainsStatusEl) domainsStatusEl.textContent = UI_TEXT.LOADING;
    if (domainsTreeEl) domainsTreeEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.LOADING}</div>`;
    try {
      const result = await api.fetchDomainsTree();
      renderDomainsTree(result?.items || []);
      if (domainsStatusEl) domainsStatusEl.textContent = formatTemplate(UI_TEXT.SYSTEM_DOMAINS_STATUS_TEMPLATE, { total: result?.total ?? "-" });
    } catch (error) {
      if (domainsStatusEl) domainsStatusEl.textContent = UI_TEXT.LOAD_FAIL;
      if (domainsTreeEl) domainsTreeEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.LOAD_FAIL}</div>`;
    }
  }

  // --- Resources：Solutions ---

  function renderSolutionsList(items) {
    renderList(
      solutionsListEl,
      items,
      (li, item) => {
        const header = document.createElement("div");
        header.className = "panel-list-item-content";
        const sid = item?.id != null ? `#${item.id}` : "#-";
        const name = String(item?.name || "").trim();
        header.textContent = `${sid} ${name || UI_TEXT.DASH}`.trim();

        const tags = document.createElement("span");
        tags.className = "panel-list-actions";

        const version = String(item?.version || "").trim();
        if (version) {
          const tag = document.createElement("span");
          tag.className = "panel-tag";
          tag.textContent = `v${version}`;
          tags.appendChild(tag);
        }

        const domainId = String(item?.domain_id || "").trim();
        if (domainId) {
          const tag = document.createElement("span");
          tag.className = "panel-tag";
          tag.textContent = domainId;
          tags.appendChild(tag);
        }

        const status = String(item?.status || "").trim();
        if (status) {
          const tag = document.createElement("span");
          tag.className = `panel-tag ${status === "deprecated" ? "panel-tag--warning" : status === "draft" ? "panel-tag--accent" : "panel-tag--success"}`.trim();
          tag.textContent = status;
          tags.appendChild(tag);
        }

        li.appendChild(header);
        li.appendChild(tags);

        const desc = String(item?.description || "").trim();
        if (desc) {
          const sub = document.createElement("div");
          sub.className = "panel-list-subtext";
          sub.textContent = truncateText(desc, 180);
          li.appendChild(sub);
        }
      },
      UI_TEXT.NO_DATA
    );
  }

  async function loadSolutionsList(queryText) {
    if (solutionsStatusEl) solutionsStatusEl.textContent = UI_TEXT.LOADING;
    if (solutionsListEl) setListLoading(solutionsListEl, UI_TEXT.LOADING);
    try {
      const result = await api.searchSkillsLibrary({
        q: queryText || null,
        skill_type: "solution",
        status: "approved",
        limit: 50,
        offset: 0
      });
      const items = Array.isArray(result?.items) ? result.items : [];
      renderSolutionsList(items);
      if (solutionsStatusEl) {
        solutionsStatusEl.textContent = formatTemplate(UI_TEXT.SYSTEM_SOLUTIONS_STATUS_TEMPLATE, { total: result?.total ?? items.length });
      }
    } catch (error) {
      if (solutionsStatusEl) solutionsStatusEl.textContent = UI_TEXT.LOAD_FAIL;
      if (solutionsListEl) setListError(solutionsListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  // --- Resources：Tools ---

  function toolDisabledSet() {
    return new Set(normalizeDisabledList(permissionsState.disabled_tools));
  }

  function renderToolsList(items) {
    const disabled = toolDisabledSet();
    renderList(
      toolsListEl,
      items,
      (li, item) => {
        const header = document.createElement("div");
        header.className = "panel-list-item-content";
        const tid = item?.id != null ? `#${item.id}` : "#-";
        const name = String(item?.name || "").trim();
        const version = String(item?.version || "").trim();
        header.textContent = `${tid} ${name || UI_TEXT.DASH}${version ? ` v${version}` : ""}`.trim();

        const tags = document.createElement("span");
        tags.className = "panel-list-actions";

        const reuse = item?.reuse_stats || null;
        const calls = safeNumber(reuse?.calls, 0) || 0;
        const reuseRate = reuse?.reuse_rate != null ? formatPercent(reuse.reuse_rate) : null;
        const callsTag = document.createElement("span");
        callsTag.className = "panel-tag";
        callsTag.textContent = formatTemplate(UI_TEXT.SYSTEM_TOOL_CALLS_TEMPLATE, { calls });
        tags.appendChild(callsTag);
        if (reuseRate) {
          const reuseTag = document.createElement("span");
          reuseTag.className = "panel-tag";
          reuseTag.textContent = formatTemplate(UI_TEXT.SYSTEM_TOOL_REUSE_TEMPLATE, { rate: reuseRate });
          tags.appendChild(reuseTag);
        }

        if (name) {
          const isDisabled = disabled.has(name);
          const st = document.createElement("span");
          st.className = `panel-tag ${isDisabled ? "panel-tag--warning" : "panel-tag--success"}`.trim();
          st.textContent = isDisabled ? UI_TEXT.SYSTEM_TOOL_STATUS_DISABLED : UI_TEXT.SYSTEM_TOOL_STATUS_ENABLED;
          tags.appendChild(st);
        }

        li.appendChild(header);
        li.appendChild(tags);

        const desc = String(item?.description || "").trim();
        if (desc) {
          const sub = document.createElement("div");
          sub.className = "panel-list-subtext";
          sub.textContent = truncateText(desc, 180);
          li.appendChild(sub);
        }
      },
      UI_TEXT.NO_DATA
    );
  }

  function filterTools(items, q) {
    const list = Array.isArray(items) ? items : [];
    const query = normalizeString(q);
    if (!query) return list;
    return list.filter((it) => {
      const name = normalizeString(it?.name);
      const desc = normalizeString(it?.description);
      return name.includes(query) || desc.includes(query);
    });
  }

  async function loadToolsList() {
    if (toolsListEl) setListLoading(toolsListEl, UI_TEXT.LOADING);
    try {
      const result = await api.fetchTools();
      cachedTools = Array.isArray(result?.items) ? result.items : [];
      setMetric(countToolsEl, cachedTools.length);
      const filtered = filterTools(cachedTools, toolsFilterEl?.value);
      renderToolsList(filtered);
    } catch (error) {
      setMetric(countToolsEl, UI_TEXT.DASH);
      if (toolsListEl) setListError(toolsListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  const renderToolsFilteredDebounced = debounce(() => {
    const filtered = filterTools(cachedTools, toolsFilterEl?.value);
    renderToolsList(filtered);
  }, 120);

  // --- Resources：整体刷新 ---

  async function fetchPermissionsStateShallow() {
    try {
      permissionsState = await fetchMergedPermissionsState();
    } catch (error) {
      // ignore
    }
  }

  async function loadResources() {
    await fetchPermissionsStateShallow();
    await Promise.all([
      loadResourceOverview(),
      loadDomainsTree(),
      loadSolutionsList(solutionsQueryEl?.value?.trim() || ""),
      loadToolsList()
    ]);
  }

  if (resourcesRefreshBtn) eventManager.add(resourcesRefreshBtn, "click", debounce(loadResources, 250));
  if (domainsRefreshBtn) eventManager.add(domainsRefreshBtn, "click", debounce(loadDomainsTree, 250));
  if (solutionsRefreshBtn) eventManager.add(solutionsRefreshBtn, "click", debounce(() => loadSolutionsList(solutionsQueryEl?.value?.trim() || ""), 250));
  if (toolsRefreshBtn) eventManager.add(toolsRefreshBtn, "click", debounce(loadToolsList, 250));

  if (toolsFilterEl) eventManager.add(toolsFilterEl, "input", renderToolsFilteredDebounced);

  if (solutionsFormEl && solutionsQueryEl) {
    attachFormClear(solutionsFormEl);
    eventManager.add(solutionsFormEl, "submit", (event) => {
      event.preventDefault();
      loadSolutionsList(solutionsQueryEl.value.trim());
    });
  }

  // --- Search：统一检索 ---
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
          const label = item.skill_type === "solution" ? UI_TEXT.SYSTEM_SKILL_TYPE_SOLUTION : UI_TEXT.SYSTEM_SKILL_TYPE_SKILL;
          searchItems.push({
            type: `${UI_TEXT.SEARCH_TYPE_SKILLS_LABEL}/${label}`,
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
        if (searchResultListEl) setListError(searchResultListEl, UI_TEXT.SEARCH_FAIL);
        if (searchInjectionListEl) setListError(searchInjectionListEl, UI_TEXT.SEARCH_FAIL);
      }
    };
    eventManager.add(searchFormEl, "submit", searchHandler);
  }

  // --- Governance：权限 ---

  function normalizePaths(value) {
    return value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
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
      setListError(permissionsActionsEl, UI_TEXT.LOAD_FAIL);
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
      setListError(permissionsToolsEl, UI_TEXT.LOAD_FAIL);
    }
  }

  async function loadPermissions() {
    if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.LOADING;
    try {
      permissionsState = await fetchMergedPermissionsState();
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
      await Promise.all([renderActionToggles(), renderToolToggles()]);
      if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_OK;
    } catch (error) {
      if (permissionsStatusEl) permissionsStatusEl.textContent = UI_TEXT.PERMISSIONS_STATUS_FAIL;
    }
  }

  if (permissionsFormEl) {
    attachFormClear(permissionsFormEl);
    eventManager.add(permissionsFormEl, "submit", async (event) => {
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
    });
  }

  if (permissionsLoadBtn) {
    eventManager.add(permissionsLoadBtn, "click", debounce(loadPermissions, 300));
  }

  // --- Governance：复用摘要 ---

  function setReuseSummary(el, text) {
    if (!el) return;
    el.textContent = text || UI_TEXT.DASH;
  }

  async function loadReuseSummary() {
    setReuseSummary(toolReuseSummaryEl, UI_TEXT.LOADING);
    setReuseSummary(skillReuseSummaryEl, UI_TEXT.LOADING);
    if (toolReuseListEl) setListLoading(toolReuseListEl, UI_TEXT.LOADING);
    if (skillReuseListEl) setListLoading(skillReuseListEl, UI_TEXT.LOADING);
    try {
      const [toolReuse, skillReuse] = await Promise.all([
        api.fetchToolReuseSummary({ limit: 12 }),
        api.fetchSkillReuseSummary({ limit: 12 })
      ]);

      const toolSummary = toolReuse?.summary || null;
      if (toolSummary) {
        setReuseSummary(
          toolReuseSummaryEl,
          formatTemplate(UI_TEXT.SYSTEM_REUSE_SUMMARY_TEMPLATE, {
            calls: toolSummary.total_calls ?? 0,
            reuse_calls: toolSummary.reuse_calls ?? 0,
            rate: formatPercent(toolSummary.reuse_rate ?? 0)
          })
        );
      } else {
        setReuseSummary(toolReuseSummaryEl, UI_TEXT.DASH);
      }

      const toolItems = Array.isArray(toolReuse?.by_tool) ? toolReuse.by_tool : [];
      renderList(
        toolReuseListEl,
        toolItems,
        (li, item) => {
          const content = document.createElement("div");
          content.className = "panel-list-item-content";
          const tid = item?.tool_id != null ? `tool#${item.tool_id}` : "tool#-";
          const calls = safeNumber(item?.calls, 0) || 0;
          const rate = formatPercent(item?.reuse_rate ?? 0);
          content.textContent = `${tid} calls:${calls} reuse:${rate}`;
          li.appendChild(content);
        },
        UI_TEXT.NO_DATA
      );

      const skillSummary = skillReuse?.summary || null;
      if (skillSummary) {
        setReuseSummary(
          skillReuseSummaryEl,
          formatTemplate(UI_TEXT.SYSTEM_REUSE_SUMMARY_TEMPLATE, {
            calls: skillSummary.total_calls ?? 0,
            reuse_calls: skillSummary.reuse_calls ?? 0,
            rate: formatPercent(skillSummary.reuse_rate ?? 0)
          })
        );
      } else {
        setReuseSummary(skillReuseSummaryEl, UI_TEXT.DASH);
      }

      const skillItems = Array.isArray(skillReuse?.by_skill) ? skillReuse.by_skill : [];
      renderList(
        skillReuseListEl,
        skillItems,
        (li, item) => {
          const content = document.createElement("div");
          content.className = "panel-list-item-content";
          const sid = item?.skill_id != null ? `skill#${item.skill_id}` : "skill#-";
          const calls = safeNumber(item?.calls, 0) || 0;
          const rate = formatPercent(item?.reuse_rate ?? 0);
          content.textContent = `${sid} calls:${calls} reuse:${rate}`;
          li.appendChild(content);
        },
        UI_TEXT.NO_DATA
      );
    } catch (error) {
      setReuseSummary(toolReuseSummaryEl, UI_TEXT.LOAD_FAIL);
      setReuseSummary(skillReuseSummaryEl, UI_TEXT.LOAD_FAIL);
      if (toolReuseListEl) setListError(toolReuseListEl, UI_TEXT.LOAD_FAIL);
      if (skillReuseListEl) setListError(skillReuseListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  async function loadGovernance() {
    await Promise.all([loadPermissions(), loadReuseSummary()]);
  }

  if (reuseRefreshBtn) eventManager.add(reuseRefreshBtn, "click", debounce(loadReuseSummary, 250));

  // --- Runtime：配置 ---

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

  if (configFormEl) {
    attachFormClear(configFormEl);
    eventManager.add(configFormEl, "submit", async (event) => {
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
    });
  }

  if (configLoadBtn) eventManager.add(configLoadBtn, "click", debounce(loadConfig, 300));

  // --- Advanced：工具注册 ---

  if (toolFormEl && toolNameEl && toolDescEl && toolVersionEl) {
    attachFormClear(toolFormEl);
    eventManager.add(toolFormEl, "submit", async (event) => {
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
        // 刷新资源页的工具列表与概览（即使用户不在资源页，也保持一致）
        loadToolsList();
        loadResourceOverview();
      } catch (error) {
        showFormError(toolFormEl, UI_TEXT.TOOL_CREATE_FAIL);
      }
    });
  }

  // --- Advanced：LLM 记录（开发调试）---

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
      if (llmListEl) setListError(llmListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  if (llmFormEl && llmPromptEl && llmResponseEl) {
    attachFormClear(llmFormEl);
    eventManager.add(llmFormEl, "submit", async (event) => {
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
    });
  }
  if (llmRefreshBtn) eventManager.add(llmRefreshBtn, "click", debounce(loadLlmRecords, 300));

  // --- Advanced：工具调用记录（开发调试）---

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
      if (toolRecordListEl) setListError(toolRecordListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  if (toolRecordReuseEl && toolRecordReuseEl.options.length === 0) {
    REUSE_OPTIONS.forEach((opt) => {
      const option = document.createElement("option");
      option.value = opt.value;
      option.textContent = opt.label;
      toolRecordReuseEl.appendChild(option);
    });
  }

  if (toolRecordFormEl && toolRecordToolEl && toolRecordInputEl && toolRecordOutputEl) {
    attachFormClear(toolRecordFormEl);
    eventManager.add(toolRecordFormEl, "submit", async (event) => {
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
    });
  }
  if (toolRecordRefreshBtn) eventManager.add(toolRecordRefreshBtn, "click", debounce(loadToolRecords, 300));

  // --- 初始化加载 ---
  const initialPage =
    pages.find((page) => page.classList.contains("is-visible"))?.id ||
    (pages[0] ? pages[0].id : null);
  if (initialPage) {
    activateSystemPage(initialPage, true);
  }

  return eventManager;
}

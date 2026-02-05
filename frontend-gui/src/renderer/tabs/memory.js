// 记忆标签页模块

import * as api from "../api.js";
import { UI_TEXT } from "../constants.js";
import { createEventManager, debounce } from "../utils.js";
import { AGENT_EVENT_NAME } from "../agent_events.js";
import {
  clearFormError,
  showFormError,
  validateRequiredText,
  attachFormClear
} from "../form-utils.js";

export function bind(section) {
  const eventManager = createEventManager();

  // 元素
  const countEl = section.querySelector("#memory-count");
  const formEl = section.querySelector("#memory-form");
  const contentEl = section.querySelector("#memory-content");
  const listEl = section.querySelector("#memory-list");
  const listRefreshBtn = section.querySelector("#memory-list-refresh");
  const searchFormEl = section.querySelector("#memory-search-form");
  const searchEl = section.querySelector("#memory-search");

  // 抽屉元素（用于查看完整记忆内容，避免列表被截断）
  const drawerOverlay = section.querySelector("#memory-drawer");
  const drawerCloseBtn = section.querySelector("#drawer-memory-close");
  const drawerTitle = section.querySelector("#drawer-memory-title");
  const drawerMeta = section.querySelector("#drawer-memory-meta");
  const drawerContent = section.querySelector("#drawer-memory-content");
  const drawerCopyBtn = section.querySelector("#drawer-memory-copy");
  const drawerDeleteBtn = section.querySelector("#drawer-memory-delete");

  // 状态
  let currentItemId = null;
  let currentItemContent = "";
  // 用于避免“无变化也刷新”导致的 UI 闪烁
  let lastSummaryKey = null;
  let lastCountValue = null;

  // 更新统计
  function buildSummaryKey(summary) {
    const count = Number(summary?.items ?? 0);
    // 兼容后端当前的 last_update（MVP 占位：最新记忆内容），用截断值做轻量变化检测
    const lastUpdate = String(summary?.last_update ?? "");
    const preview = lastUpdate.slice(0, 160);
    return `${count}|${preview}`;
  }

  async function updateStatus(options = {}) {
    const silent = options.silent === true;
    if (countEl && !silent) countEl.textContent = UI_TEXT.LOADING;
    try {
      const result = await api.fetchMemorySummary();
      const count = Number(result?.items ?? 0);
      const key = buildSummaryKey(result);
      lastSummaryKey = key;
      if (countEl) {
        // 静默刷新：无变化就不触发 DOM 更新，避免“看起来一直在刷新”
        if (silent && lastCountValue === count) return;
        countEl.textContent = count;
      }
      lastCountValue = count;
    } catch (error) {
      if (countEl) countEl.textContent = UI_TEXT.DASH;
    }
  }

  // --- 抽屉：查看详情 ---

  function closeDrawer() {
    if (drawerOverlay) drawerOverlay.classList.remove("is-visible");
    currentItemId = null;
    currentItemContent = "";
  }

  function safeText(value) {
    return String(value ?? "").trim();
  }

  async function copyToClipboard(text) {
    const content = String(text ?? "");
    if (!content) return;
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(content);
        alert(UI_TEXT.COPY_OK);
        return;
      }
    } catch (e) {
      // fallthrough
    }

    // 兼容旧环境：用隐藏 textarea 走 execCommand 复制
    try {
      const ta = document.createElement("textarea");
      ta.value = content;
      ta.setAttribute("readonly", "readonly");
      ta.style.position = "fixed";
      ta.style.left = "-9999px";
      ta.style.top = "0";
      document.body.appendChild(ta);
      ta.select();
      const ok = document.execCommand("copy");
      document.body.removeChild(ta);
      if (ok) alert(UI_TEXT.COPY_OK);
      else alert(UI_TEXT.COPY_FAIL);
    } catch (e) {
      alert(UI_TEXT.COPY_FAIL);
    }
  }

  function renderDrawerMeta(item) {
    if (!drawerMeta) return;
    drawerMeta.innerHTML = "";

    const idTag = document.createElement("span");
    idTag.className = "panel-tag";
    idTag.textContent = `#${item.id}`;

    const timeTag = document.createElement("span");
    timeTag.className = "panel-tag";
    timeTag.textContent = safeText(item.created_at) || UI_TEXT.DASH;

    const typeTag = document.createElement("span");
    typeTag.className = "panel-tag";
    typeTag.textContent = safeText(item.memory_type) || UI_TEXT.DASH;

    drawerMeta.appendChild(idTag);
    drawerMeta.appendChild(timeTag);
    drawerMeta.appendChild(typeTag);

    const tags = Array.isArray(item.tags) ? item.tags : [];
    tags.slice(0, 12).forEach((t) => {
      const tag = document.createElement("span");
      tag.className = "panel-tag";
      tag.textContent = safeText(t) || UI_TEXT.DASH;
      drawerMeta.appendChild(tag);
    });
  }

  async function openDrawer(itemId) {
    if (!drawerOverlay) return;
    currentItemId = Number(itemId);
    drawerOverlay.classList.add("is-visible");

    if (drawerTitle) drawerTitle.textContent = UI_TEXT.MEMORY_DETAIL_LOADING;
    if (drawerContent) drawerContent.textContent = UI_TEXT.MEMORY_DETAIL_LOADING;
    if (drawerMeta) drawerMeta.innerHTML = "";
    if (drawerDeleteBtn) drawerDeleteBtn.disabled = true;
    if (drawerCopyBtn) drawerCopyBtn.disabled = true;

    try {
      const result = await api.fetchMemoryItem(currentItemId);
      const item = result.item;
      if (!item) throw new Error("no item");

      currentItemContent = String(item.content ?? "");
      if (drawerTitle) drawerTitle.textContent = `#${item.id}`;
      renderDrawerMeta(item);
      if (drawerContent) drawerContent.textContent = currentItemContent || UI_TEXT.DASH;
      if (drawerDeleteBtn) drawerDeleteBtn.disabled = false;
      if (drawerCopyBtn) drawerCopyBtn.disabled = false;
    } catch (error) {
      if (drawerTitle) drawerTitle.textContent = UI_TEXT.MEMORY_DETAIL_LOAD_FAIL;
      if (drawerContent) drawerContent.textContent = UI_TEXT.MEMORY_DETAIL_LOAD_FAIL;
      if (drawerDeleteBtn) drawerDeleteBtn.disabled = true;
      if (drawerCopyBtn) drawerCopyBtn.disabled = true;
    }
  }

  // 渲染卡片
  function createMemoryCard(item) {
      const card = document.createElement("div");
      card.className = "note-card";
      card.onclick = () => openDrawer(item.id);
      
      const contentDiv = document.createElement("div");
      contentDiv.className = "note-content";
      contentDiv.textContent = item.content;
      
      const footer = document.createElement("div");
      footer.className = "note-footer";
      footer.innerHTML = `<span>#${item.id}</span>`;

      // 展开/收起：列表默认做行数截断，避免一条记忆把整个页面撑爆
      const toggleBtn = document.createElement("button");
      toggleBtn.className = "panel-button panel-button--small";
      toggleBtn.textContent = UI_TEXT.BUTTON_EXPAND;
      toggleBtn.style.fontSize = "10px";
      const contentText = String(item?.content || "");
      const isLong = contentText.length > 220 || contentText.includes("\n");
      if (!isLong) toggleBtn.classList.add("is-hidden");
      toggleBtn.onclick = (e) => {
          e.stopPropagation();
          const expanded = card.classList.toggle("is-expanded");
          toggleBtn.textContent = expanded ? UI_TEXT.BUTTON_COLLAPSE : UI_TEXT.BUTTON_EXPAND;
      };
      
      const delBtn = document.createElement("button");
      delBtn.className = "panel-button panel-button--small";
      delBtn.textContent = UI_TEXT.BUTTON_DELETE;
      delBtn.style.fontSize = "10px";
      delBtn.onclick = async (e) => {
          e.stopPropagation();
          if (confirm(UI_TEXT.MEMORY_DELETE_CONFIRM)) {
              await api.deleteMemoryItem(item.id);
              loadMemoryItems();
              updateStatus();
          }
      };
      
      footer.appendChild(toggleBtn);
      footer.appendChild(delBtn);
      card.appendChild(contentDiv);
      card.appendChild(footer);
      return card;
  }

  // 加载列表
  async function loadMemoryItems(options = {}) {
    const silent = options.silent === true;
    if (listEl && !silent) listEl.innerHTML = `<div class="panel-loading">${UI_TEXT.MEMORY_LOADING}</div>`;
    try {
      const result = await api.fetchMemoryItems();
      const items = (result.items || []).slice();
      // 记忆按最新优先展示，避免用户误以为“没写入/没刷新”
      items.sort((a, b) => Number(b?.id || 0) - Number(a?.id || 0));
      
      if (items.length === 0) {
          listEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.MEMORY_EMPTY}</div>`;
          return;
      }

      listEl.innerHTML = "";
      items.forEach(item => {
          listEl.appendChild(createMemoryCard(item));
      });
    } catch (error) {
      if (listEl) listEl.innerHTML = `<div class="panel-error">${UI_TEXT.LOAD_FAIL}</div>`;
    }
  }

  // 检索
  async function searchItems(q) {
      if (listEl) listEl.innerHTML = `<div class="panel-loading">${UI_TEXT.SEARCHING}</div>`;
      try {
        const result = await api.searchMemory(q);
        const items = result.items || [];
        
        listEl.innerHTML = "";
        if (items.length === 0) {
            listEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.MEMORY_SEARCH_EMPTY}</div>`;
        } else {
            items.forEach(item => listEl.appendChild(createMemoryCard(item)));
        }
      } catch (error) {
         if (listEl) listEl.innerHTML = `<div class="panel-error">${UI_TEXT.SEARCH_FAIL}</div>`;
      }
  }

  // 事件绑定
  if (formEl && contentEl) {
    attachFormClear(formEl);
    eventManager.add(formEl, "submit", async (e) => {
      e.preventDefault();
      clearFormError(formEl);
      const content = contentEl.value.trim();
      if (!validateRequiredText(formEl, content)) return;
      try {
        await api.createMemoryItem(content);
        contentEl.value = "";
        updateStatus();
        loadMemoryItems();
      } catch (error) {
        showFormError(formEl, UI_TEXT.WRITE_FAIL);
      }
    });
  }

  if (searchFormEl && searchEl) {
      eventManager.add(searchFormEl, "submit", (e) => {
          e.preventDefault();
          const q = searchEl.value.trim();
          if (q) searchItems(q);
          else loadMemoryItems();
      });
  }

  if (listRefreshBtn) {
    eventManager.add(listRefreshBtn, "click", debounce(() => {
        searchEl.value = "";
        loadMemoryItems();
    }, 300));
  }

  // 抽屉事件
  if (drawerCloseBtn) eventManager.add(drawerCloseBtn, "click", closeDrawer);
  if (drawerOverlay) {
    eventManager.add(drawerOverlay, "click", (e) => {
      if (e.target === drawerOverlay) closeDrawer();
    });
  }
  if (drawerCopyBtn) eventManager.add(drawerCopyBtn, "click", () => copyToClipboard(currentItemContent));
  if (drawerDeleteBtn) {
    eventManager.add(drawerDeleteBtn, "click", async () => {
      if (!currentItemId) return;
      if (!confirm(UI_TEXT.MEMORY_DELETE_CONFIRM)) return;
      await api.deleteMemoryItem(currentItemId);
      closeDrawer();
      await updateStatus();
      await loadMemoryItems();
    });
  }

  // 复用现有 SSE：当 Agent 在执行链路中写入记忆时，前端会收到 memory_item 事件
  eventManager.add(window, AGENT_EVENT_NAME, (event) => {
    const obj = event?.detail;
    if (!obj || obj.type !== "memory_item") return;
    const q = searchEl ? String(searchEl.value || "").trim() : "";
    if (q) return; // 用户搜索时不打断
    if (drawerOverlay && drawerOverlay.classList.contains("is-visible")) return; // 查看详情时不打断
    updateStatus({ silent: true });
    loadMemoryItems({ silent: true });
  });

  // 初始化
  updateStatus();
  loadMemoryItems();

  return {
    ...eventManager,
    removeAll: () => {
      eventManager.removeAll();
    }
  };
}

// 任务标签页模块

import * as api from "../api.js";
import {
  UI_TEXT,
  TASK_STATUS
} from "../constants.js";
import { createEventManager, formatTemplate, debounce } from "../utils.js";
import {
  clearFormError,
  showFormError,
  validateRequiredText,
  validateOptionalNumber,
  attachFormClear
} from "../form-utils.js";

/**
 * 绑定任务标签页
 */
export function bind(section, onStatusChange) {
  const eventManager = createEventManager();

  // 元素
  const formEl = section.querySelector("#task-form");
  const titleEl = section.querySelector("#task-title");
  const expectationEl = section.querySelector("#task-expectation");
  const listEl = section.querySelector("#task-list");
  const listRefreshBtn = section.querySelector("#task-list-refresh");
  const filterDateEl = section.querySelector("#task-filter-date");
  const filterDaysEl = section.querySelector("#task-filter-days");
  const filterClearBtn = section.querySelector("#task-filter-clear");

  // 抽屉元素
  const drawerOverlay = section.querySelector("#task-drawer");
  const drawerCloseBtn = section.querySelector("#drawer-close");
  const drawerTitle = section.querySelector("#drawer-title");
  const drawerStatus = section.querySelector("#drawer-status");
  const drawerTime = section.querySelector("#drawer-time");
  const drawerSteps = section.querySelector("#drawer-steps");
  const drawerTimeline = section.querySelector("#drawer-timeline");
  const actionDone = section.querySelector("#action-done");
  const actionCancel = section.querySelector("#action-cancel");
  const actionResume = section.querySelector("#action-resume");

  // 状态
  let currentTaskId = null;
  let executingTask = false;

  // --- 抽屉函数 ---

  function openDrawer(taskId) {
    currentTaskId = taskId;
    drawerOverlay.classList.add("is-visible");
    loadTaskDetail(taskId);
  }

  function closeDrawer() {
    drawerOverlay.classList.remove("is-visible");
    currentTaskId = null;
  }

  async function loadTaskDetail(taskId) {
    // 重置与加载状态
    drawerTitle.textContent = formatTemplate(UI_TEXT.TASK_TITLE_TEMPLATE, { id: taskId });
    drawerStatus.textContent = UI_TEXT.TASK_STATUS_LOADING;
    drawerSteps.innerHTML = `<div class="panel-loading">${UI_TEXT.TASK_STEPS_LOADING}</div>`;
    drawerTimeline.innerHTML = `<div class="panel-loading">${UI_TEXT.TASK_TIMELINE_LOADING}</div>`;

    try {
      // 并行获取
      const [record, timeline] = await Promise.all([
        api.fetchTaskRecord(taskId),
        api.fetchTaskTimeline(taskId)
      ]);

      const task = record.task;
      
      // 渲染头部
      drawerTitle.textContent = formatTemplate(UI_TEXT.TASK_TITLE_WITH_NAME_TEMPLATE, {
        id: task.id,
        title: task.title
      });
      drawerStatus.textContent = task.status;
      drawerStatus.className = `panel-tag panel-tag--${getStatusColor(task.status)}`;
      drawerTime.textContent = task.created_at || UI_TEXT.TASK_CREATED_RECENT;

      // 渲染步骤
      if (record.steps && record.steps.length > 0) {
        drawerSteps.innerHTML = "";
        record.steps.forEach((step, index) => {
            const stepEl = document.createElement("div");
            stepEl.className = "panel-list-item";
            stepEl.innerHTML = `
                <div class="panel-list-item-content">
                    <strong>${index + 1}. ${step.title || step.action_type}</strong>
                    <div style="font-size: 11px; color: var(--color_muted);">${step.status}</div>
                </div>
            `;
            drawerSteps.appendChild(stepEl);
        });
      } else {
        drawerSteps.innerHTML = `<div class="panel-empty-text">${UI_TEXT.TASK_STEPS_EMPTY}</div>`;
      }

      // 渲染时间线
      const events = timeline.events || timeline.items || [];
      if (events.length > 0) {
        drawerTimeline.innerHTML = "";
        events.forEach(event => {
            const time = event.timestamp || event.created_at || "";
            const type = event.type || event.event_type || UI_TEXT.TASK_EVENT_DEFAULT;
            const rawDetail = (event && typeof event === "object") ? (event.detail ?? event.data) : "";
            let detailText = "";
            if (rawDetail && typeof rawDetail === "object") {
              try {
                detailText = JSON.stringify(rawDetail, null, 2);
              } catch (e) {
                detailText = String(rawDetail);
              }
            } else {
              detailText = String(rawDetail ?? "");
            }

            const item = document.createElement("div");
            item.className = "timeline-item";

            const dot = document.createElement("div");
            dot.className = "timeline-dot";

            const timeEl = document.createElement("div");
            timeEl.className = "timeline-time";
            timeEl.textContent = time;

            const contentEl = document.createElement("div");
            contentEl.className = "timeline-content";

            const header = document.createElement("div");
            header.className = "timeline-content-header";

            const titleEl = document.createElement("strong");
            titleEl.textContent = type;

            const toggleBtn = document.createElement("button");
            toggleBtn.className = "panel-button panel-button--small timeline-toggle";
            toggleBtn.type = "button";
            toggleBtn.style.fontSize = "10px";
            toggleBtn.textContent = UI_TEXT.BUTTON_EXPAND;

            const detailEl = document.createElement("div");
            detailEl.className = "timeline-detail";
            detailEl.textContent = detailText || UI_TEXT.DASH;

            const isLong = detailText.length > 220 || detailText.includes("\n");
            if (!isLong) toggleBtn.classList.add("is-hidden");
            toggleBtn.onclick = (e) => {
              e.stopPropagation();
              const expanded = detailEl.classList.toggle("is-expanded");
              toggleBtn.textContent = expanded ? UI_TEXT.BUTTON_COLLAPSE : UI_TEXT.BUTTON_EXPAND;
            };

            header.appendChild(titleEl);
            header.appendChild(toggleBtn);
            contentEl.appendChild(header);
            contentEl.appendChild(detailEl);

            item.appendChild(dot);
            item.appendChild(timeEl);
            item.appendChild(contentEl);
            drawerTimeline.appendChild(item);
        });
      } else {
        drawerTimeline.innerHTML = `<div class="panel-empty-text">${UI_TEXT.TASK_TIMELINE_EMPTY}</div>`;
      }

    } catch (error) {
      console.error(error);
      drawerSteps.innerHTML = `<div class="panel-error">${UI_TEXT.TASK_DETAIL_LOAD_FAIL}</div>`;
      drawerTimeline.innerHTML = "";
    }
  }

  function getStatusColor(status) {
      switch(status) {
          case TASK_STATUS.DONE: return 'success';
          case TASK_STATUS.CANCELLED:
          case TASK_STATUS.FAILED: return 'error';
          case TASK_STATUS.RUNNING: return 'accent';
          case TASK_STATUS.STOPPED: return 'warning';
          default: return 'warning';
      }
  }

  // --- 操作 ---

  async function handleTaskAction(status) {
    if (!currentTaskId) return;
    try {
        await api.updateTask(currentTaskId, { status });
        // 刷新详情与列表
        await loadTaskDetail(currentTaskId);
        loadTasks();
        if (onStatusChange) onStatusChange();
    } catch (e) {
        alert(UI_TEXT.TASK_STATUS_UPDATE_FAIL);
    }
  }

  async function handleTaskResume() {
    if (!currentTaskId) return;
    if (executingTask) return;
    executingTask = true;
    if (actionResume) actionResume.disabled = true;
    try {
      // 直接复用后端 /tasks/{id}/execute：会跳过已完成步骤，继续执行 planned/running 的步骤。
      drawerStatus.textContent = TASK_STATUS.RUNNING;
      drawerStatus.className = `panel-tag panel-tag--${getStatusColor(TASK_STATUS.RUNNING)}`;
      await api.executeTask(currentTaskId, {});
      await loadTaskDetail(currentTaskId);
      loadTasks();
      if (onStatusChange) onStatusChange();
    } catch (e) {
      alert(UI_TEXT.TASK_EXECUTE_FAIL);
    } finally {
      executingTask = false;
      if (actionResume) actionResume.disabled = false;
    }
  }

  // --- 列表函数 ---

  async function loadTasks() {
    if (listEl) listEl.innerHTML = `<div class="panel-loading">${UI_TEXT.TASKS_LOADING}</div>`;
    try {
      const params = {};
      const dateValue = String(filterDateEl?.value || "").trim();
      const daysValue = String(filterDaysEl?.value || "").trim();
      if (dateValue) {
        params.date = dateValue;
        if (daysValue) params.days = Number(daysValue);
      }

      const result = await api.fetchTasks(params);
      const items = (result.items || []).slice();

      // 兜底：确保按时间倒序展示（最近优先）
      items.sort((a, b) => {
        const at = String(a?.created_at || "");
        const bt = String(b?.created_at || "");
        if (at && bt && at !== bt) return bt.localeCompare(at);
        const aid = Number(a?.id || 0);
        const bid = Number(b?.id || 0);
        return bid - aid;
      });
      
      if (items.length === 0) {
        const emptyText = dateValue ? UI_TEXT.TASKS_EMPTY_FILTER : UI_TEXT.TASKS_EMPTY;
        listEl.innerHTML = `<div class="panel-empty"><div class="panel-empty-text">${emptyText}</div></div>`;
        return;
      }

      listEl.innerHTML = "";
      items.forEach(item => {
        const card = document.createElement("div");
        card.className = "task-card";
        card.innerHTML = `
            <div class="task-card-header">
                <span class="task-card-id">#${item.id}</span>
                <span class="panel-tag panel-tag--${getStatusColor(item.status)}">${item.status}</span>
            </div>
            <div class="task-card-title">${item.title}</div>
            <div style="font-size: 11px; color: var(--color_muted);">
                ${item.created_at || UI_TEXT.TASK_CREATED_RECENT}
            </div>
        `;
        card.addEventListener("click", () => openDrawer(item.id));
        listEl.appendChild(card);
      });

    } catch (error) {
      if (listEl) listEl.innerHTML = `<div class="panel-error">${UI_TEXT.LOAD_FAIL}</div>`;
    }
  }

  // --- 事件绑定 ---

  // 抽屉
  eventManager.add(drawerCloseBtn, "click", closeDrawer);
  eventManager.add(drawerOverlay, "click", (e) => {
      if (e.target === drawerOverlay) closeDrawer();
  });
  eventManager.add(actionDone, "click", () => handleTaskAction(TASK_STATUS.DONE));
  eventManager.add(actionCancel, "click", () => handleTaskAction(TASK_STATUS.CANCELLED));
  eventManager.add(actionResume, "click", handleTaskResume);

  // 创建任务
  if (formEl && titleEl) {
    attachFormClear(formEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(formEl);
      const title = titleEl.value.trim();
      if (!validateRequiredText(formEl, title)) return;
      
      try {
        const expectationId = expectationEl?.value ? Number(expectationEl.value) : null;
        if (expectationId) {
          await api.createTaskWithExpectation(title, expectationId);
        } else {
          await api.createTask(title);
        }
        titleEl.value = "";
        if (expectationEl) expectationEl.value = "";
        
        loadTasks();
        if (onStatusChange) onStatusChange();
      } catch (error) {
        showFormError(formEl, UI_TEXT.SUBMIT_FAIL);
      }
    };
    eventManager.add(formEl, "submit", submitHandler);
  }

  // 刷新
  if (listRefreshBtn) {
    eventManager.add(listRefreshBtn, "click", debounce(loadTasks, 300));
  }

  // 筛选：按日期/连续天数查看
  const filterChanged = debounce(loadTasks, 200);
  if (filterDateEl) {
    eventManager.add(filterDateEl, "change", filterChanged);
  }
  if (filterDaysEl) {
    eventManager.add(filterDaysEl, "change", filterChanged);
  }
  if (filterClearBtn) {
    eventManager.add(filterClearBtn, "click", () => {
      if (filterDateEl) filterDateEl.value = "";
      if (filterDaysEl) filterDaysEl.value = "1";
      loadTasks();
    });
  }

  // 初始加载
  loadTasks();

  return eventManager;
}

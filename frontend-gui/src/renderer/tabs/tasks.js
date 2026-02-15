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
  const drawerNeedInput = section.querySelector("#drawer-need-input");
  const drawerNeedInputQuestion = section.querySelector("#drawer-need-input-question");
  const drawerNeedInputChoices = section.querySelector("#drawer-need-input-choices");
  const drawerNeedInputInput = section.querySelector("#drawer-need-input-value");
  const drawerNeedInputSend = section.querySelector("#drawer-need-input-send");
  const actionDone = section.querySelector("#action-done");
  const actionCancel = section.querySelector("#action-cancel");
  const actionResume = section.querySelector("#action-resume");

  // 状态
  let currentTaskId = null;
  let executingTask = false;
  let currentNeedInput = null;
  let resumingNeedInput = false;

  function normalizeNeedInputChoices(rawChoices) {
    if (!Array.isArray(rawChoices)) return [];
    const out = [];
    const seen = new Set();
    for (const rawItem of rawChoices) {
      let label = "";
      let value = "";
      if (typeof rawItem === "string") {
        label = String(rawItem || "").trim();
        value = label;
      } else if (rawItem && typeof rawItem === "object") {
        label = String(rawItem.label || "").trim();
        const rawValue = rawItem.value;
        value = String(rawValue != null ? rawValue : label).trim();
      }
      if (!label || !value) continue;
      const dedupeKey = `${label}__${value}`;
      if (seen.has(dedupeKey)) continue;
      seen.add(dedupeKey);
      out.push({ label, value });
      if (out.length >= 12) break;
    }
    return out;
  }

  function inferNeedInputChoices(question, kind) {
    const normalizedKind = String(kind || "").trim().toLowerCase();
    if (normalizedKind === "task_feedback") {
      return [{ label: "是", value: "是" }, { label: "否", value: "否" }];
    }
    const text = String(question || "").trim();
    if (!text) return [];
    const looksLikeYesNo = (
      text.includes("是否")
      || text.includes("可否")
      || text.includes("能否")
      || text.includes("要不要")
      || text.includes("确认")
      || text.endsWith("吗")
      || text.endsWith("吗？")
      || text.endsWith("吗?")
    );
    if (looksLikeYesNo) {
      return [{ label: "是", value: "是" }, { label: "否", value: "否" }];
    }
    return [];
  }

  function buildNeedInputPendingFromRunDetail(detail) {
    const run = detail?.run && typeof detail.run === "object" ? detail.run : null;
    if (!run) return null;
    const runId = Number(run.run_id);
    if (!Number.isFinite(runId) || runId <= 0) return null;
    const status = String(run.status || "").trim().toLowerCase();
    if (status !== TASK_STATUS.WAITING) return null;

    const taskId = Number(run.task_id) || null;
    const agentState = detail?.agent_state && typeof detail.agent_state === "object" ? detail.agent_state : null;
    const paused = agentState?.paused && typeof agentState.paused === "object" ? agentState.paused : null;
    if (!paused) {
      return {
        runId,
        taskId,
        question: UI_TEXT.TASK_NEED_INPUT_DEFAULT_QUESTION,
        kind: null,
        choices: []
      };
    }

    const question = String(paused.question || "").trim() || UI_TEXT.TASK_NEED_INPUT_DEFAULT_QUESTION;
    const kind = String(paused.kind || "").trim() || null;
    const choices = normalizeNeedInputChoices(paused.choices);
    const finalChoices = choices.length ? choices : inferNeedInputChoices(question, kind);
    return { runId, taskId, question, kind, choices: finalChoices };
  }

  function setNeedInputControlsDisabled(disabled) {
    const nextDisabled = !!disabled;
    if (drawerNeedInputInput) drawerNeedInputInput.disabled = nextDisabled;
    if (drawerNeedInputSend) drawerNeedInputSend.disabled = nextDisabled;
    if (drawerNeedInputChoices) {
      const buttons = drawerNeedInputChoices.querySelectorAll("button");
      for (const btn of Array.from(buttons)) {
        btn.disabled = nextDisabled;
      }
    }
  }

  async function consumeStreamResponse(response) {
    const reader = response?.body?.getReader ? response.body.getReader() : null;
    if (!reader) {
      await response.text();
      return;
    }
    try {
      while (true) {
        const { done } = await reader.read();
        if (done) break;
      }
    } finally {
      try {
        reader.releaseLock();
      } catch (error) {
      }
    }
  }

  async function handleNeedInputSubmit(rawValue = null) {
    const pending = currentNeedInput;
    if (!pending?.runId || !currentTaskId) return;
    if (resumingNeedInput) return;

    const text = String(rawValue != null ? rawValue : (drawerNeedInputInput?.value || "")).trim();
    if (!text) {
      if (drawerNeedInputInput) drawerNeedInputInput.focus();
      return;
    }

    resumingNeedInput = true;
    setNeedInputControlsDisabled(true);
    if (actionResume) actionResume.disabled = true;

    try {
      const response = await api.streamAgentResume({ run_id: Number(pending.runId), message: text });
      if (!response || !response.ok) {
        throw new Error("resume_failed");
      }
      await consumeStreamResponse(response);
      if (drawerNeedInputInput) drawerNeedInputInput.value = "";
      await loadTaskDetail(currentTaskId);
      loadTasks();
      if (onStatusChange) onStatusChange();
    } catch (error) {
      alert(UI_TEXT.TASK_NEED_INPUT_RESUME_FAIL);
      setNeedInputControlsDisabled(false);
      if (drawerNeedInputInput) drawerNeedInputInput.focus();
    } finally {
      resumingNeedInput = false;
    }
  }

  function renderNeedInputSection(pending) {
    currentNeedInput = pending && typeof pending === "object" ? pending : null;
    if (!drawerNeedInput) return;

    if (!currentNeedInput) {
      drawerNeedInput.classList.add("is-hidden");
      if (drawerNeedInputQuestion) drawerNeedInputQuestion.textContent = "";
      if (drawerNeedInputChoices) drawerNeedInputChoices.innerHTML = "";
      if (drawerNeedInputInput) drawerNeedInputInput.value = "";
      return;
    }

    drawerNeedInput.classList.remove("is-hidden");
    if (drawerNeedInputQuestion) {
      drawerNeedInputQuestion.textContent = String(currentNeedInput.question || UI_TEXT.TASK_NEED_INPUT_DEFAULT_QUESTION);
    }

    if (drawerNeedInputChoices) {
      drawerNeedInputChoices.innerHTML = "";
      const choices = Array.isArray(currentNeedInput.choices) ? currentNeedInput.choices : [];
      for (const choice of choices) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "panel-button panel-button--small task-choice-btn";
        btn.textContent = String(choice?.label || "").trim() || UI_TEXT.TASK_NEED_INPUT_CUSTOM;
        btn.addEventListener("click", () => {
          handleNeedInputSubmit(String(choice?.value || "").trim());
        });
        drawerNeedInputChoices.appendChild(btn);
      }
      const customBtn = document.createElement("button");
      customBtn.type = "button";
      customBtn.className = "panel-button panel-button--small task-choice-btn";
      customBtn.textContent = UI_TEXT.TASK_NEED_INPUT_CUSTOM;
      customBtn.addEventListener("click", () => {
        if (drawerNeedInputInput) drawerNeedInputInput.focus();
      });
      drawerNeedInputChoices.appendChild(customBtn);
    }

    setNeedInputControlsDisabled(resumingNeedInput);
  }

  // --- 抽屉函数 ---

  function openDrawer(taskId) {
    currentTaskId = taskId;
    drawerOverlay.classList.add("is-visible");
    renderNeedInputSection(null);
    loadTaskDetail(taskId);
  }

  function closeDrawer() {
    drawerOverlay.classList.remove("is-visible");
    currentTaskId = null;
    currentNeedInput = null;
    resumingNeedInput = false;
    renderNeedInputSection(null);
  }

  async function loadTaskDetail(taskId) {
    // 重置与加载状态
    drawerTitle.textContent = formatTemplate(UI_TEXT.TASK_TITLE_TEMPLATE, { id: taskId });
    drawerStatus.textContent = UI_TEXT.TASK_STATUS_LOADING;
    drawerSteps.innerHTML = `<div class="panel-loading">${UI_TEXT.TASK_STEPS_LOADING}</div>`;
    drawerTimeline.innerHTML = `<div class="panel-loading">${UI_TEXT.TASK_TIMELINE_LOADING}</div>`;
    renderNeedInputSection(null);

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

      const runRows = Array.isArray(record.runs) ? record.runs.slice() : [];
      runRows.sort((left, right) => Number(right?.id || 0) - Number(left?.id || 0));
      const waitingRun = runRows.find(
        (row) => String(row?.status || "").trim().toLowerCase() === TASK_STATUS.WAITING
      );
      const inspectRun = waitingRun || runRows[0] || null;
      let pendingNeedInput = null;
      if (inspectRun && Number(inspectRun.id) > 0) {
        try {
          const runDetail = await api.fetchAgentRunDetail(Number(inspectRun.id));
          pendingNeedInput = buildNeedInputPendingFromRunDetail(runDetail);
        } catch (error) {
          pendingNeedInput = null;
        }
      }
      renderNeedInputSection(pendingNeedInput);
      if (actionResume) {
        const blockedByNeedInput = !!pendingNeedInput;
        actionResume.disabled = !!(executingTask || blockedByNeedInput);
        actionResume.title = blockedByNeedInput ? UI_TEXT.TASK_RESUME_DISABLED_WAITING : "";
      }

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
      renderNeedInputSection(null);
      if (actionResume) {
        actionResume.disabled = !!executingTask;
        actionResume.title = "";
      }
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
    if (currentNeedInput?.runId) {
      if (drawerNeedInputInput) drawerNeedInputInput.focus();
      return;
    }
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
        const normalizedStatus = String(item?.status || "").trim().toLowerCase();
        const isWaiting = normalizedStatus === TASK_STATUS.WAITING;
        const waitingBadge = isWaiting
          ? `<span class="task-card-waiting-badge" title="${UI_TEXT.TASK_CARD_WAITING_TITLE}">${UI_TEXT.TASK_CARD_WAITING_BADGE}</span>`
          : "";
        const waitingHint = isWaiting
          ? `<div class="task-card-waiting-hint">${UI_TEXT.TASK_CARD_WAITING_HINT}</div>`
          : "";
        const card = document.createElement("div");
        card.className = isWaiting ? "task-card task-card--waiting" : "task-card";
        card.innerHTML = `
            <div class="task-card-header">
                <span class="task-card-id">#${item.id}</span>
                <div class="task-card-header-right">
                    ${waitingBadge}
                    <span class="panel-tag panel-tag--${getStatusColor(item.status)}">${item.status}</span>
                </div>
            </div>
            <div class="task-card-title">${item.title}</div>
            ${waitingHint}
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
  eventManager.add(drawerNeedInputSend, "click", () => handleNeedInputSubmit());
  eventManager.add(drawerNeedInputInput, "keydown", (event) => {
    if (event.key !== "Enter") return;
    event.preventDefault();
    handleNeedInputSubmit();
  });

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

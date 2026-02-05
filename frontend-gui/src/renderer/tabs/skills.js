// 技能标签页模块

import * as api from "../api.js";
import { UI_TEXT } from "../constants.js";
import { createEventManager, debounce } from "../utils.js";
import {
  clearFormError,
  showFormError,
  validateRequiredText,
  attachFormClear
} from "../form-utils.js";

function parseCommaSeparated(value) {
  if (!value) return [];
  return value.split(",").map(s => s.trim()).filter(Boolean);
}

function parseLineSeparated(value) {
  if (!value) return [];
  return value.split("\n").map(s => s.trim()).filter(Boolean);
}

export function bind(section) {
  const eventManager = createEventManager();

  // 元素
  const countEl = section.querySelector("#skills-count");
  const refreshBtn = section.querySelector("#skills-refresh");
  const listEl = section.querySelector("#skills-list");
  const searchFormEl = section.querySelector("#skills-search-form");
  const searchEl = section.querySelector("#skills-search");
  
  // 创建面板
  const createBtn = section.querySelector("#skills-create-btn");
  const createPanel = section.querySelector("#skill-create-panel");
  const createCancelBtn = section.querySelector("#skill-create-cancel");
  const fullFormEl = section.querySelector("#skills-full-form");

  // 抽屉
  const drawerOverlay = section.querySelector("#skill-drawer");
  const drawerClose = section.querySelector("#drawer-skill-close");
  const drawerTitle = section.querySelector("#drawer-skill-title");
  const drawerDesc = section.querySelector("#drawer-skill-desc");
  const drawerMeta = section.querySelector("#drawer-skill-meta");
  const drawerInputs = section.querySelector("#drawer-skill-inputs");
  const drawerOutputs = section.querySelector("#drawer-skill-outputs");
  const drawerSteps = section.querySelector("#drawer-skill-steps");
  const actionDelete = section.querySelector("#action-delete-skill");

  let currentSkillId = null;

  // --- 函数 ---

  function truncateText(value, maxChars = 160) {
    const text = String(value || "").trim();
    if (!text) return "";
    if (text.length <= maxChars) return text;
    return `${text.slice(0, maxChars).trimEnd()}...`;
  }

  function formatActionSummary(detailText) {
    const raw = String(detailText || "").trim();
    if (!raw) return "";
    let obj = null;
    try {
      obj = JSON.parse(raw);
    } catch (e) {
      obj = null;
    }
    if (!obj || typeof obj !== "object") return truncateText(raw, 160);

    const type = String(obj.type || obj.action?.type || "").trim();
    const payload = obj.payload || obj.action?.payload || {};

    if (type === "shell_command") {
      const cmd = payload?.command;
      const wd = payload?.workdir;
      return `shell: ${truncateText(typeof cmd === "string" ? cmd : JSON.stringify(cmd), 120)}${wd ? ` @ ${wd}` : ""}`;
    }
    if (type === "file_write") {
      const p = payload?.path;
      return `file: ${truncateText(p, 180)}`;
    }
    if (type === "tool_call") {
      const toolName = payload?.tool_name || payload?.tool_id || "";
      const input = payload?.input;
      return `tool: ${toolName} | in: ${truncateText(input, 120)}`;
    }
    if (type === "llm_call") {
      const tid = payload?.template_id;
      const prompt = payload?.prompt;
      return `llm: ${tid != null ? `template:${tid} ` : ""}${truncateText(prompt, 180)}`;
    }
    if (type === "task_output") {
      const content = payload?.content;
      return `output: ${truncateText(content, 200)}`;
    }
    if (type === "memory_write") {
      const content = payload?.content;
      return `memory: ${truncateText(content, 200)}`;
    }
    if (type === "user_prompt") {
      const q = payload?.question;
      return `ask: ${truncateText(q, 200)}`;
    }

    return `${type || "action"}: ${truncateText(JSON.stringify(payload), 200)}`;
  }

  function createSkillStepRow(step, index) {
    const row = document.createElement("div");
    row.className = "panel-list-item";

    const titleEl = document.createElement("div");
    titleEl.className = "panel-list-item-content";

    let title = "";
    let detail = "";
    if (typeof step === "string") {
      title = step;
    } else if (step && typeof step === "object") {
      title = String(step.title || step.name || step.brief || "").trim();
      detail = String(step.detail || step.action || "").trim();
    } else {
      title = String(step || "").trim();
    }

    titleEl.textContent = `${index}. ${title || UI_TEXT.DASH}`;
    row.appendChild(titleEl);

    const summary = formatActionSummary(detail);
    if (summary) {
      const sub = document.createElement("div");
      sub.style.fontSize = "11px";
      sub.style.color = "var(--color_muted)";
      sub.style.marginTop = "4px";
      sub.style.whiteSpace = "pre-wrap";
      sub.style.wordBreak = "break-word";
      sub.textContent = summary;
      row.appendChild(sub);
    }

    return row;
  }

  function normalizeCategory(value) {
    const v = String(value || "").trim();
    return v || "misc";
  }

  function splitCategoryParts(category) {
    return normalizeCategory(category).split(".").map((s) => s.trim()).filter(Boolean);
  }

  function groupSkillsByCategory(skills) {
    // 结构：top -> { total, children: Map(leaf -> list) }
    const groups = new Map();
    (skills || []).forEach((skill) => {
      const parts = splitCategoryParts(skill.category);
      const top = parts[0] || "misc";
      const leaf = parts.length > 1 ? parts.slice(1).join(".") : top;

      if (!groups.has(top)) groups.set(top, { total: 0, children: new Map() });
      const group = groups.get(top);
      group.total += 1;

      if (!group.children.has(leaf)) group.children.set(leaf, []);
      group.children.get(leaf).push(skill);
    });
    return groups;
  }

  function sortGroupKeys(keys) {
    // misc 放最后，其余按字母序；避免 UI 太“随机”
    return [...keys].sort((a, b) => {
      if (a === b) return 0;
      if (a === "misc") return 1;
      if (b === "misc") return -1;
      return String(a).localeCompare(String(b));
    });
  }

  async function updateStatus() {
    if (countEl) countEl.textContent = UI_TEXT.LOADING;
    try {
      const result = await api.fetchSkillsList();
      if (countEl) countEl.textContent = result.count ?? 0;
    } catch (e) { /* ignore */ }
  }

  function createSkillCard(skill) {
    const card = document.createElement("div");
    card.className = "skill-card";
    card.innerHTML = `
      <div class="skill-header">
          <span class="skill-title">${skill.name}</span>
          <span class="panel-tag">${skill.scope || UI_TEXT.SKILLS_SCOPE_DEFAULT}</span>
      </div>
      <div class="skill-desc">${skill.description || UI_TEXT.SKILLS_NO_DESC}</div>
      <div class="skill-tags">
          <span style="font-size: 10px; color: var(--color_muted);">v${skill.version || UI_TEXT.SKILLS_VERSION_DEFAULT}</span>
      </div>
    `;
    card.onclick = () => openDrawer(skill.id);
    return card;
  }

  function renderSkillsGrouped(items) {
    if (!listEl) return;
    listEl.innerHTML = "";

    const groups = groupSkillsByCategory(items);
    const topKeys = sortGroupKeys(groups.keys());
    if (topKeys.length === 0) {
      listEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.SKILLS_EMPTY}</div>`;
      return;
    }

    topKeys.forEach((top, idx) => {
      const group = groups.get(top);
      if (!group) return;

      const details = document.createElement("details");
      details.className = "skill-group";
      // 默认展开第一组，减少“点一下才看到内容”的摩擦
      details.open = idx === 0;

      const summary = document.createElement("summary");
      summary.innerHTML = `
        <span>${top}</span>
        <span class="panel-tag">${group.total}</span>
      `;
      details.appendChild(summary);

      const body = document.createElement("div");
      body.className = "skill-group-body";

      const leafKeys = sortGroupKeys(group.children.keys());
      leafKeys.forEach((leaf) => {
        const list = group.children.get(leaf) || [];
        if (!list.length) return;

        const section = document.createElement("div");

        const header = document.createElement("div");
        header.className = "skill-subgroup-header";
        header.innerHTML = `
          <div class="skill-subgroup-title">${leaf}</div>
          <span class="panel-tag">${list.length}</span>
        `;
        section.appendChild(header);

        const grid = document.createElement("div");
        grid.className = "skill-card-list skill-card-list--nested";
        list.forEach((skill) => grid.appendChild(createSkillCard(skill)));
        section.appendChild(grid);

        body.appendChild(section);
      });

      details.appendChild(body);
      listEl.appendChild(details);
    });
  }

  async function loadSkills() {
    if (listEl) listEl.innerHTML = `<div class="panel-loading">${UI_TEXT.SKILLS_LOADING}</div>`;
    try {
      const result = await api.fetchSkillsList();
      const items = result.items || [];
      
      renderSkillsGrouped(items);

    } catch (error) {
      if (listEl) listEl.innerHTML = `<div class="panel-error">${UI_TEXT.LOAD_FAIL}</div>`;
    }
  }

  // 抽屉
  async function openDrawer(skillId) {
      currentSkillId = skillId;
      drawerOverlay.classList.add("is-visible");
      
      // 重置
      drawerTitle.textContent = UI_TEXT.SKILLS_DETAIL_LOADING;
      drawerDesc.textContent = UI_TEXT.SKILLS_NO_DESC;
      drawerMeta.innerHTML = "";
      drawerInputs.textContent = UI_TEXT.DASH;
      drawerOutputs.textContent = UI_TEXT.DASH;
      drawerSteps.innerHTML = "";
      
      try {
          const result = await api.fetchSkillDetail(skillId);
          const skill = result.item;
          
          drawerTitle.textContent = skill.name;
          drawerDesc.textContent = skill.description || UI_TEXT.SKILLS_NO_DESC;
          
          drawerMeta.innerHTML = `
             <span class="panel-tag">${skill.scope || UI_TEXT.SKILLS_SCOPE_DEFAULT}</span>
             <span class="panel-tag">${skill.category || "misc"}</span>
             <span class="panel-tag">v${skill.version || UI_TEXT.SKILLS_VERSION_DEFAULT}</span>
          `;
          
          drawerInputs.textContent = (skill.inputs || []).join(", ") || UI_TEXT.DASH;
          drawerOutputs.textContent = (skill.outputs || []).join(", ") || UI_TEXT.DASH;
          
          if (skill.steps && skill.steps.length) {
              drawerSteps.innerHTML = "";
              skill.steps.forEach((step, i) => {
                drawerSteps.appendChild(createSkillStepRow(step, i + 1));
              });
          } else {
              drawerSteps.innerHTML = `<div class="panel-empty-text">${UI_TEXT.SKILLS_NO_STEPS}</div>`;
          }
          
      } catch (e) {
          drawerTitle.textContent = UI_TEXT.SKILLS_DETAIL_LOAD_FAIL;
      }
  }

  function closeDrawer() {
      drawerOverlay.classList.remove("is-visible");
      currentSkillId = null;
  }

  // --- 事件绑定 ---

  eventManager.add(drawerClose, "click", closeDrawer);
  eventManager.add(drawerOverlay, "click", (e) => {
      if (e.target === drawerOverlay) closeDrawer();
  });

  eventManager.add(actionDelete, "click", async () => {
      if (!currentSkillId) return;
      if (confirm(UI_TEXT.SKILLS_DELETE_CONFIRM)) {
          await api.deleteSkill(currentSkillId);
          closeDrawer();
          loadSkills();
          updateStatus();
      }
  });

  // 创建面板开关
  if (createBtn && createPanel) {
      eventManager.add(createBtn, "click", () => {
          createPanel.style.display = "block";
          fullFormEl.scrollIntoView({ behavior: "smooth" });
      });
      eventManager.add(createCancelBtn, "click", () => {
          createPanel.style.display = "none";
      });
  }

  // 创建表单
  if (fullFormEl) {
      const nameEl = fullFormEl.querySelector("#skills-full-name");
      const descEl = fullFormEl.querySelector("#skills-full-description");
      // 若需重置其他字段可按ID获取，基础清理由 FormClear 处理
      
      attachFormClear(fullFormEl);
      eventManager.add(fullFormEl, "submit", async (e) => {
          e.preventDefault();
          clearFormError(fullFormEl);
          
          const name = nameEl.value.trim();
          if (!validateRequiredText(fullFormEl, name)) return;

          // 组装提交数据
          const payload = { 
              name,
              description: descEl.value.trim(),
              scope: fullFormEl.querySelector("#skills-full-scope").value.trim(),
              version: fullFormEl.querySelector("#skills-full-version").value.trim(),
              inputs: parseCommaSeparated(fullFormEl.querySelector("#skills-full-inputs").value),
              outputs: parseCommaSeparated(fullFormEl.querySelector("#skills-full-outputs").value),
              steps: parseLineSeparated(fullFormEl.querySelector("#skills-full-steps").value)
          };

          try {
              await api.createSkillFull(payload);
              createPanel.style.display = "none";
              // 表单通常由 attachFormClear 清理，也可手动清空
              fullFormEl.reset();
              loadSkills();
              updateStatus();
          } catch (error) {
              showFormError(fullFormEl, UI_TEXT.WRITE_FAIL);
          }
      });
  }

  // 检索
  if (searchFormEl) {
      eventManager.add(searchFormEl, "submit", async (e) => {
          e.preventDefault();
          const q = searchEl.value.trim();
          if (!q) {
              loadSkills();
              return;
          }
          
          if (listEl) listEl.innerHTML = `<div class="panel-loading">${UI_TEXT.SKILLS_SEARCHING}</div>`;
          try {
              const result = await api.searchSkills(q);
              const items = result.items || [];
              if (items.length === 0) {
                listEl.innerHTML = `<div class="panel-empty-text">${UI_TEXT.SKILLS_SEARCH_EMPTY}</div>`;
              } else {
                renderSkillsGrouped(items);
              }
          } catch(e) {
              listEl.innerHTML = `<div class="panel-error">${UI_TEXT.SKILLS_SEARCH_FAIL}</div>`;
          }
      });
  }
  
  if (refreshBtn) {
      eventManager.add(refreshBtn, "click", debounce(() => {
          loadSkills();
          updateStatus();
      }, 300));
  }

  // 初始化
  updateStatus();
  loadSkills();

  return eventManager;
}

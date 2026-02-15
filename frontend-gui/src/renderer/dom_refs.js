// DOM 引用集中管理：避免在主逻辑文件里散落大量 querySelector/getElementById。
// 说明：仅负责“查找并返回元素引用”，不做业务逻辑与事件绑定。

export function getPetDomRefs() {
  const petEl = document.querySelector(".pet");
  const imageEl = document.querySelector(".pet-image");
  const bubbleEl = document.querySelector(".pet-bubble");
  const bubbleContentEl = bubbleEl?.querySelector?.(".pet-bubble-content") || bubbleEl;
  const bubbleActionsEl = bubbleEl?.querySelector?.(".pet-bubble-actions") || null;
  const bubbleYesEl = bubbleEl?.querySelector?.(".pet-bubble-action-yes") || null;
  const bubbleNoEl = bubbleEl?.querySelector?.(".pet-bubble-action-no") || null;
  const chatEl = document.querySelector(".pet-chat");
  const chatInputEl = document.querySelector(".pet-chat-input");
  const chatSendEl = document.querySelector(".pet-chat-send");
  const planEl = document.querySelector(".pet-plan");
  const planSlotEls = Array.from(document.querySelectorAll(".pet-plan-slot"));

  return {
    petEl,
    imageEl,
    bubbleEl,
    bubbleContentEl,
    bubbleActionsEl,
    bubbleYesEl,
    bubbleNoEl,
    chatEl,
    chatInputEl,
    chatSendEl,
    planEl,
    planSlotEls
  };
}

export function getPanelDomRefs() {
  // 更新选择器以匹配新的侧边栏导航
  const tabs = document.querySelectorAll(".nav-item");
  const sections = document.querySelectorAll(".panel-section");

  // 顶栏：世界/状态标签（首页隐藏）
  const topbarTabsEl = document.querySelector(".panel-topbar-tabs");

  // 顶层页面：世界 / 状态
  const pageHomeEl = document.getElementById("page-home");
  const pageStateEl = document.getElementById("page-state");
  const pageWorldEl = document.getElementById("page-world");
  const pageTabWorldBtn = document.getElementById("page-tab-world");
  const pageTabStateBtn = document.getElementById("page-tab-state");
  const panelEnterMainBtn = document.getElementById("panel-enter-main");

  // 世界页组件
  const worldTaskPlanEl = document.getElementById("world-task-plan");
  const worldEvalPlanEl = document.getElementById("world-eval-plan");
  const worldResultEl = document.getElementById("world-result");
  const worldChatEl = document.getElementById("world-chat");
  const worldThoughtsEl = document.getElementById("world-thoughts");
  const worldChoicesEl = document.getElementById("world-choices");
  const worldUploadBtn = document.getElementById("world-upload");
  const worldInputEl = document.getElementById("world-input");
  const worldSendBtn = document.getElementById("world-send");

  return {
    tabs,
    sections,
    topbarTabsEl,
    pageHomeEl,
    pageStateEl,
    pageWorldEl,
    pageTabWorldBtn,
    pageTabStateBtn,
    panelEnterMainBtn,
    worldTaskPlanEl,
    worldEvalPlanEl,
    worldResultEl,
    worldChatEl,
    worldThoughtsEl,
    worldChoicesEl,
    worldUploadBtn,
    worldInputEl,
    worldSendBtn
  };
}

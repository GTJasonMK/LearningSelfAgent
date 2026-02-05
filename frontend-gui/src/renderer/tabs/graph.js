// 图谱标签页模块

import * as api from "../api.js";
import { UI_TEXT, INPUT_ATTRS } from "../constants.js";
import { createEventManager, formatTemplate, debounce } from "../utils.js";
import {
  clearFormError,
  showFormError,
  validateRequiredText,
  isNumberInRange,
  attachFormClear
} from "../form-utils.js";
import { setListLoading, renderList, createActionButton } from "../list-utils.js";
import { GraphVisualizer } from "../graph-visualizer.js";

/**
 * 绑定图谱标签页
 * @param {HTMLElement} section - 标签页容器
 * @returns {Object} 事件管理器
 */
export function bind(section) {
  const eventManager = createEventManager();

  // 获取元素
  const nodesEl = section.querySelector("#graph-nodes");
  const edgesEl = section.querySelector("#graph-edges");
  const refreshBtn = section.querySelector("#graph-refresh");
  const nodeFormEl = section.querySelector("#graph-node-form");
  const nodeLabelEl = section.querySelector("#graph-node-label");
  const edgeFormEl = section.querySelector("#graph-edge-form");
  const edgeSourceEl = section.querySelector("#graph-edge-source");
  const edgeTargetEl = section.querySelector("#graph-edge-target");
  const edgeRelationEl = section.querySelector("#graph-edge-relation");
  const nodesListEl = section.querySelector("#graph-nodes-list");
  const edgesListEl = section.querySelector("#graph-edges-list");
  const nodesRefreshBtn = section.querySelector("#graph-nodes-refresh");
  const edgesRefreshBtn = section.querySelector("#graph-edges-refresh");
  const nodeUpdateFormEl = section.querySelector("#graph-node-update-form");
  const nodeUpdateIdEl = section.querySelector("#graph-node-update-id");
  const nodeUpdateLabelEl = section.querySelector("#graph-node-update-label");
  const edgeUpdateFormEl = section.querySelector("#graph-edge-update-form");
  const edgeUpdateIdEl = section.querySelector("#graph-edge-update-id");
  const edgeUpdateRelationEl = section.querySelector("#graph-edge-update-relation");

  // 可视化相关元素
  const visualizerContainer = section.querySelector("#graph-visualizer-container");
  const zoomInBtn = section.querySelector("#graph-zoom-in");
  const zoomOutBtn = section.querySelector("#graph-zoom-out");
  const resetBtn = section.querySelector("#graph-reset");

  if (!nodesEl || !edgesEl || !refreshBtn) return eventManager;

  // 初始化可视化器
  let visualizer = null;
  if (visualizerContainer) {
    visualizer = new GraphVisualizer(visualizerContainer, {
      height: 400,
      nodeRadius: 18,
      linkDistance: 120,
      chargeStrength: -400
    });
    visualizer.init();
  }

  // 缓存数据用于可视化
  let cachedNodes = [];
  let cachedEdges = [];

  // 更新可视化
  function updateVisualization() {
    if (visualizer) {
      visualizer.setData(cachedNodes, cachedEdges);
      visualizer.render();
    }
  }

  // 更新统计信息
  async function updateStatus() {
    nodesEl.textContent = UI_TEXT.LOADING;
    edgesEl.textContent = UI_TEXT.LOADING;
    try {
      const result = await api.fetchGraph();
      nodesEl.textContent = result.nodes ?? 0;
      edgesEl.textContent = result.edges ?? 0;
    } catch (error) {
      nodesEl.textContent = UI_TEXT.UNAVAILABLE;
      edgesEl.textContent = UI_TEXT.UNAVAILABLE;
    }
  }

  // 加载节点列表
  async function loadGraphNodes() {
    if (nodesListEl) setListLoading(nodesListEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchGraphNodes();
      cachedNodes = result.items || [];
      renderList(nodesListEl, result.items, (li, item) => {
        const info = document.createElement("div");
        info.textContent = formatTemplate(UI_TEXT.LIST_ITEM_GRAPH_NODE, {
          id: item.id,
          label: item.label
        });

        const actions = document.createElement("div");
        actions.className = "panel-inline";

        const delBtn = createActionButton(UI_TEXT.BUTTON_DELETE, "delete");
        delBtn.addEventListener("click", async () => {
          await api.deleteGraphNode(item.id);
          loadGraphNodes();
          loadGraphEdges();
          updateStatus();
        });

        actions.appendChild(delBtn);
        li.appendChild(info);
        li.appendChild(actions);
      }, UI_TEXT.NO_DATA);
      updateVisualization();
    } catch (error) {
      if (nodesListEl) setListLoading(nodesListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  // 加载边列表
  async function loadGraphEdges() {
    if (edgesListEl) setListLoading(edgesListEl, UI_TEXT.LOAD_LIST);
    try {
      const result = await api.fetchGraphEdges();
      cachedEdges = result.items || [];
      renderList(edgesListEl, result.items, (li, item) => {
        const info = document.createElement("div");
        info.textContent = formatTemplate(UI_TEXT.LIST_ITEM_GRAPH_EDGE, {
          id: item.id,
          source: item.source,
          target: item.target,
          relation: item.relation
        });

        const actions = document.createElement("div");
        actions.className = "panel-inline";

        const delBtn = createActionButton(UI_TEXT.BUTTON_DELETE, "delete");
        delBtn.addEventListener("click", async () => {
          await api.deleteGraphEdge(item.id);
          loadGraphEdges();
          updateStatus();
        });

        actions.appendChild(delBtn);
        li.appendChild(info);
        li.appendChild(actions);
      }, UI_TEXT.NO_DATA);
      updateVisualization();
    } catch (error) {
      if (edgesListEl) setListLoading(edgesListEl, UI_TEXT.LOAD_FAIL);
    }
  }

  // 绑定刷新按钮
  eventManager.add(refreshBtn, "click", debounce(updateStatus, 300));

  // 绑定节点刷新
  if (nodesRefreshBtn) {
    eventManager.add(nodesRefreshBtn, "click", debounce(loadGraphNodes, 300));
  }

  // 绑定边刷新
  if (edgesRefreshBtn) {
    eventManager.add(edgesRefreshBtn, "click", debounce(loadGraphEdges, 300));
  }

  // 绑定可视化工具栏
  if (zoomInBtn && visualizer) {
    eventManager.add(zoomInBtn, "click", () => visualizer.zoomBy(1.3));
  }
  if (zoomOutBtn && visualizer) {
    eventManager.add(zoomOutBtn, "click", () => visualizer.zoomBy(0.7));
  }
  if (resetBtn && visualizer) {
    eventManager.add(resetBtn, "click", () => visualizer.resetView());
  }

  // 绑定添加节点表单
  if (nodeFormEl && nodeLabelEl) {
    attachFormClear(nodeFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(nodeFormEl);
      const label = nodeLabelEl.value.trim();
      if (!validateRequiredText(nodeFormEl, label)) return;
      try {
        await api.createGraphNode(label);
        nodeLabelEl.value = "";
        updateStatus();
        loadGraphNodes();
      } catch (error) {
        showFormError(nodeFormEl, UI_TEXT.WRITE_FAIL);
      }
    };
    eventManager.add(nodeFormEl, "submit", submitHandler);
  }

  // 绑定添加边表单
  if (edgeFormEl && edgeSourceEl && edgeTargetEl && edgeRelationEl) {
    attachFormClear(edgeFormEl);
    const submitHandler = async (event) => {
      event.preventDefault();
      clearFormError(edgeFormEl);
      if (!isNumberInRange(edgeSourceEl.value, INPUT_ATTRS.GRAPH_EDGE_SOURCE)) {
        showFormError(edgeFormEl, UI_TEXT.VALIDATION_NUMBER);
        return;
      }
      if (!isNumberInRange(edgeTargetEl.value, INPUT_ATTRS.GRAPH_EDGE_TARGET)) {
        showFormError(edgeFormEl, UI_TEXT.VALIDATION_NUMBER);
        return;
      }
      const source = Number(edgeSourceEl.value);
      const target = Number(edgeTargetEl.value);
      const relation = edgeRelationEl.value.trim();
      if (!validateRequiredText(edgeFormEl, relation)) return;
      try {
        await api.createGraphEdge(source, target, relation);
        edgeSourceEl.value = "";
        edgeTargetEl.value = "";
        edgeRelationEl.value = "";
        updateStatus();
        loadGraphEdges();
      } catch (error) {
        showFormError(edgeFormEl, UI_TEXT.WRITE_FAIL);
      }
    };
    eventManager.add(edgeFormEl, "submit", submitHandler);
  }

  // 绑定更新节点表单
  if (nodeUpdateFormEl && nodeUpdateIdEl && nodeUpdateLabelEl) {
    attachFormClear(nodeUpdateFormEl);
    const updateHandler = async (event) => {
      event.preventDefault();
      clearFormError(nodeUpdateFormEl);
      if (!isNumberInRange(nodeUpdateIdEl.value, INPUT_ATTRS.GRAPH_NODE_ID)) {
        showFormError(nodeUpdateFormEl, UI_TEXT.VALIDATION_NUMBER);
        return;
      }
      const nodeId = Number(nodeUpdateIdEl.value);
      const label = nodeUpdateLabelEl.value.trim();
      if (!validateRequiredText(nodeUpdateFormEl, label)) return;
      try {
        await api.updateGraphNode(nodeId, label);
        nodeUpdateIdEl.value = "";
        nodeUpdateLabelEl.value = "";
        loadGraphNodes();
      } catch (error) {
        showFormError(nodeUpdateFormEl, UI_TEXT.UPDATE_FAIL);
      }
    };
    eventManager.add(nodeUpdateFormEl, "submit", updateHandler);
  }

  // 绑定更新边表单
  if (edgeUpdateFormEl && edgeUpdateIdEl && edgeUpdateRelationEl) {
    attachFormClear(edgeUpdateFormEl);
    const updateHandler = async (event) => {
      event.preventDefault();
      clearFormError(edgeUpdateFormEl);
      if (!isNumberInRange(edgeUpdateIdEl.value, INPUT_ATTRS.GRAPH_EDGE_ID)) {
        showFormError(edgeUpdateFormEl, UI_TEXT.VALIDATION_NUMBER);
        return;
      }
      const edgeId = Number(edgeUpdateIdEl.value);
      const relation = edgeUpdateRelationEl.value.trim();
      if (!validateRequiredText(edgeUpdateFormEl, relation)) return;
      try {
        await api.updateGraphEdge(edgeId, relation);
        edgeUpdateIdEl.value = "";
        edgeUpdateRelationEl.value = "";
        loadGraphEdges();
      } catch (error) {
        showFormError(edgeUpdateFormEl, UI_TEXT.UPDATE_FAIL);
      }
    };
    eventManager.add(edgeUpdateFormEl, "submit", updateHandler);
  }

  // 返回带有销毁逻辑的事件管理器
  const originalRemoveAll = eventManager.removeAll;
  eventManager.removeAll = () => {
    if (visualizer) {
      visualizer.destroy();
    }
    originalRemoveAll();
  };

  // 初始化加载
  updateStatus();
  loadGraphNodes();
  loadGraphEdges();

  return eventManager;
}

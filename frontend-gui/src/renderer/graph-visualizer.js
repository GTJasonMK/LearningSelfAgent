// 图谱可视化模块 - 基于 D3.js 力导向布局

/**
 * 图谱可视化器
 */
export class GraphVisualizer {
  /**
   * @param {HTMLElement} containerEl - 容器元素
   * @param {Object} options - 配置选项
   */
  constructor(containerEl, options = {}) {
    this.container = containerEl;
    this.options = {
      width: options.width || containerEl.clientWidth || 600,
      height: options.height || 400,
      nodeRadius: options.nodeRadius || 20,
      linkDistance: options.linkDistance || 100,
      chargeStrength: options.chargeStrength || -300,
      ...options
    };

    this.nodes = [];
    this.links = [];
    this.simulation = null;
    this.svg = null;
    this.nodeGroup = null;
    this.linkGroup = null;
    this.labelGroup = null;
    this.zoom = null;
  }

  /**
   * 初始化 SVG 画布和力模拟
   */
  init() {
    // 检查 D3 是否可用
    if (typeof d3 === "undefined") {
      this.container.innerHTML = "<div class=\"graph-error\">D3.js 未加载</div>";
      return false;
    }

    // 清空容器
    this.container.innerHTML = "";

    // 创建 SVG
    this.svg = d3.select(this.container)
      .append("svg")
      .attr("class", "graph-svg")
      .attr("width", "100%")
      .attr("height", this.options.height)
      .attr("viewBox", [0, 0, this.options.width, this.options.height]);

    // 添加箭头标记定义
    this.svg.append("defs")
      .append("marker")
      .attr("id", "arrow")
      .attr("viewBox", "0 -5 10 10")
      .attr("refX", 25)
      .attr("refY", 0)
      .attr("markerWidth", 6)
      .attr("markerHeight", 6)
      .attr("orient", "auto")
      .append("path")
      .attr("d", "M0,-5L10,0L0,5")
      .attr("class", "graph-arrow");

    // 创建主容器组（用于缩放平移）
    const mainGroup = this.svg.append("g").attr("class", "graph-main");

    // 创建图层
    this.linkGroup = mainGroup.append("g").attr("class", "graph-links");
    this.nodeGroup = mainGroup.append("g").attr("class", "graph-nodes");
    this.labelGroup = mainGroup.append("g").attr("class", "graph-labels");

    // 初始化缩放
    this.zoom = d3.zoom()
      .scaleExtent([0.1, 4])
      .on("zoom", (event) => {
        mainGroup.attr("transform", event.transform);
      });

    this.svg.call(this.zoom);

    // 初始化力模拟
    this.simulation = d3.forceSimulation()
      .force("link", d3.forceLink().id(d => d.id).distance(this.options.linkDistance))
      .force("charge", d3.forceManyBody().strength(this.options.chargeStrength))
      .force("center", d3.forceCenter(this.options.width / 2, this.options.height / 2))
      .force("collision", d3.forceCollide().radius(this.options.nodeRadius + 5));

    return true;
  }

  /**
   * 设置数据
   * @param {Array} nodes - 节点数组 [{id, label}]
   * @param {Array} edges - 边数组 [{id, source, target, relation}]
   */
  setData(nodes, edges) {
    // 转换节点格式
    this.nodes = (nodes || []).map(n => ({
      id: n.id,
      label: n.label || `Node ${n.id}`
    }));

    // 创建节点 ID 集合用于验证
    const nodeIds = new Set(this.nodes.map(n => n.id));

    // 转换边格式，过滤无效边
    this.links = (edges || []).filter(e => {
      return nodeIds.has(e.source) && nodeIds.has(e.target);
    }).map(e => ({
      id: e.id,
      source: e.source,
      target: e.target,
      relation: e.relation || ""
    }));
  }

  /**
   * 渲染图谱
   */
  render() {
    if (!this.simulation || !this.svg) {
      return;
    }

    // 渲染边
    const links = this.linkGroup.selectAll(".graph-edge")
      .data(this.links, d => d.id)
      .join(
        enter => enter.append("line")
          .attr("class", "graph-edge")
          .attr("marker-end", "url(#arrow)"),
        update => update,
        exit => exit.remove()
      );

    // 渲染节点
    const nodes = this.nodeGroup.selectAll(".graph-node")
      .data(this.nodes, d => d.id)
      .join(
        enter => {
          const g = enter.append("g")
            .attr("class", "graph-node")
            .call(this._drag());

          g.append("circle")
            .attr("r", this.options.nodeRadius);

          return g;
        },
        update => update,
        exit => exit.remove()
      );

    // 渲染标签
    const labels = this.labelGroup.selectAll(".graph-label")
      .data(this.nodes, d => d.id)
      .join(
        enter => enter.append("text")
          .attr("class", "graph-label")
          .attr("text-anchor", "middle")
          .attr("dy", this.options.nodeRadius + 15)
          .text(d => d.label),
        update => update.text(d => d.label),
        exit => exit.remove()
      );

    // 更新模拟
    this.simulation
      .nodes(this.nodes)
      .on("tick", () => {
        links
          .attr("x1", d => d.source.x)
          .attr("y1", d => d.source.y)
          .attr("x2", d => d.target.x)
          .attr("y2", d => d.target.y);

        nodes.attr("transform", d => `translate(${d.x}, ${d.y})`);

        labels
          .attr("x", d => d.x)
          .attr("y", d => d.y);
      });

    this.simulation.force("link").links(this.links);
    this.simulation.alpha(1).restart();
  }

  /**
   * 高亮节点
   * @param {number} nodeId - 节点 ID
   */
  highlightNode(nodeId) {
    this.nodeGroup.selectAll(".graph-node")
      .classed("highlighted", d => d.id === nodeId);
  }

  /**
   * 高亮路径
   * @param {number} sourceId - 起点 ID
   * @param {number} targetId - 终点 ID
   */
  highlightPath(sourceId, targetId) {
    this.linkGroup.selectAll(".graph-edge")
      .classed("highlighted", d => {
        const srcId = typeof d.source === "object" ? d.source.id : d.source;
        const tgtId = typeof d.target === "object" ? d.target.id : d.target;
        return srcId === sourceId && tgtId === targetId;
      });
  }

  /**
   * 清除高亮
   */
  clearHighlight() {
    this.nodeGroup.selectAll(".graph-node").classed("highlighted", false);
    this.linkGroup.selectAll(".graph-edge").classed("highlighted", false);
  }

  /**
   * 缩放控制
   * @param {number} scale - 缩放比例（相对于当前）
   */
  zoomBy(scale) {
    this.svg.transition().duration(300).call(
      this.zoom.scaleBy, scale
    );
  }

  /**
   * 居中视图
   */
  resetView() {
    this.svg.transition().duration(500).call(
      this.zoom.transform, d3.zoomIdentity
    );
  }

  /**
   * 销毁可视化
   */
  destroy() {
    if (this.simulation) {
      this.simulation.stop();
      this.simulation = null;
    }
    if (this.container) {
      this.container.innerHTML = "";
    }
  }

  /**
   * 创建拖拽行为
   * @private
   */
  _drag() {
    const simulation = this.simulation;

    function dragstarted(event) {
      if (!event.active) simulation.alphaTarget(0.3).restart();
      event.subject.fx = event.subject.x;
      event.subject.fy = event.subject.y;
    }

    function dragged(event) {
      event.subject.fx = event.x;
      event.subject.fy = event.y;
    }

    function dragended(event) {
      if (!event.active) simulation.alphaTarget(0);
      event.subject.fx = null;
      event.subject.fy = null;
    }

    return d3.drag()
      .on("start", dragstarted)
      .on("drag", dragged)
      .on("end", dragended);
  }
}

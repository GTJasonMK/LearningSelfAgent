import { API_BASE } from "./constants.js";

async function request(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, options);
  if (!response.ok) {
    throw new Error(`请求失败: ${response.status}`);
  }
  return response.json();
}

function jsonRequest(method, path, payload) {
  return request(path, {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
}

function buildQueryString(params = {}) {
  const qs = new URLSearchParams();
  const entries = params && typeof params === "object" ? Object.entries(params) : [];
  entries.forEach(([key, value]) => {
    if (value === undefined || value === null) return;
    const text = String(value);
    if (text.trim() === "") return;
    qs.set(String(key), text);
  });
  const out = qs.toString();
  return out ? `?${out}` : "";
}

export async function streamPetChat(payload, signal) {
  const response = await fetch(`${API_BASE}/llm/chat/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal
  });
  return response;
}

export async function streamAgentCommand(payload, signal) {
  const response = await fetch(`${API_BASE}/agent/command/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal
  });
  return response;
}

export async function streamAgentResume(payload, signal) {
  const response = await fetch(`${API_BASE}/agent/command/resume/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal
  });
  return response;
}

export async function streamAgentEvaluate(payload, signal) {
  const response = await fetch(`${API_BASE}/agent/evaluate/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
    signal
  });
  return response;
}

export async function routeAgentMode(payload) {
  return jsonRequest("POST", "/agent/route", payload);
}

export async function createChatMessage(payload) {
  return jsonRequest("POST", "/chat/messages", payload);
}

export async function fetchChatMessages(params = {}) {
  const suffix = buildQueryString({
    limit: params.limit,
    before_id: params.before_id,
    after_id: params.after_id
  });
  return request(`/chat/messages${suffix}`);
}

export async function searchChatMessages(params = {}) {
  const suffix = buildQueryString({ q: params.q, limit: params.limit });
  return request(`/chat/search${suffix}`);
}

export async function fetchCurrentAgentRun() {
  return request("/agent/runs/current");
}

export async function fetchAgentRunDetail(runId) {
  return request(`/agent/runs/${runId}`);
}

export async function fetchAgentReviews(params = {}) {
  const suffix = buildQueryString({
    offset: params.offset,
    limit: params.limit,
    task_id: params.task_id,
    run_id: params.run_id
  });
  return request(`/agent/reviews${suffix}`);
}

export async function fetchAgentReview(reviewId) {
  return request(`/agent/reviews/${reviewId}`);
}

export async function fetchRecentRecords(params = {}) {
  const suffix = buildQueryString({
    limit: params.limit,
    offset: params.offset,
    task_id: params.task_id,
    run_id: params.run_id
  });
  return request(`/records/recent${suffix}`);
}

export async function streamExecuteTask(taskId, payload, signal) {
  const response = await fetch(`${API_BASE}/tasks/${taskId}/execute/stream`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
    signal
  });
  return response;
}

export async function executeTask(taskId, payload) {
  return jsonRequest("POST", `/tasks/${taskId}/execute`, payload || {});
}

export async function fetchHealth() {
  return request("/health");
}

export async function fetchMemorySummary() {
  return request("/memory/summary");
}

export async function fetchSkills() {
  return request("/memory/skills");
}

export async function fetchGraph() {
  return request("/memory/graph");
}

export async function fetchDomainsStats() {
  return request("/domains/stats");
}

export async function fetchDomainsTree() {
  return request("/domains/tree");
}

export async function fetchSkillsCatalog() {
  return request("/skills/catalog");
}

export async function searchSkillsLibrary(params = {}) {
  const suffix = buildQueryString({
    q: params.q,
    category: params.category,
    tag: params.tag,
    skill_type: params.skill_type,
    status: params.status,
    limit: params.limit,
    offset: params.offset
  });
  return request(`/skills/search${suffix}`);
}

export async function fetchToolReuseSummary(params = {}) {
  const suffix = buildQueryString({
    task_id: params.task_id,
    run_id: params.run_id,
    tool_id: params.tool_id,
    reuse_status: params.reuse_status,
    limit: params.limit
  });
  return request(`/records/tools/reuse${suffix}`);
}

export async function fetchSkillReuseSummary(params = {}) {
  const suffix = buildQueryString({
    task_id: params.task_id,
    run_id: params.run_id,
    tool_id: params.tool_id,
    reuse_status: params.reuse_status,
    limit: params.limit
  });
  return request(`/records/skills/reuse${suffix}`);
}

export async function fetchEvalLatest() {
  return request("/eval/latest");
}

export async function fetchTasksSummary() {
  return request("/tasks/summary");
}

export async function createTask(title) {
  return jsonRequest("POST", "/tasks", { title });
}

export async function createTaskWithExpectation(title, expectationId) {
  return jsonRequest("POST", "/tasks", { title, expectation_id: expectationId });
}

export async function fetchTasks(params = {}) {
  const suffix = buildQueryString({ date: params.date, days: params.days });
  return request(`/tasks${suffix}`);
}

export async function updateTask(taskId, payload) {
  return jsonRequest("PATCH", `/tasks/${taskId}`, payload);
}

export async function createExpectation(goal, criteria) {
  return jsonRequest("POST", "/expectations", { goal, criteria });
}

export async function fetchExpectation(expectationId) {
  return request(`/expectations/${expectationId}`);
}

export async function createEval(payload) {
  return jsonRequest("POST", "/eval", payload);
}

export async function fetchEval(evalId) {
  return request(`/eval/${evalId}`);
}

export async function createMemoryItem(content) {
  return jsonRequest("POST", "/memory/items", { content });
}

export async function fetchMemoryItems() {
  return request("/memory/items");
}

export async function fetchMemoryItem(itemId) {
  return request(`/memory/items/${itemId}`);
}

export async function deleteMemoryItem(itemId) {
  return request(`/memory/items/${itemId}`, { method: "DELETE" });
}

export async function updateMemoryItem(itemId, content) {
  return jsonRequest("PATCH", `/memory/items/${itemId}`, { content });
}

export async function searchMemory(q) {
  return request(`/memory/search${buildQueryString({ q })}`);
}

export async function createSkill(name) {
  return jsonRequest("POST", "/memory/skills", { name });
}

export async function fetchSkillsList() {
  return request("/memory/skills");
}

export async function deleteSkill(skillId) {
  return request(`/memory/skills/${skillId}`, { method: "DELETE" });
}

export async function updateSkill(skillId, name) {
  return jsonRequest("PATCH", `/memory/skills/${skillId}`, { name });
}

export async function searchSkills(q) {
  return request(`/memory/skills/search${buildQueryString({ q })}`);
}

export async function fetchSkillDetail(skillId) {
  return request(`/memory/skills/${skillId}`);
}

export async function createSkillFull(payload) {
  return jsonRequest("POST", "/memory/skills", payload);
}

export async function updateSkillFull(skillId, payload) {
  return jsonRequest("PATCH", `/memory/skills/${skillId}`, payload);
}

export async function createSkillValidation(skillId, payload) {
  return jsonRequest("POST", `/memory/skills/${skillId}/validate`, payload);
}

export async function fetchSkillValidations(skillId) {
  return request(`/memory/skills/${skillId}/validations`);
}

export async function fetchTaskSteps(taskId) {
  return request(`/tasks/${taskId}/steps`);
}

export async function fetchTaskRuns(taskId) {
  return request(`/tasks/${taskId}/runs`);
}

export async function fetchTaskTimeline(taskId) {
  return request(`/records/tasks/${taskId}/timeline`);
}

export async function fetchTaskRecord(taskId) {
  return request(`/records/tasks/${taskId}`);
}

export async function fetchTaskOutputs(taskId, params = {}) {
  const suffix = buildQueryString({
    run_id: params.run_id,
    offset: params.offset,
    limit: params.limit
  });
  return request(`/tasks/${taskId}/outputs${suffix}`);
}

export async function createGraphNode(label) {
  return jsonRequest("POST", "/memory/graph/nodes", { label });
}

export async function createGraphEdge(source, target, relation) {
  return jsonRequest("POST", "/memory/graph/edges", { source, target, relation });
}

export async function fetchGraphNodes() {
  return request("/memory/graph/nodes");
}

export async function fetchGraphEdges() {
  return request("/memory/graph/edges");
}

export async function deleteGraphNode(nodeId) {
  return request(`/memory/graph/nodes/${nodeId}`, { method: "DELETE" });
}

export async function deleteGraphEdge(edgeId) {
  return request(`/memory/graph/edges/${edgeId}`, { method: "DELETE" });
}

export async function updateGraphNode(nodeId, label) {
  return jsonRequest("PATCH", `/memory/graph/nodes/${nodeId}`, { label });
}

export async function updateGraphEdge(edgeId, relation) {
  return jsonRequest("PATCH", `/memory/graph/edges/${edgeId}`, { relation });
}

export async function searchUnified(q, limit) {
  return request(`/search${buildQueryString({ q, limit })}`);
}

export async function createTool(payload) {
  return jsonRequest("POST", "/tools", payload);
}

export async function fetchTools() {
  return request("/tools");
}

export async function createLlmRecord(payload) {
  return jsonRequest("POST", "/records/llm", payload);
}

export async function fetchLlmRecords() {
  return request("/records/llm");
}

export async function createToolRecord(payload) {
  return jsonRequest("POST", "/records/tools", payload);
}

export async function fetchToolRecords() {
  return request("/records/tools");
}

export async function fetchLlmConfig() {
  return request("/config/llm");
}

export async function updateLlmConfig(payload) {
  return jsonRequest("PATCH", "/config/llm", payload);
}

export async function fetchConfig() {
  return request("/config");
}

export async function updateConfig(payload) {
  return jsonRequest("PATCH", "/config", payload);
}

export async function fetchPermissions() {
  return request("/permissions");
}

export async function updatePermissions(payload) {
  return jsonRequest("PATCH", "/permissions", payload);
}

export async function fetchActions() {
  return request("/actions");
}

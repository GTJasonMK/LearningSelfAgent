import * as api from "./api.js";

export async function buildPetChatContextMessages(systemPrompt, maxMessages, fallbackMessages) {
  const system = { role: "system", content: String(systemPrompt || "") };
  const fallback = Array.isArray(fallbackMessages) ? fallbackMessages.slice() : [system];
  try {
    const resp = await api.fetchChatMessages({ limit: 80 });
    const items = Array.isArray(resp?.items) ? resp.items : [];
    const tail = items
      .filter((m) => {
        const role = String(m?.role || "").trim().toLowerCase();
        if (role !== "user" && role !== "assistant") return false;
        return !!String(m?.content || "").trim();
      })
      .slice(-(Number(maxMessages) - 1))
      .map((m) => ({ role: String(m.role), content: String(m.content) }));
    return [system, ...tail];
  } catch (e) {
    return fallback;
  }
}

export async function writePetChatMessage(role, content, extra = {}) {
  const text = String(content || "").trim();
  if (!text) return;
  try {
    await api.createChatMessage({
      role,
      content: text,
      task_id: extra.task_id || null,
      run_id: extra.run_id || null,
      metadata: { source: "pet", ...(extra.metadata || {}) }
    });
  } catch (e) {
    // 聊天记录落库失败不应中断桌宠交互
  }
}


// 轻量 Markdown 渲染（无第三方依赖，适配桌宠/世界页的“短文本可读性”需求）
// 说明：
// - 仅支持常用子集：标题/列表/引用/代码块/行内格式（粗体/斜体/行内代码/链接）
// - 默认转义 HTML，避免把模型输出当作可执行 HTML 注入 DOM

function escapeHtml(text) {
  return String(text || "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

function sanitizeUrl(url) {
  const raw = String(url || "").trim();
  if (!raw) return "#";
  const lower = raw.toLowerCase();
  // 仅允许常见可外链协议；其余降级为不可点击
  if (lower.startsWith("http://") || lower.startsWith("https://") || lower.startsWith("mailto:")) {
    return raw;
  }
  return "#";
}

function renderInline(md) {
  const codeSpans = [];
  let text = String(md || "");

  // 先抽出行内 code，避免后续正则把其中的 **/* 等当作格式
  text = text.replace(/`([^`]+)`/g, (_m, code) => {
    const idx = codeSpans.length;
    codeSpans.push(`<code>${escapeHtml(code)}</code>`);
    return `\u0000C${idx}\u0000`;
  });

  // 普通文本先转义
  text = escapeHtml(text);

  // 链接：[text](url)
  text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_m, label, url) => {
    const safeUrl = sanitizeUrl(url);
    const safeLabel = String(label || "");
    return `<a href="${escapeHtml(safeUrl)}" target="_blank" rel="noreferrer noopener">${safeLabel}</a>`;
  });

  // 粗体：**text**
  text = text.replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>");
  // 斜体：*text*（避免与 ** 冲突）
  text = text.replace(/(^|[^*])\*([^*]+)\*(?!\*)/g, "$1<em>$2</em>");

  // 恢复行内 code
  text = text.replace(/\u0000C(\d+)\u0000/g, (_m, n) => {
    const idx = Number(n);
    return codeSpans[idx] || "";
  });

  return text;
}

function renderInlineMultiline(lines) {
  const list = Array.isArray(lines) ? lines : String(lines || "").split("\n");
  return list.map((line) => renderInline(line)).join("<br>");
}

export function renderMarkdownToHtml(mdText) {
  const raw = String(mdText || "");
  if (!raw.trim()) return "";

  const text = raw.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  const lines = text.split("\n");

  const out = [];
  let i = 0;

  while (i < lines.length) {
    const line = lines[i] ?? "";

    // 代码块：```lang ... ```
    const fenceMatch = line.match(/^```(\S+)?\s*$/);
    if (fenceMatch) {
      const lang = String(fenceMatch[1] || "").trim();
      i += 1;
      const codeLines = [];
      while (i < lines.length && !String(lines[i] || "").startsWith("```")) {
        codeLines.push(lines[i] ?? "");
        i += 1;
      }
      // 跳过闭合 fence
      if (i < lines.length && String(lines[i] || "").startsWith("```")) i += 1;
      const code = escapeHtml(codeLines.join("\n"));
      const langClass = lang ? ` class="language-${escapeHtml(lang)}"` : "";
      out.push(`<pre><code${langClass}>${code}</code></pre>`);
      continue;
    }

    // 空行：段落分隔
    if (!String(line).trim()) {
      i += 1;
      continue;
    }

    // 分隔线
    if (/^(-{3,}|\*{3,}|_{3,})\s*$/.test(line)) {
      out.push("<hr>");
      i += 1;
      continue;
    }

    // 标题
    const headingMatch = line.match(/^(#{1,6})\s+(.*)$/);
    if (headingMatch) {
      const level = headingMatch[1].length;
      const content = renderInline(String(headingMatch[2] || ""));
      out.push(`<h${level}>${content}</h${level}>`);
      i += 1;
      continue;
    }

    // 引用块（连续 > 行）
    if (/^\s*>\s+/.test(line)) {
      const quoteLines = [];
      while (i < lines.length && /^\s*>\s+/.test(String(lines[i] || ""))) {
        quoteLines.push(String(lines[i]).replace(/^\s*>\s+/, ""));
        i += 1;
      }
      out.push(`<blockquote>${renderInlineMultiline(quoteLines)}</blockquote>`);
      continue;
    }

    // 无序列表（连续 -/*/+ 行）
    if (/^\s*[-*+]\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*[-*+]\s+/.test(String(lines[i] || ""))) {
        const item = String(lines[i]).replace(/^\s*[-*+]\s+/, "");
        items.push(`<li>${renderInline(item)}</li>`);
        i += 1;
      }
      out.push(`<ul>${items.join("")}</ul>`);
      continue;
    }

    // 有序列表（连续 1. 行）
    if (/^\s*\d+\.\s+/.test(line)) {
      const items = [];
      while (i < lines.length && /^\s*\d+\.\s+/.test(String(lines[i] || ""))) {
        const item = String(lines[i]).replace(/^\s*\d+\.\s+/, "");
        items.push(`<li>${renderInline(item)}</li>`);
        i += 1;
      }
      out.push(`<ol>${items.join("")}</ol>`);
      continue;
    }

    // 普通段落：合并到下一个“空行/块级元素”之前
    const paraLines = [];
    while (i < lines.length) {
      const l = lines[i] ?? "";
      if (!String(l).trim()) break;
      if (/^```/.test(String(l))) break;
      if (/^(#{1,6})\s+/.test(String(l))) break;
      if (/^\s*>\s+/.test(String(l))) break;
      if (/^\s*[-*+]\s+/.test(String(l))) break;
      if (/^\s*\d+\.\s+/.test(String(l))) break;
      if (/^(-{3,}|\*{3,}|_{3,})\s*$/.test(String(l))) break;
      paraLines.push(String(l));
      i += 1;
    }
    out.push(`<p>${renderInlineMultiline(paraLines)}</p>`);
  }

  return out.join("\n");
}

export function setMarkdownContent(el, mdText) {
  if (!el) return;
  el.classList.add("md");
  el.innerHTML = renderMarkdownToHtml(mdText);
}


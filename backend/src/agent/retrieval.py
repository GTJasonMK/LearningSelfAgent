import hashlib
import json
import os
import re
import threading
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from backend.src.actions.registry import normalize_action_type
from backend.src.agent.json_utils import _extract_json_object
from backend.src.constants import (
    AGENT_DOMAIN_PICK_CANDIDATE_LIMIT,
    AGENT_DOMAIN_PICK_MAX_DOMAINS,
    AGENT_DOMAIN_PICK_PROMPT_TEMPLATE,
    AGENT_GRAPH_PICK_CANDIDATE_LIMIT,
    AGENT_GRAPH_PICK_MAX_NODES,
    AGENT_GRAPH_PICK_PROMPT_TEMPLATE,
    AGENT_GRAPH_PROMPT_SNIPPET_MAX_CHARS,
    AGENT_MEMORY_PICK_CANDIDATE_LIMIT,
    AGENT_MEMORY_PICK_MAX_ITEMS,
    AGENT_MEMORY_PICK_PROMPT_TEMPLATE,
    AGENT_MEMORY_PROMPT_SNIPPET_MAX_CHARS,
    AGENT_RETRIEVAL_LLM_CACHE_MAX_ENTRIES,
    AGENT_RETRIEVAL_LLM_CACHE_TTL_SECONDS,
    AGENT_SKILL_PICK_CANDIDATE_LIMIT,
    AGENT_SKILL_PICK_MAX_SKILLS,
    AGENT_SKILL_PICK_PROMPT_TEMPLATE,
    AGENT_SOLUTION_PICK_CANDIDATE_LIMIT,
    AGENT_SOLUTION_PICK_MAX_SOLUTIONS,
    AGENT_SOLUTION_PICK_PROMPT_TEMPLATE,
    AGENT_SOLUTION_DRAFT_PROMPT_TEMPLATE,
    DB_ENV_VAR,
    PROMPT_ENV_VAR,
    KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE,
    SKILL_COMPOSE_PROMPT_TEMPLATE,
    SKILL_DRAFT_PROMPT_TEMPLATE,
)
from backend.src.repositories import agent_retrieval_repo
from backend.src.services.llm.llm_client import call_openai

_RETRIEVAL_LLM_CACHE_LOCK = threading.Lock()
# key -> (expires_at_monotonic, text, tokens)
_RETRIEVAL_LLM_CACHE: Dict[str, Tuple[float, Optional[str], Optional[dict]]] = {}


def _cached_call_openai(
    *,
    cache_namespace: str,
    prompt: str,
    model: str,
    params: dict,
) -> Tuple[Optional[str], Optional[dict], Optional[str]]:
    """
    检索阶段 LLM 调用缓存（P2：成本与策略）。

    说明：
    - 仅缓存成功响应（err=None 且 text 非空）
    - key 维度包含 DB 路径与 prompt_root，避免测试/多实例串扰
    - TTL/max_entries 由常量控制；<=0 时自动禁用
    """
    try:
        ttl = int(AGENT_RETRIEVAL_LLM_CACHE_TTL_SECONDS or 0)
    except Exception:
        ttl = 0
    try:
        max_entries = int(AGENT_RETRIEVAL_LLM_CACHE_MAX_ENTRIES or 0)
    except Exception:
        max_entries = 0
    if ttl <= 0 or max_entries <= 0:
        return call_openai(prompt, model, params)

    db_key = str(os.getenv(DB_ENV_VAR, "") or "").strip()
    prompt_root = str(os.getenv(PROMPT_ENV_VAR, "") or "").strip()
    try:
        params_key = json.dumps(params or {}, ensure_ascii=False, sort_keys=True)
    except Exception:
        params_key = str(params or "")
    raw = f"{cache_namespace}|db:{db_key}|prompt_root:{prompt_root}|model:{model}|params:{params_key}|prompt:{prompt}"
    key = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    now_value = time.monotonic()
    with _RETRIEVAL_LLM_CACHE_LOCK:
        entry = _RETRIEVAL_LLM_CACHE.get(key)
        if entry is not None:
            expires_at, cached_text, cached_tokens = entry
            if float(expires_at) > now_value and cached_text:
                return cached_text, cached_tokens, None
            _RETRIEVAL_LLM_CACHE.pop(key, None)

    text, tokens, err = call_openai(prompt, model, params)
    if err or not text:
        return text, tokens, err

    with _RETRIEVAL_LLM_CACHE_LOCK:
        # 过期清理（不追求严格 LRU：保持实现简单）
        if len(_RETRIEVAL_LLM_CACHE) >= max_entries:
            expired_keys = [k for k, (exp, _t, _tok) in _RETRIEVAL_LLM_CACHE.items() if float(exp) <= now_value]
            for k in expired_keys[: max_entries]:
                _RETRIEVAL_LLM_CACHE.pop(k, None)
        if len(_RETRIEVAL_LLM_CACHE) >= max_entries:
            _RETRIEVAL_LLM_CACHE.clear()
        _RETRIEVAL_LLM_CACHE[key] = (now_value + float(ttl), text, tokens)

    return text, tokens, None


@dataclass
class KnowledgeSufficiencyResult:
    """知识充分性判断结果。"""
    sufficient: bool
    reason: str
    missing_knowledge: str  # skill / methodology / tool / domain_knowledge / none
    suggestion: str  # proceed / compose_skills / create_draft_skill / ask_user
    skill_count: int
    graph_count: int
    memory_count: int


def _list_tool_hints(limit: int = 8) -> str:
    """
    提供给 LLM 的工具清单提示：鼓励优先使用 tool_call 而不是"纯 llm_call 瞎编"。
    """
    try:
        items = agent_retrieval_repo.list_tool_hints(limit=limit)
        if not items:
            return "(无)"
        lines = []
        for item in items:
            tid = item.get("id")
            name = item.get("name")
            desc = item.get("description") or ""
            lines.append(f"- {tid}. {name}: {desc}")
        return "\n".join(lines)
    except Exception:
        return "(读取失败)"


def _list_domain_candidates(limit: int = 20) -> List[dict]:
    """读取领域候选集。"""
    return agent_retrieval_repo.list_domain_candidates(limit=limit)


def _format_domain_candidates_for_prompt(items: List[dict]) -> str:
    """格式化领域列表用于 LLM prompt。"""
    if not items:
        return "(无)"
    lines = []
    for item in items:
        domain_id = item.get("domain_id", "")
        name = item.get("name", "")
        desc = item.get("description", "")
        keywords = item.get("keywords", [])
        keywords_text = ",".join(keywords) if keywords else ""
        parts = []
        if desc:
            parts.append(desc)
        if keywords_text:
            parts.append(f"keywords={keywords_text}")
        tail = " ".join(parts).strip()
        line = f"{domain_id}. {name}"
        if tail:
            line += f" - {tail}"
        lines.append(line)
    return "\n".join(lines)


def _filter_relevant_domains(
    message: str,
    graph_hint: str,
    model: str,
    parameters: Optional[dict],
) -> List[str]:
    """
    根据用户消息筛选相关领域。
    返回最多 AGENT_DOMAIN_PICK_MAX_DOMAINS 个领域 ID。
    """
    candidates = _list_domain_candidates(AGENT_DOMAIN_PICK_CANDIDATE_LIMIT)
    if not candidates:
        return ["misc"]

    # 候选很少时跳过 LLM 筛选
    if len(candidates) <= AGENT_DOMAIN_PICK_MAX_DOMAINS:
        return [item["domain_id"] for item in candidates]

    candidates_text = _format_domain_candidates_for_prompt(candidates)
    pick_prompt = AGENT_DOMAIN_PICK_PROMPT_TEMPLATE.format(
        message=message,
        graph=graph_hint or "(无)",
        domains=candidates_text,
        max_domains=AGENT_DOMAIN_PICK_MAX_DOMAINS,
    )
    pick_text, _, pick_err = _cached_call_openai(
        cache_namespace="domain_pick",
        prompt=pick_prompt,
        model=model,
        params={"temperature": 0, "max_tokens": 100},
    )
    if pick_err or not pick_text:
        return ["misc"]

    obj = _extract_json_object(pick_text)
    if not obj:
        return ["misc"]

    ids_raw = obj.get("domain_ids")
    if not isinstance(ids_raw, list):
        return ["misc"]

    # 验证返回的领域 ID 是否有效
    valid_ids = {item["domain_id"] for item in candidates}
    selected: List[str] = []
    for item in ids_raw:
        domain_id = str(item).strip()
        if domain_id in valid_ids and domain_id not in selected:
            selected.append(domain_id)
        if len(selected) >= AGENT_DOMAIN_PICK_MAX_DOMAINS:
            break

    return selected if selected else ["misc"]


def _list_skill_candidates(
    limit: int,
    query_text: Optional[str] = None,
    domain_ids: Optional[List[str]] = None,
    skill_type: Optional[str] = "methodology",
    debug: Optional[dict] = None,
) -> List[dict]:
    """读取技能候选集，支持按领域筛选。"""
    if domain_ids:
        return agent_retrieval_repo.list_skill_candidates_by_domains(
            domain_ids=domain_ids,
            limit=limit,
            query_text=query_text,
            debug=debug,
            skill_type=skill_type,
        )
    return agent_retrieval_repo.list_skill_candidates(
        limit=limit,
        query_text=query_text,
        debug=debug,
        skill_type=skill_type,
    )


def _format_skill_candidates_for_prompt(items: List[dict]) -> str:
    if not items:
        return "(无)"
    lines = []
    for item in items:
        sid = item.get("id")
        name = str(item.get("name") or "").strip()
        desc = str(item.get("description") or "").strip()
        scope = str(item.get("scope") or "").strip()
        category = str(item.get("category") or "").strip()
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        tags_text = ",".join(str(t).strip() for t in tags if str(t).strip())
        head = f"{sid}. {name}" if sid is not None else name
        parts = []
        if category:
            parts.append(f"category={category}")
        if tags_text:
            parts.append(f"tags={tags_text}")
        tail = " ".join(parts) or desc or scope
        if tail:
            lines.append(f"{head} - {tail}")
        else:
            lines.append(head)
    return "\n".join(lines)


def _load_skills_by_ids(skill_ids: List[int]) -> List[dict]:
    return agent_retrieval_repo.load_skills_by_ids(skill_ids)


def _format_skills_for_prompt(skills: List[dict], max_steps_per_skill: int = 6) -> str:
    if not skills:
        return "(无)"
    lines: List[str] = []
    for skill in skills:
        sid = skill.get("id")
        name = str(skill.get("name") or "").strip()
        version = str(skill.get("version") or "").strip()
        desc = str(skill.get("description") or "").strip()
        scope = str(skill.get("scope") or "").strip()
        category = str(skill.get("category") or "").strip()
        tags = skill.get("tags") if isinstance(skill.get("tags"), list) else []
        tags_text = ",".join(str(t).strip() for t in tags if str(t).strip())
        header = f"#{sid} {name}" if sid is not None else name
        if version:
            header += f" v{version}"
        lines.append(header)
        if category or tags_text:
            extra = []
            if category:
                extra.append(f"category={category}")
            if tags_text:
                extra.append(f"tags={tags_text}")
            lines.append("元信息: " + " ".join(extra))
        if desc:
            lines.append(f"描述: {desc}")
        if scope:
            lines.append(f"范围: {scope}")
        steps = skill.get("steps") or []
        if isinstance(steps, list) and steps:
            steps = [str(s).strip() for s in steps if str(s).strip()]
            if steps:
                lines.append("步骤: " + " ; ".join(steps[:max_steps_per_skill]))
        lines.append("")
    return "\n".join(lines).strip() or "(无)"


def _list_memory_candidates(
    limit: int,
    query_text: Optional[str] = None,
    debug: Optional[dict] = None,
) -> List[dict]:
    """
    读取记忆候选集（用于 LLM 二次筛选）。
    - 相关性优先：FTS5
    - 兜底：最近 memories
    """
    return agent_retrieval_repo.list_memory_candidates(limit=limit, query_text=query_text, debug=debug)


def _format_memory_candidates_for_prompt(items: List[dict]) -> str:
    """
    格式化记忆候选用于 LLM prompt。
    输出格式：id. content - tags/type
    """
    if not items:
        return "(无)"
    lines: List[str] = []
    for item in items:
        mid = item.get("id")
        content = str(item.get("content") or "").replace("\r", " ").replace("\n", " ").strip()
        if len(content) > AGENT_MEMORY_PROMPT_SNIPPET_MAX_CHARS:
            content = content[:AGENT_MEMORY_PROMPT_SNIPPET_MAX_CHARS] + "..."
        memory_type = str(item.get("memory_type") or "").strip()
        tags = item.get("tags") if isinstance(item.get("tags"), list) else []
        tags_text = ",".join(str(t).strip() for t in tags if str(t).strip())
        meta_parts: List[str] = []
        if tags_text:
            meta_parts.append(f"tags={tags_text}")
        if memory_type:
            meta_parts.append(f"type={memory_type}")
        meta = "/".join(meta_parts).strip()
        head = f"{mid}. " if mid is not None else ""
        tail = f" - {meta}" if meta else ""
        lines.append(f"{head}{content}{tail}".strip())
    return "\n".join(lines) or "(无)"


def _format_memories_for_prompt(memories: List[dict]) -> str:
    """
    给 plan prompt 用的记忆块（简短、可直接引用）。
    """
    return _format_memory_candidates_for_prompt(memories)


def _select_relevant_memories(
    message: str,
    model: str,
    parameters: Optional[dict],
    debug: Optional[dict] = None,
) -> List[dict]:
    """
    先从 DB 召回记忆候选，再用 LLM 精选少量最相关记忆。

    注意：
    - docs/agent 中记忆默认不注入上下文；此函数作为能力保留，供未来“偏好/配置”类任务按需启用。
    """
    candidates = _list_memory_candidates(
        AGENT_MEMORY_PICK_CANDIDATE_LIMIT, query_text=message, debug=debug
    )
    if not candidates:
        return []

    # 候选很少时跳过 LLM 精选
    if len(candidates) <= AGENT_MEMORY_PICK_MAX_ITEMS:
        return candidates[:AGENT_MEMORY_PICK_MAX_ITEMS]

    candidates_text = _format_memory_candidates_for_prompt(candidates)
    pick_prompt = AGENT_MEMORY_PICK_PROMPT_TEMPLATE.format(
        message=message,
        memories=candidates_text,
        max_items=AGENT_MEMORY_PICK_MAX_ITEMS,
    )
    pick_text, _, pick_err = _cached_call_openai(
        cache_namespace="memory_pick",
        prompt=pick_prompt,
        model=model,
        params={"temperature": 0, "max_tokens": 160},
    )
    if pick_err or not pick_text:
        return []
    obj = _extract_json_object(pick_text)
    if not obj:
        return []
    ids_raw = obj.get("memory_ids")
    if not isinstance(ids_raw, list):
        return []

    by_id: dict[int, dict] = {}
    for it in candidates:
        if not isinstance(it, dict) or it.get("id") is None:
            continue
        try:
            mid = int(it.get("id"))
        except Exception:
            continue
        if mid > 0 and mid not in by_id:
            by_id[mid] = it

    selected: List[dict] = []
    for raw in ids_raw:
        try:
            mid = int(raw)
        except Exception:
            continue
        if mid <= 0:
            continue
        item = by_id.get(mid)
        if not item:
            continue
        selected.append(item)
        if len(selected) >= AGENT_MEMORY_PICK_MAX_ITEMS:
            break

    return selected


def _select_relevant_skills(
    message: str,
    model: str,
    parameters: Optional[dict],
    domain_ids: Optional[List[str]] = None,
    debug: Optional[dict] = None,
) -> List[dict]:
    """
    先用 DB 拉取候选技能，再用 LLM 选出最相关的少量技能，最后加载完整技能卡片。
    支持按领域筛选：如果提供了 domain_ids，则只在指定领域内检索技能。
    """
    candidates = _list_skill_candidates(
        AGENT_SKILL_PICK_CANDIDATE_LIMIT, query_text=message, domain_ids=domain_ids, debug=debug
    )
    if not candidates:
        return []
    candidates_text = _format_skill_candidates_for_prompt(candidates)
    pick_prompt = AGENT_SKILL_PICK_PROMPT_TEMPLATE.format(
        message=message,
        skills=candidates_text,
        max_skills=AGENT_SKILL_PICK_MAX_SKILLS,
    )
    # 技能筛选应尽量稳定：固定低温度，避免随机挑选
    pick_text, _, pick_err = _cached_call_openai(
        cache_namespace="skill_pick",
        prompt=pick_prompt,
        model=model,
        params={"temperature": 0},
    )
    if pick_err or not pick_text:
        return []
    obj = _extract_json_object(pick_text)
    if not obj:
        return []
    ids_raw = obj.get("skill_ids")
    if not isinstance(ids_raw, list):
        return []
    selected: List[int] = []
    for item in ids_raw:
        try:
            sid = int(item)
        except Exception:
            continue
        if sid <= 0:
            continue
        if sid not in selected:
            selected.append(sid)
        if len(selected) >= AGENT_SKILL_PICK_MAX_SKILLS:
            break
    return _load_skills_by_ids(selected)


def _build_solutions_query_text(message: str, skills: List[dict]) -> str:
    """
    生成“方案检索”的 query_text：
    - 基础：用户 message
    - 追加：已命中的技能名（提高召回）
    """
    parts: List[str] = [str(message or "").strip()]
    for s in skills or []:
        name = str(s.get("name") or "").strip() if isinstance(s, dict) else ""
        if name:
            parts.append(name)
    return " ".join(p for p in parts if p).strip()


def _list_solution_candidates(
    limit: int,
    query_text: Optional[str] = None,
    domain_ids: Optional[List[str]] = None,
    debug: Optional[dict] = None,
) -> List[dict]:
    """
    读取方案候选集（Solution=skills_items.skill_type='solution'），支持按领域筛选。
    """
    if domain_ids:
        return agent_retrieval_repo.list_skill_candidates_by_domains(
            domain_ids=domain_ids,
            limit=limit,
            query_text=query_text,
            debug=debug,
            skill_type="solution",
        )
    return agent_retrieval_repo.list_skill_candidates(
        limit=limit, query_text=query_text, debug=debug, skill_type="solution"
    )


def _load_solutions_by_ids(solution_ids: List[int]) -> List[dict]:
    # 复用 skills_items 表：Solution 与 Skill 共享同一张表
    return agent_retrieval_repo.load_skills_by_ids(solution_ids)


def _select_relevant_solutions(
    message: str,
    skills: List[dict],
    model: str,
    parameters: Optional[dict],
    *,
    domain_ids: Optional[List[str]] = None,
    max_solutions: Optional[int] = None,
    debug: Optional[dict] = None,
) -> List[dict]:
    """
    方案匹配：
    - 优先按 skills 的 id 生成 tag（skill:{id}），在 solutions(tags) 中做匹配召回（更准）
    - 先从 DB 召回候选（FTS + 最近），限定 skill_type='solution'
    - 再用 LLM 精选少量方案

    说明：
    - 为了与 docs/agent 对齐，方案会在 tags 中记录 skills_used（skill:{id}），便于按技能匹配；
    - 若缺少该类 tag（历史数据/老版本），则回退为 “message + 技能名” 的 query_text 提高召回。
    """
    max_pick = int(max_solutions) if isinstance(max_solutions, int) and int(max_solutions) > 0 else int(AGENT_SOLUTION_PICK_MAX_SOLUTIONS)

    skill_ids: List[int] = []
    for s in skills or []:
        if not isinstance(s, dict) or s.get("id") is None:
            continue
        try:
            sid = int(s.get("id"))
        except Exception:
            continue
        if sid <= 0:
            continue
        if sid not in skill_ids:
            skill_ids.append(sid)
        if len(skill_ids) >= 20:
            break
    skill_tags = [f"skill:{sid}" for sid in skill_ids]

    merged_candidates: List[dict] = []
    by_id: dict[int, dict] = {}

    def _merge_candidates(items: List[dict]) -> None:
        for it in items or []:
            if not isinstance(it, dict) or it.get("id") is None:
                continue
            try:
                cid = int(it.get("id"))
            except Exception:
                continue
            if cid <= 0 or cid in by_id:
                continue
            by_id[cid] = it
            merged_candidates.append(it)

    # 1) 优先用 skill tag 匹配（更准）
    if skill_tags:
        try:
            tag_candidates = agent_retrieval_repo.list_solution_candidates_by_skill_tags(
                skill_tags=skill_tags,
                limit=int(AGENT_SOLUTION_PICK_CANDIDATE_LIMIT),
                domain_ids=domain_ids,
                debug=debug,
            )
        except Exception:
            tag_candidates = []
        _merge_candidates(tag_candidates)

    # 2) 回退：message + 技能名，提高召回（兼容旧数据）
    query_text = _build_solutions_query_text(message, skills)
    candidates = _list_solution_candidates(
        AGENT_SOLUTION_PICK_CANDIDATE_LIMIT,
        query_text=query_text,
        domain_ids=domain_ids,
        debug=debug,
    )
    _merge_candidates(candidates)

    if not merged_candidates:
        return []

    # 候选很少时跳过 LLM 精选
    if len(merged_candidates) <= max_pick:
        ids = []
        for it in merged_candidates[:max_pick]:
            try:
                ids.append(int(it.get("id")))
            except Exception:
                continue
        return _load_solutions_by_ids(ids)

    candidates_text = _format_skill_candidates_for_prompt(merged_candidates)
    pick_prompt = AGENT_SOLUTION_PICK_PROMPT_TEMPLATE.format(
        message=message,
        solutions=candidates_text,
        max_solutions=max_pick,
    )
    pick_text, _, pick_err = _cached_call_openai(
        cache_namespace="solution_pick",
        prompt=pick_prompt,
        model=model,
        params={"temperature": 0, "max_tokens": 160},
    )
    if pick_err or not pick_text:
        # 降级：LLM 精选失败时，按 skill tag 命中数 + 新近排序返回，避免“有候选但返回空”
        def _match_count(it: dict) -> int:
            tags = it.get("tags") if isinstance(it.get("tags"), list) else []
            if not tags or not skill_tags:
                return 0
            s = set(str(t).strip() for t in tags if str(t).strip())
            return sum(1 for tag in skill_tags if tag in s)

        ranked = sorted(
            merged_candidates,
            key=lambda it: (
                _match_count(it),
                int(it.get("id") or 0),
            ),
            reverse=True,
        )
        fallback_ids = []
        for it in ranked[:max_pick]:
            try:
                fallback_ids.append(int(it.get("id")))
            except Exception:
                continue
        if isinstance(debug, dict):
            debug["llm_pick_fallback"] = True
            debug["llm_pick_error"] = str(pick_err or "empty_response")
        return _load_solutions_by_ids(fallback_ids)

    obj = _extract_json_object(pick_text)
    if not obj:
        # 降级：输出无法解析时同样走 deterministic fallback
        if isinstance(debug, dict):
            debug["llm_pick_fallback"] = True
            debug["llm_pick_error"] = "invalid_json"
        fallback_ids = []
        for it in merged_candidates[:max_pick]:
            try:
                fallback_ids.append(int(it.get("id")))
            except Exception:
                continue
        return _load_solutions_by_ids(fallback_ids)

    ids_raw = obj.get("solution_ids")
    if not isinstance(ids_raw, list):
        if isinstance(debug, dict):
            debug["llm_pick_fallback"] = True
            debug["llm_pick_error"] = "missing_solution_ids"
        fallback_ids = []
        for it in merged_candidates[:max_pick]:
            try:
                fallback_ids.append(int(it.get("id")))
            except Exception:
                continue
        return _load_solutions_by_ids(fallback_ids)

    selected: List[int] = []
    for item in ids_raw:
        try:
            sid = int(item)
        except Exception:
            continue
        if sid <= 0:
            continue
        if sid not in selected:
            selected.append(sid)
        if len(selected) >= max_pick:
            break

    return _load_solutions_by_ids(selected)


def _format_solutions_for_prompt(solutions: List[dict], max_steps_per_solution: int = 10) -> str:
    """
    给 plan prompt 用的方案块（简短、可直接引用）。
    """
    if not solutions:
        return "(无)"

    lines: List[str] = []
    for sol in solutions:
        sid = sol.get("id")
        name = str(sol.get("name") or "").strip()
        version = str(sol.get("version") or "").strip()
        desc = str(sol.get("description") or "").strip()
        header = f"#{sid} {name}" if sid is not None else name
        if version:
            header += f" v{version}"
        lines.append(header)
        if desc:
            lines.append(f"描述: {desc}")

        steps_raw = sol.get("steps") or []
        steps: List[str] = []
        if isinstance(steps_raw, list):
            for step in steps_raw:
                if isinstance(step, dict):
                    title = str(step.get("title") or "").strip()
                    allow = step.get("allow")
                    allow_text = ""
                    if isinstance(allow, list):
                        allow_text = ",".join(str(a).strip() for a in allow if str(a).strip())
                    if title and allow_text:
                        steps.append(f"{title} [allow:{allow_text}]")
                    elif title:
                        steps.append(title)
                    else:
                        steps.append(json.dumps(step, ensure_ascii=False))
                else:
                    text = str(step).strip()
                    if text:
                        steps.append(text)
        if steps:
            lines.append("步骤: " + " ; ".join(steps[:max_steps_per_solution]))
        lines.append("")

    return "\n".join(lines).strip() or "(无)"


def _extract_tool_names_from_solutions(solutions: List[dict], limit: int = 16) -> List[str]:
    """
    从方案 steps 中提取 tool_call 的工具名（用于工具优先注入）。

    兼容两种常见写法：
    - tool_call:web_fetch ...
    - [tool_call] web_fetch ...
    """
    names: List[str] = []
    seen = set()
    if not solutions:
        return names

    patterns = [
        re.compile(r"tool_call\s*[:：]\s*(?P<name>[A-Za-z0-9_.-]{2,64})", re.IGNORECASE),
        re.compile(r"\[tool_call\]\s*(?P<name>[A-Za-z0-9_.-]{2,64})", re.IGNORECASE),
    ]

    for sol in solutions:
        steps_raw = sol.get("steps") if isinstance(sol, dict) else None
        if not isinstance(steps_raw, list):
            continue
        for step in steps_raw:
            text = ""
            if isinstance(step, dict):
                text = str(step.get("title") or "").strip()
            else:
                text = str(step).strip()
            if not text:
                continue
            for pat in patterns:
                m = pat.search(text)
                if not m:
                    continue
                tool_name = str(m.group("name") or "").strip()
                if tool_name and tool_name not in seen:
                    seen.add(tool_name)
                    names.append(tool_name)
                    if len(names) >= limit:
                        return names
    return names


def _collect_tools_from_solutions(
    solutions: List[dict],
    *,
    limit: int = 8,
) -> str:
    """
    工具汇总：
    - 先把方案中提到的工具排在最前（若工具存在且已批准）
    - 再用“已注册工具清单”补齐到 limit
    """
    try:
        limit_value = int(limit)
    except Exception:
        limit_value = 8
    if limit_value <= 0:
        limit_value = 8

    tool_names = _extract_tool_names_from_solutions(solutions, limit=max(limit_value * 2, 16))

    items: List[dict] = []
    seen_names = set()

    if tool_names:
        try:
            prioritized = agent_retrieval_repo.list_tool_hints_by_names(names=tool_names, limit=limit_value)
        except Exception:
            prioritized = []
        for it in prioritized:
            name = str(it.get("name") or "").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            items.append(it)
            if len(items) >= limit_value:
                break

    if len(items) < limit_value:
        try:
            fallback = agent_retrieval_repo.list_tool_hints(limit=max(limit_value * 4, 16))
        except Exception:
            fallback = []
        for it in fallback:
            name = str(it.get("name") or "").strip()
            if not name or name in seen_names:
                continue
            seen_names.add(name)
            items.append(it)
            if len(items) >= limit_value:
                break

    if not items:
        return "(无)"

    lines = []
    for item in items:
        tid = item.get("id")
        name = item.get("name")
        desc = item.get("description") or ""
        lines.append(f"- {tid}. {name}: {desc}")
    return "\n".join(lines)


def _extract_graph_terms(message: str, limit: int = 8) -> List[str]:
    """
    从用户输入中提取少量“检索图谱”的关键字（不依赖额外 LLM 调用）。

    规则（尽量保守）：
    - 英文/数字/路径片段：长度 >= 3
    - 中文片段：长度 2-6
    """
    text = str(message or "").strip()
    if not text:
        return []
    terms: List[str] = []
    seen = set()

    for token in re.findall(r"[A-Za-z0-9_./:\\-]{3,}", text):
        t = token.strip()
        if t and t not in seen:
            seen.add(t)
            terms.append(t)
        if len(terms) >= limit:
            return terms

    for token in re.findall(r"[\u4e00-\u9fff]{2,6}", text):
        t = token.strip()
        if t and t not in seen:
            seen.add(t)
            terms.append(t)
        if len(terms) >= limit:
            return terms

    return terms


def _list_graph_candidates(message: str, limit: int) -> List[dict]:
    """
    读取图谱节点候选集：
    - 先按关键字 LIKE 搜索（命中更准）
    - 再补充最近节点（避免无命中时完全为空）
    """
    terms = _extract_graph_terms(message, limit=6)
    return agent_retrieval_repo.list_graph_candidates(terms=terms, limit=limit)


def _format_graph_candidates_for_prompt(items: List[dict]) -> str:
    if not items:
        return "(无)"
    lines: List[str] = []
    for item in items:
        gid = item.get("id")
        label = str(item.get("label") or "").strip()
        node_type = str(item.get("node_type") or "").strip()
        evidence = (
            str(item.get("evidence") or "").strip().replace("\r", " ").replace("\n", " ")
        )
        if len(evidence) > AGENT_GRAPH_PROMPT_SNIPPET_MAX_CHARS:
            evidence = evidence[:AGENT_GRAPH_PROMPT_SNIPPET_MAX_CHARS] + "..."
        meta = []
        if node_type:
            meta.append(f"type={node_type}")
        if evidence:
            meta.append(f"evidence={evidence}")
        tail = " ".join(meta).strip()
        head = f"{gid}. {label}" if gid is not None else label
        lines.append(f"{head} - {tail}" if tail else head)
    return "\n".join(lines) or "(无)"


def _load_graph_nodes_by_ids(node_ids: List[int]) -> List[dict]:
    return agent_retrieval_repo.load_graph_nodes_by_ids(node_ids)


def _load_graph_edges_between(node_ids: List[int], limit: int = 24) -> List[dict]:
    """
    只加载“候选节点集合内部”的边（避免边无限膨胀）。
    """
    return agent_retrieval_repo.load_graph_edges_between(node_ids=node_ids, limit=limit)


def _format_graph_for_prompt(nodes: List[dict]) -> str:
    """
    给 plan/ReAct prompt 用的图谱块（简短、可直接引用）。
    """
    if not nodes:
        return "(无)"
    id_list = [int(n.get("id")) for n in nodes if n.get("id") is not None]
    label_map = {
        int(n.get("id")): str(n.get("label") or "")
        for n in nodes
        if n.get("id") is not None
    }

    lines: List[str] = []
    for node in nodes:
        nid = node.get("id")
        label = str(node.get("label") or "").strip()
        node_type = str(node.get("node_type") or "").strip()
        extra = f" type={node_type}" if node_type else ""
        lines.append(f"- node#{nid}: {label}{extra}")

    edges = _load_graph_edges_between(id_list)
    if edges:
        lines.append("关系:")
        for edge in edges:
            src = label_map.get(int(edge.get("source")), str(edge.get("source")))
            tgt = label_map.get(int(edge.get("target")), str(edge.get("target")))
            rel = str(edge.get("relation") or "").strip()
            if not rel:
                continue
            lines.append(f"- {src} -[{rel}]-> {tgt}")

    return "\n".join(lines).strip() or "(无)"


def _select_relevant_graph_nodes(message: str, model: str, parameters: Optional[dict]) -> List[dict]:
    """
    先读取图谱候选节点，再用 LLM 选择最相关的少量节点。
    """
    candidates = _list_graph_candidates(message=message, limit=AGENT_GRAPH_PICK_CANDIDATE_LIMIT)
    if not candidates:
        return []
    if len(candidates) <= AGENT_GRAPH_PICK_MAX_NODES:
        return candidates[:AGENT_GRAPH_PICK_MAX_NODES]

    candidates_text = _format_graph_candidates_for_prompt(candidates)
    pick_prompt = AGENT_GRAPH_PICK_PROMPT_TEMPLATE.format(
        message=message,
        nodes=candidates_text,
        max_nodes=AGENT_GRAPH_PICK_MAX_NODES,
    )
    pick_text, _, pick_err = _cached_call_openai(
        cache_namespace="graph_pick",
        prompt=pick_prompt,
        model=model,
        params={"temperature": 0, "max_tokens": 160},
    )
    if pick_err or not pick_text:
        return []
    obj = _extract_json_object(pick_text)
    if not obj:
        return []
    ids_raw = obj.get("node_ids")
    if not isinstance(ids_raw, list):
        return []
    selected_ids: List[int] = []
    for item in ids_raw:
        try:
            nid = int(item)
        except Exception:
            continue
        if nid <= 0:
            continue
        if nid not in selected_ids:
            selected_ids.append(nid)
        if len(selected_ids) >= AGENT_GRAPH_PICK_MAX_NODES:
            break
    return _load_graph_nodes_by_ids(selected_ids)


def _assess_knowledge_sufficiency(
    message: str,
    skills: List[dict],
    graph_nodes: List[dict],
    memories: List[dict],
    model: str,
    parameters: Optional[dict],
) -> KnowledgeSufficiencyResult:
    """
    评估检索到的知识是否足以支撑任务规划。

    返回 KnowledgeSufficiencyResult，包含：
    - sufficient: 是否充分
    - reason: 判断原因
    - missing_knowledge: 缺失的知识类型
    - suggestion: 建议动作
    """
    skill_count = len(skills)
    graph_count = len(graph_nodes)
    memory_count = len(memories or [])

    # 快速路径：如果有技能且任务简单，直接认为充分
    # 这里不做过多判断，让 LLM 来评估复杂情况

    # 格式化检索结果用于 LLM 判断
    skills_text = _format_skill_candidates_for_prompt(skills) if skills else "(无)"
    graph_text = _format_graph_candidates_for_prompt(graph_nodes) if graph_nodes else "(无)"
    memories_text = _format_memory_candidates_for_prompt(memories) if memories else "(无)"

    prompt = KNOWLEDGE_SUFFICIENCY_PROMPT_TEMPLATE.format(
        message=message,
        skill_count=skill_count,
        skills=skills_text,
        graph_count=graph_count,
        graph=graph_text,
        memory_count=memory_count,
        memories=memories_text,
    )

    response_text, _, err = _cached_call_openai(
        cache_namespace="knowledge_sufficiency",
        prompt=prompt,
        model=model,
        params={"temperature": 0, "max_tokens": 200},
    )

    if err or not response_text:
        # LLM 调用失败，默认认为充分（不阻塞流程）
        return KnowledgeSufficiencyResult(
            sufficient=True,
            reason="知识评估跳过（LLM 调用失败）",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=skill_count,
            graph_count=graph_count,
            memory_count=memory_count,
        )

    obj = _extract_json_object(response_text)
    if not obj:
        # JSON 解析失败，默认认为充分
        return KnowledgeSufficiencyResult(
            sufficient=True,
            reason="知识评估跳过（解析失败）",
            missing_knowledge="none",
            suggestion="proceed",
            skill_count=skill_count,
            graph_count=graph_count,
            memory_count=memory_count,
        )

    sufficient = bool(obj.get("sufficient", True))
    reason = str(obj.get("reason", "")).strip() or "未知"
    missing_knowledge = str(obj.get("missing_knowledge", "none")).strip()
    suggestion = str(obj.get("suggestion", "proceed")).strip()

    # 验证 missing_knowledge 和 suggestion 的取值
    valid_missing = {"skill", "methodology", "tool", "domain_knowledge", "none"}
    if missing_knowledge not in valid_missing:
        missing_knowledge = "none"

    valid_suggestions = {"proceed", "compose_skills", "create_draft_skill", "ask_user"}
    if suggestion not in valid_suggestions:
        suggestion = "proceed"

    return KnowledgeSufficiencyResult(
        sufficient=sufficient,
        reason=reason,
        missing_knowledge=missing_knowledge,
        suggestion=suggestion,
        skill_count=skill_count,
        graph_count=graph_count,
        memory_count=memory_count,
    )


@dataclass
class ComposedSkillResult:
    """技能组合结果。"""
    success: bool
    name: str
    description: str
    steps: List[str]
    source_skill_ids: List[int]
    domain_id: str
    error: Optional[str] = None


@dataclass
class DraftSkillResult:
    """草拟技能结果（create_draft_skill）。"""
    success: bool
    name: str
    description: str
    steps: List[str]
    domain_id: str
    error: Optional[str] = None


def _draft_skill_from_message(
    message: str,
    skills: List[dict],
    graph_hint: str,
    domain_id: str,
    model: str,
    parameters: Optional[dict],
) -> DraftSkillResult:
    """
    当知识不足且建议 create_draft_skill 时，草拟一个 methodology 草稿技能供 planning 参考。
    """
    skills_text = _format_skill_candidates_for_prompt(skills) if skills else "(无)"
    graph_text = str(graph_hint or "").strip() or "(无)"

    prompt = SKILL_DRAFT_PROMPT_TEMPLATE.format(
        message=message,
        graph=graph_text,
        skills=skills_text,
    )

    response_text, _, err = call_openai(
        prompt, model, {"temperature": 0, "max_tokens": 260}
    )
    if err or not response_text:
        return DraftSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            domain_id=str(domain_id or "misc"),
            error=str(err or "empty_response"),
        )

    obj = _extract_json_object(response_text)
    if not obj:
        return DraftSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            domain_id=str(domain_id or "misc"),
            error="invalid_json",
        )

    name = str(obj.get("name") or "").strip()
    description = str(obj.get("description") or "").strip()

    steps_raw = obj.get("steps")
    steps: List[str] = []
    if isinstance(steps_raw, list):
        for item in steps_raw:
            s = str(item or "").strip()
            if not s:
                continue
            steps.append(s)
            if len(steps) >= 12:
                break

    if not name:
        name = "草稿技能"
    if not description:
        description = str(message or "").strip() or "（无描述）"

    if not steps:
        return DraftSkillResult(
            success=False,
            name=name,
            description=description,
            steps=[],
            domain_id=str(domain_id or "misc"),
            error="missing_steps",
        )

    # 规范化：控制步骤数量，避免过长污染上下文
    steps = steps[:8]

    return DraftSkillResult(
        success=True,
        name=name,
        description=description,
        steps=steps,
        domain_id=str(domain_id or "misc"),
        error=None,
    )


@dataclass
class DraftSolutionResult:
    """草拟方案结果。"""
    success: bool
    name: str
    description: str
    steps: List[dict]
    artifacts: List[str]
    tool_names: List[str]
    error: Optional[str] = None


def _draft_solution_from_skills(
    message: str,
    skills: List[dict],
    tools_hint: str,
    graph_hint: str,
    model: str,
    parameters: Optional[dict],
    *,
    max_steps: int = 8,
) -> DraftSolutionResult:
    """
    当“有技能但无匹配方案”时，基于技能草拟一个 draft 方案。

    说明：
    - 用于 docs/agent 的 Solution Create 流程 A（规划阶段草拟执行路径）。
    - 草拟失败不应阻塞主链路：失败时返回 success=False，让上层直接继续 planning。
    """
    if not skills:
        return DraftSolutionResult(
            success=False,
            name="",
            description="",
            steps=[],
            artifacts=[],
            tool_names=[],
            error="无可用技能草拟方案",
        )

    try:
        max_steps_value = int(max_steps)
    except Exception:
        max_steps_value = 8
    if max_steps_value <= 0:
        max_steps_value = 8
    if max_steps_value > 20:
        max_steps_value = 20

    skills_text = _format_skills_for_prompt(skills, max_steps_per_skill=4)

    prompt = AGENT_SOLUTION_DRAFT_PROMPT_TEMPLATE.format(
        message=message,
        graph=graph_hint or "(无)",
        skills=skills_text or "(无)",
        tools=tools_hint or "(无)",
        max_steps=max_steps_value,
    )

    response_text, _, err = call_openai(
        prompt, model, {"temperature": 0.2, "max_tokens": 700}
    )
    if err or not response_text:
        return DraftSolutionResult(
            success=False,
            name="",
            description="",
            steps=[],
            artifacts=[],
            tool_names=[],
            error=f"LLM 调用失败: {err}" if err else "LLM 返回空",
        )

    obj = _extract_json_object(response_text)
    if not obj:
        return DraftSolutionResult(
            success=False,
            name="",
            description="",
            steps=[],
            artifacts=[],
            tool_names=[],
            error="JSON 解析失败",
        )

    name = str(obj.get("name") or "").strip()
    description = str(obj.get("description") or "").strip()
    steps_raw = obj.get("steps")
    artifacts_raw = obj.get("artifacts")
    tool_names_raw = obj.get("tool_names") or obj.get("tools") or obj.get("tool_names_hint")

    # 归一化工具名列表（可为空）
    tool_names: List[str] = []
    if isinstance(tool_names_raw, list):
        for item in tool_names_raw:
            text = str(item or "").strip()
            if not text:
                continue
            if text not in tool_names:
                tool_names.append(text)
            if len(tool_names) >= 12:
                break
    elif isinstance(tool_names_raw, str):
        text = str(tool_names_raw).strip()
        if text:
            tool_names.append(text)

    # 归一化 artifacts（可为空）
    artifacts: List[str] = []
    if isinstance(artifacts_raw, list):
        for item in artifacts_raw:
            rel = str(item or "").strip()
            if not rel:
                continue
            if rel not in artifacts:
                artifacts.append(rel)
            if len(artifacts) >= 50:
                break
    elif isinstance(artifacts_raw, str):
        rel = str(artifacts_raw).strip()
        if rel:
            artifacts.append(rel)

    allowed_types = {
        "llm_call",
        "memory_write",
        "task_output",
        "tool_call",
        "http_request",
        "shell_command",
        "file_list",
        "file_read",
        "file_append",
        "file_write",
        "file_delete",
        "json_parse",
        "user_prompt",
    }

    def _infer_allow_from_title(title: str) -> List[str]:
        t = str(title or "").strip().lower()
        if t.startswith("file_write:"):
            return ["file_write"]
        if t.startswith("file_read:"):
            return ["file_read"]
        if t.startswith("file_list:"):
            return ["file_list"]
        if t.startswith("file_append:"):
            return ["file_append"]
        if t.startswith("file_delete:"):
            return ["file_delete"]
        if t.startswith("tool_call:") or t.startswith("[tool_call]"):
            return ["tool_call"]
        if t.startswith("shell_command:") or t.startswith("[shell_command]"):
            return ["shell_command"]
        if t.startswith("http_request:") or t.startswith("[http_request]"):
            return ["http_request"]
        if t.startswith("json_parse:") or t.startswith("[json_parse]"):
            return ["json_parse"]
        if t.startswith("llm_call:") or t.startswith("[llm_call]"):
            return ["llm_call"]
        if t.startswith("memory_write:") or t.startswith("[memory_write]"):
            return ["memory_write"]
        if t.startswith("user_prompt:") or t.startswith("[user_prompt]"):
            return ["user_prompt"]
        if t.startswith("task_output") or t.startswith("[task_output]"):
            return ["task_output"]
        return []

    def _normalize_allow_list(raw) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, list):
            values = raw
        else:
            return []
        out: List[str] = []
        for item in values:
            normalized = normalize_action_type(str(item or ""))
            if not normalized:
                continue
            normalized = str(normalized).strip().lower()
            if normalized not in allowed_types:
                continue
            if normalized not in out:
                out.append(normalized)
        return out

    # 归一化 steps
    steps: List[dict] = []
    if isinstance(steps_raw, list):
        for raw in steps_raw:
            if isinstance(raw, dict):
                title = str(raw.get("title") or raw.get("name") or "").strip()
                allow = _normalize_allow_list(raw.get("allow") or raw.get("allows") or raw.get("allowed"))
            else:
                title = str(raw or "").strip()
                allow = []
            if not title:
                continue
            if not allow:
                allow = _infer_allow_from_title(title)
            if not allow:
                allow = ["llm_call"]
            steps.append({"title": title, "allow": allow})
            if len(steps) >= max_steps_value:
                break

    # 兜底：无有效 steps 视为失败（避免创建空方案污染库）
    if not steps:
        return DraftSolutionResult(
            success=False,
            name="",
            description="",
            steps=[],
            artifacts=artifacts,
            tool_names=tool_names,
            error="草拟方案缺少有效 steps",
        )

    # 兜底：确保包含 task_output（便于 plan 参考“收尾输出”）
    has_task_output = any("task_output" in (s.get("allow") or []) for s in steps if isinstance(s, dict))
    if not has_task_output:
        if len(steps) < max_steps_value:
            steps.append({"title": "task_output 输出结果", "allow": ["task_output"]})
        else:
            steps[-1] = {"title": "task_output 输出结果", "allow": ["task_output"]}

    # 名称兜底：避免空 name 导致上层无法落库
    if not name:
        base = str(message or "").strip().replace("\r", " ").replace("\n", " ")
        name = base[:40] if base else "草稿方案"
    if not description:
        description = str(message or "").strip().replace("\r", " ").replace("\n", " ")[:200]

    return DraftSolutionResult(
        success=True,
        name=name,
        description=description,
        steps=steps,
        artifacts=artifacts,
        tool_names=tool_names,
    )


def _compose_skills(
    message: str,
    skills: List[dict],
    model: str,
    parameters: Optional[dict],
) -> ComposedSkillResult:
    """
    组合已有技能生成新的复合技能。

    当知识不充分时，尝试将多个相关技能组合成一个新的复合技能，
    以支撑当前任务的规划和执行。
    """
    if not skills:
        return ComposedSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            source_skill_ids=[],
            domain_id="misc",
            error="无可用技能进行组合",
        )

    # 格式化技能列表用于 LLM
    skills_text = _format_skills_for_prompt(skills)

    prompt = SKILL_COMPOSE_PROMPT_TEMPLATE.format(
        message=message,
        skills=skills_text,
    )

    response_text, _, err = call_openai(
        prompt, model, {"temperature": 0.3, "max_tokens": 500}
    )

    if err or not response_text:
        return ComposedSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            source_skill_ids=[],
            domain_id="misc",
            error=f"LLM 调用失败: {err}" if err else "LLM 返回空",
        )

    obj = _extract_json_object(response_text)
    if not obj:
        return ComposedSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            source_skill_ids=[],
            domain_id="misc",
            error="JSON 解析失败",
        )

    name = str(obj.get("name", "")).strip()
    description = str(obj.get("description", "")).strip()
    steps_raw = obj.get("steps", [])
    source_skills_raw = obj.get("source_skills", [])
    domain_id = str(obj.get("domain_id", "misc")).strip()

    # 验证和清理 steps
    steps: List[str] = []
    if isinstance(steps_raw, list):
        for step in steps_raw:
            step_str = str(step).strip()
            if step_str:
                steps.append(step_str)

    # 验证和清理 source_skill_ids
    source_skill_ids: List[int] = []
    if isinstance(source_skills_raw, list):
        for sid in source_skills_raw:
            try:
                source_skill_ids.append(int(sid))
            except (TypeError, ValueError):
                continue

    if not name:
        return ComposedSkillResult(
            success=False,
            name="",
            description="",
            steps=[],
            source_skill_ids=[],
            domain_id="misc",
            error="组合结果缺少名称",
        )

    return ComposedSkillResult(
        success=True,
        name=name,
        description=description,
        steps=steps,
        source_skill_ids=source_skill_ids,
        domain_id=domain_id or "misc",
    )

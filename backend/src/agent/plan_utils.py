import os
import re
from typing import List, Optional

from backend.src.actions.registry import normalize_action_type
from backend.src.constants import (
    ACTION_TYPE_FILE_APPEND,
    ACTION_TYPE_FILE_DELETE,
    ACTION_TYPE_FILE_LIST,
    ACTION_TYPE_FILE_READ,
    ACTION_TYPE_FILE_WRITE,
    ACTION_TYPE_HTTP_REQUEST,
    ACTION_TYPE_JSON_PARSE,
    ACTION_TYPE_LLM_CALL,
    ACTION_TYPE_MEMORY_WRITE,
    ACTION_TYPE_SHELL_COMMAND,
    ACTION_TYPE_TASK_OUTPUT,
    ACTION_TYPE_TOOL_CALL,
    ACTION_TYPE_USER_PROMPT,
    AGENT_EXPERIMENT_DIR_REL,
    AGENT_PLAN_BRIEF_MAX_CHARS,
)


def _looks_like_file_path(value: str) -> bool:
    """
    判断一个字符串是否“像路径”：
    - 目的：避免 LLM 把 file_write 的 title 写成自然语言（如“编写”），却被当作文件名强行覆盖 payload.path，
      导致写入到错误文件并引发后续脚本找不到/文件不匹配等连锁问题。
    - 这里只做启发式：宁可不覆盖，也不要把明显不是路径的词当成文件名。
    """
    raw = str(value or "").strip()
    if not raw:
        return False
    # Windows 绝对路径
    if re.match(r"^[A-Za-z]:[\\/]", raw):
        return True
    if "/" in raw or "\\" in raw:
        return True
    base = raw.split("/")[-1].split("\\")[-1].strip()
    if not base:
        return False
    # 常见“无扩展名但确实是文件名”的场景
    if base in {"Makefile", "Dockerfile", "LICENSE", "README", "README.md", ".env", ".gitignore"}:
        return True
    # dotfile / 有扩展名
    if base.startswith(".") and len(base) > 1:
        return True
    if "." in base:
        return True
    return False


def _fallback_brief_from_title(title: str, max_len: int = AGENT_PLAN_BRIEF_MAX_CHARS) -> str:
    """
    planning 输出缺少 brief 时的兜底：
    - 去掉常见 action 前缀
    - 优先取第一段描述，截断到固定长度
    """
    value = str(title or "").strip()
    if not value:
        return ""
    for prefix in (
        "tool_call:",
        "tool_call：",
        "llm_call:",
        "llm_call：",
        "task_output:",
        "task_output：",
        "shell_command:",
        "shell_command：",
        "script_run:",
        "script_run：",
    ):
        if value.startswith(prefix):
            value = value[len(prefix) :].strip()
            break
    # 取第一段（到空格/冒号为止），避免 URL 或参数撑满 UI
    for sep in (" ", "：", ":"):
        if sep in value:
            value = value.split(sep, 1)[0].strip()
            break
    if len(value) > max_len:
        value = value[:max_len]
    return value


def sanitize_plan_brief(value: str, *, fallback_title: str = "") -> str:
    """
    统一清洗 plan brief，保证 UI 文本稳定可读。

    规则：
    - 先 trim；为空时可回退到 title 兜底 brief；
    - 移除空格与中英文冒号；
    - 截断到 AGENT_PLAN_BRIEF_MAX_CHARS。
    """
    text = str(value or "").strip()
    if not text and fallback_title:
        text = _fallback_brief_from_title(fallback_title)
    text = text.replace(" ", "").replace("：", "").replace(":", "")
    if len(text) > AGENT_PLAN_BRIEF_MAX_CHARS:
        text = text[:AGENT_PLAN_BRIEF_MAX_CHARS]
    return text


def extract_file_write_target_path(step_title: str) -> str:
    """
    从步骤标题中提取 file_write 的目标路径。

    约定：
    - title 形如：`file_write:relative/path.md 写入...`
    - 或 `file_write:"path with space.md" ...`
    """
    raw = str(step_title or "").strip()
    if not raw:
        return ""
    match = re.match(r"^file_write[:：]\s*(\"[^\"]+\"|'[^']+'|\S+)", raw)
    if not match:
        return ""
    value = str(match.group(1) or "").strip()
    if (value.startswith("\"") and value.endswith("\"")) or (
        value.startswith("'") and value.endswith("'")
    ):
        value = value[1:-1].strip()
    return value


def coerce_file_write_payload_path_from_title(step_title: str, payload: dict) -> dict:
    """
    为 file_write action 增加“路径对齐”的兜底：
    - 如果 payload.path 为空或与 title 中声明的路径不一致，则以 title 为准覆盖 path。
    """
    if not isinstance(payload, dict):
        return {}
    target = extract_file_write_target_path(step_title)
    if not target or not _looks_like_file_path(target):
        return payload
    current = str(payload.get("path") or "").strip()
    if not current or current != target:
        patched = dict(payload)
        patched["path"] = target
        return patched
    return payload


def _normalize_plan_titles(
    plan_obj: dict, max_steps: int
) -> tuple[
    Optional[List[str]],
    Optional[List[str]],
    Optional[List[List[str]]],
    Optional[List[str]],
    Optional[str],
]:
    """
    兼容多种结构：
    - 新：{"plan":[{"title":"...","brief":"...","allow":["tool_call"]}, ...], "artifacts":[...]}
    - 旧：{"plan":[{"title":"..."}, "..."]}（缺少 allow 会报错，触发重新规划）
    - 旧：{"steps":[{"title":"...","action":...}, ...]}（缺少 allow 会报错）
    """
    if not isinstance(plan_obj, dict):
        return None, None, None, None, "计划不是对象"
    items = None
    if isinstance(plan_obj.get("plan"), list):
        items = plan_obj.get("plan")
    elif isinstance(plan_obj.get("steps"), list):
        items = plan_obj.get("steps")
    if not isinstance(items, list) or not items:
        return None, None, None, None, "计划为空"

    raw_artifacts = plan_obj.get("artifacts")
    artifacts: List[str] = []
    if isinstance(raw_artifacts, list):
        for item in raw_artifacts:
            value = str(item or "").strip()
            if value:
                artifacts.append(value)

    def _normalize_allow_list(raw) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, list):
            values = raw
        else:
            return []
        result: List[str] = []
        for item in values:
            normalized = normalize_action_type(str(item))
            if normalized and normalized not in result:
                result.append(normalized)
        return result

    titles: List[str] = []
    briefs: List[str] = []
    allows: List[List[str]] = []
    for item in items:
        title = ""
        brief = ""
        allow_raw = None
        if isinstance(item, str):
            title = item
        elif isinstance(item, dict):
            title = item.get("title") or ""
            brief = item.get("brief") or item.get("short") or item.get("summary") or ""
            allow_raw = item.get("allow")
            if allow_raw is None:
                allow_raw = item.get("allowed") or item.get("allowed_actions")
        title = str(title).strip()
        brief = str(brief).strip()
        if title:
            titles.append(title)
            briefs.append(brief or _fallback_brief_from_title(title))
            allows.append(_normalize_allow_list(allow_raw))
    if not titles:
        return None, None, None, None, "计划 title 为空"
    if len(titles) > max_steps:
        return None, None, None, None, f"计划 steps 超出上限: {len(titles)} > {max_steps}"
    # briefs 兜底：保证长度与 titles 对齐
    while len(briefs) < len(titles):
        briefs.append(_fallback_brief_from_title(titles[len(briefs)]))
    while len(allows) < len(titles):
        allows.append([])

    # allow 是执行阶段关键约束：缺失会导致“看起来完成但未执行/未落盘”
    for idx, allow in enumerate(allows, start=1):
        if not allow:
            return None, None, None, None, f"第 {idx} 步 allow 不能为空"

    # 兜底：如果步骤标题显式声明了 file_write，则强制该步只允许 file_write。
    # 目的：避免 LLM 在“写文件步骤”里选择 llm_call/task_output 等动作，导致 artifacts 校验失败。
    for idx, title in enumerate(titles):
        value = str(title or "").strip()
        if value.startswith("file_write:") or value.startswith("file_write："):
            allows[idx] = [ACTION_TYPE_FILE_WRITE]

    return titles, briefs, allows, artifacts, None


def repair_plan_artifacts_with_file_write_steps(
    *,
    titles: List[str],
    briefs: List[str],
    allows: List[List[str]],
    artifacts: List[str],
    max_steps: int,
) -> tuple[List[str], List[str], List[List[str]], List[str], Optional[str], int]:
    """
    计划修复：当声明了 artifacts（预期写入/更新文件）但 file_write 步骤数不足时，自动补齐。

    背景：
    - 执行链路是“每个计划步骤 -> 只执行一条 action”
    - file_write action 一次只能写一个文件
    - 如果 artifacts 有 N 个，但 allow 包含 file_write 的步骤少于 N 个，会导致最后输出阶段校验缺文件而失败

    修复策略（尽量不依赖再次调用 LLM）：
    1) 先尝试把已有 file_write 步骤“绑定”到 artifacts（把文件路径写进步骤 title）
    2) 对剩余未覆盖的 artifacts，在 task_output 之前插入补齐的 file_write 步骤

    返回：
    - 修复后的 titles/briefs/allows/artifacts
    - error：无法在 max_steps 内修复时返回错误
    - patched_count：新增的 file_write 步骤数量（用于给前端/日志提示）
    """
    if not artifacts:
        return titles, briefs, allows, artifacts, None, 0

    def _extract_path_from_title(value: str) -> str:
        return extract_file_write_target_path(value)

    def _ensure_file_write_title(original: str, rel_path: str) -> str:
        raw = str(original or "").strip()
        # 去掉已存在的 file_write 前缀，避免重复
        for prefix in ("file_write:", "file_write："):
            if raw.startswith(prefix):
                raw = raw[len(prefix) :].strip()
                break
        # 如果原 title 以路径开头，剔除路径部分避免重复
        if raw.startswith(rel_path):
            raw = raw[len(rel_path) :].strip()
        suffix = raw.strip()
        if suffix:
            return f"file_write:{rel_path} {suffix}".strip()
        return f"file_write:{rel_path} 写入文件"

    # 找到所有 file_write 允许的步骤
    file_step_indices: List[int] = [
        idx
        for idx, allow in enumerate(allows)
        if ACTION_TYPE_FILE_WRITE in set(allow or [])
    ]
    if not file_step_indices:
        file_step_indices = []

    artifact_list = [str(a or "").strip() for a in artifacts if str(a or "").strip()]
    if not artifact_list:
        return titles, briefs, allows, [], None, 0

    assigned: List[str] = []
    unassigned_steps: List[int] = []

    # 先识别“已显式绑定”的 file_write 步骤
    artifact_set = set(artifact_list)
    for idx in file_step_indices:
        path_in_title = (
            _extract_path_from_title(titles[idx]) if 0 <= idx < len(titles) else ""
        )
        if (
            path_in_title
            and path_in_title in artifact_set
            and path_in_title not in assigned
        ):
            # 已绑定：强制该步只允许 file_write，避免执行阶段选错动作导致未落盘
            if 0 <= idx < len(allows):
                allows[idx] = [ACTION_TYPE_FILE_WRITE]
            assigned.append(path_in_title)
        else:
            unassigned_steps.append(idx)

    # 将未绑定的 file_write 步骤依次绑定到未覆盖的 artifact
    for idx in unassigned_steps:
        candidate = ""
        for a in artifact_list:
            if a not in assigned:
                candidate = a
                break
        if not candidate:
            break
        if 0 <= idx < len(titles):
            titles[idx] = _ensure_file_write_title(titles[idx], candidate)
        # 绑定后强制该步只允许 file_write，确保写文件动作一定发生
        if 0 <= idx < len(allows):
            allows[idx] = [ACTION_TYPE_FILE_WRITE]
        assigned.append(candidate)

    # 仍未覆盖的 artifacts：需要补齐新的 file_write 步骤
    missing: List[str] = [a for a in artifact_list if a not in assigned]
    if not missing:
        return titles, briefs, allows, artifact_list, None, 0

    # 插入点：第一个 task_output 之前（通常是最后一步），确保写文件在输出前完成
    output_index = len(titles)
    for i, allow in enumerate(allows):
        if ACTION_TYPE_TASK_OUTPUT in set(allow or []):
            output_index = i
            break

    needed = len(missing)
    if len(titles) + needed > max_steps:
        return (
            titles,
            briefs,
            allows,
            artifact_list,
            (
                f"无法在 max_steps={max_steps} 内补齐写文件步骤（需要新增 {needed} 步，当前已有 {len(titles)} 步）"
            ),
            0,
        )

    patched_count = 0
    for rel_path in missing:
        insert_title = f"file_write:{rel_path} 写入文件"
        titles.insert(output_index, insert_title)
        briefs.insert(output_index, "写文件")
        allows.insert(output_index, [ACTION_TYPE_FILE_WRITE])
        output_index += 1
        patched_count += 1

    return titles, briefs, allows, artifact_list, None, patched_count


def reorder_script_file_writes_before_exec_steps(
    *,
    titles: List[str],
    briefs: List[str],
    allows: List[List[str]],
) -> tuple[List[str], List[str], List[List[str]], int]:
    """
    计划修复：将“脚本类文件”的 file_write 步骤尽量前置到首个可执行步骤（tool_call/shell_command）之前。

    背景：
    - 模型经常把 tool_call/shell_command 放在写脚本之前，导致执行时报：
      python: can't open file '.../xxx.py'（脚本尚未落盘）
    - 虽然后续可能通过 replan 自愈，但会产生一次失败记录并影响观感与评估证据。

    策略：
    - 找到计划中第一个“可执行步骤”（allow 含 tool_call 或 shell_command）
    - 将其后的“脚本类 file_write”（.py/.sh/.ps1/.js/.ts 等）移动到该步骤之前
    - 仅重排顺序，不增减步骤数
    """
    if not titles or not allows:
        return titles, briefs, allows, 0

    first_exec_idx = None
    for idx, allow in enumerate(allows):
        allow_set = set(allow or [])
        if ACTION_TYPE_TOOL_CALL in allow_set or ACTION_TYPE_SHELL_COMMAND in allow_set:
            first_exec_idx = idx
            break
    if first_exec_idx is None:
        return titles, briefs, allows, 0

    script_exts = {".py", ".sh", ".ps1", ".js", ".ts", ".cmd", ".bat"}
    movable: List[int] = []
    for idx in range(int(first_exec_idx) + 1, len(titles)):
        allow_set = set(allows[idx] or []) if idx < len(allows) else set()
        if ACTION_TYPE_FILE_WRITE not in allow_set:
            continue
        path = extract_file_write_target_path(titles[idx])
        ext = os.path.splitext(str(path or ""))[1].lower()
        if ext and ext in script_exts:
            movable.append(idx)

    if not movable:
        return titles, briefs, allows, 0

    prefix = list(range(0, int(first_exec_idx)))
    moved = list(movable)
    rest = [idx for idx in range(int(first_exec_idx), len(titles)) if idx not in set(movable)]
    new_order = prefix + moved + rest

    new_titles = [titles[i] for i in new_order]
    new_briefs = [briefs[i] for i in new_order] if len(briefs) == len(titles) else briefs
    new_allows = [allows[i] for i in new_order]
    return new_titles, new_briefs, new_allows, len(movable)


def drop_non_artifact_file_write_steps(
    *,
    titles: List[str],
    briefs: List[str],
    allows: List[List[str]],
    artifacts: List[str],
) -> tuple[List[str], List[str], List[List[str]], int]:
    """
    约束：当 artifacts 非空时，禁止出现“写入非 artifacts 路径”的 file_write 步骤。
    - 若 file_write.title 无法解析出路径或路径不在 artifacts 中，则直接移除该步骤。
    """
    artifact_set = {str(a or "").strip() for a in (artifacts or []) if str(a or "").strip()}
    if not artifact_set:
        return titles, briefs, allows, 0
    experiment_rel = str(AGENT_EXPERIMENT_DIR_REL or "").strip().replace("\\", "/")

    new_titles: List[str] = []
    new_briefs: List[str] = []
    new_allows: List[List[str]] = []
    removed = 0

    for idx, title in enumerate(titles):
        allow_value = allows[idx] if idx < len(allows) else []
        brief_value = briefs[idx] if idx < len(briefs) else _fallback_brief_from_title(title)
        if ACTION_TYPE_FILE_WRITE in set(allow_value or []):
            target = extract_file_write_target_path(title)
            if not target:
                removed += 1
                continue
            target_norm = str(target).strip().replace("\\", "/")
            if experiment_rel and (
                target_norm == experiment_rel
                or target_norm.startswith(experiment_rel + "/")
            ):
                # 允许写入实验目录：用于工具自举与临时脚本
                pass
            elif target not in artifact_set:
                removed += 1
                continue
        new_titles.append(title)
        new_briefs.append(brief_value)
        new_allows.append(list(allow_value or []))

    return new_titles, new_briefs, new_allows, removed


def apply_next_step_patch(
    current_step_index: int,
    patch_obj: dict,
    plan_titles: List[str],
    plan_items: List[dict],
    plan_allows: List[List[str]],
    plan_artifacts: List[str],
    *,
    max_steps: Optional[int] = None,
) -> Optional[str]:
    """
    受限计划修正（plan_patch）：只允许修改“当前步骤的下一步”（k+1）。

    设计目标：
    - 支持 ReAct “走一步看一步”时微调下一步
    - 禁止跨多步重写，避免跑偏

    约定：
    - patch_obj.step_index（可选）必须等于 current_step_index + 1
    - 支持修改：title/brief/allow/artifacts_add
    - 支持插入：insert_steps（在“下一步位置”插入 1..N 个新步骤，会把原下一步及后续步骤整体后移）
    """
    # plan_patch 必须是“原子操作”：校验/修复失败时不应留下半改动。
    if not isinstance(patch_obj, dict):
        return "plan_patch 不是对象"

    try:
        k = int(current_step_index)
    except Exception:
        return "current_step_index 不合法"
    if k < 1:
        return "current_step_index 不合法"

    next_index = k + 1

    # step_index 可选，但若给了必须等于下一步
    if patch_obj.get("step_index") is not None:
        try:
            wanted = int(patch_obj.get("step_index"))
        except Exception:
            return "plan_patch.step_index 不合法"
        if wanted != next_index:
            return f"plan_patch.step_index 只能是 {next_index}"

    title = patch_obj.get("title")
    brief = patch_obj.get("brief")
    allow_raw = patch_obj.get("allow")
    artifacts_raw = patch_obj.get("artifacts_add")
    if artifacts_raw is None:
        artifacts_raw = patch_obj.get("artifacts")
    insert_steps_raw = patch_obj.get("insert_steps")
    if insert_steps_raw is None:
        insert_steps_raw = patch_obj.get("steps")

    has_any_change = any(
        value is not None
        for value in (
            title,
            brief,
            allow_raw,
            artifacts_raw,
            insert_steps_raw,
        )
    )
    if not has_any_change:
        return "plan_patch 为空"

    limit = None
    if isinstance(max_steps, int) and max_steps > 0:
        limit = int(max_steps)

    # --- 在副本上应用 patch ---
    orig_items = [
        dict(it) if isinstance(it, dict) else {"id": 0, "brief": "", "status": "pending"}
        for it in (plan_items or [])
    ]
    titles = [str(t or "").strip() for t in (plan_titles or []) if str(t or "").strip()]
    allows = [list(a or []) for a in (plan_allows or [])]
    items = [dict(it) for it in orig_items]
    artifacts = [str(a or "").strip() for a in (plan_artifacts or []) if str(a or "").strip()]

    # 对齐长度：避免下游 index 出界
    while len(allows) < len(titles):
        allows.append([])
    while len(items) < len(titles):
        items.append({"id": 0, "brief": _fallback_brief_from_title(titles[len(items)]), "status": "pending"})
    briefs = [str((it or {}).get("brief") or "").strip() for it in items]

    allowed_types = {
        ACTION_TYPE_LLM_CALL,
        ACTION_TYPE_MEMORY_WRITE,
        ACTION_TYPE_TASK_OUTPUT,
        ACTION_TYPE_TOOL_CALL,
        ACTION_TYPE_HTTP_REQUEST,
        ACTION_TYPE_SHELL_COMMAND,
        ACTION_TYPE_FILE_LIST,
        ACTION_TYPE_FILE_READ,
        ACTION_TYPE_FILE_APPEND,
        ACTION_TYPE_FILE_WRITE,
        ACTION_TYPE_FILE_DELETE,
        ACTION_TYPE_JSON_PARSE,
        ACTION_TYPE_USER_PROMPT,
    }

    def _sanitize_brief(value: str) -> str:
        return sanitize_plan_brief(value)

    def _normalize_action_type(value: str) -> Optional[str]:
        normalized = normalize_action_type(str(value or ""))
        return normalized if normalized in allowed_types else None

    def _normalize_allow_list(raw) -> List[str]:
        if raw is None:
            return []
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, list):
            values = raw
        else:
            return []
        result: List[str] = []
        for item in values:
            normalized = _normalize_action_type(str(item))
            if normalized and normalized not in result:
                result.append(normalized)
        return result

    def _normalize_artifact_token(value: object) -> str:
        raw_value = str(value or "").strip().strip('"').strip("'").strip()
        if not raw_value:
            return ""
        return raw_value.replace("\\", "/")

    def _append_artifacts(raw) -> None:
        if raw is None:
            return

        values: List[str] = []
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, list):
            values = [str(v) for v in raw]

        existing_tokens = {
            _normalize_artifact_token(v)
            for v in (artifacts or [])
            if _normalize_artifact_token(v)
        }
        existing_basenames = {
            os.path.basename(token).lower()
            for token in existing_tokens
            if token
        }

        file_targets: List[str] = []
        for title_text in (titles or []):
            target = _normalize_artifact_token(extract_file_write_target_path(title_text))
            if target:
                file_targets.append(target)

        for v in values:
            token = _normalize_artifact_token(v)
            if not token:
                continue

            is_abs = bool(re.match(r"^[A-Za-z]:/", token)) or token.startswith("/")
            if is_abs:
                basename = os.path.basename(token).lower()
                if basename and basename in existing_basenames:
                    continue

                mapped = ""
                if basename:
                    for target in file_targets:
                        if os.path.basename(target).lower() == basename:
                            mapped = target
                            break
                if not mapped:
                    continue
                token = mapped

            if token in existing_tokens:
                continue

            artifacts.append(token)
            existing_tokens.add(token)
            basename = os.path.basename(token).lower()
            if basename:
                existing_basenames.add(basename)

    # --- 插入新步骤（会把原 next_index 以及后续整体后移） ---
    if insert_steps_raw is not None:
        if not isinstance(insert_steps_raw, list) or not insert_steps_raw:
            return "plan_patch.insert_steps 不能为空"
        if limit is not None and len(titles) + len(insert_steps_raw) > limit:
            return f"plan_patch.insert_steps 超出 max_steps={limit}"

        normalized_insert_steps: List[dict] = []
        insert_at = max(0, min(len(titles), next_index - 1))
        for i, raw in enumerate(insert_steps_raw, start=1):
            if not isinstance(raw, dict):
                return f"plan_patch.insert_steps[{i}] 不是对象"
            step_title = str(raw.get("title") or "").strip()
            if not step_title:
                return f"plan_patch.insert_steps[{i}].title 不能为空"
            step_allow = _normalize_allow_list(raw.get("allow"))
            if not step_allow:
                return f"plan_patch.insert_steps[{i}].allow 不能为空"
            step_brief = str(raw.get("brief") or "").strip() or _fallback_brief_from_title(step_title)
            step_brief = _sanitize_brief(step_brief)
            if not step_brief:
                step_brief = _sanitize_brief(_fallback_brief_from_title(step_title))

            normalized_insert_steps.append(
                {
                    "title": step_title,
                    "allow": list(step_allow),
                    "brief": step_brief,
                }
            )

        # 约束：插入步骤里若存在“脚本 file_write”，必须排在首个 shell/tool 执行步骤之前。
        # 背景：模型经常把 shell_command 放在写脚本之前，导致运行时报脚本不存在。
        script_exts = {".py", ".sh", ".ps1", ".js", ".ts", ".cmd", ".bat"}

        def _is_exec_step(allow_list: List[str]) -> bool:
            allow_set = set(allow_list or [])
            return ACTION_TYPE_SHELL_COMMAND in allow_set or ACTION_TYPE_TOOL_CALL in allow_set

        def _is_script_file_write_step(step_obj: dict) -> bool:
            allow_set = set(step_obj.get("allow") or [])
            if ACTION_TYPE_FILE_WRITE not in allow_set:
                return False
            target = extract_file_write_target_path(str(step_obj.get("title") or ""))
            ext = os.path.splitext(str(target or ""))[1].lower()
            return bool(ext and ext in script_exts)

        first_exec_idx = None
        for idx, step_obj in enumerate(normalized_insert_steps):
            if _is_exec_step(step_obj.get("allow") or []):
                first_exec_idx = idx
                break
        if first_exec_idx is not None:
            movable = [
                idx
                for idx in range(int(first_exec_idx) + 1, len(normalized_insert_steps))
                if _is_script_file_write_step(normalized_insert_steps[idx])
            ]
            if movable:
                prefix = list(range(0, int(first_exec_idx)))
                moved = list(movable)
                rest = [
                    idx
                    for idx in range(int(first_exec_idx), len(normalized_insert_steps))
                    if idx not in set(movable)
                ]
                new_order = prefix + moved + rest
                normalized_insert_steps = [normalized_insert_steps[i] for i in new_order]

        offset = 0
        for step_obj in normalized_insert_steps:
            step_title = str(step_obj.get("title") or "").strip()
            step_allow = list(step_obj.get("allow") or [])
            step_brief = str(step_obj.get("brief") or "").strip() or _sanitize_brief(
                _fallback_brief_from_title(step_title)
            )

            titles.insert(insert_at + offset, step_title)
            allows.insert(insert_at + offset, step_allow)
            items.insert(insert_at + offset, {"id": 0, "brief": step_brief, "status": "pending"})
            offset += 1
    else:
        # --- 修改下一步（或在最后一步后追加一个新步骤） ---
        is_append = next_index > len(titles)
        if is_append:
            title_value = str(title or "").strip()
            if not title_value:
                return "plan_patch.title 不能为空"
            allow_list = _normalize_allow_list(allow_raw)
            if not allow_list:
                return "plan_patch.allow 不能为空"
            if limit is not None and len(titles) + 1 > limit:
                return f"plan_patch 超出 max_steps={limit}"

            if brief is not None:
                brief_value = str(brief or "").strip()
                if not brief_value:
                    return "plan_patch.brief 不能为空"
            else:
                brief_value = _fallback_brief_from_title(title_value)
            brief_value = _sanitize_brief(brief_value) or _sanitize_brief(_fallback_brief_from_title(title_value))

            titles.append(title_value)
            allows.append(allow_list)
            items.append({"id": 0, "brief": brief_value, "status": "pending"})
        else:
            if title is not None:
                title_value = str(title or "").strip()
                if not title_value:
                    return "plan_patch.title 不能为空"
                titles[next_index - 1] = title_value

        if allow_raw is not None:
            allow_list = _normalize_allow_list(allow_raw)
            if not allow_list:
                return "plan_patch.allow 不能为空"
            if next_index - 1 < len(allows):
                allows[next_index - 1] = allow_list

            # brief：若未提供，则从（可能已更新的）title 推导
            if brief is not None:
                brief_value = str(brief or "").strip()
                if not brief_value:
                    return "plan_patch.brief 不能为空"
            else:
                brief_value = _fallback_brief_from_title(titles[next_index - 1])
            brief_value = _sanitize_brief(brief_value) or _sanitize_brief(_fallback_brief_from_title(titles[next_index - 1]))
            if next_index - 1 < len(items) and isinstance(items[next_index - 1], dict):
                items[next_index - 1]["brief"] = brief_value

    # artifacts 仅支持“追加”，避免 patch 里直接覆盖导致验收丢失
    _append_artifacts(artifacts_raw)

    # 计划一致性强约束：artifacts 必须被 file_write 步骤覆盖（否则会在 task_output 前被拦截）
    if artifacts:
        max_limit = limit if limit is not None else (len(titles) + len(artifacts) + 10)
        (
            repaired_titles,
            repaired_briefs,
            repaired_allows,
            repaired_artifacts,
            repair_err,
            _patched_count,
        ) = repair_plan_artifacts_with_file_write_steps(
            titles=list(titles),
            briefs=list(briefs),
            allows=list(allows),
            artifacts=list(artifacts),
            max_steps=int(max_limit),
        )
        if repair_err:
            return f"artifacts/file_write 不一致：{repair_err}"

        # 重新构建 plan_items：保留当前步之前（含当前步）的 status，后续步骤全部 pending
        new_items: List[dict] = []
        for idx, step_title in enumerate(repaired_titles, start=1):
            brief_value = ""
            if idx - 1 < len(repaired_briefs):
                brief_value = str(repaired_briefs[idx - 1] or "").strip()
            if not brief_value:
                brief_value = _fallback_brief_from_title(step_title)
            brief_value = _sanitize_brief(brief_value) or _sanitize_brief(_fallback_brief_from_title(step_title))
            status = "pending"
            if idx <= len(orig_items) and idx <= k:
                try:
                    status = str(orig_items[idx - 1].get("status") or "pending")
                except Exception:
                    status = "pending"
            new_items.append({"id": idx, "brief": brief_value, "status": status})

        titles = repaired_titles
        allows = repaired_allows
        artifacts = repaired_artifacts
        items = new_items
        briefs = [str((it or {}).get("brief") or "").strip() for it in items]

    def _compact_plan_steps(
        titles_in: List[str],
        briefs_in: List[str],
        allows_in: List[List[str]],
        items_in: List[dict],
        artifacts_in: List[str],
    ) -> tuple[List[str], List[str], List[List[str]], List[dict]]:
        if not titles_in:
            return titles_in, briefs_in, allows_in, items_in
        artifact_set = {str(a or "").strip() for a in (artifacts_in or []) if str(a or "").strip()}
        experiment_rel = str(AGENT_EXPERIMENT_DIR_REL or "").strip().replace("\\", "/")

        new_titles: List[str] = []
        new_briefs: List[str] = []
        new_allows: List[List[str]] = []
        new_items: List[dict] = []
        prev_title = None
        prev_allow: Optional[List[str]] = None
        file_write_bound = 0
        if artifact_set:
            for idx, allow_value in enumerate(allows_in):
                if ACTION_TYPE_FILE_WRITE in set(allow_value or []):
                    path = extract_file_write_target_path(titles_in[idx] if idx < len(titles_in) else "")
                    if path and path in artifact_set:
                        file_write_bound += 1

        for i, title_value in enumerate(titles_in):
            allow_value = allows_in[i] if i < len(allows_in) else []
            item_value = items_in[i] if i < len(items_in) else {"id": 0, "brief": "", "status": "pending"}
            status = str(item_value.get("status") or "pending")

            if (
                artifact_set
                and ACTION_TYPE_FILE_WRITE in set(allow_value or [])
                and status == "pending"
            ):
                target = extract_file_write_target_path(title_value)
                target_norm = str(target or "").strip().replace("\\", "/")
                in_experiment_dir = bool(
                    target_norm
                    and experiment_rel
                    and (
                        target_norm == experiment_rel
                        or target_norm.startswith(experiment_rel + "/")
                    )
                )
                if not target or target not in artifact_set:
                    # 实验目录脚本是执行依赖，不应因为 artifacts 已覆盖就被压缩掉。
                    if (not in_experiment_dir) and file_write_bound >= len(artifact_set):
                        continue

            if status == "pending" and prev_title == title_value and set(prev_allow or []) == set(allow_value or []):
                continue

            new_titles.append(title_value)
            new_briefs.append(briefs_in[i] if i < len(briefs_in) else _fallback_brief_from_title(title_value))
            new_allows.append(list(allow_value or []))
            new_items.append(dict(item_value))
            prev_title = title_value
            prev_allow = list(allow_value or [])

        for idx, item in enumerate(new_items, start=1):
            if isinstance(item, dict):
                item["id"] = idx
        return new_titles, new_briefs, new_allows, new_items

    briefs = [str((it or {}).get("brief") or "").strip() for it in items]
    titles, briefs, allows, items = _compact_plan_steps(titles, briefs, allows, items, artifacts)

    # 重新编号：保持 plan_items.id 与顺序一致
    for idx, it in enumerate(items, start=1):
        if isinstance(it, dict):
            it["id"] = idx

    # --- 提交到原列表（原子替换） ---
    plan_titles.clear()
    plan_titles.extend(titles)
    plan_allows.clear()
    plan_allows.extend([list(a or []) for a in allows])
    plan_artifacts.clear()
    plan_artifacts.extend([str(a or "").strip() for a in artifacts if str(a or "").strip()])
    plan_items.clear()
    plan_items.extend([dict(it) for it in items])

    return None

from typing import Optional

from backend.src.actions.registry import validate_action_object


def _validate_action(action_obj: dict) -> Optional[str]:
    """
    ReAct 每一步只校验 action 结构与关键字段，具体执行结果由执行器负责。

    说明：
    - 校验规则集中在 actions.registry（对应质量报告 P2#7：避免多处硬编码）；
    - 这里保留旧函数名，避免大面积改动 import。
    """
    return validate_action_object(action_obj)

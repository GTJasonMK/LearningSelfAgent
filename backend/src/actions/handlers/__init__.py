"""
actions.handlers

将 action_type 的执行逻辑按职责拆分，避免 executor.py 成为上帝类。

约定：
- 每个 handler 只负责一种 action_type 的校验与执行；
- handler 返回 (result, error_message)，由上层统一持久化到 task_steps.result/error。
"""


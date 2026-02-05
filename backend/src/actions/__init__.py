"""
Action 执行层：把 Agent/ReAct 产出的 action 转成真实的系统行为。

说明：
- Agent 负责“决定做什么”（plan + react）
- Actions 负责“真正去做”（写文件/跑命令/调用工具/写数据库）
"""


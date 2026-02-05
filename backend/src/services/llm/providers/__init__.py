"""
LLM Provider 抽象层（对应质量报告 P2#8）。

说明：
- 该目录用于隔离不同供应商的 SDK 依赖；
- services/llm/llm_client.py 只依赖 Provider 接口与注册表，避免强耦合 OpenAI SDK。
"""


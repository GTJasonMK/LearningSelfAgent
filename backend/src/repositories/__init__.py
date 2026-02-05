"""
数据库访问层（Repository）。

目的：
- 把 SQL 语句从 API/Agent 逻辑中抽离出来，降低耦合、减少重复；
- 便于单元测试：repo 函数只依赖 sqlite/get_connection，不依赖 FastAPI。
"""


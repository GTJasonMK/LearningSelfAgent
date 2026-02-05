"""
权限相关服务（读写/执行）。

说明：
- 权限存储在 permissions_store 单例表中；
- API 层可以用它做拦截；Agent/执行器也可以复用，避免反向依赖 api.utils。
"""


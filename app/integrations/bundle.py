"""Integration 客户端聚合体:三进程共享的进程级 client 集合。

新增业务 client:在此加一个字段,并在 bootstrap 的 integrations_lifecycle() 里
多 enter 一层对应的 *_client_ctx()。infra 层只按参数名转发本类型的实例(不 import)。
"""

from __future__ import annotations

from dataclasses import dataclass

from app.integrations.dummyjson.client import DummyJsonClient


@dataclass
class Integrations:
    dummyjson: DummyJsonClient  # demo:dummyjson —— 删除示例时连带删
    # 新增业务 client:这里加一个字段

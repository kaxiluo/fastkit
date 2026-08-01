"""Integration 客户端聚合体:进程级 client 注册表。

未列入进程 *_CLIENTS 清单的 client 不会被 enter,其 settings 也不会被读,
三进程互不强耦合对方不需要的配置。新增 client 见 docs/development-guide.md。
"""

from __future__ import annotations

from typing import TypeVar

T = TypeVar("T")


class ClientNotRegisteredError(RuntimeError):
    """业务侧 .get(SomeClient) 取一个未在本进程装配的 client。

    通常意味着:用到的进程对应的 *_CLIENTS 清单(在 bootstrap/container.py)里
    漏了对应的 *_client_ctx()。
    """

    def __init__(self, typ: type) -> None:
        super().__init__(
            f"{typ.__name__} not registered in this process; "
            f"add the corresponding *_client_ctx to the process's *_CLIENTS tuple "
            f"in bootstrap/container.py."
        )
        self.client_type = typ


class Integrations:
    """进程级 integration client 注册表。"""

    def __init__(self) -> None:
        self._registry: dict[type, object] = {}

    def register(self, client: object) -> None:
        """注册已构造好的 client,按 type 索引。

        仅供 integrations_lifecycle 内部调用;业务侧不直接调。
        多实例请用不同子类(见模块 docstring)。
        """
        self._registry[type(client)] = client

    def get(self, typ: type[T]) -> T:
        """按类型取 client;未注册则抛 ClientNotRegisteredError。"""
        try:
            return self._registry[typ]  # type: ignore[no-any-return]
        except KeyError:
            raise ClientNotRegisteredError(typ) from None

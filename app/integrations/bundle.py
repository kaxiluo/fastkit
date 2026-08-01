"""Integration 客户端聚合体:进程级 client 注册表(Service Locator 模式)。

进程装配显式声明:bootstrap/container.py 的 API_CLIENTS / WORKER_CLIENTS /
SCHEDULER_CLIENTS 三个清单各填一份 *_client_ctx(),lifespan body 统一为
integrations_lifecycle(*<NAME>_CLIENTS)。未列入清单的 client 不会被 enter,
其 settings 也不会被读 → 三进程互不强耦合对方不需要的 client 配置(例如
Scheduler 不需要某 client,该 client 的 settings 缺失也不会阻塞 Scheduler 启动)。

新增业务 client 的步骤:
1. 写一个 <name>_client_ctx() async context manager,返回 AsyncGenerator[<Name>Client]
2. 在 app/bootstrap/container.py 把 <name>_client_ctx 加进用到的进程对应的
   *_CLIENTS 清单(API_CLIENTS / WORKER_CLIENTS / SCHEDULER_CLIENTS)
3. API 在 _ContextProvider 加一个 @provide,返回 self._integrations.get(<Name>Client)
4. 业务侧 handler 经 `*, integrations: Integrations` 拿 bundle,内部 .get(<Name>Client) 取

多实例:**拆成不同的独立类**(如 `<A>Client` / `<B>Client`),
不用 (type, name) 二元 key —— 保持类型安全、业务侧调用的明确性、IDE 跳转可用。

Service Locator 在本场景不是 anti-pattern:bundle 由进程 lifespan 显式构造,通过
handler 参数注入(非全局单例);单测直接 mock 整个 Integrations 对象即可。
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

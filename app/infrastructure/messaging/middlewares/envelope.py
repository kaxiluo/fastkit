"""EnvelopeMiddleware:consumer 前解析 AMQP headers → dict,存进 message context。

publish 侧不需要注入 —— relay 层已经在调 broker.publish 时把 headers 参数塞满,
本中间件只服务 consume 路径。
"""

from __future__ import annotations

from faststream import BaseMiddleware


class EnvelopeMiddleware(BaseMiddleware):
    """占位中间件:实际 envelope 解析由 subscriber 层(_entry 内 msg.headers → parse_envelope)
    完成,不依赖本类做魔法传递。这样行为显式、可 grep。
    """

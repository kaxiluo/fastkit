import atexit
import logging
import queue
import sys
from logging.handlers import QueueHandler, QueueListener

import structlog

from app.config.settings import AppSettings

_listener: QueueListener | None = None


class _PassthroughQueueHandler(QueueHandler):
    """进程内队列无需 pickle,原样入队以保留 structlog 的 dict ``record.msg``。

    默认 ``QueueHandler.prepare`` 会调 ``format`` 并把结果回写 ``record.msg``,
    从而把 structlog 事件字典 str 化,导致后台 ``ProcessorFormatter`` 收到 str 崩溃。

    前提:仅适用进程内 ``queue.Queue``。原始 record 带 dict ``msg`` 与活的
    ``exc_info``,不保证可 pickle;若改用 ``multiprocessing.Queue`` 需换成入队前渲染成字符串。
    """

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        return record


def configure_logging(settings: AppSettings) -> None:
    """在进程启动最早时调用一次。此后 stdlib logging 和 structlog 都走同一条渲染链。

    使用 stdlib QueueHandler/QueueListener 把格式化 + stderr 写入挪到后台线程,
    避免高并发消费时事件循环线程被容器日志驱动限流阻塞。
    幂等:scheduler 路径可能调两次,重配时先 stop 旧 listener。
    """
    global _listener

    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=False),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if settings.log_format == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    formatter = structlog.stdlib.ProcessorFormatter(
        processor=renderer,
        foreign_pre_chain=shared_processors,
    )
    stream = logging.StreamHandler(sys.stderr)
    stream.setFormatter(formatter)

    if _listener is not None:
        _listener.stop()
        atexit.unregister(_listener.stop)
    log_queue: queue.Queue = queue.Queue(-1)
    _listener = QueueListener(log_queue, stream, respect_handler_level=True)
    _listener.start()
    atexit.register(_listener.stop)

    root = logging.getLogger()
    root.handlers = [_PassthroughQueueHandler(log_queue)]
    root.setLevel(level)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

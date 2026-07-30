from functools import lru_cache

from pydantic import AliasChoices, Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def _format_ttl_suffix(ttl_ms: int) -> str:
    if ttl_ms % 1000 == 0:
        return f"{ttl_ms // 1000}s"
    return f"{ttl_ms}ms"


class MessagingSettings(BaseSettings):
    """消息引擎(outbox 可靠投递 / inbox 幂等去重 / 消费侧重试与延迟队列)。

    DLQ / retry 资源名缺省由 app_name 派生,见 _derive_messaging_names。
    """

    model_config = SettingsConfigDict(
        env_prefix="MESSAGING_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
        populate_by_name=True,
    )

    # 仅用于派生 MQ 资源名;直接读 APP_NAME env var(绕过 MESSAGING_ 前缀)
    app_name: str = Field(
        default="fastkit",
        validation_alias=AliasChoices("APP_NAME", "app_name"),
    )
    # 兼容旧名 BROKER_URL 和新名 MESSAGING_BROKER_URL
    broker_url: SecretStr = Field(
        validation_alias=AliasChoices("BROKER_URL", "MESSAGING_BROKER_URL", "broker_url"),
    )

    # outbox 可靠投递
    outbox_poll_interval_seconds: float = 1.0  # outbox 表轮询间隔
    outbox_max_attempts: int = 100  # 单条消息最大投递尝试次数;超出转 DLQ
    outbox_backoff_max_seconds: int = 600  # 重试退避上限秒数
    outbox_batch_size: int = 100  # 单次轮询抓取的消息条数上限

    # DLQ 资源名(缺省由 app_name 派生)
    dlq_exchange: str | None = None  # 死信交换机名;缺省 {app_name}.dlx
    dlq_queue: str | None = None  # 死信队列名;缺省 {app_name}.dlq

    # 消费侧重试与延迟队列(retry 资源名缺省由 app_name 派生)
    retry_ttl_ms: int = 30000  # 延迟队列消息 TTL(毫秒);到期重回主队列重试
    retry_exchange: str | None = None  # 缺省 {app_name}.retry.ex
    retry_queue: str | None = None  # 缺省 {app_name}.retry.{ttl};ttl 派生见 _derive_messaging_names
    # 注:消费侧 max_attempts 不放全局 settings,每个 @task_consumer 用 RetryPolicy(...) 显式声明

    # 消费侧 handler 超时(秒);handler await 挂起超过此值 → 取消协程,当作失败走 retry/DLQ。
    # 仅防"await 外部 IO 挂起",不覆盖阻塞同步调用/CPU 死循环。per-consumer 可用
    # @task_consumer(timeout=...) 覆盖;timeout=None 关闭。
    consumer_timeout_seconds: float = 180.0

    # 关停宽限期;消费者 drain 超时则放弃未确认消息
    shutdown_grace_seconds: int = 30  # 秒

    @model_validator(mode="after")
    def _derive_messaging_names(self) -> "MessagingSettings":
        if self.dlq_exchange is None:
            self.dlq_exchange = f"{self.app_name}.dlx"
        if self.dlq_queue is None:
            self.dlq_queue = f"{self.app_name}.dlq"
        if self.retry_exchange is None:
            self.retry_exchange = f"{self.app_name}.retry.ex"
        if self.retry_queue is None:
            self.retry_queue = (
                f"{self.app_name}.retry.{_format_ttl_suffix(self.retry_ttl_ms)}"
            )
        return self


@lru_cache
def get_messaging_settings() -> MessagingSettings:
    return MessagingSettings()

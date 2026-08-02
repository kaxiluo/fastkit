import re
from functools import lru_cache
from typing import Literal

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_APP_NAME_RE = re.compile(r"^[a-z][a-z0-9-]{2,31}$")


class AppSettings(BaseSettings):
    """进程级全局配置。

    项目所有配置项按基础设施组件 / 集成模块拆分为独立的 BaseSettings 子类,
    各自带 env_prefix 隔离命名空间、lru_cache 进程级单例。本类只保留跨进程
    共享的全局字段;组件配置见对应模块:

    全局(本类,APP_ / LOG_ 前缀):
      APP_NAME                        应用名;同时用于 MessagingSettings 派生 MQ 资源名
      APP_ENV                         dev / test / prod
      LOG_LEVEL                       stdlib logging level
      LOG_FORMAT                      json / console
      READY_BROKER_PING_TIMEOUT       /ready 探活 broker 超时秒数

    DatabaseSettings  (app.infrastructure.database.settings,   DATABASE_ 前缀):
      DATABASE_URL                    数据库连接串
      DATABASE_POOL_SIZE              池内常驻连接数
      DATABASE_MAX_OVERFLOW           超出 pool_size 后允许的临时扩张
      DATABASE_POOL_RECYCLE           单连接最大存活秒数,绕开 DB 侧 idle 超时
      DATABASE_POOL_TIMEOUT           池耗尽时获取连接的等待秒数

    RedisSettings     (app.infrastructure.redis.settings,      REDIS_ 前缀):
      REDIS_URL                       Redis 连接串
      REDIS_MAX_CONNECTIONS           连接池上限
      REDIS_HEALTH_CHECK_INTERVAL     自动健康检查间隔秒数

    MessagingSettings (app.infrastructure.messaging.settings,  MESSAGING_ 前缀):
      BROKER_URL / MESSAGING_BROKER_URL   RabbitMQ 连接串(前者兼容旧名)
      MESSAGING_APP_NAME                  仅派生 MQ 资源名;缺省读 APP_NAME
      MESSAGING_OUTBOX_POLL_INTERVAL_SECONDS   outbox 表轮询间隔
      MESSAGING_OUTBOX_MAX_ATTEMPTS           单条消息最大投递尝试次数;超出标 status='dead'(不转 DLQ)
      MESSAGING_OUTBOX_BACKOFF_MAX_SECONDS    重试退避上限秒数
      MESSAGING_OUTBOX_BATCH_SIZE             单次轮询抓取的消息条数上限
      MESSAGING_DLQ_EXCHANGE                  死信交换机名;缺省 {app_name}.dlx
      MESSAGING_DLQ_QUEUE                     死信队列名;缺省 {app_name}.dlq
      MESSAGING_RETRY_TTL_MS                  延迟队列消息 TTL(毫秒);到期重回主队列重试
      MESSAGING_RETRY_EXCHANGE                缺省 {app_name}.retry.ex
      MESSAGING_RETRY_QUEUE                   缺省 {app_name}.retry.{ttl}
      MESSAGING_SHUTDOWN_GRACE_SECONDS        关停宽限期;消费者 drain 超时则放弃未确认消息
      MESSAGING_CONSUMER_TIMEOUT_SECONDS      消费侧 handler 超时秒数;超时取消协程走 retry/DLQ

      注:消费侧 max_attempts 不在全局 settings,每个 @task_consumer 用 RetryPolicy(...) 显式声明

    RateLimitSettings (app.infrastructure.ratelimit.settings,  RATELIMIT_ 前缀):
      RATELIMIT_POLL_INTERVAL_SECONDS  桶空时令牌补充的轮询间隔秒数
      RATELIMIT_MAX_WAIT_SECONDS       桶空时调用方最大等待秒数;超时抛限流错误

    DummyJsonSettings (app.integrations.dummyjson.settings,    DUMMYJSON_ 前缀):
      DUMMYJSON_BASE_URL              DummyJson API 根地址
      DUMMYJSON_TIMEOUT               HTTP 超时秒数

    .env.example 只暴露常用项;全部默认值见各 settings 模块。
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    app_name: str = "fastkit"
    app_env: Literal["dev", "test", "prod"] = "dev"

    log_level: str = "INFO"
    log_format: Literal["json", "console"] = "json"

    # /ready 中 RabbitMQ 探活超时秒数;超时即 /ready 返回不就绪
    ready_broker_ping_timeout: float = 5.0

    @field_validator("app_name")
    @classmethod
    def _validate_app_name(cls, v: str) -> str:
        if not _APP_NAME_RE.match(v):
            raise ValueError(
                f"app_name {v!r} invalid; must match ^[a-z][a-z0-9-]{{2,31}}$ "
                "(lowercase ASCII, kebab-case, 3-32 chars)"
            )
        return v


@lru_cache
def get_app_settings() -> AppSettings:
    return AppSettings()

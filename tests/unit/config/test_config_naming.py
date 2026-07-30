"""MessagingSettings 命名空间校验与 MQ 资源名派生;AppSettings app_name 校验。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.config.settings import AppSettings
from app.infrastructure.messaging.settings import MessagingSettings


def _make(**over) -> MessagingSettings:
    base = dict(
        broker_url="amqp://guest:guest@localhost/",
        app_name="fastkit",
    )
    base.update(over)
    return MessagingSettings(**base)


def _make_app(**over) -> AppSettings:
    base = dict(app_name="fastkit")
    base.update(over)
    return AppSettings(**base)


def test_messaging_names_derive_from_app_name():
    s = _make(app_name="ebay-fastkit")
    assert s.dlq_exchange == "ebay-fastkit.dlx"
    assert s.dlq_queue == "ebay-fastkit.dlq"
    assert s.retry_exchange == "ebay-fastkit.retry.ex"
    assert s.retry_queue == "ebay-fastkit.retry.30s"


def test_default_app_name_derives():
    s = _make()
    assert s.dlq_exchange == "fastkit.dlx"
    assert s.retry_queue == "fastkit.retry.30s"


def test_explicit_env_value_overrides_derivation():
    s = _make(app_name="fastkit", dlq_exchange="legacy.dlx")
    assert s.dlq_exchange == "legacy.dlx"
    assert s.dlq_queue == "fastkit.dlq"


@pytest.mark.parametrize(
    "ttl_ms, expected_suffix",
    [
        (30000, "30s"),
        (1000, "1s"),
        (60000, "60s"),
        (500, "500ms"),
        (1500, "1500ms"),
    ],
)
def test_retry_queue_name_derives_from_ttl(ttl_ms, expected_suffix):
    s = _make(app_name="fastkit", retry_ttl_ms=ttl_ms)
    assert s.retry_queue == f"fastkit.retry.{expected_suffix}"


@pytest.mark.parametrize(
    "bad",
    ["Ebay-Fastkit", "ab", "ebay_fastkit", "ebay fastkit", "ebay.price", "1abc", "a" * 33],
)
def test_invalid_app_name_rejected(bad):
    with pytest.raises(ValidationError):
        _make_app(app_name=bad)


def test_valid_app_name_accepted():
    for good in ["fastkit", "ebay-fastkit", "price-fastkit", "order-service"]:
        assert _make_app(app_name=good).app_name == good

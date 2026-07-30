from __future__ import annotations

from app.infrastructure.messaging.settings import MessagingSettings

_BROKER = "amqp://guest:guest@localhost/"


def test_consumer_timeout_defaults_to_180():
    s = MessagingSettings(broker_url=_BROKER, app_name="fastkit")
    assert s.consumer_timeout_seconds == 180.0


def test_consumer_timeout_overridable_via_env(monkeypatch):
    monkeypatch.setenv("MESSAGING_CONSUMER_TIMEOUT_SECONDS", "42.5")
    s = MessagingSettings(broker_url=_BROKER, app_name="fastkit")
    assert s.consumer_timeout_seconds == 42.5

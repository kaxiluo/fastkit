from __future__ import annotations

from app.infrastructure.messaging.settings import MessagingSettings


def test_concurrency_lease_settings_removed():
    s = MessagingSettings(broker_url="amqp://guest:guest@localhost/", app_name="fastkit")
    assert not hasattr(s, "messaging_concurrency_lease_seconds")
    assert not hasattr(s, "messaging_concurrency_poll_interval_seconds")

from faststream.rabbit import RabbitBroker

from app.infrastructure.messaging.middlewares.envelope import EnvelopeMiddleware
from app.infrastructure.messaging.settings import MessagingSettings


def build_broker(settings: MessagingSettings) -> RabbitBroker:
    return RabbitBroker(
        settings.broker_url.get_secret_value(),
        middlewares=[EnvelopeMiddleware],
        graceful_timeout=settings.shutdown_grace_seconds,
    )

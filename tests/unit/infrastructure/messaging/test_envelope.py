from datetime import UTC, datetime

from app.infrastructure.messaging.envelope import fill_envelope, parse_envelope


def test_fill_envelope_produces_all_required_fields():
    env = fill_envelope(
        routing_key="canary.ping",
        schema_version=1,
        producer="fastkit-test",
    )
    assert env["routing_key"] == "canary.ping"
    assert env["message_version"] == 1
    assert env["producer"] == "fastkit-test"
    assert env["correlation_id"] is None
    assert env["causation_id"] is None
    # message_id 是 UUID4 字符串
    assert len(env["message_id"]) == 36 and env["message_id"].count("-") == 4
    # published_at 是可解析的 ISO8601
    datetime.fromisoformat(env["published_at"])


def test_fill_envelope_propagates_correlation_and_causation():
    env = fill_envelope(
        routing_key="canary.ping",
        schema_version=2,
        producer="fastkit-test",
        correlation_id="corr-123",
        causation_id="cause-456",
    )
    assert env["correlation_id"] == "corr-123"
    assert env["causation_id"] == "cause-456"


def test_fill_envelope_generates_unique_message_ids():
    a = fill_envelope("t", 1, "p")
    b = fill_envelope("t", 1, "p")
    assert a["message_id"] != b["message_id"]


def test_parse_envelope_tolerates_missing_fields():
    partial = {"message_id": "abc", "routing_key": "t"}
    env = parse_envelope(partial)
    assert env["message_id"] == "abc"
    assert env["routing_key"] == "t"
    assert env["correlation_id"] is None
    assert env["message_version"] == 1  # 默认版本 1


def test_parse_envelope_preserves_all_known_fields():
    now = datetime.now(UTC).isoformat()
    full = {
        "message_id": "id-1",
        "message_version": 3,
        "correlation_id": "c-1",
        "causation_id": "ca-1",
        "producer": "svc",
        "published_at": now,
        "routing_key": "x.y",
        "attempts": 1,
        "failure": None,
    }
    env = parse_envelope(full)
    assert env == full


def test_fill_envelope_carries_attempts_and_failure():
    from app.infrastructure.messaging.envelope import FailureInfo

    failure = FailureInfo(type="ValueError", message="boom", at="2026-07-27T10:00:00+00:00")
    env = fill_envelope(
        routing_key="test.q",
        schema_version=1,
        producer="fastkit",
        attempts=3,
        failure=failure,
    )
    assert env["attempts"] == 3
    assert env["failure"] == failure


def test_parse_envelope_defaults_attempts_and_failure_for_old_messages():
    # 旧消息 headers 里没有 attempts 和 failure
    old_headers = {
        "message_id": "abc",
        "message_version": 1,
        "producer": "fastkit",
        "published_at": "2026-07-27T10:00:00+00:00",
        "routing_key": "test.q",
    }
    env = parse_envelope(old_headers)
    assert env["attempts"] == 1
    assert env["failure"] is None


def test_parse_envelope_roundtrip_with_failure():
    headers_with_failure = {
        "message_id": "abc",
        "message_version": 1,
        "producer": "fastkit",
        "published_at": "2026-07-27T10:00:00+00:00",
        "routing_key": "test.q",
        "attempts": 2,
        "failure": {
            "type": "httpx.ConnectError",
            "message": "connection refused",
            "at": "2026-07-27T10:00:30+00:00",
        },
    }
    env = parse_envelope(headers_with_failure)
    assert env["attempts"] == 2
    assert env["failure"]["type"] == "httpx.ConnectError"

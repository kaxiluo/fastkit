"""Integrations registry 行为:按 type 注册 / 按 type 取 / 未注册明确报错。"""

from __future__ import annotations

import pytest

from app.integrations.bundle import ClientNotRegisteredError, Integrations


def test_register_and_get_by_type() -> None:
    class FakeClient:
        pass

    bundle = Integrations()
    client = FakeClient()

    bundle.register(client)

    assert bundle.get(FakeClient) is client


def test_get_unregistered_raises_clear_error() -> None:
    class FakeClient:
        pass

    bundle = Integrations()

    with pytest.raises(ClientNotRegisteredError) as exc_info:
        bundle.get(FakeClient)

    assert "FakeClient" in str(exc_info.value)
    assert "not registered" in str(exc_info.value)
    assert exc_info.value.client_type is FakeClient


def test_register_same_type_replaces() -> None:
    """多实例应拆类(见 bundle.py docstring);同 type 二次注册明确语义:后者覆盖前者。"""

    class FakeClient:
        pass

    bundle = Integrations()
    first = FakeClient()
    second = FakeClient()

    bundle.register(first)
    bundle.register(second)

    assert bundle.get(FakeClient) is second


def test_distinct_types_coexist() -> None:
    """验证 OuterKongClient / InnerKongClient 拆类后能共存(多实例解法)。"""

    class OuterKongClient:
        pass

    class InnerKongClient:
        pass

    bundle = Integrations()
    outer = OuterKongClient()
    inner = InnerKongClient()

    bundle.register(outer)
    bundle.register(inner)

    assert bundle.get(OuterKongClient) is outer
    assert bundle.get(InnerKongClient) is inner

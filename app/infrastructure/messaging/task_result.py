"""Consumer handler 结果二态:FINISHED / ABORT。"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


@dataclass(frozen=True)
class TaskResult:
    kind: Literal["FINISHED", "ABORT"]
    reason: str | None = None

    # noqa: N802 — 类方法用 UPPER 是"命名构造器"语义,与 kind 字面量同形
    @classmethod
    def FINISHED(cls) -> TaskResult:  # noqa: N802
        return cls(kind="FINISHED")

    @classmethod
    def ABORT(cls, reason: str) -> TaskResult:  # noqa: N802
        return cls(kind="ABORT", reason=reason)

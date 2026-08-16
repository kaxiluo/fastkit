from __future__ import annotations

import inspect
from collections.abc import Callable
from dataclasses import dataclass, field

from apscheduler.triggers.cron import CronTrigger


@dataclass
class _CronJobSpec:
    func: Callable
    trigger: CronTrigger
    job_id: str
    misfire_grace_time: int
    max_instances: int
    accepts_session_factory: bool = field(init=False)
    accepts_integrations: bool = field(init=False)
    accepts_databases: bool = field(init=False)
    accepts_redis: bool = field(init=False)

    def __post_init__(self) -> None:
        sig = inspect.signature(self.func)
        self.accepts_session_factory = "session_factory" in sig.parameters
        self.accepts_integrations = "integrations" in sig.parameters
        self.accepts_databases = "databases" in sig.parameters
        self.accepts_redis = "redis" in sig.parameters


_CRON_JOBS: list[_CronJobSpec] = []


def get_registered_cron_jobs() -> list[_CronJobSpec]:
    return list(_CRON_JOBS)


def clear_registered_cron_jobs() -> None:
    _CRON_JOBS.clear()


def cron_job(
    trigger: CronTrigger,
    *,
    job_id: str,
    misfire_grace_time: int = 3600,
    max_instances: int = 1,
) -> Callable[[Callable], Callable]:
    def decorator(func: Callable) -> Callable:
        spec = _CronJobSpec(
            func=func,
            trigger=trigger,
            job_id=job_id,
            misfire_grace_time=misfire_grace_time,
            max_instances=max_instances,
        )
        _CRON_JOBS.append(spec)
        return func

    return decorator

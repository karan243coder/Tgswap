"""Linux cgroup resource inspection for safe FaceFusion admission control."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class RuntimeResources:
    memory_limit_bytes: int | None
    memory_current_bytes: int | None
    cpu_quota_cores: float | None

    @property
    def memory_limit_mib(self) -> float | None:
        return (
            self.memory_limit_bytes / (1024 * 1024) if self.memory_limit_bytes else None
        )

    @property
    def memory_current_mib(self) -> float | None:
        return (
            self.memory_current_bytes / (1024 * 1024)
            if self.memory_current_bytes
            else None
        )


class RenderResourceError(RuntimeError):
    """Raised before model load when a container cannot safely render a video."""


def inspect_runtime_resources() -> RuntimeResources:
    """Read cgroup v2 first, then v1. Missing files simply mean unknown limits."""
    memory_limit = _read_memory_limit(
        Path("/sys/fs/cgroup/memory.max"),
        Path("/sys/fs/cgroup/memory/memory.limit_in_bytes"),
    )
    memory_current = _read_int(
        Path("/sys/fs/cgroup/memory.current"),
        Path("/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    )
    cpu_quota = _read_cpu_quota()
    return RuntimeResources(memory_limit, memory_current, cpu_quota)


def require_render_memory(resources: RuntimeResources, minimum_memory_mb: int) -> None:
    """Reject known-small containers before downloading/loading large ONNX models."""
    if minimum_memory_mb <= 0 or resources.memory_limit_bytes is None:
        return
    minimum_bytes = minimum_memory_mb * 1024 * 1024
    if resources.memory_limit_bytes >= minimum_bytes:
        return

    limit = _format_mib(resources.memory_limit_bytes)
    current = (
        f"; currently using {_format_mib(resources.memory_current_bytes)}"
        if resources.memory_current_bytes is not None
        else ""
    )
    cpu = (
        f"; CPU quota {resources.cpu_quota_cores:.2f} vCPU"
        if resources.cpu_quota_cores is not None
        else ""
    )
    raise RenderResourceError(
        "Renderer blocked before model load: this instance has "
        f"{limit} RAM{current}{cpu}. The active FaceFusion video profile requires at least "
        f"{minimum_memory_mb} MiB. Upgrade the Koyeb instance or use a separate render worker."
    )


def _read_memory_limit(*paths: Path) -> int | None:
    value = _read_int(*paths)
    # cgroup v1 commonly reports an effectively infinite host value.
    if value is not None and value > 1 << 50:
        return None
    return value


def _read_int(*paths: Path) -> int | None:
    for path in paths:
        try:
            raw = path.read_text(encoding="utf-8").strip()
        except OSError:
            continue
        if raw == "max" or not raw:
            continue
        try:
            return int(raw)
        except ValueError:
            continue
    return None


def _read_cpu_quota() -> float | None:
    try:
        raw = Path("/sys/fs/cgroup/cpu.max").read_text(encoding="utf-8").strip()
        quota, period = raw.split(maxsplit=1)
        if quota != "max":
            return int(quota) / int(period)
    except (OSError, ValueError, ZeroDivisionError):
        pass

    quota = _read_int(Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us"))
    period = _read_int(Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us"))
    if quota is not None and period and quota > 0:
        return quota / period
    return None


def _format_mib(value: int | None) -> str:
    if value is None:
        return "unknown"
    return f"{value / (1024 * 1024):.0f} MiB"

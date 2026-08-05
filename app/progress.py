"""A rate-limited, English-only advanced Telegram progress display.

Telegram messages cannot be edited for every single frame without hitting flood
limits. This display records every incoming frame/byte measurement but edits the
visible message only at a controlled interval, showing exact counters, speed and
an ETA calculated from those measurements.
"""

from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .telegram_mtproto import MTProtoTelegramClient


TransferCallback = Callable[[int, int], Awaitable[None]]


@dataclass(slots=True)
class _Meter:
    current: int = 0
    total: int = 0
    unit: str = ""
    started_at: float = field(default_factory=time.monotonic)
    updated_at: float = field(default_factory=time.monotonic)

    def update(self, current: int, total: int, unit: str) -> None:
        now = time.monotonic()
        # A new stage has a different unit or starts its counter again. Reset the
        # rate window so displayed speed/ETA reflects this stage, not the queue.
        if unit != self.unit or current < self.current:
            self.started_at = now
        self.current = max(0, current)
        self.total = max(0, total)
        self.unit = unit
        self.updated_at = now

    @property
    def fraction(self) -> float | None:
        if self.total <= 0:
            return None
        return min(1.0, max(0.0, self.current / self.total))

    @property
    def speed(self) -> float | None:
        elapsed = time.monotonic() - self.started_at
        if elapsed <= 0 or self.current <= 0:
            return None
        return self.current / elapsed

    @property
    def eta_seconds(self) -> float | None:
        speed = self.speed
        if speed is None or self.total <= self.current:
            return 0.0 if self.total and self.current >= self.total else None
        return (self.total - self.current) / speed


class ProgressDisplay:
    """One editable Telegram message carrying rich job telemetry."""

    def __init__(
        self,
        telegram: MTProtoTelegramClient,
        chat_id: int,
        job_id: str,
        message_id: int | None,
        *,
        edit_interval_seconds: float,
    ) -> None:
        self.telegram = telegram
        self.chat_id = chat_id
        self.job_id = job_id
        self.message_id = message_id
        self.edit_interval_seconds = edit_interval_seconds
        self.created_at = time.monotonic()
        self.stage = "Waiting"
        self.detail = "Waiting for the worker."
        self.overall: float | None = 0.0
        self.meter = _Meter()
        self.video_duration_seconds: float | None = None
        self.quality_line = "Frame-by-frame processing"
        self.notes: list[str] = []
        self._last_edit_at = 0.0
        self._last_text = ""
        self._lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        telegram: MTProtoTelegramClient,
        chat_id: int,
        job_id: str,
        *,
        edit_interval_seconds: float,
    ) -> ProgressDisplay:
        display = cls(
            telegram,
            chat_id,
            job_id,
            None,
            edit_interval_seconds=edit_interval_seconds,
        )
        sent = await telegram.send_message(chat_id, display.render())
        display.message_id = telegram.message_id(sent)
        display._last_text = display.render()
        display._last_edit_at = time.monotonic()
        return display

    @classmethod
    def attach(
        cls,
        telegram: MTProtoTelegramClient,
        chat_id: int,
        job_id: str,
        message_id: int | None,
        *,
        edit_interval_seconds: float,
    ) -> ProgressDisplay:
        return cls(
            telegram,
            chat_id,
            job_id,
            message_id,
            edit_interval_seconds=edit_interval_seconds,
        )

    def set_video_context(self, *, duration_seconds: float, quality_line: str) -> None:
        self.video_duration_seconds = duration_seconds if duration_seconds > 0 else None
        self.quality_line = quality_line

    async def update(
        self,
        *,
        stage: str,
        detail: str,
        overall: float | None = None,
        current: int | None = None,
        total: int | None = None,
        unit: str | None = None,
        note: str | None = None,
        force: bool = False,
    ) -> None:
        async with self._lock:
            self.stage = stage
            self.detail = detail
            if overall is not None:
                self.overall = min(100.0, max(0.0, overall))
            if current is not None and total is not None:
                self.meter.update(current, total, unit or self.meter.unit)
            if note and (not self.notes or self.notes[-1] != note):
                self.notes = (self.notes + [note])[-3:]
            await self._render_if_due(force=force)

    def transfer_callback(
        self,
        *,
        stage: str,
        detail: str,
        overall_start: float,
        overall_span: float,
        note: str | None = None,
    ) -> TransferCallback:
        async def callback(current: int, total: int) -> None:
            fraction = current / total if total > 0 else 0.0
            await self.update(
                stage=stage,
                detail=detail,
                overall=overall_start + overall_span * fraction,
                current=current,
                total=total,
                unit="bytes",
                note=note,
            )

        return callback

    async def frame_update(
        self,
        *,
        current: int,
        total: int,
        fps: float | None,
        detail: str = "Swapping every frame with FaceFusion.",
    ) -> None:
        fraction = current / total if total > 0 else 0.0
        note = f"Inference speed: {fps:.2f} frames/s" if fps and fps > 0 else None
        await self.update(
            stage="Frame-by-frame face swap",
            detail=detail,
            overall=25.0 + 60.0 * fraction,
            current=current,
            total=total,
            unit="frames",
            note=note,
        )

    async def ffmpeg_update(
        self,
        *,
        stage: str,
        detail: str,
        seconds_done: float,
        seconds_total: float,
        overall_start: float,
        overall_span: float,
    ) -> None:
        fraction = seconds_done / seconds_total if seconds_total > 0 else 0.0
        await self.update(
            stage=stage,
            detail=detail,
            overall=overall_start + overall_span * min(1.0, max(0.0, fraction)),
            current=max(0, int(seconds_done * 1000)),
            total=max(0, int(seconds_total * 1000)),
            unit="milliseconds",
        )

    async def complete(self, detail: str = "Completed successfully.") -> None:
        await self.update(stage="Completed", detail=detail, overall=100.0, force=True)

    async def fail(self, detail: str) -> None:
        await self.update(stage="Stopped", detail=detail, force=True)

    async def _render_if_due(self, *, force: bool) -> None:
        if self.message_id is None:
            return
        now = time.monotonic()
        text = self.render()
        if not force and (
            text == self._last_text
            or now - self._last_edit_at < self.edit_interval_seconds
        ):
            return
        try:
            queue_edit = getattr(self.telegram, "queue_progress_edit", None)
            if callable(queue_edit):
                # The real transport coalesces this in a separate task. Never
                # await a Telegram network RPC from the FaceFusion log reader.
                queue_edit(self.chat_id, self.message_id, text)
            else:
                # Small fake transports used by unit tests keep the simple async
                # path, while production always uses the coalescing queue above.
                await self.telegram.edit_message(
                    self.chat_id,
                    self.message_id,
                    text,
                    noncritical=True,
                )
        except Exception:  # noqa: BLE001 - progress must never stop inference
            # Progress must never stop inference. The transport handles its own
            # token-safe diagnostics; Telegram may also reject a no-op edit.
            self._last_edit_at = now
            return
        self._last_edit_at = now
        self._last_text = text

    def render(self) -> str:
        elapsed = max(0.0, time.monotonic() - self.created_at)
        lines = [
            "FACE SWAP JOB",
            f"Job: {self.job_id[:12]}",
            f"Stage: {self.stage}",
            f"Activity: {self.detail}",
            "",
        ]
        if self.overall is not None:
            lines.append(f"Overall: {self._bar(self.overall)} {self.overall:5.1f}%")
        fraction = self.meter.fraction
        if fraction is not None:
            if self.meter.unit == "bytes":
                lines.append(
                    "Transfer: "
                    f"{self._bytes(self.meter.current)} / {self._bytes(self.meter.total)} "
                    f"({fraction * 100:5.1f}%)"
                )
            elif self.meter.unit == "frames":
                lines.append(
                    f"Frames: {self.meter.current:,} / {self.meter.total:,} ({fraction * 100:5.1f}%)"
                )
                if self.video_duration_seconds and self.meter.total:
                    timeline = self.video_duration_seconds * fraction
                    lines.append(
                        f"Timeline: {self._duration(timeline)} / {self._duration(self.video_duration_seconds)}"
                    )
            elif self.meter.unit == "milliseconds":
                lines.append(
                    f"Media time: {self._duration(self.meter.current / 1000)} / "
                    f"{self._duration(self.meter.total / 1000)}"
                )
        speed = self.meter.speed
        if speed:
            if self.meter.unit == "bytes":
                lines.append(f"Transfer speed: {self._bytes(speed)}/s")
            elif self.meter.unit == "frames":
                lines.append(f"Processing speed: {speed:.2f} frames/s")
        eta = self.meter.eta_seconds
        lines.append(
            f"Estimated stage remaining: {self._duration(eta) if eta is not None else 'Calculating…'}"
        )
        lines.append(f"Elapsed: {self._duration(elapsed)}")
        lines.append(f"Quality: {self.quality_line}")
        if self.notes:
            lines.append("Telemetry: " + " • ".join(self.notes[-2:]))
        lines.append("Use My Status or Cancel Render at any time.")
        return "\n".join(lines)

    @staticmethod
    def _bar(percent: float) -> str:
        cells = 12
        full = min(cells, max(0, math.floor(percent / 100 * cells)))
        return "[" + "█" * full + "░" * (cells - full) + "]"

    @staticmethod
    def _bytes(value: float) -> str:
        value = float(value)
        units = ("B", "KiB", "MiB", "GiB", "TiB")
        index = 0
        while value >= 1024 and index < len(units) - 1:
            value /= 1024
            index += 1
        return f"{value:.2f} {units[index]}"

    @staticmethod
    def _duration(value: float | None) -> str:
        if value is None or not math.isfinite(value):
            return "Calculating…"
        seconds = max(0, round(value))
        hours, remainder = divmod(seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

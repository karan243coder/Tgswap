"""Rate-safe, detailed, modern Telegram progress display.

The display deliberately separates *truthful stage telemetry* from the weighted
end-to-end estimate. Every value shown is sourced from MTProto callbacks,
FFmpeg progress, FaceFusion tqdm output, or inspected media metadata. Progress
updates are coalesced by the transport so rendering never waits on Telegram UI.
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
        # A counter reset or unit change means a new stage. Do not let an old
        # transfer's elapsed time poison current-stage speed/ETA.
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
    """One editable Telegram message carrying advanced job telemetry."""

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
        self.video_width = 0
        self.video_height = 0
        self.video_fps = 0.0
        self.video_frame_count = 0
        self.video_has_audio: bool | None = None
        self.input_bytes = 0
        self.quality_line = "Frame-by-frame processing"

        self.asset_name: str | None = None
        self.asset_percent: float | None = None
        self.model_cache_bytes: int | None = None
        self.reported_frame_fps: float | None = None
        self.memory_limit_bytes: int | None = None
        self.memory_current_bytes: int | None = None
        self.cpu_quota_cores: float | None = None
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

    def set_video_context(
        self,
        *,
        duration_seconds: float,
        quality_line: str,
        width: int = 0,
        height: int = 0,
        fps: float = 0.0,
        frame_count: int = 0,
        has_audio: bool | None = None,
        input_bytes: int = 0,
    ) -> None:
        self.video_duration_seconds = duration_seconds if duration_seconds > 0 else None
        self.video_width = max(0, width)
        self.video_height = max(0, height)
        self.video_fps = max(0.0, fps)
        self.video_frame_count = max(0, frame_count)
        self.video_has_audio = has_audio
        self.input_bytes = max(0, input_bytes)
        self.quality_line = quality_line

    def set_runtime_context(
        self,
        *,
        memory_limit_bytes: int | None,
        memory_current_bytes: int | None,
        cpu_quota_cores: float | None,
    ) -> None:
        self.memory_limit_bytes = memory_limit_bytes
        self.memory_current_bytes = memory_current_bytes
        self.cpu_quota_cores = cpu_quota_cores

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

    async def asset_download_update(
        self,
        *,
        asset_name: str | None,
        current_bytes: int | None,
        total_bytes: int | None,
        percent: float | None = None,
        model_cache_bytes: int | None = None,
    ) -> None:
        """Render exact current-model download telemetry when FaceFusion exposes it."""
        async with self._lock:
            self.stage = "Downloading model assets"
            self.detail = (
                "FaceFusion is obtaining model assets for the selected quality profile."
            )
            if asset_name:
                self.asset_name = asset_name
            if percent is not None:
                self.asset_percent = min(100.0, max(0.0, percent))
            if model_cache_bytes is not None:
                self.model_cache_bytes = max(0, model_cache_bytes)
            if (
                current_bytes is not None
                and total_bytes is not None
                and total_bytes > 0
            ):
                self.meter.update(current_bytes, total_bytes, "bytes")
            if self.overall is None or self.overall < 25:
                self.overall = 25.0
            await self._render_if_due(force=False)

    async def frame_update(
        self,
        *,
        current: int,
        total: int,
        fps: float | None,
        detail: str = "Swapping every frame with FaceFusion.",
    ) -> None:
        fraction = current / total if total > 0 else 0.0
        if fps and fps > 0:
            self.reported_frame_fps = fps
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
                await self.telegram.edit_message(
                    self.chat_id,
                    self.message_id,
                    text,
                    noncritical=True,
                )
        except Exception:  # noqa: BLE001 - progress must never stop inference
            self._last_edit_at = now
            return
        self._last_edit_at = now
        self._last_text = text

    def render(self) -> str:
        elapsed = max(0.0, time.monotonic() - self.created_at)
        stage_fraction = self.meter.fraction
        stage_eta = self.meter.eta_seconds
        lines = [
            "╭─ FACE SWAP ENGINE ──────────────────",
            f"│ Job       {self.job_id[:12]}",
            f"│ State     {self.stage}",
            f"│ Activity  {self._trim(self.detail, 72)}",
            "├─ PIPELINE ───────────────────────────",
            f"│ {self._pipeline_line()}",
            "├─ PROGRESS ───────────────────────────",
        ]
        if self.overall is not None:
            lines.append(
                f"│ Overall   {self._bar(self.overall, cells=18)} {self.overall:5.1f}%"
            )
        if stage_fraction is not None:
            lines.append(
                f"│ Stage     {self._bar(stage_fraction * 100, cells=18)} {stage_fraction * 100:5.1f}%"
            )
        else:
            lines.append("│ Stage     Live telemetry pending")

        lines.extend(self._meter_lines(stage_fraction))
        lines.extend(self._video_lines())

        lines.append("├─ RUNTIME ────────────────────────────")
        if self.memory_limit_bytes is not None:
            current = (
                f" · {self._bytes(self.memory_current_bytes)} used"
                if self.memory_current_bytes is not None
                else ""
            )
            lines.append(
                f"│ Memory    {self._bytes(self.memory_limit_bytes)} limit{current}"
            )
        if self.cpu_quota_cores is not None:
            lines.append(f"│ CPU quota {self.cpu_quota_cores:.2f} vCPU")
        if stage_eta is not None:
            lines.append(f"│ Stage ETA {self._duration(stage_eta)}")
        else:
            lines.append("│ Stage ETA Calculating…")
        lines.append(f"│ Elapsed   {self._duration(elapsed)}")
        lines.append(f"│ Quality   {self._trim(self.quality_line, 66)}")
        if self.notes:
            lines.append(f"│ Telemetry {self._trim(' • '.join(self.notes[-2:]), 66)}")
        lines.append("╰─ Use My Status or Cancel Render")
        return "\n".join(lines)

    def _meter_lines(self, stage_fraction: float | None) -> list[str]:
        lines: list[str] = []
        if self.meter.unit == "bytes" and stage_fraction is not None:
            label = self._byte_label()
            lines.append(
                f"│ {label:<9}{self._bytes(self.meter.current)} / {self._bytes(self.meter.total)} "
                f"({stage_fraction * 100:5.1f}%)"
            )
            speed = self.meter.speed
            if speed:
                lines.append(f"│ Speed     {self._bytes(speed)}/s")
            if self._is_asset_stage() and self.asset_name:
                lines.append(f"│ Asset     {self._trim(self.asset_name, 66)}")
            if self._is_asset_stage() and self.model_cache_bytes is not None:
                lines.append(
                    f"│ Cache     {self._bytes(self.model_cache_bytes)} available locally"
                )
        elif self.meter.unit == "frames" and stage_fraction is not None:
            lines.append(
                f"│ Frames    {self.meter.current:,} / {self.meter.total:,} ({stage_fraction * 100:5.1f}%)"
            )
            if self.video_duration_seconds and self.meter.total:
                timeline = self.video_duration_seconds * stage_fraction
                lines.append(
                    f"│ Timeline  {self._duration(timeline)} / {self._duration(self.video_duration_seconds)}"
                )
            speed = self.reported_frame_fps or self.meter.speed
            if speed:
                lines.append(f"│ Inference {speed:.2f} frames/s")
        elif self.meter.unit == "milliseconds" and stage_fraction is not None:
            lines.append(
                f"│ Media     {self._duration(self.meter.current / 1000)} / "
                f"{self._duration(self.meter.total / 1000)}"
            )
        elif self._is_asset_stage() and self.asset_name:
            percent = (
                f" ({self.asset_percent:.1f}%)"
                if self.asset_percent is not None
                else ""
            )
            lines.append(f"│ Asset     {self._trim(self.asset_name, 66)}{percent}")
        return lines

    def _video_lines(self) -> list[str]:
        if not self.video_width or not self.video_height:
            return []
        audio = "audio" if self.video_has_audio else "no audio"
        fps = f"{self.video_fps:.3g} fps" if self.video_fps else "unknown fps"
        frames = (
            f" · {self.video_frame_count:,} frames" if self.video_frame_count else ""
        )
        lines = [
            "├─ SOURCE VIDEO ───────────────────────",
            f"│ Video     {self.video_width}×{self.video_height} · {fps}{frames} · {audio}",
        ]
        if self.input_bytes:
            lines.append(f"│ Input     {self._bytes(self.input_bytes)}")
        return lines

    def _pipeline_line(self) -> str:
        phases = ("Input", "Assets", "Frames", "Finalize", "Deliver")
        active = self._pipeline_index()
        tokens = []
        for index, phase in enumerate(phases):
            if self.stage == "Completed" or index < active:
                marker = "✓"
            elif index == active:
                marker = "●"
            else:
                marker = "○"
            tokens.append(f"{marker} {phase}")
        return " › ".join(tokens)

    def _pipeline_index(self) -> int:
        stage = self.stage.lower()
        if (
            "inspecting" in stage
            or "preparing job" in stage
            or "queued" in stage
            or ("download" in stage and ("source" in stage or "target" in stage))
        ):
            return 0
        if any(value in stage for value in ("model", "initializing")):
            return 1
        if any(value in stage for value in ("frame", "preparing frames")):
            return 2
        if any(
            value in stage
            for value in (
                "assembling",
                "restoring",
                "labelling",
                "finalizing",
                "splitting",
            )
        ):
            return 3
        if "upload" in stage:
            return 4
        if self.stage in {"Stopped", "Waiting"}:
            return 0
        return 1

    def _is_asset_stage(self) -> bool:
        return self._pipeline_index() == 1

    def _byte_label(self) -> str:
        stage = self.stage.lower()
        if self._is_asset_stage():
            return "Asset"
        if "upload" in stage:
            return "Upload"
        return "Transfer"

    @staticmethod
    def _bar(percent: float, *, cells: int) -> str:
        full = min(cells, max(0, math.floor(percent / 100 * cells)))
        return "[" + "█" * full + "░" * (cells - full) + "]"

    @staticmethod
    def _bytes(value: float) -> str:
        amount = float(value)
        units = ("B", "KiB", "MiB", "GiB", "TiB")
        index = 0
        while amount >= 1024 and index < len(units) - 1:
            amount /= 1024
            index += 1
        return f"{amount:.2f} {units[index]}"

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

    @staticmethod
    def _trim(value: str, limit: int) -> str:
        return value if len(value) <= limit else value[: max(1, limit - 1)] + "…"

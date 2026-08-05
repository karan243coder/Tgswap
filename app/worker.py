"""Single-concurrency, frame-by-frame FaceFusion worker with advanced progress."""

from __future__ import annotations

import asyncio
import logging
import math
import os
import re
import shutil
import signal
import time
from pathlib import Path
from typing import Any, Literal

from .config import Settings
from .media import (
    MediaError,
    build_normalize_command,
    build_watermark_command,
    ensure_workspace_capacity,
    probe_media,
    validate_target_video,
)
from .models import ActiveJob, Job
from .progress import ProgressDisplay
from .storage import Storage
from .telegram_mtproto import MTProtoTelegramClient, TelegramTransportError

logger = logging.getLogger(__name__)
_ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_TQDM_RE = re.compile(
    r"(?P<label>processing).*?(?P<percent>\d{1,3})%\|.*?(?P<current>[\d,]+)\/(?P<total>[\d,]+)"
    r"(?:.*?(?P<fps>[\d.]+)\s*frame/s)?",
    re.IGNORECASE,
)


class JobCancelled(RuntimeError):
    pass


class JobTimedOut(RuntimeError):
    pass


class ExternalProcessError(RuntimeError):
    def __init__(self, stage: str, return_code: int, log_tail: str) -> None:
        super().__init__(f"{stage} exited with status {return_code}")
        self.stage = stage
        self.return_code = return_code
        self.log_tail = log_tail


class JobWorker:
    """Runs one resource-intensive render at a time.

    FaceFusion processes each video frame independently in the configured memory
    or disk workflow. One worker preserves GPU/CPU stability and lets the
    progress display report a truthful queue position and frame counter.
    """

    def __init__(
        self,
        settings: Settings,
        storage: Storage,
        telegram: MTProtoTelegramClient,
    ) -> None:
        self.settings = settings
        self.storage = storage
        self.telegram = telegram
        # SQLite admission limits the queue when configured. An unbounded memory
        # queue ensures persisted jobs are not stranded after a restart.
        self.queue: asyncio.Queue[Job] = asyncio.Queue()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._active: ActiveJob | None = None
        self._cancel_requested: set[str] = set()

    @property
    def active_job_id(self) -> str | None:
        return self._active.job.job_id if self._active else None

    async def start(self) -> None:
        interrupted = self.storage.mark_running_jobs_interrupted()
        for job in interrupted:
            await self._cleanup_job(job)
        if not self.settings.keep_job_artifacts:
            for job in self.storage.list_final_jobs():
                await self._cleanup_job(job)

        self._task = asyncio.create_task(self._run(), name="facefusion-job-worker")
        for job in self.storage.list_queued_jobs():
            if not job.source_path.exists() or not job.target_path.exists():
                self.storage.set_job_status(
                    job.job_id,
                    "failed",
                    error="Queued media was unavailable after service restart",
                )
                await self._cleanup_job(job)
                continue
            self.queue.put_nowait(job)

    async def stop(self) -> None:
        self._stopping = True
        if self._active is not None:
            self._cancel_requested.add(self._active.job.job_id)
            active_process = self._active.process
            if isinstance(active_process, asyncio.subprocess.Process):
                await self._terminate_process(active_process)
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def enqueue(self, job: Job) -> None:
        self.queue.put_nowait(job)

    async def cancel_for_chat(self, chat_id: int) -> tuple[int, bool]:
        queued_count = self.storage.cancel_queued_jobs(chat_id)
        cancelled_active = False
        if self._active is not None and self._active.job.chat_id == chat_id:
            cancelled_active = True
            self._cancel_requested.add(self._active.job.job_id)
            process = self._active.process
            if isinstance(process, asyncio.subprocess.Process):
                await self._terminate_process(process)
        return queued_count, cancelled_active

    async def _run(self) -> None:
        while not self._stopping:
            job = await self.queue.get()
            try:
                if self.storage.is_cancelled(job.job_id):
                    await self._cleanup_job(job)
                    continue
                await self._process(job)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Unexpected worker error for job=%s", job.job_id)
            finally:
                self.queue.task_done()

    async def _process(self, job: Job) -> None:
        if not self.storage.mark_running(job.job_id):
            await self._cleanup_job(job)
            return

        self._active = ActiveJob(job=job)
        progress = ProgressDisplay.attach(
            self.telegram,
            job.chat_id,
            job.job_id,
            job.progress_message_id,
            edit_interval_seconds=self.settings.progress_edit_seconds,
        )
        job_dir = job.target_path.parent
        normalized_target = job_dir / "target-normalized.mp4"

        try:
            await progress.update(
                stage="Starting render",
                detail="Verifying the isolated media workspace and render profile.",
                overall=17,
                force=True,
            )
            self._raise_if_cancelled(job)
            input_info = await probe_media(job.target_path, self.settings.ffprobe_path)
            validate_target_video(input_info, self.settings)
            ensure_workspace_capacity(
                self.settings.data_dir, job.target_path.stat().st_size, self.settings
            )
            progress.set_video_context(
                duration_seconds=input_info.duration_seconds,
                quality_line=self._quality_line(
                    input_info.width, input_info.height, input_info.fps
                ),
            )

            target_for_facefusion = job.target_path
            if self.settings.normalize_input:
                await progress.update(
                    stage="Normalizing input",
                    detail="Converting the video without reducing resolution unless MAX_VIDEO_SIDE is configured.",
                    overall=18,
                    force=True,
                )
                await self._run_command(
                    build_normalize_command(
                        job.target_path, normalized_target, self.settings
                    ),
                    job,
                    stage="input normalization",
                    progress=progress,
                    mode="ffmpeg",
                    duration_seconds=input_info.duration_seconds,
                    progress_start=18,
                    progress_span=7,
                )
                self._require_output(
                    normalized_target, "The target video could not be normalized."
                )
                target_for_facefusion = normalized_target

            output_extension = target_for_facefusion.suffix.lower() or ".mp4"
            facefusion_output = job_dir / f"facefusion-output{output_extension}"
            labelled_output = job_dir / f"face-swap-labelled{output_extension}"

            await progress.update(
                stage="Initializing FaceFusion",
                detail="Loading models and preparing frame-by-frame inference.",
                overall=25,
                note="Every target frame will be processed.",
                force=True,
            )
            await self._run_command(
                self._facefusion_command(
                    job.source_path,
                    target_for_facefusion,
                    facefusion_output,
                    job_dir,
                ),
                job,
                stage="FaceFusion",
                progress=progress,
                mode="facefusion",
                duration_seconds=input_info.duration_seconds,
                total_frames=input_info.frame_count,
            )
            self._require_output(
                facefusion_output, "FaceFusion did not produce an output video."
            )
            self._raise_if_cancelled(job)

            result_path = facefusion_output
            if self.settings.watermark_output:
                output_info = await probe_media(
                    facefusion_output, self.settings.ffprobe_path
                )
                await progress.update(
                    stage="Labelling output",
                    detail="Applying the visible AI face-swap disclosure label.",
                    overall=86,
                    force=True,
                )
                await self._run_command(
                    build_watermark_command(
                        facefusion_output, labelled_output, self.settings
                    ),
                    job,
                    stage="output labelling",
                    progress=progress,
                    mode="ffmpeg",
                    duration_seconds=output_info.duration_seconds,
                    progress_start=86,
                    progress_span=6,
                )
                self._require_output(
                    labelled_output, "The output label could not be added."
                )
                result_path = labelled_output

            self._raise_if_cancelled(job)
            await self._send_result(job, result_path, progress)
            self.storage.set_job_status(job.job_id, "completed")
            await progress.complete(
                "Completed successfully. The result was delivered in this chat."
            )
        except JobCancelled:
            self.storage.set_job_status(
                job.job_id, "cancelled", error="Cancelled by user"
            )
            await progress.fail(
                "Cancelled by the user. Uploaded job media is being removed."
            )
        except JobTimedOut:
            self.storage.set_job_status(
                job.job_id, "failed", error="Processing timed out"
            )
            await progress.fail(
                "Processing exceeded the configured timeout and was stopped."
            )
        except MediaError as exc:
            self.storage.set_job_status(job.job_id, "failed", error=str(exc)[:500])
            await progress.fail(f"Media processing stopped: {exc}")
        except ExternalProcessError as exc:
            self.storage.set_job_status(job.job_id, "failed", error=str(exc))
            logger.warning(
                "External stage failed: job=%s stage=%s rc=%s log_tail=%r",
                job.job_id,
                exc.stage,
                exc.return_code,
                exc.log_tail[-1200:],
            )
            await progress.fail(self._friendly_process_error(exc))
        except TelegramTransportError:
            self.storage.set_job_status(
                job.job_id, "failed", error="Could not deliver output to Telegram"
            )
            logger.warning("Telegram delivery failed for job=%s", job.job_id)
            await progress.fail(
                "The render finished, but Telegram could not receive the output file."
            )
        except Exception:
            self.storage.set_job_status(
                job.job_id, "failed", error="Unexpected processing error"
            )
            logger.exception("Processing failed for job=%s", job.job_id)
            await progress.fail(
                "The render stopped unexpectedly. Please try the media again."
            )
        finally:
            self._active = None
            self._cancel_requested.discard(job.job_id)
            await self._cleanup_job(job)

    def _facefusion_command(
        self,
        source_path: Path,
        target_path: Path,
        output_path: Path,
        job_dir: Path,
    ) -> list[str]:
        temp_dir = job_dir / "facefusion-temp"
        jobs_dir = job_dir / "facefusion-jobs"
        temp_dir.mkdir(parents=True, exist_ok=True)
        jobs_dir.mkdir(parents=True, exist_ok=True)
        return [
            self.settings.python_executable,
            self.settings.facefusion_entrypoint,
            "headless-run",
            "--source-paths",
            str(source_path),
            "--target-path",
            str(target_path),
            "--output-path",
            str(output_path),
            "--processors",
            "face_swapper",
            "--face-swapper-model",
            self.settings.facefusion_model,
            "--face-swapper-pixel-boost",
            self.settings.face_swapper_pixel_boost,
            "--face-detector-model",
            self.settings.face_detector_model,
            "--face-detector-size",
            self.settings.face_detector_size,
            "--face-detector-score",
            str(self.settings.face_detector_score),
            "--face-selector-mode",
            self.settings.face_selector_mode,
            "--face-selector-order",
            "large-small",
            "--reference-frame-number",
            str(self.settings.reference_frame_number),
            "--reference-face-position",
            str(self.settings.reference_face_position),
            "--reference-face-distance",
            str(self.settings.reference_face_distance),
            "--face-tracker-score",
            str(self.settings.face_tracker_score),
            "--face-mask-types",
            *self.settings.face_mask_types,
            "--face-mask-blur",
            "0.3",
            "--workflow-strategy",
            self.settings.workflow_strategy,
            "--video-memory-strategy",
            "strict",
            "--execution-providers",
            self.settings.execution_provider,
            "--execution-thread-count",
            str(self.settings.execution_threads),
            "--output-video-encoder",
            "libx264",
            "--output-video-preset",
            self.settings.output_video_preset,
            "--output-video-quality",
            str(self.settings.output_video_quality),
            "--temp-path",
            str(temp_dir),
            "--jobs-path",
            str(jobs_dir),
            "--log-level",
            "info",
        ]

    async def _run_command(
        self,
        command: list[str],
        job: Job,
        *,
        stage: str,
        progress: ProgressDisplay,
        mode: Literal["facefusion", "ffmpeg"],
        duration_seconds: float,
        total_frames: int = 0,
        progress_start: float = 0.0,
        progress_span: float = 0.0,
    ) -> None:
        """Run a process group, persist its log, and parse honest live telemetry."""
        log_path = job.target_path.parent / "worker.log"
        with log_path.open("ab") as log_handle:
            log_handle.write(
                f"\n\n--- {stage} @ {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} ---\n".encode()
            )
            log_handle.flush()
            try:
                process = await asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    start_new_session=True,
                )
            except FileNotFoundError as exc:
                raise ExternalProcessError(
                    stage, 127, f"Executable unavailable: {command[0]}"
                ) from exc

            if self._active is not None:
                self._active.process = process
            consumer = asyncio.create_task(
                self._consume_process_output(
                    process.stdout,
                    log_handle,
                    progress,
                    mode=mode,
                    duration_seconds=duration_seconds,
                    total_frames=total_frames,
                    progress_start=progress_start,
                    progress_span=progress_span,
                ),
                name=f"process-output-{job.job_id[:8]}",
            )
            try:
                return_code = await self._wait_for_process(process, job)
                await consumer
            except BaseException:
                consumer.cancel()
                await asyncio.gather(consumer, return_exceptions=True)
                raise
            finally:
                if self._active is not None:
                    self._active.process = None

        if return_code != 0:
            raise ExternalProcessError(stage, return_code, self._tail(log_path))

    async def _consume_process_output(
        self,
        stream: asyncio.StreamReader | None,
        log_handle: Any,
        progress: ProgressDisplay,
        *,
        mode: Literal["facefusion", "ffmpeg"],
        duration_seconds: float,
        total_frames: int,
        progress_start: float,
        progress_span: float,
    ) -> None:
        if stream is None:
            return
        buffer = ""
        while True:
            chunk = await stream.read(4096)
            if not chunk:
                break
            log_handle.write(chunk)
            log_handle.flush()
            buffer += chunk.decode("utf-8", errors="replace")
            pieces = re.split(r"[\r\n]", buffer)
            buffer = pieces.pop()
            for line in pieces:
                await self._handle_process_line(
                    line,
                    progress,
                    mode=mode,
                    duration_seconds=duration_seconds,
                    total_frames=total_frames,
                    progress_start=progress_start,
                    progress_span=progress_span,
                )
        if buffer:
            await self._handle_process_line(
                buffer,
                progress,
                mode=mode,
                duration_seconds=duration_seconds,
                total_frames=total_frames,
                progress_start=progress_start,
                progress_span=progress_span,
            )

    async def _handle_process_line(
        self,
        line: str,
        progress: ProgressDisplay,
        *,
        mode: Literal["facefusion", "ffmpeg"],
        duration_seconds: float,
        total_frames: int,
        progress_start: float,
        progress_span: float,
    ) -> None:
        clean = _ANSI_RE.sub("", line).strip()
        if not clean:
            return
        lowered = clean.lower()

        if mode == "ffmpeg":
            seconds = self._ffmpeg_progress_seconds(clean)
            if seconds is not None:
                await progress.ffmpeg_update(
                    stage="Video preparation"
                    if progress_start < 50
                    else "Finalizing output",
                    detail="Encoding media while preserving the configured render profile.",
                    seconds_done=seconds,
                    seconds_total=duration_seconds,
                    overall_start=progress_start,
                    overall_span=progress_span,
                )
            return

        if "downloading" in lowered and ("model" in lowered or ".onnx" in lowered):
            await progress.update(
                stage="Downloading model assets",
                detail="FaceFusion is obtaining required model assets for the selected quality profile.",
                overall=25,
                note="This normally happens only on the first render after deployment.",
            )
            return
        if "extracting frames" in lowered:
            await progress.update(
                stage="Preparing frames",
                detail="FaceFusion is decoding the target video for frame-by-frame inference.",
                overall=26,
            )
            return
        if "merging video" in lowered:
            await progress.update(
                stage="Assembling video",
                detail="Merging the fully swapped frames into the output video.",
                overall=86,
            )
            return
        if "restoring audio" in lowered or "replacing audio" in lowered:
            await progress.update(
                stage="Restoring audio",
                detail="Restoring the original target audio track.",
                overall=90,
            )
            return

        match = _TQDM_RE.search(clean)
        if match:
            current = self._int_from_tqdm(match.group("current"))
            total = self._int_from_tqdm(match.group("total")) or total_frames
            fps = self._float_or_none(match.group("fps"))
            if total > 0:
                await progress.frame_update(current=current, total=total, fps=fps)

    @staticmethod
    def _ffmpeg_progress_seconds(line: str) -> float | None:
        if line.startswith("out_time_us="):
            value = JobWorker._float_or_none(line.split("=", 1)[1])
            return value / 1_000_000 if value is not None else None
        if line.startswith("out_time_ms="):
            value = JobWorker._float_or_none(line.split("=", 1)[1])
            if value is None:
                return None
            # FFmpeg versions have historically exposed this field in microseconds.
            return value / 1_000_000
        if line.startswith("out_time="):
            value = line.split("=", 1)[1]
            try:
                hours, minutes, seconds = value.split(":")
                return int(hours) * 3600 + int(minutes) * 60 + float(seconds)
            except ValueError:
                return None
        return None

    async def _wait_for_process(
        self, process: asyncio.subprocess.Process, job: Job
    ) -> int:
        started = time.monotonic()
        while True:
            self._raise_if_cancelled(job)
            if self.settings.job_timeout_seconds > 0:
                elapsed = time.monotonic() - started
                if elapsed >= self.settings.job_timeout_seconds:
                    await self._terminate_process(process)
                    raise JobTimedOut()
                wait_seconds = min(1.0, self.settings.job_timeout_seconds - elapsed)
            else:
                wait_seconds = 1.0
            try:
                return await asyncio.wait_for(process.wait(), timeout=wait_seconds)
            except TimeoutError:
                continue

    def _raise_if_cancelled(self, job: Job) -> None:
        if (
            self._stopping
            or job.job_id in self._cancel_requested
            or self.storage.is_cancelled(job.job_id)
        ):
            raise JobCancelled()

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:
                process.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=8)
            return
        except TimeoutError:
            pass
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
        except ProcessLookupError:
            return
        await asyncio.gather(process.wait(), return_exceptions=True)

    async def _send_result(
        self, job: Job, path: Path, progress: ProgressDisplay
    ) -> None:
        size = path.stat().st_size
        if size <= self.settings.telegram_upload_part_bytes:
            await progress.update(
                stage="Uploading result",
                detail="Uploading the completed face-swap video through MTProto.",
                overall=93,
                force=True,
            )
            callback = self._cancellable_transfer_callback(
                job,
                progress.transfer_callback(
                    stage="Uploading result",
                    detail="Uploading the completed face-swap video through MTProto.",
                    overall_start=93,
                    overall_span=7,
                ),
            )
            caption = "AI face-swap result • share only with consent"
            if path.suffix.lower() == ".mp4":
                await self.telegram.send_video(
                    job.chat_id, path, caption, progress_callback=callback
                )
            else:
                await self.telegram.send_document(
                    job.chat_id, path, caption, progress_callback=callback
                )
            return

        if not self.settings.split_large_results:
            raise MediaError(
                "The result exceeds Telegram's per-file upload limit and SPLIT_LARGE_RESULTS is disabled."
            )

        part_count = math.ceil(size / self.settings.telegram_upload_part_bytes)
        await progress.update(
            stage="Splitting large result",
            detail=f"The result is larger than Telegram's per-file cap; preparing {part_count} lossless parts.",
            overall=93,
            force=True,
        )
        for index in range(1, part_count + 1):
            self._raise_if_cancelled(job)
            part_path = path.with_name(f"{path.name}.part{index:03d}")
            await progress.update(
                stage="Splitting large result",
                detail=f"Preparing lossless result part {index}/{part_count}.",
                overall=93,
            )
            offset = (index - 1) * self.settings.telegram_upload_part_bytes
            await asyncio.to_thread(
                self._write_result_part,
                path,
                part_path,
                offset,
                self.settings.telegram_upload_part_bytes,
            )
            self._raise_if_cancelled(job)
            try:
                start = 93 + 7 * ((index - 1) / part_count)
                span = 7 / part_count
                await self.telegram.send_document(
                    job.chat_id,
                    part_path,
                    (
                        f"Face-swap result part {index}/{part_count}. "
                        "Download all parts, keep their order, then reassemble them exactly as documented."
                    ),
                    progress_callback=self._cancellable_transfer_callback(
                        job,
                        progress.transfer_callback(
                            stage=f"Uploading result part {index}/{part_count}",
                            detail="Uploading a lossless numbered result part through MTProto.",
                            overall_start=start,
                            overall_span=span,
                        ),
                    ),
                )
            finally:
                part_path.unlink(missing_ok=True)
        await self.telegram.safe_send_message(
            job.chat_id,
            "Large result delivered as binary parts. Reassemble on Linux/macOS with: "
            f"cat {path.name}.part* > {path.name}\n"
            "On Windows Command Prompt use: copy /b part001+part002+... output-file",
        )

    def _cancellable_transfer_callback(self, job: Job, callback):  # type: ignore[no-untyped-def]
        async def wrapped(current: int, total: int) -> None:
            self._raise_if_cancelled(job)
            await callback(current, total)

        return wrapped

    @staticmethod
    def _write_result_part(
        source_path: Path,
        part_path: Path,
        offset: int,
        part_bytes: int,
    ) -> None:
        """Copy exactly one bounded binary slice without blocking the event loop."""
        with source_path.open("rb") as source, part_path.open("wb") as destination:
            source.seek(offset)
            remaining = part_bytes
            while remaining > 0:
                chunk = source.read(min(8 * 1024 * 1024, remaining))
                if not chunk:
                    break
                destination.write(chunk)
                remaining -= len(chunk)
        if part_path.stat().st_size == 0:
            part_path.unlink(missing_ok=True)
            raise MediaError("Could not create a non-empty Telegram result part.")

    def _quality_line(self, width: int, height: int, fps: float) -> str:
        resolution = f"{width}×{height}" if width and height else "original resolution"
        fps_text = f"{fps:.3g} fps" if fps else "original frame rate"
        selector = (
            "reference tracking"
            if self.settings.face_selector_mode == "reference"
            else "single-face selection"
        )
        return (
            f"{resolution} • {fps_text} • {selector} • pixel boost "
            f"{self.settings.face_swapper_pixel_boost} • {self.settings.workflow_strategy} frame workflow"
        )

    @staticmethod
    def _require_output(path: Path, message: str) -> None:
        if not path.exists() or path.stat().st_size < 1024:
            raise MediaError(message)

    @staticmethod
    def _int_from_tqdm(value: str | None) -> int:
        try:
            return int((value or "0").replace(",", ""))
        except ValueError:
            return 0

    @staticmethod
    def _float_or_none(value: str | None) -> float | None:
        try:
            return float(value) if value is not None else None
        except ValueError:
            return None

    def _friendly_process_error(self, error: ExternalProcessError) -> str:
        tail = error.log_tail.lower()
        if "no source face" in tail or "choose image source" in tail:
            return "No clear face was found in the source image. Send a well-lit, front-facing image with one face."
        if "no target face" in tail or "choose image or video target" in tail:
            return "No target face was found. Ensure the intended target face is visible in the configured reference frame."
        if "content" in tail and ("unsafe" in tail or "not suitable" in tail):
            return "This media cannot be processed. Use non-sensitive, consented media only."
        if error.stage == "FaceFusion":
            return "FaceFusion stopped. Try a clearer source image or choose a target video with a visible reference face."
        return "Video processing stopped. Inspect the input codec and the worker log, then try again."

    @staticmethod
    def _tail(path: Path, limit: int = 4000) -> str:
        try:
            with path.open("rb") as handle:
                handle.seek(0, os.SEEK_END)
                handle.seek(max(0, handle.tell() - limit))
                return handle.read().decode("utf-8", errors="replace")
        except OSError:
            return ""

    async def _cleanup_job(self, job: Job) -> None:
        if self.settings.keep_job_artifacts:
            return
        await asyncio.to_thread(self._safe_rmtree, job.target_path.parent)

    def _safe_rmtree(self, path: Path) -> None:
        try:
            jobs_root = (self.settings.data_dir / "jobs").resolve()
            resolved = path.resolve()
            if resolved == jobs_root or jobs_root not in resolved.parents:
                logger.error(
                    "Refusing to delete a path outside generated jobs: %s", path
                )
                return
            shutil.rmtree(resolved, ignore_errors=True)
        except OSError:
            logger.warning("Could not clean job directory %s", path, exc_info=True)

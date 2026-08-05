"""Industrial MTProto Telegram workflow, buttons, state and worker lifecycle."""

from __future__ import annotations

import asyncio
import logging
import shutil
import time
from collections.abc import Coroutine
from pathlib import Path
from typing import Any
from uuid import uuid4

from telethon import events

from .config import Settings
from .media import (
    MediaError,
    allowed_extension,
    ensure_workspace_capacity,
    probe_media,
    validate_source_image,
    validate_target_video,
)
from .menus import (
    AGREE,
    CANCEL,
    HELP,
    HOME,
    QUALITY,
    RESET,
    SOURCE,
    STATUS,
    TARGET,
    back_keyboard,
    home_keyboard,
    status_keyboard,
)
from .models import Job, MediaRef
from .progress import ProgressDisplay
from .resources import (
    RenderResourceError,
    inspect_runtime_resources,
    require_render_memory,
)
from .storage import Storage
from .telegram_mtproto import MTProtoTelegramClient, TelegramTransportError
from .worker import JobWorker

logger = logging.getLogger(__name__)
_IMAGE_EXTENSIONS = {".bmp", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp"}
_VIDEO_EXTENSIONS = {
    ".avi",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".mxf",
    ".webm",
    ".wmv",
}
_ALLOWED_CALLBACKS = {HOME, AGREE, SOURCE, TARGET, STATUS, QUALITY, CANCEL, RESET, HELP}


class BotService:
    """Coordinates MTProto updates, button actions, SQLite state and rendering.

    Incoming Telethon callbacks return almost immediately: expensive download,
    model and render work is moved into background tasks or the single worker.
    This is important because Telethon sequential update handling otherwise lets
    a long callback create an unbounded update backlog.
    """

    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.storage = Storage(settings.data_dir)
        self.telegram: MTProtoTelegramClient | None = None
        self.worker: JobWorker | None = None
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._chat_locks: dict[int, asyncio.Lock] = {}
        self._callback_last_seen: dict[tuple[int, bytes], float] = {}
        self._deferred_panel_chats: set[int] = set()
        self._deferred_panel_payloads: dict[int, tuple[str, Any]] = {}
        self._ingest_tasks: dict[int, asyncio.Task[Any]] = {}
        self._cleanup_task: asyncio.Task[None] | None = None
        self._connect_task: asyncio.Task[Any] | None = None
        self._stopping = False

    async def start(self) -> None:
        self.settings.data_dir.mkdir(parents=True, exist_ok=True)
        (self.settings.data_dir / "sessions").mkdir(parents=True, exist_ok=True)
        (self.settings.data_dir / "jobs").mkdir(parents=True, exist_ok=True)
        self.storage.open()
        self._cleanup_task = asyncio.create_task(
            self._periodic_cleanup(), name="media-cleanup"
        )

        if not self.settings.telegram_enabled:
            logger.warning(
                "MTProto bot is disabled until TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, "
                "and TELEGRAM_API_HASH are configured. The health endpoint remains available."
            )
            return

        self.telegram = MTProtoTelegramClient(self.settings, self.storage)
        self.worker = JobWorker(self.settings, self.storage, self.telegram)
        await self.worker.start()
        # Health on port 8080 must not wait for Telegram reconnects or FloodWait.
        self._connect_task = asyncio.create_task(
            self._connect_mtproto(), name="connect-mtproto"
        )

    async def stop(self) -> None:
        self._stopping = True
        if self._connect_task is not None:
            self._connect_task.cancel()
            await asyncio.gather(self._connect_task, return_exceptions=True)
            self._connect_task = None
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            await asyncio.gather(self._cleanup_task, return_exceptions=True)
            self._cleanup_task = None

        for task in list(self._background_tasks):
            task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._deferred_panel_chats.clear()
        self._deferred_panel_payloads.clear()

        if self.worker is not None:
            await self.worker.stop()
            self.worker = None
        if self.telegram is not None:
            await self.telegram.close()
            self.telegram = None
        self.storage.close()

    async def _connect_mtproto(self) -> None:
        assert self.telegram is not None
        # Keep Koyeb alive during temporary Telegram/DC outages. The delay is
        # bounded and asynchronous; health checks and the worker never crash-loop.
        attempt = 0
        delay = 0.0
        while not self._stopping:
            if delay:
                await asyncio.sleep(delay)
            if self._stopping:
                return
            try:
                await self.telegram.start(self.accept_update, self.accept_callback)
                return
            except TelegramTransportError:
                attempt += 1
                delay = min(300.0, 5.0 * (2 ** min(attempt - 1, 6)))
                logger.warning(
                    "MTProto connection attempt %s failed; retrying in %.0fs",
                    attempt,
                    delay,
                )

    @property
    def ready(self) -> bool:
        return bool(self.worker and self.telegram and self.telegram.is_connected)

    def health_payload(self) -> dict[str, Any]:
        rate: dict[str, Any] = {}
        if self.telegram is not None:
            status = self.telegram.rate_status
            rate = {
                "flood_wait_remaining_seconds": status.remaining_seconds,
                "flood_events_since_start": status.flood_events,
                "transient_retries_since_start": status.transient_retries,
                "dropped_noncritical_progress_updates": status.dropped_noncritical,
            }
        return {
            "status": "ok",
            "transport": "mtproto",
            "telegram_configured": self.settings.telegram_enabled,
            "transport_connected": bool(self.telegram and self.telegram.is_connected),
            "worker_started": self.worker is not None,
            "active_job_id": self.worker.active_job_id if self.worker else None,
            "queued_jobs": self.worker.queue.qsize() if self.worker else 0,
            "rate_control": rate,
        }

    # --- Event admission ----------------------------------------------------------------

    def accept_update(self, update: dict[str, Any]) -> None:
        """Persist dedupe state then detach work from Telethon's event handler."""
        update_key = update.get("update_id")
        if update_key is not None and not self.storage.remember_update(str(update_key)):
            return
        self._spawn(self._handle_update(update), name=f"telegram-update-{update_key}")

    def accept_callback(self, event: events.CallbackQuery.Event) -> None:
        """Acknowledge inline taps quickly and debounce accidental rapid tapping."""
        if self._stopping:
            return
        chat_id = event.chat_id
        data = event.data or b""
        if chat_id is None:
            return
        key = (int(chat_id), bytes(data))
        now = time.monotonic()
        previous = self._callback_last_seen.get(key, 0.0)
        self._callback_last_seen[key] = now
        # Callback acknowledgement is deliberately independent from the action.
        # A slow Telegram acknowledgement must never delay cancel/reset/status.
        self._spawn(self._ack_callback(event), name="callback-ack")
        if now - previous < 0.55:
            # Acknowledge the duplicate tap, but do not queue duplicate work.
            return
        self._spawn(self._route_callback(event), name=f"callback-{int(chat_id)}")

    def _spawn(self, coroutine: Coroutine[Any, Any, Any], *, name: str) -> None:
        if self._stopping:
            coroutine.close()
            return
        task = asyncio.create_task(coroutine, name=name)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(self._log_background_task_error)

    @staticmethod
    def _log_background_task_error(task: asyncio.Task[Any]) -> None:
        if task.cancelled():
            return
        try:
            task.result()
        except Exception:  # noqa: BLE001 - detached task failures must be consumed safely
            logger.error("Background task %s failed", task.get_name())

    # --- Message and callback routing ---------------------------------------------------

    async def _handle_update(self, update: dict[str, Any]) -> None:
        message = update.get("message")
        if not isinstance(message, dict):
            return
        chat = message.get("chat")
        sender = message.get("from")
        if not isinstance(chat, dict) or not isinstance(sender, dict):
            return
        try:
            chat_id = int(chat["id"])
            user_id = int(sender["id"])
        except (KeyError, TypeError, ValueError):
            return
        if not self._is_allowed_user(user_id, str(chat.get("type", ""))):
            return

        lock = self._chat_locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            await self._handle_message(chat_id, user_id, message)

    async def _route_callback(self, event: events.CallbackQuery.Event) -> None:
        if self.telegram is None or self.worker is None:
            return
        chat_id = event.chat_id
        user_id = event.sender_id
        message_id = event.message_id
        data = event.data or b""
        if (
            chat_id is None
            or user_id is None
            or message_id is None
            or data not in _ALLOWED_CALLBACKS
        ):
            return
        try:
            chat_id = int(chat_id)
            user_id = int(user_id)
            message_id = int(message_id)
        except (TypeError, ValueError):
            return
        # The project deliberately defaults to private chats. Callback events do
        # not expose the same simple chat-type field as NewMessage, so reject
        # non-positive IDs unless the operator explicitly opted into groups.
        if not self.settings.allow_groups and chat_id <= 0:
            return
        if (
            self.settings.allowed_user_ids
            and user_id not in self.settings.allowed_user_ids
        ):
            return

        lock = self._chat_locks.setdefault(chat_id, asyncio.Lock())
        async with lock:
            await self._handle_callback_action(chat_id, user_id, message_id, data)

    async def _ack_callback(self, event: events.CallbackQuery.Event) -> None:
        if self.telegram is not None:
            # Noncritical and rate-gated: a delayed acknowledgement must never
            # block model rendering or cause Telethon's update queue to grow.
            await self.telegram.answer_callback(event)

    def _is_allowed_user(self, user_id: int, chat_type: str) -> bool:
        if (
            self.settings.allowed_user_ids
            and user_id not in self.settings.allowed_user_ids
        ):
            logger.info("Ignoring a user outside ALLOWED_USER_IDS")
            return False
        return chat_type == "private" or self.settings.allow_groups

    async def _handle_message(
        self, chat_id: int, user_id: int, message: dict[str, Any]
    ) -> None:
        if self.telegram is None or self.worker is None:
            return
        text = message.get("text")
        if isinstance(text, str) and text.startswith("/"):
            await self._handle_command(chat_id, text, message.get("message_id"))
            return

        source = self._extract_source(message)
        if source is not None:
            await self._begin_ingest(
                chat_id, self._receive_source(chat_id, source), "source image"
            )
            return

        target = self._extract_video(message)
        if target is not None:
            await self._begin_ingest(
                chat_id,
                self._receive_target(chat_id, user_id, target),
                "target video",
            )
            return

        session = self.storage.get_session(chat_id)
        if session.consent:
            await self._send_control_panel(
                chat_id,
                intro="Use the buttons below to view status, change your source image, or prepare a target video.",
            )

    async def _handle_command(self, chat_id: int, raw_text: str, reply_to: Any) -> None:
        """Keep only essential commands; the normal workflow is button-first."""
        command = raw_text.split(maxsplit=1)[0].split("@", 1)[0].lower()
        message_id = reply_to if isinstance(reply_to, int) else None
        if command in {"/start", "/menu", "/help"}:
            await self._send_control_panel(chat_id, reply_to_message_id=message_id)
            return
        if command == "/status":
            # Emergency/keyboard fallback; the panel has the preferred button.
            await self._send_status_panel(chat_id, reply_to_message_id=message_id)
            return
        if command == "/cancel":
            await self._cancel_render(chat_id)
            await self._send_status_panel(chat_id, reply_to_message_id=message_id)
            return
        await self._send_control_panel(
            chat_id,
            intro="Use /menu or the inline buttons below. The main workflow does not require commands.",
            reply_to_message_id=message_id,
        )

    async def _handle_callback_action(
        self,
        chat_id: int,
        user_id: int,
        message_id: int,
        action: bytes,
    ) -> None:
        del user_id  # Authorization was checked before taking the per-chat lock.
        session = self.storage.get_session(chat_id)

        if action == HOME:
            await self._edit_control_panel(chat_id, message_id)
        elif action == AGREE:
            self.storage.set_consent(chat_id, True)
            await self._edit_control_panel(
                chat_id,
                message_id,
                intro=(
                    "Consent acknowledgement saved. Select “Set Source Image” and send one clear, "
                    "front-facing image."
                ),
            )
        elif action == SOURCE:
            if not session.consent:
                await self._edit_panel(
                    chat_id,
                    message_id,
                    (
                        "CONSENT REQUIRED\n\n"
                        "Before uploading a face image, confirm that you have permission to use the media. "
                        "Select “I Agree” in the control panel."
                    ),
                    buttons=back_keyboard(),
                )
            else:
                await self._edit_panel(
                    chat_id,
                    message_id,
                    (
                        "SOURCE IMAGE READY\n\n"
                        "Send one clear, well-lit, front-facing source image now. "
                        "The bot will save it privately for the next render."
                    ),
                    buttons=back_keyboard(),
                )
        elif action == TARGET:
            if not session.consent:
                await self._edit_panel(
                    chat_id,
                    message_id,
                    "CONSENT REQUIRED\n\nSelect “I Agree” before uploading media.",
                    buttons=back_keyboard(),
                )
            elif session.source_path is None or not session.source_path.exists():
                await self._edit_panel(
                    chat_id,
                    message_id,
                    (
                        "SOURCE IMAGE REQUIRED\n\n"
                        "Select “Set Source Image” first, then send a target video."
                    ),
                    buttons=back_keyboard(),
                )
            else:
                await self._edit_panel(
                    chat_id,
                    message_id,
                    (
                        "TARGET VIDEO READY\n\n"
                        "Send the target video now. A live progress panel will appear before the MTProto "
                        "download begins."
                    ),
                    buttons=back_keyboard(),
                )
        elif action == STATUS:
            await self._edit_panel(
                chat_id,
                message_id,
                self._status_text(chat_id),
                buttons=status_keyboard(),
            )
        elif action == QUALITY:
            await self._edit_panel(
                chat_id,
                message_id,
                self._quality_text(),
                buttons=back_keyboard(),
            )
        elif action == CANCEL:
            result = await self._cancel_render(chat_id)
            await self._edit_panel(
                chat_id,
                message_id,
                result + "\n\n" + self._status_text(chat_id),
                buttons=status_keyboard(),
            )
        elif action == RESET:
            await self._cancel_render(chat_id)
            old_source = self.storage.reset_session(chat_id)
            self._safe_unlink(old_source)
            await self._edit_control_panel(
                chat_id,
                message_id,
                intro="Your source image and consent state were removed. Select “I Agree” to begin again.",
            )
        elif action == HELP:
            await self._edit_panel(
                chat_id, message_id, self._help_text(), buttons=back_keyboard()
            )

    async def _begin_ingest(
        self,
        chat_id: int,
        coroutine: Coroutine[Any, Any, Any],
        media_label: str,
    ) -> None:
        """Start a cancellable file-ingest task without holding the chat lock."""
        active = self._ingest_tasks.get(chat_id)
        if active is not None and not active.done():
            coroutine.close()
            await self._reply(
                chat_id,
                f"A {media_label} cannot be received yet because another upload is still being processed. "
                "Use Cancel Render to stop it first.",
            )
            return
        task = asyncio.create_task(
            coroutine, name=f"ingest-{media_label.replace(' ', '-')}-{chat_id}"
        )
        self._ingest_tasks[chat_id] = task
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        task.add_done_callback(
            lambda completed: self._clear_ingest_task(chat_id, completed)
        )
        task.add_done_callback(self._log_background_task_error)

    def _clear_ingest_task(self, chat_id: int, task: asyncio.Task[Any]) -> None:
        if self._ingest_tasks.get(chat_id) is task:
            self._ingest_tasks.pop(chat_id, None)

    async def _cancel_ingest(self, chat_id: int) -> bool:
        task = self._ingest_tasks.get(chat_id)
        if task is None or task.done():
            return False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return True

    # --- Button panels ------------------------------------------------------------------

    async def _send_control_panel(
        self,
        chat_id: int,
        *,
        intro: str | None = None,
        reply_to_message_id: int | None = None,
    ) -> None:
        if self.telegram is None:
            return
        session = self.storage.get_session(chat_id)
        text = self._home_text(chat_id, intro=intro)
        try:
            await self.telegram.send_message(
                chat_id,
                text,
                reply_to_message_id=reply_to_message_id,
                buttons=home_keyboard(consented=session.consent),
            )
        except TelegramTransportError:
            logger.warning("Could not send control panel to chat_id=%s", chat_id)

    async def _edit_control_panel(
        self,
        chat_id: int,
        message_id: int,
        *,
        intro: str | None = None,
    ) -> None:
        session = self.storage.get_session(chat_id)
        await self._edit_panel(
            chat_id,
            message_id,
            self._home_text(chat_id, intro=intro),
            buttons=home_keyboard(consented=session.consent),
        )

    async def _send_status_panel(
        self,
        chat_id: int,
        *,
        reply_to_message_id: int | None = None,
    ) -> None:
        if self.telegram is None:
            return
        try:
            await self.telegram.send_message(
                chat_id,
                self._status_text(chat_id),
                reply_to_message_id=reply_to_message_id,
                buttons=status_keyboard(),
            )
        except TelegramTransportError:
            logger.warning("Could not send status panel to chat_id=%s", chat_id)

    async def _edit_panel(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Any,
    ) -> None:
        """Prefer a nonblocking edit; defer a fresh panel if rate-controlled."""
        if self.telegram is None:
            return
        outcome = await self.telegram.edit_menu_message(
            chat_id,
            message_id,
            text,
            buttons=buttons,
        )
        if outcome == "deferred":
            # Coalesce to the newest requested panel while Telegram is paced or
            # rate-limited. A rapid Status → Quality → Home tap never produces
            # stale panels in the wrong order.
            self._deferred_panel_payloads[chat_id] = (text, buttons)
            if chat_id not in self._deferred_panel_chats:
                self._deferred_panel_chats.add(chat_id)
                self._spawn(
                    self._send_deferred_panel(chat_id),
                    name=f"deferred-panel-{chat_id}",
                )

    async def _send_deferred_panel(self, chat_id: int) -> None:
        try:
            while self.telegram is not None:
                payload = self._deferred_panel_payloads.pop(chat_id, None)
                if payload is None:
                    return
                text, buttons = payload
                try:
                    await self.telegram.send_message(chat_id, text, buttons=buttons)
                except TelegramTransportError:
                    logger.warning(
                        "Could not deliver deferred control panel to chat_id=%s",
                        chat_id,
                    )
                    return
        finally:
            self._deferred_panel_chats.discard(chat_id)
            # Handle a value that arrived between the final pop and task exit.
            if chat_id in self._deferred_panel_payloads and not self._stopping:
                self._deferred_panel_chats.add(chat_id)
                self._spawn(
                    self._send_deferred_panel(chat_id),
                    name=f"deferred-panel-{chat_id}",
                )

    # --- Media workflow -----------------------------------------------------------------

    async def _receive_source(self, chat_id: int, media: MediaRef) -> None:
        assert self.telegram is not None
        session = self.storage.get_session(chat_id)
        if not session.consent:
            await self._send_control_panel(
                chat_id,
                intro="Consent is required before uploading a source image. Select “I Agree” first.",
            )
            return
        if (
            self.settings.max_image_bytes > 0
            and media.file_size
            and media.file_size > self.settings.max_image_bytes
        ):
            await self._reply(
                chat_id,
                f"Source image exceeds the configured {self.settings.max_image_mb} MiB limit.",
            )
            return
        if media.file_size:
            try:
                ensure_workspace_capacity(
                    self.settings.data_dir, media.file_size, self.settings
                )
            except MediaError as exc:
                await self._reply(chat_id, str(exc))
                return

        display = await ProgressDisplay.create(
            self.telegram,
            chat_id,
            f"source-{uuid4().hex}",
            edit_interval_seconds=self.settings.progress_edit_seconds,
        )
        extension = allowed_extension(media.filename, "image")
        destination = (
            self.settings.data_dir
            / "sessions"
            / str(chat_id)
            / f"source-{uuid4().hex}{extension}"
        )
        try:
            await display.update(
                stage="Downloading source image",
                detail="Receiving the source image through MTProto.",
                overall=0,
                force=True,
            )
            await self.telegram.download_file(
                media.file_id,
                destination,
                max_bytes=self.settings.max_image_bytes,
                progress_callback=display.transfer_callback(
                    stage="Downloading source image",
                    detail="Receiving the source image through MTProto.",
                    overall_start=0,
                    overall_span=90,
                ),
            )
            info = await probe_media(destination, self.settings.ffprobe_path)
            validate_source_image(info)
        except asyncio.CancelledError:
            self._safe_unlink(destination)
            await display.fail(
                "Source-image upload was cancelled. Partial media was removed."
            )
            raise
        except (TelegramTransportError, MediaError) as exc:
            self._safe_unlink(destination)
            await display.fail(f"Source image was not accepted: {exc}")
            return

        previous = self.storage.set_source(chat_id, destination)
        self._safe_unlink(previous)
        await display.complete(
            "Source image saved. Select “Send Target Video” in the control panel, then upload the video."
        )

    async def _receive_target(
        self, chat_id: int, user_id: int, media: MediaRef
    ) -> None:
        assert self.telegram is not None and self.worker is not None
        session = self.storage.get_session(chat_id)
        if not session.consent:
            await self._send_control_panel(
                chat_id,
                intro="Consent is required before uploading media. Select “I Agree” first.",
            )
            return
        if session.source_path is None or not session.source_path.exists():
            await self._send_control_panel(
                chat_id,
                intro="A source image is required before a target video can be rendered.",
            )
            return
        try:
            require_render_memory(
                inspect_runtime_resources(),
                self.settings.min_render_memory_mb,
            )
        except RenderResourceError as exc:
            await self._send_control_panel(chat_id, intro=str(exc))
            return
        if (
            self.settings.max_jobs_per_user > 0
            and self.storage.count_active_jobs(chat_id)
            >= self.settings.max_jobs_per_user
        ):
            await self._reply(
                chat_id,
                "You already have the configured maximum number of active jobs.",
            )
            return
        if (
            self.settings.queue_max_size > 0
            and self.storage.count_queued_jobs() >= self.settings.queue_max_size
        ):
            await self._reply(
                chat_id, "The render queue is currently full. Please try again shortly."
            )
            return
        if (
            self.settings.max_video_bytes > 0
            and media.file_size
            and media.file_size > self.settings.max_video_bytes
        ):
            await self._reply(
                chat_id,
                f"Video exceeds the configured {self.settings.max_video_mb} MiB limit.",
            )
            return
        if media.file_size:
            try:
                ensure_workspace_capacity(
                    self.settings.data_dir, media.file_size, self.settings
                )
            except MediaError as exc:
                await self._reply(chat_id, str(exc))
                return

        job_id = uuid4().hex
        display = await ProgressDisplay.create(
            self.telegram,
            chat_id,
            job_id,
            edit_interval_seconds=self.settings.progress_edit_seconds,
        )
        job_dir = self.settings.data_dir / "jobs" / job_id
        job_dir.mkdir(parents=True, exist_ok=False)
        target_path = (
            job_dir / f"target-upload{allowed_extension(media.filename, 'video')}"
        )
        source_path = (
            job_dir / f"source{allowed_extension(session.source_path.name, 'image')}"
        )
        job_enqueued = False

        try:
            await display.update(
                stage="Preparing job",
                detail="Copying the saved source image into an isolated job workspace.",
                overall=1,
                force=True,
            )
            await asyncio.to_thread(shutil.copy2, session.source_path, source_path)
            await display.update(
                stage="Downloading target video",
                detail="Receiving the original target video through MTProto.",
                overall=2,
                force=True,
            )
            await self.telegram.download_file(
                media.file_id,
                target_path,
                max_bytes=self.settings.max_video_bytes,
                progress_callback=display.transfer_callback(
                    stage="Downloading target video",
                    detail="Receiving the original target video through MTProto.",
                    overall_start=2,
                    overall_span=12,
                ),
            )
            await display.update(
                stage="Inspecting media",
                detail="Reading exact duration, resolution, frame rate and frame count.",
                overall=15,
                force=True,
            )
            target_info = await probe_media(target_path, self.settings.ffprobe_path)
            validate_target_video(target_info, self.settings)
            ensure_workspace_capacity(
                self.settings.data_dir, target_path.stat().st_size, self.settings
            )

            job = Job(
                job_id=job_id,
                chat_id=chat_id,
                user_id=user_id,
                source_path=source_path,
                target_path=target_path,
                status="queued",
                created_at=time.time(),
                progress_message_id=display.message_id,
            )
            accepted = self.storage.create_job(
                job, max_queued_jobs=self.settings.queue_max_size
            )
            if not accepted:
                await self._remove_job_directory(job_dir)
                await display.fail(
                    "The render queue filled while this file was being prepared. Please try again."
                )
                return
            self.worker.enqueue(job)
            job_enqueued = True
        except asyncio.CancelledError:
            if not job_enqueued:
                await self._remove_job_directory(job_dir)
                await display.fail(
                    "Target-video upload was cancelled. Partial media was removed."
                )
            raise
        except (TelegramTransportError, MediaError, OSError) as exc:
            await self._remove_job_directory(job_dir)
            await display.fail(f"Target video was not accepted: {exc}")
            return

        position = self.storage.queue_position(job_id)
        await display.update(
            stage="Queued",
            detail=(
                f"The render is queued. Position: {position}."
                if position
                else "The render is queued."
            ),
            overall=16,
            note="No application file-duration cap is enabled.",
            force=True,
        )

    async def _cancel_render(self, chat_id: int) -> str:
        ingest_cancelled = await self._cancel_ingest(chat_id)
        if self.worker is None:
            return "The render worker is not available."
        queued, active = await self.worker.cancel_for_chat(chat_id)
        if ingest_cancelled and (queued or active):
            return "Upload and render cancellation requested. Active work will stop shortly."
        if ingest_cancelled:
            return "The active upload was cancelled and partial media was removed."
        if queued or active:
            return (
                "Cancellation requested. The active render, if any, will stop shortly."
            )
        return "There is no active upload, queued job, or render to cancel."

    # --- Media extraction ---------------------------------------------------------------

    def _extract_source(self, message: dict[str, Any]) -> MediaRef | None:
        photos = message.get("photo")
        if isinstance(photos, list) and photos:
            photo = photos[-1]
            if isinstance(photo, dict) and isinstance(photo.get("file_id"), str):
                return MediaRef(
                    file_id=photo["file_id"],
                    file_size=self._optional_int(photo.get("file_size")),
                    filename=str(photo.get("file_name") or "source-photo.jpg"),
                    mime_type=str(photo.get("mime_type") or "image/jpeg"),
                    kind="image",
                )

        document = message.get("document")
        if not isinstance(document, dict) or not isinstance(
            document.get("file_id"), str
        ):
            return None
        filename = str(document.get("file_name") or "source-image")
        mime_type = str(document.get("mime_type") or "").lower()
        if (
            mime_type.startswith("image/")
            or Path(filename).suffix.lower() in _IMAGE_EXTENSIONS
        ):
            return MediaRef(
                file_id=document["file_id"],
                file_size=self._optional_int(document.get("file_size")),
                filename=filename,
                mime_type=mime_type,
                kind="image",
            )
        return None

    def _extract_video(self, message: dict[str, Any]) -> MediaRef | None:
        video = message.get("video")
        if isinstance(video, dict) and isinstance(video.get("file_id"), str):
            return MediaRef(
                file_id=video["file_id"],
                file_size=self._optional_int(video.get("file_size")),
                filename=str(video.get("file_name") or "target-video.mp4"),
                mime_type=str(video.get("mime_type") or "video/mp4"),
                kind="video",
            )

        document = message.get("document")
        if isinstance(document, dict) and isinstance(document.get("file_id"), str):
            filename = str(document.get("file_name") or "target-video")
            mime_type = str(document.get("mime_type") or "").lower()
            if (
                mime_type.startswith("video/")
                or Path(filename).suffix.lower() in _VIDEO_EXTENSIONS
            ):
                return MediaRef(
                    file_id=document["file_id"],
                    file_size=self._optional_int(document.get("file_size")),
                    filename=filename,
                    mime_type=mime_type,
                    kind="video",
                )
        return None

    # --- Text ---------------------------------------------------------------------------

    async def _reply(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
    ) -> None:
        if self.telegram is None:
            return
        try:
            await self.telegram.send_message(
                chat_id, text, reply_to_message_id=reply_to_message_id
            )
        except TelegramTransportError:
            logger.warning("Could not reply to Telegram chat_id=%s", chat_id)

    def _home_text(self, chat_id: int, *, intro: str | None = None) -> str:
        session = self.storage.get_session(chat_id)
        source_state = (
            "Saved"
            if session.source_path and session.source_path.exists()
            else "Not saved"
        )
        consent_state = "Confirmed" if session.consent else "Required"
        heading = (
            intro or "Use the buttons below to control the complete face-swap workflow."
        )
        return (
            "FACE SWAP CONTROL PANEL\n\n"
            f"{heading}\n\n"
            f"Consent: {consent_state}\n"
            f"Source image: {source_state}\n"
            f"Active or queued jobs: {self.storage.count_active_jobs(chat_id)}\n\n"
            "Recommended flow: I Agree → Set Source Image → Send Target Video"
        )

    def _status_text(self, chat_id: int) -> str:
        session = self.storage.get_session(chat_id)
        active_count = self.storage.count_active_jobs(chat_id)
        latest = self.storage.latest_job(chat_id)
        source = (
            "Saved"
            if session.source_path and session.source_path.exists()
            else "Not saved"
        )
        lines = [
            "BOT STATUS",
            "Transport: MTProto large-file mode",
            f"Connection: {'Connected' if self.telegram and self.telegram.is_connected else 'Connecting'}",
            f"Consent: {'Confirmed' if session.consent else 'Required'}",
            f"Source image: {source}",
            f"Active or queued jobs: {active_count}",
            "Application media caps: Disabled"
            if self.settings.max_video_mb == 0
            else f"Video cap: {self.settings.max_video_mb} MiB",
        ]
        if self.telegram is not None:
            rate = self.telegram.rate_status
            if rate.remaining_seconds:
                lines.append(
                    f"Telegram rate control: Paused for approximately {rate.remaining_seconds}s"
                )
            else:
                lines.append("Telegram rate control: Normal")
            lines.append(f"FloodWait events since start: {rate.flood_events}")
            lines.append(f"Deferred progress updates: {rate.dropped_noncritical}")
        if latest:
            job, error = latest
            lines.append(f"Latest job: {job.status}")
            if error and job.status == "failed":
                lines.append(
                    "Tip: Use a clear source image and ensure the target face is visible in the reference frame."
                )
        return "\n".join(lines)

    def _quality_text(self) -> str:
        return (
            "QUALITY PROFILE\n\n"
            "Processing: Every target frame is processed.\n"
            f"Face selector: {self.settings.face_selector_mode}\n"
            f"Reference frame: {self.settings.reference_frame_number}\n"
            f"Detector: {self.settings.face_detector_model} at {self.settings.face_detector_size}\n"
            f"Pixel boost: {self.settings.face_swapper_pixel_boost}\n"
            f"Masks: {' + '.join(self.settings.face_mask_types)}\n"
            f"Workflow: {self.settings.workflow_strategy}\n"
            f"Output quality: {self.settings.output_video_quality}\n"
            f"Input normalization: {'Enabled' if self.settings.normalize_input else 'Disabled'}\n\n"
            "Reference tracking is most stable when the intended target face is visible in the configured reference frame."
        )

    def _help_text(self) -> str:
        return (
            "HOW TO USE THE BOT\n\n"
            "This bot is button-first. Use /start or /menu only when you need to open the control panel.\n\n"
            "1. Select I Agree.\n"
            "2. Select Set Source Image, then send one clear face image.\n"
            "3. Select Send Target Video, then send the target video.\n"
            "4. Follow the live progress panel or use My Status / Cancel Render.\n\n"
            "The bot uses MTProto with your configured Telegram API ID and API hash for large-file transfers. "
            "Application size and duration caps are disabled by default. Physical Telegram, storage and compute limits still apply; "
            "oversized result files are sent as lossless numbered parts.\n\n"
            "Use only media you own or have permission to edit. Do not use minors, intimate media, or deceptive impersonation."
        )

    # --- Cleanup ------------------------------------------------------------------------

    async def _periodic_cleanup(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.settings.cleanup_interval_minutes * 60)
                expired = self.storage.expire_sources(
                    older_than_seconds=self.settings.source_retention_hours * 3600
                )
                for path in expired:
                    self._safe_unlink(path)
                self.storage.prune_seen_updates(older_than_seconds=7 * 24 * 3600)
                self.storage.prune_old_final_jobs(older_than_seconds=7 * 24 * 3600)
                self._prune_callback_debounce()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Periodic media cleanup failed")

    def _prune_callback_debounce(self) -> None:
        cutoff = time.monotonic() - 300
        self._callback_last_seen = {
            key: seen
            for key, seen in self._callback_last_seen.items()
            if seen >= cutoff
        }

    @staticmethod
    def _optional_int(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    def _safe_unlink(self, path: Path | None) -> None:
        if path is None:
            return
        try:
            resolved = path.resolve()
            sessions_root = (self.settings.data_dir / "sessions").resolve()
            if sessions_root not in resolved.parents:
                logger.error(
                    "Refusing to delete a path outside generated sessions: %s", path
                )
                return
            resolved.unlink(missing_ok=True)
        except OSError:
            logger.warning("Could not remove source media %s", path, exc_info=True)

    async def _remove_job_directory(self, job_dir: Path) -> None:
        try:
            jobs_root = (self.settings.data_dir / "jobs").resolve()
            resolved = job_dir.resolve()
            if jobs_root not in resolved.parents:
                logger.error(
                    "Refusing to delete a path outside generated jobs: %s", job_dir
                )
                return
            await asyncio.to_thread(shutil.rmtree, resolved, True)
        except OSError:
            logger.warning(
                "Could not remove failed job directory %s", job_dir, exc_info=True
            )

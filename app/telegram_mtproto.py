"""MTProto transport with durable FloodWait control and inline-button support."""

from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

from telethon import TelegramClient, events
from telethon.errors import MessageNotModifiedError, RPCError
from telethon.tl.custom.message import Message

from .config import Settings
from .flood_control import (
    FloodStatus,
    TelegramFloodGuard,
    TelegramFloodRetryExceeded,
    TelegramFloodWaitExceeded,
    TelegramTransientFailure,
)
from .storage import Storage

logger = logging.getLogger(__name__)
UpdateHandler = Callable[[dict[str, Any]], None]
CallbackHandler = Callable[[events.CallbackQuery.Event], None]
ProgressCallback = Callable[[int, int], Awaitable[None] | None]
_UNSET = object()
_NOT_MODIFIED = object()


class TelegramTransportError(RuntimeError):
    """A token-safe Telegram/MTProto failure for callers to handle."""


class MTProtoTelegramClient:
    """A bot-identity MTProto client; it never needs a personal phone session.

    Every Telegram operation flows through ``TelegramFloodGuard``. Important
    actions wait asynchronously for a server-mandated FloodWait and retry;
    noncritical progress edits are dropped/coalesced instead of holding the AI
    rendering pipeline hostage.
    """

    def __init__(self, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self._client: TelegramClient | None = None
        self._update_handler: UpdateHandler | None = None
        self._callback_handler: CallbackHandler | None = None
        self._media_messages: OrderedDict[str, Message] = OrderedDict()
        self._media_capacity = 512
        self._flood_guard = TelegramFloodGuard(settings, storage)
        # A progress update is never allowed to await a network RPC on the
        # FaceFusion stdout-consumer path. Keep only the newest text per message.
        self._pending_progress_edits: dict[tuple[int, int], str] = {}
        self._progress_edit_tasks: dict[tuple[int, int], asyncio.Task[None]] = {}
        self._closing = False

    @property
    def is_connected(self) -> bool:
        return bool(self._client and self._client.is_connected())

    @property
    def rate_status(self) -> FloodStatus:
        return self._flood_guard.status()

    @property
    def _telegram(self) -> TelegramClient:
        if self._client is None:
            raise RuntimeError("MTProto client has not been started")
        return self._client

    async def start(
        self,
        update_handler: UpdateHandler,
        callback_handler: CallbackHandler,
    ) -> None:
        self._closing = False
        self._update_handler = update_handler
        self._callback_handler = callback_handler
        session_path = self.settings.data_dir / "telegram-mtproto-bot"
        client = TelegramClient(
            str(session_path),
            self.settings.telegram_api_id,
            self.settings.telegram_api_hash,
            sequential_updates=True,
            # Surface every FloodWait to our durable guard instead of allowing
            # Telethon to sleep inside a random caller coroutine.
            flood_sleep_threshold=0,
            request_retries=5,
            connection_retries=5,
            retry_delay=1,
            auto_reconnect=True,
            device_model="FaceSwap Worker",
            system_version="Linux",
            app_version="2.1",
        )
        try:
            result = await self._flood_guard.run(
                lambda: client.start(bot_token=self.settings.telegram_bot_token),
                chat_id=None,
                operation_name="MTProto bot sign-in",
                category="control",
            )
            if result is None:
                raise TelegramTransportError(
                    "MTProto bot sign-in was deferred unexpectedly"
                )
        except (
            OSError,
            RPCError,
            ValueError,
            TelegramFloodWaitExceeded,
            TelegramFloodRetryExceeded,
            TelegramTransientFailure,
            TelegramTransportError,
        ) as exc:
            try:
                await client.disconnect()
            except (OSError, RPCError):
                pass
            raise TelegramTransportError(
                "Could not connect the MTProto bot client"
            ) from exc
        client.add_event_handler(self._on_new_message, events.NewMessage(incoming=True))
        client.add_event_handler(self._on_callback_query, events.CallbackQuery())
        self._client = client
        logger.info("MTProto bot client connected")

    async def close(self) -> None:
        self._closing = True
        for task in list(self._progress_edit_tasks.values()):
            task.cancel()
        if self._progress_edit_tasks:
            await asyncio.gather(
                *self._progress_edit_tasks.values(), return_exceptions=True
            )
        self._progress_edit_tasks.clear()
        self._pending_progress_edits.clear()
        if self._client is not None:
            try:
                await self._client.disconnect()
            except (OSError, RPCError):
                logger.warning("MTProto client disconnect did not complete cleanly")
            finally:
                self._client = None
        self._media_messages.clear()

    async def _on_new_message(self, event: events.NewMessage.Event) -> None:
        try:
            message = event.message
            chat_id = event.chat_id
            sender_id = event.sender_id
            if chat_id is None or sender_id is None:
                return
            payload = self._normalise_message(
                message,
                chat_id=int(chat_id),
                sender_id=int(sender_id),
                is_private=bool(event.is_private),
            )
            if payload is not None and self._update_handler is not None:
                self._update_handler(payload)
        except Exception:  # noqa: BLE001 - malformed updates must not disconnect the client
            logger.error("Unable to normalize an incoming MTProto update")

    async def _on_callback_query(self, event: events.CallbackQuery.Event) -> None:
        try:
            if self._callback_handler is not None:
                self._callback_handler(event)
        except Exception:  # noqa: BLE001 - malformed callbacks must not disconnect the client
            logger.error("Unable to route an inline-button callback")

    def _normalise_message(
        self,
        message: Message,
        *,
        chat_id: int,
        sender_id: int,
        is_private: bool,
    ) -> dict[str, Any] | None:
        normalized: dict[str, Any] = {
            "update_id": f"mtproto:{chat_id}:{message.id}",
            "message": {
                "message_id": message.id,
                "chat": {"id": chat_id, "type": "private" if is_private else "group"},
                "from": {"id": sender_id},
                "text": message.raw_text or "",
            },
        }
        container = normalized["message"]
        if not message.media:
            return normalized

        media_key = self._remember_media(message)
        file = message.file
        file_size = self._safe_int(getattr(file, "size", None))
        file_name = str(getattr(file, "name", "") or "")
        mime_type = str(getattr(file, "mime_type", "") or "").lower()

        if message.photo:
            container["photo"] = [
                {
                    "file_id": media_key,
                    "file_size": file_size,
                    "file_name": file_name or "source-photo.jpg",
                    "mime_type": mime_type or "image/jpeg",
                }
            ]
            return normalized

        if message.video or mime_type.startswith("video/"):
            container["video"] = {
                "file_id": media_key,
                "file_size": file_size,
                "file_name": file_name or "target-video.mp4",
                "mime_type": mime_type or "video/mp4",
            }
            return normalized

        if message.document:
            container["document"] = {
                "file_id": media_key,
                "file_size": file_size,
                "file_name": file_name or "attachment",
                "mime_type": mime_type,
            }
            return normalized

        self._media_messages.pop(media_key, None)
        return normalized

    def _remember_media(self, message: Message) -> str:
        key = uuid4().hex
        self._media_messages[key] = message
        self._media_messages.move_to_end(key)
        while len(self._media_messages) > self._media_capacity:
            self._media_messages.popitem(last=False)
        return key

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        try:
            return int(value) if value is not None else None
        except (TypeError, ValueError):
            return None

    async def send_message(
        self,
        chat_id: int,
        text: str,
        *,
        reply_to_message_id: int | None = None,
        buttons: Any | None = None,
    ) -> Message:
        try:
            result = await self._flood_guard.run(
                lambda: self._telegram.send_message(
                    chat_id,
                    text,
                    reply_to=reply_to_message_id,
                    buttons=buttons,
                ),
                chat_id=chat_id,
                operation_name="send message",
                category="control",
            )
            if result is None:
                raise TelegramTransportError("Message send was deferred unexpectedly")
            return result
        except (
            OSError,
            RPCError,
            TelegramFloodWaitExceeded,
            TelegramFloodRetryExceeded,
            TelegramTransientFailure,
        ) as exc:
            raise TelegramTransportError("Could not send a Telegram message") from exc

    async def edit_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Any = _UNSET,
        noncritical: bool = False,
    ) -> Message | None:
        async def operation() -> Message | None:
            try:
                kwargs = {} if buttons is _UNSET else {"buttons": buttons}
                return await self._telegram.edit_message(
                    chat_id, message_id, text, **kwargs
                )
            except MessageNotModifiedError:
                return None

        try:
            return await self._flood_guard.run(
                operation,
                chat_id=chat_id,
                operation_name="edit message",
                category="control",
                noncritical=noncritical,
            )
        except (
            OSError,
            RPCError,
            TelegramFloodWaitExceeded,
            TelegramFloodRetryExceeded,
            TelegramTransientFailure,
        ) as exc:
            if noncritical:
                return None
            raise TelegramTransportError("Could not update a Telegram message") from exc

    async def edit_menu_message(
        self,
        chat_id: int,
        message_id: int,
        text: str,
        *,
        buttons: Any,
    ) -> str:
        """Return sent/unchanged/deferred without ever blocking a button action."""

        async def operation() -> Message | object:
            try:
                return await self._telegram.edit_message(
                    chat_id,
                    message_id,
                    text,
                    buttons=buttons,
                )
            except MessageNotModifiedError:
                return _NOT_MODIFIED

        try:
            result = await self._flood_guard.run(
                operation,
                chat_id=chat_id,
                operation_name="edit control panel",
                category="control",
                noncritical=True,
            )
        except (
            OSError,
            RPCError,
            TelegramFloodWaitExceeded,
            TelegramFloodRetryExceeded,
            TelegramTransientFailure,
        ):
            return "deferred"
        if result is None:
            return "deferred"
        if result is _NOT_MODIFIED:
            return "unchanged"
        return "sent"

    def queue_progress_edit(self, chat_id: int, message_id: int, text: str) -> None:
        """Coalesce a noncritical progress edit without blocking the render loop."""
        if self._closing:
            return
        key = (chat_id, message_id)
        self._pending_progress_edits[key] = text
        task = self._progress_edit_tasks.get(key)
        if task is None or task.done():
            task = asyncio.create_task(
                self._drain_progress_edit(key),
                name=f"telegram-progress-edit-{chat_id}-{message_id}",
            )
            self._progress_edit_tasks[key] = task

    async def _drain_progress_edit(self, key: tuple[int, int]) -> None:
        try:
            while True:
                text = self._pending_progress_edits.pop(key, None)
                if text is None:
                    return
                try:
                    await self.edit_message(
                        key[0],
                        key[1],
                        text,
                        noncritical=True,
                    )
                except TelegramTransportError:
                    # Progress is intentionally best-effort. The next update will
                    # coalesce into a fresh task after network/rate recovery.
                    return
                # If no new text arrived while the RPC was in flight, stop. This
                # prevents retries in a tight loop during FloodWait.
                if key not in self._pending_progress_edits:
                    return
        finally:
            self._progress_edit_tasks.pop(key, None)
            # A new progress value could have arrived immediately before the task
            # was removed. Start one fresh drain in that rare race.
            if (
                key in self._pending_progress_edits
                and not self._stopping_for_progress()
            ):
                self.queue_progress_edit(
                    key[0], key[1], self._pending_progress_edits[key]
                )

    def _stopping_for_progress(self) -> bool:
        return self._closing

    async def answer_callback(
        self,
        event: events.CallbackQuery.Event,
        *,
        text: str | None = None,
        alert: bool = False,
    ) -> None:
        """Acknowledge the tap without allowing a callback spinner to block work."""
        try:
            await self._flood_guard.run(
                lambda: event.answer(text or "", alert=alert),
                chat_id=int(event.chat_id) if event.chat_id is not None else None,
                operation_name="answer callback",
                # Callback acknowledgement is tiny and must clear the client's
                # spinning indicator immediately. It still honors any actual
                # server FloodWait deadline and callback debounce in BotService.
                category="callback",
                noncritical=True,
            )
        except Exception:  # noqa: BLE001 - callback acknowledgement is best-effort
            return

    async def safe_send_message(
        self, chat_id: int, text: str, *, buttons: Any | None = None
    ) -> None:
        try:
            await self.send_message(chat_id, text, buttons=buttons)
        except TelegramTransportError:
            logger.warning("Unable to send a Telegram message to chat_id=%s", chat_id)

    async def download_file(
        self,
        media_key: str,
        destination: Path,
        *,
        max_bytes: int = 0,
        progress_callback: ProgressCallback | None = None,
    ) -> None:
        message = self._media_messages.pop(media_key, None)
        if message is None:
            raise TelegramTransportError(
                "The uploaded media reference expired. Please send the file again."
            )
        expected_size = self._safe_int(getattr(message.file, "size", None))
        if max_bytes > 0 and expected_size is not None and expected_size > max_bytes:
            raise TelegramTransportError(
                "The uploaded file exceeds this bot's configured size limit"
            )

        destination.parent.mkdir(parents=True, exist_ok=True)

        async def operation() -> None:
            with destination.open("wb") as handle:
                await self._telegram.download_media(
                    message,
                    file=handle,
                    progress_callback=progress_callback,
                )
            if max_bytes > 0 and destination.stat().st_size > max_bytes:
                raise TelegramTransportError(
                    "The downloaded file exceeds this bot's configured size limit"
                )

        try:
            await self._flood_guard.run(
                operation,
                chat_id=None,
                operation_name="download media",
                category="transfer",
                transient_retries=min(2, self.settings.telegram_transient_retries),
            )
        except asyncio.CancelledError:
            destination.unlink(missing_ok=True)
            raise
        except TelegramTransportError:
            destination.unlink(missing_ok=True)
            raise
        except (
            OSError,
            RPCError,
            ValueError,
            TelegramFloodWaitExceeded,
            TelegramFloodRetryExceeded,
            TelegramTransientFailure,
        ) as exc:
            destination.unlink(missing_ok=True)
            raise TelegramTransportError(
                "Could not download the Telegram media"
            ) from exc

    async def send_video(
        self,
        chat_id: int,
        path: Path,
        caption: str,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> Message:
        return await self._send_file(
            chat_id,
            path,
            caption,
            force_document=False,
            supports_streaming=True,
            progress_callback=progress_callback,
            operation_name="upload result video",
        )

    async def send_document(
        self,
        chat_id: int,
        path: Path,
        caption: str,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> Message:
        return await self._send_file(
            chat_id,
            path,
            caption,
            force_document=True,
            supports_streaming=False,
            progress_callback=progress_callback,
            operation_name="upload result document",
        )

    async def _send_file(
        self,
        chat_id: int,
        path: Path,
        caption: str,
        *,
        force_document: bool,
        supports_streaming: bool,
        progress_callback: ProgressCallback | None,
        operation_name: str,
    ) -> Message:
        try:
            result = await self._flood_guard.run(
                lambda: self._telegram.send_file(
                    chat_id,
                    str(path),
                    caption=caption,
                    force_document=force_document,
                    supports_streaming=supports_streaming,
                    progress_callback=progress_callback,
                ),
                chat_id=chat_id,
                operation_name=operation_name,
                category="transfer",
                transient_retries=min(2, self.settings.telegram_transient_retries),
            )
            if result is None:
                raise TelegramTransportError("File upload was deferred unexpectedly")
            return result
        except (
            OSError,
            RPCError,
            ValueError,
            TelegramFloodWaitExceeded,
            TelegramFloodRetryExceeded,
            TelegramTransientFailure,
        ) as exc:
            raise TelegramTransportError(
                "Could not upload the result file to Telegram"
            ) from exc

    @staticmethod
    def message_id(message: Any) -> int | None:
        value = getattr(message, "id", None)
        return int(value) if isinstance(value, int) else None

"""Persistent, non-blocking Telegram FloodWait and transient-error control.

The renderer must continue even if Telegram asks the bot to wait. This module
keeps outbound control calls below conservative pacing thresholds, records a
server-mandated FloodWait deadline in SQLite, retries important calls with the
exact requested wait, and drops/coalesces noncritical progress edits instead of
blocking FaceFusion output consumption.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TypeVar

from telethon.errors import FloodWaitError, RPCError, ServerError, TimedOutError

from .config import Settings
from .storage import Storage

logger = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class FloodStatus:
    blocked_until_epoch: float
    flood_events: int
    transient_retries: int
    dropped_noncritical: int

    @property
    def remaining_seconds(self) -> int:
        return max(0, round(self.blocked_until_epoch - time.time()))


class TelegramFloodGuard:
    """Rate gate shared by all MTProto operations for one bot session.

    ``control`` operations (messages, button acknowledgements and edits) are
    proactively paced globally and per chat. ``transfer`` operations are not
    serialized behind a long upload, but still honor a persisted global
    FloodWait deadline. Noncritical progress edits never sleep: they return
    immediately when the gate is busy, preventing rendering from stalling.
    """

    _STATE_KEY = "telegram_flood_until_epoch"

    def __init__(self, settings: Settings, storage: Storage) -> None:
        self.settings = settings
        self.storage = storage
        self._lock = asyncio.Lock()
        self._global_next = 0.0
        self._chat_next: dict[int, float] = {}
        self._blocked_until_epoch = storage.get_runtime_float(self._STATE_KEY)
        self._flood_events = 0
        self._transient_retries = 0
        self._dropped_noncritical = 0

    def status(self) -> FloodStatus:
        return FloodStatus(
            blocked_until_epoch=self._blocked_until_epoch,
            flood_events=self._flood_events,
            transient_retries=self._transient_retries,
            dropped_noncritical=self._dropped_noncritical,
        )

    async def run(
        self,
        operation: Callable[[], Awaitable[T]],
        *,
        chat_id: int | None,
        operation_name: str,
        category: str = "control",
        noncritical: bool = False,
        transient_retries: int | None = None,
    ) -> T | None:
        """Execute an MTProto operation without violating FloodWait deadlines.

        Important operations await Telegram's exact retry deadline asynchronously;
        no event-loop thread is blocked. Noncritical progress updates return
        ``None`` immediately whenever throttling, a flood wait, or a transient
        failure would delay them.
        """
        persisted_wait = self.status().remaining_seconds
        if persisted_wait > self.settings.telegram_max_flood_wait_seconds:
            if noncritical:
                self._dropped_noncritical += 1
                return None
            raise TelegramFloodWaitExceeded(persisted_wait, operation_name)

        flood_attempt = 0
        transient_attempt = 0
        transient_limit = (
            self.settings.telegram_transient_retries
            if transient_retries is None
            else max(0, transient_retries)
        )

        while True:
            current_wait = self.status().remaining_seconds
            if current_wait > self.settings.telegram_max_flood_wait_seconds:
                if noncritical:
                    self._dropped_noncritical += 1
                    return None
                raise TelegramFloodWaitExceeded(current_wait, operation_name)
            allowed = await self._acquire_turn(
                chat_id=chat_id,
                category=category,
                noncritical=noncritical,
            )
            if not allowed:
                return None
            try:
                return await operation()
            except FloodWaitError as exc:
                flood_attempt += 1
                wait_seconds = max(1, int(exc.seconds))
                await self._record_flood_wait(wait_seconds, operation_name)
                if noncritical:
                    self._dropped_noncritical += 1
                    return None
                if wait_seconds > self.settings.telegram_max_flood_wait_seconds:
                    raise TelegramFloodWaitExceeded(
                        wait_seconds, operation_name
                    ) from exc
                if flood_attempt > self.settings.telegram_max_flood_retries:
                    raise TelegramFloodRetryExceeded(operation_name) from exc
                # The next loop's acquire_turn observes the durable deadline.
                continue
            except (OSError, asyncio.TimeoutError, ServerError, TimedOutError) as exc:
                if noncritical:
                    self._dropped_noncritical += 1
                    return None
                transient_attempt += 1
                if transient_attempt > transient_limit:
                    raise TelegramTransientFailure(operation_name) from exc
                self._transient_retries += 1
                delay = min(
                    60.0,
                    self.settings.telegram_retry_base_seconds
                    * (2 ** (transient_attempt - 1)),
                ) + random.uniform(0.0, 0.25)
                logger.warning(
                    "Transient Telegram failure for %s; retrying in %.2fs (%s/%s)",
                    operation_name,
                    delay,
                    transient_attempt,
                    transient_limit,
                )
                await asyncio.sleep(delay)
            except RPCError:
                # Permission, invalid-media and message-content RPC errors are
                # deterministic. Retrying them increases API pressure.
                raise

    async def _acquire_turn(
        self,
        *,
        chat_id: int | None,
        category: str,
        noncritical: bool,
    ) -> bool:
        while True:
            now_monotonic = time.monotonic()
            now_epoch = time.time()
            async with self._lock:
                blocked_delay = max(0.0, self._blocked_until_epoch - now_epoch)
                if category == "control":
                    global_delay = max(0.0, self._global_next - now_monotonic)
                    chat_delay = (
                        max(0.0, self._chat_next.get(chat_id, 0.0) - now_monotonic)
                        if chat_id is not None
                        else 0.0
                    )
                else:
                    global_delay = 0.0
                    chat_delay = 0.0
                delay = max(blocked_delay, global_delay, chat_delay)
                if delay <= 0:
                    if category == "control":
                        now = time.monotonic()
                        self._global_next = (
                            now + self.settings.telegram_global_action_interval_seconds
                        )
                        if chat_id is not None:
                            self._chat_next[chat_id] = (
                                now
                                + self.settings.telegram_chat_action_interval_seconds
                            )
                    return True

            if noncritical:
                return False
            # A bounded sleep keeps shutdown cancellation responsive and lets us
            # re-read a later persisted FloodWait extension.
            await asyncio.sleep(min(delay, 30.0))

    async def _record_flood_wait(self, seconds: int, operation_name: str) -> None:
        # Add tiny jitter after the exact Telegram deadline to avoid synchronized
        # retries when several tasks received the same error simultaneously.
        until = time.time() + seconds + random.uniform(0.15, 0.75)
        async with self._lock:
            self._blocked_until_epoch = max(self._blocked_until_epoch, until)
            persisted = self._blocked_until_epoch
        self.storage.set_runtime_float(self._STATE_KEY, persisted)
        self._flood_events += 1
        logger.warning(
            "Telegram FloodWait for %s; outbound requests paused for approximately %ss",
            operation_name,
            seconds,
        )


class TelegramFloodWaitExceeded(RuntimeError):
    def __init__(self, seconds: int, operation_name: str) -> None:
        super().__init__(
            f"Telegram requested a {seconds}s FloodWait during {operation_name}"
        )
        self.seconds = seconds
        self.operation_name = operation_name


class TelegramFloodRetryExceeded(RuntimeError):
    def __init__(self, operation_name: str) -> None:
        super().__init__(f"FloodWait retry budget exhausted during {operation_name}")
        self.operation_name = operation_name


class TelegramTransientFailure(RuntimeError):
    def __init__(self, operation_name: str) -> None:
        super().__init__(
            f"Transient Telegram retry budget exhausted during {operation_name}"
        )
        self.operation_name = operation_name

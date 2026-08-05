"""SQLite state for sessions, MTProto update deduplication, and durable jobs."""

from __future__ import annotations

import sqlite3
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from .models import Job, JobStatus, Session

_FINAL_STATUSES = ("completed", "failed", "cancelled")


class Storage:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self.db_path = data_dir / "bot.sqlite3"
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def open(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        with self._lock:
            if self._connection is not None:
                return
            connection = sqlite3.connect(
                self.db_path, timeout=30, check_same_thread=False
            )
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA journal_mode=WAL")
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA busy_timeout=30000")
            self._connection = connection
            self._create_schema_and_migrate()

    def close(self) -> None:
        with self._lock:
            if self._connection is not None:
                self._connection.close()
                self._connection = None

    @property
    def _conn(self) -> sqlite3.Connection:
        if self._connection is None:
            raise RuntimeError("Storage has not been opened")
        return self._connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        with self._lock:
            try:
                yield self._conn
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def _create_schema_and_migrate(self) -> None:
        with self._transaction() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS sessions (
                    chat_id INTEGER PRIMARY KEY,
                    consent INTEGER NOT NULL DEFAULT 0,
                    source_path TEXT,
                    source_updated_at REAL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    user_id INTEGER NOT NULL,
                    source_path TEXT NOT NULL,
                    target_path TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    error TEXT,
                    progress_message_id INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_jobs_chat_status
                    ON jobs(chat_id, status, created_at);
                CREATE INDEX IF NOT EXISTS idx_jobs_status_created
                    ON jobs(status, created_at);
                """
            )

            job_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(jobs)")
            }
            if "progress_message_id" not in job_columns:
                conn.execute("ALTER TABLE jobs ADD COLUMN progress_message_id INTEGER")

            # V1 used an INTEGER update_id. MTProto uses an opaque chat/message
            # key, so recreate this tiny non-critical deduplication table once.
            update_columns = {
                row["name"] for row in conn.execute("PRAGMA table_info(seen_updates)")
            }
            if update_columns and "update_key" not in update_columns:
                conn.execute("DROP TABLE seen_updates")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS seen_updates (
                    update_key TEXT PRIMARY KEY,
                    received_at REAL NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runtime_state (
                    state_key TEXT PRIMARY KEY,
                    state_value TEXT NOT NULL,
                    updated_at REAL NOT NULL
                )
                """
            )

    # --- Durable runtime state ----------------------------------------------------------

    def get_runtime_float(self, state_key: str, default: float = 0.0) -> float:
        with self._lock:
            row = self._conn.execute(
                "SELECT state_value FROM runtime_state WHERE state_key = ?",
                (state_key,),
            ).fetchone()
        if row is None:
            return default
        try:
            return float(row["state_value"])
        except (TypeError, ValueError):
            return default

    def set_runtime_float(self, state_key: str, value: float) -> None:
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO runtime_state(state_key, state_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(state_key) DO UPDATE SET
                    state_value = excluded.state_value,
                    updated_at = excluded.updated_at
                """,
                (state_key, str(value), time.time()),
            )

    # --- Update deduplication -----------------------------------------------------------

    def remember_update(self, update_key: str | int) -> bool:
        """Return True only the first time a message/update key is observed."""
        with self._transaction() as conn:
            result = conn.execute(
                "INSERT OR IGNORE INTO seen_updates(update_key, received_at) VALUES (?, ?)",
                (str(update_key), time.time()),
            )
            return result.rowcount == 1

    def prune_seen_updates(self, *, older_than_seconds: int) -> int:
        with self._transaction() as conn:
            result = conn.execute(
                "DELETE FROM seen_updates WHERE received_at < ?",
                (time.time() - older_than_seconds,),
            )
            return result.rowcount

    # --- Sessions -----------------------------------------------------------------------

    def get_session(self, chat_id: int) -> Session:
        with self._lock:
            row = self._conn.execute(
                "SELECT consent, source_path, source_updated_at FROM sessions WHERE chat_id = ?",
                (chat_id,),
            ).fetchone()
        if row is None:
            return Session(
                chat_id=chat_id, consent=False, source_path=None, source_updated_at=None
            )
        return Session(
            chat_id=chat_id,
            consent=bool(row["consent"]),
            source_path=Path(row["source_path"]) if row["source_path"] else None,
            source_updated_at=row["source_updated_at"],
        )

    def set_consent(self, chat_id: int, consent: bool) -> None:
        now = time.time()
        with self._transaction() as conn:
            conn.execute(
                """
                INSERT INTO sessions(chat_id, consent, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    consent = excluded.consent,
                    updated_at = excluded.updated_at
                """,
                (chat_id, int(consent), now),
            )

    def set_source(self, chat_id: int, source_path: Path) -> Path | None:
        now = time.time()
        with self._transaction() as conn:
            previous = conn.execute(
                "SELECT source_path FROM sessions WHERE chat_id = ?", (chat_id,)
            ).fetchone()
            conn.execute(
                """
                INSERT INTO sessions(chat_id, consent, source_path, source_updated_at, updated_at)
                VALUES (?, 0, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    source_path = excluded.source_path,
                    source_updated_at = excluded.source_updated_at,
                    updated_at = excluded.updated_at
                """,
                (chat_id, str(source_path), now, now),
            )
        return (
            Path(previous["source_path"])
            if previous and previous["source_path"]
            else None
        )

    def reset_session(self, chat_id: int) -> Path | None:
        now = time.time()
        with self._transaction() as conn:
            previous = conn.execute(
                "SELECT source_path FROM sessions WHERE chat_id = ?", (chat_id,)
            ).fetchone()
            conn.execute(
                """
                INSERT INTO sessions(chat_id, consent, source_path, source_updated_at, updated_at)
                VALUES (?, 0, NULL, NULL, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    consent = 0,
                    source_path = NULL,
                    source_updated_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (chat_id, now),
            )
        return (
            Path(previous["source_path"])
            if previous and previous["source_path"]
            else None
        )

    def expire_sources(self, *, older_than_seconds: int) -> list[Path]:
        cutoff = time.time() - older_than_seconds
        with self._transaction() as conn:
            rows = conn.execute(
                """
                SELECT source_path FROM sessions
                WHERE source_path IS NOT NULL AND source_updated_at IS NOT NULL
                  AND source_updated_at < ?
                """,
                (cutoff,),
            ).fetchall()
            conn.execute(
                """
                UPDATE sessions
                SET source_path = NULL, source_updated_at = NULL, updated_at = ?
                WHERE source_path IS NOT NULL AND source_updated_at IS NOT NULL
                  AND source_updated_at < ?
                """,
                (time.time(), cutoff),
            )
        return [Path(row["source_path"]) for row in rows]

    # --- Jobs ---------------------------------------------------------------------------

    def create_job(self, job: Job, *, max_queued_jobs: int = 0) -> bool:
        """Create a job and atomically enforce a nonzero queue cap, if configured."""
        with self._transaction() as conn:
            if max_queued_jobs > 0:
                queued = conn.execute(
                    "SELECT COUNT(*) AS count FROM jobs WHERE status = 'queued'"
                ).fetchone()
                if int(queued["count"]) >= max_queued_jobs:
                    return False
            conn.execute(
                """
                INSERT INTO jobs(
                    job_id, chat_id, user_id, source_path, target_path, status, created_at,
                    progress_message_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    job.job_id,
                    job.chat_id,
                    job.user_id,
                    str(job.source_path),
                    str(job.target_path),
                    job.status,
                    job.created_at,
                    job.progress_message_id,
                ),
            )
            return True

    @staticmethod
    def _row_to_job(row: sqlite3.Row) -> Job:
        return Job(
            job_id=row["job_id"],
            chat_id=int(row["chat_id"]),
            user_id=int(row["user_id"]),
            source_path=Path(row["source_path"]),
            target_path=Path(row["target_path"]),
            status=row["status"],  # type: ignore[arg-type]
            created_at=float(row["created_at"]),
            progress_message_id=row["progress_message_id"],
        )

    def get_job(self, job_id: str) -> Job | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._row_to_job(row) if row else None

    def list_queued_jobs(self) -> list[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status = 'queued' ORDER BY created_at ASC"
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def list_final_jobs(self) -> list[Job]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status IN ('completed', 'failed', 'cancelled')"
            ).fetchall()
        return [self._row_to_job(row) for row in rows]

    def count_active_jobs(self, chat_id: int) -> int:
        with self._lock:
            row = self._conn.execute(
                """
                SELECT COUNT(*) AS count FROM jobs
                WHERE chat_id = ? AND status IN ('queued', 'running')
                """,
                (chat_id,),
            ).fetchone()
        return int(row["count"])

    def count_queued_jobs(self) -> int:
        with self._lock:
            row = self._conn.execute(
                "SELECT COUNT(*) AS count FROM jobs WHERE status = 'queued'"
            ).fetchone()
        return int(row["count"])

    def queue_position(self, job_id: str) -> int | None:
        with self._lock:
            current = self._conn.execute(
                "SELECT created_at, status FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if current is None or current["status"] != "queued":
                return None
            row = self._conn.execute(
                "SELECT COUNT(*) AS count FROM jobs WHERE status = 'queued' AND created_at <= ?",
                (current["created_at"],),
            ).fetchone()
        return int(row["count"])

    def mark_running(self, job_id: str) -> bool:
        with self._transaction() as conn:
            result = conn.execute(
                """
                UPDATE jobs
                SET status = 'running', started_at = ?, error = NULL
                WHERE job_id = ? AND status = 'queued'
                """,
                (time.time(), job_id),
            )
            return result.rowcount == 1

    def set_job_status(
        self, job_id: str, status: JobStatus, *, error: str | None = None
    ) -> None:
        if status not in {"queued", "running", *_FINAL_STATUSES}:
            raise ValueError(f"Unsupported job status: {status}")
        with self._transaction() as conn:
            if status in _FINAL_STATUSES:
                conn.execute(
                    """
                    UPDATE jobs SET status = ?, error = ?, finished_at = ?
                    WHERE job_id = ?
                    """,
                    (status, error, time.time(), job_id),
                )
            else:
                conn.execute(
                    "UPDATE jobs SET status = ?, error = ? WHERE job_id = ?",
                    (status, error, job_id),
                )

    def cancel_queued_jobs(self, chat_id: int) -> int:
        with self._transaction() as conn:
            result = conn.execute(
                """
                UPDATE jobs
                SET status = 'cancelled', error = 'Cancelled by user', finished_at = ?
                WHERE chat_id = ? AND status = 'queued'
                """,
                (time.time(), chat_id),
            )
            return result.rowcount

    def is_cancelled(self, job_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT status FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return bool(row and row["status"] == "cancelled")

    def latest_job(self, chat_id: int) -> tuple[Job, str | None] | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM jobs WHERE chat_id = ? ORDER BY created_at DESC LIMIT 1",
                (chat_id,),
            ).fetchone()
        return (self._row_to_job(row), row["error"]) if row else None

    def mark_running_jobs_interrupted(self) -> list[Job]:
        with self._transaction() as conn:
            rows = conn.execute(
                "SELECT * FROM jobs WHERE status = 'running'"
            ).fetchall()
            conn.execute(
                """
                UPDATE jobs
                SET status = 'failed', error = 'Service restarted while this job was running',
                    finished_at = ?
                WHERE status = 'running'
                """,
                (time.time(),),
            )
        return [self._row_to_job(row) for row in rows]

    def prune_old_final_jobs(self, *, older_than_seconds: int) -> int:
        with self._transaction() as conn:
            result = conn.execute(
                """
                DELETE FROM jobs
                WHERE status IN ('completed', 'failed', 'cancelled')
                  AND COALESCE(finished_at, created_at) < ?
                """,
                (time.time() - older_than_seconds,),
            )
            return result.rowcount

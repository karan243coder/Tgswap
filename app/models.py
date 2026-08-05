"""Small data objects shared by the MTProto, storage, service and worker layers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Literal

JobStatus = Literal["queued", "running", "completed", "failed", "cancelled"]
MediaKind = Literal["image", "video"]


@dataclass(frozen=True, slots=True)
class MediaRef:
    """An opaque MTProto media key plus metadata extracted from an incoming message."""

    file_id: str
    file_size: int | None
    filename: str
    mime_type: str
    kind: MediaKind


@dataclass(frozen=True, slots=True)
class Session:
    chat_id: int
    consent: bool
    source_path: Path | None
    source_updated_at: float | None


@dataclass(frozen=True, slots=True)
class Job:
    job_id: str
    chat_id: int
    user_id: int
    source_path: Path
    target_path: Path
    status: JobStatus
    created_at: float
    progress_message_id: int | None = None


@dataclass(slots=True)
class ActiveJob:
    """The one external FaceFusion/FFmpeg process owned by this worker."""

    job: Job
    process: object | None = None

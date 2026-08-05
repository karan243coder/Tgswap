"""Environment-backed configuration for the high-capacity MTProto bot.

The service deliberately exposes its HTTP health endpoint even when Telegram
credentials are missing. This lets Koyeb diagnose a container without exposing a
bot token or API hash. The MTProto bot starts only when all three Telegram
credentials are present.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final

_API_HASH_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Fa-f0-9]{32}$")
_MODEL_RE: Final[re.Pattern[str]] = re.compile(r"^[a-z0-9_]+$")
_PIXEL_BOOST_RE: Final[re.Pattern[str]] = re.compile(r"^[1-9][0-9]*x[1-9][0-9]*$")
_ALLOWED_PROVIDERS: Final[set[str]] = {
    "cpu",
    "cuda",
    "tensorrt",
    "rocm",
    "openvino",
    "coreml",
    "directml",
}
_ALLOWED_SELECTOR_MODES: Final[set[str]] = {"one", "many", "reference"}
_ALLOWED_MASK_TYPES: Final[set[str]] = {"box", "occlusion", "area", "region"}
_ALLOWED_WORKFLOW_STRATEGIES: Final[set[str]] = {"memory", "disk"}
_ALLOWED_DETECTORS: Final[set[str]] = {
    "many",
    "retinaface",
    "scrfd",
    "yolo_face",
    "yunet",
}


class ConfigError(ValueError):
    """Raised when an environment setting is malformed or unsafe."""


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ConfigError(f"{name} must be true or false")


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value


def _env_float(name: str, default: float, *, minimum: float, maximum: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise ConfigError(f"{name} must be a number") from exc
    if not minimum <= value <= maximum:
        raise ConfigError(f"{name} must be between {minimum} and {maximum}")
    return value


def _csv_ints(name: str) -> frozenset[int]:
    raw = os.getenv(name, "").strip()
    if not raw:
        return frozenset()
    values: set[int] = set()
    for item in raw.split(","):
        try:
            value = int(item.strip())
        except ValueError as exc:
            raise ConfigError(
                f"{name} must be a comma-separated list of numeric Telegram IDs"
            ) from exc
        if value <= 0:
            raise ConfigError(f"{name} may contain only positive Telegram IDs")
        values.add(value)
    return frozenset(values)


def _safe_path(value: str) -> Path:
    return Path(value).expanduser().resolve()


@dataclass(frozen=True, slots=True)
class Settings:
    # Telegram MTProto. api_id/api_hash are registered at my.telegram.org/apps.
    telegram_bot_token: str
    telegram_api_id: int
    telegram_api_hash: str
    allowed_user_ids: frozenset[int]
    allow_groups: bool

    # Files, retention and queue. Zero means "no application-enforced cap".
    data_dir: Path
    max_video_mb: int
    max_image_mb: int
    max_video_seconds: int
    max_video_side: int
    max_jobs_per_user: int
    queue_max_size: int
    job_timeout_seconds: int
    workspace_headroom_mb: int
    source_retention_hours: int
    cleanup_interval_minutes: int
    keep_job_artifacts: bool
    telegram_upload_part_mb: int
    split_large_results: bool

    # FaceFusion / FFmpeg high-quality frame-by-frame configuration.
    facefusion_entrypoint: str
    python_executable: str
    ffmpeg_path: str
    ffprobe_path: str
    execution_provider: str
    execution_threads: int
    facefusion_model: str
    face_swapper_pixel_boost: str
    face_selector_mode: str
    reference_frame_number: int
    reference_face_position: int
    reference_face_distance: float
    face_tracker_score: float
    face_detector_model: str
    face_detector_size: str
    face_detector_score: float
    face_mask_types: tuple[str, ...]
    workflow_strategy: str
    normalize_input: bool
    output_video_quality: int
    output_video_preset: str
    normalize_crf: int

    # UX, output and Telegram flood-control behaviour.
    watermark_output: bool
    watermark_text: str
    progress_edit_seconds: float
    telegram_global_action_interval_seconds: float
    telegram_chat_action_interval_seconds: float
    telegram_max_flood_wait_seconds: int
    telegram_max_flood_retries: int
    telegram_transient_retries: int
    telegram_retry_base_seconds: float
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        execution_provider = os.getenv("EXECUTION_PROVIDER", "cpu").strip().lower()
        if execution_provider not in _ALLOWED_PROVIDERS:
            raise ConfigError(
                "EXECUTION_PROVIDER must be one of "
                + ", ".join(sorted(_ALLOWED_PROVIDERS))
            )

        model = os.getenv("FACEFUSION_MODEL", "hyperswap_1a_256").strip().lower()
        if not _MODEL_RE.fullmatch(model):
            raise ConfigError(
                "FACEFUSION_MODEL may contain only lowercase letters, digits and underscores"
            )

        pixel_boost = os.getenv("FACE_SWAPPER_PIXEL_BOOST", "512x512").strip().lower()
        if not _PIXEL_BOOST_RE.fullmatch(pixel_boost):
            raise ConfigError("FACE_SWAPPER_PIXEL_BOOST must look like 512x512")

        selector_mode = os.getenv("FACE_SELECTOR_MODE", "reference").strip().lower()
        if selector_mode not in _ALLOWED_SELECTOR_MODES:
            raise ConfigError("FACE_SELECTOR_MODE must be one, many or reference")

        detector_model = os.getenv("FACE_DETECTOR_MODEL", "yolo_face").strip().lower()
        if detector_model not in _ALLOWED_DETECTORS:
            raise ConfigError(
                "FACE_DETECTOR_MODEL is not supported by the pinned FaceFusion version"
            )
        detector_size = os.getenv("FACE_DETECTOR_SIZE", "640x640").strip().lower()
        if not _PIXEL_BOOST_RE.fullmatch(detector_size):
            raise ConfigError("FACE_DETECTOR_SIZE must look like 640x640")

        raw_mask_types = os.getenv("FACE_MASK_TYPES", "box occlusion").split()
        if not raw_mask_types:
            raw_mask_types = ["box"]
        if any(item not in _ALLOWED_MASK_TYPES for item in raw_mask_types):
            raise ConfigError(
                "FACE_MASK_TYPES may contain: " + ", ".join(sorted(_ALLOWED_MASK_TYPES))
            )

        workflow_strategy = os.getenv("WORKFLOW_STRATEGY", "memory").strip().lower()
        if workflow_strategy not in _ALLOWED_WORKFLOW_STRATEGIES:
            raise ConfigError("WORKFLOW_STRATEGY must be memory or disk")

        watermark_text = os.getenv("WATERMARK_TEXT", "bimbo").strip()
        if len(watermark_text) > 80:
            raise ConfigError("WATERMARK_TEXT must be at most 80 characters")

        api_hash = os.getenv("TELEGRAM_API_HASH", "").strip()
        if api_hash and not _API_HASH_RE.fullmatch(api_hash):
            raise ConfigError(
                "TELEGRAM_API_HASH must be the 32-character hexadecimal value from my.telegram.org"
            )

        log_level = os.getenv("LOG_LEVEL", "INFO").strip().upper()
        if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
            raise ConfigError("LOG_LEVEL must be DEBUG, INFO, WARNING or ERROR")

        return cls(
            telegram_bot_token=os.getenv("TELEGRAM_BOT_TOKEN", "").strip(),
            telegram_api_id=_env_int(
                "TELEGRAM_API_ID", 0, minimum=0, maximum=2_147_483_647
            ),
            telegram_api_hash=api_hash,
            allowed_user_ids=_csv_ints("ALLOWED_USER_IDS"),
            allow_groups=_env_bool("ALLOW_GROUPS", False),
            data_dir=_safe_path(os.getenv("DATA_DIR", "/data")),
            max_video_mb=_env_int("MAX_VIDEO_MB", 0, minimum=0, maximum=2_000_000),
            max_image_mb=_env_int("MAX_IMAGE_MB", 0, minimum=0, maximum=2_000_000),
            max_video_seconds=_env_int(
                "MAX_VIDEO_SECONDS", 0, minimum=0, maximum=604_800
            ),
            max_video_side=_env_int("MAX_VIDEO_SIDE", 0, minimum=0, maximum=16_384),
            max_jobs_per_user=_env_int(
                "MAX_JOBS_PER_USER", 0, minimum=0, maximum=10_000
            ),
            queue_max_size=_env_int("QUEUE_MAX_SIZE", 0, minimum=0, maximum=100_000),
            job_timeout_seconds=_env_int(
                "JOB_TIMEOUT_SECONDS", 0, minimum=0, maximum=604_800
            ),
            workspace_headroom_mb=_env_int(
                "WORKSPACE_HEADROOM_MB", 1024, minimum=128, maximum=1_000_000
            ),
            source_retention_hours=_env_int(
                "SOURCE_RETENTION_HOURS", 24, minimum=1, maximum=720
            ),
            cleanup_interval_minutes=_env_int(
                "CLEANUP_INTERVAL_MINUTES", 60, minimum=5, maximum=1440
            ),
            keep_job_artifacts=_env_bool("KEEP_JOB_ARTIFACTS", False),
            # Telegram has a real per-upload limit. 1900 MiB leaves protocol headroom.
            telegram_upload_part_mb=_env_int(
                "TELEGRAM_UPLOAD_PART_MB", 1900, minimum=100, maximum=2000
            ),
            split_large_results=_env_bool("SPLIT_LARGE_RESULTS", True),
            facefusion_entrypoint=os.getenv(
                "FACEFUSION_ENTRYPOINT", "/facefusion/facefusion.py"
            ).strip(),
            python_executable=os.getenv("PYTHON_EXECUTABLE", "python").strip(),
            ffmpeg_path=os.getenv("FFMPEG_PATH", "ffmpeg").strip(),
            ffprobe_path=os.getenv("FFPROBE_PATH", "ffprobe").strip(),
            execution_provider=execution_provider,
            execution_threads=_env_int("EXECUTION_THREADS", 2, minimum=1, maximum=64),
            facefusion_model=model,
            face_swapper_pixel_boost=pixel_boost,
            face_selector_mode=selector_mode,
            reference_frame_number=_env_int(
                "REFERENCE_FRAME_NUMBER", 0, minimum=0, maximum=10_000_000
            ),
            reference_face_position=_env_int(
                "REFERENCE_FACE_POSITION", 0, minimum=0, maximum=100
            ),
            reference_face_distance=_env_float(
                "REFERENCE_FACE_DISTANCE", 0.3, minimum=0.05, maximum=1.0
            ),
            face_tracker_score=_env_float(
                "FACE_TRACKER_SCORE", 0.0, minimum=0.0, maximum=1.0
            ),
            face_detector_model=detector_model,
            face_detector_size=detector_size,
            face_detector_score=_env_float(
                "FACE_DETECTOR_SCORE", 0.5, minimum=0.0, maximum=0.99
            ),
            face_mask_types=tuple(raw_mask_types),
            workflow_strategy=workflow_strategy,
            normalize_input=_env_bool("NORMALIZE_INPUT", False),
            output_video_quality=_env_int(
                "OUTPUT_VIDEO_QUALITY", 95, minimum=1, maximum=100
            ),
            output_video_preset=os.getenv("OUTPUT_VIDEO_PRESET", "medium").strip(),
            normalize_crf=_env_int("NORMALIZE_CRF", 18, minimum=0, maximum=51),
            watermark_output=_env_bool("WATERMARK_OUTPUT", True),
            watermark_text=watermark_text,
            progress_edit_seconds=_env_float(
                "PROGRESS_EDIT_SECONDS", 2.5, minimum=1.0, maximum=30.0
            ),
            # Conservative proactive pacing. Telegram's actual limits vary, so
            # FloodWaitError seconds are always treated as the authority.
            telegram_global_action_interval_seconds=_env_float(
                "TELEGRAM_GLOBAL_ACTION_INTERVAL_SECONDS",
                0.08,
                minimum=0.02,
                maximum=5.0,
            ),
            telegram_chat_action_interval_seconds=_env_float(
                "TELEGRAM_CHAT_ACTION_INTERVAL_SECONDS",
                0.80,
                minimum=0.10,
                maximum=30.0,
            ),
            telegram_max_flood_wait_seconds=_env_int(
                "TELEGRAM_MAX_FLOOD_WAIT_SECONDS", 86400, minimum=10, maximum=604800
            ),
            telegram_max_flood_retries=_env_int(
                "TELEGRAM_MAX_FLOOD_RETRIES", 12, minimum=1, maximum=100
            ),
            telegram_transient_retries=_env_int(
                "TELEGRAM_TRANSIENT_RETRIES", 6, minimum=0, maximum=30
            ),
            telegram_retry_base_seconds=_env_float(
                "TELEGRAM_RETRY_BASE_SECONDS", 1.0, minimum=0.1, maximum=30.0
            ),
            log_level=log_level,
        )

    @property
    def telegram_enabled(self) -> bool:
        """Whether the MTProto worker has all credentials required to start."""
        return bool(
            self.telegram_bot_token
            and self.telegram_api_id > 0
            and self.telegram_api_hash
        )

    @property
    def max_video_bytes(self) -> int:
        return self.max_video_mb * 1024 * 1024

    @property
    def max_image_bytes(self) -> int:
        return self.max_image_mb * 1024 * 1024

    @property
    def telegram_upload_part_bytes(self) -> int:
        return self.telegram_upload_part_mb * 1024 * 1024

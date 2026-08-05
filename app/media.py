"""Media inspection, storage checks, and FFmpeg command builders."""

from __future__ import annotations

import asyncio
import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .config import Settings


class MediaError(RuntimeError):
    """A safe, user-facing media validation error."""


@dataclass(frozen=True, slots=True)
class MediaInfo:
    duration_seconds: float
    width: int
    height: int
    fps: float
    frame_count: int
    has_audio: bool
    format_name: str


async def probe_media(
    path: Path, ffprobe_path: str, *, timeout_seconds: int = 90
) -> MediaInfo:
    """Read ffprobe JSON; filenames and MIME types are never trusted alone."""
    process = await asyncio.create_subprocess_exec(
        ffprobe_path,
        "-v",
        "error",
        "-show_entries",
        "format=format_name,duration:stream=codec_type,width,height,avg_frame_rate,nb_frames",
        "-of",
        "json",
        str(path),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(
            process.communicate(), timeout=timeout_seconds
        )
    except TimeoutError as exc:
        process.kill()
        await process.wait()
        raise MediaError("The uploaded file could not be inspected in time.") from exc

    if process.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").strip()
        raise MediaError(
            f"This file is not a readable image or video. ({detail[:180]})"
        )

    try:
        payload: dict[str, Any] = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise MediaError("Could not read media metadata from this file.") from exc

    video_stream = next(
        (
            stream
            for stream in payload.get("streams", [])
            if stream.get("codec_type") == "video"
        ),
        None,
    )
    if video_stream is None:
        raise MediaError("The upload does not contain a video or image stream.")

    format_data = payload.get("format", {})
    duration = _safe_float(format_data.get("duration"))
    fps = _parse_rate(str(video_stream.get("avg_frame_rate") or "0/0"))
    frame_count = _safe_int(video_stream.get("nb_frames"))
    if frame_count <= 0 and duration > 0 and fps > 0:
        frame_count = max(1, round(duration * fps))

    return MediaInfo(
        duration_seconds=max(duration, 0.0),
        width=_safe_int(video_stream.get("width")),
        height=_safe_int(video_stream.get("height")),
        fps=max(fps, 0.0),
        frame_count=max(frame_count, 0),
        has_audio=any(
            stream.get("codec_type") == "audio" for stream in payload.get("streams", [])
        ),
        format_name=str(format_data.get("format_name") or ""),
    )


def validate_source_image(info: MediaInfo) -> None:
    if info.width <= 0 or info.height <= 0:
        raise MediaError("The source photo has no readable image dimensions.")
    if info.width < 96 or info.height < 96:
        raise MediaError("Please send a clearer source photo (at least 96×96 pixels).")


def validate_target_video(info: MediaInfo, settings: Settings) -> None:
    if info.width <= 0 or info.height <= 0:
        raise MediaError("The target video has no readable dimensions.")
    if info.duration_seconds <= 0:
        raise MediaError("The target video has no readable duration.")
    if (
        settings.max_video_seconds > 0
        and info.duration_seconds > settings.max_video_seconds
    ):
        raise MediaError(
            f"This video is {info.duration_seconds:.0f}s long; the configured limit is "
            f"{settings.max_video_seconds}s."
        )


def ensure_workspace_capacity(
    data_dir: Path, input_bytes: int, settings: Settings
) -> None:
    """Avoid a corrupted render when a real disk limit, not an app limit, is hit.

    The application intentionally has no default input-size cap. It still checks
    live free space before it starts a long render. Memory workflow avoids a full
    PNG-frame cache, but normalized input, output and muxing need working room.
    """
    free = shutil.disk_usage(data_dir).free
    headroom = settings.workspace_headroom_mb * 1024 * 1024
    estimated_required = max(headroom, input_bytes * 2 + headroom)
    if free < estimated_required:
        raise MediaError(
            "The worker volume does not have enough free storage for this render. "
            f"Available: {_human_bytes(free)}; estimated minimum: {_human_bytes(estimated_required)}. "
            "Attach a larger /data volume and try again."
        )


def build_normalize_command(
    input_path: Path, output_path: Path, settings: Settings
) -> list[str]:
    """Produce a compatible MP4 only when NORMALIZE_INPUT=true.

    No scale filter is used when MAX_VIDEO_SIDE=0, preserving the original frame
    dimensions. FFmpeg's machine-readable progress is emitted to stdout.
    """
    command = [
        settings.ffmpeg_path,
        "-y",
        "-progress",
        "pipe:1",
        "-nostats",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
    ]
    if settings.max_video_side > 0:
        side = settings.max_video_side
        command.extend(
            [
                "-vf",
                f"scale={side}:{side}:force_original_aspect_ratio=decrease,scale=trunc(iw/2)*2:trunc(ih/2)*2",
            ]
        )
    command.extend(
        [
            "-c:v",
            "libx264",
            "-preset",
            settings.output_video_preset,
            "-crf",
            str(settings.normalize_crf),
            "-pix_fmt",
            "yuv420p",
            "-c:a",
            "aac",
            "-b:a",
            "192k",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
    )
    return command


def build_watermark_command(
    input_path: Path, output_path: Path, settings: Settings
) -> list[str]:
    text = _escape_drawtext(settings.watermark_text or "AI face swap")
    filter_value = (
        "drawtext="
        "fontfile=/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf:"
        f"text='{text}':"
        "x=w-tw-18:y=h-th-18:fontsize=h/28:"
        "fontcolor=white@0.92:box=1:boxcolor=black@0.45:boxborderw=7"
    )
    return [
        settings.ffmpeg_path,
        "-y",
        "-progress",
        "pipe:1",
        "-nostats",
        "-i",
        str(input_path),
        "-map",
        "0:v:0",
        "-map",
        "0:a?",
        "-vf",
        filter_value,
        "-c:v",
        "libx264",
        "-preset",
        settings.output_video_preset,
        "-crf",
        str(settings.normalize_crf),
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "192k",
        "-movflags",
        "+faststart",
        str(output_path),
    ]


def allowed_extension(filename: str, kind: str) -> str:
    suffix = Path(filename).suffix.lower()
    image_extensions = {".bmp", ".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp"}
    video_extensions = {
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
    allowed = image_extensions if kind == "image" else video_extensions
    if suffix in allowed:
        return suffix
    return ".jpg" if kind == "image" else ".mp4"


def _parse_rate(value: str) -> float:
    try:
        numerator, denominator = value.split("/", 1)
        denominator_value = float(denominator)
        return float(numerator) / denominator_value if denominator_value else 0.0
    except (ValueError, ZeroDivisionError):
        return 0.0


def _safe_int(value: Any) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def _safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _escape_drawtext(value: str) -> str:
    return (
        value.replace("\\", r"\\")
        .replace("'", r"\'")
        .replace(":", r"\:")
        .replace("%", r"\%")
    )


def _human_bytes(value: int) -> str:
    units = ("B", "KiB", "MiB", "GiB", "TiB")
    amount = float(value)
    index = 0
    while amount >= 1024 and index < len(units) - 1:
        amount /= 1024
        index += 1
    return f"{amount:.2f} {units[index]}"

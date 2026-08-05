# High-Capacity Telegram Face-Swap Video Bot

A Docker/Koyeb-ready Telegram bot that uses **MTProto** (`TELEGRAM_API_ID` +
`TELEGRAM_API_HASH` + bot token) for direct Telegram media transfer, FaceFusion
for frame-by-frame video processing, and an advanced live Telegram progress
display.

The HTTP service still listens on **port 8080** for Koyeb:

```text
GET /healthz  → platform health probe
GET /readyz   → MTProto transport connected
```

## What changed in version 2

- **MTProto transport** replaces the public HTTP Bot API file-transfer path.
  This avoids the public Bot API's small download/upload limits and provides
  truthful byte-level download and upload callbacks.
- Default application caps are **disabled**:
  `MAX_VIDEO_MB=0`, `MAX_VIDEO_SECONDS=0`, `MAX_VIDEO_SIDE=0`,
  `QUEUE_MAX_SIZE=0`, and `JOB_TIMEOUT_SECONDS=0`.
- **No automatic downscaling** by default. Original dimensions are passed to
  FaceFusion; `NORMALIZE_INPUT=false` avoids an extra input re-encode.
- **Every target frame is processed**. The default high-quality profile uses
  FaceFusion reference tracking, `512x512` pixel boost, YOLO face detection,
  box + occlusion masks, and a memory frame workflow.
- A single editable Telegram message shows actual transfer bytes, frame count,
  processing speed, ETA, elapsed time, current stage, video timeline, and
  output upload progress.
- Results that exceed Telegram's per-file bot upload ceiling are sent as
  numbered, lossless binary parts instead of being silently reduced in quality.
- The bot is now **button-first**: an inline control panel handles consent,
  source/target guidance, status, quality details, reset, and cancellation.
- A durable FloodWait guard proactively paces control requests, persists server
  wait deadlines, coalesces progress edits, and keeps rendering independent from
  Telegram UI/network stalls.

## Important reality check: “no limit” cannot mean infinite

The bot removes its **own default media, duration, queue, resolution, and job
timeout caps**, but no deployment can exceed physical or upstream limits:

1. Telegram itself has a per-message/file limit.
2. A bot cannot deliver an arbitrarily large result as one Telegram attachment.
   The default `TELEGRAM_UPLOAD_PART_MB=1900` stays below the official local/
   MTProto bot upload ceiling; larger completed files are split automatically.
3. Koyeb volume size, CPU/GPU RAM, compute time, and network bandwidth remain
   real limits. The worker checks free disk before rendering rather than letting
   a long job corrupt itself on a full volume.
4. No face-swap model can guarantee perfect identity or tracking on every input.
   Clear source images, visible target faces, stable lighting, and a GPU produce
   the best results.

Telegram's official local Bot API documentation explains that local mode can
download without a size limit and upload up to 2000 MB; the MTProto approach in
this project is used for direct transfer progress and bot event handling.

## Responsible-use defaults

Only process media you own or have explicit permission to edit. Do not use
minors, non-consensual intimate media, harassment, fraud, or deceptive
impersonation. The bot keeps the following protections:

- consent acknowledgement through the control-panel button before a source image can be stored;
- private chats by default;
- visible `AI face swap` output label by default;
- source-image expiry and job-media cleanup;
- optional owner allowlist with `ALLOWED_USER_IDS`.

Technical controls cannot verify real-world consent. The operator is
responsible for the media and users of the bot.

---

## Telegram control panel and workflow

The normal interface is **button-first**, not command-first:

```text
/start or /menu
→ I Agree
→ Set Source Image
→ send one clear source face image
→ Send Target Video
→ send the target video
```

The persistent inline control panel provides these actions:

| Button | Action |
| --- | --- |
| `I Agree` | Stores the consent acknowledgement. |
| `Set Source Image` | Explains exactly when to send the source image. |
| `Send Target Video` | Checks source readiness and prepares target-video upload. |
| `My Status` | Shows connection, queue, FloodWait and job state. |
| `Quality Details` | Shows the active FaceFusion profile. |
| `Cancel Render` | Cancels an in-progress upload, queued work, output transfer, or active render. |
| `Reset Session` | Deletes the saved source image and consent state. |

Only `/start`, `/menu`, and `/help` are needed for normal operation. `/status`
and `/cancel` remain as small emergency/keyboard fallbacks. If you configure
BotFather commands, expose only these five concise commands:

```text
start - Open the control panel
menu - Open the control panel
help - Show help
status - Emergency status fallback
cancel - Emergency cancellation fallback
```

### Live progress display

The bot edits one message at a controlled interval to avoid Telegram flood
limits. The values shown are based on real callbacks/log output, not fake timer
percentages:

```text
╭─ FACE SWAP ENGINE ──────────────────
│ Job       91a0e34b5f20
│ State     Frame-by-frame face swap
│ Activity  Swapping every frame with FaceFusion.
├─ PIPELINE ───────────────────────────
│ ✓ Input › ✓ Assets › ● Frames › ○ Finalize › ○ Deliver
├─ PROGRESS ───────────────────────────
│ Overall   [████████████░░░░░░]  67.5%
│ Stage     [████████████░░░░░░]  67.5%
│ Frames    3,240 / 4,800 ( 67.5%)
│ Timeline  01:21 / 02:00
│ Inference 6.42 frames/s
├─ SOURCE VIDEO ───────────────────────
│ Video     1920×1080 · 30 fps · 4,800 frames · audio
├─ RUNTIME ────────────────────────────
│ Stage ETA 04:03
│ Elapsed   08:11
╰─ Use My Status or Cancel Render
```

During the first model warm-up it also reports the **current model filename**,
actual downloaded bytes, current-asset percentage, transfer speed, cache size,
and stage ETA whenever FaceFusion exposes those values in its download output.
The display also reports MTProto download/upload bytes, FFmpeg preparation,
model-asset download, output labelling, part splitting, and delivery stages.

### FloodWait and no-hang design

Telegram can rate-limit messages, edits, callback answers, uploads, and other
requests. This project handles it as a normal operating condition instead of
letting it freeze the bot:

- Telethon automatic FloodWait sleeping is disabled; every `FloodWaitError` is
  captured by one shared guard.
- Control actions are proactively paced globally and per chat.
- Telegram's exact `FloodWaitError.seconds` deadline is persisted in SQLite, so
  a Koyeb restart does not immediately retry and make the restriction worse.
- Important actions wait **asynchronously** and retry with bounded, jittered
  backoff. The ASGI health server, incoming updates, buttons, and renderer stay
  alive during the wait.
- Progress edits are noncritical, coalesced per message, and sent in a separate
  task. They are dropped while rate-limited rather than blocking FaceFusion's
  frame/log reader.
- Callback taps are debounced and acknowledged immediately when Telegram allows
  it, so repeated button taps do not create duplicate jobs or an update backlog.

`/healthz` and the **My Status** button expose FloodWait remaining time, retry
counts, and deferred progress-update telemetry.

---

## Required credentials

This project uses a **bot identity**, not a personal Telegram account session.
You need all three values below:

| Secret | Where to obtain it | Purpose |
| --- | --- | --- |
| `TELEGRAM_BOT_TOKEN` | [@BotFather](https://t.me/BotFather) | Your bot identity. |
| `TELEGRAM_API_ID` | [my.telegram.org/apps](https://my.telegram.org/apps) | Direct MTProto application ID. |
| `TELEGRAM_API_HASH` | [my.telegram.org/apps](https://my.telegram.org/apps) | Direct MTProto application hash. |

Do **not** paste any of these values into GitHub, an issue, Docker build args,
or chat. Add them only as Koyeb Secrets/environment variables.

### Migrating from the old webhook build

This version receives updates through MTProto and does **not** require
`PUBLIC_BASE_URL`, `WEBHOOK_PATH_SECRET`, or `TELEGRAM_WEBHOOK_SECRET`.
Before switching, remove any old public Bot API webhook so only one update path
is active:

```bash
curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/deleteWebhook" \
  --data-urlencode "drop_pending_updates=false"
```

---

## Local Docker run

```bash
cp .env.example .env
# Edit .env and add TELEGRAM_BOT_TOKEN, TELEGRAM_API_ID, TELEGRAM_API_HASH.
docker compose up --build
curl -fsS http://localhost:8080/healthz
```

The MTProto bot receives Telegram messages directly. Unlike a webhook bot, it
does not need a public tunnel for incoming updates; port 8080 remains for
Koyeb's health check and optional operational probes.

## Koyeb deployment

See [KOYEB.md](KOYEB.md) for the complete checklist. Essentials:

1. Push this repository to a private GitHub repository.
2. Create a Koyeb **Web Service** using `Dockerfile`.
3. Configure HTTP port **8080** and health check **`GET /healthz`** on port
   **8080**.
4. Attach a persistent volume at **`/data`** (it also preserves MTProto session and FloodWait state).
5. Add `TELEGRAM_BOT_TOKEN`, `TELEGRAM_API_ID`, and `TELEGRAM_API_HASH` as
   Koyeb Secrets.
6. Keep **one instance**. The durable queue is SQLite-based and FaceFusion is
   intentionally single-concurrency.

### Storage sizing for large jobs

Use a large persistent `/data` volume. Memory workflow avoids a full PNG image
sequence, but a large render still needs room for the uploaded video, output,
optional label pass, temporary files, model cache, and one output part while it
is uploaded. A practical baseline is at least **three times the largest target
video plus 10 GB**; use more for long 4K videos.

### Do not use the 512 MiB Koyeb instance for FaceFusion rendering

The 512 MiB Koyeb Free/Micro-style instance can keep the Telegram control bot
alive, but it cannot safely load the current FaceFusion video profile. The
`hyperswap_1a_256.onnx` asset alone is about 384 MiB on disk; ONNX Runtime,
face detection/landmark models, Python, FFmpeg buffers, and video frames need
substantial additional RAM. Linux therefore kills the renderer with signal
`-9` (SIGKILL/OOM), exactly as a container memory limit is reached.

The bot now detects the cgroup memory limit **before model load** and reports a
clear resource message instead of incorrectly blaming the source image.

| Koyeb instance | RAM / CPU | Result |
| --- | --- | --- |
| Free / 512 MiB | 512 MiB / 0.1 vCPU | Bot UI only; FaceFusion video rendering is not viable. |
| Standard Medium / Eco Medium | 2 GiB | Low-memory testing only; reduce profile settings below. |
| Standard Large / Eco Large | 4 GiB | Recommended minimum for the current high-quality CPU profile. |
| GPU instance | GPU + large RAM | Recommended for frequent or long video renders. |

For the current high-quality profile, set:

```dotenv
MIN_RENDER_MEMORY_MB=3072
```

For a **2 GiB test-only** profile, lower quality deliberately:

```dotenv
MIN_RENDER_MEMORY_MB=2048
EXECUTION_THREADS=1
FACE_SWAPPER_PIXEL_BOOST=256x256
FACE_MASK_TYPES=box
OUTPUT_VIDEO_QUALITY=85
```

This lower profile still does not make 512 MiB viable. The worker is already
single-concurrency: only one FaceFusion video subprocess runs at a time.

### CPU versus GPU

The default image is CPU-compatible:

```bash
docker build -t mtproto-facefusion-bot .
```

It works, but long/high-resolution frame-by-frame renders can be very slow. For
regular large-file use, deploy on compatible NVIDIA GPU infrastructure:

```bash
docker build \
  --build-arg FACEFUSION_IMAGE=facefusion/facefusion:3.8.0-cuda \
  -t mtproto-facefusion-bot:cuda .
```

Then set:

```dotenv
EXECUTION_PROVIDER=cuda
```

Do not set `cuda` on a CPU-only Koyeb instance.

---

## High-quality FaceFusion profile

The default settings in `.env.example` favour identity stability and quality:

```dotenv
FACE_SELECTOR_MODE=reference
REFERENCE_FRAME_NUMBER=0
FACE_SWAPPER_PIXEL_BOOST=512x512
FACE_DETECTOR_MODEL=yolo_face
FACE_DETECTOR_SIZE=640x640
FACE_MASK_TYPES=box occlusion
WORKFLOW_STRATEGY=memory
OUTPUT_VIDEO_QUALITY=95
NORMALIZE_INPUT=false
```

### Reference tracking requirement

In `reference` mode FaceFusion identifies the target identity from
`REFERENCE_FRAME_NUMBER` (zero means the first frame). Make sure the intended
target face is visible in that frame. If the video begins with a transition,
black frame, or a different person, either trim the video first, change
`REFERENCE_FRAME_NUMBER`, or use `FACE_SELECTOR_MODE=one` for a simple
largest-face-per-frame selection.

### Fidelity notes

- `memory` workflow still processes each frame; it prevents a large temporary
  image sequence from consuming the entire volume.
- `MAX_VIDEO_SIDE=0` preserves input dimensions. Set a positive value only when
  you explicitly want a downscaled normalization pass.
- `NORMALIZE_INPUT=false` avoids an additional lossy conversion. Enable it only
  for codecs/containers that FaceFusion cannot read directly.
- `box occlusion` masks improve boundaries around hands, hair, and occluded
  faces, but require additional model assets and compute.
- Higher pixel boost improves some faces but increases GPU/CPU time and memory.

## Large output delivery

A completed output at or below `TELEGRAM_UPLOAD_PART_MB` is sent as one video or
document. A larger file is sent as ordered binary files:

```text
face-swap-labelled.mp4.part001
face-swap-labelled.mp4.part002
...
```

Reassemble them without transcoding:

```bash
# Linux / macOS
cat face-swap-labelled.mp4.part* > face-swap-labelled.mp4

# Windows Command Prompt
copy /b face-swap-labelled.mp4.part001+face-swap-labelled.mp4.part002 face-swap-labelled.mp4
```

This preserves the exact rendered bytes instead of lowering quality just to fit
a single Telegram message.

---

## Key environment variables

| Variable | Default | Meaning |
| --- | ---: | --- |
| `MAX_VIDEO_MB` | `0` | No application-enforced input size cap. |
| `MAX_VIDEO_SECONDS` | `0` | No application-enforced duration cap. |
| `MAX_VIDEO_SIDE` | `0` | Preserve original dimensions. |
| `QUEUE_MAX_SIZE` | `0` | No application-enforced waiting queue cap. |
| `JOB_TIMEOUT_SECONDS` | `0` | No application render timeout. |
| `TELEGRAM_UPLOAD_PART_MB` | `1900` | Safe maximum per Telegram output part. |
| `SPLIT_LARGE_RESULTS` | `true` | Send lossless parts when one file is too large. |
| `WORKSPACE_HEADROOM_MB` | `1024` | Minimum free-space reserve before rendering. |
| `PROGRESS_EDIT_SECONDS` | `2.5` | Minimum interval between progress-message edits. |
| `TELEGRAM_GLOBAL_ACTION_INTERVAL_SECONDS` | `0.08` | Conservative global pacing for control requests. |
| `TELEGRAM_CHAT_ACTION_INTERVAL_SECONDS` | `0.80` | Per-chat pacing for messages and menu edits. |
| `TELEGRAM_MAX_FLOOD_WAIT_SECONDS` | `86400` | Longest persisted FloodWait an important action will await. |
| `TELEGRAM_MAX_FLOOD_RETRIES` | `12` | Bounded retry count for server-mandated FloodWaits. |
| `TELEGRAM_TRANSIENT_RETRIES` | `6` | Bounded retry count for network/server failures. |
| `WORKFLOW_STRATEGY` | `memory` | Per-frame processing without a full on-disk frame cache. |

See [`.env.example`](.env.example) for every setting.

## Troubleshooting

| Symptom | Resolution |
| --- | --- |
| `/healthz` is healthy but `/readyz` is `503` | Verify `TELEGRAM_BOT_TOKEN`, `TELEGRAM_API_ID`, and `TELEGRAM_API_HASH` in Koyeb Secrets. |
| Bot does not answer | Remove an old webhook, verify that one deployment is active, and inspect Koyeb logs for MTProto connection attempts. |
| A button spinner appears briefly | Callback taps are acknowledged first; repeated taps are debounced. Open **My Status** if Telegram is temporarily rate-controlling outbound actions. |
| Status shows Telegram rate control paused | This is a server-requested FloodWait. The deadline is persisted and important actions retry asynchronously; do not restart repeatedly. |
| Progress pauses during model setup | The first FaceFusion render downloads model assets. Keep `/data` persistent. |
| The job stops before rendering | Expand the `/data` volume. The bot refuses to start a job that lacks estimated working space. |
| The wrong face is selected | Ensure the intended target face appears in `REFERENCE_FRAME_NUMBER`; use a clear source image. |
| A result arrives as parts | Reassemble the parts with the exact command shown above. No quality was removed. |

## Testing

```bash
python -m compileall -q app tests
python -m unittest discover -s tests -v
```

The repository also includes a GitHub Actions CI workflow.

## Third-party terms

The wrapper code is MIT-licensed; see [LICENSE](LICENSE). FaceFusion, Telethon,
FFmpeg, and downloaded model assets have their own terms; see
[ATTRIBUTIONS.md](ATTRIBUTIONS.md). Review all terms before commercial use.

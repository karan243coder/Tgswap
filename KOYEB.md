# Koyeb deployment checklist — MTProto, large files, port 8080

This version receives Telegram messages through **MTProto**, not a public Bot
API webhook. Koyeb still runs a Web Service because it needs a stable HTTP
health endpoint on port **8080**.

## 1. Create and protect credentials

You need three secrets:

```text
TELEGRAM_BOT_TOKEN  → @BotFather
TELEGRAM_API_ID     → https://my.telegram.org/apps
TELEGRAM_API_HASH   → https://my.telegram.org/apps
```

- Do not commit them to GitHub.
- Do not pass them as Docker build arguments.
- Add them only in **Koyeb Environment Variables / Secrets**.
- The API ID/hash belong to your Telegram developer application. They are not a
  replacement for the BotFather bot token; all three are required.

If you are upgrading from the old webhook project, remove the existing webhook
before starting MTProto mode:

```bash
curl -sS -X POST "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/deleteWebhook" \
  --data-urlencode "drop_pending_updates=false"
```

## 2. Create a Koyeb Web Service

1. Push this project to a **private GitHub repository**.
2. In Koyeb select **Create App** → **Web Service** → GitHub.
3. Select the repository and branch.
4. Build method: **Dockerfile**.
5. Dockerfile path: `Dockerfile`.
6. Set the exposed port to **8080**, protocol **HTTP**.
7. Route `/` to port **8080**.
8. Set **minimum instances = 1** and **maximum instances = 1**.

Why one instance? This deployment deliberately has one FaceFusion worker and a
SQLite queue under `/data`. Multiple instances can receive different MTProto
updates and would need Redis/shared queue coordination.

## 3. Attach persistent storage

Attach a Koyeb volume at exactly:

```text
/data
```

Recommended sizing:

```text
minimum: 20 GB for small/medium jobs
large renders: at least 3 × largest target video + 10 GB
```

The volume stores:

- FaceFusion model cache (`/data/facefusion-assets`);
- MTProto session state;
- SQLite sessions/jobs and persisted Telegram FloodWait deadlines;
- active input/output media and temporary render files.

Do not mount a volume over `/app` or `/facefusion`. The Docker entrypoint fixes
`/data` ownership and drops to an unprivileged runtime account before serving.

## 4. Add Koyeb environment variables

Set at minimum:

```dotenv
TELEGRAM_BOT_TOKEN=<BotFather token>
TELEGRAM_API_ID=<numeric API ID>
TELEGRAM_API_HASH=<32 character hexadecimal API hash>
EXECUTION_PROVIDER=cpu
```

Recommended large-file settings are already documented in `.env.example`:

```dotenv
MAX_VIDEO_MB=0
MAX_VIDEO_SECONDS=0
MAX_VIDEO_SIDE=0
QUEUE_MAX_SIZE=0
JOB_TIMEOUT_SECONDS=0
WORKSPACE_HEADROOM_MB=1024
TELEGRAM_UPLOAD_PART_MB=1900
SPLIT_LARGE_RESULTS=true
WORKFLOW_STRATEGY=memory

# Durable Telegram FloodWait protection
TELEGRAM_GLOBAL_ACTION_INTERVAL_SECONDS=0.08
TELEGRAM_CHAT_ACTION_INTERVAL_SECONDS=0.80
TELEGRAM_MAX_FLOOD_WAIT_SECONDS=86400
TELEGRAM_MAX_FLOOD_RETRIES=12
TELEGRAM_TRANSIENT_RETRIES=6
TELEGRAM_RETRY_BASE_SECONDS=1.0
```

`0` removes an **application-enforced** cap. It does not make disk, RAM,
network, GPU capacity, or Telegram's own per-file service rules infinite.

For a private owner-only bot, add:

```dotenv
ALLOWED_USER_IDS=123456789
```

## 5. Configure the health check

In Koyeb Service Settings configure an HTTP health check:

| Setting | Value |
| --- | --- |
| Protocol | HTTP |
| Port | `8080` |
| Method | `GET` |
| Path | `/healthz` |
| Grace period | `20–60 seconds` |
| Timeout | `5 seconds` |

`/healthz` returns `200` as soon as the container is alive. It intentionally
does not wait for model downloads or MTProto reconnects. Use `/readyz` only for
manual diagnostics; it becomes `200` when Telegram is connected.

## 6. Verify

After deployment:

```bash
curl -fsS https://YOUR-KOYEB-DOMAIN/healthz
```

Expected fields include:

```json
{
  "status": "ok",
  "transport": "mtproto",
  "telegram_configured": true,
  "worker_started": true,
  "rate_control": {
    "flood_wait_remaining_seconds": 0
  }
}
```

Then open Telegram and test the button-first control panel:

```text
/start → I Agree → Set Source Image → source image → Send Target Video → target video
```

The bot should create an editable progress message before the large target video
is downloaded.

## 7. GPU deployment

CPU works but is not suitable for frequent large/high-resolution renders. For a
compatible NVIDIA GPU deployment, build using the official CUDA FaceFusion base:

```bash
docker build \
  --build-arg FACEFUSION_IMAGE=facefusion/facefusion:3.8.0-cuda \
  -t mtproto-facefusion-bot:cuda .
```

Set:

```dotenv
EXECUTION_PROVIDER=cuda
```

Only do this on infrastructure that exposes an NVIDIA GPU and compatible CUDA
driver to the container.

## Operational behavior for very large files

- MTProto supplies direct media download/upload callbacks so the bot can show
  true transferred-byte progress.
- FaceFusion processes every frame. Memory workflow prevents a full PNG frame
  cache, but high-resolution videos are still compute intensive.
- The bot checks actual free disk before rendering. If it rejects a job for
  storage, increase the `/data` volume; it has not imposed a hidden file cap.
- Outputs above Telegram's single-bot-upload ceiling are delivered as lossless
  numbered parts. Users can concatenate them exactly as described in `README.md`.

## FloodWait resilience on Koyeb

The bot does not treat FloodWait as a crash condition:

- outbound control requests are paced globally and per chat;
- server-requested wait deadlines are persisted in `/data/bot.sqlite3`;
- a Koyeb restart therefore does not retry a rate-limited request immediately;
- important menu/messages wait asynchronously and retry with bounded backoff;
- progress edits are coalesced and dropped while throttled, so FaceFusion frame
  processing is never stalled by Telegram UI updates;
- inline-button taps are debounced, callback spinners are acknowledged separately,
  and a paced menu edit is deferred rather than blocking the user workflow;
- `GET /healthz` includes rate-control telemetry; **My Status** exposes the same
  information to the user.

Keep minimum scale at **1** and keep the `/data` volume attached. Scaling to
zero or deleting the volume loses MTProto session/flood state and makes large
file jobs less reliable.

## Disable safely

To stop the bot, first scale the Koyeb service down/delete it, then revoke the
BotFather token if it may have leaked. The persistent `/data` volume contains
session state and temporary cache, so delete the volume as well if you no longer
need it.

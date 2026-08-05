"""ASGI health application for the MTProto FaceFusion bot on port 8080."""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse

from .config import ConfigError, Settings
from .service import BotService


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Do not let libraries print connection/session details at INFO level.
    logging.getLogger("telethon").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)


try:
    settings = Settings.from_env()
except ConfigError as exc:
    raise RuntimeError(f"Invalid bot configuration: {exc}") from exc

_configure_logging(settings.log_level)


@asynccontextmanager
async def lifespan(app: FastAPI):
    service = BotService(settings)
    await service.start()
    app.state.service = service
    try:
        yield
    finally:
        await service.stop()


app = FastAPI(
    title="MTProto FaceFusion Video Bot",
    version="2.0.0",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)


def _service(request: Request) -> BotService:
    service = getattr(request.app.state, "service", None)
    if service is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is starting",
        )
    return service


@app.get("/", include_in_schema=False)
async def root(request: Request) -> JSONResponse:
    service = _service(request)
    return JSONResponse(
        {
            "service": "mtproto-facefusion-video-bot",
            "transport": "mtproto",
            "health": "/healthz",
            "telegram_configured": service.settings.telegram_enabled,
        }
    )


@app.get("/healthz", include_in_schema=False)
async def healthz(request: Request) -> JSONResponse:
    """Fast Koyeb probe. It does not wait for model loading or Telegram reconnects."""
    return JSONResponse(_service(request).health_payload())


@app.get("/readyz", include_in_schema=False)
async def readyz(request: Request) -> JSONResponse:
    service = _service(request)
    if not service.ready:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="MTProto transport is not connected",
        )
    return JSONResponse({"status": "ready", "transport": "mtproto"})

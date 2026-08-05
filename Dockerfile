# Official FaceFusion CPU image, pinned to the version available when this
# project was prepared. For a GPU deployment, build with:
# --build-arg FACEFUSION_IMAGE=facefusion/facefusion:3.8.0-cuda
ARG FACEFUSION_IMAGE=facefusion/facefusion:3.8.0-cpu
FROM ${FACEFUSION_IMAGE}

USER root
WORKDIR /app

# ffmpeg is already present in the official FaceFusion image. DejaVu provides a
# reliable local font for the visible AI-output label; no CDN assets are needed.
RUN apt-get update \
    && apt-get install --no-install-recommends -y fonts-dejavu-core gosu \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.bot.txt ./
RUN python -m pip install --no-cache-dir -r requirements.bot.txt

COPY app ./app
COPY scripts/docker-entrypoint.sh /usr/local/bin/docker-entrypoint
RUN chmod 0755 /usr/local/bin/docker-entrypoint

# FaceFusion stores downloaded models under /facefusion/.assets. Point that path
# at /data so one Koyeb volume persists both models and the SQLite/job state.
RUN rm -rf /facefusion/.assets \
    && mkdir -p /data/facefusion-assets /data/jobs /data/sessions \
    && ln -s /data/facefusion-assets /facefusion/.assets \
    && useradd --create-home --uid 10001 --shell /usr/sbin/nologin botuser \
    && chown -R botuser:botuser /app /data

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DATA_DIR=/data \
    EXECUTION_PROVIDER=cpu \
    EXECUTION_THREADS=2

# The entrypoint fixes persistent-volume ownership (when needed) and immediately
# drops to botuser before Python/FaceFusion starts.
ENTRYPOINT ["/usr/local/bin/docker-entrypoint"]
EXPOSE 8080

# Docker's check is useful locally; configure Koyeb's HTTP health check to the
# same path and port: GET /healthz on 8080.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD python -c "from urllib.request import urlopen; urlopen('http://127.0.0.1:8080/healthz', timeout=3)" || exit 1

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080", "--proxy-headers", "--forwarded-allow-ips=*", "--no-access-log"]

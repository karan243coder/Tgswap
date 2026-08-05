#!/bin/sh
set -eu

# Persistent platforms can mount an empty root-owned volume on /data after the
# image is built. Make only the bot's writable directory available to the
# unprivileged runtime account, then drop root before serving any request.
mkdir -p /data/facefusion-assets /data/jobs /data/sessions
if [ "$(stat -c '%u:%g' /data)" != "10001:10001" ]; then
  chown -R botuser:botuser /data
fi

exec gosu botuser "$@"

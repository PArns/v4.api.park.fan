#!/bin/sh
set -e

# Ensure writable dirs exist and are owned by nestjs (volumes may be mounted here)
# - /app/geoip: GeoIP DB (download on first start or 48h job)
# - /app/data: fallback path in code
# - /app/logs: slow-request log (slow-requests.log)
mkdir -p /app/geoip /app/data /app/logs
chown -R nestjs:nodejs /app/geoip /app/data /app/logs 2>/dev/null || true

# Switch to nestjs user and execute CMD
exec su-exec nestjs "$@"

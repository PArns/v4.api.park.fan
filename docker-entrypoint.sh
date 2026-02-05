#!/bin/sh
set -e

# Ensure GeoIP directory exists and is writable by nestjs user (download on first start or 48h job)
# Use /app/geoip (mounted volume in production); /app/data is fallback path in code
mkdir -p /app/geoip /app/data
chown -R nestjs:nodejs /app/geoip /app/data 2>/dev/null || true

# Switch to nestjs user and execute CMD
exec su-exec nestjs "$@"

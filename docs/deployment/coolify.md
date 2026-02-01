# Coolify Deployment

This guide details how to deploy Park Fan API v4 on Coolify.

## Quick Start

1. **Project**: Create a new Docker Compose resource in Coolify.
2. **Repo**: Select `PArns/v4.api.park.fan`.
3. **Branch**: `main`.
4. **Compose File**: `docker-compose.production.yml`.

## Environment Variables

Configure these in Coolify's environment variable section:

### Application
- `NODE_ENV=production`
- `PORT=3000`
- `API_PREFIX=v1`

### Database
- `DB_HOST=postgres`
- `DB_PORT=5432`
- `DB_USERNAME=parkfan`
- `DB_PASSWORD=<SECURE_PASSWORD>`
- `DB_DATABASE=parkfan`
- `DB_SYNCHRONIZE=false` (Use migrations in prod if available, else be careful)

### Redis
- `REDIS_HOST=redis`
- `REDIS_PORT=6379`

### ML Service
- `ML_SERVICE_URL=http://ml-service:8000`
- `MODEL_DIR=/app/models`

## Volumes

Coolify manages these persistent volumes automatically:
- `pgdata`
- `redisdata`
- `ml-models`

## Verification

After deployment, check health endpoints:
- API: `https://api.yourdomain.com/health`
- Queue Board: `https://queue.yourdomain.com`

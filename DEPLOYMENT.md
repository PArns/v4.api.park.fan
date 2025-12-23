# Coolify Deployment Guide

This guide walks you through deploying park.fan API v4 on your Coolify server.

## Prerequisites

- Coolify instance running on your server
- GitHub repository connected to Coolify
- Domain name (optional, but recommended)

## Quick Deployment Steps

### 1. Create New Project in Coolify

1. Log into your Coolify dashboard
2. Click **+ New Resource** → **Docker Compose**
3. Select your GitHub repository: `PArns/v4.api.park.fan`
4. Choose branch: `main` (or your production branch)

### 2. Configure Build Settings

**Docker Compose File**: `docker-compose.production.yml`

Coolify will automatically detect this file and use it for deployment.

### 3. Configure Environment Variables

In Coolify, add these environment variables (all in one `.env` file):

```env
# Application
NODE_ENV=production
PORT=3000
API_PREFIX=v1

# Database - PostgreSQL with TimescaleDB
DB_HOST=postgres
DB_PORT=5432
DB_USERNAME=parkfan
DB_PASSWORD=<GENERATE_STRONG_PASSWORD>
DB_DATABASE=parkfan
DB_SYNCHRONIZE=true
DB_LOGGING=false

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Bull Queue
BULL_PREFIX=parkfan

# External APIs
GOOGLE_API_KEY=<YOUR_GOOGLE_API_KEY>
OPEN_WEATHER_API_KEY=<YOUR_OPENWEATHER_API_KEY>

# ML Service Configuration
ML_SERVICE_URL=http://ml-service:8000
MODEL_DIR=/app/models
MODEL_VERSION=v1.0.0
```

> **Important**: Replace `<GENERATE_STRONG_PASSWORD>` with a secure password for PostgreSQL

### 4. Configure Persistent Volumes

Coolify will automatically create and manage these volumes:
- `pgdata` - PostgreSQL database (persistent across deployments)
- `redisdata` - Redis data (persistent across deployments)
- `ml-models` - ML trained models (persistent across deployments)

**Your data will be preserved during redeployments!**

### 5. Configure Domains (Optional)

If you want custom domains:

1. In Coolify, go to your project settings
2. Add domains for each service:
   - API: `api.yourdomain.com`
   - Bull Board: `queue.yourdomain.com`

Coolify will automatically configure SSL certificates via Let's Encrypt.

### 6. Deploy

1. Click **Deploy** in Coolify
2. Monitor the build logs
3. Wait for all services to become healthy (indicated by green status)

### 7. Verify Deployment

Once deployed, verify all services are running:

```bash
# Check API health
curl https://api.yourdomain.com/health

# Check ML service health (internal)
# This runs inside the network, so you can test via API endpoints

# Access Bull Board
# Open https://queue.yourdomain.com in browser
```

## Service Architecture

```
┌─────────────────────────────────────────────┐
│                  Coolify                     │
├─────────────────────────────────────────────┤
│                                              │
│  ┌──────────┐         ┌──────────────┐      │
│  │   API    │────────▶│  PostgreSQL  │      │
│  │ (NestJS) │         │ (TimescaleDB)│      │
│  └────┬─────┘         └──────────────┘      │
│       │                                      │
│       │               ┌──────────────┐      │
│       ├──────────────▶│    Redis     │      │
│       │               └──────────────┘      │
│       │                                      │
│       │               ┌──────────────┐      │
│       └──────────────▶│  ML Service  │      │
│                       │   (Python)   │      │
│                       └──────────────┘      │
│                                              │
│  ┌──────────────┐                            │
│  │ Bull Board   │                            │
│  │ (Monitoring) │                            │
│  └──────────────┘                            │
│                                              │
└─────────────────────────────────────────────┘
```

## Post-Deployment Tasks

### Initial Data Seeding

After first deployment, you may want to seed the database:

```bash
# Connect to your API container in Coolify terminal
# Then run:
npm run db:reset  # Only on first deployment!
```

### Monitor Queue Jobs

Access Bull Board at `https://queue.yourdomain.com` to:
- Monitor queue processing
- View failed jobs
- Retry failed jobs
- Check queue statistics

## Redeployment & Updates

### Deploying New Versions

1. Push your changes to GitHub
2. In Coolify, click **Redeploy**
3. Your data persists automatically thanks to Docker volumes

### Zero-Downtime Deployments

Coolify supports rolling updates. Your services will be updated one by one without full downtime.

## Backup & Recovery

### Database Backup

It's recommended to setup automated PostgreSQL backups:

```bash
# Manual backup
docker exec <postgres-container> pg_dump -U parkfan parkfan > backup.sql

# Restore
docker exec -i <postgres-container> psql -U parkfan parkfan < backup.sql
```

Consider using Coolify's built-in backup features or external backup solutions.

### Volume Backup

Your persistent volumes are stored in:
- `/var/lib/docker/volumes/<project-name>_pgdata`
- `/var/lib/docker/volumes/<project-name>_redisdata`
- `/var/lib/docker/volumes/<project-name>_ml-models`

## Troubleshooting

### Service Won't Start

1. Check environment variables are set correctly
2. View logs in Coolify dashboard
3. Verify database connection

### Database Connection Issues

- Ensure `DB_HOST=postgres` (service name in docker-compose)
- Verify database password matches in all services
- Check if PostgreSQL service is healthy

### ML Service Failing

- Check if ML service can reach PostgreSQL
- Verify MODEL_DIR is correctly set
- Ensure ml-models volume is mounted

### Performance Optimization

For production, consider:
- Increasing container resources in Coolify
- Setting up connection pooling for PostgreSQL
- Configuring Redis maxmemory policy
- Adding a reverse proxy/CDN for static assets

## Logs & Monitoring

Access logs via Coolify dashboard:
- Real-time logs for all services
- Filter by service
- Download logs for analysis

## Health Checks

All services include health checks:
- **API**: `GET /health`
- **PostgreSQL**: `pg_isready`
- **Redis**: `redis-cli ping`
- **ML Service**: `GET /health`

Coolify automatically monitors these and restarts unhealthy containers.

## Security Considerations

- All services communicate via internal Docker network
- Only API and Bull Board are exposed to internet
- Database and Redis are not directly accessible
- Use strong passwords for database
- Store API keys as Coolify secrets
- Enable SSL/TLS for public endpoints (auto via Coolify)

## Support

For issues specific to:
- **Coolify**: Check [Coolify Documentation](https://coolify.io/docs)
- **Application**: Check application logs and README.md

# ML Service ECONNRESET Fix

**Date**: 2026-02-15
**Issue**: Recurring `ECONNRESET` and "socket hang up" errors when API calls ML service
**Status**: ✅ Fixed

## Problem

The API was experiencing intermittent connection reset errors when calling the Python ML service:

```
Error: read ECONNRESET
Error: socket hang up
```

**Symptoms**:
- Errors occurred during cache warmup operations
- ML predictions took 5-10 seconds to complete
- Connection was reset mid-request
- Happened sporadically (not constantly)

## Root Cause

1. **Missing uvicorn timeouts**: The Python ML service (uvicorn) had no explicit timeout configuration
2. **Keep-alive timeout**: Default keep-alive timeout (5s) was too short for ML operations
3. **No retry logic**: Transient network errors weren't retried automatically

## Solution

### 1. Uvicorn Configuration (`ml-service/Dockerfile`)

Added explicit timeout settings to uvicorn:

```dockerfile
CMD ["uvicorn", "main:app",
     "--host", "0.0.0.0",
     "--port", "8000",
     "--no-access-log",
     "--workers", "4",
     "--timeout-keep-alive", "120",        # 2 minutes keep-alive
     "--timeout-graceful-shutdown", "30"]  # 30s graceful shutdown
```

**Changes**:
- `--timeout-keep-alive 120`: Increased from default 5s to 120s
- `--timeout-graceful-shutdown 30`: Allow graceful shutdown (30s)

### 2. Axios Client Configuration (`src/ml/ml.service.ts`)

Improved axios client with:

**Timeout increase**:
- Changed from 90s to 120s to match uvicorn keep-alive

**Connection stability**:
- Added `Connection: keep-alive` header
- Set `maxContentLength: Infinity` and `maxBodyLength: Infinity`

**Retry logic**:
- Added axios interceptor to retry transient connection errors
- Retries up to 2 times (3 total attempts) for:
  - `ECONNRESET`
  - `ETIMEDOUT`
  - `ECONNABORTED`
  - "socket hang up" errors
- Exponential backoff: 1s, 2s

```typescript
// Retry up to 2 times for connection errors (total 3 attempts)
if (isRetryable && config._retryCount < 2) {
  config._retryCount += 1;
  const delay = 1000 * config._retryCount;
  await new Promise((resolve) => setTimeout(resolve, delay));
  return this.mlClient.request(config);
}
```

## Deployment

### Local Development

No changes needed - rebuild and restart:

```bash
npm run build
docker-compose restart ml-service
```

### Production (Coolify)

1. Push changes to repository
2. Coolify will rebuild the ML service container automatically
3. New uvicorn settings will apply on next deployment

## Monitoring

After deployment, monitor for:

1. **Reduction in ECONNRESET errors** in logs
2. **Successful retry attempts** (look for "retry X/2" messages)
3. **ML prediction latency** (should remain 5-10s but with fewer failures)

## Testing

Verify the fix:

```bash
# Check ML service health
curl http://ml-service:8000/health

# Test prediction endpoint (should complete without ECONNRESET)
curl -X POST http://api:3000/v1/predictions/park/{parkId}/hourly
```

## Fallback Plan

If issues persist:

1. **Reduce worker count**: Change `--workers 4` to `--workers 2`
2. **Increase timeout further**: Change `120` to `180` seconds
3. **Disable keep-alive**: Remove `Connection: keep-alive` header
4. **Check ML service logs**: Look for memory issues or crashes

## Related Files

- `ml-service/Dockerfile` - Uvicorn configuration
- `src/ml/ml.service.ts` - Axios client and retry logic
- `docs/troubleshooting/common-issues.md` - General troubleshooting

## References

- [Uvicorn Settings](https://www.uvicorn.org/settings/)
- [Axios Retry Patterns](https://github.com/axios/axios/issues/164)
- Issue thread: See error logs from 2026-02-12 to 2026-02-15

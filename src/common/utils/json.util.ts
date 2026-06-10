/**
 * JSON.parse that treats malformed input as absent instead of throwing.
 *
 * Intended for Redis cache reads on request paths: a corrupted or
 * truncated cache entry should behave like a cache miss (rebuild),
 * not bubble a SyntaxError into a 500 response.
 */
export function safeJsonParse<T>(raw: string | null | undefined): T | null {
  if (raw == null) return null;
  try {
    return JSON.parse(raw) as T;
  } catch {
    return null;
  }
}

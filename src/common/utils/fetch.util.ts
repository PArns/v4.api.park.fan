import { Logger } from "@nestjs/common";

const logger = new Logger("FetchUtils");

export interface RetryOptions {
  retries?: number;
  backoff?: number;
  timeout?: number;
}

/**
 * Enhanced fetch with built-in retry and exponential backoff.
 * Primarily used for external theme park APIs (Wiki, Queue-Times).
 */
export async function fetchWithRetry(
  url: string,
  options: RequestInit = {},
  retryOptions: RetryOptions = {},
): Promise<Response> {
  const { retries = 3, backoff = 1000, timeout = 15000 } = retryOptions;
  let lastError: any;

  for (let attempt = 0; attempt <= retries; attempt++) {
    try {
      if (attempt > 0) {
        const delay = backoff * Math.pow(2, attempt - 1);
        logger.debug(
          `Retry attempt ${attempt}/${retries} for ${url} in ${delay}ms`,
        );
        await new Promise((resolve) => setTimeout(resolve, delay));
      }

      const controller = new AbortController();
      const id = setTimeout(() => controller.abort(), timeout);

      const response = await fetch(url, {
        ...options,
        signal: controller.signal,
      });

      clearTimeout(id);

      if (response.ok) {
        return response;
      }

      // Don't retry 4xx errors (client errors)
      if (response.status >= 400 && response.status < 500) {
        return response;
      }

      lastError = new Error(`HTTP ${response.status}: ${response.statusText}`);
    } catch (err: any) {
      lastError = err;

      // Don't retry aborts (manual timeouts) unless we want to
      if (err.name === "AbortError" && attempt === retries) {
        throw new Error(`Request timed out for ${url} after ${timeout}ms`);
      }
    }
  }

  throw lastError;
}

import {
  CanActivate,
  ExecutionContext,
  HttpException,
  HttpStatus,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import type { Response } from "express";
import {
  carriesThrottleBypassKey,
  getThrottleBypassHeader,
} from "../common/throttler/throttler.config";
import {
  API_KEY_HEADER,
  API_KEY_LIMIT_HEADERS,
  API_KEY_PREFIX,
  DATA_TERMS_URL,
} from "./api-key.constants";
import { ApiKeyService, type ResolvedApiKey } from "./api-key.service";
import {
  isForecastSharedCacheEnabled,
  keepResponsePrivate,
  varyResponseByKey,
} from "./private-cache";

/** The key a request presents, and whether it came in `X-API-Key` or as a Bearer token. */
export function readPresentedApiKeyWithSource(
  headers: Record<string, unknown> | undefined,
): { key: string; source: "header" | "bearer" } | null {
  const header = headers?.[API_KEY_HEADER];
  if (typeof header === "string" && header.trim()) {
    return { key: header.trim(), source: "header" };
  }
  const authorization = headers?.authorization;
  if (typeof authorization === "string") {
    const match = /^Bearer\s+(\S+)$/i.exec(authorization.trim());
    if (match && match[1].startsWith(API_KEY_PREFIX)) {
      return { key: match[1], source: "bearer" };
    }
  }
  return null;
}

/** The key a request presents, from `X-API-Key` or `Authorization: Bearer pfk_…`. */
export function readPresentedApiKey(
  headers: Record<string, unknown> | undefined,
): string | null {
  return readPresentedApiKeyWithSource(headers)?.key ?? null;
}

export interface ForecastRequest {
  headers: Record<string, unknown>;
  apiKey?: ResolvedApiKey;
}

/**
 * Admits park.fan's own frontend (the throttle bypass key it sends on every
 * server-side call) or the holder of an active data key, and refuses everyone
 * else with a 401 that says where the terms are.
 *
 * The cache policy is settled first, whatever the outcome, because a guard runs
 * before every interceptor and its 401 would otherwise leave with no
 * `Cache-Control` at all. By default every answer is private: Cloudflare keys
 * on the URL, not on the key. With `FORECAST_SHARED_CACHE=vary` (the Cloudflare
 * Vary rule is in place) a success keeps its public policy and varies on the
 * key headers instead. A key sent as a Bearer token stays private either way,
 * because the Vary rule does not name `Authorization`.
 *
 * A data key also counts against its hourly allowance; over it, the answer is
 * a 429 with `Retry-After` until the next full hour. park.fan's own frontend is
 * not counted.
 */
export async function authorizeForecastRequest(
  apiKeys: ApiKeyService,
  request: ForecastRequest,
  response: Response,
): Promise<void> {
  if (isForecastSharedCacheEnabled()) {
    varyResponseByKey(response, ["X-API-Key", getThrottleBypassHeader()]);
  } else {
    keepResponsePrivate(response);
  }
  if (carriesThrottleBypassKey(request.headers)) return;

  const presentedWith = readPresentedApiKeyWithSource(request.headers);
  const presented = presentedWith?.key ?? null;
  const resolved = presented ? await apiKeys.resolve(presented) : null;
  if (resolved) {
    if (presentedWith?.source === "bearer") keepResponsePrivate(response);
    const usage = await apiKeys.countRequest(resolved);
    if (usage && !response.headersSent) {
      response.setHeader(API_KEY_LIMIT_HEADERS.limit, String(usage.limit));
      response.setHeader(
        API_KEY_LIMIT_HEADERS.remaining,
        String(usage.remaining),
      );
      response.setHeader(
        API_KEY_LIMIT_HEADERS.reset,
        String(usage.resetSeconds),
      );
    }
    if (usage?.exceeded) {
      throw new HttpException(
        {
          error: "api-key-rate-limited",
          message: `This API key has used its ${usage.limit} requests for this hour.`,
          retryAfterSeconds: usage.resetSeconds,
          terms: DATA_TERMS_URL,
        },
        HttpStatus.TOO_MANY_REQUESTS,
      );
    }
    request.apiKey = resolved;
    return;
  }

  if (!response.headersSent) {
    response.setHeader("WWW-Authenticate", 'ApiKey header="X-API-Key"');
  }
  throw new UnauthorizedException({
    error: presented ? "api-key-invalid" : "api-key-required",
    message: presented
      ? "This API key is unknown or revoked."
      : "Forecasts need an API key in the X-API-Key header. Keys for non-commercial use are free on request.",
    terms: DATA_TERMS_URL,
  });
}

/**
 * The key store, looked up when a request needs it rather than injected. Nest
 * builds a route's guards and interceptors with every module that mounts the
 * controller, test modules included; a constructor dependency would make each
 * of those provide the store, though none of them ever runs the check.
 */
export function apiKeyStore(moduleRef: ModuleRef): ApiKeyService {
  return moduleRef.get(ApiKeyService, { strict: false });
}

/** Puts a whole route behind a data key; see `authorizeForecastRequest`. */
@Injectable()
export class ForecastAccessGuard implements CanActivate {
  constructor(private readonly moduleRef: ModuleRef) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const http = context.switchToHttp();
    await authorizeForecastRequest(
      apiKeyStore(this.moduleRef),
      http.getRequest<ForecastRequest>(),
      http.getResponse<Response>(),
    );
    return true;
  }
}

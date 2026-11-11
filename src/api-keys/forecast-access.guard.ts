import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import type { Response } from "express";
import { carriesThrottleBypassKey } from "../common/throttler/throttler.config";
import {
  API_KEY_HEADER,
  API_KEY_PREFIX,
  DATA_TERMS_URL,
} from "./api-key.constants";
import { ApiKeyService, type ResolvedApiKey } from "./api-key.service";
import { keepResponsePrivate } from "./private-cache";

/** The key a request presents, from `X-API-Key` or `Authorization: Bearer pfk_…`. */
export function readPresentedApiKey(
  headers: Record<string, unknown> | undefined,
): string | null {
  const header = headers?.[API_KEY_HEADER];
  if (typeof header === "string" && header.trim()) return header.trim();
  const authorization = headers?.authorization;
  if (typeof authorization === "string") {
    const match = /^Bearer\s+(\S+)$/i.exec(authorization.trim());
    if (match && match[1].startsWith(API_KEY_PREFIX)) return match[1];
  }
  return null;
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
 * The response is made private first, whatever the outcome: a guard runs
 * before every interceptor, so the 401 would otherwise leave with no
 * `Cache-Control` at all, and a 200 must never reach Cloudflare's shared
 * cache, which keys on the URL and not on the key.
 */
export async function authorizeForecastRequest(
  apiKeys: ApiKeyService,
  request: ForecastRequest,
  response: Response,
): Promise<void> {
  keepResponsePrivate(response);
  if (carriesThrottleBypassKey(request.headers)) return;

  const presented = readPresentedApiKey(request.headers);
  const resolved = presented ? await apiKeys.resolve(presented) : null;
  if (resolved) {
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

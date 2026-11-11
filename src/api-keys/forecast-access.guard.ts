import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  HttpException,
  HttpStatus,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { ModuleRef, Reflector } from "@nestjs/core";
import { ParksService } from "../parks/parks.service";
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
  bypassKeyOpensForecasts,
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
  params?: Record<string, string | undefined>;
  query?: unknown;
  apiKey?: ResolvedApiKey;
}

/** The four slugs that name a park, as the park routes carry them. */
export type ParkPath = [string, string, string, string];

/**
 * The park a request is about: the route's `:continent/:country/:city/:parkSlug`,
 * or `?park=continent/country/city/park` on a route that takes one (`/verify`).
 */
export function requestedParkPath(request: ForecastRequest): ParkPath | null {
  const { continent, country, city, parkSlug } = request.params ?? {};
  if (continent && country && city && parkSlug) {
    return [continent, country, city, parkSlug];
  }
  const park = (request.query as Record<string, unknown> | undefined)?.park;
  if (typeof park !== "string") return null;
  const segments = park.split("/").filter(Boolean);
  return segments.length === 4 ? (segments as ParkPath) : null;
}

/** How the check finds a park's id; the guard passes `ParksService`. */
export type ParkIdResolver = (path: ParkPath) => Promise<string | null>;

export interface ForecastAccessOptions {
  /** Resolves the requested park, for a key limited to some parks. */
  resolveParkId?: ParkIdResolver;
  /**
   * The route answers without a park (`/verify` without `?park=`), so a key
   * limited to some parks may call it. Every other route without a park in its
   * path refuses such a key, because it could reach any park's forecasts.
   */
  anyPark?: boolean;
}

/**
 * Admits the holder of an active data key, park.fan's own frontend included,
 * and refuses everyone else with a 401 that says where the terms are. Until
 * `FORECAST_BYPASS_KEY=off`, the throttle bypass key the frontend sends on
 * every server-side call opens the routes too.
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
  options: ForecastAccessOptions = {},
): Promise<void> {
  if (isForecastSharedCacheEnabled()) {
    varyResponseByKey(response, ["X-API-Key", getThrottleBypassHeader()]);
  } else {
    keepResponsePrivate(response);
  }
  if (bypassKeyOpensForecasts() && carriesThrottleBypassKey(request.headers)) {
    return;
  }

  const presentedWith = readPresentedApiKeyWithSource(request.headers);
  const presented = presentedWith?.key ?? null;
  const resolved = presented ? await apiKeys.resolve(presented) : null;
  if (resolved) {
    if (presentedWith?.source === "bearer") keepResponsePrivate(response);
    await checkAllowedPark(resolved, request, options);
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
 * Refuses a key limited to some parks when the request is about another park,
 * or about no park on a route that could reach any. Before the hourly count, so
 * a refused request does not use up the allowance.
 */
async function checkAllowedPark(
  key: ResolvedApiKey,
  request: ForecastRequest,
  options: ForecastAccessOptions,
): Promise<void> {
  if (!key.allowedParkIds) return;
  const path = requestedParkPath(request);
  if (!path) {
    if (options.anyPark) return;
    throw new ForbiddenException({
      error: "api-key-park-required",
      message:
        "This API key is limited to certain parks, so it can only call routes that name one.",
      terms: DATA_TERMS_URL,
    });
  }
  const parkId = options.resolveParkId
    ? await options.resolveParkId(path)
    : null;
  if (!parkId || !key.allowedParkIds.includes(parkId)) {
    throw new ForbiddenException({
      error: "api-key-park-not-allowed",
      message: "This API key does not cover this park.",
      terms: DATA_TERMS_URL,
    });
  }
}

/** Finds a park's id by its slugs, through `ParksService`. */
export function parkIdResolver(moduleRef: ModuleRef): ParkIdResolver {
  return async (path) =>
    (
      await moduleRef
        .get(ParksService, { strict: false })
        .findByGeographicPath(...path)
    )?.id ?? null;
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

/** Set by `RequiresApiKey({ anyPark: true })`; see `ForecastAccessOptions.anyPark`. */
export const API_KEY_ANY_PARK = "apiKeyAnyPark";

/** Puts a whole route behind a data key; see `authorizeForecastRequest`. */
@Injectable()
export class ForecastAccessGuard implements CanActivate {
  constructor(
    private readonly moduleRef: ModuleRef,
    private readonly reflector: Reflector,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const http = context.switchToHttp();
    await authorizeForecastRequest(
      apiKeyStore(this.moduleRef),
      http.getRequest<ForecastRequest>(),
      http.getResponse<Response>(),
      {
        resolveParkId: parkIdResolver(this.moduleRef),
        anyPark:
          this.reflector.get<boolean>(
            API_KEY_ANY_PARK,
            context.getHandler(),
          ) === true,
      },
    );
    return true;
  }
}

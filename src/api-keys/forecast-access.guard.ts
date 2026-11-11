import {
  CanActivate,
  ExecutionContext,
  ForbiddenException,
  HttpException,
  HttpStatus,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { ModuleRef } from "@nestjs/core";
import { ParksService } from "../parks/parks.service";
import { ParkRenameService } from "../parks/services/park-rename.service";
import type { Response } from "express";
import {
  carriesThrottleBypassKey,
  getThrottleBypassHeader,
} from "../common/throttler/throttler.config";
import {
  API_KEY_EXPIRES_HEADER,
  API_KEY_HEADER,
  API_KEY_LIMIT_HEADERS,
  API_KEY_PREFIX,
  DATA_TERMS_URL,
  bypassKeyOpensForecasts,
} from "./api-key.constants";
import {
  ApiKeyService,
  type ApiKeyLookup,
  type ResolvedApiKey,
} from "./api-key.service";
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
 * or, only where `fromQuery` says the route takes one (`/verify`),
 * `?park=continent/country/city/park`. Read from the query anywhere else, a
 * park-limited key could name its own park there and reach a route that
 * serves any park.
 */
export function requestedParkPath(
  request: ForecastRequest,
  fromQuery = false,
): ParkPath | null {
  const { continent, country, city, parkSlug } = request.params ?? {};
  if (continent && country && city && parkSlug) {
    return [continent, country, city, parkSlug];
  }
  if (!fromQuery) return null;
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
   * `/verify` only: the park may come as `?park=`, and without one the key
   * passes. Every other route without a park in its path refuses a key
   * limited to some parks, because it could reach any park's forecasts.
   */
  verify?: boolean;
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
 * not counted. A key with an end date reports it in `X-API-Key-Expires`, and
 * past it is refused with `api-key-expired`.
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
  const found = presented ? await apiKeys.lookup(presented) : null;
  if (found?.status === "active") {
    const resolved = found.key;
    if (presentedWith?.source === "bearer") keepResponsePrivate(response);
    await checkAllowedPark(resolved, request, options);
    if (resolved.expiresAt && !response.headersSent) {
      response.setHeader(API_KEY_EXPIRES_HEADER, resolved.expiresAt);
    }
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
  throw keyRefusal(
    found,
    "Forecasts need an API key in the X-API-Key header. Keys for non-commercial use are free on request.",
  );
}

/**
 * The 401 for a request without an active key: none sent, one past its end
 * date, or one that is unknown or revoked.
 */
export function keyRefusal(
  found: ApiKeyLookup | null,
  whenMissing: string,
): UnauthorizedException {
  if (found?.status === "expired") {
    return new UnauthorizedException({
      error: "api-key-expired",
      message: `This API key expired on ${found.expiresAt}.`,
      expiresAt: found.expiresAt,
      terms: DATA_TERMS_URL,
    });
  }
  return new UnauthorizedException({
    error: found ? "api-key-invalid" : "api-key-required",
    message: found ? "This API key is unknown or revoked." : whenMissing,
    terms: DATA_TERMS_URL,
  });
}

/**
 * Refuses a key limited to some parks when the request is about another park,
 * or about no park on a route that could reach any. Before the hourly count, so
 * a refused request does not use up the allowance.
 */
export async function checkAllowedPark(
  key: ResolvedApiKey,
  request: ForecastRequest,
  options: ForecastAccessOptions,
): Promise<void> {
  if (!key.allowedParkIds) return;
  const path = requestedParkPath(request, options.verify === true);
  if (!path) {
    if (options.verify) return;
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

/**
 * Finds a park's id by its slugs, through `ParksService`, and through the
 * rename aliases when the slugs are an old path: the park route answers an old
 * path with a 301, and a limited key must reach that redirect, not a 403.
 */
export function parkIdResolver(moduleRef: ModuleRef): ParkIdResolver {
  return async ([continent, country, city, slug]) => {
    const parks = moduleRef.get(ParksService, { strict: false });
    const park = await parks.findByGeographicPath(
      continent,
      country,
      city,
      slug,
    );
    if (park) return park.id;
    const current = await moduleRef
      .get(ParkRenameService, { strict: false })
      .resolveAlias({
        continentSlug: continent,
        countrySlug: country,
        citySlug: city,
        slug,
      });
    if (!current) return null;
    const renamed = await parks.findByGeographicPath(
      current.continentSlug,
      current.countrySlug,
      current.citySlug,
      current.slug,
    );
    return renamed?.id ?? null;
  };
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
      { resolveParkId: parkIdResolver(this.moduleRef) },
    );
    return true;
  }
}

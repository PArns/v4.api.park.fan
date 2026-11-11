import {
  CanActivate,
  ExecutionContext,
  Injectable,
  UnauthorizedException,
} from "@nestjs/common";
import { carriesThrottleBypassKey } from "../common/throttler/throttler.config";
import {
  API_KEY_HEADER,
  API_KEY_PREFIX,
  DATA_TERMS_URL,
} from "./api-key.constants";
import { ApiKeyService } from "./api-key.service";

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

/**
 * Lets a forecast route answer only park.fan's own frontend (the throttle
 * bypass key it sends on every server-side call) or the holder of an active
 * data key. Everyone else gets a 401 that says where the terms are.
 */
@Injectable()
export class ForecastAccessGuard implements CanActivate {
  constructor(private readonly apiKeys: ApiKeyService) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const http = context.switchToHttp();
    const request = http.getRequest<{
      headers: Record<string, unknown>;
      apiKey?: unknown;
    }>();
    if (carriesThrottleBypassKey(request.headers)) return true;

    const presented = readPresentedApiKey(request.headers);
    const resolved = presented ? await this.apiKeys.resolve(presented) : null;
    if (resolved) {
      request.apiKey = resolved;
      return true;
    }

    http
      .getResponse<{ setHeader?: (name: string, value: string) => void }>()
      .setHeader?.("WWW-Authenticate", 'ApiKey header="X-API-Key"');
    throw new UnauthorizedException({
      error: presented ? "api-key-invalid" : "api-key-required",
      message: presented
        ? "This API key is unknown or revoked."
        : "Forecasts need an API key in the X-API-Key header. Keys for non-commercial use are free on request.",
      terms: DATA_TERMS_URL,
    });
  }
}

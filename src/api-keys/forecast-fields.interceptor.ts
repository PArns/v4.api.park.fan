import {
  applyDecorators,
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
  SetMetadata,
  UseInterceptors,
} from "@nestjs/common";
import { ModuleRef, Reflector } from "@nestjs/core";
import { ApiQuery, ApiSecurity } from "@nestjs/swagger";
import type { Response } from "express";
import { from, map, Observable, switchMap } from "rxjs";
import {
  apiKeyStore,
  authorizeForecastRequest,
  parkIdResolver,
  type ForecastRequest,
} from "./forecast-access.guard";
import { omitPaths } from "./omit-paths";

const FORECAST_FIELDS_KEY = "forecastFields";

/** Whether the query asks for the forecast fields: `forecast=true` or `forecast=1`. */
export function wantsForecast(query: unknown): boolean {
  const value = (query as Record<string, unknown> | undefined)?.forecast;
  return value === "true" || value === "1";
}

/**
 * For a route whose answer mixes live or historical data with forecast fields.
 *
 * Without `?forecast=true` the named fields are removed and the rest keeps its
 * public cache policy. With it the caller needs a data key, the answer is
 * complete and private. Two URLs rather than one that changes with the key,
 * because Cloudflare would otherwise hand a keyed caller the copy it cached
 * for a caller without one, or the other way round.
 */
@Injectable()
export class ForecastFieldsInterceptor implements NestInterceptor {
  constructor(
    private readonly reflector: Reflector,
    private readonly moduleRef: ModuleRef,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const paths =
      this.reflector.get<string[]>(FORECAST_FIELDS_KEY, context.getHandler()) ??
      [];
    const http = context.switchToHttp();
    const request = http.getRequest<ForecastRequest>();

    if (wantsForecast(request.query)) {
      return from(
        authorizeForecastRequest(
          apiKeyStore(this.moduleRef),
          request,
          http.getResponse<Response>(),
          { resolveParkId: parkIdResolver(this.moduleRef) },
        ),
      ).pipe(switchMap(() => next.handle()));
    }
    return next.handle().pipe(map((body) => omitPaths(body, paths)));
  }
}

/**
 * Marks the forecast fields of a mixed route; see `ForecastFieldsInterceptor`.
 * Paths use `omitPaths` syntax, e.g. `attractions[].bestVisitTimes`.
 */
export const ForecastFields = (
  ...paths: string[]
): MethodDecorator & ClassDecorator =>
  applyDecorators(
    SetMetadata(FORECAST_FIELDS_KEY, paths),
    UseInterceptors(ForecastFieldsInterceptor),
    ApiSecurity("api-key"),
    ApiQuery({
      name: "forecast",
      required: false,
      description: `\`true\` adds the forecast fields (${paths.join(", ")}); needs a data key in \`X-API-Key\`. Without it they are left out.`,
    }),
  );

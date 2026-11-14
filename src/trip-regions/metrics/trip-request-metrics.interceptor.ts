import {
  applyDecorators,
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
  SetMetadata,
  UseInterceptors,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { Observable, tap } from "rxjs";
import {
  answerFactsOf,
  errorOf,
  type TripMetricRoute,
} from "./trip-request-facts";
import { withTripTrace, type TripRequestTrace } from "./trip-request-trace";
import { TripRequestMetricsService } from "./trip-request-metrics.service";

const ROUTE_KEY = "trip-request-metric:route";

/**
 * Records every answer of the route it sits on as a metric row, refused and
 * failed ones too: the query's own validation runs inside it, the key guard
 * before it. docs/frontend/trip-regions.md#what-the-server-records.
 */
export function MeasureTripRequest(route: TripMetricRoute) {
  return applyDecorators(
    SetMetadata(ROUTE_KEY, route),
    UseInterceptors(TripRequestMetricsInterceptor),
  );
}

/**
 * Opens a request trace around the route (the services note their caches and
 * the trip's shape into it), times the answer and hands its row to
 * {@link TripRequestMetricsService}. Recording never touches the answer: an
 * answer whose facts cannot be read is recorded without them, and a row that
 * cannot be built is logged and left out.
 */
@Injectable()
export class TripRequestMetricsInterceptor implements NestInterceptor {
  constructor(
    private readonly reflector: Reflector,
    private readonly metrics: TripRequestMetricsService,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const route = this.reflector.get<TripMetricRoute | undefined>(
      ROUTE_KEY,
      context.getHandler(),
    );
    if (!route) return next.handle();
    const request = context
      .switchToHttp()
      .getRequest<{ params?: Record<string, unknown> }>();
    const measure = {
      route,
      region: request.params?.region,
      caller: "site" as const,
    };
    const trace: TripRequestTrace = {};
    const startedAt = performance.now();
    // `handle()` binds the route's async context when it is called, so the
    // trace has to be open then for the services to see it.
    return withTripTrace(trace, () => next.handle()).pipe(
      tap({
        next: (body) =>
          this.metrics.recordAnswer(measure, trace, startedAt, {
            status: 200,
            facts: answerFactsOf(route, body),
          }),
        error: (error: unknown) => {
          const { status, code } = errorOf(error);
          this.metrics.recordAnswer(measure, trace, startedAt, {
            status,
            errorCode: code,
          });
        },
      }),
    );
  }
}

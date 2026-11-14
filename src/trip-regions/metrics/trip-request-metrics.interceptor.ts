import {
  applyDecorators,
  CallHandler,
  ExecutionContext,
  Injectable,
  Logger,
  NestInterceptor,
  SetMetadata,
  UseInterceptors,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { Observable, tap } from "rxjs";
import type { TripPlanDto } from "../dto/trip-region-response.dto";
import type { TripWhen } from "../trip-when";
import {
  errorOf,
  metricRowOf,
  planAnswerFacts,
  regionOf,
  whenAnswerFacts,
  type TripAnswerFacts,
  type TripErrorCode,
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

/** What each route's answer adds to its row. */
const FACTS: Record<
  TripMetricRoute,
  (body: unknown) => TripAnswerFacts | undefined
> = {
  plan: (body) => planAnswerFacts(body as TripPlanDto),
  when: (body) => whenAnswerFacts(body as TripWhen),
  best_days: () => undefined,
};

/**
 * Opens a request trace around the route (the services note their caches and
 * the trip's shape into it), times the answer and hands its row to
 * {@link TripRequestMetricsService}. Recording never touches the answer: an
 * answer whose facts cannot be read is recorded without them, and a row that
 * cannot be built is logged and left out.
 */
@Injectable()
export class TripRequestMetricsInterceptor implements NestInterceptor {
  private readonly logger = new Logger(TripRequestMetricsInterceptor.name);

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
    const region = regionOf(request.params?.region);
    const trace: TripRequestTrace = {};
    const started = performance.now();
    const record = (
      status: number,
      facts?: TripAnswerFacts,
      errorCode?: TripErrorCode,
    ) => {
      try {
        this.metrics.record(
          metricRowOf({
            route,
            region,
            at: new Date(),
            latencyMs: performance.now() - started,
            trace,
            status,
            errorCode,
            facts,
          }),
        );
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        this.logger.warn(
          `Trip request metric for ${route} not recorded: ${message}`,
        );
      }
    };
    // `handle()` binds the route's async context when it is called, so the
    // trace has to be open then for the services to see it.
    return withTripTrace(trace, () => next.handle()).pipe(
      tap({
        next: (body) => {
          let facts: TripAnswerFacts | undefined;
          try {
            facts = FACTS[route](body);
          } catch {
            facts = undefined;
          }
          record(200, facts);
        },
        error: (error: unknown) => {
          const { status, code } = errorOf(error);
          record(status, undefined, code);
        },
      }),
    );
  }
}

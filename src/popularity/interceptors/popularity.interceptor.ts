import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { tap } from "rxjs/operators";
import { PopularityService } from "../popularity.service";

/**
 * Popularity Interceptor
 *
 * Background-tracks requests to parks and attractions to identify "Hot" entities.
 * These stats are used to prioritize cache pre-warming.
 */
@Injectable()
export class PopularityInterceptor implements NestInterceptor {
  constructor(private readonly popularityService: PopularityService) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const request = context.switchToHttp().getRequest();
    const url = request.url;
    const params = request.params;

    // We only track successful GET requests
    if (request.method !== "GET") {
      return next.handle();
    }

    return next.handle().pipe(
      tap(() => {
        // Track after successful response
        this.trackRequest(url, params).catch(() => {
          /* ignore tracking errors */
        });
      }),
    );
  }

  private async trackRequest(url: string, params: any): Promise<void> {
    // 1. Attraction Hits (UUID based)
    if (params.attractionId) {
      await this.popularityService.recordAttractionHit(params.attractionId);
    }

    // 2. Park Hits (Slug based or UUID based)
    // Most park routes use :continent/:country/:city/:parkSlug
    // We can't easily resolve slug to ID here without DB call, but many requests
    // already have the parkId in the response or we can detect it from specific routes.

    // For now, if we have a parkId param (common in some internal/analytics routes)
    if (params.parkId) {
      await this.popularityService.recordParkHit(params.parkId);
    }

    // If it's a main park route, we might need to track by a combined slug or wait for
    // a better place to track.
    // Optimization: The ParkIntegrationService could also trigger a hit when building response.
  }
}

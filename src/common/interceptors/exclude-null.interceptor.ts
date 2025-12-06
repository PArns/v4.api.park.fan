import {
  Injectable,
  NestInterceptor,
  ExecutionContext,
  CallHandler,
} from "@nestjs/common";
import { Observable } from "rxjs";
import { map } from "rxjs/operators";

/**
 * Exclude Null Interceptor
 *
 * Automatically removes null values from all API responses.
 * Can be disabled per-request via ?debug=true query parameter.
 *
 * Why:
 * - Cleaner API responses (no null clutter)
 * - Reduced payload size
 * - Better frontend experience (undefined vs null handling)
 *
 * Usage:
 * - GET /v1/parks/magic-kingdom-park          → null values removed
 * - GET /v1/parks/magic-kingdom-park?debug=true → null values included
 */
@Injectable()
export class ExcludeNullInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const request = context.switchToHttp().getRequest();
    const debug = request.query?.debug === "true";

    // If debug mode, skip null removal
    if (debug) {
      return next.handle();
    }

    // Otherwise, remove null values from response
    return next.handle().pipe(map((value) => this.removeNullValues(value)));
  }

  /**
   * Recursively removes null values from objects and arrays
   *
   * @param obj - Value to process
   * @returns Value with null values removed (undefined values are ignored by JSON.stringify)
   */
  private removeNullValues(obj: unknown): unknown {
    if (obj === null || obj === undefined) {
      return undefined; // undefined is ignored by JSON.stringify()
    }

    if (Array.isArray(obj)) {
      return obj.map((item) => this.removeNullValues(item));
    }

    if (obj instanceof Date) {
      return obj;
    }

    if (typeof obj === "object" && obj !== null) {
      return Object.fromEntries(
        Object.entries(obj)
          .filter(([_, value]) => value !== null)
          .map(([key, value]) => [key, this.removeNullValues(value)]),
      );
    }

    return obj;
  }
}

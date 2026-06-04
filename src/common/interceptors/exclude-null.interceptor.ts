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
 * Implementation note:
 * This runs as a global interceptor on EVERY response, including the large,
 * fully-cached payloads (integrated park, calendar, discovery tree). It
 * therefore strips nulls **in place** rather than rebuilding the whole tree
 * via Object.fromEntries — the old approach deep-cloned every object/array on
 * every request, which on the biggest payloads was the single largest
 * non-I/O cost per request. Mutation is safe here: responses are always
 * freshly built DTOs or freshly `JSON.parse`d cache payloads — never shared
 * object references (the only persistent in-memory cache is the root HTML
 * string, which is immutable and untouched by this pass).
 *
 * The serialized output is identical to the previous behaviour: null object
 * keys are dropped; null array elements are left as-is (JSON renders both a
 * dropped→undefined and a kept null array slot as `null`).
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
   * Recursively strips null-valued keys from objects, in place.
   *
   * - Top-level `null`/`undefined` → `undefined` (JSON.stringify omits it).
   * - Objects: delete null-valued keys, recurse into nested objects/arrays.
   * - Arrays: recurse into element objects; null elements are kept (serialize
   *   to `null`, matching the previous map(null→undefined) behaviour).
   * - `Date` and primitives are returned untouched.
   */
  private removeNullValues(obj: unknown): unknown {
    if (obj === null || obj === undefined) {
      return undefined; // undefined is ignored by JSON.stringify()
    }

    if (typeof obj !== "object" || obj instanceof Date) {
      return obj;
    }

    if (Array.isArray(obj)) {
      for (const item of obj) {
        if (item !== null && typeof item === "object") {
          this.removeNullValues(item);
        }
      }
      return obj;
    }

    const record = obj as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      const value = record[key];
      if (value === null) {
        delete record[key];
      } else if (typeof value === "object") {
        this.removeNullValues(value);
      }
    }
    return obj;
  }
}

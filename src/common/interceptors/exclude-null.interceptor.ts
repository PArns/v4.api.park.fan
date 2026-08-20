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
 *
 * The administrative surface is exempt, and that is not a convenience.
 * `/v1/admin/*` answers with curated-field descriptors whose whole purpose is
 * to say what a field is: `syncedValue`, `curatedValue` and `resolvedValue`
 * are each null exactly when there is nothing there, which is the common case
 * and the most informative one. Stripped, an editor cannot tell "no correction
 * on this field" from "this field does not exist", and the PATCH contract —
 * send `null` to clear a correction — cannot survive a GET → edit → PATCH
 * round trip, because the null never arrived. The same applies to
 * `park_seasons`, where `dates: null` MEANS "every day between start and end"
 * and must stay distinguishable from an empty array.
 *
 * Matched as a path segment, the same way CacheControlInterceptor decides what
 * is private, so the two cannot disagree about what counts as admin.
 */
/** "/admin" as a path segment, so a slug like ".../adminton-park" is not one. */
const ADMIN_PATH = /\/admin(\/|$|\?)/;

@Injectable()
export class ExcludeNullInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    const request = context.switchToHttp().getRequest();
    const debug = request.query?.debug === "true";
    const path: string = request.route?.path ?? request.url ?? "";

    // Debug mode, or the admin surface, where a null is data — see the class
    // doc. Deliberately not "the client can pass ?debug=true": that also
    // un-strips nested payloads the admin does not control, and it is one
    // forgotten call site away from a silently broken form.
    if (debug || ADMIN_PATH.test(path)) {
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

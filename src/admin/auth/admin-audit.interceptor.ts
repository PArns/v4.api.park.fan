import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
  SetMetadata,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { Observable, tap } from "rxjs";
import { AdminAuditService } from "./admin-audit.service";
import type { RequestWithAdmin } from "./admin-principal";

export const ADMIN_SELF_AUDITED_KEY = "adminSelfAudited";

/**
 * Marks a handler that writes its own, richer audit row — a curation endpoint
 * that knows the before value and the reason. The interceptor then keeps out
 * of its way instead of writing a second, thinner row for the same action.
 */
export const SelfAudited = (): MethodDecorator =>
  SetMetadata(ADMIN_SELF_AUDITED_KEY, true);

/** Request bodies are logged, so anything that looks like a credential is
 *  replaced first. Matched case-insensitively against the key name. */
const REDACTED_KEYS = /pass|password|secret|token|totp|code|key/i;

/**
 * Records every mutating admin request that does not record itself.
 *
 * The point is coverage, not detail. There are ~35 administrative endpoints
 * and only a handful of them are curation writes worth a hand-written audit
 * row; the rest trigger jobs, flush caches or repair data. Those are exactly
 * the actions somebody wants to reconstruct after an incident ("who reset the
 * cache at 03:12?"), and exactly the ones nobody would remember to instrument
 * one by one. So the default is automatic and the exception is explicit.
 *
 * Only successful requests are recorded. A rejected call changed nothing, and
 * a log where most rows are failed attempts is a log nobody reads.
 */
@Injectable()
export class AdminAuditInterceptor implements NestInterceptor {
  constructor(
    private readonly reflector: Reflector,
    private readonly audit: AdminAuditService,
  ) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const request = context
      .switchToHttp()
      .getRequest<
        RequestWithAdmin & { body?: unknown; route?: { path?: string } }
      >();

    const method = request.method?.toUpperCase();
    if (method === "GET" || method === "HEAD" || method === "OPTIONS") {
      return next.handle();
    }

    const selfAudited = this.reflector.getAllAndOverride<boolean>(
      ADMIN_SELF_AUDITED_KEY,
      [context.getHandler(), context.getClass()],
    );
    if (selfAudited) return next.handle();

    return next.handle().pipe(
      tap(() => {
        const admin = request.admin;
        if (!admin) return;
        void this.audit.record({
          actor: admin,
          action: `job.${routeAction(request)}`,
          entityType: "system",
          entityId: null,
          entityLabel: `${method} ${routeAction(request)}`,
          after: redact(request.body),
        });
      }),
    );
  }
}

/**
 * The action name for a request: the admin route without its prefix, with
 * concrete path parameters left in. `/v1/admin/pcn/train` becomes `pcn/train`,
 * which is what somebody scanning the log is actually looking for — the
 * parameterised `pcn/:action` would collapse three different jobs into one.
 */
function routeAction(request: { originalUrl?: string; url?: string }): string {
  const raw = (request.originalUrl ?? request.url ?? "").split("?")[0];
  const marker = "/admin/";
  const index = raw.indexOf(marker);
  return index === -1 ? raw : raw.slice(index + marker.length);
}

function redact(body: unknown): Record<string, unknown> | null {
  if (!body || typeof body !== "object" || Array.isArray(body)) return null;
  const output: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(body as Record<string, unknown>)) {
    output[key] = REDACTED_KEYS.test(key) ? "[redacted]" : truncate(value);
  }
  return output;
}

/**
 * Keep one audit row small.
 *
 * `retire-attractions` accepts an unbounded array and `merge-duplicate-parks`
 * a report-sized body; storing those verbatim would turn the audit table into
 * a second copy of the request log. The count is the part a human reads.
 */
function truncate(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.length <= 5
      ? value
      : { count: value.length, first: value.slice(0, 3) };
  }
  if (typeof value === "string" && value.length > 500) {
    return `${value.slice(0, 500)}…`;
  }
  return value;
}

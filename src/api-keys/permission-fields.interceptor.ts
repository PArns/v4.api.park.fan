import {
  CallHandler,
  ExecutionContext,
  Injectable,
  NestInterceptor,
} from "@nestjs/common";
import { Reflector } from "@nestjs/core";
import { map, Observable } from "rxjs";
import type { ForecastRequest } from "./forecast-access.guard";
import { omitPaths } from "./omit-paths";
import {
  KEYLESS_ACCESS,
  PERMISSION_FIELDS,
  withheldPaths,
} from "./permissions";

/** The metadata `@PermissionFields` leaves: which mixed answer the route returns. */
export const PERMISSION_FIELDS_METADATA = "permissionFields";

/**
 * Removes from a park or ride detail every field whose permission the caller
 * does not hold, as the route's guard recorded it (`request.apiAccess`). The
 * answer is already private by then: the guard made it so for any caller who
 * is not keyless, and a keyless caller never reaches these routes.
 */
@Injectable()
export class PermissionFieldsInterceptor implements NestInterceptor {
  constructor(private readonly reflector: Reflector) {}

  intercept(context: ExecutionContext, next: CallHandler): Observable<unknown> {
    const kind = this.reflector.get<keyof typeof PERMISSION_FIELDS>(
      PERMISSION_FIELDS_METADATA,
      context.getHandler(),
    );
    if (!kind) return next.handle();
    // Without the guard's verdict nothing is assumed: the caller gets what a
    // keyless caller holds, so a route that lost its guard leaks no field.
    const held =
      context.switchToHttp().getRequest<ForecastRequest>().apiAccess
        ?.permissions ?? KEYLESS_ACCESS;
    const paths = withheldPaths(kind, held);
    if (paths.length === 0) return next.handle();
    return next.handle().pipe(map((body) => omitPaths(body, paths)));
  }
}

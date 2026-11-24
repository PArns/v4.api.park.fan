import {
  Controller,
  UseGuards,
  UseInterceptors,
  applyDecorators,
} from "@nestjs/common";
import { ApiSecurity, ApiTags } from "@nestjs/swagger";
import { AdminAuditInterceptor } from "./admin-audit.interceptor";
import { AdminAuthGuard } from "./admin-auth.guard";

type Interceptor = Parameters<typeof UseInterceptors>[number];

/**
 * An audited admin controller under `/v1/<path>`: the session guard, the
 * audit row for every write, and the Swagger tag, in one decorator.
 *
 * One decorator rather than five, so a controller split off another cannot
 * keep the route prefix and lose the guard on the way. Interceptors passed
 * here run after the audit interceptor.
 */
export function AdminApi(path: string, ...interceptors: Interceptor[]) {
  return applyDecorators(
    ApiTags("admin"),
    ApiSecurity("admin-auth"),
    Controller(path),
    UseGuards(AdminAuthGuard),
    UseInterceptors(AdminAuditInterceptor, ...interceptors),
  );
}

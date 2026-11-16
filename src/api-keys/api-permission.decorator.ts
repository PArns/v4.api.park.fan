import {
  applyDecorators,
  SetMetadata,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { ApiResponse, ApiSecurity } from "@nestjs/swagger";
import { DATA_TERMS_URL, DEVELOPER_PORTAL_URL } from "./api-key.constants";
import {
  API_PERMISSION_METADATA,
  ForecastAccessGuard,
  type ApiPermissionMetadata,
} from "./forecast-access.guard";
import {
  KEYLESS_PERMISSIONS,
  type ApiPermission as Permission,
  type RoutePermission,
} from "./permissions";
import {
  PERMISSION_FIELDS_METADATA,
  PermissionFieldsInterceptor,
} from "./permission-fields.interceptor";

/** The metadata `@OpenRoute` leaves: why a public route needs no permission. */
export const API_OPEN_METADATA = "apiOpen";

/**
 * Puts a public route behind one permission (`permissions.ts`): the guard, and
 * the Swagger security entry with the refusals it can answer. Every public
 * route carries this or `@OpenRoute`; `api-permission.coverage.spec.ts` fails
 * on one that carries neither.
 *
 * `parkRequired` (default false) refuses a key limited to some parks on a
 * route that names no park, because the route could reach any park's data.
 */
export const ApiPermission = (
  permission: RoutePermission,
  options: { parkRequired?: boolean } = {},
): MethodDecorator & ClassDecorator => {
  const metadata: ApiPermissionMetadata = {
    permission,
    parkRequired: options.parkRequired ?? false,
  };
  const keyless = (KEYLESS_PERMISSIONS as readonly string[]).includes(
    permission,
  );
  return applyDecorators(
    SetMetadata(API_PERMISSION_METADATA, metadata),
    UseGuards(ForecastAccessGuard),
    ...(keyless ? [] : [ApiSecurity("api-key")]),
    ApiResponse({
      status: 401,
      description: keyless
        ? "`api-key-invalid` or `api-key-expired`: a key was sent and is not usable. Without a key this route is open."
        : `\`api-key-required\`, \`api-key-invalid\` or \`api-key-expired\`. Needs a data key carrying \`${permission}\` in \`X-API-Key\`; keys: ${DEVELOPER_PORTAL_URL}, terms: ${DATA_TERMS_URL}`,
    }),
    ApiResponse({
      status: 403,
      description:
        "`api-key-permission-missing` (with `requiredPermission`): the key does not carry this route's permission. `api-key-park-not-allowed` or `api-key-park-required`: the key is limited to other parks.",
    }),
  );
};

/**
 * Marks a public route that serves no data a key would license: health,
 * the site's own push, trips, assistant and statistics beacons, the key check
 * itself. `reason` is for the reader and the coverage spec.
 */
export const OpenRoute = (reason: string): MethodDecorator & ClassDecorator =>
  SetMetadata(API_OPEN_METADATA, reason);

/**
 * Drops the fields of a mixed answer (the park or ride detail) whose
 * permission the caller lacks (`PERMISSION_FIELDS`). Goes with an
 * `@ApiPermission`, whose guard decides what the caller holds.
 */
export const PermissionFields = (
  kind: "park" | "ride",
): MethodDecorator & ClassDecorator =>
  applyDecorators(
    SetMetadata(PERMISSION_FIELDS_METADATA, kind),
    UseInterceptors(PermissionFieldsInterceptor),
  );

export type { Permission };

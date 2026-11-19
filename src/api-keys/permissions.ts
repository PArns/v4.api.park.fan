import { BadRequestException } from "@nestjs/common";

/**
 * What a data key may read, permission by permission, in groups an admin can
 * grant as a whole. Without a key a caller gets `KEYLESS_PERMISSIONS` and
 * nothing else: the live wait times. Every other public route names the one
 * permission it needs (`@ApiPermission`), and the two mixed answers (park and
 * ride detail) drop each field whose permission the caller lacks
 * (`PERMISSION_FIELDS`).
 *
 * The frontend mirrors the ids in `lib/developers/permissions.ts` for its
 * portal, its key request form and the admin's key dialog; `GET
 * /v1/api-keys/permissions` serves this catalog to anyone else.
 */

/** Every permission a key can carry. */
export const API_PERMISSIONS = [
  "live.waits",
  "live.schedule",
  "live.weather",
  "parks.directory",
  "parks.details",
  "parks.seasons",
  "rides.details",
  "rides.profiles",
  "stats.live",
  "stats.history",
  "stats.typical-waits",
  "stats.platform",
  "stats.accuracy",
  "forecasts.crowd",
  "forecasts.waits",
  "planner.day",
  "planner.trip",
  "travel.regions",
  "travel.tickets",
  "travel.school-breaks",
] as const;
export type ApiPermission = (typeof API_PERMISSIONS)[number];

/**
 * Held only by park.fan's own frontend (`usage: internal`) and the throttle
 * bypass key while it still opens data routes: debug routes nobody else
 * should call. Never granted, never listed in the catalog.
 */
export const INTERNAL_PERMISSION = "internal" as const;

/** A permission a route can require: a grantable one, or the internal one. */
export type RoutePermission = ApiPermission | typeof INTERNAL_PERMISSION;

/** A group of permissions, granted or shown together. */
export interface ApiPermissionGroup {
  id: string;
  /** English, for the catalog route; the frontend translates by id. */
  label: string;
  permissions: readonly ApiPermission[];
}

/** The groups, in the order every list shows them. Each permission is in exactly one. */
export const API_PERMISSION_GROUPS: readonly ApiPermissionGroup[] = [
  {
    id: "live",
    label: "Live data",
    permissions: ["live.waits", "live.schedule", "live.weather"],
  },
  {
    id: "parks",
    label: "Parks",
    permissions: ["parks.directory", "parks.details", "parks.seasons"],
  },
  {
    id: "rides",
    label: "Rides, shows and restaurants",
    permissions: ["rides.details", "rides.profiles"],
  },
  {
    id: "stats",
    label: "Statistics",
    permissions: [
      "stats.live",
      "stats.history",
      "stats.typical-waits",
      "stats.platform",
      "stats.accuracy",
    ],
  },
  {
    id: "forecasts",
    label: "Forecasts",
    permissions: ["forecasts.crowd", "forecasts.waits"],
  },
  {
    id: "planner",
    label: "Planner",
    permissions: ["planner.day", "planner.trip"],
  },
  {
    id: "travel",
    label: "Travel data",
    permissions: ["travel.regions", "travel.tickets", "travel.school-breaks"],
  },
];

/** What each permission opens, in English, for the catalog route and Swagger. */
export const API_PERMISSION_DESCRIPTIONS: Record<ApiPermission, string> = {
  "live.waits":
    "Current wait times and status of every attraction (/wait-times). Open without a key.",
  "live.schedule":
    "Opening hours and the park calendar's schedule (/schedule, `schedule` in the park and ride detail, show times).",
  "live.weather":
    "Current weather, forecast and nowcast from Open-Meteo (/weather, `weather` in the park detail).",
  "parks.directory":
    "The park directory: lists by continent, country and city, discovery, search, nearby parks, the ride sitemap, most viewed and popular parks.",
  "parks.details":
    "A park's detail (/v1/parks/{…}/{park}): name, address, coordinates, time zone, links, facts. Its other fields need their own permissions.",
  "parks.seasons":
    "Seasons and events (/seasons), whether a ride is in season, and planned maintenance windows.",
  "rides.details":
    "The ride, show and restaurant catalog: lists, ride detail, lands, coordinates, minimum and maximum height, single rider, virtual queue, indoor, wet.",
  "rides.profiles":
    "Ride profiles: manufacturer, model, elements, inversions, height, length, speed, duration, and the glossary routes.",
  "stats.live":
    "park.fan's live analysis: crowd level, occupancy, trend, the comparison with a typical day.",
  "stats.history":
    "Measured history: park statistics, hourly profiles, a ride's day curve and history, typical waits, rope drop, downtime. Includes stats.typical-waits.",
  "stats.typical-waits":
    "Each ride's typical waits and which rides are a park's headliners, in the park and ride detail (`typicalWaits`, `isHeadliner`), without the rest of the history.",
  "stats.platform":
    "Platform-wide figures: realtime totals, the live ticker, live map, best times overall.",
  "stats.accuracy":
    "How accurate the forecasts were: ML accuracy, a trip region's accuracy, a ride's prediction accuracy.",
  "forecasts.crowd":
    "Crowd forecasts: calendar, best days, yearly predictions, a trip region's calendar, best days and best dates.",
  "forecasts.waits":
    "Predicted wait times with `?forecast=true`: best visit times, hourly forecast, the forecast in a day curve.",
  "planner.day":
    "The day planner: a day's hourly plan per ride and the optimiser.",
  "planner.trip": "The trip planner: a region's itinerary and its ideas.",
  "travel.regions":
    "Trip regions with their parks, groups, hotel areas and travel times.",
  "travel.tickets": "A trip region's ticket catalog.",
  "travel.school-breaks": "School breaks of the visitors' home regions.",
};

/** What a caller without a key may read. */
export const KEYLESS_PERMISSIONS: readonly ApiPermission[] = ["live.waits"];

/** `KEYLESS_PERMISSIONS` as the set a caller without a key holds, built once. */
export const KEYLESS_ACCESS: ReadonlySet<string> = new Set(KEYLESS_PERMISSIONS);

/**
 * What a key issued before permissions existed reads: everything that was
 * open then, plus forecasts and the planner, which such a key opened. The
 * admin shows these keys as legacy so they get an explicit list.
 */
export const LEGACY_KEY_PERMISSIONS: readonly ApiPermission[] = API_PERMISSIONS;

/**
 * Permissions a wider one brings along: whoever may read the whole measured
 * history may read the typical waits taken from it. A key stores only what
 * the admin granted; `effectivePermissions` adds these when it is read.
 */
export const IMPLIED_PERMISSIONS: Partial<
  Record<ApiPermission, readonly ApiPermission[]>
> = {
  "stats.history": ["stats.typical-waits"],
};

/** The value is a grantable permission. */
export function isApiPermission(value: unknown): value is ApiPermission {
  return (
    typeof value === "string" &&
    (API_PERMISSIONS as readonly string[]).includes(value)
  );
}

/** `permissions` in catalog order, unknown values and repeats dropped. */
export function sortPermissions(
  permissions: readonly string[],
): ApiPermission[] {
  return API_PERMISSIONS.filter((permission) =>
    permissions.includes(permission),
  );
}

/**
 * The fields of the two mixed answers that each permission covers, in
 * `omitPaths` syntax. A caller who lacks a permission gets the answer without
 * its fields. `bestVisitTimes`, `hourlyForecast` and the forecast fields of a
 * day curve stay with `ForecastFields`, whose `?forecast=true` also asks for
 * `forecasts.waits`.
 */
export const PERMISSION_FIELDS: Record<
  "park" | "ride",
  Partial<Record<ApiPermission, readonly string[]>>
> = {
  park: {
    "live.schedule": [
      "schedule",
      "nextSchedule",
      "hasOperatingSchedule",
      "scheduleCoverage",
      "attractions[].performanceSchedules",
      "shows[].showtimes",
      "restaurants[].operatingHours",
    ],
    "live.weather": ["weather"],
    "parks.seasons": [
      "attractions[].isSeasonal",
      "attractions[].seasonMonths",
      "attractions[].isCurrentlyInSeason",
      "attractions[].boundSeason",
      "attractions[].pausedBySeason",
      "attractions[].worksPeriod",
      "attractions[].worksPeriods",
      "shows[].isSeasonal",
      "shows[].seasonMonths",
    ],
    "rides.details": [
      "attractions[].land",
      "attractions[].url",
      "attractions[].latitude",
      "attractions[].longitude",
      "attractions[].minimumHeight",
      "attractions[].minimumHeightUnit",
      "attractions[].maximumHeight",
      "attractions[].mayGetWet",
      "attractions[].hasSingleRider",
      "attractions[].attractionKind",
      "attractions[].indoorOutdoor",
      "attractions[].hasVirtualLine",
      "attractions[].fastPass",
      "attractions[].rcdbId",
      "restaurants[].cuisineType",
      "restaurants[].requiresReservation",
      "closedAttractions",
    ],
    "rides.profiles": ["attractions[].rideProfile"],
    "stats.live": [
      "analytics",
      "attractions[].crowdLevel",
      "attractions[].trend",
      "attractions[].baseline",
      "attractions[].comparison",
    ],
    "stats.history": [
      "ropeDropHeadliners",
      "attractions[].statistics",
      "attractions[].runsOnWeekdays",
      "attractions[].ropeDrop",
    ],
    "stats.typical-waits": [
      "attractions[].typicalWaits",
      "attractions[].isHeadliner",
    ],
  },
  ride: {
    "live.schedule": ["schedule", "performanceSchedules"],
    "parks.seasons": [
      "isSeasonal",
      "seasonMonths",
      "isCurrentlyInSeason",
      "boundSeason",
      "pausedBySeason",
      "worksPeriod",
      "worksPeriods",
    ],
    "rides.profiles": ["rideProfile"],
    "stats.live": ["crowdLevel", "trend", "baseline", "comparison"],
    "stats.history": [
      "statistics",
      "history",
      "downtime",
      "ropeDrop",
      "hasDayCurve",
      "runsOnWeekdays",
    ],
    "stats.typical-waits": ["typicalWaits", "isHeadliner"],
    "stats.accuracy": ["predictionAccuracy"],
  },
};

/** The paths to drop from a mixed answer for a caller holding `held`. */
export function withheldPaths(
  kind: keyof typeof PERMISSION_FIELDS,
  held: ReadonlySet<string>,
): string[] {
  return Object.entries(PERMISSION_FIELDS[kind]).flatMap(
    ([permission, paths]) => (held.has(permission) ? [] : [...(paths ?? [])]),
  );
}

/**
 * The permissions to store for a key: null when none were named (a legacy
 * key, kept as it was), else the list in catalog order. An empty list or one
 * with an unknown value is a 400, so a key never opens nothing by accident
 * and a typo never drops a permission silently.
 */
export function grantedPermissions(
  permissions: readonly string[] | null | undefined,
): ApiPermission[] | null {
  if (permissions === undefined || permissions === null) return null;
  const unknown = permissions.filter((value) => !isApiPermission(value));
  if (unknown.length > 0) {
    throw new BadRequestException(`Unknown permissions: ${unknown.join(", ")}`);
  }
  const sorted = sortPermissions(permissions);
  if (sorted.length === 0) {
    throw new BadRequestException("A key needs at least one permission");
  }
  return sorted;
}

/**
 * What a key may read: everything plus the internal routes for park.fan's own
 * frontend, the legacy set for a key from before permissions, else its list
 * with what each of them implies (`IMPLIED_PERMISSIONS`).
 */
export function effectivePermissions(key: {
  usage: string;
  permissions: readonly string[] | null;
}): ReadonlySet<string> {
  if (key.usage === "internal") return ALL_PERMISSIONS_AND_INTERNAL;
  const granted = key.permissions ?? LEGACY_KEY_PERMISSIONS;
  return new Set<string>(
    granted.flatMap((permission) => [
      permission,
      ...(isApiPermission(permission)
        ? (IMPLIED_PERMISSIONS[permission] ?? [])
        : []),
    ]),
  );
}

/** Every permission and the internal one: park.fan's own frontend. */
export const ALL_PERMISSIONS_AND_INTERNAL: ReadonlySet<string> =
  new Set<string>([...API_PERMISSIONS, INTERNAL_PERMISSION]);

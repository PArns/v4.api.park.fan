import { DocumentBuilder, OpenAPIObject } from "@nestjs/swagger";
import * as packageJson from "../../../package.json";

/**
 * The OpenAPI config and the route prefix, shared by `src/main.ts` (runtime
 * fallback) and `scripts/generate-swagger-spec.ts` (build time).
 *
 * The build-time spec in `dist/swagger-spec.json` is what production serves
 * whenever it exists, so both paths must build the same document. They used to
 * carry a copy each and had drifted apart: different tags, contact and license,
 * and the script did not exclude `/robots.txt` from the prefix (PAR-389).
 */
export const GLOBAL_PREFIX = "v1";

// Root controller — docs page and robots.txt must stay at the host root to be found.
export const GLOBAL_PREFIX_OPTIONS = { exclude: ["/", "/robots.txt"] };

export function buildSwaggerConfig(): Omit<OpenAPIObject, "paths"> {
  return (
    new DocumentBuilder()
      .setTitle("park.fan API v4")
      .setDescription(
        "Real-time theme park intelligence powered by machine learning. " +
          "Aggregating wait times, weather forecasts, park schedules, and ML predictions " +
          "for optimal theme park experiences worldwide.",
      )
      .setVersion(packageJson.version)
      .setExternalDoc("Frontend Application", "https://park.fan")
      // Core API tags
      .addTag(
        "health",
        "System health checks, database connectivity, and application monitoring",
      )
      .addTag(
        "parks",
        "Park metadata, operating hours, weather, and geographic details",
      )
      // Data & Analytics tags
      .addTag(
        "stats",
        "Park-wide analytics, crowd levels, and historical performance",
      )
      // Utility tags
      .addTag("root", "API root and documentation")
      .addTag(
        "favorites",
        "User favorites management for parks, attractions, and more",
      )
      .addTag("search", "Intelligent search across parks and attractions")
      .addTag(
        "discovery",
        "Geographic hierarchy for route generation (continents → countries → cities)",
      )
      // ML Service tags
      .addTag("ML", "Machine learning predictions and model information")
      .addTag(
        "ML Monitoring",
        "ML monitoring, drift detection, alerts, and anomaly detection",
      )
      .addTag(
        "ML Dashboard",
        "ML service health, metrics, and model diagnostics",
      )
      // Admin tag with security notice
      .addTag(
        "admin",
        "🔒 Administrative endpoints — require a signed-in administrator",
      )
      // Admin session token. Obtain one from POST /v1/admin/auth/login and
      // send it as `Authorization: Bearer <token>`. The old `?pass=` query key
      // still authenticates while ADMIN_LEGACY_PASS is configured, but it is
      // deprecated and is deliberately no longer what the docs teach.
      .addBearerAuth(
        {
          type: "http",
          scheme: "bearer",
          bearerFormat: "opaque",
          description:
            "Admin session token from POST /v1/admin/auth/login. Opaque and " +
            "revocable — it is looked up in Redis on every request, not decoded.",
        },
        "admin-auth",
      )
      .build()
  );
}

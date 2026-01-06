/**
 * Generate Swagger/OpenAPI spec at build time
 * This script bootstraps the app, generates the spec, and saves it to a JSON file
 * Run this during the build process to pre-compile the Swagger documentation
 */

import { NestFactory } from "@nestjs/core";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";
import * as fs from "fs";
import * as path from "path";
import * as packageJson from "../package.json";

// Import from compiled dist to avoid TypeScript compilation issues
// eslint-disable-next-line @typescript-eslint/no-require-imports
const { AppModule } = require("../dist/src/app.module");

async function generateSwaggerSpec(): Promise<void> {
  console.log("🚀 Generating Swagger/OpenAPI spec...");

  // Set environment to use minimal connection timeout during build
  // This allows the app to bootstrap even if database is unavailable
  const originalDbHost = process.env.DB_HOST;
  const originalDbPort = process.env.DB_PORT;
  
  // Use a non-existent host to prevent actual connection attempts
  // TypeORM will fail to connect but we can catch it
  process.env.DB_HOST = process.env.DB_HOST || "localhost";
  process.env.DB_PORT = process.env.DB_PORT || "5432";
  
  // Set very short timeout to fail fast if connection is attempted
  process.env.DB_CONNECTION_TIMEOUT = "1000";

  let app;
  try {
    // Create a minimal app instance for spec generation
    // Note: TypeORM may attempt to connect, but Swagger generation only needs metadata
    app = await NestFactory.create(AppModule, {
      logger: false, // Suppress logs during build
    });
  } catch (error) {
    // If app creation fails due to database, we can't generate the spec
    // This is acceptable - the app will generate it at runtime
    console.warn("⚠️  Failed to bootstrap app for spec generation:", error instanceof Error ? error.message : String(error));
    console.warn("⚠️  Swagger spec will be generated at runtime instead");
    // Restore original environment before exiting
    if (originalDbHost) process.env.DB_HOST = originalDbHost;
    if (originalDbPort) process.env.DB_PORT = originalDbPort;
    process.exit(0); // Exit successfully - runtime generation will handle it
  } finally {
    // Restore original environment
    if (originalDbHost) process.env.DB_HOST = originalDbHost;
    if (originalDbPort) process.env.DB_PORT = originalDbPort;
  }

  // Apply global prefix to match production setup
  app.setGlobalPrefix("v1", {
    exclude: ["/"],
  });

  // Build Swagger configuration (same as in main.ts)
  const config = new DocumentBuilder()
    .setTitle("park.fan API v4")
    .setDescription(
      "Real-time theme park intelligence powered by machine learning. " +
        "Aggregating wait times, weather forecasts, park schedules, and ML predictions " +
        "for optimal theme park experiences worldwide.",
    )
    .setVersion(packageJson.version)
    .setContact("Patrick Arns", "https://arns.dev", "contact@arns.dev")
    .setLicense("UNLICENSED", "")
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
    .addTag(
      "attractions",
      "Attraction info, live status, wait times, and queue data",
    )
    .addTag("shows", "Live entertainment schedules and showtimes")
    .addTag("restaurants", "Dining options, menus, and operating hours")
    // Data & Analytics tags
    .addTag(
      "queue-data",
      "Historical wait times, queue performance, and ride availability",
    )
    .addTag(
      "stats",
      "Park-wide analytics, crowd levels, and historical performance",
    )
    .addTag(
      "predictions",
      "ML-powered crowd predictions and wait time forecasts",
    )
    // Utility tags
    .addTag(
      "search",
      "Intelligent search across parks, attractions, shows, and restaurants",
    )
    .addTag(
      "discovery",
      "Geographic hierarchy for route generation (continents → countries → cities)",
    )
    .addTag("destinations", "Resort-level aggregation grouping multiple parks")
    .addTag(
      "holidays",
      "Public holiday data affecting crowds and operating hours",
    )
    // ML Service tags
    .addTag("ML", "Machine learning predictions and model information")
    .addTag("ML Dashboard", "ML service health, metrics, and model diagnostics")
    // Admin tag with security notice
    .addTag(
      "admin",
      "⚠️ Administrative endpoints - PROTECTED IN PRODUCTION via Cloudflare",
    )
    // Security schemes (Cloudflare API Key via query parameter)
    .addApiKey(
      {
        type: "apiKey",
        name: "pass",
        in: "query",
        description: "Admin API key (Cloudflare protected - production only)",
      },
      "admin-auth",
    )
    .build();

  // Generate the OpenAPI document
  const document = SwaggerModule.createDocument(app, config);

  // Ensure dist directory exists
  const distPath = path.join(__dirname, "..", "dist");
  if (!fs.existsSync(distPath)) {
    fs.mkdirSync(distPath, { recursive: true });
  }

  // Save the spec to dist/swagger-spec.json
  const specPath = path.join(distPath, "swagger-spec.json");
  fs.writeFileSync(specPath, JSON.stringify(document, null, 2));

  console.log(`✅ Swagger spec generated: ${specPath}`);
  console.log(`   Spec size: ${(fs.statSync(specPath).size / 1024).toFixed(2)} KB`);

  // Close the app
  await app.close();
}

generateSwaggerSpec()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    // Don't fail the build if spec generation fails
    // The app will generate it at runtime as a fallback
    console.warn("⚠️  Failed to generate Swagger spec at build time:", error instanceof Error ? error.message : String(error));
    console.warn("⚠️  Swagger spec will be generated at runtime instead");
    process.exit(0); // Exit successfully - don't break the build
  });

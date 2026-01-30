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

  // Set environment to skip database connection during build
  // Swagger generation only needs metadata, not actual DB connection
  const originalNodeEnv = process.env.NODE_ENV;
  process.env.NODE_ENV = "build"; // Special mode for build-time spec generation

  // Set very short connection timeout to fail fast if connection is attempted
  // Use invalid host to prevent actual connection
  process.env.DB_HOST = "127.0.0.1";
  process.env.DB_PORT = "1"; // Invalid port
  process.env.DB_CONNECTION_TIMEOUT = "100";
  process.env.SKIP_REDIS = "true"; // Skip Redis connection

  let app;
  try {
    // Create a minimal app instance for spec generation
    // TypeORM will try to connect but will fail quickly with invalid host/port
    // We catch the error and continue - Swagger doesn't need DB connection
    app = await NestFactory.create(AppModule, {
      logger: false, // Suppress logs during build
    });
  } catch (error) {
    // TypeORM connection failure is expected during build
    // Swagger generation only needs decorator metadata, not DB connection
    const errorMessage = error instanceof Error ? error.message : String(error);

    // If it's a connection error, we can still try to generate the spec
    // by catching the error at the module level
    if (errorMessage.includes("connect") || errorMessage.includes("ECONNREFUSED") || errorMessage.includes("timeout")) {
      console.warn("⚠️  Database connection failed during build (expected)");
      console.warn("⚠️  Attempting to generate Swagger spec without DB connection...");

      // Try to create app with abortOnError: false to skip connection errors
      try {
        app = await NestFactory.create(AppModule, {
          logger: false,
          abortOnError: false, // Don't abort on module initialization errors
        });
      } catch (retryError) {
        console.warn("⚠️  Could not bootstrap app for spec generation");
        console.warn("⚠️  Swagger spec will be generated at runtime instead");
        if (originalNodeEnv) process.env.NODE_ENV = originalNodeEnv;
        process.exit(0);
      }
    } else {
      // Other errors - can't proceed
      console.warn("⚠️  Failed to bootstrap app for spec generation:", errorMessage);
      console.warn("⚠️  Swagger spec will be generated at runtime instead");
      if (originalNodeEnv) process.env.NODE_ENV = originalNodeEnv;
      process.exit(0);
    }
  } finally {
    // Restore original environment
    if (originalNodeEnv) process.env.NODE_ENV = originalNodeEnv;
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

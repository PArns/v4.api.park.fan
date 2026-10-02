/**
 * Generate the Swagger/OpenAPI spec at build time (`postbuild`).
 *
 * Writes dist/swagger-spec.json, which src/main.ts serves in preference to
 * generating the spec at startup. If this step fails, the API still generates
 * the spec at runtime, so a failure warns and exits 0 rather than breaking
 * the build.
 *
 * The app is created in preview mode: Nest builds the module graph and reads
 * the controller and DTO metadata, but calls no provider factory, so nothing
 * connects to Postgres or Redis. A full bootstrap needs a database. Without
 * one, Nest's default `abortOnError: true` ends the process with exit 1 before
 * any catch here runs, and `logger: false` hid even that error (PAR-389).
 */

import { NestFactory } from "@nestjs/core";
import { SwaggerModule } from "@nestjs/swagger";
import * as fs from "fs";
import * as path from "path";

// Import from compiled dist to avoid TypeScript compilation issues
/* eslint-disable @typescript-eslint/no-require-imports */
const { AppModule } = require("../dist/src/app.module");
const {
  GLOBAL_PREFIX,
  GLOBAL_PREFIX_OPTIONS,
  buildSwaggerConfig,
} = require("../dist/src/common/swagger/swagger-config");
/* eslint-enable @typescript-eslint/no-require-imports */

async function generateSwaggerSpec(): Promise<void> {
  console.log("🚀 Generating Swagger/OpenAPI spec...");

  const app = await NestFactory.create(AppModule, {
    preview: true, // no provider instances, so no DB or Redis connection
    abortOnError: false, // throw into the catch below instead of process.exit(1)
    logger: ["error", "warn"],
  });

  app.setGlobalPrefix(GLOBAL_PREFIX, GLOBAL_PREFIX_OPTIONS);

  const document = SwaggerModule.createDocument(app, buildSwaggerConfig());

  const distPath = path.join(__dirname, "..", "dist");
  fs.mkdirSync(distPath, { recursive: true });
  const specPath = path.join(distPath, "swagger-spec.json");
  fs.writeFileSync(specPath, JSON.stringify(document, null, 2));

  console.log(
    `✅ Swagger spec generated: ${specPath} ` +
      `(${Object.keys(document.paths).length} paths, ` +
      `${(fs.statSync(specPath).size / 1024).toFixed(2)} KB)`,
  );

  await app.close();
}

generateSwaggerSpec()
  .then(() => {
    process.exit(0);
  })
  .catch((error) => {
    // Don't fail the build: the app generates the spec at runtime as a fallback.
    console.warn(
      "⚠️  Failed to generate Swagger spec at build time:",
      error instanceof Error ? (error.stack ?? error.message) : String(error),
    );
    console.warn("⚠️  Swagger spec will be generated at runtime instead");
    process.exit(0);
  });

import { ModulesContainer, NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { Logger, ValidationPipe, LogLevel } from "@nestjs/common";
import { warnWhenCfOriginSecretUnset } from "./common/throttler/throttler.config";
import { Request, Response, NextFunction } from "express";
import compression from "compression";
import helmet from "helmet";
import { SwaggerModule } from "@nestjs/swagger";
import { AppModule } from "./app.module";
import { enableGracefulShutdown } from "./common/utils/graceful-shutdown";
import { watchShutdownWork } from "./common/utils/shutdown-work";
import { installCrashHandlers } from "./common/utils/crash-handlers";
import { HttpExceptionFilter } from "./common/filters/http-exception.filter";
import { LoggingInterceptor } from "./common/interceptors/logging.interceptor";
import { ExcludeNullInterceptor } from "./common/interceptors/exclude-null.interceptor";
import { CacheControlInterceptor } from "./common/interceptors/cache-control.interceptor";
import { buildCorsDelegate } from "./common/cors/cors.config";
import {
  GLOBAL_PREFIX,
  GLOBAL_PREFIX_OPTIONS,
  buildSwaggerConfig,
} from "./common/swagger/swagger-config";
import * as fs from "fs";
import * as path from "path";

// Suppress EPIPE on stdout/stderr — happens when Docker's log pipe closes while
// the process is still writing (e.g. under heavy error-log volume). Not a crash.
process.stdout.on("error", (err: NodeJS.ErrnoException) => {
  if (err.code !== "EPIPE") throw err;
});
process.stderr.on("error", (err: NodeJS.ErrnoException) => {
  if (err.code !== "EPIPE") throw err;
});

// Record an uncaught exception or an unhandled rejection and end the process —
// unless the shutdown hooks are running, where a rejection out of a closing
// connection must not abort them (PAR-903).
installCrashHandlers();

async function bootstrap(): Promise<void> {
  const logLevel = process.env.LOG_LEVEL ?? "log";
  const logLevelMap: Record<string, LogLevel[]> = {
    verbose: ["verbose", "debug", "log", "warn", "error"],
    debug: ["debug", "log", "warn", "error"],
    log: ["log", "warn", "error"],
    warn: ["warn", "error"],
    error: ["error"],
  };
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: logLevelMap[logLevel] ?? logLevelMap["log"],
  });

  // Make SIGTERM run the lifecycle hooks — a Coolify redeploy stops the old
  // container with it, and without this nothing closed the Bull queues, the
  // TypeORM pool or the abortable boot chains (PAR-903). Enabled before the
  // middleware wiring so a signal during the boot is covered too.
  enableGracefulShutdown(app, {
    work: watchShutdownWork(app.get(ModulesContainer)),
  });

  // Trust the proxy in front of us (Cloudflare → reverse proxy) so
  // `req.ip` and protocol reflect the real client, not the edge. Rate
  // limiting additionally prefers the CF-Connecting-IP header.
  app.set("trust proxy", true);

  // Gzip/deflate responses at the origin. Cloudflare compresses at the
  // edge for browsers, but compressing here also shrinks the origin↔edge
  // hop (egress + latency) and covers any non-CDN/direct API consumer.
  // Threshold keeps tiny payloads uncompressed (compression overhead > win).
  app.use(compression({ threshold: 1024 }));

  // Security response headers (HSTS, X-Content-Type-Options: nosniff, frameguard,
  // Referrer-Policy, …). CSP and COEP are disabled: this is a JSON API and the
  // only HTML surface is the Swagger UI at /api, which helmet's default CSP/COEP
  // would break. Defense-in-depth in addition to whatever Cloudflare adds.
  app.use(
    helmet({
      contentSecurityPolicy: false,
      crossOriginEmbedderPolicy: false,
    }),
  );

  // Custom X-Powered-By header
  app.disable("x-powered-by");
  app.use((req: Request, res: Response, next: NextFunction) => {
    res.setHeader("X-Powered-By", "api.park.fan");
    next();
  });

  // Global validation pipe for DTOs
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true, // Strip unknown properties
      forbidNonWhitelisted: true, // Throw error on unknown properties
      transform: true, // Auto-transform payloads to DTO instances
      transformOptions: {
        enableImplicitConversion: true,
      },
    }),
  );

  // Global exception filter
  app.useGlobalFilters(new HttpExceptionFilter());

  // Global interceptors
  app.useGlobalInterceptors(
    new CacheControlInterceptor(), // Cloudflare caching
    new LoggingInterceptor(),
    new ExcludeNullInterceptor(), // Remove null values (disable with ?debug=true)
  );

  // Cloudflare adds no CORS headers; the allowed origins live here. See
  // docs/deployment/cors.md.
  app.enableCors(buildCorsDelegate(process.env.NODE_ENV === "production"));

  // API versioning prefix (exclude root controller — docs page and robots.txt,
  // both of which must stay at the host root to be found)
  app.setGlobalPrefix(GLOBAL_PREFIX, GLOBAL_PREFIX_OPTIONS);

  // Swagger/OpenAPI Documentation
  // Try to load pre-generated spec from build time, otherwise generate at runtime
  const specPath = path.join(__dirname, "..", "swagger-spec.json");
  let document: any;

  if (fs.existsSync(specPath)) {
    // Use pre-generated spec (faster startup)
    console.log("📚 Loading pre-generated Swagger spec...");
    const specContent = fs.readFileSync(specPath, "utf-8");
    document = JSON.parse(specContent);
  } else {
    // Fallback: generate spec at runtime (slower, but works in dev)
    console.log("📚 Generating Swagger spec at runtime...");
    const config = buildSwaggerConfig();

    document = SwaggerModule.createDocument(app, config);
  }

  // Swagger UI options (cache headers handled by CacheControlInterceptor)
  SwaggerModule.setup("api", app, document, {
    customSiteTitle: "park.fan API Documentation",
    swaggerOptions: {
      persistAuthorization: true,
    },
  });

  const port = process.env.PORT || 3000;
  await app.listen(port);

  const logger = new Logger("Bootstrap");
  warnWhenCfOriginSecretUnset(logger);
  logger.log(`🚀 park.fan API v4 running on: http://localhost:${port}/v1`);
  logger.log(`📚 API Documentation: http://localhost:${port}/api`);
  logger.log(`📊 Bull Board Dashboard: http://localhost:3001`);
  logger.log(`🗄️  Database: PostgreSQL + TimescaleDB on port 5432`);
  logger.log(`⚡ Redis: localhost:6379`);
}

bootstrap();

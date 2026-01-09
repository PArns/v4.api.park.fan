import { NestFactory } from "@nestjs/core";
import { NestExpressApplication } from "@nestjs/platform-express";
import { ValidationPipe } from "@nestjs/common";
import { Request, Response, NextFunction } from "express";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";
import { AppModule } from "./app.module";
import { HttpExceptionFilter } from "./common/filters/http-exception.filter";
import { LoggingInterceptor } from "./common/interceptors/logging.interceptor";
import { ExcludeNullInterceptor } from "./common/interceptors/exclude-null.interceptor";
import { CacheControlInterceptor } from "./common/interceptors/cache-control.interceptor";
import * as packageJson from "../package.json";
import * as fs from "fs";
import * as path from "path";

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create<NestExpressApplication>(AppModule, {
    logger: ["log", "error", "warn"],
  });

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

  // CORS Configuration
  // SECURITY: In production, CORS should be disabled or use a whitelist
  // Cloudflare handles CORS in production, so we disable it at application level
  const isProduction = process.env.NODE_ENV === "production";
  const allowedOrigins = process.env.CORS_ORIGINS
    ? process.env.CORS_ORIGINS.split(",").map((origin) => origin.trim())
    : [];

  if (isProduction) {
    // Production: Use whitelist if provided, otherwise disable CORS (Cloudflare handles it)
    if (allowedOrigins.length > 0) {
      app.enableCors({
        origin: (origin, callback) => {
          if (origin && allowedOrigins.includes(origin)) {
            callback(null, true);
          } else {
            callback(new Error("Not allowed by CORS"));
          }
        },
        credentials: true,
        methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        allowedHeaders: ["Content-Type", "Authorization", "X-Admin-API-Key"],
      });
    } else {
      // No CORS origins configured - disable CORS (Cloudflare will handle it)
      // This is safer than allowing all origins
      app.enableCors({
        origin: false,
        credentials: false,
      });
    }
  } else {
    // Development: Allow all origins for local testing
    // SECURITY WARNING: Never deploy with this configuration to production
    app.enableCors({
      origin: "*",
      credentials: false, // SECURITY: Don't allow credentials with wildcard origin
      methods: ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
      allowedHeaders: ["Content-Type", "Authorization", "X-Admin-API-Key"],
    });
  }

  // API versioning prefix (exclude root controller)
  app.setGlobalPrefix("v1", {
    exclude: ["/"],
  });

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
    const config = new DocumentBuilder()
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

  console.log(`🚀 park.fan API v4 running on: http://localhost:${port}/v1`);
  console.log(`📚 API Documentation: http://localhost:${port}/api`);
  console.log(`📊 Bull Board Dashboard: http://localhost:3001`);
  console.log(`🗄️  Database: PostgreSQL + TimescaleDB on port 5432`);
  console.log(`⚡ Redis: localhost:6379`);
}

bootstrap();

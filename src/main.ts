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

  // Enable CORS for development
  app.enableCors({
    origin: process.env.NODE_ENV === "production" ? false : "*",
    credentials: true,
  });

  // API versioning prefix (exclude root controller)
  app.setGlobalPrefix("v1", {
    exclude: ["/"],
  });

  // Swagger/OpenAPI Documentation
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

  const document = SwaggerModule.createDocument(app, config);

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

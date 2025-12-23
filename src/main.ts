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
    logger: ["log", "error", "warn", "debug", "verbose"],
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
      "Theme park wait times, predictions, and statistics API powered by ThemeParks.wiki and Queue-Times.com data",
    )
    .setVersion(packageJson.version)
    .addTag(
      "health",
      "System health checks, database connectivity status, and application monitoring endpoints",
    )
    .addTag(
      "parks",
      "Core park data, including operating hours, metadata, and geographic details",
    )
    .addTag(
      "attractions",
      "Detailed attraction information, live status, and wait time data",
    )
    .addTag(
      "queue-data",
      "Historical wait time data, queue performance metrics, and ride availability history",
    )
    .addTag(
      "predictions",
      "Machine learning-powered crowd predictions and wait time forecasts",
    )
    .addTag(
      "stats",
      "Park-wide analytics, crowd level statistics, and historical performance metrics",
    )
    .addTag(
      "search",
      "Global search capabilities for finding parks, attractions, and resorts",
    )
    .addTag(
      "destinations",
      "Resort-level destination data, grouping multiple parks and amenities",
    )
    .addTag("restaurants", "Dining options, menus, and operating hours")
    .addTag(
      "shows",
      "Live entertainment schedules, showtimes, and performance details",
    )
    .addTag(
      "holidays",
      "Public holiday data affecting park crowds and operating hours",
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

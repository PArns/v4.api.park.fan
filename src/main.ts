import { NestFactory } from "@nestjs/core";
import { ValidationPipe } from "@nestjs/common";
import { DocumentBuilder, SwaggerModule } from "@nestjs/swagger";
import { AppModule } from "./app.module";
import { HttpExceptionFilter } from "./common/filters/http-exception.filter";
import { LoggingInterceptor } from "./common/interceptors/logging.interceptor";
import { ExcludeNullInterceptor } from "./common/interceptors/exclude-null.interceptor";
import { CacheControlInterceptor } from "./common/interceptors/cache-control.interceptor";

async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule, {
    logger: ["log", "error", "warn", "debug", "verbose"],
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

  // API versioning prefix
  app.setGlobalPrefix("v1");

  // Swagger/OpenAPI Documentation
  const config = new DocumentBuilder()
    .setTitle("park.fan API v4")
    .setDescription(
      "Theme park wait times, predictions, and statistics API powered by ThemeParks.wiki and Queue-Times.com data",
    )
    .setVersion("4.0.0")
    .addTag("health", "Health and monitoring endpoints")
    .addTag("parks", "Park information and management")
    .addTag("attractions", "Attraction details and wait times")
    .addTag("queue-data", "Historical wait time data")
    .addTag("predictions", "ML-powered wait time predictions")
    .addTag("stats", "Global and park statistics")
    .addTag("admin", "Administrative operations")
    .build();

  const document = SwaggerModule.createDocument(app, config);
  SwaggerModule.setup("api", app, document);

  const port = process.env.PORT || 3000;
  await app.listen(port);

  console.log(`🚀 park.fan API v4 running on: http://localhost:${port}/v1`);
  console.log(`📚 API Documentation: http://localhost:${port}/api`);
  console.log(`📊 Bull Board Dashboard: http://localhost:3001`);
  console.log(`🗄️  Database: PostgreSQL + TimescaleDB on port 5432`);
  console.log(`⚡ Redis: localhost:6379`);
}

bootstrap();

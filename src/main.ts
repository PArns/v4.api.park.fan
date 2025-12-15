import { NestFactory } from "@nestjs/core";
import { ValidationPipe } from "@nestjs/common";
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

  const port = process.env.PORT || 3000;
  await app.listen(port);

  console.log(`🚀 park.fan API v4 running on: http://localhost:${port}/v1`);
  console.log(`📊 Bull Board Dashboard: http://localhost:3001`);
  console.log(`🗄️  Database: PostgreSQL + TimescaleDB on port 5432`);
  console.log(`⚡ Redis: localhost:6379`);
}

bootstrap();

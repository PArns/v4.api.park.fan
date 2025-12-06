import { Test, TestingModule } from "@nestjs/testing";
import { INestApplication, ValidationPipe } from "@nestjs/common";
import { TypeOrmModule } from "@nestjs/typeorm";
import { getDatabaseConfig } from "../../src/config/database.config";

/**
 * Creates a full NestJS test application with database connection
 * Use this for E2E tests that need the full app context
 *
 * @param moduleImports - Array of NestJS modules to import
 * @returns Initialized NestJS application
 */
export async function createTestApp(
  moduleImports: any[],
): Promise<INestApplication> {
  const dbConfig = getDatabaseConfig();

  const moduleFixture: TestingModule = await Test.createTestingModule({
    imports: [
      TypeOrmModule.forRoot({
        type: "postgres",
        host: dbConfig.host,
        port: dbConfig.port,
        username: dbConfig.username,
        password: dbConfig.password,
        database: dbConfig.database,
        entities: [__dirname + "/../../src/**/*.entity{.ts,.js}"],
        synchronize: true, // Auto-create schema for tests
        logging: false, // Disable SQL logging in tests
      }),
      ...moduleImports,
    ],
  }).compile();

  const app = moduleFixture.createNestApplication();

  // Apply global pipes (same as production)
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      transform: true,
    }),
  );

  await app.init();

  return app;
}

/**
 * Cleanup helper to close app and database connections
 */
export async function closeTestApp(app: INestApplication): Promise<void> {
  if (app) {
    await app.close();
  }
}

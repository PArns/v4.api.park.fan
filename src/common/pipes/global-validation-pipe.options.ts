import type { ValidationPipeOptions } from "@nestjs/common";

/**
 * The options of the ValidationPipe `main.ts` mounts for every request body:
 * keep only decorated properties, refuse the rest, and turn the body into the
 * DTO. An admin MCP tool that runs a route's handler checks its body with the
 * same options (`src/mcp/admin-route.ts`), so a tool cannot accept what the
 * route refuses.
 */
export const GLOBAL_VALIDATION_PIPE_OPTIONS: ValidationPipeOptions = {
  whitelist: true,
  forbidNonWhitelisted: true,
  transform: true,
  transformOptions: {
    enableImplicitConversion: true,
  },
};

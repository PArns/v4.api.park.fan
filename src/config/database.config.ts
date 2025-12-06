export interface DatabaseConfig {
  host: string;
  port: number;
  username: string;
  password: string;
  database: string;
  synchronize: boolean;
  logging: boolean;
}

export const getDatabaseConfig = (): DatabaseConfig => {
  const nodeEnv = process.env.NODE_ENV;
  const dbName = process.env.DB_DATABASE || "parkfan";

  // ==================== SAFETY VALIDATION ====================
  // CRITICAL: Prevent tests from connecting to dev/prod databases
  if (nodeEnv === "test") {
    if (!dbName.includes("test")) {
      throw new Error(
        `\n` +
          `╔════════════════════════════════════════════════════════════╗\n` +
          `║  ❌ DATABASE SAFETY VIOLATION ❌                           ║\n` +
          `╠════════════════════════════════════════════════════════════╣\n` +
          `║  NODE_ENV=test but DB_DATABASE="${dbName}"                 ║\n` +
          `║  does not contain "test".                                 ║\n` +
          `║                                                            ║\n` +
          `║  This safety check prevents accidental pollution of       ║\n` +
          `║  development or production databases during testing.      ║\n` +
          `║                                                            ║\n` +
          `║  Fix: Set DB_DATABASE=parkfan_test in .env.test          ║\n` +
          `╚════════════════════════════════════════════════════════════╝\n`,
      );
    }

    // Warning banner
    console.log(
      `\n` +
        `┌────────────────────────────────────────────────────┐\n` +
        `│  ⚠️  TEST MODE ACTIVE                              │\n` +
        `│  Database: ${dbName.padEnd(38)} │\n` +
        `│  Host: ${(process.env.DB_HOST || "localhost").padEnd(42)} │\n` +
        `└────────────────────────────────────────────────────┘\n`,
    );
  }

  return {
    host: process.env.DB_HOST || "localhost",
    port: parseInt(process.env.DB_PORT || "5432", 10),
    username: process.env.DB_USERNAME || "parkfan",
    password: process.env.DB_PASSWORD || "parkfan_dev_password",
    database: dbName,
    synchronize: process.env.DB_SYNCHRONIZE === "true",
    logging: process.env.DB_LOGGING === "true",
  };
};

import { Module } from "@nestjs/common";
import { BullModule } from "@nestjs/bull";
import { TypeOrmModule } from "@nestjs/typeorm";
import { RedisModule } from "../common/redis/redis.module";
import { SearchConsoleBatch } from "./entities/search-console-batch.entity";
import { SearchConsoleRow } from "./entities/search-console-row.entity";
import { SearchConsoleRun } from "./entities/search-console-run.entity";
import { AdminSearchConsoleController } from "./search-console.controller";
import { SearchConsoleClient } from "./search-console.client";
import { SearchConsoleImportService } from "./search-console-import.service";
import { SearchConsoleService } from "./search-console.service";

/**
 * Google Search Console: a nightly import of the queries and pages the site
 * was found with (a service account against Google's REST API), the admin's
 * reports over them, and the same reports for the MCP tools. Off without
 * `GSC_SERVICE_ACCOUNT_JSON` and `GSC_SITE_URL`. The job's processor is
 * `SearchConsoleProcessor` in the queues module. See
 * docs/frontend/search-console.md and docs/admin/search-console-setup.md.
 */
@Module({
  imports: [
    TypeOrmModule.forFeature([
      SearchConsoleRow,
      SearchConsoleBatch,
      SearchConsoleRun,
    ]),
    BullModule.registerQueue({ name: "search-console" }),
    RedisModule,
  ],
  controllers: [AdminSearchConsoleController],
  providers: [
    SearchConsoleClient,
    SearchConsoleImportService,
    SearchConsoleService,
  ],
  exports: [SearchConsoleImportService, SearchConsoleService],
})
export class SearchConsoleModule {}

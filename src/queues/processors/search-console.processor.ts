import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { Job } from "bull";
import { SearchConsoleImportService } from "../../search-console/search-console-import.service";
import { SearchConsoleReindexService } from "../../search-console/search-console-reindex.service";

/**
 * The Google Search Console jobs (docs/frontend/search-console.md):
 *
 * - `import`, nightly and on demand from the admin: the last days again and
 *   every day never fetched, a transaction per day. Throws when Google
 *   refused the account or days failed, so the failure reaches the data-quality
 *   report; the status carries the reason. Does nothing without credentials.
 * - `drop-old-chunks`, nightly: chunks past 500 days dropped whole, and the
 *   bookkeeping rows that name nothing any more.
 * - `reindex-scan`, daily after the import and on demand: the reindex queue's
 *   candidates from the sitemaps, and URL Inspection for the ones Google may
 *   not have crawled since their change.
 */
@Processor("search-console")
export class SearchConsoleProcessor {
  private readonly logger = new Logger(SearchConsoleProcessor.name);

  constructor(
    private readonly imports: SearchConsoleImportService,
    private readonly reindex: SearchConsoleReindexService,
  ) {}

  @Process("import")
  async handleImport(job: Job<{ trigger?: "cron" | "manual" }>): Promise<void> {
    const started = Date.now();
    const summary = await this.imports.run(job.data?.trigger ?? "cron");
    if (summary.skipped === "running") {
      this.logger.log("Search Console import already running, skipped");
    } else if (!summary.skipped) {
      this.logger.debug(
        `Search Console import: ${summary.daysChecked} day(s) checked, ${summary.batchesWritten} written, ${summary.unchanged} unchanged, in ${Date.now() - started} ms`,
      );
    }
  }

  @Process("drop-old-chunks")
  async handleDropOldChunks(_job: Job): Promise<void> {
    const dropped = await this.imports.dropOldChunks();
    if (dropped > 0) {
      this.logger.log(`🧹 Dropped ${dropped} Search Console chunk(s)`);
    }
  }

  @Process("reindex-scan")
  async handleReindexScan(_job: Job): Promise<void> {
    const summary = await this.reindex.scan();
    if (!summary.skipped) {
      this.logger.log(
        `Reindex scan: ${summary.candidates} changed page(s), ${summary.inspected} inspected, ${summary.errors} refused`,
      );
    }
  }
}

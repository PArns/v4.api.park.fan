import { Injectable, Logger } from "@nestjs/common";
import axios from "axios";
import {
  getRevalidateUrl,
  getRevalidateSecret,
  isRevalidationEnabled,
} from "../../config/revalidation.config";

/** Max tags per POST so a full-fleet revalidation never sends a giant body. */
const REVALIDATE_TAG_BATCH = 200;

/**
 * On-demand revalidation webhook client.
 *
 * Fires `POST {REVALIDATE_URL}` with `{ tags: [...] }` so the frontend can drop
 * the affected Next.js cache entries the moment the backend recomputes them —
 * the "read first, only write on change" path that lets the frontend keep long
 * time-based TTLs without serving stale data.
 *
 * No-op unless `REVALIDATE_SECRET` is configured, so dev / test / CI instances
 * never ping the production frontend. Best-effort: a failed webhook is logged
 * and swallowed — it must never fail the batch that triggered it.
 */
@Injectable()
export class RevalidationService {
  private readonly logger = new Logger(RevalidationService.name);

  /**
   * Revalidate the given frontend cache tags (deduped + batched).
   * Returns true if at least one request succeeded, false otherwise.
   */
  async revalidateTags(tags: string[]): Promise<boolean> {
    const unique = [...new Set(tags.filter((t) => t && t.length > 0))];
    if (unique.length === 0) return false;

    if (!isRevalidationEnabled()) {
      this.logger.debug(
        `Revalidation webhook disabled (no REVALIDATE_SECRET); ` +
          `would have revalidated ${unique.length} tag(s)`,
      );
      return false;
    }

    const url = getRevalidateUrl();
    const secret = getRevalidateSecret();
    let anySuccess = false;

    for (let i = 0; i < unique.length; i += REVALIDATE_TAG_BATCH) {
      const batch = unique.slice(i, i + REVALIDATE_TAG_BATCH);
      try {
        await axios.post(
          url,
          { tags: batch },
          {
            headers: { "x-revalidate-secret": secret },
            timeout: 10_000,
          },
        );
        anySuccess = true;
      } catch (error) {
        const msg = error instanceof Error ? error.message : String(error);
        this.logger.warn(
          `Revalidation POST failed for ${batch.length} tag(s): ${msg}`,
        );
      }
    }

    if (anySuccess) {
      this.logger.log(`Revalidated ${unique.length} frontend cache tag(s)`);
    }
    return anySuccess;
  }

  /**
   * Revalidate best-days tags for the given park slugs
   * (`best-days:<slug>`), the tag the frontend best-days clients read.
   */
  async revalidateBestDays(parkSlugs: string[]): Promise<boolean> {
    return this.revalidateTags(parkSlugs.map((slug) => `best-days:${slug}`));
  }
}

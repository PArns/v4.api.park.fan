import { Injectable, Logger } from "@nestjs/common";
import axios from "axios";
import { getGlossaryTermIdsUrl } from "../../config/glossary.config";
import { RideProfileService } from "./ride-profile.service";

/** One term id the curation stores that the glossary does not define. */
export interface BrokenTermId {
  termId: string;
  /** The ride pages that render short because of it, `park/ride`. */
  usedBy: string[];
}

export interface RideProfileTermAudit {
  checkedAt: string;
  /** Distinct term ids stored across `elements`, `types` and the manufacturer. */
  storedTermIds: number;
  /** Term ids the frontend publishes as resolvable. */
  glossaryTermIds: number;
  /** Stored ids with no glossary term — these render as nothing. */
  broken: BrokenTermId[];
  /**
   * Glossary terms no curated ride references. Informational, never a problem:
   * most of the glossary is concepts (airtime, g-force, GP) that no ride
   * layout would ever name.
   */
  unusedGlossaryTermIds: number;
}

/**
 * Checks that every glossary term id the curation stores still resolves.
 *
 * This is the safety net that went away with the ride-profile seed. The seed's
 * spec validated every id against a checked-in allowlist and failed CI on a
 * typo; the curation now lives in the database, where no build can see it. The
 * failure mode is silent by design — `GlossaryTermLink` drops an id it has no
 * term for rather than rendering a dead link — so a renamed term shortens a
 * ride's layout and nothing anywhere complains.
 *
 * Hence a check that spans both sides at runtime: the frontend publishes the
 * ids that actually resolve to a page, and this compares them with the ids the
 * curation actually stores.
 */
@Injectable()
export class RideProfileAuditService {
  private readonly logger = new Logger(RideProfileAuditService.name);

  constructor(private readonly rideProfiles: RideProfileService) {}

  async audit(): Promise<RideProfileTermAudit> {
    const url = getGlossaryTermIdsUrl();
    const { data } = await axios.get<{ count: number; ids: string[] }>(url, {
      timeout: 15_000,
    });

    if (!Array.isArray(data?.ids) || data.ids.length === 0) {
      // Treat an empty list as a broken source, never as "every id is wrong" —
      // otherwise a frontend deploy blip would report the whole curation as
      // dead and bury the real signal.
      throw new Error(
        `Glossary term ids unavailable or empty from ${url} — audit aborted`,
      );
    }

    const glossary = new Set(data.ids);
    const stored = await this.rideProfiles.findDistinctTermIds();
    const brokenIds = stored.filter((id) => !glossary.has(id));

    const usage = await this.rideProfiles.findRidesUsingTermIds(brokenIds);
    const byTerm = new Map<string, string[]>();
    for (const row of usage) {
      const rides = byTerm.get(row.termId) ?? [];
      rides.push(`${row.parkSlug}/${row.attractionSlug}`);
      byTerm.set(row.termId, rides);
    }

    const storedSet = new Set(stored);
    const result: RideProfileTermAudit = {
      checkedAt: new Date().toISOString(),
      storedTermIds: stored.length,
      glossaryTermIds: glossary.size,
      broken: brokenIds.map((termId) => ({
        termId,
        usedBy: byTerm.get(termId) ?? [],
      })),
      unusedGlossaryTermIds: [...glossary].filter((id) => !storedSet.has(id))
        .length,
    };

    if (result.broken.length > 0) {
      // Naming them is the whole point — a bare count is exactly as invisible
      // as the failure this exists to surface.
      this.logger.warn(
        `🎢 ${result.broken.length} ride-profile term id(s) no longer resolve: ` +
          result.broken
            .map((b) => `${b.termId} (${b.usedBy.length} ride(s))`)
            .join(", "),
      );
    } else {
      this.logger.log(
        `🎢 Ride-profile term audit clean: ${result.storedTermIds} stored ids all resolve`,
      );
    }

    return result;
  }
}

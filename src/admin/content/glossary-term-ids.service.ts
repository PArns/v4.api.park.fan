import { Injectable, Logger } from "@nestjs/common";
import axios from "axios";
import { getGlossaryTermIdsUrl } from "../../config/glossary.config";

/** How long a fetched glossary id list is trusted. */
const TERM_ID_TTL_MS = 10 * 60 * 1000;

/**
 * The glossary's own id list, fetched from the frontend and briefly cached.
 *
 * The ids stored here — a ride profile's track figures, a park's fast-pass term
 * — are frontend glossary ids, and nothing in this database can validate one.
 * A wrong id does not error: the component that resolves it drops what it
 * cannot find, so the ride's layout walkthrough simply comes out shorter and
 * the "QuickPass" chip links nowhere. Checking at write time turns that into a
 * message while the editor still remembers what they meant.
 *
 * It is a service rather than a private helper because two curation paths now
 * need it, and the second one would otherwise have been a copy — including a
 * copy of the judgement call below, which is the part worth keeping in one
 * place.
 */
@Injectable()
export class GlossaryTermIdsService {
  private readonly logger = new Logger(GlossaryTermIdsService.name);

  private termIds: Set<string> | null = null;
  private fetchedAt = 0;

  /**
   * Which of these ids the glossary does not define.
   *
   * Returns nothing to complain about when the frontend cannot be reached.
   * That is deliberate: an unreachable frontend is not evidence that the ids
   * are wrong, and refusing every curation because a deploy is mid-flight would
   * make this check the thing that breaks curation rather than the thing that
   * protects it. The nightly audit still catches whatever slips through.
   */
  async unknownIds(ids: string[]): Promise<string[]> {
    const wanted = [...new Set(ids.filter((id) => id.length > 0))];
    if (wanted.length === 0) return [];

    const known = await this.load();
    if (!known) return [];

    return wanted.filter((id) => !known.has(id));
  }

  private async load(): Promise<Set<string> | null> {
    if (this.termIds && Date.now() - this.fetchedAt < TERM_ID_TTL_MS) {
      return this.termIds;
    }
    try {
      const { data } = await axios.get<{ count: number; ids: string[] }>(
        getGlossaryTermIdsUrl(),
        { timeout: 8000 },
      );
      // An empty list is a broken source, never "every id is wrong" — the same
      // rule the nightly audit follows.
      if (!Array.isArray(data?.ids) || data.ids.length === 0) return null;
      this.termIds = new Set(data.ids);
      this.fetchedAt = Date.now();
      return this.termIds;
    } catch (error) {
      this.logger.warn(
        `Glossary term ids unavailable, skipping the write-time check: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      return null;
    }
  }
}

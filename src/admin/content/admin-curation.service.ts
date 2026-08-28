import {
  BadRequestException,
  Inject,
  Injectable,
  Logger,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { InjectQueue } from "@nestjs/bull";
import { Queue } from "bull";
import { Redis } from "ioredis";
import { IsNull, Not, Repository } from "typeorm";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import { RevalidationService } from "../../common/revalidation/revalidation.service";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Park } from "../../parks/entities/park.entity";
import { parseHttpUrl } from "../../common/utils/http-url.util";
import { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import { GlossaryTermIdsService } from "./glossary-term-ids.service";
import {
  ATTRACTION_CURATED_KEYS,
  CURATED_FIELD_SPEC_BY_KEY,
  PARK_CURATED_KEYS,
  type CuratedFieldSpec,
} from "./curated-field.spec-list";

/**
 * Delay before the second revalidation sweep.
 *
 * The park endpoint is served `max-age=300, s-maxage=300,
 * stale-while-revalidate=600` — a 900 s worst case at the edge, which nothing
 * in this codebase can purge. Sixteen minutes is that window plus a minute of
 * margin. Copied deliberately from CuratedDataProcessor rather than shared:
 * the two publish paths answer to the same cache and would have to change
 * together anyway, and a shared constant in a third file is how the reason for
 * the number gets separated from the number.
 */
const CDN_SETTLE_MS = 16 * 60 * 1000;

export interface CurationPatch {
  fields: Record<string, unknown>;
  reason?: string | null;
  sourceUrl?: string | null;
}

export interface CurationResult<T> {
  entity: T;
  changed: string[];
  auditId: string | null;
}

/**
 * Writes the curated columns, and everything that has to happen around a write.
 *
 * A curation is four steps, not one, and the order of the last three is
 * load-bearing:
 *
 *  1. write the curated column (never the sync-owned one),
 *  2. evict our own Redis entries for the affected park,
 *  3. tell the frontend to revalidate,
 *  4. tell it again once the Cloudflare window has expired.
 *
 * Doing 3 before 2 does not publish the change — it makes the frontend refetch
 * the pre-write payload, still warm in Redis and at the edge, and pin it in its
 * own data cache for 24 hours. That is how a curated ride profile could be
 * written, announced, and still missing from the ride page the next morning.
 *
 * Every write also produces an audit row carrying the reason and the URL the
 * editor established it from. That is not bookkeeping for its own sake: a
 * curated column is a claim about the world, and the claims that turn out to be
 * wrong are the ones nobody can trace back to a source.
 */
@Injectable()
export class AdminCurationService {
  private readonly logger = new Logger(AdminCurationService.name);

  constructor(
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    @InjectRepository(Park)
    private readonly parks: Repository<Park>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
    private readonly revalidation: RevalidationService,
    private readonly audit: AdminAuditService,
    @InjectQueue("manual-metadata") private readonly curationQueue: Queue,
    private readonly glossary: GlossaryTermIdsService,
  ) {}

  // ── attractions ───────────────────────────────────────────────────────────

  async findAttraction(id: string): Promise<Attraction> {
    const attraction = await this.attractions.findOne({
      where: { id },
      relations: ["park"],
    });
    if (!attraction) throw new NotFoundException(`No attraction ${id}`);
    return attraction;
  }

  async curateAttraction(
    id: string,
    patch: CurationPatch,
    actor: AdminPrincipal,
  ): Promise<CurationResult<Attraction>> {
    const attraction = await this.findAttraction(id);
    const result = await this.applyAttractionPatch(attraction, patch, actor);

    if (result.changed.length > 0) {
      await this.publish(attraction.parkId, [attraction.id]);
    }

    return result;
  }

  /**
   * Curate several rides of one park in a single request.
   *
   * The editor that needs this is the fast-pass table: which of a park's rides
   * sell a queue-jump product is one decision taken across the whole list, and
   * Phantasialand has forty rides. Sending forty PATCHes would work — every
   * step below is per-ride except the last — but it would also fire forty
   * revalidation webhooks at the frontend for one edit.
   *
   * So the per-ride work stays per-ride: each attraction gets its own diff and
   * its own audit row, which is what keeps undo working one ride at a time.
   * Only `publish` is hoisted, and it is the only part that was ever about the
   * park rather than the ride.
   */
  async curateAttractionsBulk(
    parkId: string,
    patches: Array<{ id: string } & CurationPatch>,
    actor: AdminPrincipal,
  ): Promise<{
    changed: Array<{ id: string; changed: string[]; auditId: string | null }>;
  }> {
    const changed: Array<{
      id: string;
      changed: string[];
      auditId: string | null;
    }> = [];

    for (const patch of patches) {
      const attraction = await this.findAttraction(patch.id);
      if (attraction.parkId !== parkId) {
        throw new BadRequestException(
          `"${attraction.name}" is not in this park — a bulk edit stays inside ` +
            `the park it was opened from`,
        );
      }

      const result = await this.applyAttractionPatch(attraction, patch, actor);
      if (result.changed.length > 0) {
        changed.push({
          id: attraction.id,
          changed: result.changed,
          auditId: result.auditId,
        });
      }
    }

    // One eviction, one revalidation, one delayed sweep — for the whole edit.
    if (changed.length > 0) {
      await this.publish(
        parkId,
        changed.map((entry) => entry.id),
      );
    }

    return { changed };
  }

  /**
   * Everything a curation does except publishing it.
   *
   * Split out so the bulk path can run it per ride and publish once. Publishing
   * is the only step that is about the park rather than the row.
   */
  private async applyAttractionPatch(
    attraction: Attraction,
    patch: CurationPatch,
    actor: AdminPrincipal,
  ): Promise<CurationResult<Attraction>> {
    const changes = await this.normalizePatch(
      patch.fields,
      ATTRACTION_CURATED_KEYS,
      CURATED_FIELD_SPEC_BY_KEY.attraction,
    );

    const before: Record<string, unknown> = {};
    const after: Record<string, unknown> = {};
    const changed: string[] = [];

    for (const [key, value] of Object.entries(changes)) {
      const current = (attraction as unknown as Record<string, unknown>)[key];
      if (JSON.stringify(current ?? null) === JSON.stringify(value)) continue;
      before[key] = current ?? null;
      after[key] = value;
      changed.push(key);
      (attraction as unknown as Record<string, unknown>)[key] = value;
    }

    // Nothing changed: no write, no cache eviction, no audit row. A form that
    // PATCHes every field on every blur would otherwise fill the log with
    // empty edits and bury the real ones.
    if (changed.length === 0) {
      return { entity: attraction, changed: [], auditId: null };
    }

    // One RCDB id must never sit on two attractions. Nothing in the database
    // enforces it, and the consequence is not cosmetic: the ride page links to
    // a different ride, and RideStatsService joins Wikidata on this id and
    // hands both rows the same height, speed and length. Two rides already did
    // this once, from a name-based match across parks holding a ride of the
    // same name.
    if (changed.includes("rcdbId") && attraction.rcdbId !== null) {
      const clash = await this.attractions.findOne({
        where: { rcdbId: attraction.rcdbId, id: Not(attraction.id) },
        relations: ["park"],
      });
      if (clash) {
        throw new BadRequestException(
          `RCDB id ${attraction.rcdbId} already belongs to "${clash.name}"` +
            `${clash.park ? ` (${clash.park.name})` : ""}. One id points at one ride.`,
        );
      }
    }

    await this.attractions.save(attraction);

    const auditRow = await this.audit.record({
      actor,
      action: "attraction.curate",
      entityType: "attraction",
      entityId: attraction.id,
      entityLabel: attraction.name,
      before,
      after,
      reason: patch.reason ?? null,
      sourceUrl: patch.sourceUrl ?? null,
    });

    return { entity: attraction, changed, auditId: auditRow?.id ?? null };
  }

  // ── parks ─────────────────────────────────────────────────────────────────

  async findPark(id: string): Promise<Park> {
    const park = await this.parks.findOne({ where: { id } });
    if (!park) throw new NotFoundException(`No park ${id}`);
    return park;
  }

  async curatePark(
    id: string,
    patch: CurationPatch,
    actor: AdminPrincipal,
  ): Promise<CurationResult<Park>> {
    const park = await this.findPark(id);

    const changes = await this.normalizePatch(
      patch.fields,
      PARK_CURATED_KEYS,
      CURATED_FIELD_SPEC_BY_KEY.park,
    );

    const before: Record<string, unknown> = {};
    const after: Record<string, unknown> = {};
    const changed: string[] = [];

    for (const [key, value] of Object.entries(changes)) {
      const current = (park as unknown as Record<string, unknown>)[key];
      if (JSON.stringify(current ?? null) === JSON.stringify(value)) continue;
      before[key] = current ?? null;
      after[key] = value;
      changed.push(key);
      (park as unknown as Record<string, unknown>)[key] = value;
    }

    if (changed.length === 0) {
      return { entity: park, changed: [], auditId: null };
    }

    await this.parks.save(park);

    const auditRow = await this.audit.record({
      actor,
      action: "park.curate",
      entityType: "park",
      entityId: park.id,
      entityLabel: park.name,
      before,
      after,
      reason: patch.reason ?? null,
      sourceUrl: patch.sourceUrl ?? null,
    });

    // A park-level curation changes every ride card in the park's payload
    // (the park name is embedded in each), so the attractions go along.
    const attractionIds = await this.attractionIdsOf(park.id);
    await this.publish(park.id, attractionIds);

    return { entity: park, changed, auditId: auditRow?.id ?? null };
  }

  async attractionIdsOf(parkId: string): Promise<string[]> {
    const rows = await this.attractions.find({
      where: { parkId, retiredAt: IsNull() },
      select: { id: true },
    });
    return rows.map((row) => row.id);
  }

  // ── undo ──────────────────────────────────────────────────────────────────

  /**
   * Put a curation back the way it was.
   *
   * An undo is itself an edit and gets its own audit row — the original is
   * marked as reverted rather than deleted, because "this was changed and then
   * changed back" is a different fact from "this was never changed", and only
   * the first one explains why somebody's note is attached to a value that
   * matches upstream.
   */
  async revert(
    auditId: string,
    actor: AdminPrincipal,
  ): Promise<CurationResult<unknown>> {
    const entry = await this.audit.findOne(auditId);
    if (!entry) throw new NotFoundException(`No audit entry ${auditId}`);
    if (entry.revertedBy) {
      throw new BadRequestException("That change has already been undone");
    }
    if (!entry.before) {
      throw new BadRequestException(
        "That entry records a creation, not a change",
      );
    }
    if (entry.entityType !== "attraction" && entry.entityType !== "park") {
      throw new BadRequestException(
        `Undo is only available for curation entries, not "${entry.action}"`,
      );
    }
    if (!entry.entityId) {
      throw new BadRequestException("That entry names no entity to undo");
    }

    // Refuse to undo a change something else has already changed again.
    //
    // Without this, undoing an older entry silently discards every correction
    // made since: editor A sets a height null→120, editor B corrects it
    // 120→140, somebody undoes A's entry and the column becomes null — B's work
    // gone, B's entry still standing in the log as though it were current. The
    // check is "is the field still where this entry left it", which is exactly
    // the question an undo is entitled to assume.
    const current = (entry.entityType === "attraction"
      ? await this.findAttraction(entry.entityId)
      : await this.findPark(entry.entityId)) as unknown as Record<
      string,
      unknown
    >;

    const drifted = Object.entries(entry.after ?? {}).filter(
      ([key, value]) =>
        JSON.stringify(current[key] ?? null) !== JSON.stringify(value ?? null),
    );
    if (drifted.length > 0) {
      throw new BadRequestException(
        `This change has been overwritten since — ${drifted
          .map(([key]) => key)
          .join(
            ", ",
          )} no longer holds the value it set. Undo the newer change first.`,
      );
    }

    const patch: CurationPatch = {
      fields: entry.before,
      reason: `Undo of ${entry.action} from ${entry.createdAt.toISOString()}`,
    };

    const result =
      entry.entityType === "attraction"
        ? await this.curateAttraction(entry.entityId, patch, actor)
        : await this.curatePark(entry.entityId, patch, actor);

    if (result.auditId) await this.audit.markReverted(auditId, result.auditId);
    return result;
  }

  // ── plumbing ──────────────────────────────────────────────────────────────

  /**
   * Coerce and validate a PATCH body against the field descriptors.
   *
   * Only keys the descriptors know are accepted, so a typo'd or hostile key
   * cannot reach a sync-owned column through this endpoint — which is the
   * whole point of keeping the two apart. `null` is meaningful and means
   * "clear the correction, accept upstream again"; it is not the same as
   * omitting the key, which leaves the field alone.
   */
  private async normalizePatch(
    fields: Record<string, unknown>,
    allowed: Set<string>,
    specs: Map<string, CuratedFieldSpec>,
  ): Promise<Record<string, unknown>> {
    const output: Record<string, unknown> = {};
    const termIds: string[] = [];

    for (const [key, raw] of Object.entries(fields ?? {})) {
      if (!allowed.has(key)) {
        throw new BadRequestException(
          `"${key}" is not a curatable field — see the field descriptors on the detail endpoint`,
        );
      }
      const spec = specs.get(key)!;
      const value = this.coerce(spec, raw);
      output[key] = value;
      if (spec.type === "glossaryTerm" && typeof value === "string") {
        termIds.push(value);
      }
    }

    // Checked after coercion rather than inside it, because it is the one
    // validation that leaves this process: the glossary lives in the frontend.
    // A wrong id does not error anywhere downstream — the chip just links
    // nowhere — so this is the only moment anybody finds out.
    const unknown = await this.glossary.unknownIds(termIds);
    if (unknown.length > 0) {
      throw new BadRequestException(
        `The glossary does not define these term ids, so they would link ` +
          `nowhere: ${unknown.join(", ")}`,
      );
    }

    return output;
  }

  private coerce(spec: CuratedFieldSpec, raw: unknown): unknown {
    // Clearing a field writes what "nothing decided" is for that column — null
    // for almost all of them, `false` for `open_with_park`, which is NOT NULL.
    // Writing null there is an UPDATE the database rejects, and the editor
    // offers to clear it because the stored `false` looked like a correction.
    const unset = spec.defaultValue ?? null;
    if (raw === null || raw === undefined) return unset;

    // An emptied numeric input is "no correction", not zero — and on a curated
    // height, zero is a positive claim that the ride has no minimum at all.
    // The admin sends null, but curl and any future client will send "".
    if (
      raw === "" &&
      (spec.type === "number" ||
        spec.type === "decimal" ||
        spec.type === "months")
    ) {
      return unset;
    }

    switch (spec.type) {
      case "text":
      case "longtext":
      // Same input, same trimming — the id is checked separately, against a
      // list that lives in another application.
      case "glossaryTerm": {
        if (typeof raw !== "string") {
          throw new BadRequestException(`${spec.label} must be text`);
        }
        const trimmed = raw.trim();
        // An emptied input clears the correction rather than storing "".
        if (trimmed.length === 0) return unset;
        if (spec.maxLength !== undefined && trimmed.length > spec.maxLength) {
          throw new BadRequestException(
            `${spec.label} must be at most ${spec.maxLength} characters`,
          );
        }
        return trimmed;
      }

      case "url": {
        if (typeof raw !== "string") {
          throw new BadRequestException(`${spec.label} must be text`);
        }
        const trimmed = raw.trim();
        if (trimmed.length === 0) return unset;
        if (spec.maxLength !== undefined && trimmed.length > spec.maxLength) {
          throw new BadRequestException(
            `${spec.label} must be at most ${spec.maxLength} characters`,
          );
        }
        // See `parseHttpUrl` — these end up as `href`s on a public page.
        return parseHttpUrl(trimmed, spec.label);
      }

      case "enum": {
        if (typeof raw !== "string") {
          throw new BadRequestException(`${spec.label} must be text`);
        }
        const trimmed = raw.trim();
        if (trimmed.length === 0) return unset;
        if (spec.options && !spec.options.includes(trimmed)) {
          throw new BadRequestException(
            `${spec.label} must be one of: ${spec.options.join(", ")}`,
          );
        }
        return trimmed;
      }

      case "boolean": {
        if (typeof raw !== "boolean") {
          throw new BadRequestException(`${spec.label} must be true or false`);
        }
        return raw;
      }

      case "number": {
        const value = typeof raw === "string" ? Number(raw) : raw;
        if (typeof value !== "number" || !Number.isFinite(value)) {
          throw new BadRequestException(`${spec.label} must be a number`);
        }
        if (!Number.isInteger(value)) {
          throw new BadRequestException(`${spec.label} must be a whole number`);
        }
        if (spec.min !== undefined && value < spec.min) {
          throw new BadRequestException(
            `${spec.label} must be at least ${spec.min}`,
          );
        }
        if (spec.max !== undefined && value > spec.max) {
          throw new BadRequestException(
            `${spec.label} must be at most ${spec.max}`,
          );
        }
        return value;
      }

      case "decimal": {
        const value = typeof raw === "string" ? Number(raw) : raw;
        if (typeof value !== "number" || !Number.isFinite(value)) {
          throw new BadRequestException(`${spec.label} must be a number`);
        }
        if (spec.min !== undefined && value < spec.min) {
          throw new BadRequestException(
            `${spec.label} must be at least ${spec.min}`,
          );
        }
        if (spec.max !== undefined && value > spec.max) {
          throw new BadRequestException(
            `${spec.label} must be at most ${spec.max}`,
          );
        }
        // Two decimals is more than any park's area is known to.
        return Math.round(value * 100) / 100;
      }

      case "months": {
        if (!Array.isArray(raw)) {
          throw new BadRequestException(
            `${spec.label} must be a list of months`,
          );
        }
        // An empty list would say "operates in no month at all", which is what
        // retirement is for. It clears the correction instead.
        if (raw.length === 0) return unset;
        const months = raw.map((entry) =>
          typeof entry === "string" ? Number(entry) : entry,
        );
        for (const month of months) {
          if (
            typeof month !== "number" ||
            !Number.isInteger(month) ||
            month < 1 ||
            month > 12
          ) {
            throw new BadRequestException(
              `${spec.label} must contain whole numbers between 1 and 12`,
            );
          }
        }
        // Sorted and deduped: these are a set, and two rows differing only in
        // the order somebody clicked the months would read as a change.
        return [...new Set(months as number[])].sort((a, b) => a - b);
      }
    }
  }

  /**
   * Make a write visible, in the only order that works. See the class doc.
   *
   * Never throws: the write has already happened and the caller has already
   * been told it succeeded. A failure here means the change surfaces on a TTL
   * instead of immediately, which is a delay, not a lie.
   */
  async publish(parkId: string, attractionIds: string[]): Promise<void> {
    try {
      await invalidateParkCaches(this.redis, parkId, attractionIds);
    } catch (error) {
      // Also the SKIP_REDIS=true path, where the injected client is a stub
      // with no `del`. A dev instance must not fail a curation over it.
      this.logger.warn(
        `Cache invalidation failed for park ${parkId}: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }

    await this.revalidation.revalidateTags(["parks", "attractions"]);

    try {
      await this.curationQueue.add(
        "revalidate-parks",
        {},
        {
          delay: CDN_SETTLE_MS,
          removeOnComplete: true,
          removeOnFail: true,
          // Fixed id: a curation session is a burst of small edits, and each
          // one must not leave its own pending sweep. One is enough — they all
          // revalidate the same two tags.
          jobId: "revalidate-parks-after-cdn",
        },
      );
    } catch (error) {
      this.logger.warn(
        `Could not schedule the post-CDN revalidation sweep: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
    }
  }
}

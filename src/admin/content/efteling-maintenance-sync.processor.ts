import { Process, Processor } from "@nestjs/bull";
import { Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import axios from "axios";
import { Job } from "bull";
import { IsNull, Repository } from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";
import {
  MAX_WORKS_PERIODS,
  normalizeWorksPeriod,
  resolveWorksPeriods,
  type WorksPeriod,
  worksPeriodsOverlap,
} from "../../attractions/utils/curated-out-of-service.util";
import { BROWSER_HEADERS } from "../../common/constants/http-headers.constant";
import { getCurrentDateInTimezone } from "../../common/utils/date.util";
import {
  EFTELING_MAINTENANCE_URL,
  type EftelingMaintenanceRow,
  parseEftelingMaintenance,
} from "../../external-apis/efteling/efteling-maintenance.parser";
import { Park } from "../../parks/entities/park.entity";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminCurationService } from "./admin-curation.service";

/** The `manual-metadata` job, weekly and from the admin. */
export const SYNC_EFTELING_MAINTENANCE_JOB = "sync-efteling-maintenance";

/** `source` on every window this sync writes; windows without it are never touched. */
export const EFTELING_MAINTENANCE_SOURCE = "efteling-maintenance-calendar";

/** Who the sync writes as, the way the fun-fact translator does. */
export const EFTELING_MAINTENANCE_SYNC: AdminPrincipal = {
  userId: null,
  email: "maintenance-sync@park.fan",
  displayName: "Efteling maintenance calendar",
  role: "editor",
  sessionToken: null,
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
};

export interface RideWindowDiff {
  added: WorksPeriod[];
  updated: Array<{ before: WorksPeriod; after: WorksPeriod }>;
  removed: WorksPeriod[];
  skipped: Array<{ window: WorksPeriod; reason: string }>;
  /** The ride's whole list after the sync, sorted. */
  next: WorksPeriod[];
}

export interface EftelingSyncResult {
  dryRun: boolean;
  today: string;
  windowsListed: number;
  unparsed: string[];
  unmatched: string[];
  totals: { added: number; updated: number; removed: number; skipped: number };
  rides: Array<
    { id: string; name: string } & Omit<RideWindowDiff, "next"> & {
        auditId?: string | null;
      }
  >;
}

/** Case-, diacritics- and whitespace-insensitive, for exact name matching. */
export function normalizeRideName(name: string): string {
  return name
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .replace(/\s+/g, " ")
    .trim();
}

/**
 * Each page name to the one ride it names: exactly on `name`, else exactly on
 * `curatedName`. Two candidates are as unmatched as none — never guess.
 */
export function matchRides<
  T extends { name: string; curatedName?: string | null },
>(
  names: string[],
  rides: T[],
): { matched: Map<string, T>; unmatched: string[] } {
  const matched = new Map<string, T>();
  const unmatched: string[] = [];
  for (const name of new Set(names)) {
    const key = normalizeRideName(name);
    let hits = rides.filter((ride) => normalizeRideName(ride.name) === key);
    if (hits.length === 0) {
      hits = rides.filter(
        (ride) =>
          ride.curatedName && normalizeRideName(ride.curatedName) === key,
      );
    }
    if (hits.length === 1) matched.set(name, hits[0]);
    else unmatched.push(name);
  }
  return { matched, unmatched };
}

/**
 * What the page's rows mean for one ride's list.
 *
 * Only windows carrying {@link EFTELING_MAINTENANCE_SOURCE} are the sync's. A
 * listed row updates the sync's window it overlaps, else is added; a sync
 * window no row claims is removed only while it has not started (park-local
 * `today`) — one that ran is the ride's history. A row with no start ("until
 * X") starts on the day the sync first saw it, never open: an open start would
 * cover the ride's whole past. Nothing is written that would overlap a
 * hand-written window; that row is skipped and reported.
 */
export function diffRideWindows(
  current: readonly WorksPeriod[],
  listed: ReadonlyArray<Pick<EftelingMaintenanceRow, "from" | "to">>,
  today: string,
): RideWindowDiff {
  const own = current.filter((w) => w.source === EFTELING_MAINTENANCE_SOURCE);
  const next = current.filter((w) => w.source !== EFTELING_MAINTENANCE_SOURCE);
  const claimed = new Set<WorksPeriod>();
  const diff: RideWindowDiff = {
    added: [],
    updated: [],
    removed: [],
    skipped: [],
    next,
  };

  const rows = [...listed].sort((a, b) =>
    (a.from ?? today) < (b.from ?? today) ? -1 : 1,
  );
  for (const row of rows) {
    const probe = normalizeWorksPeriod({ from: row.from ?? today, to: row.to });
    const mine = own.find(
      (w) => !claimed.has(w) && worksPeriodsOverlap(w, probe),
    );
    if (mine) claimed.add(mine);
    const from =
      row.from ?? (mine?.from && mine.from <= today ? mine.from : today);
    const want = normalizeWorksPeriod({
      from,
      to: row.to,
      toUncertain: false,
      source: EFTELING_MAINTENANCE_SOURCE,
    });

    if (from > row.to) {
      // "until" a day already past: the page has not caught up yet.
      if (mine) next.push(mine);
      else diff.skipped.push({ window: want, reason: "already over" });
      continue;
    }
    const clash = next.find((w) => worksPeriodsOverlap(w, want));
    if (clash) {
      if (mine) next.push(mine);
      diff.skipped.push({
        window: want,
        reason: clash.source
          ? "overlaps another listed window"
          : `overlaps a hand-written window (${clash.from ?? "open start"} – ${clash.to ?? "open end"})`,
      });
      continue;
    }

    if (!mine) diff.added.push(want);
    else if (JSON.stringify(mine) !== JSON.stringify(want)) {
      diff.updated.push({ before: mine, after: want });
    }
    next.push(want);
  }

  for (const window of own) {
    if (claimed.has(window)) continue;
    // Not started, or swallowed by a listed window that now covers it.
    if (
      (window.from && window.from > today) ||
      next.some((w) => worksPeriodsOverlap(w, window))
    ) {
      diff.removed.push(window);
    } else next.push(window);
  }

  diff.next = resolveWorksPeriods({ curatedOutOfServicePeriods: next });
  if (diff.next.length > MAX_WORKS_PERIODS && diff.added.length > 0) {
    // The write would be refused; leave the ride as it is and say why.
    return {
      added: [],
      updated: [],
      removed: [],
      skipped: [
        ...diff.skipped,
        ...diff.added.map((window) => ({
          window,
          reason: `more than ${MAX_WORKS_PERIODS} windows`,
        })),
      ],
      next: resolveWorksPeriods({ curatedOutOfServicePeriods: current }),
    };
  }
  return diff;
}

/**
 * Keeps Efteling's rides' works periods in line with the park's maintenance
 * calendar, weekly (PAR-1245). One page request; writes go through the admin's
 * curation path, so each ride gets an audit row and the park is published once.
 *
 * A page that no longer has the "Closed" list, or whose list parses to nothing,
 * fails the job and writes nothing — that is a layout change, not a calendar
 * with no works in it.
 */
@Processor("manual-metadata")
export class EftelingMaintenanceSyncProcessor {
  private readonly logger = new Logger(EftelingMaintenanceSyncProcessor.name);

  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    private readonly curation: AdminCurationService,
  ) {}

  @Process(SYNC_EFTELING_MAINTENANCE_JOB)
  async handle(job: Job<{ dryRun?: boolean }>): Promise<EftelingSyncResult> {
    return this.sync(job.data?.dryRun === true);
  }

  async sync(dryRun: boolean): Promise<EftelingSyncResult> {
    const parks = await this.parks.find({ where: { slug: "efteling" } });
    if (parks.length !== 1) {
      throw new Error(
        `Expected one park with slug "efteling", found ${parks.length}`,
      );
    }
    const park = parks[0];
    const today = getCurrentDateInTimezone(park.timezone);

    const response = await axios.get<string>(EFTELING_MAINTENANCE_URL, {
      headers: { ...BROWSER_HEADERS, Accept: "text/html" },
      timeout: 20_000,
      responseType: "text",
    });
    const { rows, unparsed } = parseEftelingMaintenance(response.data);

    const rides = await this.attractions.find({
      where: { parkId: park.id, retiredAt: IsNull() },
    });
    const { matched, unmatched } = matchRides(
      rows.map((row) => row.name),
      rides,
    );

    const listedByRide = new Map<string, EftelingMaintenanceRow[]>();
    for (const row of rows) {
      const ride = matched.get(row.name);
      if (ride)
        listedByRide.set(ride.id, [...(listedByRide.get(ride.id) ?? []), row]);
    }

    const result: EftelingSyncResult = {
      dryRun,
      today,
      windowsListed: rows.length,
      unparsed,
      unmatched,
      totals: { added: 0, updated: 0, removed: 0, skipped: 0 },
      rides: [],
    };
    const patches: Array<{
      id: string;
      fields: Record<string, unknown>;
      reason: string;
      sourceUrl: string;
    }> = [];

    for (const ride of rides) {
      const current = resolveWorksPeriods(ride);
      const listed = listedByRide.get(ride.id) ?? [];
      // A ride the page does not list and the sync never wrote to is not its business.
      if (
        listed.length === 0 &&
        !current.some((w) => w.source === EFTELING_MAINTENANCE_SOURCE)
      ) {
        continue;
      }
      const { next, ...diff } = diffRideWindows(current, listed, today);
      const counts = {
        added: diff.added.length,
        updated: diff.updated.length,
        removed: diff.removed.length,
        skipped: diff.skipped.length,
      };
      if (Object.values(counts).every((n) => n === 0)) continue;
      for (const key of Object.keys(counts) as Array<keyof typeof counts>) {
        result.totals[key] += counts[key];
      }
      result.rides.push({ id: ride.id, name: ride.name, ...diff });
      this.logger.log(
        `🛠️ ${ride.name}: +${counts.added} ~${counts.updated} -${counts.removed} skipped ${counts.skipped}` +
          diff.skipped
            .map((s) => ` [${s.window.from}–${s.window.to}: ${s.reason}]`)
            .join(""),
      );
      if (counts.added + counts.updated + counts.removed > 0) {
        patches.push({
          id: ride.id,
          fields: { curatedOutOfServicePeriods: next },
          reason: `Efteling maintenance calendar as read on ${today}: ${counts.added} added, ${counts.updated} updated, ${counts.removed} removed`,
          sourceUrl: EFTELING_MAINTENANCE_URL,
        });
      }
    }

    if (unmatched.length > 0) {
      this.logger.warn(
        `🛠️ Efteling names matching no ride: ${unmatched.join(", ")}`,
      );
    }
    if (unparsed.length > 0) {
      this.logger.warn(
        `🛠️ Efteling rows with unreadable dates: ${unparsed.join("; ")}`,
      );
    }

    if (!dryRun && patches.length > 0) {
      const { changed } = await this.curation.curateAttractionsBulk(
        park.id,
        patches,
        EFTELING_MAINTENANCE_SYNC,
      );
      for (const entry of result.rides) {
        entry.auditId = changed.find((c) => c.id === entry.id)?.auditId ?? null;
      }
    }

    const t = result.totals;
    this.logger.log(
      `🛠️ Efteling maintenance sync${dryRun ? " (dry run)" : ""}: ${rows.length} windows listed, ` +
        `+${t.added} ~${t.updated} -${t.removed} skipped ${t.skipped}, ` +
        `${unmatched.length} unmatched, ${unparsed.length} unparsed`,
    );
    return result;
  }
}

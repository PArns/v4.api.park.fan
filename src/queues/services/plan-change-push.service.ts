import { createHash } from "crypto";
import { Inject, Injectable, Logger } from "@nestjs/common";
import { Redis } from "ioredis";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import { QueueType } from "../../external-apis/themeparks/themeparks.types";
import type { Park } from "../../parks/entities/park.entity";
import { ParksService } from "../../parks/parks.service";
import { cachedPlanDay } from "../../parks/services/plan-day-cache";
import { PlanDayService } from "../../parks/services/plan-day.service";
import {
  buildPlanDayFacts,
  isDigestDue,
  nextBaseline,
  planChanges,
  planDaysToWatch,
  WAIT_DAYS_AHEAD,
  type PlanDayFacts,
  type PlanDayInput,
  type PlanDayToWatch,
  type StandbyReading,
} from "../../push/plan-change";
import { isApnsConfigured } from "../../push/apns";
import {
  parkDayThread,
  writePlanChangeMessage,
} from "../../push/push-messages";
import { isWithinQuietHours } from "../../push/quiet-hours";
import { PushSubscription } from "../../push/entities/push-subscription.entity";
import { PushService } from "../../push/push.service";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { TripsService } from "../../trips/trips.service";

interface StoredBaseline {
  facts: PlanDayFacts;
  sentMs: number | null;
}

/**
 * The `plan-change` producer (rules in `push/plan-change.ts`): for each iOS
 * device with the kind and a stored plan, compares each plan day from the
 * park's today on with what the device last heard, and sends one digest when
 * something it was planned around changed.
 *
 * **Bounded.** Devices only, so an install base without the switch costs one
 * indexed query. At most `MAX_PLAN_DAYS_PER_RUN` plan days per tick; `/plan/day`
 * comes from its shared quarter-hour cache, and a day that needs neither shows,
 * statuses nor forecasts reads only the schedule. Each day is isolated: one
 * failing park never stops the others.
 *
 * **Baseline in Redis**, per trip, park and day, until two days after the
 * date. Eviction loses a baseline, and the next tick starts a new one in
 * silence: the failure is a missed notice, never a false one.
 */
@Injectable()
export class PlanChangePushService {
  private readonly logger = new Logger(PlanChangePushService.name);

  // shortcut: N plan days per tick, trips in random order so none starves;
  // rotate a cursor when the warning below shows in the logs every tick.
  static readonly MAX_PLAN_DAYS_PER_RUN = 150;
  /** One trip's share of a tick, nearest days first: a 60-park plan cannot take it all. */
  static readonly MAX_PLAN_DAYS_PER_TRIP = 31;
  /** Wall-clock cap: the tick also sends `next-up`, which cannot wait for this half. */
  static readonly MAX_RUN_MS = 60_000;
  /** A standby row older than this says nothing about now. */
  private static readonly STATUS_WINDOW_MINUTES = 6 * 60;

  constructor(
    private readonly pushService: PushService,
    private readonly tripsService: TripsService,
    private readonly parksService: ParksService,
    private readonly planDayService: PlanDayService,
    private readonly queueDataService: QueueDataService,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async run(nowMs: number): Promise<number> {
    // Without APNs nothing can be sent, and a baseline only moves on a send.
    if (!isApnsConfigured()) return 0;
    const startedMs = Date.now();
    const devices =
      await this.pushService.devicesWithTripAndKind("plan-change");
    if (devices.length === 0) return 0;

    const byTrip = new Map<string, PushSubscription[]>();
    for (const device of devices) {
      if (!device.tripId) continue;
      byTrip.set(device.tripId, [...(byTrip.get(device.tripId) ?? []), device]);
    }

    const parks = new Map<string, Promise<Park | null>>();
    let budget = PlanChangePushService.MAX_PLAN_DAYS_PER_RUN;
    let sent = 0;
    const spent = () =>
      budget <= 0 || Date.now() - startedMs > PlanChangePushService.MAX_RUN_MS;
    const trips = [...byTrip].sort(() => Math.random() - 0.5);
    for (const [tripId, subscriptions] of trips) {
      if (spent()) {
        this.logger.warn(
          `plan-change: tick budget spent, ${byTrip.size} trip(s) watched`,
        );
        break;
      }
      const trip = await this.tripsService.find(tripId).catch(() => null);
      if (!trip) continue;
      const days = planDaysToWatch(trip.payload, nowMs).slice(
        0,
        PlanChangePushService.MAX_PLAN_DAYS_PER_TRIP,
      );
      for (const day of days) {
        if (spent()) break;
        budget--;
        try {
          sent += await this.checkDay(tripId, day, subscriptions, parks, nowMs);
        } catch (error) {
          this.logger.warn(
            `plan-change: ${day.slug} ${day.date} failed: ${(error as Error)?.message ?? error}`,
          );
        }
      }
    }
    return sent;
  }

  private async checkDay(
    tripId: string,
    day: PlanDayToWatch,
    subscriptions: PushSubscription[],
    parks: Map<string, Promise<Park | null>>,
    nowMs: number,
  ): Promise<number> {
    const geoKey = `${day.geo.continent}/${day.geo.country}/${day.geo.city}/${day.slug}`;
    if (!parks.has(geoKey)) {
      parks.set(
        geoKey,
        this.parksService.findByGeographicPath(
          day.geo.continent,
          day.geo.country,
          day.geo.city,
          day.slug,
        ),
      );
    }
    const park = await parks.get(geoKey)!;
    // The plan's zone is client input; one that is not the park's own moves the day.
    if (!park?.timezone || park.timezone !== day.timezone) return 0;

    const key = `push:plan-change:${hash(tripId)}:${day.slug}:${day.date}`;
    let stored: StoredBaseline | null;
    try {
      const raw = await this.redis.get(key);
      stored = raw ? (JSON.parse(raw) as StoredBaseline) : null;
    } catch {
      return 0; // No baseline to trust this tick; never guess one.
    }

    const named = day.entries.some((e) => e.attractionSlug || e.showSlug);
    const planDay: PlanDayInput | null =
      named && (day.isToday || day.daysAhead <= WAIT_DAYS_AHEAD)
        ? await cachedPlanDay(this.redis, this.planDayService, park, day.date)
        : null;
    const schedule = await this.parksService.getScheduleForDate(
      park.id,
      day.date,
    );

    const standby = new Map<string, StandbyReading>();
    if (day.isToday && planDay) {
      const slugs = new Set(day.entries.map((e) => e.attractionSlug));
      const ids = planDay.rides
        .filter((r) => slugs.has(r.attractionSlug))
        .map((r) => r.attractionId);
      const rows = await this.queueDataService.findCurrentStatusByAttractionIds(
        ids,
        PlanChangePushService.STATUS_WINDOW_MINUTES,
      );
      for (const [id, list] of rows) {
        const row = list.find((r) => r.queueType === QueueType.STANDBY);
        if (row) {
          standby.set(id, {
            status: row.status,
            isHeartbeat: !!row.isHeartbeat,
            timestampMs: new Date(row.timestamp).getTime(),
          });
        }
      }
    }

    const base = stored?.facts ?? null;
    const cur = buildPlanDayFacts({
      day,
      nowMs,
      schedule,
      planDay,
      standby,
      previous: base,
    });
    const changes = planChanges(base, cur, day.entries);

    let sent = 0;
    if (isDigestDue(changes, stored?.sentMs ?? null, nowMs)) {
      const copy = {
        dedupeKey: `plan-change:${day.slug}:${day.date}`,
        parkName: day.name,
        date: day.date,
        isToday: day.isToday,
        changes,
        url: `/trip-planner?park=${encodeURIComponent(geoKey)}&date=${day.date}`,
        // Today's change is about the next hour; a later day's may wait for the phone.
        expiresAtMs: nowMs + (day.isToday ? 1 : 24) * 60 * 60_000,
        threadId: parkDayThread(day.slug, day.date),
        data: { park: geoKey, date: day.date },
      };
      for (const subscription of subscriptions) {
        // Held back, not dropped: the baseline stays, so the change is still
        // pending when the subscriber's morning comes.
        if (isWithinQuietHours(subscription.timezone, nowMs)) continue;
        const message = writePlanChangeMessage(copy, subscription.locale);
        if (await this.pushService.send(subscription, message)) sent++;
      }
    }

    const next: StoredBaseline = {
      facts: nextBaseline(base, cur, day.entries, sent > 0),
      sentMs: sent > 0 ? nowMs : (stored?.sentMs ?? null),
    };
    const ttl = Math.max(
      3600,
      Math.round(
        (Date.parse(`${day.date}T00:00:00Z`) + 3 * 86_400_000 - nowMs) / 1000,
      ),
    );
    await this.redis.set(key, JSON.stringify(next), "EX", ttl);
    return sent;
  }
}

function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex").slice(0, 24);
}

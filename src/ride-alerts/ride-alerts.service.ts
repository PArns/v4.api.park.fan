import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, IsNull, Repository } from "typeorm";
import { RideAlert } from "./entities/ride-alert.entity";
import {
  diffRideAlerts,
  type AlertTrigger,
  type RideReading,
} from "./ride-alert-transitions";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { PushSubscription } from "../push/entities/push-subscription.entity";
import { PushService } from "../push/push.service";
import { writeRideAlertMessage } from "../push/push-messages";
import { QueueDataService } from "../queue-data/queue-data.service";
import { QueueData } from "../queue-data/entities/queue-data.entity";
import {
  QueueType,
  LiveStatus,
} from "../external-apis/themeparks/themeparks.types";
import { getNoLiveWaitTimesReason } from "../parks/data/live-wait-time-sources";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../attractions/utils/curated-attraction-facts.util";
import { frontendAttractionPath } from "../common/utils/frontend-url.util";
import { getStartOfDayInTimezoneAt } from "../common/utils/date.util";

/** How many alerts one browser may hold — a person watches a handful of rides. */
export const MAX_RIDE_ALERTS_PER_SUBSCRIPTION = 100;

/**
 * How stale a reading may be and still arm/trigger an alert — three 5-minute
 * poll cycles, enough slack for one delayed cycle without accepting a truly
 * stale row. `findCurrentStatusByAttractionIds`'s own default (6 hours) is
 * sized for a batch admin view, not a "is this happening right now" decision.
 */
const MAX_READING_AGE_MINUTES = 15;

export interface AttractionForAlert {
  attraction: Attraction;
  park: Park;
}

/** The park fields a wait-time-alert check needs — the caller's poll loop already has all of them. */
export interface ParkForRideAlertCheck {
  name: string;
  slug: string;
  citySlug: string | null;
  countrySlug: string | null;
  continentSlug: string | null;
  /** For the day-boundary re-arm below. Falsy means skip that step, never default to UTC. */
  timezone: string;
}

@Injectable()
export class RideAlertsService {
  private readonly logger = new Logger(RideAlertsService.name);

  /** How many triggers to send concurrently — see `checkAndNotify`. */
  private static readonly SEND_BATCH_SIZE = 20;

  constructor(
    @InjectRepository(RideAlert)
    private readonly repository: Repository<RideAlert>,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    private readonly queueDataService: QueueDataService,
    private readonly pushService: PushService,
  ) {}

  /**
   * The attraction an alert would watch, with its park — or `null` for one
   * that does not exist or has been retired. A retired ride's wait time will
   * never move again, so an alert against it can never fire.
   */
  async findAttractionForAlert(
    attractionId: string,
  ): Promise<AttractionForAlert | null> {
    const attraction = await this.attractionRepository.findOne({
      where: { id: attractionId, retiredAt: IsNull() },
      relations: { park: true },
    });
    if (!attraction || !attraction.park) return null;
    return { attraction, park: attraction.park };
  }

  async countForSubscription(subscriptionId: string): Promise<number> {
    return this.repository.count({ where: { subscriptionId } });
  }

  async find(
    subscriptionId: string,
    attractionId: string,
  ): Promise<RideAlert | null> {
    return this.repository.findOne({ where: { subscriptionId, attractionId } });
  }

  /**
   * Upsert on `(subscriptionId, attractionId)` — a real database upsert, not
   * read-then-write: two concurrent POSTs for the same ride (a double-tap)
   * used to both pass the `findOne` check, then have the second `save()`'s
   * INSERT hit the unique index and surface as a bare 500. `ON CONFLICT ...
   * DO UPDATE` makes the write atomic instead, so the second caller safely
   * updates the row the first one just created rather than racing it.
   *
   * Re-arms against the RIDE'S CURRENT READING rather than unconditionally —
   * a changed threshold, or simply asking again, is a fresh request to be
   * told, but if the wait is already under the new threshold that request is
   * already satisfied, and arming it would fire on the very next poll about
   * the number the visitor was already looking at when they pressed the
   * button. Unknown (no reading, not OPERATING, a carried-forward heartbeat)
   * arms rather than withholding — the same "not sure, don't suppress"
   * default `checkAndNotify` uses for `isCurrentlyInSeason`.
   *
   * `lastTriggeredAt` deliberately stays out of the upserted columns: it is
   * the sweep's own state, and a re-sent threshold must not erase when this
   * alert last fired. Left off a fresh row, it takes the column's own
   * default (null); on conflict, `ON CONFLICT DO UPDATE` only ever touches
   * columns actually present in `.upsert()`'s entity, so an omitted column
   * is never part of the `SET` clause and the existing value survives.
   */
  async upsert(
    subscriptionId: string,
    attractionId: string,
    thresholdMinutes: number,
  ): Promise<RideAlert> {
    const armed = await this.isBelowThresholdAlreadyUnknownOrFalse(
      attractionId,
      thresholdMinutes,
    );
    await this.repository.upsert(
      {
        subscriptionId,
        attractionId,
        thresholdMinutes,
        armed,
        // Set explicitly rather than relying on `@UpdateDateColumn`'s
        // `onUpdate` — that is triggered by TypeORM's own `save()`
        // lifecycle, which this raw upsert query does not go through.
        updatedAt: new Date(),
      },
      { conflictPaths: ["subscriptionId", "attractionId"] },
    );
    // `.upsert()` returns an `InsertResult`, not the row shape the rest of
    // this service and its callers expect — one more indexed point lookup
    // (this is a user-initiated write, not a hot path) is simpler and safer
    // than parsing `InsertResult.raw`/`generatedMaps` by hand.
    return (await this.find(subscriptionId, attractionId))!;
  }

  /** Whether a fresh alert at `thresholdMinutes` should start armed — see `upsert`. */
  private async isBelowThresholdAlreadyUnknownOrFalse(
    attractionId: string,
    thresholdMinutes: number,
  ): Promise<boolean> {
    const statusByAttraction =
      await this.queueDataService.findCurrentStatusByAttractionIds(
        [attractionId],
        MAX_READING_AGE_MINUTES,
      );
    const standby = (statusByAttraction.get(attractionId) ?? []).find(
      (row) => row.queueType === QueueType.STANDBY,
    );
    if (!standby || standby.status !== LiveStatus.OPERATING) return true;
    if (standby.isHeartbeat) return true;
    if (standby.waitTime === null || standby.waitTime === undefined) {
      return true;
    }
    return standby.waitTime >= thresholdMinutes;
  }

  /** Idempotent — removing an alert that is not there is not an error. */
  async remove(subscriptionId: string, attractionId: string): Promise<void> {
    await this.repository.delete({ subscriptionId, attractionId });
  }

  /** Every alert for one subscription, with the attraction+park to render it. */
  async listForSubscription(subscriptionId: string): Promise<RideAlert[]> {
    return this.repository.find({
      where: { subscriptionId },
      relations: { attraction: { park: true } },
      order: { createdAt: "ASC" },
    });
  }

  /**
   * Check this cycle's polled attractions against any alerts on them, and
   * send what just crossed. Invoked from `WaitTimesProcessor` right after its
   * batch save for one park — never on a cron of its own, because "just
   * polled" already is the intraday gate: a park not open today produces no
   * fresh readings, and this only ever looks at ones that exist.
   *
   * `nowMs` comes from the caller's own captured cycle start rather than a
   * `new Date()` in here, so the day-boundary re-arm below gives the same
   * answer on every read of one cycle's data.
   *
   * Best-effort like `ParkStatusRevalidationService` — logs and returns
   * rather than throwing, so a bug here can never fail the wait-times cycle
   * it rides on.
   */
  async checkAndNotify(
    park: ParkForRideAlertCheck,
    polledAttractionIds: string[],
    nowMs: number,
  ): Promise<void> {
    try {
      if (polledAttractionIds.length === 0) return;

      // Permanent, not a freshness check — a park with no real wait-time feed
      // (Hansa-Park) must never be read as "every ride shows 0 minutes".
      if (getNoLiveWaitTimesReason(park.citySlug, park.slug)) return;

      // One query serves both "does anyone have an alert on this park's
      // polled rides at all" (almost always no, and cheap to find out) and
      // the alert rows themselves, used again once today's readings are in
      // — a second `find()` against the exact same rows was redundant.
      const alerts = await this.repository.find({
        where: { attractionId: In(polledAttractionIds) },
      });
      if (alerts.length === 0) return;
      const watchedIds = [...new Set(alerts.map((a) => a.attractionId))];

      const attractions = await this.attractionRepository.find({
        where: { id: In(watchedIds) },
      });
      const attractionById = new Map(attractions.map((a) => [a.id, a]));

      const statusByAttraction =
        await this.queueDataService.findCurrentStatusByAttractionIds(
          watchedIds,
          MAX_READING_AGE_MINUTES,
        );

      const readings = RideAlertsService.buildEligibleReadings(
        watchedIds,
        attractionById,
        statusByAttraction,
        nowMs,
      );
      if (readings.length === 0) return;

      // A ride CLOSED overnight produces no reading, so `diffRideAlerts`
      // never sees it and never re-arms it — an alert that fired yesterday
      // evening would otherwise stay disarmed forever on a day the ride
      // simply never again reaches the threshold (a walk-on all day reads
      // as "still below, already fired" to a pure same-day diff). Crossing
      // a park-local day boundary re-arms on its own, independent of
      // today's reading — the same way any other daily state here resets.
      if (park.timezone) {
        const startOfToday = getStartOfDayInTimezoneAt(nowMs, park.timezone);
        const staleDisarmed = alerts.filter(
          (a) =>
            !a.armed && a.lastTriggeredAt && a.lastTriggeredAt < startOfToday,
        );
        if (staleDisarmed.length > 0) {
          await this.repository.update(
            { id: In(staleDisarmed.map((a) => a.id)) },
            { armed: true },
          );
          for (const alert of staleDisarmed) alert.armed = true;
        }
      }

      const { triggers, armedUpdates } = diffRideAlerts(readings, alerts);

      // Re-arms (wait recovered above threshold) send nothing and race
      // nothing — one bulk update instead of one per alert.
      const reArmIds = armedUpdates
        .filter((update) => update.armed)
        .map((update) => update.id);
      if (reArmIds.length > 0) {
        await this.repository.update({ id: In(reArmIds) }, { armed: true });
      }
      if (triggers.length === 0) return;

      const subscriptions = await this.pushService.findByIds(
        triggers.map((t) => t.subscriptionId),
      );

      // Sent in small batches, never all at once and never fully
      // sequential: a ride with hundreds of alerts must not turn into
      // hundreds of concurrent outbound HTTPS calls, nor stall every park
      // behind this one in the poll's batch for minutes.
      for (
        let i = 0;
        i < triggers.length;
        i += RideAlertsService.SEND_BATCH_SIZE
      ) {
        const batch = triggers.slice(i, i + RideAlertsService.SEND_BATCH_SIZE);
        await Promise.allSettled(
          batch.map((trigger) =>
            this.sendTrigger(trigger, subscriptions, attractionById, park),
          ),
        );
      }
    } catch (error) {
      this.logger.warn(
        `Ride-alert check failed for park ${park.name}: ${(error as Error)?.message ?? error}`,
      );
    }
  }

  /**
   * Which of this cycle's watched attractions have a reading worth diffing
   * against an alert — extracted out of `checkAndNotify` as its own step,
   * pure given what that method already fetched.
   *
   * Three reasons a watched attraction contributes nothing: out of season
   * (confirmed `false`, never merely unknown — see the `!== false` comment
   * this mirrors below), no fresh OPERATING STANDBY row at all, or one that
   * is a carried-forward heartbeat rather than an observation (alerting on
   * one would fire on a ride nothing has actually reported for as long as
   * `writeHourlyHeartbeats` has been filling the gap, up to 24h).
   */
  private static buildEligibleReadings(
    watchedIds: string[],
    attractionById: Map<string, Attraction>,
    statusByAttraction: Map<string, QueueData[]>,
    nowMs: number,
  ): RideReading[] {
    const readings: RideReading[] = [];
    for (const id of watchedIds) {
      const attraction = attractionById.get(id);
      if (!attraction) continue;
      // `!== false`, never `=== true` — see season.ts's frontend twin and
      // the seasonal-attractions REQUIREMENT this mirrors: `null` means
      // "seasonal, nothing else known" and must not hide a ride we simply
      // have not understood yet.
      if (
        isCurrentlyInSeason(
          resolveCuratedFacts(attraction),
          new Date(nowMs),
        ) === false
      ) {
        continue;
      }

      const standby = (statusByAttraction.get(id) ?? []).find(
        (row) => row.queueType === QueueType.STANDBY,
      );
      if (!standby || standby.status !== LiveStatus.OPERATING) continue;
      if (standby.isHeartbeat) continue;
      if (standby.waitTime === null || standby.waitTime === undefined) {
        continue;
      }
      readings.push({ attractionId: id, waitTime: standby.waitTime });
    }
    return readings;
  }

  /**
   * Disarms one trigger and sends it — in that order, and the disarm IS the
   * concurrency guard. `armed: true` in the WHERE clause makes this a
   * compare-and-swap: only the caller that actually flips the row true→false
   * gets `affected === 1` and proceeds to send. A second, overlapping sweep
   * (a reclaimed stalled Bull job re-running the same cycle) racing this one
   * finds `affected === 0` and returns — the same crossing can never notify
   * twice. Never throws; a failure here must not stop the rest of the batch.
   */
  private async sendTrigger(
    trigger: AlertTrigger,
    subscriptions: Map<string, PushSubscription>,
    attractionById: Map<string, Attraction>,
    park: ParkForRideAlertCheck,
  ): Promise<void> {
    try {
      const result = await this.repository.update(
        { id: trigger.alertId, armed: true },
        { armed: false, lastTriggeredAt: new Date() },
      );
      if (!result.affected) return;

      const subscription = subscriptions.get(trigger.subscriptionId);
      if (!subscription) return;
      const attraction = attractionById.get(trigger.attractionId);
      if (!attraction) return;

      const message = writeRideAlertMessage(
        {
          dedupeKey: `ride-alert:${trigger.alertId}`,
          attractionName: resolveCuratedFacts(attraction).name,
          parkName: park.name,
          waitTime: trigger.waitTime,
          url: frontendAttractionPath(park, { slug: attraction.slug }) ?? "/",
        },
        subscription.locale,
      );
      const sent = await this.pushService.send(subscription, message);
      if (!sent) {
        // Delivery failed (push service having a bad minute, or a dead
        // subscription `PushService` is about to drop) — re-arm so the
        // next qualifying reading gets another try instead of leaving the
        // alert silently, permanently disarmed with nothing to explain it.
        await this.repository.update(trigger.alertId, { armed: true });
      }
    } catch (error) {
      this.logger.warn(
        `Ride-alert send failed for alert ${trigger.alertId}: ${(error as Error)?.message ?? error}`,
      );
    }
  }
}

import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, IsNull, Repository } from "typeorm";
import { RideAlert } from "./entities/ride-alert.entity";
import { diffRideAlerts, type RideReading } from "./ride-alert-transitions";
import { Attraction } from "../attractions/entities/attraction.entity";
import { Park } from "../parks/entities/park.entity";
import { PushService } from "../push/push.service";
import { writeRideAlertMessage } from "../push/push-messages";
import { QueueDataService } from "../queue-data/queue-data.service";
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

/** How many alerts one browser may hold — a person watches a handful of rides. */
export const MAX_RIDE_ALERTS_PER_SUBSCRIPTION = 100;

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
}

@Injectable()
export class RideAlertsService {
  private readonly logger = new Logger(RideAlertsService.name);

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
   * Upsert on `(subscriptionId, attractionId)`. Always re-arms — a changed
   * threshold, or simply asking again, is a fresh request to be told, and the
   * alternative (leaving `armed: false` because a stale trigger happened to
   * still be in that state) would silently swallow the request.
   */
  async upsert(
    subscriptionId: string,
    attractionId: string,
    thresholdMinutes: number,
  ): Promise<RideAlert> {
    const existing = await this.repository.findOne({
      where: { subscriptionId, attractionId },
    });
    const row =
      existing ?? this.repository.create({ subscriptionId, attractionId });
    row.thresholdMinutes = thresholdMinutes;
    row.armed = true;
    return this.repository.save(row);
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
   * Best-effort like `ParkStatusRevalidationService` — logs and returns
   * rather than throwing, so a bug here can never fail the wait-times cycle
   * it rides on.
   */
  async checkAndNotify(
    park: ParkForRideAlertCheck,
    polledAttractionIds: string[],
  ): Promise<void> {
    try {
      if (polledAttractionIds.length === 0) return;

      // Permanent, not a freshness check — a park with no real wait-time feed
      // (Hansa-Park) must never be read as "every ride shows 0 minutes".
      if (getNoLiveWaitTimesReason(park.citySlug, park.slug)) return;

      // Cheap and almost always empty: most rides nobody has an alert on.
      const watchedIds =
        await this.attractionIdsWithAlerts(polledAttractionIds);
      if (watchedIds.length === 0) return;

      const attractions = await this.attractionRepository.find({
        where: { id: In(watchedIds) },
      });
      const attractionById = new Map(attractions.map((a) => [a.id, a]));

      const statusByAttraction =
        await this.queueDataService.findCurrentStatusByAttractionIds(
          watchedIds,
        );

      const readings: RideReading[] = [];
      for (const id of watchedIds) {
        const attraction = attractionById.get(id);
        if (!attraction) continue;
        // `!== false`, never `=== true` — see season.ts's frontend twin and
        // the seasonal-attractions REQUIREMENT this mirrors: `null` means
        // "seasonal, nothing else known" and must not hide a ride we simply
        // have not understood yet.
        if (isCurrentlyInSeason(resolveCuratedFacts(attraction)) === false) {
          continue;
        }

        const standby = (statusByAttraction.get(id) ?? []).find(
          (row) => row.queueType === QueueType.STANDBY,
        );
        if (!standby || standby.status !== LiveStatus.OPERATING) continue;
        if (standby.waitTime === null || standby.waitTime === undefined) {
          continue;
        }
        readings.push({ attractionId: id, waitTime: standby.waitTime });
      }
      if (readings.length === 0) return;

      const alerts = await this.repository.find({
        where: { attractionId: In(watchedIds) },
      });
      const { triggers, armedUpdates } = diffRideAlerts(readings, alerts);

      for (const update of armedUpdates) {
        await this.repository.update(update.id, {
          armed: update.armed,
          ...(update.armed ? {} : { lastTriggeredAt: new Date() }),
        });
      }
      if (triggers.length === 0) return;

      const subscriptions = await this.pushService.findByIds(
        triggers.map((t) => t.subscriptionId),
      );
      for (const trigger of triggers) {
        const subscription = subscriptions.get(trigger.subscriptionId);
        if (!subscription) continue;
        const attraction = attractionById.get(trigger.attractionId);
        if (!attraction) continue;

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
        await this.pushService.send(subscription, message);
      }
    } catch (error) {
      this.logger.warn(
        `Ride-alert check failed for park ${park.name}: ${(error as Error)?.message ?? error}`,
      );
    }
  }

  /** Attraction ids from `polledAttractionIds` that have at least one alert. */
  private async attractionIdsWithAlerts(
    polledAttractionIds: string[],
  ): Promise<string[]> {
    const rows = await this.repository.find({
      where: { attractionId: In(polledAttractionIds) },
      select: { attractionId: true },
    });
    return [...new Set(rows.map((r) => r.attractionId))];
  }
}

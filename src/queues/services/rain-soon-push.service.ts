import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { formatInTimeZone } from "date-fns-tz";
import { Redis } from "ioredis";
import { IsNull, Repository } from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../../attractions/utils/curated-attraction-facts.util";
import { frontendParkPath } from "../../common/utils/frontend-url.util";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  LiveStatus,
  QueueType,
} from "../../external-apis/themeparks/themeparks.types";
import { Park } from "../../parks/entities/park.entity";
import { getNoLiveWaitTimesReason } from "../../parks/data/live-wait-time-sources";
import { ParksService } from "../../parks/parks.service";
import { WeatherService } from "../../parks/weather.service";
import { parksPlannedToday } from "../../push/notification-planner";
import { writeRainSoonMessage } from "../../push/push-messages";
import { isWithinQuietHours } from "../../push/quiet-hours";
import { PushSubscription } from "../../push/entities/push-subscription.entity";
import { PushService } from "../../push/push.service";
import {
  coveredOfferReady,
  decideRainSoon,
  isCovered,
  rankCoveredRides,
  type CoveredCandidate,
  type CoveredRideLine,
  type RainPhase,
} from "../../push/rain-soon";
import { QueueDataService } from "../../queue-data/queue-data.service";
import { RideAlertsService } from "../../ride-alerts/ride-alerts.service";
import { TripsService } from "../../trips/trips.service";

/**
 * The `rain-soon` producer: warns subscribers that rain is about to reach a
 * park they are visiting, and names the open covered rides when the park knows
 * enough to.
 *
 * **Who counts as visiting.** A subscriber with the topic who either has a plan
 * with at least one block on the park's own today, or a ride alert on an
 * attraction of that park. A plan is the strongest signal this API has, and a
 * ride alert is the other one: the alert's armed state is re-set each park day,
 * so it names the park someone is watching right now. Anything weaker, such as
 * a favourite, would warn people who are not going, and a weather push they did
 * not expect is the kind that gets the switch turned off.
 *
 * **One push per front.** The state is per PARK, not per subscriber, and lives in
 * Redis (`decideRainSoon` holds the transitions): warned, then raining, then
 * re-armed once the rain has stopped. The first write is an atomic `SET NX`
 * before anything is sent, so two overlapping ticks cannot both win. A Redis
 * error skips the park: unlike the other topics a rain warning repeats every
 * tick for as long as the rain is ahead, so a duplicate here is not a one-off.
 */
@Injectable()
export class RainSoonPushService {
  private readonly logger = new Logger(RainSoonPushService.name);

  /** A genuine OPERATING reading no older than this, as `RideAlertsService` requires. */
  private static readonly MAX_READING_AGE_MINUTES = 15;
  /** How long a warning suppresses the next one when the rain never arrives. */
  private static readonly WARNED_TTL_SECONDS = 3 * 60 * 60;
  /** How long "it is raining" is remembered; a longer shower re-marks itself. */
  private static readonly RAINING_TTL_SECONDS = 6 * 60 * 60;
  /** Same bound as the other halves: no hundreds of concurrent HTTPS calls. */
  private static readonly SEND_BATCH_SIZE = 20;

  constructor(
    private readonly pushService: PushService,
    private readonly tripsService: TripsService,
    private readonly rideAlertsService: RideAlertsService,
    private readonly parksService: ParksService,
    private readonly weatherService: WeatherService,
    private readonly queueDataService: QueueDataService,
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  /** One tick. Returns how many notifications reached a push service. */
  async run(nowMs: number): Promise<number> {
    const subscriptions =
      await this.pushService.subscriptionsWithTopic("rain-soon");
    if (subscriptions.length === 0) return 0;

    const byPark = await this.subscribersByPark(subscriptions, nowMs);
    let sent = 0;
    for (const { park, subscribers } of byPark.values()) {
      try {
        sent += await this.handlePark(park, subscribers, nowMs);
      } catch (error) {
        // One park's failure must not silence the others.
        this.logger.warn(
          `rain-soon failed for ${park.name}: ${(error as Error)?.message ?? error}`,
        );
      }
    }
    return sent;
  }

  /**
   * Subscribers grouped by the park they are visiting.
   *
   * A trip is read once however many browsers share it. A park in a plan is
   * resolved through its geographic path, because a bare slug repeats across
   * destinations.
   */
  private async subscribersByPark(
    subscriptions: PushSubscription[],
    nowMs: number,
  ): Promise<Map<string, { park: Park; subscribers: PushSubscription[] }>> {
    const byPark = new Map<
      string,
      { park: Park; subscribers: Map<string, PushSubscription> }
    >();
    const parkById = new Map<string, Park | null>();
    const add = (park: Park, subscription: PushSubscription) => {
      const entry = byPark.get(park.id) ?? {
        park,
        subscribers: new Map<string, PushSubscription>(),
      };
      entry.subscribers.set(subscription.id, subscription);
      byPark.set(park.id, entry);
    };

    const byTrip = new Map<string, PushSubscription[]>();
    for (const subscription of subscriptions) {
      if (!subscription.tripId) continue;
      const list = byTrip.get(subscription.tripId) ?? [];
      list.push(subscription);
      byTrip.set(subscription.tripId, list);
    }
    for (const [tripId, tripSubscriptions] of byTrip) {
      const trip = await this.tripsService.find(tripId);
      if (!trip) continue;
      for (const planned of parksPlannedToday(trip.payload, nowMs)) {
        const park = await this.parksService.findByGeographicPath(
          planned.geo.continent,
          planned.geo.country,
          planned.geo.city,
          planned.slug,
        );
        if (!park) continue;
        parkById.set(park.id, park);
        for (const subscription of tripSubscriptions) add(park, subscription);
      }
    }

    const alertParks = await this.rideAlertsService.parkIdsBySubscription(
      subscriptions.map((s) => s.id),
    );
    const subscriptionById = new Map(subscriptions.map((s) => [s.id, s]));
    for (const [subscriptionId, parkIds] of alertParks) {
      const subscription = subscriptionById.get(subscriptionId);
      if (!subscription) continue;
      for (const parkId of parkIds) {
        if (!parkById.has(parkId)) {
          parkById.set(parkId, await this.parksService.findById(parkId));
        }
        const park = parkById.get(parkId);
        if (park) add(park, subscription);
      }
    }

    return new Map(
      [...byPark].map(([id, { park, subscribers }]) => [
        id,
        { park, subscribers: [...subscribers.values()] },
      ]),
    );
  }

  private async handlePark(
    park: Park,
    subscribers: PushSubscription[],
    nowMs: number,
  ): Promise<number> {
    // No zone, no notification: the time in the message is the park's clock.
    if (!park.timezone) return 0;

    const nowcast = await this.weatherService.getNowcast(park.id);
    if (!nowcast) return 0;

    const stateKey = `push:rain-soon:${park.id}`;
    let phase: RainPhase | null;
    try {
      phase = (await this.redis.get(stateKey)) as RainPhase | null;
    } catch (error) {
      this.logger.warn(
        `rain-soon state unreadable: ${(error as Error)?.message ?? error}`,
      );
      return 0;
    }

    const decision = decideRainSoon(nowcast, phase, nowMs);
    try {
      if (decision.action === "rearm") {
        await this.redis.del(stateKey);
        return 0;
      }
      if (decision.action === "mark-raining") {
        await this.redis.set(
          stateKey,
          "raining",
          "EX",
          RainSoonPushService.RAINING_TTL_SECONDS,
        );
        return 0;
      }
      if (decision.action !== "send") return 0;

      const claimed = await this.redis.set(
        stateKey,
        "warned",
        "EX",
        RainSoonPushService.WARNED_TTL_SECONDS,
        "NX",
      );
      if (claimed !== "OK") return 0;
    } catch (error) {
      this.logger.warn(
        `rain-soon state unwritable: ${(error as Error)?.message ?? error}`,
      );
      return 0;
    }

    const startsAt = new Date(decision.startsAtMs);
    const atTime = formatInTimeZone(startsAt, park.timezone, "HH:mm");
    const localDate = formatInTimeZone(
      new Date(nowMs),
      park.timezone,
      "yyyy-MM-dd",
    );
    const rides = await this.coveredRides(park, nowMs);
    const copy = {
      dedupeKey: `rain-soon:${park.id}:${localDate}:${decision.startsAtMs}`,
      parkName: park.name,
      atTime,
      rides,
      url: frontendParkPath(park) ?? "/",
    };

    const recipients = subscribers.filter(
      (s) => !isWithinQuietHours(s.timezone, nowMs),
    );
    let sent = 0;
    for (
      let i = 0;
      i < recipients.length;
      i += RainSoonPushService.SEND_BATCH_SIZE
    ) {
      const batch = recipients.slice(
        i,
        i + RainSoonPushService.SEND_BATCH_SIZE,
      );
      const results = await Promise.allSettled(
        batch.map((s) =>
          this.pushService.send(s, writeRainSoonMessage(copy, s.locale)),
        ),
      );
      sent += results.filter((r) => r.status === "fulfilled" && r.value).length;
    }
    this.logger.log(
      `rain-soon ${park.name}: rain from ${atTime}, ${rides.length} covered ride(s) named, ${sent}/${subscribers.length} sent (${subscribers.length - recipients.length} quiet)`,
    );
    return sent;
  }

  /**
   * The open covered rides to name, or none.
   *
   * A park with no readable wait-time source names none: its rides read
   * `UNKNOWN`, and "open" would be a guess. The gate comes first and is cheap;
   * the live readings are only fetched for the rides that pass it.
   */
  private async coveredRides(
    park: Park,
    nowMs: number,
  ): Promise<CoveredRideLine[]> {
    if (getNoLiveWaitTimesReason(park.citySlug, park.slug)) return [];

    const attractions = await this.attractionRepository.find({
      where: { parkId: park.id, retiredAt: IsNull() },
    });
    const gateRows = attractions.map((attraction) => ({
      attraction,
      facts: resolveCuratedFacts(attraction),
      indoorOutdoor: attraction.indoorOutdoor,
      isCurrentlyInSeason: isCurrentlyInSeason(
        resolveCuratedFacts(attraction),
        new Date(nowMs),
      ),
    }));
    if (!coveredOfferReady(gateRows)) return [];

    const coveredRows = gateRows.filter((row) => isCovered(row));
    const readings =
      await this.queueDataService.findCurrentStatusByAttractionIds(
        coveredRows.map((row) => row.attraction.id),
        RainSoonPushService.MAX_READING_AGE_MINUTES,
      );
    const candidates: CoveredCandidate[] = coveredRows.map((row) => {
      const standby = (readings.get(row.attraction.id) ?? []).find(
        (r) => r.queueType === QueueType.STANDBY,
      );
      // A heartbeat carries the last status forward and is no reading; only a
      // real OPERATING row inside the window says the ride is open now.
      const operating =
        !!standby &&
        standby.status === LiveStatus.OPERATING &&
        !standby.isHeartbeat;
      return {
        id: row.attraction.id,
        name: row.facts.name,
        indoorOutdoor: row.indoorOutdoor,
        isCurrentlyInSeason: row.isCurrentlyInSeason,
        operating,
        waitTime: operating ? (standby?.waitTime ?? null) : null,
      };
    });
    return rankCoveredRides(candidates);
  }
}

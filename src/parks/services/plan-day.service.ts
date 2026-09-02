import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { MLService } from "../../ml/ml.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { CalendarService } from "./calendar.service";
import { composeDayCurve } from "../../common/utils/day-shape.util";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import { formatInParkTimezone } from "../../common/utils/date.util";
import { formatInTimeZone } from "date-fns-tz";
import { PlanDayDto, PlanDayRideDto, PlanDayTier } from "../dto/plan-day.dto";

/**
 * One day, ride by ride, hour by hour — the series a trip planner draws.
 *
 * Nothing upstream answers "what will Taron's queue be at 14:00 on 17 October".
 * The model generates hourly predictions 24 hours ahead
 * (`HOURLY_PREDICTIONS = 24` in the python service) and day-level predictions
 * out to 60 days. So there are two regimes, and which one produced a number
 * travels with it as `tier`:
 *
 * - **measured** — today and tomorrow, straight from the stored hourly
 *   predictions. The model's own answer at its own resolution.
 * - **composed** — beyond that, a day-level prediction scaled by the ride's
 *   historical hour shape (see `composeDayCurve`). The level is predicted, the
 *   shape is historical.
 * - **long_range** — the same composition past the stored 60-day daily horizon,
 *   where the day level is thinner and says so.
 *
 * The alternative to composing was returning nothing past tomorrow, and a
 * planner that goes blank in March for a July trip is not a planner. The
 * alternative to labelling it was letting a composed number look exactly like a
 * measured one, which is the failure this whole design is arranged against.
 */
@Injectable()
export class PlanDayService {
  private readonly logger = new Logger(PlanDayService.name);

  /** Where the stored daily predictions stop (`MLService.deduplicatePredictions`). */
  private static readonly DAILY_HORIZON_DAYS = 60;

  /** Where the python service's hourly generation stops. */
  private static readonly HOURLY_HORIZON_DAYS = 1;

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    private readonly mlService: MLService,
    private readonly calendarService: CalendarService,
    private readonly historicalStatsService: ParkHistoricalStatsService,
  ) {}

  async buildPlanDay(park: Park, dateStr: string): Promise<PlanDayDto> {
    const today = formatInParkTimezone(new Date(), park.timezone);
    const leadDays = this.daysBetween(today, dateStr);

    // The day's own facts: opening hours, crowd level, weather, holiday flags.
    // One day rather than a month — the caller asked about one day, and the
    // calendar's month cache would answer with 92 KB to serve 1 KB of it.
    const day = await this.loadDay(park, dateStr);

    const openHour = this.hourIn(day?.hours?.openingTime, park.timezone);
    const closeHour = this.hourIn(day?.hours?.closingTime, park.timezone);

    const tier = this.tierFor(leadDays);

    const context = {
      date: dateStr,
      status: day?.status ?? "UNKNOWN",
      openHour,
      closeHour,
      crowdLevel: day?.crowdLevel ?? null,
      // No climate normal substituted past the forecast's reach. A made-up rain
      // probability would silently move every bar on the day, and the caller
      // cannot tell an invented one from a real one.
      weather: (day?.weather as Record<string, unknown> | undefined) ?? null,
      isHoliday: Boolean(day?.isHoliday),
      isBridgeDay: Boolean(day?.isBridgeDay),
      isSchoolVacation: Boolean(day?.isSchoolVacation),
      // Derived here rather than read: the calendar carries no isWeekend, and
      // every surface that wanted one was deriving it from the date separately.
      isWeekend: this.isWeekend(dateStr),
      neighborHolidays: (day?.neighborHolidays ?? []) as unknown as Array<
        Record<string, unknown>
      >,
    };

    const base: PlanDayDto = {
      parkSlug: park.slug,
      timezone: park.timezone,
      context,
      tier,
      leadDays,
      leadTimeMae: null,
      rides: [],
      // Empty, and deliberately so for now. The calendar DTO has no showtimes
      // field at all — `IntegratedCalendarDayDto` never declared one, which is
      // why every calendar response looks like a park with no shows rather
      // than like a field that was not asked for. Wiring ShowsService in here
      // is its own change: showtimes are only known for a narrow window, so
      // this has to return "not known this far out" rather than "no shows",
      // and those are different answers.
      shows: [],
    };

    // A closed day has no curves to draw, and saying so is the answer.
    if (openHour === null || closeHour === null || closeHour < openHour) {
      return base;
    }

    base.rides = await this.buildRides(
      park,
      dateStr,
      tier,
      openHour,
      closeHour,
    );
    return base;
  }

  /**
   * Rides for the day. `measured` reads the stored hourly predictions and
   * collapses each hour's 15-minute slots; the other tiers compose a curve from
   * the day level and the historical shape.
   */
  private async buildRides(
    park: Park,
    dateStr: string,
    tier: PlanDayTier,
    openHour: number,
    closeHour: number,
  ): Promise<PlanDayRideDto[]> {
    const attractions = await this.attractionRepository.find({
      where: { parkId: park.id, retiredAt: IsNull() },
      select: ["id", "slug", "name", "landName", "latitude", "longitude"],
    });
    if (attractions.length === 0) return [];

    const bySlug = new Map(attractions.map((a) => [a.slug, a]));
    const byId = new Map(attractions.map((a) => [a.id, a]));
    const downIds = await this.downYesterday(park, dateStr);

    // The historical shape. topN is the endpoint's own cap rather than the
    // caller's: a planner wants every ride it can get, and this payload is a
    // few hundred bytes per ride.
    const profile = await this.historicalStatsService
      .getParkHourlyProfile(park, 1, 20, 20)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: hourly profile unavailable for ${park.slug}: ${err.message}`,
        );
        return null;
      });

    if (tier === "measured") {
      const measured = await this.measuredRides(
        park,
        dateStr,
        openHour,
        closeHour,
        byId,
        profile,
        downIds,
      );
      if (measured.length > 0) return measured;
      // Fall through: the hourly rows can be missing for a park the generator
      // skipped tonight, and a composed curve beats an empty day.
    }

    if (!profile || profile.attractions.length === 0) return [];

    const dayLevels = await this.dayLevels(park, dateStr);
    if (dayLevels.size === 0) return [];

    const rides: PlanDayRideDto[] = [];
    for (const shape of profile.attractions) {
      const attraction = bySlug.get(shape.attractionSlug);
      if (!attraction) continue;

      const level = dayLevels.get(attraction.id);
      if (!level) continue;

      const curve = composeDayCurve({
        shapeHours: profile.hours,
        shapeP50: shape.p50,
        dayPeak: level.predictedWaitTime,
        openHour,
        closeHour,
      });
      // No measured shape — omitted rather than drawn flat, which would assert
      // the queue is the same all day.
      if (!curve) continue;

      rides.push({
        attractionSlug: shape.attractionSlug,
        attractionName: shape.attractionName,
        land: shape.land ?? attraction.landName ?? null,
        hours: curve,
        dayPeak: level.predictedWaitTime,
        uncertaintyMinutes: level.uncertaintyMinutes ?? null,
        sampleDays: shape.sampleDays,
        latitude: PlanDayService.coord(attraction.latitude),
        longitude: PlanDayService.coord(attraction.longitude),
        ...(downIds.has(attraction.id) ? { downYesterday: true } : {}),
      });
    }

    return rides;
  }

  /**
   * Today and tomorrow, from the model's own hourly predictions.
   *
   * The stored rows are 15-minute slots, so each hour is the MEAN of its slots:
   * they are already point estimates from the median quantile, and taking the
   * maximum instead would quietly turn an honest hour into a pessimistic one.
   */
  private async measuredRides(
    park: Park,
    dateStr: string,
    openHour: number,
    closeHour: number,
    byId: Map<string, Attraction>,
    profile: Awaited<
      ReturnType<ParkHistoricalStatsService["getParkHourlyProfile"]>
    > | null,
    downIds: ReadonlySet<string>,
  ): Promise<PlanDayRideDto[]> {
    const stored = await this.mlService
      .getParkPredictions(park.id, "hourly")
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: hourly predictions unavailable for ${park.slug}: ${err.message}`,
        );
        return { predictions: [] as PredictionDto[] };
      });

    const sampleDaysBySlug = new Map(
      (profile?.attractions ?? []).map((a) => [a.attractionSlug, a.sampleDays]),
    );

    // attraction → hour → slot values
    const byRide = new Map<string, Map<number, number[]>>();
    const bandByRide = new Map<string, number>();

    for (const p of stored.predictions) {
      if (p.predictionType !== "hourly") continue;
      const when = new Date(p.predictedTime);
      if (formatInParkTimezone(when, park.timezone) !== dateStr) continue;

      const hour = Number(formatInTimeZone(when, park.timezone, "HH"));
      if (hour < openHour || hour > closeHour) continue;

      const hours = byRide.get(p.attractionId) ?? new Map<number, number[]>();
      const slots = hours.get(hour) ?? [];
      slots.push(p.predictedWaitTime);
      hours.set(hour, slots);
      byRide.set(p.attractionId, hours);

      if (p.uncertaintyMinutes != null) {
        // The widest band the day carries for this ride: the planner draws one
        // channel per ride, and understating it is the failure that matters.
        bandByRide.set(
          p.attractionId,
          Math.max(bandByRide.get(p.attractionId) ?? 0, p.uncertaintyMinutes),
        );
      }
    }

    const rides: PlanDayRideDto[] = [];
    for (const [attractionId, hourMap] of byRide) {
      const attraction = byId.get(attractionId);
      if (!attraction) continue;

      const hours = [];
      for (let h = openHour; h <= closeHour; h++) {
        const slots = hourMap.get(h);
        if (!slots || slots.length === 0) continue;
        const mean = slots.reduce((a, b) => a + b, 0) / slots.length;
        hours.push({ hour: h, wait: roundToNearest5Minutes(mean) });
      }
      if (hours.length === 0) continue;

      rides.push({
        attractionSlug: attraction.slug,
        attractionName: attraction.name,
        land: attraction.landName ?? null,
        hours,
        dayPeak: Math.max(...hours.map((h) => h.wait)),
        uncertaintyMinutes: bandByRide.get(attractionId) ?? null,
        sampleDays: sampleDaysBySlug.get(attraction.slug) ?? 0,
        latitude: PlanDayService.coord(attraction.latitude),
        longitude: PlanDayService.coord(attraction.longitude),
        ...(downIds.has(attractionId) ? { downYesterday: true } : {}),
      });
    }

    return rides;
  }

  /**
   * A coordinate as a NUMBER.
   *
   * TypeORM hands back a `decimal` column as a string, and the entity's
   * `latitude: number` does not change that — which is why the park payload has
   * always shipped `"50.7992616"` with quotes around it. A planner measures
   * distances with these, so this endpoint sends numbers and the conversion
   * happens once, here, rather than in every consumer that forgets.
   *
   * `null` for anything that is not a finite number: an unplaced ride has no
   * coordinate, and NaN in a distance is worse than an absent one — it
   * propagates silently through every sum after it, and `NaN < 5` is false, so
   * a broken leg would read as a comfortable one.
   *
   * The blank check is not defensive noise. `Number("")` is **0**, not NaN, so
   * an empty column would arrive as a perfectly finite 0.0 — a coordinate in
   * the Gulf of Guinea, which is a plausible-looking answer and therefore worse
   * than no answer. Same for a whitespace-only value.
   */
  private static coord(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === "string" && value.trim() === "") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  /**
   * Rides that were down for all of the previous operating day.
   *
   * "Down" and "unobserved" are different answers and this only reports the
   * first: a ride qualifies when it was seen at least once yesterday and was
   * NEVER `OPERATING` in any of those readings. A ride with no rows at all is
   * silence — a feed that stopped, a park that was shut — and warning about it
   * would be this service asserting something it did not observe.
   *
   * Yesterday is the PARK's yesterday, not UTC's. `queue_data` carries a partial
   * index on `status = 'DOWN'` written for exactly this question, so the scan is
   * bounded to one park's rows for one day.
   *
   * Only asked for today and tomorrow: past that, whether a ride broke yesterday
   * says nothing a visitor can act on, and the query is not worth its cost.
   */
  private async downYesterday(
    park: Park,
    dateStr: string,
  ): Promise<Set<string>> {
    const out = new Set<string>();

    const today = formatInTimeZone(new Date(), park.timezone, "yyyy-MM-dd");
    const tomorrow = formatInTimeZone(
      new Date(Date.now() + 86_400_000),
      park.timezone,
      "yyyy-MM-dd",
    );
    if (dateStr !== today && dateStr !== tomorrow) return out;

    try {
      const rows: Array<{ attractionId: string }> =
        await this.attractionRepository.manager.query(
          `SELECT qd."attractionId" AS "attractionId"
             FROM queue_data qd
             JOIN attractions a ON a.id::text = qd."attractionId"
            WHERE a."parkId" = $1
              AND (qd.timestamp AT TIME ZONE $2)::date = ($3::date - INTERVAL '1 day')
            GROUP BY qd."attractionId"
           HAVING COUNT(*) FILTER (WHERE qd.status = 'OPERATING') = 0`,
          [park.id, park.timezone, dateStr],
        );
      for (const row of rows) out.add(row.attractionId);
    } catch (error) {
      // A warning is a nicety; the plan is not. A failure here must not take the
      // day's forecast with it.
      this.logger.warn(
        `Plan day: downYesterday unavailable for ${park.slug}: ${(error as Error).message}`,
      );
    }

    return out;
  }

  /** Day-level prediction per attraction for one date. */
  private async dayLevels(
    park: Park,
    dateStr: string,
  ): Promise<Map<string, PredictionDto>> {
    const out = new Map<string, PredictionDto>();
    const serving = await this.mlService
      .getServingDailyPredictions(park.id)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: daily predictions unavailable for ${park.slug}: ${err.message}`,
        );
        return { predictions: [] as PredictionDto[] };
      });

    for (const p of serving.predictions) {
      if (p.predictedTime.slice(0, 10) !== dateStr) continue;
      // Freshest wins where a park has more than one row for the day.
      out.set(p.attractionId, p);
    }
    return out;
  }

  private async loadDay(park: Park, dateStr: string) {
    const at = new Date(`${dateStr}T12:00:00Z`);
    const response = await this.calendarService
      .buildCalendarResponse(park, at, at, "none")
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: calendar unavailable for ${park.slug} on ${dateStr}: ${err.message}`,
        );
        return null;
      });
    return response?.days?.find((d) => d.date === dateStr) ?? null;
  }

  private tierFor(leadDays: number): PlanDayTier {
    if (leadDays <= PlanDayService.HOURLY_HORIZON_DAYS) return "measured";
    if (leadDays <= PlanDayService.DAILY_HORIZON_DAYS) return "composed";
    return "long_range";
  }

  /** Park-local hour of an instant, or null when there is no instant. */
  private hourIn(
    value: Date | string | undefined,
    timezone: string,
  ): number | null {
    if (!value) return null;
    const at = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(at.getTime())) return null;
    return Number(formatInTimeZone(at, timezone, "HH"));
  }

  /** Whole days between two YYYY-MM-DD strings. */
  private daysBetween(from: string, to: string): number {
    const a = Date.parse(`${from}T00:00:00Z`);
    const b = Date.parse(`${to}T00:00:00Z`);
    return Math.round((b - a) / 86_400_000);
  }

  private isWeekend(dateStr: string): boolean {
    const day = new Date(`${dateStr}T00:00:00Z`).getUTCDay();
    return day === 0 || day === 6;
  }
}

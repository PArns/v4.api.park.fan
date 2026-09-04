import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { MLService } from "../../ml/ml.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { AttractionHourlyHistory } from "../../analytics/entities/attraction-hourly-history.entity";
import { CalendarService } from "./calendar.service";
import { composeDayCurve } from "../../common/utils/day-shape.util";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import { formatInParkTimezone } from "../../common/utils/date.util";
import { formatInTimeZone } from "date-fns-tz";
import { PlanDayDto, PlanDayRideDto, PlanDayTier } from "../dto/plan-day.dto";
import { resolveCuratedFacts } from "../../attractions/utils/curated-attraction-facts.util";

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
    private readonly analyticsService: AnalyticsService,
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
      // The four height/wet columns are here because the two the payload
      // states are RESOLVED from them (`riderFacts`): leave them out of the
      // select and the resolver reads `undefined` on every row, so both fields
      // are silently absent everywhere and nothing fails.
      select: [
        "id",
        "slug",
        "name",
        "landName",
        "latitude",
        "longitude",
        "minimumHeight",
        "curatedMinimumHeight",
        "mayGetWet",
        "curatedMayGetWet",
      ],
    });
    if (attractions.length === 0) return [];

    const bySlug = new Map(attractions.map((a) => [a.slug, a]));
    const byId = new Map(attractions.map((a) => [a.id, a]));
    // Both are per-park sets keyed by attraction id, and neither is worth
    // serialising behind the other. The headliner set is the park's CURATED
    // answer — never re-derived from `dayPeak`, because a headliner having a
    // quiet Tuesday is still a headliner, and a planner that pointed at the
    // day's tallest bars instead would recommend whatever happens to be busy.
    const [downIds, headlinerIds] = await Promise.all([
      this.downYesterday(park, dateStr),
      this.analyticsService
        .getHeadlinerAttractionIds(park.id)
        .catch((err: Error) => {
          this.logger.warn(
            `Plan day: headliners unavailable for ${park.slug}: ${err.message}`,
          );
          return new Set<string>();
        }),
    ]);

    // Before the historical shape is fetched, because an observed day does not
    // want one: it is answered from what happened, and `getParkHourlyProfile`
    // is a year of aggregates over every ride in the park. Composing a shape
    // onto a past date would draw a forecast for a day the visitor walked.
    if (tier === "observed") {
      return this.observedRides(
        park,
        dateStr,
        openHour,
        closeHour,
        byId,
        headlinerIds,
      );
    }

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
        headlinerIds,
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
        ...(headlinerIds.has(attraction.id) ? { isHeadliner: true } : {}),
        ...PlanDayService.riderFacts(attraction),
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
    headlinerIds: ReadonlySet<string>,
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
        ...(headlinerIds.has(attractionId) ? { isHeadliner: true } : {}),
        ...PlanDayService.riderFacts(attraction),
      });
    }

    return rides;
  }

  /**
   * A day in the past, from what the queues actually did.
   *
   * `attraction_hourly_history` already holds it: one row per (attraction,
   * park-local date) with the day's 15-minute slots, written by the 04:30 cron
   * out of raw `queue_data`. So this endpoint needed no new storage and no new
   * aggregation — one indexed read per request, against a primary key.
   *
   * The hour is the SAMPLE-WEIGHTED MEAN of its slots' `avgWait`, and both
   * halves of that are deliberate. `avgWait` rather than `p90`, because the
   * forward tiers are point estimates from the median quantile and a reader
   * comparing yesterday against tomorrow must be comparing the same kind of
   * number — `p90` is the bad case, and swapping it in for the past would make
   * every finished day look worse than the day that follows it. Weighted,
   * because a slot the feed only answered twice must not count as much as one
   * it answered twelve times; a plain mean of four slots hands a two-minute
   * outage the same weight as a full quarter of an hour.
   *
   * What is NOT here is as deliberate. There is no `uncertaintyMinutes`: an
   * observation has no band, and sending a width of zero would be a claim about
   * precision rather than the absence of a claim. `downYesterday` is not asked
   * either — it answers a question about tomorrow's plan. And a ride the day has
   * no row for is omitted rather than drawn at zero: the table is written per
   * ride per day, so absence means the rollup has not reached that day (today is
   * never in it, nor is anything before the job first ran), which is not the
   * same statement as an empty queue.
   */
  private async observedRides(
    park: Park,
    dateStr: string,
    openHour: number,
    closeHour: number,
    byId: Map<string, Attraction>,
    headlinerIds: ReadonlySet<string>,
  ): Promise<PlanDayRideDto[]> {
    const history = await this.analyticsService
      .getParkHourlyHistory(park.id, dateStr)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: hourly history unavailable for ${park.slug} on ${dateStr}: ${err.message}`,
        );
        return new Map<string, AttractionHourlyHistory>();
      });

    const rides: PlanDayRideDto[] = [];
    for (const [attractionId, row] of history) {
      const attraction = byId.get(attractionId);
      if (!attraction) continue;

      // hour → [weightedSum, weight]
      const byHour = new Map<number, [number, number]>();
      for (const slot of row.slots ?? []) {
        const hour = Number(String(slot.time_slot).slice(0, 2));
        if (!Number.isInteger(hour) || hour < openHour || hour > closeHour) {
          continue;
        }
        const wait = Number(slot.avgWait);
        if (!Number.isFinite(wait)) continue;
        // A slot with no count still happened; treat it as one reading rather
        // than dropping it, or a gap in the writer's bookkeeping deletes an
        // hour of a day somebody actually stood in.
        const weight =
          Number(slot.sampleCount) > 0 ? Number(slot.sampleCount) : 1;
        const [sum, total] = byHour.get(hour) ?? [0, 0];
        byHour.set(hour, [sum + wait * weight, total + weight]);
      }

      const hours = [];
      for (let h = openHour; h <= closeHour; h++) {
        const bucket = byHour.get(h);
        if (!bucket || bucket[1] === 0) continue;
        hours.push({
          hour: h,
          wait: roundToNearest5Minutes(bucket[0] / bucket[1]),
        });
      }
      if (hours.length === 0) continue;

      rides.push({
        attractionSlug: attraction.slug,
        attractionName: attraction.name,
        land: attraction.landName ?? null,
        hours,
        dayPeak: Math.max(...hours.map((h) => h.wait)),
        // No band around a measurement.
        uncertaintyMinutes: null,
        // Exactly one measured day stands behind this curve, and it is this one.
        // The field means what it says; a caller reading it as "how much history
        // is behind this shape" gets the honest answer for an observed day.
        sampleDays: 1,
        latitude: PlanDayService.coord(attraction.latitude),
        longitude: PlanDayService.coord(attraction.longitude),
        ...(headlinerIds.has(attractionId) ? { isHeadliner: true } : {}),
        ...PlanDayService.riderFacts(attraction),
      });
    }

    return rides.sort((a, b) =>
      a.attractionName.localeCompare(b.attractionName),
    );
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
  /**
   * Who may ride, as far as this payload can say it.
   *
   * Through `resolveCuratedFacts` rather than reading the two columns here: the
   * curated cell wins over the synced one, and a curated `0` is a real answer
   * meaning "no minimum at all" — so the rule is `??`, never `||`, and it lives
   * in one place because it was already copied into two DTO mappers once.
   *
   * Both fields are OMITTED where nothing is known. Absent has to stay
   * distinguishable from a value: "no minimum height recorded" is not the same
   * statement as "any height may ride", and a planner asked whether a child can
   * ride must be able to answer "we do not know".
   */
  private static riderFacts(attraction: Attraction): {
    minimumHeight?: number | null;
    mayGetWet?: boolean | null;
  } {
    const facts = resolveCuratedFacts(attraction);
    return {
      ...(facts.minimumHeight !== null && facts.minimumHeight !== undefined
        ? { minimumHeight: facts.minimumHeight }
        : {}),
      ...(facts.mayGetWet !== null && facts.mayGetWet !== undefined
        ? { mayGetWet: facts.mayGetWet }
        : {}),
    };
  }

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
   * The join is `a.id`, NOT `a.id::text`. `queue_data.attractionId` is a uuid —
   * the `@JoinColumn` on the entity's `attraction` relation is what creates the
   * column, and the `@Column({ type: "text" })` written beside it on the same
   * property does not change that. Only `queue_data_aggregates.attractionId` is
   * text, and it is the table every older analytics query reads, so `::text`
   * looks like the house style and is wrong here. The note at the top of
   * `park-historical-stats.service.ts` says exactly this and this method was
   * written against it anyway: Postgres answers `operator does not exist:
   * text = uuid`, the catch below swallows it, and the set came back empty
   * every single time, on every park, silently. No test saw it, because SQL is
   * only parsed when it runs.
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
             JOIN attractions a ON a.id = qd."attractionId"
            WHERE a."parkId" = $1::uuid
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
    // A date in the past is not a forecast horizon, and the ordering here is
    // the whole reason to check it first: `-38 <= 1` is true, so a day five
    // weeks gone came back labelled `measured` — the most trustworthy tier, on
    // the emptiest possible answer, because the model generates forwards and
    // nothing matched.
    if (leadDays < 0) return "observed";
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

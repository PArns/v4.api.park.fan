import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { MLService } from "../../ml/ml.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { AttractionHourlyHistory } from "../../analytics/entities/attraction-hourly-history.entity";
import { ParkHourlyProfileDto } from "../../analytics/dto/park-hourly-profile.dto";
import { CalendarService } from "./calendar.service";
import { composeDayCurve } from "../../common/utils/day-shape.util";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import { formatInParkTimezone } from "../../common/utils/date.util";
import { formatInTimeZone } from "date-fns-tz";
import {
  PlanDayDto,
  PlanDayHourDto,
  PlanDayHoursSource,
  PlanDayRideDto,
  PlanDayTier,
} from "../dto/plan-day.dto";

/**
 * One day, ride by ride, hour by hour — the series a trip planner draws.
 *
 * Nothing upstream answers "what will Taron's queue be at 14:00 on 17 October".
 * What exists is an hourly forecast for the next 24 hours (`HOURLY_PREDICTIONS`
 * in the python service) and a day-level forecast for as far ahead as the park
 * has published a schedule — about six months. So there are two regimes, and
 * which one produced a number travels with it:
 *
 * - **measured** — the model's own hourly answer, at its own resolution. It
 *   exists for the next 24 hours and not one minute further, so a day inside
 *   that window is part measured and part composed and every hour says which
 *   it is (`PlanDayHourDto.source`).
 * - **composed** — a day-level prediction scaled by the ride's historical hour
 *   shape (see `composeDayCurve`). The level is predicted, the shape is
 *   historical.
 * - **observed** — a date in the past, answered from what the queues actually
 *   did. Not a forecast at all.
 * - **long_range** — the model has said nothing about this date, so there are
 *   no curves. Reported rather than guessed.
 *
 * The alternative to composing was returning nothing past tomorrow, and a
 * planner that goes blank in March for a July trip is not a planner. The
 * alternative to labelling it was letting a composed number look exactly like a
 * measured one, which is the failure this whole design is arranged against —
 * and which the code had anyway, twice: the tier was decided by DISTANCE, so a
 * day whose hourly rows never arrived was served composed under the `measured`
 * label, and the hours the 24-hour window did not reach were simply missing,
 * which cut the evening off a park that closes at 22:00.
 */
@Injectable()
export class PlanDayService {
  private readonly logger = new Logger(PlanDayService.name);

  /**
   * Where the python service's hourly generation stops
   * (`HOURLY_PREDICTIONS = 24` hours, as 96 quarter-hour slots).
   *
   * There is deliberately no matching DAILY constant. The daily horizon is not
   * a fixed number of days — `predict.py` walks the park's schedule, so it ends
   * where the operator's published calendar does, which measured across the
   * live parks is 181 to 362 days and averages 193. A hard-coded 60 was wrong
   * for every park, and labelled two thirds of the days it could actually
   * answer as out of range. Whether a date has a day level is a question with
   * an answer, so this asks it (see `dayLevels`).
   */
  private static readonly HOURLY_HORIZON_DAYS = 1;

  /**
   * Rides the historical shape may cover.
   *
   * 60 rather than the 20 this asked for at first, and it costs nothing: the
   * profile's own SQL already fetches `min(topN * 3, 60)` rides so its
   * peak-hour re-ranking has something to re-rank, and then throws away
   * everything past `topN`. Asking for 20 therefore ran the same query and
   * discarded two thirds of it — Phantasialand's ride list went from 34 to 16
   * between tomorrow and the day after, which reads as rides closing.
   */
  private static readonly SHAPE_RIDES = 60;

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    private readonly mlService: MLService,
    private readonly calendarService: CalendarService,
    private readonly historicalStatsService: ParkHistoricalStatsService,
    private readonly analyticsService: AnalyticsService,
    private readonly leadSnapshotService: PredictionLeadSnapshotService,
  ) {}

  async buildPlanDay(park: Park, dateStr: string): Promise<PlanDayDto> {
    const today = formatInParkTimezone(new Date(), park.timezone);
    const leadDays = this.daysBetween(today, dateStr);
    const isFuture = leadDays >= 0;

    // The day's own facts: opening hours, crowd level, weather, holiday flags.
    // One day rather than a month — the caller asked about one day, and the
    // calendar's month cache would answer with 92 KB to serve 1 KB of it.
    const day = await this.loadDay(park, dateStr);
    const status = day?.status ?? "UNKNOWN";

    // The historical shape, needed both to compose curves and to fall back on
    // for opening hours. An observed day wants neither: it is answered from
    // what happened, and `getParkHourlyProfile` is a year of aggregates over
    // every ride in the park. Composing a shape onto a past date would draw a
    // forecast for a day the visitor walked.
    const profile = isFuture ? await this.loadProfile(park) : null;

    let openHour = this.hourIn(day?.hours?.openingTime, park.timezone);
    let closeHour = this.hourIn(day?.hours?.closingTime, park.timezone);
    let hoursSource: PlanDayHoursSource | undefined =
      openHour !== null && closeHour !== null ? "schedule" : undefined;

    // No published hours, and the day is not a stated closure: fall back to the
    // hours this park's queues have actually been measured in. Without this the
    // whole response was an empty shell past the operator's publishing horizon
    // — 91 of 177 parks with hours reach 60 days, 38 reach 120 — which is
    // exactly the distance the planner exists for. The window is narrower than
    // the real day (it is where we have readings, not where the gates are), so
    // it is labelled `observed` rather than passed off as the schedule.
    if (
      hoursSource === undefined &&
      isFuture &&
      status !== "CLOSED" &&
      profile
    ) {
      const derived = PlanDayService.observedHours(profile);
      if (derived) {
        openHour = derived.openHour;
        closeHour = derived.closeHour;
        hoursSource = "observed";
      }
    }

    const context = {
      date: dateStr,
      status,
      openHour,
      closeHour,
      ...(hoursSource ? { hoursSource } : {}),
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
      tier: PlanDayService.nominalTier(leadDays),
      leadDays,
      leadTimeMae: await this.leadTimeMae(leadDays),
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

    // A closed day, or one whose hours nobody knows, has no curves to draw, and
    // saying so is the answer. The tier stays the nominal one for the distance:
    // with no curves there is no method to characterise.
    if (openHour === null || closeHour === null || closeHour < openHour) {
      return base;
    }

    if (!isFuture) {
      base.tier = "observed";
      base.rides = await this.observedRides(
        park,
        dateStr,
        openHour,
        closeHour,
        new Map((await this.attractions(park)).map((a) => [a.id, a])),
        await this.headlinerIds(park),
      );
      return base;
    }

    const built = await this.forecastRides(
      park,
      dateStr,
      leadDays,
      openHour,
      closeHour,
      profile,
    );
    base.tier = built.tier;
    base.rides = built.rides;
    return base;
  }

  /**
   * The forecast tiers, in one pass.
   *
   * Measured hours and composed hours are built for the same ride list and then
   * merged per hour, which is the fix for two failures that were one mistake:
   * the tier used to be decided by distance rather than by what came back, and
   * the hours the model's 24-hour window did not reach were dropped instead of
   * filled. Measured live at 17:15, today's plan for Disneyland Paris covered
   * 17:00–21:00 of a 09:00–22:00 day, tomorrow's covered 09:00–17:00, and
   * `dayPeak` was the maximum of whatever was left.
   */
  private async forecastRides(
    park: Park,
    dateStr: string,
    leadDays: number,
    openHour: number,
    closeHour: number,
    profile: ParkHourlyProfileDto | null,
  ): Promise<{ tier: PlanDayTier; rides: PlanDayRideDto[] }> {
    const withinHourly = leadDays <= PlanDayService.HOURLY_HORIZON_DAYS;

    const attractions = await this.attractions(park);
    if (attractions.length === 0) return { tier: "composed", rides: [] };
    const byId = new Map(attractions.map((a) => [a.id, a]));
    const bySlug = new Map(attractions.map((a) => [a.slug, a]));

    // Both are per-park sets keyed by attraction id, and neither is worth
    // serialising behind the other. The headliner set is the park's CURATED
    // answer — never re-derived from `dayPeak`, because a headliner having a
    // quiet Tuesday is still a headliner, and a planner that pointed at the
    // day's tallest bars instead would recommend whatever happens to be busy.
    const [dayLevels, downIds, headlinerIds, measured] = await Promise.all([
      this.dayLevels(park, dateStr),
      this.downYesterday(park, dateStr),
      this.headlinerIds(park),
      withinHourly
        ? this.measuredHours(park, dateStr, openHour, closeHour)
        : Promise.resolve({
            hours: new Map<string, Map<number, number>>(),
            bands: new Map<string, number>(),
          }),
    ]);

    // The composed curve per ride, the sample count behind its shape, and the
    // land the profile names it in — that one prefers the curated column, which
    // the raw `landName` on the attraction does not.
    const composed = new Map<string, Map<number, number>>();
    const sampleDays = new Map<string, number>();
    const land = new Map<string, string | null>();
    for (const shape of profile?.attractions ?? []) {
      const attraction = bySlug.get(shape.attractionSlug);
      if (!attraction) continue;
      sampleDays.set(attraction.id, shape.sampleDays);
      if (shape.land) land.set(attraction.id, shape.land);

      const level = dayLevels.get(attraction.id);
      if (!level) continue;
      const curve = composeDayCurve({
        shapeHours: profile!.hours,
        shapeP50: shape.p50,
        dayPeak: level.predictedWaitTime,
        openHour,
        closeHour,
      });
      // No measured shape — nothing to scale, so this ride is carried only if
      // the model answered for it hour by hour. Drawing it flat at the day's
      // level would assert the queue is the same all day.
      if (!curve) continue;
      composed.set(attraction.id, new Map(curve.map((p) => [p.hour, p.wait])));
    }

    const tier: PlanDayTier =
      measured.hours.size > 0
        ? "measured"
        : dayLevels.size > 0
          ? "composed"
          : "long_range";

    const rides: PlanDayRideDto[] = [];
    for (const attractionId of new Set([
      ...measured.hours.keys(),
      ...composed.keys(),
    ])) {
      const attraction = byId.get(attractionId);
      if (!attraction) continue;

      const measuredHours = measured.hours.get(attractionId);
      const composedHours = composed.get(attractionId);

      const hours: PlanDayHourDto[] = [];
      for (let h = openHour; h <= closeHour; h++) {
        const fromModel = measuredHours?.get(h);
        if (fromModel !== undefined) {
          hours.push({
            hour: h,
            wait: fromModel,
            ...(tier === "measured" ? {} : { source: "measured" as const }),
          });
          continue;
        }
        const fromShape = composedHours?.get(h);
        if (fromShape !== undefined) {
          hours.push({
            hour: h,
            wait: fromShape,
            ...(tier === "composed" ? {} : { source: "composed" as const }),
          });
        }
      }
      if (hours.length === 0) continue;

      const level = dayLevels.get(attractionId);
      rides.push({
        attractionSlug: attraction.slug,
        attractionName: attraction.name,
        land: land.get(attractionId) ?? attraction.landName ?? null,
        hours,
        // The day's peak, from the day-level forecast — the same statistic on
        // every tier. The maximum of `hours` is a fallback for a ride the daily
        // run has no row for, and nothing better exists there.
        dayPeak:
          level?.predictedWaitTime ?? Math.max(...hours.map((p) => p.wait)),
        // The band belongs to the number it surrounds, so it comes from the
        // same row as `dayPeak`; the widest hourly band is the fallback.
        uncertaintyMinutes:
          level?.uncertaintyMinutes ?? measured.bands.get(attractionId) ?? null,
        sampleDays: sampleDays.get(attractionId) ?? 0,
        latitude: PlanDayService.coord(attraction.latitude),
        longitude: PlanDayService.coord(attraction.longitude),
        ...(downIds.has(attractionId) ? { downYesterday: true } : {}),
        ...(headlinerIds.has(attractionId) ? { isHeadliner: true } : {}),
      });
    }

    // Busiest first: a planner reads the top of this list to decide what to
    // book time for. Name as the tie-break so the order is stable between two
    // requests for the same day.
    rides.sort(
      (a, b) =>
        b.dayPeak - a.dayPeak ||
        a.attractionName.localeCompare(b.attractionName),
    );

    return { tier, rides };
  }

  /**
   * The model's own hourly answer for one day, collapsed to whole hours.
   *
   * This is a LIVE prediction, not a read of the stored rows:
   * `MLService.getParkPredictions` posts to the python service and caches the
   * answer per park and day, and the park page asks for the same thing on the
   * same cache. The stored `wait_time_predictions` rows are the writer's copy
   * and are not what any read path serves.
   *
   * Each hour is the MEAN of its 15-minute slots: they are already point
   * estimates from the median quantile, and taking the maximum instead would
   * quietly turn an honest hour into a pessimistic one.
   */
  private async measuredHours(
    park: Park,
    dateStr: string,
    openHour: number,
    closeHour: number,
  ): Promise<{
    hours: Map<string, Map<number, number>>;
    bands: Map<string, number>;
  }> {
    const stored = await this.mlService
      .getParkPredictions(park.id, "hourly")
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: hourly predictions unavailable for ${park.slug}: ${err.message}`,
        );
        return { predictions: [] as PredictionDto[] };
      });

    // attraction → hour → slot values
    const slots = new Map<string, Map<number, number[]>>();
    const bands = new Map<string, number>();

    for (const p of stored.predictions) {
      if (p.predictionType !== "hourly") continue;
      const when = new Date(p.predictedTime);
      if (formatInParkTimezone(when, park.timezone) !== dateStr) continue;

      const hour = Number(formatInTimeZone(when, park.timezone, "HH"));
      if (hour < openHour || hour > closeHour) continue;

      const hours = slots.get(p.attractionId) ?? new Map<number, number[]>();
      const values = hours.get(hour) ?? [];
      values.push(p.predictedWaitTime);
      hours.set(hour, values);
      slots.set(p.attractionId, hours);

      if (p.uncertaintyMinutes != null) {
        // The widest band the day carries for this ride: the planner draws one
        // channel per ride, and understating it is the failure that matters.
        bands.set(
          p.attractionId,
          Math.max(bands.get(p.attractionId) ?? 0, p.uncertaintyMinutes),
        );
      }
    }

    const hours = new Map<string, Map<number, number>>();
    for (const [attractionId, byHour] of slots) {
      const means = new Map<number, number>();
      for (const [hour, values] of byHour) {
        if (values.length === 0) continue;
        const mean = values.reduce((a, b) => a + b, 0) / values.length;
        means.set(hour, roundToNearest5Minutes(mean));
      }
      if (means.size > 0) hours.set(attractionId, means);
    }

    return { hours, bands };
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
   * number. Weighted, because a slot the feed only answered twice must not
   * count as much as one it answered twelve times; a plain mean of four slots
   * hands a two-minute outage the same weight as a full quarter of an hour.
   *
   * `dayPeak` is the day's P90 rather than the maximum of those hours, because
   * that is the statistic the forecast side of the same field carries — a
   * day-peak proxy, since predict.py collapses the peak-window hours to a
   * per-day MAX, and the same number `getHeadlinerDailyPeaks` gives the
   * calendar for its past days. A mean on one side of today and a peak on the
   * other reads as the park getting busier next week when only the statistic
   * changed.
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
      let peak = 0;
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

        const slotPeak = Number(slot.p90);
        if (Number.isFinite(slotPeak)) peak = Math.max(peak, slotPeak);
      }

      const hours: PlanDayHourDto[] = [];
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
        dayPeak: roundToNearest5Minutes(peak),
        // No band around a measurement.
        uncertaintyMinutes: null,
        // Exactly one measured day stands behind this curve, and it is this one.
        // The field means what it says; a caller reading it as "how much history
        // is behind this shape" gets the honest answer for an observed day.
        sampleDays: 1,
        latitude: PlanDayService.coord(attraction.latitude),
        longitude: PlanDayService.coord(attraction.longitude),
        ...(headlinerIds.has(attractionId) ? { isHeadliner: true } : {}),
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
  private static coord(value: unknown): number | null {
    if (value === null || value === undefined) return null;
    if (typeof value === "string" && value.trim() === "") return null;
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }

  /**
   * Rides that broke and stayed broken through the previous operating day.
   *
   * Three states have to stay apart here, and the first version kept only one
   * of them apart: a ride qualifies when it was reported **DOWN** at least once
   * yesterday and was **never OPERATING** in any reading. A ride with no rows
   * at all is silence — a feed that stopped, a park that was shut — and warning
   * about it would be this service asserting something it did not observe. A
   * ride the feed called CLOSED all day is a season or a refurbishment, not a
   * fault: without the DOWN requirement this reported nine of Phantasialand's
   * winter-only and water attractions as down every day of the summer, which is
   * the same conflation `docs/architecture/attraction-status-and-seasonality.md`
   * is written about.
   *
   * Yesterday is the PARK's yesterday, and it is expressed as a timestamp RANGE
   * rather than as a cast on the column. `(qd.timestamp AT TIME ZONE $2)::date
   * = …` cannot be answered from an index and, worse, hides the column from
   * TimescaleDB's chunk exclusion: measured on the live database it read all 254
   * chunks ("Chunks excluded during startup: 0", 35 967 buffers, 1.1 s cold) to
   * find nine rows in one of them. A half-open range on the raw column prunes to
   * the one or two chunks the day touches.
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
              AND qd.timestamp >= (($3::date - INTERVAL '1 day')::timestamp AT TIME ZONE $2)
              AND qd.timestamp <  ($3::date::timestamp AT TIME ZONE $2)
            GROUP BY qd."attractionId"
           HAVING COUNT(*) FILTER (WHERE qd.status = 'OPERATING') = 0
              AND COUNT(*) FILTER (WHERE qd.status = 'DOWN') > 0`,
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

  /**
   * The measured error at this distance, or null.
   *
   * Read from the lead-time archive rather than computed here — see
   * `PredictionLeadSnapshot` for why the question cannot be answered from
   * `wait_time_predictions` at all. Null while the archive has too few scored
   * rows at that distance, which is the honest answer and the normal one for
   * the far buckets in the weeks after this ships.
   */
  private async leadTimeMae(leadDays: number): Promise<number | null> {
    if (leadDays <= 0) return null;
    return this.leadSnapshotService
      .getLeadTimeMae(leadDays)
      .catch((err: Error) => {
        this.logger.warn(`Plan day: lead-time MAE unavailable: ${err.message}`);
        return null;
      });
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

  /**
   * The park's measured hour shape.
   *
   * `topN` is this endpoint's own cap rather than the caller's: a planner wants
   * every ride it can get, and this payload is a few hundred bytes per ride.
   */
  private async loadProfile(park: Park): Promise<ParkHourlyProfileDto | null> {
    return this.historicalStatsService
      .getParkHourlyProfile(park, 1, PlanDayService.SHAPE_RIDES, 20)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: hourly profile unavailable for ${park.slug}: ${err.message}`,
        );
        return null;
      });
  }

  /**
   * The open window as the DATA has it, for a date the operator has not
   * published hours for.
   *
   * The profile's `hours` are the hours that cleared the measurement threshold
   * across the park over the last year, so their span is where this park has
   * queues — narrower than the gates' hours, never wider, and honest about
   * being a recording window rather than a schedule. Null when the park has no
   * measured hours at all, because inventing a 10-to-18 day for a park nobody
   * has watched is exactly the made-up fact this codebase keeps banning.
   */
  private static observedHours(
    profile: ParkHourlyProfileDto,
  ): { openHour: number; closeHour: number } | null {
    const hours = (profile.hours ?? []).filter(
      (h) => Number.isInteger(h) && h >= 0 && h <= 23,
    );
    if (hours.length === 0) return null;
    return {
      openHour: Math.min(...hours),
      closeHour: Math.max(...hours),
    };
  }

  private async attractions(park: Park): Promise<Attraction[]> {
    return this.attractionRepository.find({
      where: { parkId: park.id, retiredAt: IsNull() },
      select: ["id", "slug", "name", "landName", "latitude", "longitude"],
    });
  }

  private async headlinerIds(park: Park): Promise<ReadonlySet<string>> {
    return this.analyticsService
      .getHeadlinerAttractionIds(park.id)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: headliners unavailable for ${park.slug}: ${err.message}`,
        );
        return new Set<string>();
      });
  }

  /**
   * The label a day with no curves carries.
   *
   * A date in the past is not a forecast horizon, and the ordering here is the
   * whole reason it is checked first: `-38 <= 1` is true, so a day five weeks
   * gone came back labelled `measured` — the most trustworthy tier, on the
   * emptiest possible answer. Every day that DOES produce curves gets its tier
   * from those curves instead (`forecastRides`).
   */
  private static nominalTier(leadDays: number): PlanDayTier {
    if (leadDays < 0) return "observed";
    if (leadDays <= PlanDayService.HOURLY_HORIZON_DAYS) return "measured";
    return "composed";
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

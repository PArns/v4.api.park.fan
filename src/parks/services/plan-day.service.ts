import { forwardRef, Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { IsNull, Repository } from "typeorm";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { MLService } from "../../ml/ml.service";
import { PredictionDto } from "../../ml/dto/prediction-response.dto";
import { PredictionLeadSnapshotService } from "../../ml/services/prediction-lead-snapshot.service";
import { ForecastAccuracyService } from "../../ml/services/forecast-accuracy.service";
import { ForecastAccuracyProfile } from "../../ml/entities/forecast-accuracy-profile.entity";
import { ParkHistoricalStatsService } from "../../analytics/park-historical-stats.service";
import { AnalyticsService } from "../../analytics/analytics.service";
import { AttractionHourlyHistory } from "../../analytics/entities/attraction-hourly-history.entity";
import { ParkHourlyProfileDto } from "../../analytics/dto/park-hourly-profile.dto";
import { CalendarService } from "./calendar.service";
import { ShowsService } from "../../shows/shows.service";
import { ShowSchedulePattern } from "../../shows/entities/show-schedule-pattern.entity";
import {
  composeDayCurve,
  unfoldedCloseHour,
} from "../../common/utils/day-shape.util";
import { roundToNearest5Minutes } from "../../common/utils/wait-time.utils";
import { formatInParkTimezone } from "../../common/utils/date.util";
import { formatInTimeZone } from "date-fns-tz";
import {
  PlanDayDto,
  PlanDayHourDto,
  PlanDayHoursSource,
  PlanDayRideDto,
  PlanDayShowDto,
  PlanDayTier,
} from "../dto/plan-day.dto";
import type { PlanDayAccuracyDto } from "../dto/plan-day.dto";
import { buildLiveWaitTimes } from "../dto/live-wait-times.dto";
import { resolveCuratedPark } from "../utils/curated-park-facts.util";
import {
  isCurrentlyInSeason,
  resolveCuratedFacts,
} from "../../attractions/utils/curated-attraction-facts.util";

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
   * How far "this ride is shut at the moment" still describes the day asked about.
   *
   * Today and tomorrow, the same reach `downYesterday` gives itself and for the
   * same reason: both are readings of the current state rather than facts about
   * a date. It bounds one branch of {@link outOfSeasonOn} — a detector note with
   * no months behind it — and nothing else; a ride whose season months are on
   * file is judged on them at every horizon.
   */
  private static readonly SEASON_NOW_HORIZON_DAYS = 1;

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

  /**
   * Days a show must have been seen on a weekday before its times are projected.
   *
   * One sighting is an event. "Crazy Summer with Ross Antony & Paul Reeves" was
   * seen at Europa-Park exactly once, on 23 July; projecting it forward would
   * have put a concert on every remaining Thursday of the year.
   */
  private static readonly MIN_PATTERN_DAYS = 2;

  /**
   * How stale a pattern may be before it stops being projected.
   *
   * A programme nobody has seen for a month is last month's programme. The
   * window the pattern itself is built from is eight weeks, so this is the
   * tighter of the two and the one that decides.
   */
  private static readonly MAX_PATTERN_AGE_DAYS = 28;

  constructor(
    @InjectRepository(Attraction)
    private readonly attractionRepository: Repository<Attraction>,
    private readonly mlService: MLService,
    private readonly calendarService: CalendarService,
    private readonly historicalStatsService: ParkHistoricalStatsService,
    private readonly analyticsService: AnalyticsService,
    private readonly leadSnapshotService: PredictionLeadSnapshotService,
    @Inject(forwardRef(() => ShowsService))
    private readonly showsService: ShowsService,
    private readonly accuracyService: ForecastAccuracyService,
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
    // The park's opening to the minute, not just the hour: a ride opening at
    // 09:45 in a park that opens at 09:00 shares its hour and is still 45
    // minutes later, which is exactly what a visitor needs told.
    const parkOpensAt = this.hhmmIn(day?.hours?.openingTime, park.timezone);
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
      // Off the ENTITY, not off the day: whether a source exists is a property
      // of the park and is true of every date it has. Repeated here because a
      // planner holds this payload and nothing else — and an empty `rides` on
      // a drawn axis is exactly what a park with no source and a park whose
      // rides have no history both look like, so without this a caller cannot
      // tell "we don't know" from "nothing much is queueing".
      liveWaitTimes: buildLiveWaitTimes(
        resolveCuratedPark(park).noWaitTimesReason,
      ),
    };

    const base: PlanDayDto = {
      parkSlug: park.slug,
      timezone: park.timezone,
      context,
      tier: PlanDayService.nominalTier(leadDays),
      leadDays,
      leadTimeMae: await this.leadTimeMae(leadDays),
      accuracy: { basis: "unmeasured" },
      rides: [],
      shows: await this.buildShows(park, dateStr),
    };

    // A closed day, or one whose hours nobody knows, has no curves to draw, and
    // saying so is the answer. The tier stays the nominal one for the distance:
    // with no curves there is no method to characterise.
    if (openHour === null || closeHour === null) {
      return base;
    }

    // Past this point the day is ONE ascending run of hours, midnight or no
    // midnight. `closeHour < openHour` used to land in the guard above, so La
    // Ronde (`10 → 0`), Six Flags Mexico and Six Flags Qiddiya City — wrap parks
    // every day of the year — have never carried a single hourly curve, and on
    // Halloween 2026 thirteen parks wrapped and all thirteen came back empty.
    // The context above keeps the operator's own `closeHour`; everything below
    // counts past it (see `unfoldedCloseHour`).
    const lastHour = unfoldedCloseHour(openHour, closeHour);

    if (!isFuture) {
      base.tier = "observed";
      base.rides = await this.observedRides(
        park,
        dateStr,
        openHour,
        lastHour,
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
      lastHour,
      profile,
      hoursSource === "schedule" ? parkOpensAt : null,
    );
    base.tier = built.tier;
    base.rides = built.rides;
    base.accuracy = built.accuracy;
    return base;
  }

  /**
   * The day's shows — the operator's answer where one exists, ours where it does
   * not, and never the two looking alike.
   *
   * **No feed publishes showtimes beyond the current day.** Measured against
   * production over the whole retained history (snapshots 2025-12-24…2026-09-08;
   * 15,185,105 showtime entries, 47 parks): 15,064,895 fall on the operating day
   * their snapshot was taken on and 120,058 fall *before* it — the feed's
   * uncleared litter, reaching 1,396 days back. Only 152 entries lead their own
   * snapshot at all, and 109 of those are performances past midnight
   * (00:00–05:30, Universal's late programme) that belong to the day they were
   * published on — though nothing here unfolds them the way §5 unfolds opening
   * hours, so `getShowtimesOnDate` does attribute them to the following day.
   * That leaves 43 entries genuinely dated one or two days ahead, in two parks,
   * from snapshots taken between 2025-12-23 and 2025-12-27 park-local, with no
   * successor in the eight months since. Three per million is not a horizon to
   * build on, so a planner asking about October gets nothing from the feed, and
   * `shows` was an empty array for every future date.
   *
   * The projection is the show's own recent behaviour: the times it ran at on
   * the most recent day with the SAME WEEKDAY. That distinction is not decorative
   * — measured at Europa-Park, "Big Moments – The Celebration-Show" runs twice on
   * a Thursday and three times on a Saturday, and "Carnival in Venice" runs an
   * hour later on Saturdays. A weekday-blind projection would either drop the
   * extra performance or promise it on a Tuesday.
   *
   * Two guards keep a projection from becoming a claim: a show has to have been
   * seen on that weekday more than once ({@link MIN_PATTERN_DAYS}), and recently
   * ({@link MAX_PATTERN_AGE_DAYS}). Both come from a real case in the data — a
   * one-off summer concert that would otherwise have been promised every
   * Thursday until Christmas.
   *
   * A park whose shows we have never watched gets `[]`, which is a different
   * statement from "this park has no shows" and is why the DTO says so.
   */
  private async buildShows(
    park: Park,
    dateStr: string,
  ): Promise<PlanDayShowDto[]> {
    const shows = await this.showsService
      .findByParkId(park.id)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: shows unavailable for ${park.slug}: ${err.message}`,
        );
        return [];
      });
    if (shows.length === 0) return [];

    // Postgres' EXTRACT(DOW) and JavaScript's getUTCDay() agree (0 = Sunday);
    // the pattern column is written by the former and read here by the latter.
    const weekday = new Date(`${dateStr}T00:00:00Z`).getUTCDay();

    const [scheduled, patterns] = await Promise.all([
      this.showsService
        .getShowtimesOnDate(park.id, park.timezone, dateStr)
        .catch(() => new Map<string, string[]>()),
      this.showsService
        .getSchedulePatterns(park.id, weekday)
        .catch(() => new Map<string, ShowSchedulePattern>()),
    ]);

    const out: PlanDayShowDto[] = [];
    for (const show of shows) {
      const known = scheduled.get(show.id);
      if (known && known.length > 0) {
        out.push({
          showSlug: show.slug,
          showName: show.name,
          times: known,
          source: "scheduled",
        });
        continue;
      }

      const pattern = patterns.get(show.id);
      if (!pattern || pattern.times.length === 0) continue;
      if (pattern.observedDays < PlanDayService.MIN_PATTERN_DAYS) continue;
      // Measured against TODAY, never against the target date: a pattern is
      // stale because nobody has seen it lately, not because the day asked
      // about is far away. Measuring against the target would reject every
      // date more than four weeks out — which is most of what a planner asks.
      if (
        this.daysBetween(pattern.lastObservedOn, this.today(park)) >
        PlanDayService.MAX_PATTERN_AGE_DAYS
      ) {
        continue;
      }

      out.push({
        showSlug: show.slug,
        showName: show.name,
        times: pattern.times,
        source: "projected",
        observedOn: pattern.lastObservedOn,
        sampleDays: pattern.observedDays,
      });
    }

    // Earliest first: a plan is read down the day.
    return out.sort(
      (a, b) =>
        (a.times[0] ?? "").localeCompare(b.times[0] ?? "") ||
        a.showName.localeCompare(b.showName),
    );
  }

  /** Today, in the park's timezone. */
  private today(park: Park): string {
    return formatInParkTimezone(new Date(), park.timezone);
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
    /** The park's own opening as `HH:mm`, when it published one. */
    parkOpensAt: string | null,
  ): Promise<{
    tier: PlanDayTier;
    rides: PlanDayRideDto[];
    accuracy: PlanDayAccuracyDto;
  }> {
    const withinHourly = leadDays <= PlanDayService.HOURLY_HORIZON_DAYS;

    const attractions = await this.attractions(park);
    if (attractions.length === 0)
      return { tier: "composed", rides: [], accuracy: { basis: "unmeasured" } };

    // How wrong a forecast at this distance usually is. Eighteen measured cells
    // (three predicted bands × six lead buckets), so it is read whole and looked
    // up per ride; `null` past the last bucket, which is the honest answer rather
    // than the nearest one.
    const leadBucket = ForecastAccuracyService.bucketFor(leadDays);
    const accuracy = leadBucket
      ? await this.accuracyService.getProfile().catch((err: Error) => {
          this.logger.warn(
            `Plan day: accuracy profile unavailable: ${err.message}`,
          );
          return null;
        })
      : null;
    // A ride that cannot open on the day being planned is not one of the day's
    // rides — not a closed one, absent. Dropping it here rather than at the end
    // covers both regimes at once: the composed curve resolves its ride through
    // `bySlug` and the model's hourly answer through `byId`, and neither finds
    // one that is not in these maps.
    //
    // Nothing upstream does this for us, which is the whole ticket.
    // `MLService.getParkPredictions` filters to rides with an OPERATING reading
    // in the last **90 days**, and that filter cannot answer this question: it
    // looks backwards while this endpoint is asked about a date up to half a
    // year ahead, so a summer water ride still carrying August's readings in
    // September gets a full forecast for 20 December. The same gap runs the
    // other way for up to 90 days after a season ends — the shoulder weeks
    // somebody is most likely to be planning in.
    const plannable = attractions.filter(
      (a) =>
        !PlanDayService.outOfSeasonOn(
          a,
          dateStr,
          leadDays <= PlanDayService.SEASON_NOW_HORIZON_DAYS,
        ),
    );
    const byId = new Map(plannable.map((a) => [a.id, a]));
    const bySlug = new Map(plannable.map((a) => [a.slug, a]));

    // Both are per-park sets keyed by attraction id, and neither is worth
    // serialising behind the other. The headliner set is the park's CURATED
    // answer — never re-derived from `dayPeak`, because a headliner having a
    // quiet Tuesday is still a headliner, and a planner that pointed at the
    // day's tallest bars instead would recommend whatever happens to be busy.
    const [dayLevels, downIds, headlinerIds, openings, measured] =
      await Promise.all([
        this.dayLevels(park, dateStr),
        this.downYesterday(park, dateStr),
        this.headlinerIds(park),
        this.rideOpenings(park),
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

    // `measured` describes the method behind the curves that were actually
    // served, so it counts only rides that survived to `byId`. The map arrives
    // straight from the model and knows nothing about seasons or retirements:
    // asking it for a size labelled a day `measured` on the strength of an
    // hourly answer for a ride nobody gets back, and every ride that WAS served
    // then carried `source: "composed"` because its hours disagreed with the
    // header.
    //
    // The `composed`/`long_range` line deliberately stays on the raw
    // `dayLevels`. `long_range` is published as "the model has produced no day
    // level for this date" (see the DTO), and a fully seasonal park in its off
    // months would otherwise be labelled that way while the model had in fact
    // answered for every ride — a statement about our reach, made about their
    // calendar.
    let someMeasuredServed = false;
    for (const id of measured.hours.keys()) {
      if (byId.has(id)) {
        someMeasuredServed = true;
        break;
      }
    }
    const tier: PlanDayTier = someMeasuredServed
      ? "measured"
      : dayLevels.size > 0
        ? "composed"
        : "long_range";

    const rides: PlanDayRideDto[] = [];
    // The accuracy cells actually quoted, so `sampleSize` below counts the
    // comparisons behind what was served rather than behind a bucket that may
    // have been widened away from.
    const usedCells = new Set<ForecastAccuracyProfile>();
    for (const attractionId of new Set([
      ...measured.hours.keys(),
      ...composed.keys(),
    ])) {
      const attraction = byId.get(attractionId);
      if (!attraction) continue;

      const measuredHours = measured.hours.get(attractionId);
      const composedHours = composed.get(attractionId);

      // The ride's own first hour, never earlier than the park's. Half the rides
      // report OPERATING before the gates open — the feed carries the operator's
      // system state — so the park's hour is the floor and the ride's is the
      // correction on top of it.
      //
      // Rounded, not floored: an hour in this grid stands for the whole hour, so
      // a ride opening at 09:45 belongs to hour 10 rather than putting a queue
      // against three quarters of an hour it was shut for. 10:10 rounds to 10
      // either way.
      // Keyed by THIS day's park opening: the gap is seasonal, not a property of
      // the ride. Phantasialand's coasters run an hour after a 09:00 gate and
      // with an 11:00 one, so a summer median applied to a winter morning — or
      // the reverse — would be a confident wrong answer. A park opening we have
      // never watched simply has no entry, and the ride draws the park's day.
      const opensAt = parkOpensAt
        ? openings.get(`${attractionId}|${parkOpensAt}`)
        : undefined;
      const rideOpenHour = Math.max(
        openHour,
        opensAt ? Math.round(PlanDayService.minutesOf(opensAt) / 60) : openHour,
      );
      // Reported whenever the ride opens later than the PARK, even inside the
      // same hour — comparing hours hid Phantasialand's 09:45 ride behind a
      // 09:00 park and left the reader without the one number they wanted.
      const opensLater =
        opensAt !== undefined &&
        parkOpensAt !== null &&
        PlanDayService.minutesOf(opensAt) >
          PlanDayService.minutesOf(parkOpensAt);

      const hours: PlanDayHourDto[] = [];
      for (let h = rideOpenHour; h <= closeHour; h++) {
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
      const dayPeak =
        level?.predictedWaitTime ?? Math.max(...hours.map((p) => p.wait));
      // Widens to a coarser bucket when this distance's cell is absent — which
      // is the normal state for the day after a deploy that adds a bucket, since
      // the stored grid is only replaced by the nightly rebuild.
      const cell = accuracy
        ? ForecastAccuracyService.lookup(accuracy, dayPeak, leadDays)
        : undefined;
      if (cell) usedCells.add(cell);
      rides.push({
        attractionSlug: attraction.slug,
        attractionName: attraction.name,
        land: land.get(attractionId) ?? attraction.landName ?? null,
        hours,
        // The day's peak, from the day-level forecast — the same statistic on
        // every tier. The maximum of `hours` is a fallback for a ride the daily
        // run has no row for, and nothing better exists there.
        dayPeak,
        // The band belongs to the number it surrounds, so it comes from the
        // same row as `dayPeak`; the widest hourly band is the fallback.
        uncertaintyMinutes:
          level?.uncertaintyMinutes ?? measured.bands.get(attractionId) ?? null,
        sampleDays: sampleDays.get(attractionId) ?? 0,
        ...(cell ? { expectedError: cell.mae } : {}),
        ...(opensLater ? { opensAt } : {}),
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

    const scored = rides.filter((r) => r.expectedError != null);
    const summary: PlanDayAccuracyDto =
      scored.length > 0
        ? {
            basis: "measured",
            typicalError:
              Math.round(
                (scored.reduce((a, r) => a + (r.expectedError ?? 0), 0) /
                  scored.length) *
                  10,
              ) / 10,
            // The comparisons behind the figures actually served, not behind the
            // bucket this distance ideally wanted: `lookup` may have widened to a
            // coarser one, and summing the ideal bucket would report 0 next to a
            // `measured` basis on the day after a deploy that adds a bucket.
            //
            // Unconditional because this arm requires `scored.length > 0`, and a
            // scored ride is by definition one whose cell went into `usedCells`.
            sampleSize: [...usedCells].reduce((a, c) => a + c.sampleSize, 0),
          }
        : { basis: "unmeasured" };

    return { tier, rides, accuracy: summary };
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
   *
   * `closeHour` is the UNFOLDED one, and the rows are matched against two dates
   * because of it: the 00:30 slot of a `10 → 1` day carries tomorrow's
   * park-local date, so the date test alone dropped precisely the hours that
   * make such a day unusual. They come back as hour 24 and 25.
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

    const nextDate = PlanDayService.plusDays(dateStr, 1);

    for (const p of stored.predictions) {
      if (p.predictionType !== "hourly") continue;
      const when = new Date(p.predictedTime);
      const localDate = formatInParkTimezone(when, park.timezone);
      if (localDate !== dateStr && localDate !== nextDate) continue;

      // Tomorrow's small hours belong to today's day when today runs past
      // midnight, and to tomorrow otherwise — which the range test below
      // settles on its own: on an ordinary day `closeHour` is at most 23, so
      // every hour that arrives here as 24 or later falls out.
      const hour =
        Number(formatInTimeZone(when, park.timezone, "HH")) +
        (localDate === dateStr ? 0 : 24);
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
    // The day's own row, and — only where the day runs past midnight — the next
    // date's, whose small hours belong to it. Asked together: two indexed reads
    // against a primary key, and the second is not asked for at all on an
    // ordinary day.
    const [own, afterMidnight] = await Promise.all([
      this.hourlyHistory(park, dateStr),
      closeHour > 23
        ? this.hourlyHistory(park, PlanDayService.plusDays(dateStr, 1))
        : Promise.resolve(new Map<string, AttractionHourlyHistory>()),
    ]);

    // attraction → { hour → [weightedSum, weight], the day's peak }
    const measured = new Map<
      string,
      { byHour: Map<number, [number, number]>; peak: number }
    >();

    const collect = (
      history: Map<string, AttractionHourlyHistory>,
      offset: number,
    ) => {
      for (const [attractionId, row] of history) {
        if (!byId.has(attractionId)) continue;
        for (const slot of row.slots ?? []) {
          const wallHour = Number(String(slot.time_slot).slice(0, 2));
          if (!Number.isInteger(wallHour)) continue;
          const hour = wallHour + offset;
          if (hour < openHour || hour > closeHour) continue;
          const wait = Number(slot.avgWait);
          if (!Number.isFinite(wait)) continue;
          // A slot with no count still happened; treat it as one reading rather
          // than dropping it, or a gap in the writer's bookkeeping deletes an
          // hour of a day somebody actually stood in.
          const weight =
            Number(slot.sampleCount) > 0 ? Number(slot.sampleCount) : 1;
          const ride = measured.get(attractionId) ?? {
            byHour: new Map<number, [number, number]>(),
            peak: 0,
          };
          const [sum, total] = ride.byHour.get(hour) ?? [0, 0];
          ride.byHour.set(hour, [sum + wait * weight, total + weight]);

          const slotPeak = Number(slot.p90);
          if (Number.isFinite(slotPeak)) {
            ride.peak = Math.max(ride.peak, slotPeak);
          }
          measured.set(attractionId, ride);
        }
      }
    };

    collect(own, 0);
    collect(afterMidnight, 24);

    const rides: PlanDayRideDto[] = [];
    for (const [attractionId, { byHour, peak }] of measured) {
      const attraction = byId.get(attractionId);
      if (!attraction) continue;

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
   *
   * The window is found as the LARGEST GAP on the 24-hour clock rather than as
   * min-to-max, and the two differ on exactly one kind of park. For a park
   * measured 10:00–19:00 the widest gap is the night (19 → 10), so the day it
   * hands back is 10 → 19 — min and max, the same answer as before. For a park
   * whose day crosses midnight the widest gap is the AFTERNOON it is shut: a
   * measured [0, 16 … 23] gives 16 → 24 instead of min-to-max's 0 → 23, which
   * drew a queue for 03:00 and called it the park's day. La Ronde, Six Flags
   * Mexico and Six Flags Qiddiya City close at midnight all year, and this
   * fallback is the only window they have past the operator's horizon.
   */
  private static observedHours(
    profile: ParkHourlyProfileDto,
  ): { openHour: number; closeHour: number } | null {
    const hours = [
      ...new Set(
        (profile.hours ?? []).filter(
          (h) => Number.isInteger(h) && h >= 0 && h <= 23,
        ),
      ),
    ].sort((a, b) => a - b);
    if (hours.length === 0) return null;
    if (hours.length === 1) {
      return { openHour: hours[0], closeHour: hours[0] };
    }

    // The hour after the widest silence is where the day starts. The night — the
    // step from the last measured hour back round to the first — is the opening
    // bid, so a tie leaves the window exactly where min-to-max had it and only
    // a strictly wider daytime gap moves it.
    let openIndex = 0;
    let widest = (hours[0] - hours[hours.length - 1] + 24) % 24;
    for (let i = 0; i < hours.length - 1; i++) {
      const gap = hours[i + 1] - hours[i];
      if (gap > widest) {
        widest = gap;
        openIndex = i + 1;
      }
    }

    const openHour = hours[openIndex];
    const closeHour = hours[(openIndex + hours.length - 1) % hours.length];
    return { openHour, closeHour };
  }

  /**
   * Every ride the park still has, seasonality included.
   *
   * The five season columns are selected but NOT filtered on here, because the
   * two callers want opposite things from them: {@link forecastRides} drops a
   * ride that cannot open on the day it is planning ({@link outOfSeasonOn}),
   * and {@link observedRides} must not, since a row in the hourly rollup is a
   * measurement of the ride having run.
   */
  private async attractions(park: Park): Promise<Attraction[]> {
    return this.attractionRepository.find({
      where: { parkId: park.id, retiredAt: IsNull() },
      select: [
        "id",
        "slug",
        "name",
        "landName",
        "latitude",
        "longitude",
        "isSeasonal",
        "seasonMonths",
        "seasonOutSince",
        "curatedIsSeasonal",
        "curatedSeasonMonths",
      ],
    });
  }

  /**
   * Whether this ride's season says it cannot open on the day being planned.
   *
   * Three things about this are decisions rather than mechanics.
   *
   * **The month is the PLANNED day's, never today's.** Every other surface in
   * the codebase asks `isCurrentlyInSeason` about now, because it is describing
   * now; this endpoint is asked about a date up to half a year out. A ride with
   * months on file is therefore judged against December when December is what
   * was asked about, whatever month the request arrives in. The date is turned
   * into a local `Date` from its parts, so `getMonth()` returns the month that
   * is written in the string whatever timezone the server keeps.
   *
   * **`=== false`, never `!== true`.** `isCurrentlyInSeason` has three answers
   * and the third is the point: `null` means "seasonal, and nothing else known",
   * which the detector says about everything under `MIN_OBSERVED_DAYS` of
   * history. It may not hide a ride we have merely not understood yet.
   *
   * **A `seasonOutSince` with no months only speaks for the near horizon**, and
   * that is the one place this departs from the other readers of these columns.
   * Everywhere else the question is "is it running now", so the distinction
   * cannot arise; here it decides how far a fact reaches.
   *
   * Read the detector rather than the field name. `season_out_since` is written
   * by `QueuePercentileProcessor` for a ride fully CLOSED on **7 park-open days
   * inside a 60-day window** whose **current status is CLOSED**, and it is
   * cleared as soon as the ride reports OPERATING again. That is a statement
   * about now, refreshed nightly — not a calendar. A three-week technical
   * closure satisfies it exactly as well as a season does, and months only
   * arrive at `MIN_OBSERVED_DAYS` (330) of history, so a headliner shut for
   * refurbishment in September would otherwise vanish from a plan for
   * 20 December with no field saying why. An absent fact may not become a
   * confident one (`claude.md` §4).
   *
   * So past {@link SEASON_NOW_HORIZON_DAYS} the note is dropped and only the
   * calendar question is left — which, with no months on file, answers `null`
   * and keeps the ride. It costs nothing where this ticket's own examples live:
   * a ride WITH months is judged on months at every horizon, and a ride still
   * running today carries no `seasonOutSince` at all, so the far-date case was
   * never this branch's to answer.
   *
   * What this does NOT do is overrule the season with a live reading. The park
   * page does (`closedByTheSeason` in `park-integration.service.ts`: a live
   * `OPERATING` row means the season on file is behind the park), and this
   * service reads no live status, so for TODAY the two can disagree about a ride
   * whose season data has gone stale. Closing that needs a per-request status
   * query on a hot path, which is a cost decision of its own.
   *
   * @param nearHorizon - Whether "shut now" still speaks for the day asked about.
   */
  private static outOfSeasonOn(
    attraction: Attraction,
    dateStr: string,
    nearHorizon: boolean,
  ): boolean {
    const facts = resolveCuratedFacts(attraction);
    return (
      isCurrentlyInSeason(
        nearHorizon ? facts : { ...facts, seasonOutSince: null },
        PlanDayService.localNoonOf(dateStr),
      ) === false
    );
  }

  /**
   * A `YYYY-MM-DD` as a `Date` in the running process's own zone, at midday.
   *
   * Built from the parts rather than parsed: `new Date("2026-12-20")` is UTC
   * midnight, and `getMonth()` on it answers November anywhere west of
   * Greenwich. Midday rather than midnight because a few zones have no 00:00 on
   * a DST day.
   */
  private static localNoonOf(dateStr: string): Date {
    const [year, month, day] = dateStr.split("-").map(Number);
    return new Date(year, month - 1, day, 12);
  }

  /**
   * When each ride opens, park-local `HH:mm`, or an empty map.
   *
   * A failure here costs the clamp, not the day: without it every curve starts
   * at the park's opening, which is the behaviour this replaced.
   */
  private async rideOpenings(park: Park): Promise<Map<string, string>> {
    return this.analyticsService
      .getRideOpeningTimes(park.id, park.timezone)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: ride openings unavailable for ${park.slug}: ${err.message}`,
        );
        return new Map<string, string>();
      });
  }

  /**
   * One day's hourly rollup for the whole park, or an empty map.
   *
   * A failure here costs the day's curves, not the response: the context, the
   * shows and the day's facts are worth serving on their own.
   */
  private async hourlyHistory(
    park: Park,
    dateStr: string,
  ): Promise<Map<string, AttractionHourlyHistory>> {
    return this.analyticsService
      .getParkHourlyHistory(park.id, dateStr)
      .catch((err: Error) => {
        this.logger.warn(
          `Plan day: hourly history unavailable for ${park.slug} on ${dateStr}: ${err.message}`,
        );
        return new Map<string, AttractionHourlyHistory>();
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

  /** Park-local `HH:mm` of an instant, or null when there is no instant. */
  private hhmmIn(
    value: Date | string | undefined,
    timezone: string,
  ): string | null {
    if (!value) return null;
    const at = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(at.getTime())) return null;
    return formatInTimeZone(at, timezone, "HH:mm");
  }

  /** Minutes since midnight for an `HH:mm`. */
  private static minutesOf(hhmm: string): number {
    const [h, m] = hhmm.split(":").map(Number);
    return (Number.isFinite(h) ? h : 0) * 60 + (Number.isFinite(m) ? m : 0);
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

  /**
   * A `YYYY-MM-DD` shifted by whole days.
   *
   * UTC arithmetic on a date-only string, which is right precisely because it
   * is not a timestamp: the park-local calendar day after 2026-10-31 is
   * 2026-11-01 whatever the offset does that night, and converting through the
   * park's timezone to find that out is how a DST boundary turns into an
   * off-by-one.
   */
  private static plusDays(dateStr: string, days: number): string {
    return new Date(Date.parse(`${dateStr}T00:00:00Z`) + days * 86_400_000)
      .toISOString()
      .slice(0, 10);
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

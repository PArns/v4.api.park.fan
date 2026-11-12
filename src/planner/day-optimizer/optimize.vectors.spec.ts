/**
 * The day optimiser, against a truth it cannot argue with. Ported from park.fan's
 * `scripts/test-planner-optimize.mjs`, where the optimizer ran in the browser; the vectors are kept
 * as they were, untyped, so a difference between the two engines shows up as a failing check
 * rather than a rewritten one. The checks run while the file loads; the test below reports them.
 *
 * The interesting assertion in here is the first one and it is the reason the
 * file exists: `optimizeDay` is a HEURISTIC — a beam search plus a local search
 * — so "it returns a plan" proves nothing. For every instance small enough to
 * enumerate, this brute-forces all `n!` orders through the optimiser's own
 * scorer and asserts the heuristic found one of the optima. Eight rides is
 * 40,320 orders, which a test can afford and a click cannot.
 *
 * The rest pin the properties a visitor would notice if they broke: a lunch
 * break stays where it was put, a ticked-off ride is not re-planned, a ride that
 * opens at 11:00 is not queued for at 09:15, pressing the button twice does
 * nothing the second time, and the same day always produces the same plan.
 *
 * Two of them are here because the assertion they replace was TRUE and proved
 * nothing. "No ride is laid across the lunch break" ran against a plan that
 * ended at 11:45 with the break at 13:00, so it held before anything was
 * scheduled — §4 now uses two fixed blocks and a ride whose block grows across
 * an hour boundary, which is the case that was actually broken. And
 * `uncertaintyMinutes` was set on no fixture in this file, so every span in it
 * was the bare queue and nothing exercised a band at all. That is §4b, which
 * since PAR-169 pins the two apart: the block is DRAWN at wait plus band, the
 * plan is BUILT on the wait alone, and the search and the incumbent measure it
 * the same way.
 *
 *     pnpm test -- optimize.vectors
 *     BENCH=1 pnpm test -- optimize.vectors   # plus the timing table for §16
 */

import * as gridModule from "./day-grid";
import * as estimateModule from "./estimate";
import * as legModule from "./leg";
import * as optimizeModule from "./optimize";
import { planEntries } from "./plan-entries";

// Untyped on purpose: the fixtures are the browser's, written without types.
const { RIDE_DURATION_MIN, earlyEntryOpenMin } = gridModule as any;
const { occupiedMinutes, plannedMinutes } = estimateModule as any;
const { transferBetween } = legModule as any;
const {
  IDLE_WEIGHT,
  MAX_STOPS,
  canOptimize,
  headlinersSkipped,
  headlinersToAdd,
  movableEntries,
  optimizeDay,
  scoreCurrent,
  scoreOrder,
} = optimizeModule as any;

/** The browser's signature, whose third argument was the pixel scale this port has no use for. */
function buildDayGrid(
  openHour: any,
  closeHour: any,
  _pxPerMin?: any,
  earlyOpenMin: any = null,
): any {
  return gridModule.buildDayGrid(openHour, closeHour, earlyOpenMin);
}

/** The browser store's `applyPlan`, reduced to what the vectors read: the day's entries. */
function applyPlan(state: any, params: any): any {
  const park = state.parks[params.parkSlug];
  const day = park.days[params.date];
  let minted = 0;
  const entries = planEntries(day.entries, params.stops).map((entry) =>
    entry.id === null ? { ...entry, id: `added-${minted++}` } : entry,
  );
  return {
    ...state,
    parks: {
      ...state.parks,
      [params.parkSlug]: {
        ...park,
        days: { ...park.days, [params.date]: { ...day, entries } },
      },
    },
  };
}

/**
 * The day optimiser, against a truth it cannot argue with.
 *
 * The interesting assertion in here is the first one and it is the reason the
 * file exists: `optimizeDay` is a HEURISTIC — a beam search plus a local search
 * — so "it returns a plan" proves nothing. For every instance small enough to
 * enumerate, this brute-forces all `n!` orders through the optimiser's own
 * scorer and asserts the heuristic found one of the optima. Eight rides is
 * 40,320 orders, which a test can afford and a click cannot.
 *
 * The rest pin the properties a visitor would notice if they broke: a lunch
 * break stays where it was put, a ticked-off ride is not re-planned, a ride that
 * opens at 11:00 is not queued for at 09:15, pressing the button twice does
 * nothing the second time, and the same day always produces the same plan.
 *
 * Two of them are here because the assertion they replace was TRUE and proved
 * nothing. "No ride is laid across the lunch break" ran against a plan that
 * ended at 11:45 with the break at 13:00, so it held before anything was
 * scheduled — §4 now uses two fixed blocks and a ride whose block grows across
 * an hour boundary, which is the case that was actually broken. And
 * `uncertaintyMinutes` was set on no fixture in this file, so every span in it
 * was the bare queue and nothing exercised a band at all. That is §4b, which
 * since PAR-169 pins the two apart: the block is DRAWN at wait plus band, the
 * plan is BUILT on the wait alone, and the search and the incumbent measure it
 * the same way.
 *
 *     pnpm test:planner-optimize
 *     BENCH=1 pnpm test:planner-optimize   # plus the timing table for §16
 */

const failures: string[] = [];
function check(name: string, ok: unknown, detail: unknown = ""): any {
  if (!ok) failures.push(detail ? `${name} — ${String(detail)}` : name);
}

const OPEN = 9;
/**
 * The fixture park's closing HOUR, and it moved with the meaning of the field.
 *
 * `closeHour` is the hour the park's closing time falls in, so `20` used to
 * produce a `closeMin` of 21:00 — `buildDayGrid` added sixty minutes to it —
 * and every figure in this file was computed against a twelve-hour day. The
 * grid stopped adding that hour (it was an hour of park that does not exist,
 * and the optimiser filled it), so this is 21 to keep the same twelve hours and
 * leave every assertion below measuring what it was written to measure.
 *
 * §18 is where the new rule is pinned, against its own fixture.
 */
const CLOSE = 21;

/**
 * A ride whose curve is given per hour, with coordinates so the transfer model
 * has something to work with.
 *
 * The metres are deliberately real-ish: 0.001° of latitude is about 111 m, so a
 * ride at `+0.004` is ~440 m away — one end of Phantasialand to the other.
 */
function ride(slug?: any, waits?: any, options: any = {}): any {
  // Every hour the curve names, rather than the default park's own window: a
  // fixture about a park open until 22:00 has points out there and they are its
  // whole point. `waits` decides, so nothing above this changes.
  const hours: any[] = [];
  for (let hour = 0; hour < 24; hour++) {
    const wait = waits[hour];
    if (wait !== undefined) hours.push({ hour, wait });
  }
  return {
    attractionSlug: slug,
    attractionName: slug,
    hours,
    dayPeak: Math.max(0, ...(Object.values(waits) as number[])),
    sampleDays: 90,
    latitude: options.lat ?? 50.8,
    longitude: options.lng ?? 6.87,
    land: options.land ?? "Berlin",
    uncertaintyMinutes: options.uncertainty ?? null,
    opensAt: options.opensAt ?? null,
    isHeadliner: options.headliner ?? false,
    minimumHeight: options.minimumHeight,
    mayGetWet: options.mayGetWet,
  };
}

/** A flat curve, so a ride's cost depends only on which hour it lands in. */
function flat(value?: any, open: any = OPEN, close: any = CLOSE): any {
  const out: any = {};
  for (let hour = open; hour <= close; hour++) out[hour] = value;
  return out;
}

/** What a block OCCUPIES at a minute — the queue plus the model's own spread. */
function spanOf(payload?: any, slug?: any, startMinute?: any): any {
  return Math.max(
    occupiedMinutes(payload, { id: "", attractionSlug: slug, startMinute }),
    15,
  );
}

/**
 * The same, band excluded — what the optimiser actually schedules against.
 *
 * The two must not be mixed up in an assertion: `spanOf` is the DRAWN height
 * and belongs in §4b's statement about the block, while every re-derivation of
 * the plan's own arithmetic (when a stop frees up, where the day ends, how much
 * standing about it leaves) has to use this one, or the check re-computes with
 * a longer block than `optimizeDay` filed. Both were `spanOf` until PAR-169
 * split the two, and both passed only because their fixtures carry no band.
 */
function plannedSpanOf(payload?: any, slug?: any, startMinute?: any): any {
  return Math.max(
    plannedMinutes(payload, { id: "", attractionSlug: slug, startMinute }),
    15,
  );
}

/** Low early, high after lunch — the shape a headliner actually has. */
function morningRide(low?: any, high?: any, turnHour: any = 12): any {
  const out: any = {};
  for (let hour = OPEN; hour <= CLOSE; hour++)
    out[hour] = hour < turnHour ? low : high;
  return out;
}

function day(rides?: any, context: any = {}): any {
  return {
    parkSlug: "test-park",
    timezone: "Europe/Berlin",
    tier: "measured",
    leadDays: 1,
    rides,
    shows: [],
    context: {
      date: "2026-09-05",
      status: "OPERATING",
      openHour: OPEN,
      closeHour: CLOSE,
      isHoliday: false,
      isBridgeDay: false,
      isSchoolVacation: false,
      isWeekend: false,
      ...context,
    },
  };
}

function entry(id?: any, slug?: any, startMinute?: any, extra: any = {}): any {
  return {
    id,
    attractionSlug: slug,
    attractionName: slug,
    startMinute,
    ...extra,
  };
}

const grid = (payload: any) =>
  buildDayGrid(payload.context.openHour, payload.context.closeHour);

// ── 1. Optimality, against every permutation ────────────────────────────────
//
// The heuristic's answer is compared to the best of all n! orders scored by the
// same scorer, which is what makes the comparison meaningful: if the two ever
// disagree it is the search that is wrong, not the model.

function costOf(plan?: any): any {
  // `overflowKey`, not the flat `overflow` count. The two agreed while overflow
  // was one number and stopped agreeing the day it grew tiers: `better` ranks
  // by which rides a plan gives up, this mirror ranked only by how many, so it
  // would call a plan optimal that drops the park's flagship to keep a filler.
  // `scoreOrder` hands the key out for exactly this reason.
  return [
    plan.overflowKey ?? plan.overflow,
    plan.totalWaitMinutes + IDLE_WEIGHT * plan.idleMinutes,
    plan.endMinute,
  ];
}

function permutations(items?: any): any {
  if (items.length <= 1) return [items];
  const out: any[] = [];
  for (let i = 0; i < items.length; i++) {
    const rest = [...items.slice(0, i), ...items.slice(i + 1)];
    for (const tail of permutations(rest)) out.push([items[i], ...tail]);
  }
  return out;
}

/**
 * The best of every order, scored by `scoreOrder`.
 *
 * `scoreOrder` is the optimiser's own scheduler and NOT its search, which is
 * what makes this a check rather than a tautology: running the search over each
 * permutation would be the search marking its own homework. What is being
 * proved is that the beam plus the local search find the order a full
 * enumeration would — the scheduling of one order, including its per-stop delay
 * decision, is shared by both sides on purpose.
 */
function bruteForce(input?: any, slugs?: any): any {
  let best: any = null;
  for (const order of permutations(slugs)) {
    const scored = scoreOrder(input, order);
    if (!scored) continue;
    const candidate = costOf(scored);
    if (best === null || lex(candidate) < lex(best)) best = candidate;
  }
  return best;
}

/**
 * Lexicographic as one comparable number: overflow ≫ cost ≫ clock.
 *
 * It has to be `better`'s order, and `IDLE_WEIGHT` is imported rather than
 * written down again for the same reason: a brute force that ranks orders
 * differently from the thing it is checking proves nothing about it. This
 * mirror was one revision behind once and it did not fail loudly — it failed by
 * calling a worse plan optimal.
 */
function lex([overflow, cost, end]: any): any {
  // The overflow key runs to ~2.6e7 with a full budget, so it needs the room:
  // at 1e8 a plan that gives up one more ride could be outranked by one that
  // queues four hours less, which is the opposite of what `better` does.
  return overflow * 1e12 + cost * 1e4 + end;
}

/**
 * A finished plan's holes, added up from the stops themselves.
 *
 * The point of computing it a second way: `idleMinutes` is the figure the
 * optimiser chose the plan ON, so a test that reads it back is asking the
 * search whether it agrees with itself. This walks the stops instead — the
 * block height from `occupiedMinutes`, the walk from `transferBetween` — and is
 * the plan as a visitor would time it. The base is the arrival snapped up to
 * the quarter hour, which is the rule in `idleFor`: a start on the grid is not
 * standing about.
 */
function idleOfPlan(payload?: any, stops?: any): any {
  const rideOf = (slug: any) =>
    payload.rides.find((r: any) => r.attractionSlug === slug) ?? null;
  let idle = 0;
  for (let i = 1; i < stops.length; i++) {
    const previous = stops[i - 1];
    const free =
      previous.startMinute +
      plannedSpanOf(payload, previous.attractionSlug, previous.startMinute);
    const transfer = transferBetween(
      rideOf(previous.attractionSlug),
      rideOf(stops[i].attractionSlug),
    ).ceilingMinutes;
    idle += Math.max(
      0,
      stops[i].startMinute - Math.ceil((free + transfer) / 15) * 15,
    );
  }
  return idle;
}

{
  // Seven rides across three lands, with curves that pull in different
  // directions: two collapse after lunch, two are worst in the morning, one is
  // flat and two are ordinary.
  const rides = [
    ride("alpha", morningRide(10, 70), { lat: 50.8, lng: 6.87, land: "A" }),
    ride("beta", morningRide(15, 60), { lat: 50.803, lng: 6.874, land: "B" }),
    ride("gamma", morningRide(65, 15), { lat: 50.798, lng: 6.866, land: "A" }),
    ride("delta", morningRide(55, 20), { lat: 50.806, lng: 6.879, land: "C" }),
    ride("epsilon", flat(30), { lat: 50.801, lng: 6.871, land: "B" }),
    ride("zeta", morningRide(20, 45), { lat: 50.797, lng: 6.881, land: "C" }),
    ride("eta", morningRide(40, 25), { lat: 50.805, lng: 6.864, land: "A" }),
  ];
  const payload = day(rides);
  const slugs = rides.map((r: any) => r.attractionSlug);

  // The heuristic, from a deliberately bad starting order (every ride at its
  // worst hour, in alphabetical sequence).
  const entries = slugs.map((slug: any, index: any) =>
    entry(`${slug}-1`, slug, 13 * 60 + index * 30),
  );
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  const optimum = bruteForce(
    { day: payload, grid: grid(payload), entries },
    slugs,
  );

  check(
    "die Heuristik findet bei sieben Bahnen die beste Reihenfolge",
    found !== null && lex(costOf(found)) <= lex(optimum),
    found
      ? `gefunden ${JSON.stringify(costOf(found))}, optimal ${JSON.stringify(optimum)}`
      : "kein Plan",
  );
}

{
  // Five rides, all in one land, so the ordering is decided by the curves alone
  // and a wrong answer cannot hide behind the walking model.
  const rides = [
    ride("a", morningRide(5, 80), { land: "X" }),
    ride("b", morningRide(80, 5), { land: "X" }),
    ride("c", flat(25), { land: "X" }),
    ride("d", morningRide(10, 50), { land: "X" }),
    ride("e", morningRide(50, 10), { land: "X" }),
  ];
  const payload = day(rides);
  const slugs = rides.map((r: any) => r.attractionSlug);
  const entries = slugs.map((slug: any, index: any) =>
    entry(`${slug}-1`, slug, OPEN * 60 + index * 45),
  );
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  const optimum = bruteForce(
    { day: payload, grid: grid(payload), entries },
    slugs,
  );

  check(
    "die Heuristik findet bei fünf Bahnen die beste Reihenfolge",
    found !== null && lex(costOf(found)) <= lex(optimum),
    found
      ? `gefunden ${JSON.stringify(costOf(found))}, optimal ${JSON.stringify(optimum)}`
      : "kein Plan",
  );

  // And it does the obvious thing: the two rides that collapse after lunch go
  // last, the two that are cheap in the morning go first.
  const order = found.stops.map((s: any) => s.attractionSlug);
  check(
    "die Morgenbahnen stehen vor den Nachmittagsbahnen",
    order.indexOf("a") < order.indexOf("b") &&
      order.indexOf("d") < order.indexOf("e"),
    order.join(" → "),
  );
}

// ── 1b. …including the question of WHICH rides are given up ────────────────
//
// §1 never passes `add`, so for as long as the overflow tiers have existed
// nothing has enumerated a day that cannot hold what it is asked for — the one
// case the tiers were built for. `costOf` reads `overflowKey` now, so the
// mirror ranks by which rides a plan loses and not merely by how many, and the
// plan comes back through `scoreOrder` in its own order with the rides it gave
// up on the end: that is the order the search actually chose, scored by the
// same scheduler the enumeration uses.
{
  const rides = [
    ride("own1", flat(25, 9, 12), { lat: 50.8, lng: 6.87, land: "A" }),
    ride("own2", flat(25, 9, 12), { lat: 50.8004, lng: 6.87, land: "A" }),
    ride("draw", flat(70, 9, 12), {
      lat: 50.8008,
      lng: 6.87,
      land: "A",
      headliner: true,
    }),
    ride("mid", flat(45, 9, 12), {
      lat: 50.8012,
      lng: 6.87,
      land: "A",
      headliner: true,
    }),
    ride("filler", flat(15, 9, 12), {
      lat: 50.8016,
      lng: 6.87,
      land: "A",
      headliner: true,
    }),
  ];
  const payload = day(rides, { openHour: 9, closeHour: 12 });
  const entries = [
    entry("own1-1", "own1", 9 * 60),
    entry("own2-1", "own2", 10 * 60),
  ];
  const add = [rides[2], rides[3], rides[4]];
  const input = { day: payload, grid: grid(payload), entries, add };
  const slugs = rides.map((r: any) => r.attractionSlug);

  const found = optimizeDay(input);
  const placed = [...found.stops]
    .sort((a: any, b: any) => a.startMinute - b.startMinute)
    .map((s: any) => s.attractionSlug);
  const chosen = scoreOrder(input, [
    ...placed,
    ...slugs.filter((s: any) => !placed.includes(s)),
  ]);
  const best = bruteForce(input, slugs);

  check(
    "die Heuristik gibt dieselbe Bahn auf wie die Voll-Enumeration",
    chosen !== null && best !== null && lex(costOf(chosen)) === lex(best),
    `${placed.join(" → ")} | key ${chosen?.overflowKey} gegen ${best?.[0]}`,
  );
  // And the ride nobody would give up is in it, stated separately so a
  // regression says WHAT went wrong rather than only that a number moved.
  check(
    "und behält dabei den Headliner mit der längsten Schlange",
    placed.includes("draw"),
    placed.join(" → "),
  );
}

// ── 2. Rope drop falls out of the data rather than being wired in ───────────
{
  const rides = [
    ride("headliner", morningRide(10, 90, 10), { land: "X", headliner: true }),
    ride("filler-1", flat(20), { land: "X" }),
    ride("filler-2", flat(20), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [
    entry("headliner-1", "headliner", 15 * 60),
    entry("filler-1-1", "filler-1", 9 * 60),
    entry("filler-2-1", "filler-2", 10 * 60),
  ];
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  check(
    "die Bahn mit dem steilsten Morgen wird zuerst geplant",
    found?.stops[0]?.attractionSlug === "headliner",
    found?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}`)
      .join(" → ") ?? "kein Plan",
  );
  check(
    "und zwar an ihrer eigenen Untergrenze, nicht zur Parköffnung",
    // `rideFloor` adds GATE_TO_FIRST_RIDE_MIN for a ride that opens with the
    // park: 09:00 + 15 min walk from the gates.
    found?.stops[0]?.startMinute === OPEN * 60 + 15,
    String(found?.stops[0]?.startMinute),
  );
}

// A park where the morning is NOT the answer: the same code must not recommend
// rope drop where the data says the queue builds later and falls at closing.
{
  const rides = [
    ride("evening", morningRide(70, 10, 17), { land: "X" }),
    ride("flat-1", flat(20), { land: "X" }),
    ride("flat-2", flat(20), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [
    entry("evening-1", "evening", 9 * 60),
    entry("flat-1-1", "flat-1", 11 * 60),
    entry("flat-2-1", "flat-2", 13 * 60),
  ];
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  check(
    "eine Abendbahn wird nicht in den Morgen gezogen",
    found !== null && found.stops.at(-1)?.attractionSlug === "evening",
    found?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}`)
      .join(" → ") ?? "kein Plan",
  );
}

// ── 3. Waiting is allowed, but only when it pays for itself ─────────────────
{
  // One ride whose queue collapses at 11:00, plus a filler. Reached at 09:45 it
  // is a choice between 100 minutes of queue now (free at 11:25) and 75 minutes
  // of nothing followed by 5 (free at 11:05). The second is better BECAUSE it
  // finishes earlier, which is the entire rule.
  const rides = [
    ride("cliff", morningRide(100, 5, 11), { land: "X" }),
    ride("filler", flat(15), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [
    entry("cliff-1", "cliff", 9 * 60),
    entry("filler-1", "filler", 10 * 60),
  ];
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  const cliff = found?.stops.find((s: any) => s.attractionSlug === "cliff");
  check(
    "auf einen einbrechenden Andrang wird gewartet, wenn das früher fertig macht",
    cliff !== undefined && cliff.startMinute >= 11 * 60,
    found?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}=${s.waitMinutes}`)
      .join(" → ") ?? "kein Plan",
  );
  check(
    "und das Warten macht den Tag nicht länger",
    found !== null && found.endMinute <= 11 * 60 + 20,
    String(found?.endMinute),
  );

  // The other half of the same rule, and it is what keeps the optimiser from
  // sending somebody for a four-hour coffee: a collapse too far out can never
  // pay for itself, so the plan takes the queue it has.
  const late = day([
    ride("far-cliff", morningRide(100, 5, 15), { land: "X" }),
    ride("filler", flat(15), { land: "X" }),
  ]);
  const lateFound = optimizeDay({
    day: late,
    grid: grid(late),
    entries: [
      entry("far-cliff-1", "far-cliff", 12 * 60),
      entry("filler-1", "filler", 9 * 60),
    ],
  });
  const far = lateFound?.stops.find(
    (s: any) => s.attractionSlug === "far-cliff",
  );
  check(
    "auf einen Einbruch in fünf Stunden wird nicht gewartet",
    far === undefined || far.startMinute < 15 * 60,
    lateFound?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}`)
      .join(" → ") ?? "kein Plan",
  );
}

// ── 4. What it may not touch ────────────────────────────────────────────────
//
// TWO fixed blocks, and a ride whose block grows across an hour boundary: 20
// minutes at 13:00, 90 from 14:00. That shape is the whole case, and the
// assertion that stood here could not see it — one lunch break at 13:00 against
// a plan that ended at 11:45, so "no ride is laid across the lunch break" was
// true before anything had been scheduled. The bug it existed to catch was
// real: pushed off the 13:00 lunch, the block came back ninety minutes long,
// was never measured again at its new length, and lay straight across the
// 15:00 parade.
//
// The span comes from the app's own `occupiedMinutes` rather than from a guess
// in this file, so what is being tested is the height a visitor will see.
{
  const growing: any = {};
  for (let hour = 13; hour <= CLOSE; hour++)
    growing[hour] = hour >= 14 ? 90 : 20;

  const rides = [
    ride("afternoon", growing, { land: "X" }),
    ride("a", morningRide(10, 60), { land: "X" }),
    ride("b", morningRide(60, 10), { land: "X" }),
    ride("c", flat(20), { land: "X" }),
  ];
  const payload = day(rides);
  const blocks = [
    { label: "Mittag", from: 13 * 60, to: 14 * 60 },
    { label: "Parade", from: 15 * 60, to: 15 * 60 + 30 },
  ];
  const entries = [
    entry("afternoon-1", "afternoon", 16 * 60),
    entry("a-1", "a", 15 * 60),
    entry("b-1", "b", 9 * 60),
    entry("c-1", "c", 11 * 60),
    {
      id: "lunch-1",
      startMinute: 13 * 60,
      custom: { label: "Mittag", icon: "food", durationMinutes: 60 },
    },
    {
      id: "parade-1",
      startMinute: 15 * 60,
      custom: { label: "Parade", icon: "show", durationMinutes: 30 },
    },
    entry("done-1", "a", 10 * 60, { done: true, actualWait: 25 }),
  ];
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });

  check(
    "ein Freiblock und eine abgehakte Bahn tauchen im Plan nicht auf",
    found !== null &&
      found.stops.every(
        (s: any) =>
          s.entryId !== "lunch-1" &&
          s.entryId !== "parade-1" &&
          s.entryId !== "done-1",
      ),
    found?.stops.map((s: any) => s.entryId).join(", ") ?? "kein Plan",
  );

  const clashes: any[] = [];
  for (const stop of found.stops) {
    const span = plannedSpanOf(payload, stop.attractionSlug, stop.startMinute);
    for (const block of blocks) {
      if (stop.startMinute < block.to && stop.startMinute + span > block.from) {
        clashes.push(
          `${stop.attractionSlug} ${stop.startMinute}+${span} über ${block.label}`,
        );
      }
    }
  }
  check(
    "und keine Bahn wird über einen festen Block gelegt",
    clashes.length === 0,
    clashes.join(", "),
  );

  // Without this the assertion above could be true because nothing came near
  // either block, which is exactly how the old one passed for a year.
  const late = found.stops.find((s: any) => s.attractionSlug === "afternoon");
  check(
    "der wachsende Block liegt auch wirklich im Nachmittag",
    late !== undefined &&
      late.startMinute >= 13 * 60 &&
      plannedSpanOf(payload, "afternoon", late.startMinute) === 90,
    late
      ? `${late.startMinute} + ${plannedSpanOf(payload, "afternoon", late.startMinute)}`
      : "nicht geplant",
  );
}

// ── 4b. Der Block ist einen Puffer höher, als der Plan reserviert ───────────
//
// Zwei Zahlen an einer Bahn, und der Unterschied ist keine Feinheit.
// `occupiedMinutes` ist die GEZEICHNETE Höhe: Warteschlange plus das eigene Band
// des Modells. `plannedMinutes` ist das, wogegen der Optimierer taktet, und das
// ist die Warteschlange allein.
//
// Die Richtung war bis PAR-169 andersherum, und sie war falsch:
// `uncertaintyMinutes` ist eine halbe Bandbreite UM die Vorhersage, kein
// Zuschlag darauf, also plante die Uhr jeden Stopp gegen das pessimistische Ende
// jeder Schlange, während `waitBySlot` und jede Kostenfunktion daneben den
// Erwartungswert lasen. An der echten Phantasialand-Nutzlast (2026-09-13, Band
// 29 Minuten auf Taron, 36–38 auf F.L.Y. und beide Winja's) schob das den Tag
// von 16:40 auf 18:54 und warf einen von zehn Headlinern hinaus.
//
// Und `legBetween` misst die Lücke seit jeher gegen `wait`, nicht gegen
// `wait + Band` — der Optimierer taktet jetzt also so, wie das einzige Urteil
// dieser App rechnet, das einen Plan „unmöglich" nennt.
//
// Das gilt für die Sprosse `broken` (gegen den Transfer-Floor) und ausdrücklich
// NICHT für `tight`: die Sprosse liegt bei `slack < uncertaintyMinutes`, und
// weil die Suche gegen das Transfer-CEILING baut, ist der Slack eines
// gepackten Tages ~0. Auf einem Tag mit Band liest sich damit fast jeder
// Leg-Chip als „Umstieg knapp" — auf dem Phantasialand-Tag gemessen
// (`gap=15 ceil=12 band=29`, `gap=10 ceil=9 band=36`). Vorher war es
// spiegelbildlich: die Uhr polsterte jede Lücke um das Band, also stand dort
// immer „gut". Ob die Schwelle mitziehen soll, ist eine Produktfrage und liegt
// als Beifang daneben; hier wird sie nur nicht behauptet.
{
  const rides = [
    ride("wide-a", flat(30), { land: "X", uncertainty: 25, lat: 50.8 }),
    ride("wide-b", flat(30), { land: "X", uncertainty: 25, lat: 50.8005 }),
  ];
  const payload = day(rides);
  check(
    "die Unsicherheit macht den gezeichneten Block breiter als die Warteschlange",
    spanOf(payload, "wide-a", 10 * 60) === 55,
    String(spanOf(payload, "wide-a", 10 * 60)),
  );
  check(
    "geplant wird trotzdem gegen die Warteschlange allein",
    plannedMinutes(payload, {
      id: "",
      attractionSlug: "wide-a",
      startMinute: 10 * 60,
    }) === 30,
    String(
      plannedMinutes(payload, {
        id: "",
        attractionSlug: "wide-a",
        startMinute: 10 * 60,
      }),
    ),
  );

  // Zwei Blöcke vierzig Minuten auseinander, jeder mit dreißig Minuten
  // Schlange: die zehn Minuten dazwischen tragen den Transfer, `legBetween`
  // nennt den Tag begehbar — also hat der Knopf daran nichts zu sortieren. Die
  // gezeichneten Bänder überlappen sich dabei; ein Band ist ein Intervall, in
  // dem der wahre Wert liegt, und kein Termin, den jemand einhalten muss.
  const entries = [
    entry("wide-a-1", "wide-a", 9 * 60 + 15),
    entry("wide-b-1", "wide-b", 9 * 60 + 55),
  ];
  const gapMinutes = 40 - 30;
  const floor = transferBetween(rides[0], rides[1]).floorMinutes;
  check(
    "die Lücke trägt den Transfer-Floor, gemessen wie im Leg-Chip",
    gapMinutes >= floor,
    `${gapMinutes} Minuten gegen einen Floor von ${floor}`,
  );
  check(
    "und ein so gebauter Tag gilt als fertig sortiert",
    optimizeDay({ day: payload, grid: grid(payload), entries }) === null,
    "optimizeDay wollte umstellen",
  );

  // Die Zusicherung, an der beides hängt: Suche und Ist-Plan messen einen Block
  // gleich. Täte der Ist-Plan es gegen die längere Spanne, läse sich jeder
  // frisch erzeugte Plan als überlappend und der Knopf stellte den Tag bei jedem
  // Druck neu um — für immer.
  const sparse = [
    entry("wide-a-1", "wide-a", 9 * 60 + 15),
    entry("wide-b-1", "wide-b", 14 * 60),
  ];
  const first = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries: sparse,
  });
  const after = sparse.map((row: any) => {
    const stop = first?.stops.find((s: any) => s.entryId === row.id);
    return stop ? { ...row, startMinute: stop.startMinute } : row;
  });
  check(
    "ein Tag mit Band wird einmal umgestellt",
    first !== null,
    "optimizeDay lieferte null",
  );
  check(
    "und der erzeugte Plan hält dem eigenen Maßstab stand",
    optimizeDay({ day: payload, grid: grid(payload), entries: after }) === null,
    "zweiter Druck stellte denselben Tag erneut um",
  );
}

// ── 5. A ride that opens later is not queued for before it opens ────────────
{
  const rides = [
    ride("late", flat(20), { land: "X", opensAt: "11:00" }),
    ride("early", flat(20), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [
    entry("late-1", "late", 9 * 60),
    entry("early-1", "early", 12 * 60),
  ];
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  const late = found?.stops.find((s: any) => s.attractionSlug === "late");
  check(
    "eine Bahn, die erst um 11 öffnet, wird nicht um 9 eingeplant",
    late === undefined || late.startMinute >= 11 * 60,
    found?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}`)
      .join(" → ") ?? "kein Plan",
  );
}

// ── 6. Pressing it twice does nothing the second time ───────────────────────
{
  const rides = [
    ride("a", morningRide(10, 60), { land: "X" }),
    ride("b", morningRide(60, 10), { land: "X" }),
    ride("c", flat(25), { land: "Y", lat: 50.806 }),
    ride("d", morningRide(35, 20), { land: "Y", lat: 50.807 }),
  ];
  const payload = day(rides);
  const entries = [
    entry("a-1", "a", 16 * 60),
    entry("b-1", "b", 9 * 60),
    entry("c-1", "c", 12 * 60),
    entry("d-1", "d", 10 * 60),
  ];
  const first = optimizeDay({ day: payload, grid: grid(payload), entries });
  check("ein schlecht sortierter Tag wird sortiert", first !== null);

  const applied = first.stops.map((stop: any) =>
    entry(stop.entryId, stop.attractionSlug, stop.startMinute),
  );
  const second = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries: applied,
  });
  check(
    "ein zweites Drücken ändert nichts mehr",
    second === null,
    second
      ? second.stops
          .map((s: any) => `${s.attractionSlug}@${s.startMinute}`)
          .join(" → ")
      : "",
  );

  // Determinism: the same input twice, the same plan twice.
  const again = optimizeDay({ day: payload, grid: grid(payload), entries });
  check(
    "derselbe Tag ergibt denselben Plan",
    JSON.stringify(again?.stops) === JSON.stringify(first.stops),
  );
}

// ── 7. Headliners, after the party filter ───────────────────────────────────
{
  const rides = [
    ride("big-1", morningRide(20, 70), {
      land: "X",
      headliner: true,
      minimumHeight: 140,
    }),
    ride("big-2", morningRide(30, 60), {
      land: "X",
      headliner: true,
      mayGetWet: true,
    }),
    ride("big-3", morningRide(25, 55), { land: "X", headliner: true }),
    ride("small", flat(10), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [entry("small-1", "small", 10 * 60)];

  const all = headlinersToAdd(payload, entries, undefined);
  check(
    "ohne Angaben zur Gruppe kommen alle Headliner infrage",
    all.length === 3,
  );

  const withChild = headlinersToAdd(payload, entries, {
    riderHeightCm: 120,
    avoidWet: true,
  });
  check(
    "zu klein und nass fallen nach dem Filter raus",
    withChild.length === 1 && withChild[0].attractionSlug === "big-3",
    withChild.map((r: any) => r.attractionSlug).join(", "),
  );
  check(
    "und die Übersprungenen werden gezählt",
    headlinersSkipped(payload, entries, {
      riderHeightCm: 120,
      avoidWet: true,
    }) === 2,
  );

  const planned = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries,
    add: all,
  });
  check(
    "alle Headliner landen im Plan",
    planned !== null &&
      ["big-1", "big-2", "big-3"].every((slug: any) =>
        planned.stops.some((stop: any) => stop.attractionSlug === slug),
      ),
    planned?.stops.map((s: any) => s.attractionSlug).join(" → ") ?? "kein Plan",
  );
  check(
    "die bereits geplante Bahn bleibt dabei ein bestehender Eintrag",
    planned.stops.find((s: any) => s.attractionSlug === "small")?.entryId ===
      "small-1",
  );
  check(
    "und ein neu eingefügter trägt noch keine ID",
    planned.stops.find((s: any) => s.attractionSlug === "big-1")?.entryId ===
      null,
  );
}

// ── 8. A park nobody can read gets no button ────────────────────────────────
{
  const payload = day([ride("a", flat(20)), ride("b", flat(20))], {
    liveWaitTimes: { available: false, reason: "app-only" },
  });
  check(
    "ein Park ohne lesbare Wartezeiten wird nicht optimiert",
    !canOptimize(payload, grid(payload)),
  );
  check(
    "und optimizeDay verweigert dort",
    optimizeDay({
      day: payload,
      grid: grid(payload),
      entries: [entry("a-1", "a", 10 * 60), entry("b-1", "b", 11 * 60)],
    }) === null,
  );

  const readable = day([ride("a", flat(20)), ride("b", flat(20))]);
  check(
    "ein Park mit Wartezeiten schon",
    canOptimize(readable, grid(readable)),
  );
}

// ── 9. A day that cannot fit everything says so ─────────────────────────────
{
  // Five rides of 150 minutes each against an eleven-hour day: four fit and one
  // cannot, and what must NOT happen is a silently dropped ride. They are strung
  // out along one axis so the order decides how much walking is paid for, which
  // is what makes one arrangement fit more of them than another.
  const rides = Array.from({ length: 5 }, (_: any, i: any) =>
    ride(`long-${i}`, flat(150), { land: "X", lat: 50.8 + i * 0.006 }),
  );
  const payload = day(rides);
  // Deliberately zig-zagging: 0 → 4 → 1 → 3 → 2 crosses the park four times.
  const zigzag = [0, 4, 1, 3, 2];
  const entries = zigzag.map((index: any, position: any) =>
    entry(`long-${index}-1`, `long-${index}`, 9 * 60 + position * 30),
  );
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });
  check(
    "kein Eintrag geht verloren, wenn der Tag zu kurz ist",
    found?.stops.length === 5,
  );
  check(
    "und der Überlauf wird gemeldet",
    (found?.overflow ?? 0) > 0,
    String(found?.overflow),
  );
  check(
    "der Zickzack wird begradigt",
    found !== null &&
      lex(costOf(found)) <=
        lex(
          costOf(
            scoreOrder(
              { day: payload, grid: grid(payload), entries },
              zigzag.map((i: any) => `long-${i}`),
            ),
          ),
        ),
  );
}

// ── 10. Before and after are produced by one function ───────────────────────
{
  // `a` is cheap before 11 and `b` after it. Ridden in the wrong order both cost
  // 90; in the right one `a` costs 10 and the wait for `b` pays for itself.
  const rides = [
    ride("a", morningRide(10, 90, 11), { land: "X" }),
    ride("b", morningRide(90, 10, 11), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [entry("a-1", "a", 12 * 60), entry("b-1", "b", 9 * 60)];
  const before = scoreCurrent({ day: payload, grid: grid(payload), entries });
  const after = optimizeDay({ day: payload, grid: grid(payload), entries });
  check(
    "der Vorher-Wert ist die Summe der aktuellen Stunden",
    before?.totalWaitMinutes === 180,
    String(before?.totalWaitMinutes),
  );
  check(
    "und der Nachher-Wert ist kleiner",
    after !== null && after.totalWaitMinutes < before.totalWaitMinutes,
    `${before?.totalWaitMinutes} → ${after?.totalWaitMinutes}`,
  );
}

// ── 11. Waiting is bought with idle minutes, and there is a price ──────────
//
// One ride costing 90 minutes before 11:00 and 20 after it, plus a ten-minute
// filler. Both plans ride the filler first; the question is what to do with the
// hour after it:
//
//     A  queue at once            100 queued    0 idle   done 11:15
//     B  wait for 11:00            30 queued   75 idle   done 11:20
//
// B is what a visitor would do and no lexicographic order can produce it: rank
// the clock first and A wins by five minutes, five minutes that cost seventy in
// a queue. This is the upper half of the bracket around IDLE_WEIGHT — B wins
// while 30 + k×75 < 100, i.e. k < 14/15 — and §17 is the lower half, a case
// that runs the other way. The one that used to stand here claimed the delay
// was weighed against the queue and not against the clock, which was the same
// mistake in the other direction and is why §17 exists.
{
  const rides = [
    ride("cliff", morningRide(90, 20, 11), { land: "X" }),
    ride("filler", flat(10), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = [
    entry("cliff-1", "cliff", 9 * 60 + 15),
    entry("filler-1", "filler", 11 * 60),
  ];
  const input = { day: payload, grid: grid(payload), entries };

  const before = scoreCurrent(input);
  const found = optimizeDay(input);
  check(
    "für einen Einbruch um 11 Uhr wird eine Stunde Leerlauf in Kauf genommen",
    found !== null && found.totalWaitMinutes === 30 && found.idleMinutes === 75,
    found
      ? `${found.totalWaitMinutes} Min. Schlange, ${found.idleMinutes} Min. Leerlauf`
      : "kein Plan",
  );
  check(
    "und siebzig Warteminuten sind fünf Minuten später Feierabend wert",
    found !== null &&
      before.totalWaitMinutes - found.totalWaitMinutes === 70 &&
      found.endMinute - before.endMinute === 5,
    `${before.totalWaitMinutes}/${before.endMinute} → ${found?.totalWaitMinutes}/${found?.endMinute}`,
  );

  // `scoreOrder` sees the same choice, which is what makes the brute force above
  // a statement about the whole plan space rather than about the orders alone.
  check(
    "scoreOrder rechnet mit derselben Wahl",
    scoreOrder(input, ["filler", "cliff"])?.totalWaitMinutes === 30,
    JSON.stringify(scoreOrder(input, ["filler", "cliff"])),
  );
}

// ── 12. The budget is finite, and the report has to match the plan ─────────
{
  const planned = Array.from({ length: 20 }, (_: any, i: any) =>
    ride(`p${i}`, flat(15), { land: "X", lat: 50.8 + i * 0.0005 }),
  );
  const heads = Array.from({ length: 8 }, (_: any, i: any) =>
    ride(`h${i}`, flat(45), {
      land: "X",
      lat: 50.81 + i * 0.0005,
      headliner: true,
    }),
  );
  const payload = day([...planned, ...heads]);
  const entries = planned.map((r: any, i: any) =>
    entry(`${r.attractionSlug}-1`, r.attractionSlug, 9 * 60 + i * 15),
  );
  const add = headlinersToAdd(payload, entries, undefined);
  const found = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries,
    add,
  });

  const added = found.stops.filter((s: any) => s.entryId === null).length;
  const homeless = found.stops.filter((s: any) => !s.fits);
  // MAX_STOPS is the CEILING, not the count. Twenty-four candidates reach the
  // search — twenty entries plus four of the eight headliners — and two of
  // those four then find no minute before closing, so they are not added at
  // all: a ride the optimiser was ADDING is never filed past the gate, because
  // "plan every headliner" drew Black Mamba at 19:00 and Taron at 20:00 in a
  // park that shuts at 18:00. That leaves 22.
  check(
    "mehr Stopps als das Budget: der Plan bleibt darunter",
    found.stops.length === 22 && found.stops.length <= MAX_STOPS,
    String(found.stops.length),
  );
  check(
    "die bestehenden Einträge behalten dabei alle ihren Platz",
    found.stops.length - added === entries.length,
    `${found.stops.length - added} von ${entries.length}`,
  );
  // The tier order, on a day where two rides cannot fit whatever is done. Both
  // of them are entries the visitor already had, and that is not a failure of
  // the rule but the rule's own arithmetic: twenty of those do not fit either,
  // so the plan minimises THEM first and then adds only the headliners the
  // remaining minutes hold. What must never happen is the other shape — two
  // added headliners in the hatched hours while two planned rides make room for
  // them — which is exactly what the screenshot showed.
  check(
    "kein hinzugefügter Headliner landet hinter dem Tor",
    added === 2 && homeless.every((s: any) => s.entryId !== null),
    `ergänzt ${added}, davon ohne Platz ${homeless.filter((s: any) => s.entryId === null).length}`,
  );
  // What is left over is still reported rather than swallowed: four never
  // reached the search at all, and `overflow` counts what did not fit once there.
  check(
    "und die Kappung wird gemeldet statt verschwiegen",
    found.capped === 4 && found.overflow === 2,
    `gekappt ${found.capped}, passt nicht ${found.overflow}`,
  );

  // The other half of the same bug: the before-figure covers every entry and the
  // after-figure only what fit, so their difference is not a saving. A day too
  // big for the budget must not produce one.
  const flatDay = day(
    Array.from({ length: 30 }, (_: any, i: any) =>
      ride(`q${i}`, flat(15), { land: "X", lat: 50.8 + i * 0.0005 }),
    ),
  );
  const flatEntries = Array.from({ length: 30 }, (_: any, i: any) =>
    entry(`q${i}-1`, `q${i}`, 9 * 60 + i * 15),
  );
  const flatInput = { day: flatDay, grid: grid(flatDay), entries: flatEntries };
  const flatPlan = optimizeDay(flatInput);
  const flatBefore = scoreCurrent(flatInput);
  check(
    "ein gekappter Tag liefert keine vergleichbare Ersparnis",
    flatPlan.capped === 6 &&
      flatPlan.stops.filter((s: any) => s.entryId !== null).length !==
        flatEntries.length &&
      flatBefore.totalWaitMinutes - flatPlan.totalWaitMinutes === 90,
    `gekappt ${flatPlan.capped}, Differenz ${flatBefore.totalWaitMinutes - flatPlan.totalWaitMinutes} Min. über ${flatPlan.stops.length} statt ${flatEntries.length} Bahnen`,
  );
}

// ── 12b. The clock, for a day that is today ────────────────────────────────
// Reported from a park page at 09:43: the day was sorted and came back with a
// ride at 10:00 and the one before it at 09:15 — a queue nobody can join, since
// the visitor is standing in the park with the morning already gone. The
// optimiser had no clock at all; `clock` is one, and only `today` carries a
// minute. The FLOOR is what this block measures; §12b2 measures the other half,
// which is that a slot the clock has already reached leaves the search
// entirely — and it is what makes this block's second check true for the right
// reason, since `a-1@540` is elapsed at 09:43 rather than merely floored.
{
  const rides = [
    ride("a", flat(10), { land: "X", lat: 50.8 }),
    ride("b", flat(10), { land: "X", lat: 50.8005 }),
    ride("c", flat(10), { land: "X", lat: 50.801 }),
  ];
  const payload = day(rides);
  // Spread across the day with two holes in it, so there is something to tidy:
  // a day that is already compact comes back `null` and would prove nothing
  // about where the clock puts the first block.
  const entries = [
    entry("a-1", "a", 9 * 60),
    entry("b-1", "b", 13 * 60),
    entry("c-1", "c", 16 * 60),
  ];
  const base = { day: payload, grid: grid(payload), entries };

  const withoutClock = optimizeDay(base);
  const withClock = optimizeDay({
    ...base,
    clock: { phase: "today", nowMinute: 9 * 60 + 43 },
  });

  check(
    "ohne heutigen Tag plant der Optimierer wie bisher in den Morgen",
    withoutClock !== null &&
      Math.min(...withoutClock.stops.map((s: any) => s.startMinute)) <
        9 * 60 + 43,
    JSON.stringify(withoutClock?.stops.map((s: any) => s.startMinute)),
  );
  check(
    "mit Uhrzeit liegt kein Stopp vor jetzt",
    withClock !== null &&
      withClock.stops.every((s: any) => s.startMinute >= 9 * 60 + 43),
    JSON.stringify(withClock?.stops.map((s: any) => s.startMinute)),
  );
  // Snapped UP to the grid's step: every start in this app sits on a quarter
  // hour, so 09:43 becomes 09:45 rather than seeding a front 15 minutes apart
  // from an off-grid minute.
  check(
    "und der früheste sitzt auf dem Viertelstundenraster",
    withClock !== null &&
      Math.min(...withClock.stops.map((s: any) => s.startMinute)) ===
        9 * 60 + 45,
    JSON.stringify(withClock?.stops.map((s: any) => s.startMinute)),
  );
  // A clock past closing produces placements that do not fit rather than a
  // crash or an empty plan — not fitting is a thing this plan can say.
  const afterClose = optimizeDay({
    ...base,
    clock: { phase: "today", nowMinute: 23 * 60 },
  });
  check(
    "eine Uhrzeit nach Feierabend liefert einen Plan, der nicht passt",
    afterClose === null || afterClose.overflow === afterClose.stops.length,
    afterClose
      ? `${afterClose.overflow} von ${afterClose.stops.length}`
      : "null",
  );
}

// ── 12b2. An elapsed slot is not re-planned ────────────────────────────────
// Reported after the block above shipped: at 14:00, a day holding a ride at
// 09:00 came back with that ride moved into the afternoon. The floor alone
// cannot answer it — raising a candidate's earliest minute to "now" is exactly
// what moves a morning block forward. So a slot the clock has reached leaves
// the search and joins the fixed blocks, the same place a ticked-off ride and
// a lunch break already sit, and `applyPlan` then never touches its minute.
{
  const rides = [
    ride("a", flat(10), { land: "X", lat: 50.8 }),
    ride("b", flat(10), { land: "X", lat: 50.8005 }),
    ride("c", flat(10), { land: "X", lat: 50.801 }),
  ];
  const payload = day(rides);
  const entries = [
    entry("a-1", "a", 9 * 60),
    entry("b-1", "b", 13 * 60),
    entry("c-1", "c", 16 * 60),
  ];
  const base = { day: payload, grid: grid(payload), entries };
  const atTwo = { ...base, clock: { phase: "today", nowMinute: 14 * 60 } };

  const found = optimizeDay(atTwo);
  check(
    "eine abgelaufene Bahn wird nicht mehr umgeplant",
    found !== null &&
      found.stops.every((s: any) => s.entryId !== "a-1" && s.entryId !== "b-1"),
    JSON.stringify(found?.stops.map((s: any) => [s.entryId, s.startMinute])),
  );

  // The other half, and the one a visitor sees: `applyPlan` moves and adds and
  // never removes, so a stop that is not in the plan keeps the minute it had.
  const date = payload.context.date;
  const state = {
    parks: {
      "test-park": {
        slug: "test-park",
        name: "Test",
        geo: { continent: "europe", country: "germany", city: "test" },
        days: { [date]: { date, entries } },
      },
    },
    activeParkSlug: "test-park",
    activeDate: date,
    version: 1,
  };
  const written = applyPlan(state, {
    parkSlug: "test-park",
    parkName: "Test",
    geo: state.parks["test-park"].geo,
    date,
    stops: found.stops.map((s: any) => ({
      entryId: s.entryId,
      attractionSlug: s.attractionSlug,
      attractionName: s.attractionName,
      startMinute: s.startMinute,
    })),
  });
  const byId = new Map(
    written.parks["test-park"].days[date].entries.map((e: any) => [
      e.id,
      e.startMinute,
    ]),
  );
  check(
    "und behält im Plan die Minute, die sie hatte",
    byId.get("a-1") === 9 * 60 && byId.get("b-1") === 13 * 60,
    `a-1@${byId.get("a-1")}, b-1@${byId.get("b-1")}`,
  );

  // A slot that has STARTED counts, not only one that has finished: at 13:05 the
  // visitor is standing in b's queue, and an optimiser allowed to move that
  // block is telling them to leave it and rejoin.
  const inQueue = optimizeDay({
    ...base,
    clock: { phase: "today", nowMinute: 13 * 60 + 5 },
  });
  check(
    "eine Bahn, in deren Schlange gerade gestanden wird, bleibt auch stehen",
    inQueue !== null && inQueue.stops.every((s: any) => s.entryId !== "b-1"),
    JSON.stringify(inQueue?.stops.map((s: any) => [s.entryId, s.startMinute])),
  );
  // And the ride after it waits until that queue is through — which is what the
  // fixed block buys and a plain exclusion would not.
  const afterQueue = inQueue?.stops.find((s: any) => s.entryId === "c-1");
  check(
    "und die nächste Bahn wartet, bis diese Schlange durch ist",
    afterQueue !== undefined &&
      afterQueue.startMinute >= 13 * 60 + plannedSpanOf(payload, "b", 13 * 60),
    `${afterQueue?.startMinute} gegen ${13 * 60 + plannedSpanOf(payload, "b", 13 * 60)}`,
  );

  // The boundary, and it is strict. A block starting exactly now is somebody
  // arriving, not somebody queueing — and `<=` here had a second cost the UI
  // paid: the floor snaps up to the quarter hour, so a press at 14:00 files the
  // next ride AT 14:00, which would then be elapsed the instant it was planned.
  const onTheDot = optimizeDay({
    ...base,
    clock: { phase: "today", nowMinute: 13 * 60 },
  });
  check(
    "eine Bahn, die genau jetzt anfängt, bleibt planbar",
    onTheDot !== null && onTheDot.stops.some((s: any) => s.entryId === "b-1"),
    JSON.stringify(onTheDot?.stops.map((s: any) => [s.entryId, s.startMinute])),
  );

  // The before-figure covers what is being re-planned and nothing else. Without
  // this, `saved` is the morning's queues: rides the press never touched.
  check(
    "der Vorher-Wert zählt nur die Bahnen, die noch geplant werden",
    scoreCurrent(atTwo).stops.length === 1 &&
      scoreCurrent(base).stops.length === 3,
    `${scoreCurrent(atTwo).stops.length} von ${scoreCurrent(base).stops.length}`,
  );

  // Pressing twice is a no-op on today as well. It was not: `isExecutable`
  // walked a set that still held the elapsed entry while `ctx.fixed` held it
  // too, so the block overlapped itself, the day never counted as walkable and
  // the plan was rewritten on every press.
  const settled = optimizeDay({
    ...atTwo,
    entries: [
      entries[0],
      entries[1],
      { ...entries[2], startMinute: found.stops[0].startMinute },
    ],
  });
  check(
    "ein zweiter Druck ändert auch heute nichts mehr",
    settled === null,
    JSON.stringify(settled),
  );

  // Everything behind us: there is nothing to sort, and the honest answer is
  // no plan rather than a plan of nothing.
  check(
    "ist der ganze Tag abgelaufen, gibt es nichts zu sortieren",
    optimizeDay({ ...base, clock: { phase: "today", nowMinute: 23 * 60 } }) ===
      null,
  );

  // A day that has been walked is a record. Refused in the engine, so the
  // button is not the only thing standing between a press and a rewrite.
  check(
    "ein vergangener Tag wird gar nicht erst geplant",
    optimizeDay({ ...base, clock: { phase: "past" } }) === null &&
      scoreCurrent({ ...base, clock: { phase: "past" } }) === null,
  );

  // And a date in the future reckons exactly as it did before any of this.
  check(
    "ohne Uhr bleibt alles, wie es war",
    JSON.stringify(optimizeDay(base)) ===
      JSON.stringify(optimizeDay({ ...base, clock: { phase: "future" } })),
    JSON.stringify(
      optimizeDay({ ...base, clock: { phase: "future" } })?.stops.map(
        (s: any) => s.startMinute,
      ),
    ),
  );
}

// ── 12c. Added rides never land in the closed hours ────────────────────────
// The asymmetry is the point: an entry the visitor already has keeps its
// overflow block, because deleting somebody's own plan behind their back is
// worse than a hatched block that says "this does not fit". A ride the button
// is ADDING has no such claim on the day and is simply not added.
{
  const rides = [
    ride("own", flat(30), { land: "X", lat: 50.8 }),
    ride("head", flat(200), { land: "X", lat: 50.9, headliner: true }),
  ];
  const payload = day(rides, { openHour: 9, closeHour: 10 });
  const entries = [entry("own-1", "own", 9 * 60 + 30)];
  const add = headlinersToAdd(payload, entries, undefined);
  const found = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries,
    add,
  });

  check(
    "eine hinzugefügte Bahn, die nicht mehr passt, wird nicht eingeplant",
    found !== null && found.stops.every((s: any) => s.entryId !== null),
    JSON.stringify(
      found?.stops.map((s: any) => [s.attractionSlug, s.entryId, s.fits]),
    ),
  );
  check(
    "sie wird aber gezählt, damit die Leiste die Wahrheit sagt",
    found !== null && found.overflow >= 1,
    String(found?.overflow),
  );
}

// ── 12d. Adding headliners must not evict what was already planned ─────────
// The reported shape, with a screenshot: a day holding Black Mamba and Taron,
// "plan every headliner" pressed, and the two rides that came back in the
// hatched hours past 19:00 and 20:00 were those two. Ten headliners do not fit
// in a nine-hour day, every plan was scored on a plain COUNT of what did not
// fit, and the coin toss went against the plan the visitor had already made.
// The first tier of `OVERFLOW_STRIDE` is what decides it now.
{
  const rides = [
    ride("own-a", flat(60), { land: "X", lat: 50.8, headliner: true }),
    ride("own-b", flat(60), { land: "X", lat: 50.8005, headliner: true }),
    ride("add-a", flat(60), { land: "X", lat: 50.801, headliner: true }),
    ride("add-b", flat(60), { land: "X", lat: 50.8015, headliner: true }),
    ride("add-c", flat(60), { land: "X", lat: 50.802, headliner: true }),
  ];
  const payload = day(rides, { openHour: 9, closeHour: 12 });
  const entries = [
    entry("own-a-1", "own-a", 9 * 60),
    entry("own-b-1", "own-b", 11 * 60),
  ];
  const add = headlinersToAdd(payload, entries, undefined);
  const found = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries,
    add,
  });
  const shape = JSON.stringify(
    found?.stops.map((s: any) => [s.attractionSlug, s.startMinute, s.fits]),
  );

  // The premise: three headliners are offered and the day cannot hold five
  // hour-long queues, so somebody has to lose. Asserted, because a day that
  // happened to fit everything would make the two checks below pass for the
  // wrong reason.
  check(
    "drei Headliner stehen zum Ergänzen an",
    add.length === 3,
    String(add.length),
  );
  check(
    "der Tag ist zu kurz für alle fünf",
    found !== null && found.stops.length < rides.length,
    `${found?.stops.length} von ${rides.length}`,
  );

  const kept = found?.stops.filter((s: any) => s.entryId !== null) ?? [];
  check(
    "die eigenen Bahnen behalten ihren Platz im Tag",
    kept.length === entries.length && kept.every((s: any) => s.fits),
    shape,
  );
  check(
    "und was nicht mehr passt, wird gar nicht erst ergänzt",
    found !== null && found.stops.every((s: any) => s.fits),
    shape,
  );
}

// ── 13. Exactly one candidate is a plan, not a refusal ─────────────────────
{
  const rides = [
    ride("solo", flat(40), { land: "X", headliner: true }),
    ride("other", flat(20), { land: "X" }),
  ];
  const payload = day(rides);
  const add = headlinersToAdd(payload, [], undefined);
  const found = optimizeDay({
    day: payload,
    grid: grid(payload),
    entries: [],
    add,
  });
  check(
    "ein einzelner fehlender Headliner wird eingeplant",
    found !== null &&
      found.stops.length === 1 &&
      found.stops[0].attractionSlug === "solo",
    found
      ? found.stops.map((s: any) => s.attractionSlug).join(", ")
      : "kein Plan",
  );
  check(
    "und zwar an seiner eigenen Untergrenze",
    found?.stops[0]?.startMinute === OPEN * 60 + 15,
    String(found?.stops[0]?.startMinute),
  );
}

// ── 14. The overflow keeps its sequence all the way into the store ─────────
//
// `place` files what does not fit PAST closing, in the order the day would
// reach it, so the canvas can grow and show it as an overrun. The write then
// clamped every one of those minutes to 1500 — 25:00, the drag world's ceiling
// — and a park closing at 22:00 stacked three rides on that one minute.
{
  const LATE_OPEN = 9;
  const LATE_CLOSE = 22;
  const DATE = "2026-09-05";
  const rides = Array.from({ length: 10 }, (_: any, i: any) =>
    ride(`long-${i}`, flat(120, LATE_OPEN, LATE_CLOSE), {
      land: "X",
      lat: 50.8 + i * 0.004,
    }),
  );
  const payload = day(rides, { openHour: LATE_OPEN, closeHour: LATE_CLOSE });
  const entries = rides.map((r: any, i: any) =>
    entry(`${r.attractionSlug}-1`, r.attractionSlug, LATE_OPEN * 60 + i * 30),
  );
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });

  check(
    "der Überlauf reicht über Minute 1500 hinaus",
    found.stops.filter((s: any) => s.startMinute > 1500).length >= 2,
    found.stops.map((s: any) => s.startMinute).join(", "),
  );

  const state = {
    parks: {
      "test-park": {
        slug: "test-park",
        name: "Test",
        geo: { continent: "europe", country: "germany", city: "test" },
        days: { [DATE]: { date: DATE, entries } },
      },
    },
    activeParkSlug: "test-park",
    activeDate: DATE,
    version: 1,
  };
  const written = applyPlan(state, {
    parkSlug: "test-park",
    parkName: "Test",
    geo: state.parks["test-park"].geo,
    date: DATE,
    stops: found.stops.map((s: any) => ({
      entryId: s.entryId,
      attractionSlug: s.attractionSlug,
      attractionName: s.attractionName,
      startMinute: s.startMinute,
    })),
  });
  const minutes = written.parks["test-park"].days[DATE].entries.map(
    (e: any) => e.startMinute,
  );
  const stacked = [
    ...new Set(minutes.filter((m: any, i: any) => minutes.indexOf(m) !== i)),
  ];
  check(
    "und applyPlan stapelt ihn nicht auf einer Minute",
    stacked.length === 0,
    `doppelt belegt: ${stacked.join(", ")}`,
  );
  check(
    "die Minuten des Plans kommen unverändert im Tag an",
    minutes.join(",") ===
      found.stops
        .map((s: any) => s.startMinute)
        .sort((x: any, y: any) => x - y)
        .join(","),
    minutes.join(", "),
  );
}

// ── 15. Fewer queued minutes is not the only way a day gets better ─────────
//
// A block dragged past closing carries no figure at all — `estimateFor` answers
// `outside-hours`, not zero — so it costs the before-total nothing and costs the
// after-total its real queue. The day gains a ride and the difference goes
// NEGATIVE, which is why the bar may not read that difference on its own: it
// printed „Passt schon so" over a plan it had just rebuilt.
{
  const rides = [
    ride("a", morningRide(10, 60), { land: "X" }),
    ride("b", morningRide(60, 10), { land: "X" }),
  ];
  const payload = day(rides);
  // Far enough out that `estimateFor` answers `outside-hours` and not merely
  // that the grid would refuse to file there: the API emits an hour bucket AT
  // `closeHour`, so a block inside that hour still carries a figure — which is
  // deliberate (a park closing at 17:30 is half open in hour 17) and is exactly
  // what this case must not be measuring.
  const entries = [
    entry("a-1", "a", 22 * 60 + 30),
    entry("b-1", "b", 9 * 60 + 15),
  ];
  const input = { day: payload, grid: grid(payload), entries };
  const before = scoreCurrent(input);
  const found = optimizeDay(input);

  check(
    "ein Block hinter der Schließzeit zählt im Vorher-Wert nicht mit",
    before.overflow === 1 &&
      before.stops.find((s: any) => s.entryId === "a-1")?.waitMinutes === null,
    `overflow ${before.overflow}`,
  );
  check(
    "der Plan holt ihn in den Tag zurück und wird dabei teurer",
    found !== null &&
      found.overflow === 0 &&
      found.totalWaitMinutes > before.totalWaitMinutes,
    `${before.totalWaitMinutes}/${before.overflow} → ${found?.totalWaitMinutes}/${found?.overflow}`,
  );
}

// ── 16. The instance the module docstring quotes ───────────────────────────
//
// It named a runtime and an optimum for "eight rides across four lands with six
// different turning points" and the instance was written down nowhere, so the
// numbers could not be re-measured or disputed. It lives here now. The brute
// force over all 40,320 orders runs on every pass, because it is the assertion
// the docstring's claim rests on; the timing table for ten to twenty-four stops
// is behind `BENCH=1`, since a wall-clock number is not something to assert.
const BENCH_LANDS = ["A", "B", "C", "D"];
const BENCH_TURNS = [10, 11, 12, 13, 14, 16];

function benchRides(n?: any): any {
  return Array.from({ length: n }, (_: any, i: any) =>
    ride(
      `r${i}`,
      morningRide(
        10 + ((i * 13) % 50),
        20 + ((i * 29) % 60),
        BENCH_TURNS[i % BENCH_TURNS.length],
      ),
      {
        land: BENCH_LANDS[i % BENCH_LANDS.length],
        lat: 50.8 + ((i * 7) % 11) * 0.0008,
        lng: 6.87 + ((i * 5) % 13) * 0.0008,
      },
    ),
  );
}

function benchInput(n?: any): any {
  const rides = benchRides(n);
  const payload = day(rides);
  return {
    day: payload,
    grid: grid(payload),
    entries: rides.map((r: any, i: any) =>
      entry(`${r.attractionSlug}-1`, r.attractionSlug, 13 * 60 + i * 20),
    ),
  };
}

{
  const input = benchInput(8);
  const slugs = input.entries.map((e: any) => e.attractionSlug);

  const bruteStart = performance.now();
  const optimum = bruteForce(input, slugs);
  const bruteMs = performance.now() - bruteStart;

  optimizeDay(input);
  const heuristicStart = performance.now();
  const found = optimizeDay(input);
  const heuristicMs = performance.now() - heuristicStart;

  check(
    "die Heuristik findet bei acht Bahnen die beste Reihenfolge",
    found !== null && lex(costOf(found)) <= lex(optimum),
    `gefunden ${JSON.stringify(costOf(found))} in ${heuristicMs.toFixed(1)} ms, ` +
      `optimal ${JSON.stringify(optimum)} über 40.320 Reihenfolgen in ${(bruteMs / 1000).toFixed(2)} s`,
  );

  if (process.env.BENCH) {
    console.log("\nLaufzeit (Median aus 5), BENCH=1:");
    for (const n of [10, 14, 18, 24]) {
      const sized = benchInput(n);
      optimizeDay(sized);
      const times: any[] = [];
      for (let run = 0; run < 5; run++) {
        const started = performance.now();
        optimizeDay(sized);
        times.push(performance.now() - started);
      }
      times.sort((x: any, y: any) => x - y);
      console.log(
        `  ${String(n).padStart(2)} Stopps: ${times[2].toFixed(1)} ms`,
      );
    }
    console.log(
      `  8 Bahnen: Brute Force ${(bruteMs / 1000).toFixed(2)} s gegen ${heuristicMs.toFixed(1)} ms\n`,
    );
  }
}

// ── 17. The 40-minute hole, as it was reported ─────────────────────────────
//
// Phantasialand, Sunday 2026-09-06, from production. Crazy Bats frees the
// visitor at 12:05 and Taron is 355 m away — a 15-minute transfer, so the
// earliest slot on the grid is 12:30, where Taron costs 55 minutes against 50
// at 13:00:
//
//     A  queue at 12:30    55 queued    0 idle   done 13:25
//     B  queue at 13:00    50 queued   30 idle   done 13:50
//
// The optimiser shipped **B**: five minutes less queueing, bought with forty
// minutes of standing about and a day ending twenty-five minutes later. This is
// the lower half of the bracket around IDLE_WEIGHT — A wins while
// 55 < 50 + k×30, i.e. k > 1/6 — and §11 is the other half. Between them there
// is no ordering of "queued minutes" and "the clock" that gets both right,
// which is the whole reason there is a weight.
//
// The park's opening is the fixture's own and nothing else about the case is:
// at 11:00 the walk from the gates files Crazy Bats at 11:15, its 50 minutes of
// queue leave the visitor free at 12:05, and everything this turns on happens
// after that minute.
{
  const OPEN_LATE = 11;
  const rides = [
    // 0.003193° of latitude is 355 m, which is the distance in the report and
    // what makes the transfer 15 minutes rather than a number chosen to fit.
    ride(
      "crazy-bats",
      {
        11: 50,
        12: 55,
        13: 60,
        14: 60,
        15: 60,
        16: 55,
        17: 55,
        18: 55,
        19: 50,
        20: 50,
      },
      { land: "Berlin", lat: 50.8 },
    ),
    ride(
      "taron",
      {
        11: 70,
        12: 55,
        13: 50,
        14: 60,
        15: 60,
        16: 60,
        17: 55,
        18: 55,
        19: 50,
        20: 45,
      },
      { land: "Klugheim", lat: 50.8 + 0.003193, headliner: true },
    ),
  ];
  const payload = day(rides, {
    openHour: OPEN_LATE,
    date: "2026-09-06",
    isWeekend: true,
  });
  // The day as it was planned, which is the plan that was reported.
  const entries = [
    entry("crazy-bats-1", "crazy-bats", 11 * 60 + 15),
    entry("taron-1", "taron", 13 * 60),
  ];
  const input = { day: payload, grid: grid(payload), entries };

  const before = scoreCurrent(input);
  const found = optimizeDay(input);
  const taron = found?.stops.find(
    (stop: any) => stop.attractionSlug === "taron",
  );

  check(
    "Taron wird um 12:30 angestellt und nicht um 13:00",
    taron?.startMinute === 12 * 60 + 30 && taron?.waitMinutes === 55,
    found?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}=${s.waitMinutes}`)
      .join(" → ") ?? "kein Plan",
  );
  check(
    "der Plan nimmt dafür fünf Warteminuten mehr in Kauf",
    before !== null &&
      found !== null &&
      found.totalWaitMinutes - before.totalWaitMinutes === 5,
    `${before?.totalWaitMinutes} → ${found?.totalWaitMinutes} Min. Schlange`,
  );
  check(
    "und das Loch von 30 Minuten ist weg",
    before?.idleMinutes === 30 && found?.idleMinutes === 0,
    `${before?.idleMinutes} → ${found?.idleMinutes} Min. Leerlauf`,
  );
  check(
    "der Tag endet 25 Minuten früher",
    before !== null &&
      found !== null &&
      before.endMinute - found.endMinute === 25,
    `${before?.endMinute} → ${found?.endMinute}`,
  );
}

// ── 17b. The idle total describes the plan, and is not just carried around ──
//
// The reported symptom was a hole, so a plan has to be able to say how much
// standing about is in it — and the figure has to be the plan's, not the
// search's opinion of it. `idleOfPlan` walks the stops with the app's own block
// heights and transfers; if the two ever disagree, the optimiser is choosing on
// a number that describes something other than what it hands back.
{
  const rides = [
    ride("a", morningRide(5, 80), { land: "X" }),
    ride("b", morningRide(80, 5), { land: "X" }),
    ride("c", flat(25), { land: "X" }),
    ride("d", morningRide(10, 50), { land: "X" }),
    ride("e", morningRide(50, 10), { land: "X" }),
  ];
  const payload = day(rides);
  const entries = rides.map((r: any, i: any) =>
    entry(`${r.attractionSlug}-1`, r.attractionSlug, 9 * 60 + i * 45),
  );
  const found = optimizeDay({ day: payload, grid: grid(payload), entries });

  check(
    "die gemeldete Leerlaufsumme ist die des fertigen Plans",
    found !== null && found.idleMinutes === idleOfPlan(payload, found.stops),
    `gemeldet ${found?.idleMinutes}, nachgerechnet ${idleOfPlan(payload, found?.stops ?? [])}`,
  );
  // The sixty minutes in that plan are bought and not left lying about: `b`
  // costs 80 minutes before noon and 5 after it, so an hour of standing about
  // saves seventy-five in a queue. Where nothing falls there is nothing to buy,
  // and the plan then has no holes at all — which is the assertion that would
  // have caught what was reported, on a day with no cliff in it to explain it.
  check(
    "die Stunde Leerlauf kauft fünfundsiebzig Warteminuten",
    found !== null && found.idleMinutes === 60 && found.totalWaitMinutes === 55,
    `${found?.idleMinutes} Min. Leerlauf, ${found?.totalWaitMinutes} Min. Schlange`,
  );

  const steady = day([
    ride("p", flat(20), { land: "X", lat: 50.8 }),
    ride("q", flat(35), { land: "X", lat: 50.8005 }),
    ride("r", flat(15), { land: "Y", lat: 50.803 }),
    ride("s", flat(45), { land: "Y", lat: 50.8035 }),
    ride("t", flat(25), { land: "X", lat: 50.801 }),
  ]);
  const steadyEntries = ["p", "q", "r", "s", "t"].map((slug: any, i: any) =>
    entry(`${slug}-1`, slug, 10 * 60 + i * 75),
  );
  const steadyPlan = optimizeDay({
    day: steady,
    grid: grid(steady),
    entries: steadyEntries,
  });
  check(
    "und ein Tag ohne Einbruch bekommt gar kein Loch",
    steadyPlan !== null &&
      steadyPlan.idleMinutes === 0 &&
      steadyPlan.idleMinutes === idleOfPlan(steady, steadyPlan.stops),
    `${steadyPlan?.idleMinutes} Min. Leerlauf: ${steadyPlan?.stops.map((s: any) => `${s.attractionSlug}@${s.startMinute}`).join(" → ")}`,
  );
}

// ── 18. A queue is JOINED before closing, and never after ──────────────────
//
// Two halves of one rule, reported together. `closeHour` is the hour the park's
// closing time falls in, and `buildDayGrid` used to add sixty minutes to it —
// so Phantasialand's Saturday, a nine-hour day ending at 18:00, was planned
// against a day ending at 19:00 and came back with Winja's Fear queued at
// 18:15. The other half is what keeps the fix from costing anything: a queue
// may be joined right up to the last minute, and what happens after that is the
// park emptying a line it has already let you into.
{
  const rides = [
    ride("a", flat(30), { land: "X", lat: 50.8 }),
    ride("b", flat(30), { land: "X", lat: 50.8005 }),
    ride("c", flat(30), { land: "X", lat: 50.801 }),
    ride("d", flat(30), { land: "X", lat: 50.8015 }),
  ];
  const payload = day(rides);
  const g = grid(payload);
  const entries = [entry("a-1", "a", 9 * 60)];
  const found = optimizeDay({
    day: payload,
    grid: g,
    entries,
    add: [rides[1], rides[2], rides[3]],
  });

  check(
    "kein Stopp wird nach Parkschluss angestellt",
    found !== null &&
      found.stops.every((stop: any) => stop.startMinute < g.closeMin),
    found?.stops
      .map((s: any) => `${s.attractionSlug}@${s.startMinute}`)
      .join(" "),
  );

  // And the last quarter hour before closing is a slot, not a wall: a ninety
  // minute queue joined there is a plan somebody makes on purpose, and the day
  // it overruns into is not overflow.
  const late = day([
    ride("x", flat(20), { land: "X", lat: 50.8 }),
    ride("y", flat(90), { land: "X", lat: 50.8003 }),
  ]);
  const lateGrid = grid(late);
  const lateEntries = [entry("x-1", "x", lateGrid.closeMin - 15)];
  const lateScore = scoreCurrent({
    day: late,
    grid: lateGrid,
    entries: lateEntries,
  });
  check(
    "eine Bahn kurz vor Parkschluss ist kein Überlauf",
    lateScore !== null &&
      lateScore.overflow === 0 &&
      lateScore.stops[0].fits === true,
    `overflow ${lateScore?.overflow}, fits ${lateScore?.stops[0]?.fits}`,
  );
}

// ── 19. Which headliner is given up, when one has to be ────────────────────
//
// Reported from production: Phantasialand, Saturday 2026-09-12, a lunch break
// and nothing else planned. Ten headliners, room for nine, and the button gave
// up **F.L.Y. and Taron** — the two the park is known for — while keeping both
// Winja's. The tie fell through to queued minutes, and the ride with the
// longest queue is both the most expensive to keep AND the one most people are
// there for, so minimising cost sacrificed the flagship by construction. The
// margin was five minutes over a 315-minute day.
{
  // Three headliners, room for two. `big` has the longest queue and is
  // therefore both the priciest to keep and the one somebody travelled for.
  const rides = [
    ride("hub", flat(10, 9, 11), { land: "X", lat: 50.8 }),
    ride("big", flat(60, 9, 11), { land: "X", lat: 50.8006, headliner: true }),
    ride("mid", flat(45, 9, 11), { land: "X", lat: 50.8012, headliner: true }),
    ride("small", flat(20, 9, 11), {
      land: "X",
      lat: 50.8018,
      headliner: true,
    }),
  ];
  const payload = day(rides, { openHour: 9, closeHour: 11 });
  const g = grid(payload);
  const entries = [entry("hub-1", "hub", 9 * 60)];
  const add = [rides[1], rides[2], rides[3]];
  const found = optimizeDay({ day: payload, grid: g, entries, add });
  const planned = found.stops.map((s: any) => s.attractionSlug);

  check(
    "der Headliner mit der längsten Schlange bleibt drin",
    planned.includes("big"),
    planned.join(" "),
  );
  check(
    "und der kleinste fällt weg, nicht der teuerste",
    !planned.includes("small"),
    planned.join(" "),
  );

  // The visitor overrules it. `priority` is the order they put the rides in,
  // most important first, and it is the only thing that beats the day's own
  // figures — because no payload knows which coaster somebody drove four hours
  // for.
  const said = optimizeDay({
    day: payload,
    grid: g,
    entries,
    add,
    priority: ["small", "mid", "big"],
  }).stops.map((s: any) => s.attractionSlug);
  check(
    "die Reihenfolge des Besuchers schlägt die Tagesprognose",
    said.includes("small") && !said.includes("big"),
    said.join(" "),
  );
}

// ── 20. A walk never outweighs a queue ─────────────────────────────────────
//
// "Wartezeit ist Laufwegen vorzuziehen": with one slot left and two rides for
// it, the shorter queue wins however far away it is. It holds because a walk
// costs `planCost` nothing and only moves the clock — but it did NOT hold while
// the queue of a ride that never happens was still in the total: 10 + 60 + 45
// is 10 + 45 + 60 whichever of the two actually gets ridden, so the totals tied
// and the choice fell through to the clock, which prefers the short walk.
// Measured before the fix, the near 60-minute ride beat the far 45-minute one
// at every difference up to fifteen minutes; after it, the far ride wins down
// to five.
{
  const chosen: any[] = [];
  for (const farWait of [55, 50, 45, 40, 35]) {
    const rides = [
      ride("hub", flat(10, 9, 11), { land: "A", lat: 50.8 }),
      ride("near", flat(60, 9, 11), { land: "A", lat: 50.80054 }),
      ride("far", flat(farWait, 9, 11), { land: "B", lat: 50.809 }),
    ];
    const payload = day(rides, { openHour: 9, closeHour: 11 });
    const found = optimizeDay({
      day: payload,
      grid: grid(payload),
      entries: [entry("hub-1", "hub", 9 * 60)],
      add: [rides[1], rides[2]],
    });
    const added = found.stops
      .filter((s: any) => s.entryId === null)
      .map((s: any) => s.attractionSlug);
    chosen.push(`Δ${60 - farWait}:${added.join("+") || "—"}`);
  }
  check(
    "die kürzere Schlange gewinnt gegen den kürzeren Weg",
    chosen.every((row: any) => row.includes("far")),
    chosen.join(" "),
  );

  // The other half, stated on its own: a queue nobody joins costs nobody
  // anything, so it may not appear in the total the bar prints as a saving —
  // nor may the standing about in front of it, nor the minute it would have
  // ended at.
  const rides = [
    ride("hub", flat(10, 9, 11), { land: "A", lat: 50.8 }),
    ride("near", flat(60, 9, 11), { land: "A", lat: 50.80054 }),
    ride("far", flat(45, 9, 11), { land: "B", lat: 50.809 }),
  ];
  const payload = day(rides, { openHour: 9, closeHour: 11 });
  const g = grid(payload);
  const found = optimizeDay({
    day: payload,
    grid: g,
    entries: [entry("hub-1", "hub", 9 * 60)],
    add: [rides[1], rides[2]],
  });
  const sumOfPlanned = found.stops.reduce(
    (total: any, stop: any) => total + (stop.waitMinutes ?? 0),
    0,
  );
  check(
    "die Warteminuten sind die der geplanten Bahnen",
    found.totalWaitMinutes === sumOfPlanned,
    `${found.totalWaitMinutes} gegen ${sumOfPlanned} über ${found.stops.length} Stopps`,
  );
  check(
    "und der Feierabend ist der der geplanten Bahnen, die letzte Fahrt eingerechnet",
    found.endMinute ===
      Math.max(
        ...found.stops.map(
          (s: any) =>
            s.startMinute +
            plannedSpanOf(payload, s.attractionSlug, s.startMinute) +
            RIDE_DURATION_MIN,
        ),
      ),
    `Feierabend ${found.endMinute}`,
  );
  // Dieselbe Regel auf der anderen Seite: der Ist-Zustand rechnet das Ende wie
  // die Suche, sonst meldet der Knopf einen Unterschied auf einem unveränderten Tag.
  const applied = found.stops.map((stop: any, i: any) =>
    entry(`p${i}`, stop.attractionSlug, stop.startMinute),
  );
  const current = scoreCurrent({ day: payload, grid: g, entries: applied });
  check(
    "scoreCurrent endet auf demselben Minutenwert wie die Suche",
    current !== null && current.endMinute === found.endMinute,
    `${current?.endMinute} gegen ${found.endMinute}`,
  );
}

// ── Early entry (PAR-199) ────────────────────────────────────────────────────
// On a day the visitor holds early entry, a headliner may be filed before the
// park opens and an ordinary ride may not. Without the visitor's answer nothing
// moves: the same plan, minute for minute.
{
  const rides = [
    ride("taron", flat(60), { headliner: true }),
    ride("fly", flat(55), { headliner: true, lat: 50.801 }),
    ride("maus", flat(10), { lat: 50.802 }),
  ];
  const early = { hasEarlyEntry: true, earlyEntryMinutesPeak: 30 };
  const run = (context: any) => {
    const payload = day(rides, context);
    const g = buildDayGrid(
      payload.context.openHour,
      payload.context.closeHour,
      undefined,
      earlyEntryOpenMin(payload.context),
    );
    return optimizeDay({ day: payload, grid: g, entries: [], add: rides });
  };
  const on = run({ ...early, earlyEntry: true });
  const off = run(early);
  const plain = run({});
  const openMin = OPEN * 60;
  const firstStart = Math.min(...on.stops.map((s: any) => s.startMinute));
  const first = on.stops.find((s: any) => s.startMinute === firstStart);
  check(
    "early entry: the first stop is filed before the park opens",
    firstStart < openMin && firstStart >= openMin - 30,
    `erster Start ${firstStart}`,
  );
  check(
    "early entry: and it is a headliner",
    ["taron", "fly"].includes(first?.attractionSlug),
    first?.attractionSlug,
  );
  check(
    "early entry: it carries the assumed figure",
    first?.waitMinutes === 5,
    `Wartezeit ${first?.waitMinutes}`,
  );
  check(
    "early entry: the ordinary ride waits for the park",
    on.stops.find((s: any) => s.attractionSlug === "maus").startMinute >=
      openMin,
    `maus ${on.stops.find((s: any) => s.attractionSlug === "maus").startMinute}`,
  );
  check(
    "early entry: every stop of the early day fits",
    on.stops.every((s: any) => s.fits),
    JSON.stringify(on.stops),
  );
  check(
    "early entry: it saves queueing against the same day without it",
    on.totalWaitMinutes < off.totalWaitMinutes,
    `${on.totalWaitMinutes} gegen ${off.totalWaitMinutes}`,
  );
  check(
    "early entry: without the visitor nothing starts before the opening",
    off.stops.every((s: any) => s.startMinute >= openMin),
    JSON.stringify(off.stops),
  );
  check(
    "early entry: and the plan is the plan of a park without early entry",
    JSON.stringify(off) === JSON.stringify(plain),
    "Plan weicht ab",
  );
}

// ── Quarter hours (PAR-910) ─────────────────────────────────────────────────
// With `slots` the table is read per quarter hour: a dip at 09:45 inside an hour the hourly
// series calls flat is worth a 30-minute wait (40 at 09:15 against 10 + 0.5 × 30), and a day
// without slots plans exactly as before.
{
  const hourly = ride("dip", flat(40));
  const withSlots = {
    ...hourly,
    slots: Array.from({ length: (CLOSE - OPEN) * 4 }, (_: any, i: any) => {
      const minute = OPEN * 60 + i * 15;
      return { minute, start: "", wait: minute === 9 * 60 + 45 ? 10 : 40 };
    }),
  };
  const plainDay = day([hourly]);
  const slotDay = day([withSlots]);
  const entries = [entry("d1", "dip", 9 * 60)];
  const plain = optimizeDay({ day: plainDay, grid: grid(plainDay), entries });
  const slotted = optimizeDay({ day: slotDay, grid: grid(slotDay), entries });
  check(
    "Slots: der Optimierer findet die Viertelstunde mit der kurzen Schlange",
    slotted?.stops[0]?.startMinute === 9 * 60 + 45,
    JSON.stringify(slotted?.stops),
  );
  check(
    "Slots: und rechnet mit ihrem Wert, nicht mit dem der Stunde",
    slotted?.totalWaitMinutes === 10,
    `${slotted?.totalWaitMinutes}`,
  );
  check(
    // `null` is „already as good as it gets": a flat hour has nothing to wait for.
    "Slots: ohne Slots wartet der Plan auf nichts",
    plain === null ||
      (plain.stops[0]?.startMinute === 9 * 60 + 15 &&
        plain.totalWaitMinutes === 40),
    JSON.stringify(plain?.stops),
  );
}

// ── A ride shut for maintenance is not planned ─────────────────────────────
{
  // `/plan/day` leaves the ride out and the proxy names it in `ridesInWorks`; a block the visitor
  // planned before the window was known stays where it is, and no press moves or scores it.
  const payload = {
    ...day([ride("a", flat(20)), ride("b", flat(30))]),
    ridesInWorks: [
      {
        attractionSlug: "shut",
        attractionName: "shut",
        worksPeriod: {
          from: "2026-09-01",
          to: "2026-09-30",
          toUncertain: false,
        },
      },
    ],
  };
  const entries = [
    entry("a-1", "a", 15 * 60),
    entry("shut-1", "shut", 10 * 60),
    entry("b-1", "b", 11 * 60),
  ];
  check(
    "Wartung: eine Bahn in Wartung ist nicht verschiebbar",
    movableEntries(entries, undefined, payload)
      .map((e: any) => e.id)
      .join(",") === "a-1,b-1",
  );
  const plan = optimizeDay({ day: payload, grid: grid(payload), entries });
  check(
    "Wartung: der Optimierer plant sie nicht ein",
    plan !== null &&
      plan.stops.every((stop: any) => stop.attractionSlug !== "shut"),
    JSON.stringify(plan?.stops),
  );
  check(
    "Wartung: ohne Tag bleibt der Filter, wie er war",
    movableEntries(entries).length === 3,
  );
}

describe("day optimizer vectors", () => {
  it("holds every vector ported from park.fan", () => {
    expect(failures).toEqual([]);
  });
});

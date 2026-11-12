/**
 * The fit assistant's model, against days built to make each rule visible. Ported from park.fan's
 * `scripts/test-planner-fit.mjs` with its vectors unchanged; the checks run while the file loads
 * and the test below reports them.
 *
 * `lib/planner/fit.ts` answers a question `optimizeDay` cannot: not "what is
 * the best plan for these rides" but "what would I have to change for them to
 * fit". Everything it says is a difference between two runs of the same engine,
 * so what a check has to pin is that the differences are real ones — a lever
 * that buys nothing is never offered, a lever that solves the day says so, and
 * the visitor's own order decides which ride falls out when one has to.
 *
 * The fixtures are sized so each rule is the ONLY thing that could produce the
 * answer: a six-hour day with five 60-minute headliners holds four of them, a
 * one-hour block takes the fifth away, and half an hour gives it back.
 *
 *     pnpm test -- fit.vectors
 */

import * as gridModule from "./day-grid";
import * as optimizeModule from "./optimize";
import * as legModule from "./leg";
import * as addRideFitModule from "./add-ride-fit";
import * as fitModule from "./fit";

// Untyped on purpose: the fixtures are the browser's, written without types.
const { RIDE_DURATION_MIN, earlyEntryOpenMin } = gridModule as any;
const { clashCount, headlinersToAdd, optimizeDay } = optimizeModule as any;
const { EXIT_MIN, entryPlace, transferBetween } = legModule as any;
const { noRoomForRide, requestedRideChoice } = addRideFitModule as any;
const {
  SHORT_BLOCK_MIN,
  addWishKey,
  entryWishKey,
  evaluateFit,
  fitBlocks,
  fitChoiceAll,
  fitLevers,
  fitOrder,
  fitWishes,
  needsFitHelp,
  togglePin,
} = fitModule as any;

/** The browser's signature, whose third argument was the pixel scale this port has no use for. */
function buildDayGrid(
  openHour: any,
  closeHour: any,
  _pxPerMin?: any,
  earlyOpenMin: any = null,
): any {
  return gridModule.buildDayGrid(openHour, closeHour, earlyOpenMin);
}

/**
 * The browser's `dayClock`: where `date` sits against the park's clock at `now`. In the API the
 * clock comes from the request; the vectors still name it by an instant.
 */
function dayClock(date: string, timeZone: string, now: number): any {
  const today = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(new Date(now));
  if (date < today) return { phase: "past" };
  if (date > today) return { phase: "future" };
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hourCycle: "h23",
  }).formatToParts(new Date(now));
  const part = (type: string) =>
    Number(parts.find((p) => p.type === type)?.value ?? "0");
  return { phase: "today", nowMinute: part("hour") * 60 + part("minute") };
}

/**
 * The fit assistant's model, against days built to make each rule visible.
 *
 * `lib/planner/fit.ts` answers a question `optimizeDay` cannot: not "what is
 * the best plan for these rides" but "what would I have to change for them to
 * fit". Everything it says is a difference between two runs of the same engine,
 * so what a check has to pin is that the differences are real ones — a lever
 * that buys nothing is never offered, a lever that solves the day says so, and
 * the visitor's own order decides which ride falls out when one has to.
 *
 * The fixtures are sized so each rule is the ONLY thing that could produce the
 * answer: a six-hour day with five 60-minute headliners holds four of them, a
 * one-hour block takes the fifth away, and half an hour gives it back.
 *
 *     pnpm test:planner-fit
 */

const failures: string[] = [];
function check(name: string, ok: unknown, detail: unknown = ""): any {
  if (!ok) failures.push(detail ? `${name} — ${String(detail)}` : name);
}

const OPEN = 9;
const CLOSE = 15;

/** A ride with a flat curve, so its cost never depends on which hour it lands in. */
function ride(slug?: any, wait?: any, options: any = {}): any {
  const hours: any[] = [];
  for (let hour = 0; hour < 24; hour++) hours.push({ hour, wait });
  return {
    attractionSlug: slug,
    attractionName: slug,
    hours,
    dayPeak: options.dayPeak ?? wait,
    sampleDays: 90,
    // Same corner of the park, so the transfer is a constant and the day's
    // capacity is arithmetic on the queues rather than on the walking.
    latitude: 50.8,
    longitude: 6.87,
    land: "Berlin",
    uncertaintyMinutes: null,
    opensAt: null,
    isHeadliner: options.headliner ?? true,
  };
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
      date: "2026-09-12",
      status: "OPERATING",
      openHour: OPEN,
      closeHour: CLOSE,
      isHoliday: false,
      isBridgeDay: false,
      isSchoolVacation: false,
      isWeekend: true,
      ...context,
    },
  };
}

function block(
  id?: any,
  startMinute?: any,
  durationMinutes?: any,
  extra: any = {},
): any {
  return {
    id,
    custom: { label: id, icon: "food", durationMinutes },
    startMinute,
    ...extra,
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

/** The whole input in one call — the shape every call site builds. */
function inputFor(payload?: any, entries?: any, add?: any, clock?: any): any {
  const grid = buildDayGrid(
    payload.context.openHour,
    payload.context.closeHour,
    undefined,
    earlyEntryOpenMin(payload.context),
  );
  return {
    day: payload,
    grid,
    entries,
    wishes: fitWishes(payload, entries, add, clock),
    blocks: fitBlocks(entries),
    clock,
  };
}

const names = (input: any, keys: any) => {
  const byKey = new Map<any, any>(
    input.wishes.map((wish: any) => [wish.key, wish]),
  );
  return keys
    .map((key: any) => byKey.get(key)?.attractionName ?? key)
    .join(", ");
};

// ── 1. A day that holds everything is never asked about ─────────────────────

{
  const payload = day([ride("a", 30), ride("b", 30), ride("c", 30)]);
  const add = headlinersToAdd(payload, [], undefined);
  const input = inputFor(payload, [], add);
  const choice = fitChoiceAll();
  const outcome = evaluateFit(input, choice);

  check(
    "1a three short queues in a six-hour day all fit",
    outcome.fitted.length === 3,
    `${outcome.fitted.length}`,
  );
  check("1b nothing to help with", needsFitHelp(input, choice) === false);
  check(
    "1c no levers on a day that fits",
    fitLevers(input, choice).length === 0,
  );
}

// ── 2. The break is what does not fit, and the lever says so ────────────────
//
// Five 50-minute headliners against six hours: with a full hour out of the
// middle four of them fill the day and with half an hour all five do. Nothing
// else in the fixture can produce that difference — the curves are flat and the
// rides share a land, so the only variable left is the break.

const FIVE = ["a", "b", "c", "d", "e"].map((slug: any) => ride(slug, 50));

/** The same five with an hour of queue each, where half a break is not enough. */
const FIVE_LONG = ["a", "b", "c", "d", "e"].map((slug: any) => ride(slug, 60));

{
  const payload = day(FIVE);
  const entries = [block("lunch", 12 * 60, 60)];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);
  const choice = fitChoiceAll();
  const outcome = evaluateFit(input, choice);

  check(
    "2a five wishes offered",
    input.wishes.length === 5,
    `${input.wishes.length}`,
  );
  check(
    "2b one of them does not fit",
    outcome.missed.length === 1,
    names(input, outcome.missed),
  );
  check(
    "2c the day is worth asking about",
    needsFitHelp(input, choice) === true,
  );

  const levers = fitLevers(input, choice);
  const solving = levers.filter((lever: any) => lever.solves);
  check("2d a lever solves it", solving.length >= 1, JSON.stringify(levers));
  check(
    "2e and it is the break, cut short rather than skipped",
    solving[0]?.kind === "shorten-block" && solving[0]?.entryId === "lunch",
    JSON.stringify(solving[0]),
  );
  check(
    "2f the short version is offered at SHORT_BLOCK_MIN",
    solving[0]?.minutes === SHORT_BLOCK_MIN,
    `${solving[0]?.minutes}`,
  );
  check(
    "2g dropping the block outright is not offered beside it",
    !levers.some(
      (lever: any) => lever.kind === "drop-block" && lever.entryId === "lunch",
    ),
    JSON.stringify(levers.map((lever: any) => lever.kind)),
  );

  const shortened = { ...choice, shortBlocks: new Set(["lunch"]) };
  check(
    "2h pulling it makes every wish fit",
    evaluateFit(input, shortened).missed.length === 0,
  );
  check(
    "2i and the block is still in the day, at half the length",
    evaluateFit(input, shortened).entries.find((e: any) => e.id === "lunch")
      ?.custom.durationMinutes === SHORT_BLOCK_MIN,
  );
}

// ── 3. A break too long to cut short enough is dropped instead ──────────────

{
  const payload = day(FIVE_LONG);
  const entries = [block("lunch", 12 * 60, 120)];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);
  const choice = fitChoiceAll();
  const levers = fitLevers(input, choice);

  check(
    "3a a two-hour break costs two rides",
    evaluateFit(input, choice).missed.length === 2,
  );
  check(
    "3b both answers are offered, the one that solves the day first",
    levers[0]?.kind === "drop-block" &&
      levers.some((lever: any) => lever.kind === "shorten-block"),
    JSON.stringify(levers.map((lever: any) => `${lever.kind}:${lever.fits}`)),
  );
  const shorten = levers.find((lever: any) => lever.kind === "shorten-block");
  check(
    "3c and half a break is offered too, for what it does buy",
    shorten.fits > shorten.fitsNow && shorten.fits < levers[0].fits,
    `${shorten.fits} vs ${levers[0].fits}`,
  );
  check(
    "3d and the block is gone from the day it produces",
    evaluateFit(input, {
      ...choice,
      droppedBlocks: new Set(["lunch"]),
    }).entries.every((e: any) => e.id !== "lunch"),
  );
}

// ── 4. Which wish falls out is the visitor's decision ───────────────────────

{
  const payload = day(FIVE);
  const entries = [block("lunch", 12 * 60, 60)];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);
  const choice = fitChoiceAll();
  const dropped = evaluateFit(input, choice).missed[0];

  // Whatever the engine gave up by itself, put it first and something else has
  // to go instead. The five rides are identical apart from their slugs, so the
  // ONLY thing that can move the answer is the order.
  const promoted = {
    ...choice,
    priority: [
      dropped,
      ...choice.priority.filter((key: any) => key !== dropped),
    ],
  };
  const after = evaluateFit(input, promoted);
  check(
    "4a the promoted wish now fits",
    after.fitted.includes(dropped),
    names(input, after.missed),
  );
  check("4b and exactly one other does not", after.missed.length === 1);

  // The last in the list is the first to go, which is the sentence the dialog
  // makes to the visitor: pin the other four and the fifth is what goes.
  const order = fitOrder(input, choice);
  const last = order[order.length - 1].key;
  const pinned = {
    ...choice,
    priority: order.slice(0, -1).map((wish: any) => wish.key),
  };
  check(
    "4c the one nobody pinned is the one given up",
    evaluateFit(input, pinned).missed[0] === last,
    names(input, evaluateFit(input, pinned).missed),
  );
}

// ── 4b. A ride the park does not curate still wins where the visitor said so ─
//
// `isHeadliner` used to be the only thing that could put a ride in the tier
// that survives an overflow, so a filler ranked FIRST by the visitor was still
// the first thing dropped. The engine reads `priority` for the tier now
// (`isWanted`), and this is the day that says so.

{
  const payload = day([
    ride("head-a", 60),
    ride("head-b", 60),
    ride("head-c", 60),
    ride("head-d", 60),
    ride("filler", 60, { headliner: false }),
  ]);
  const entries = [block("lunch", 12 * 60, 60)];
  // Not `headlinersToAdd` — the whole point is a ride that is not one.
  const add = payload.rides;
  const input = inputFor(payload, entries, add);
  const choice = fitChoiceAll();
  const fillerKey = addWishKey("filler");

  const byDefault = evaluateFit(input, choice);
  check(
    "4d without a word from the visitor the filler is what goes",
    byDefault.missed[0] === fillerKey,
    names(input, byDefault.missed),
  );

  const promoted = {
    ...choice,
    priority: [
      fillerKey,
      ...choice.priority.filter((key: any) => key !== fillerKey),
    ],
  };
  const after = evaluateFit(input, promoted);
  check(
    "4e ranked first, the filler is kept and a headliner goes",
    after.fitted.includes(fillerKey),
    names(input, after.missed),
  );
}

// ── 5. Unticking a wish takes it out of the day ─────────────────────────────

{
  const payload = day(FIVE);
  const entries = [
    entry("own-a", "a", 9 * 60),
    entry("own-b", "b", 11 * 60),
    block("lunch", 12 * 60, 60),
  ];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);

  check(
    "5a the two planned rides come first in the list",
    input.wishes
      .slice(0, 2)
      .map((w: any) => w.key)
      .join() === `${entryWishKey("own-a")},${entryWishKey("own-b")}`,
  );
  check(
    "5b and the three missing headliners follow",
    input.wishes.length === 5,
  );

  const choice = fitChoiceAll();
  const withoutOwn = { ...choice, dropped: new Set([entryWishKey("own-a")]) };
  const outcome = evaluateFit(input, withoutOwn);
  check(
    "5c an unticked entry is not in the day that comes back",
    outcome.entries.every((e: any) => e.id !== "own-a"),
  );
  check(
    "5d and the free block still is",
    outcome.entries.some((e: any) => e.id === "lunch"),
  );
  check(
    "5e nor is it planned",
    !outcome.fitted.includes(entryWishKey("own-a")) &&
      !outcome.missed.includes(entryWishKey("own-a")),
  );

  const kept = evaluateFit(input, choice);
  check(
    "5f a ticked entry is re-planned rather than parked past the gate",
    kept.entries.every((e: any) => e.id !== "own-a" && e.id !== "own-b"),
  );
}

// ── 6. What the assistant may not touch ─────────────────────────────────────

{
  const payload = day(FIVE);
  const entries = [
    entry("done-a", "a", 9 * 60, { done: true, actualWait: 40 }),
    block("eaten", 11 * 60, 60, { done: true }),
    block("lunch", 13 * 60, 60),
    entry("own-b", "b", 14 * 60),
  ];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);

  check(
    "6a a ticked-off ride is not a wish",
    input.wishes.every((wish: any) => wish.entryId !== "done-a"),
  );
  check(
    "6b a ticked-off block is not a lever",
    input.blocks.every((b: any) => b.entryId !== "eaten"),
    JSON.stringify(input.blocks),
  );
  check(
    "6c the open block is",
    input.blocks.some((b: any) => b.entryId === "lunch"),
  );

  const outcome = evaluateFit(input, fitChoiceAll());
  check(
    "6d and both stay in the day whatever is chosen",
    outcome.entries.some((e: any) => e.id === "done-a") &&
      outcome.entries.some((e: any) => e.id === "eaten"),
  );
}

// ── 7. A ride the payload has no row for ────────────────────────────────────

{
  const payload = day(FIVE);
  const entries = [entry("ghost", "gone-from-the-park", 10 * 60)];
  const input = inputFor(payload, entries, []);
  const wish = input.wishes.find((w: any) => w.entryId === "ghost");

  check("7a it is still a wish", Boolean(wish));
  check("7b with no row behind it", wish.ride === null);

  const outcome = evaluateFit(input, fitChoiceAll());
  check(
    "7c and it keeps its minute rather than being re-planned",
    outcome.entries.some(
      (e: any) => e.id === "ghost" && e.startMinute === 10 * 60,
    ),
  );

  const removed = evaluateFit(input, {
    ...fitChoiceAll(),
    dropped: new Set([entryWishKey("ghost")]),
  });
  check(
    "7d unticking it is still the way out",
    removed.entries.every((e: any) => e.id !== "ghost"),
  );
}

// ── 8. A lever that buys nothing is not offered ─────────────────────────────

{
  const payload = day(FIVE);
  const entries = [block("lunch", 12 * 60, 60)];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);
  const choice = fitChoiceAll();
  check(
    "8a tight, so the break is worth offering",
    fitLevers(input, choice).length > 0,
  );

  // Give up the ride that did not fit and the break costs nothing any more.
  const slimmed = {
    ...choice,
    dropped: new Set(evaluateFit(input, choice).missed),
  };
  check(
    "8b once the list fits, no lever is offered",
    fitLevers(input, slimmed).length === 0,
  );
  check(
    "8c and nothing is left to ask about",
    needsFitHelp(input, slimmed) === false,
  );
}

// ── 9. The same day always comes back the same way ──────────────────────────

{
  const payload = day(FIVE);
  const entries = [block("lunch", 12 * 60, 60)];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);
  const choice = fitChoiceAll();
  const a = evaluateFit(input, choice);
  const b = evaluateFit(input, choice);
  check(
    "9a deterministic",
    JSON.stringify(a.fitted) === JSON.stringify(b.fitted) &&
      a.endMinute === b.endMinute,
  );
  check(
    "9b and so are the levers",
    JSON.stringify(fitLevers(input, choice)) ===
      JSON.stringify(fitLevers(input, choice)),
  );
}

// ── 10. Nothing lands past closing ──────────────────────────────────────────

{
  const payload = day(FIVE);
  const entries = [block("lunch", 12 * 60, 60)];
  const add = headlinersToAdd(payload, entries, undefined);
  const input = inputFor(payload, entries, add);
  const outcome = evaluateFit(input, fitChoiceAll());
  check(
    "10a every stop starts before the park shuts",
    outcome.stops.every((stop: any) => stop.startMinute < input.grid.closeMin),
    JSON.stringify(
      outcome.stops.map((s: any) => `${s.attractionSlug}@${s.startMinute}`),
    ),
  );
}

// ── 11. A second go on a ride is given up before a ride nobody has had ─────
//
// Reported on Phantasialand: the assistant kept Chiapas three times and Winja's
// Force twice and struck Winja's Fear and Raik. The weights were keyed on the
// slug, so a lap took the rank of the ride's first go.

{
  const payload = day(
    ["a", "b", "c", "d", "e", "f"].map((slug: any) => ride(slug, 60)),
  );
  const entries = [
    entry("a1", "a", 540),
    entry("b1", "b", 600),
    entry("a2", "a", 660),
    entry("c1", "c", 720),
    entry("d1", "d", 780),
    entry("b2", "b", 840),
    entry("e1", "e", 900),
    entry("f1", "f", 960),
  ];
  const input = inputFor(payload, entries, []);
  const choice = fitChoiceAll();
  const outcome = evaluateFit(input, choice);
  const lapKeys = ["a2", "b2"].map(entryWishKey);
  check(
    "11a eight hour-long queues in a six-hour day leave some out",
    outcome.missed.length >= 2,
    names(input, outcome.missed),
  );
  check(
    "11b and both second goes are among them",
    lapKeys.every((key: any) => outcome.missed.includes(key)),
    outcome.missed.join(", "),
  );
  const order = fitOrder(input, choice).map((wish: any) => wish.key);
  check(
    "11c the list shows the second goes at the bottom",
    JSON.stringify(order.slice(-2)) === JSON.stringify(lapKeys),
    order.join(", "),
  );
  // Pinned, a lap is the visitor's word and stays where they put it.
  const pinned = togglePin(choice, entryWishKey("b2"));
  check(
    "11d a pinned second go stays at the top",
    fitOrder(input, pinned)[0]?.key === entryWishKey("b2"),
    fitOrder(input, pinned)
      .map((wish: any) => wish.key)
      .join(", "),
  );
}

// ── Early entry: the hour before the gates is room the assistant can use (PAR-199)

{
  const rides = ["a", "b", "c", "d", "e"].map((slug: any) => ride(slug, 70));
  const early = { hasEarlyEntry: true, earlyEntryMinutesPeak: 60 };
  const run = (context: any) => {
    const payload = day(rides, context);
    const add = headlinersToAdd(payload, [], undefined);
    const input = inputFor(payload, [], add);
    return { input, outcome: evaluateFit(input, fitChoiceAll()) };
  };
  const off = run(early);
  const on = run({ ...early, earlyEntry: true });
  check(
    "EE1 five 70-minute headliners do not all fit a six-hour day",
    off.outcome.fitted.length < 5,
    `${off.outcome.fitted.length}`,
  );
  check(
    "EE2 so the assistant is needed",
    needsFitHelp(off.input, fitChoiceAll()) === true,
  );
  check(
    "EE3 with early entry the hour before the gates holds one more",
    on.outcome.fitted.length > off.outcome.fitted.length,
    `${on.outcome.fitted.length} gegen ${off.outcome.fitted.length}`,
  );
  check(
    "EE4 and a stop of it is filed before the park opens",
    on.outcome.stops.some((stop: any) => stop.startMinute < OPEN * 60),
    JSON.stringify(on.outcome.stops.map((stop: any) => stop.startMinute)),
  );
}

// ── 12. One press on „In den Plan" asks the same question (PAR-67) ──────────
//
// The ride-page button files without a minute. Where the day has no room for
// the ride it must open the assistant on that question instead of filing past
// closing, and where the question cannot be asked it must file as before.

{
  // Long before the fixture's date, so the clock floors nothing.
  const clock = dayClock(
    "2026-09-12",
    "Europe/Berlin",
    Date.UTC(2026, 8, 1, 8),
  );
  const full = day([...FIVE_LONG, ride("f", 60)]);
  const planned = FIVE_LONG.map((r: any, i: any) =>
    entry(`e${i}`, r.attractionSlug, (OPEN + i) * 60),
  );

  const conflict = noRoomForRide({
    day: full,
    entries: planned,
    attractionSlug: "f",
    clock,
  });
  check("12a a full day plus one ride is a question", conflict !== null);
  check(
    "12b and the question names the requested ride as the one addition",
    conflict?.wishes
      .filter((wish: any) => wish.entryId === null)
      .map((wish: any) => wish.key)
      .join() === addWishKey("f"),
    JSON.stringify(conflict?.wishes.map((wish: any) => wish.key)),
  );
  check(
    "12c and it is the question the optimise buttons ask",
    conflict !== null && needsFitHelp(conflict, fitChoiceAll()) === true,
  );

  const roomy = day([ride("a", 30), ride("b", 30), ride("c", 30)]);
  check(
    "12d a day with room files the ride as before",
    noRoomForRide({
      day: roomy,
      entries: [entry("e0", "a", 10 * 60)],
      attractionSlug: "b",
      clock,
    }) === null,
  );
  check(
    "12e a ride the payload does not list is not a conflict",
    noRoomForRide({
      day: full,
      entries: planned,
      attractionSlug: "nope",
      clock,
    }) === null,
  );
  check(
    "12f no payload is not a conflict",
    noRoomForRide({
      day: null,
      entries: planned,
      attractionSlug: "f",
      clock,
    }) === null,
  );
  const walked = dayClock(
    "2026-09-12",
    "Europe/Berlin",
    Date.UTC(2026, 8, 20, 8),
  );
  check(
    "12g a day already walked is not asked about",
    noRoomForRide({
      day: full,
      entries: planned,
      attractionSlug: "f",
      clock: walked,
    }) === null,
  );
  const unread = day([...FIVE_LONG, ride("f", 60)], {
    liveWaitTimes: { available: false, reason: "not_published" },
  });
  check(
    "12h a park whose waits nobody can read is not asked about",
    noRoomForRide({
      day: unread,
      entries: planned,
      attractionSlug: "f",
      clock,
    }) === null,
  );

  // Late on the day itself: 13:10 in a park shutting at 15:00 holds one
  // hour-long queue, not three.
  const late = dayClock(
    "2026-09-12",
    "Europe/Berlin",
    Date.UTC(2026, 8, 12, 11, 10),
  );
  const three = day(["a", "b", "c"].map((slug: any) => ride(slug, 60)));
  check(
    "12l the first press at 13:10 files",
    noRoomForRide({
      day: three,
      entries: [],
      attractionSlug: "a",
      clock: late,
    }) === null,
  );
  check(
    "12m the third press at 13:10 asks",
    noRoomForRide({
      day: three,
      entries: [entry("e0", "a", 13 * 60 + 15), entry("e1", "b", 14 * 60 + 15)],
      attractionSlug: "c",
      clock: late,
    }) !== null,
  );

  // „Nochmal": a second go on a planned ride is a wish of its own. Repeated
  // presses on one ride are how the 25:00 stack was reached.
  const once = [entry("e0", "a", 13 * 60 + 15), entry("e1", "b", 14 * 60 + 15)];
  check(
    "12i two hour-long queues from 13:15 fit on their own",
    needsFitHelp(inputFor(three, once, [], late), fitChoiceAll()) === false,
  );
  const lap = noRoomForRide({
    day: three,
    entries: once,
    attractionSlug: "a",
    clock: late,
  });
  check("12j and a second go on it does not", lap !== null);
  check(
    "12k the second go is in the question, as an addition",
    lap?.wishes.some(
      (wish: any) => wish.key === addWishKey("a") && wish.entryId === null,
    ) === true,
    JSON.stringify(lap?.wishes.map((wish: any) => wish.key)),
  );
}

// ── 13. The pressed ride opens pinned (PAR-637) ───────────────────────────────
//
// Unpinned, the ride the visitor just asked for was one wish among the rest,
// and the plan the dialog proposed could leave exactly that ride out.

{
  const clock = dayClock(
    "2026-09-12",
    "Europe/Berlin",
    Date.UTC(2026, 8, 1, 8),
  );
  const full = day([...FIVE_LONG, ride("f", 60)]);
  const planned = FIVE_LONG.map((r: any, i: any) =>
    entry(`e${i}`, r.attractionSlug, (OPEN + i) * 60),
  );
  const conflict = noRoomForRide({
    day: full,
    entries: planned,
    attractionSlug: "f",
    clock,
  });
  const opening = requestedRideChoice("f");
  const key = addWishKey("f");

  check(
    "13a the dialog opens with the pressed ride pinned and nothing else",
    opening.priority.join() === key,
    JSON.stringify(opening.priority),
  );
  check(
    "13b and with everything ticked, nothing shortened",
    opening.dropped.size === 0 &&
      opening.droppedBlocks.size === 0 &&
      opening.shortBlocks.size === 0,
  );
  check(
    "13c the pin is on a wish the question holds",
    conflict?.wishes.some((wish: any) => wish.key === key) === true,
  );
  check(
    "13d the pinned ride heads the order the engine gives things up in",
    conflict !== null && fitOrder(conflict, opening)[0]?.key === key,
  );
  check(
    "13e the proposed plan keeps the pressed ride",
    conflict !== null && evaluateFit(conflict, opening).fitted.includes(key),
    JSON.stringify(conflict && evaluateFit(conflict, opening).fitted),
  );
  check(
    "13e2 where unpinned, this fixture gives up exactly the pressed ride",
    conflict !== null &&
      !evaluateFit(conflict, fitChoiceAll()).fitted.includes(key),
  );
  check(
    "13f the day is still a question with the pin: something else has to go",
    conflict !== null && needsFitHelp(conflict, opening) === true,
  );
  check(
    "13g the visitor can take the pin off again",
    togglePin(opening, key).priority.length === 0,
  );

  // A second go is pinned under the same key it is asked about.
  const late = dayClock(
    "2026-09-12",
    "Europe/Berlin",
    Date.UTC(2026, 8, 12, 11, 10),
  );
  const three = day(["a", "b", "c"].map((slug: any) => ride(slug, 60)));
  const once = [entry("e0", "a", 13 * 60 + 15), entry("e1", "b", 14 * 60 + 15)];
  const lap = noRoomForRide({
    day: three,
    entries: once,
    attractionSlug: "a",
    clock: late,
  });
  check(
    "13h a pressed lap is pinned and kept",
    lap !== null &&
      fitOrder(lap, requestedRideChoice("a"))[0]?.key === addWishKey("a") &&
      evaluateFit(lap, requestedRideChoice("a")).fitted.includes(
        addWishKey("a"),
      ),
    JSON.stringify(lap && evaluateFit(lap, requestedRideChoice("a")).fitted),
  );
}

// ── 14. A show with coordinates costs the walk to it and from it ───────────
//
// The same day twice, once with the show located 1.5 km from the rides and once
// with the show carrying no coordinates, which is the normal case and must be
// what it always was. Everything else is identical: five 55-minute headliners
// in a six-hour day, a half-hour show as the first block of the morning.

{
  const SHOW_SLUG = "sh";
  const showEntry = {
    id: "s",
    showSlug: SHOW_SLUG,
    custom: { label: "Show", icon: "show", durationMinutes: 30 },
    startMinute: OPEN * 60,
  };
  const withShow = (lat: any) => {
    const payload = day(
      ["a", "b", "c", "d", "e"].map((slug: any) => ride(slug, 55)),
    );
    payload.shows = [
      {
        showSlug: SHOW_SLUG,
        showName: "Show",
        times: ["09:00"],
        source: "scheduled",
        ...(lat === null ? {} : { latitude: lat, longitude: 6.87 }),
      },
    ];
    return payload;
  };
  const planFor = (payload: any) => {
    const entries = [showEntry];
    const add = headlinersToAdd(payload, entries, undefined);
    const input = inputFor(payload, entries, add);
    return {
      input,
      add,
      plan: optimizeDay({ day: payload, grid: input.grid, entries, add }),
    };
  };

  const none = planFor(withShow(null));
  const far = planFor(withShow(50.8135));
  const showEnds = showEntry.startMinute + 30;
  const walk = transferBetween(
    entryPlace(far.input.day, showEntry),
    far.input.day.rides[0],
    null,
    {
      fromBlock: true,
    },
  ).ceilingMinutes;

  check(
    "14a a show with no coordinates has no place and no walk",
    entryPlace(none.input.day, showEntry) === null &&
      none.plan.stops[0].startMinute === showEnds,
    `${none.plan.stops[0].startMinute}`,
  );
  check(
    "14b a located show is a place, as a ride is",
    entryPlace(far.input.day, showEntry)?.latitude === 50.8135,
  );
  check(
    "14b2 a position that is not on the globe is no place",
    [Number.NaN, Infinity, 91].every(
      (lat: any) => entryPlace(withShow(lat), showEntry) === null,
    ),
  );
  check(
    "14c the first ride after it starts a walk later, not when the show ends",
    far.plan.stops[0].startMinute >= showEnds + walk,
    `${far.plan.stops[0].startMinute} against ${showEnds + walk}`,
  );
  check(
    "14d the same walk holds the other way round, in front of the show",
    (() => {
      const early = { ...showEntry, startMinute: 11 * 60 };
      const payload = withShow(50.8135);
      const input = inputFor(
        payload,
        [early],
        headlinersToAdd(payload, [early], undefined),
      );
      const plan = optimizeDay({
        day: payload,
        grid: input.grid,
        entries: [early],
        add: headlinersToAdd(payload, [early], undefined),
      });
      const before = plan.stops.filter(
        (stop: any) => stop.startMinute < early.startMinute,
      );
      const last = before[before.length - 1];
      return (
        last !== undefined && last.startMinute + 55 + walk <= early.startMinute
      );
    })(),
  );
  check(
    "14e the day that fitted without the walk no longer does, and the assistant is asked",
    evaluateFit(none.input, fitChoiceAll()).missed.length === 0 &&
      needsFitHelp(none.input, fitChoiceAll()) === false &&
      evaluateFit(far.input, fitChoiceAll()).missed.length === 1 &&
      needsFitHelp(far.input, fitChoiceAll()) === true,
    `${evaluateFit(none.input, fitChoiceAll()).missed.length} / ${evaluateFit(far.input, fitChoiceAll()).missed.length}`,
  );

  // The clash count reads the same walk: fifteen minutes between the end of a
  // 55-minute queue and the show is enough with no position and not enough with one.
  const ten = [
    { id: "ea", attractionSlug: "a", attractionName: "a", startMinute: 540 },
    { ...showEntry, startMinute: 610 },
  ];
  check(
    "14f a show fifteen minutes after the end of a queue is a clash with the walk and none without it",
    clashCount(withShow(null), ten) === 0 &&
      clashCount(withShow(50.8135), ten) === 1,
    `${clashCount(withShow(null), ten)} / ${clashCount(withShow(50.8135), ten)}`,
  );

  // PAR-696 (PO, evening): the other direction keeps the exit and the ride, also
  // for a show with no position. A show eight minutes after the queue ends is
  // fine, one minute earlier clashes, and the optimiser files the same eight.
  const beforeShow = (startMinute: any) => [
    { id: "ea", attractionSlug: "a", attractionName: "a", startMinute: 540 },
    { ...showEntry, startMinute },
  ];
  check(
    "14j a ride in front of a show with no position pays exit and ride, to the minute",
    clashCount(
      withShow(null),
      beforeShow(595 + EXIT_MIN + RIDE_DURATION_MIN - 1),
    ) === 1 &&
      clashCount(
        withShow(null),
        beforeShow(595 + EXIT_MIN + RIDE_DURATION_MIN),
      ) === 0,
    `${clashCount(withShow(null), beforeShow(602))} / ${clashCount(withShow(null), beforeShow(603))}`,
  );
  check(
    "14k the optimiser leaves the same eight minutes in front of that show",
    (() => {
      const early = { ...showEntry, startMinute: 11 * 60 };
      const payload = withShow(null);
      const add = headlinersToAdd(payload, [early], undefined);
      const input = inputFor(payload, [early], add);
      const plan = optimizeDay({
        day: payload,
        grid: input.grid,
        entries: [early],
        add,
      });
      const before = plan.stops.filter(
        (stop: any) => stop.startMinute < early.startMinute,
      );
      const last = before[before.length - 1];
      return (
        last !== undefined &&
        last.startMinute + 55 + EXIT_MIN + RIDE_DURATION_MIN <=
          early.startMinute
      );
    })(),
  );

  // PAR-696: nothing is left behind after a show, so no exit and no ride are
  // charged. A ride at the minute the show ends is no clash with no position,
  // and with one it clashes by exactly the walk, not by the walk plus eight.
  const afterShow = (startMinute: any) => [
    { ...showEntry },
    { id: "ea", attractionSlug: "a", attractionName: "a", startMinute },
  ];
  const floor = transferBetween(
    entryPlace(far.input.day, showEntry),
    far.input.day.rides[0],
    null,
    { fromBlock: true },
  ).floorMinutes;
  check(
    "14g a ride that starts when a show with no position ends is no clash",
    clashCount(withShow(null), afterShow(showEnds)) === 0,
    `${clashCount(withShow(null), afterShow(showEnds))}`,
  );
  check(
    "14h after a located show only the walk is charged, to the minute",
    floor > 0 &&
      clashCount(withShow(50.8135), afterShow(showEnds + floor)) === 0 &&
      clashCount(withShow(50.8135), afterShow(showEnds + floor - 1)) === 1,
    `${floor}`,
  );
  check(
    "14i optimiser and clash count agree on a show with no position: the plan has none",
    clashCount(none.input.day, [
      showEntry,
      ...none.plan.stops.map((stop: any) => ({ ...stop })),
    ]) === 0,
  );
}

describe("fit assistant vectors", () => {
  it("holds every vector ported from park.fan", () => {
    expect(failures).toEqual([]);
  });
});

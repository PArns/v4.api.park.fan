/**
 * Records what park.fan's browser optimizer answered, so the API's port can be held to it.
 *
 * Runs the frontend's own `lib/planner/*` (the checkout next to this one) over the three days in
 * `<payloads>` and a set of seeded synthetic ones, through the same call shapes the planner's
 * buttons, fit assistant and "add to plan" button used, and writes inputs and outputs to
 * `src/planner/day-optimizer/__fixtures__/browser-parity.json.gz`. `browser-parity.spec.ts` replays
 * every input through the port and expects the same answers.
 *
 * The frontend no longer ships the search, so this runs against the commit before it went:
 *
 *     git -C ../park.fan worktree add /tmp/fe-parity <commit before the removal>
 *     node --experimental-strip-types \
 *       --import /tmp/fe-parity/scripts/register-path-alias.mjs \
 *       src/planner/day-optimizer/__fixtures__/record-browser-parity.mjs /tmp/fe-parity <payload dir>
 *
 * `<payload dir>` holds `/plan/day` responses saved as JSON, one file per day.
 */

import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { gzipSync } from "node:zlib";
import { pathToFileURL } from "node:url";

const [frontend, payloadDir] = process.argv.slice(2);
if (!frontend || !payloadDir) {
  console.error(
    "usage: record-browser-parity.mjs <park.fan checkout> <payload dir>",
  );
  process.exit(2);
}
const fe = (path) => import(pathToFileURL(resolve(frontend, path)).href);

const grid = await fe("lib/planner/day-grid.ts");
const estimate = await fe("lib/planner/estimate.ts");
const optimize = await fe("lib/planner/optimize.ts");
const fit = await fe("lib/planner/fit.ts");
const addRideFit = await fe("lib/planner/add-ride-fit.ts");

/** A seeded generator, so the fixture is the same on every run. */
function mulberry32(seed) {
  let a = seed >>> 0;
  return () => {
    a = (a + 0x6d2b79f5) >>> 0;
    let t = a;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}
const rand = mulberry32(1290);
const int = (lo, hi) => lo + Math.floor(rand() * (hi - lo + 1));
const pick = (list) => list[Math.floor(rand() * list.length)];
const chance = (p) => rand() < p;
function shuffle(list) {
  const out = [...list];
  for (let i = out.length - 1; i > 0; i--) {
    const j = Math.floor(rand() * (i + 1));
    [out[i], out[j]] = [out[j], out[i]];
  }
  return out;
}

/** Only the fields the optimizer reads, so the fixture stays small. */
function trim(payload) {
  const c = payload.context;
  return {
    tier: payload.tier,
    context: {
      date: c.date,
      openHour: c.openHour,
      closeHour: c.closeHour,
      ...(c.liveWaitTimes && { liveWaitTimes: c.liveWaitTimes }),
      ...(c.hasEarlyEntry && { hasEarlyEntry: c.hasEarlyEntry }),
      ...(c.earlyEntryMinutesPeak && {
        earlyEntryMinutesPeak: c.earlyEntryMinutesPeak,
      }),
    },
    rides: payload.rides.map((r) => {
      const out = {};
      for (const key of [
        "attractionSlug",
        "attractionName",
        "hours",
        "slots",
        "dayPeak",
        "sampleDays",
        "latitude",
        "longitude",
        "land",
        "uncertaintyMinutes",
        "expectedError",
        "opensAt",
        "isHeadliner",
        "minimumHeight",
        "mayGetWet",
      ]) {
        if (r[key] !== undefined) out[key] = r[key];
      }
      return out;
    }),
    shows: (payload.shows ?? []).map((s) => ({
      showSlug: s.showSlug,
      latitude: s.latitude ?? null,
      longitude: s.longitude ?? null,
    })),
    ...(payload.ridesInWorks && { ridesInWorks: payload.ridesInWorks }),
  };
}

const LANDS = ["North", "South", "East", "West"];

function syntheticDay(index) {
  const lateNight = chance(0.12);
  const openHour = lateNight ? int(15, 17) : int(8, 11);
  const length = lateNight ? int(8, 10) : int(6, 13);
  const closeHour = (openHour + length) % 24;
  const lastHour = openHour + length;
  const slotted = chance(0.4);
  const rideCount = int(6, 22);
  const centre = [50.8 + rand(), 6.8 + rand()];
  const rides = [];
  for (let i = 0; i < rideCount; i++) {
    const base = int(5, 45);
    const peak = base + int(0, 60);
    const turn = int(openHour + 1, lastHour - 1);
    const hours = [];
    const slots = [];
    const from = openHour - (chance(0.2) ? 1 : 0);
    const to = lastHour + (chance(0.2) ? 1 : 0);
    const startsLate = chance(0.15) ? int(1, 3) : 0;
    for (let h = from + startsLate; h <= to; h++) {
      const wait = Math.round((h < turn ? base : peak) / 5) * 5;
      hours.push({
        hour: h,
        wait,
        ...(chance(0.1) && { source: pick(["observed", "measured"]) }),
      });
      if (slotted && chance(0.85)) {
        // Now and then a quarter is missing, which is a ride not running then.
        for (let q = 0; q < 4; q++) {
          if (chance(0.06)) continue;
          slots.push({
            minute: h * 60 + q * 15,
            wait: Math.max(0, wait + int(-8, 8)),
          });
        }
      }
    }
    const positioned = chance(0.9);
    rides.push({
      attractionSlug: `ride-${index}-${i}${chance(0.2) ? "-x" : ""}`,
      attractionName: `Ride ${index}.${i}`,
      hours,
      ...(slots.length > 0 && { slots }),
      dayPeak: peak,
      sampleDays: pick([5, 20, 30, 60, 200]),
      latitude: positioned ? centre[0] + (rand() - 0.5) * 0.012 : null,
      longitude: positioned ? centre[1] + (rand() - 0.5) * 0.012 : null,
      land: chance(0.85) ? pick(LANDS) : null,
      uncertaintyMinutes: chance(0.5) ? int(2, 20) : null,
      ...(chance(0.3) && { expectedError: int(3, 25) }),
      ...(chance(0.12) && {
        opensAt: pick([
          `${String(openHour + 1).padStart(2, "0")}:00`,
          `${String(openHour).padStart(2, "0")}:30`,
          "noon",
        ]),
      }),
      isHeadliner: chance(0.3),
      ...(chance(0.4) && {
        minimumHeight: pick([90, 100, 110, 120, 130, 140]),
      }),
      ...(chance(0.3) && { mayGetWet: chance(0.5) }),
    });
  }
  const early = chance(0.3);
  return {
    tier: pick([
      "measured",
      "composed",
      "climatology",
      "observed",
      "long_range",
    ]),
    context: {
      date: "2026-11-14",
      openHour,
      closeHour,
      liveWaitTimes: chance(0.05)
        ? { available: false, reason: "not_published" }
        : { available: true },
      ...(early && {
        hasEarlyEntry: true,
        earlyEntryMinutesPeak: pick([30, 60, 45]),
      }),
    },
    rides,
    shows: Array.from({ length: int(0, 3) }, (_, i) => ({
      showSlug: `show-${index}-${i}`,
      latitude: chance(0.7) ? centre[0] + (rand() - 0.5) * 0.01 : null,
      longitude: chance(0.7) ? centre[1] + (rand() - 0.5) * 0.01 : null,
    })),
    ...(chance(0.3) && {
      ridesInWorks: [
        {
          attractionSlug: `works-${index}`,
          attractionName: `Works ${index}`,
          worksPeriod: { from: "2026-11-01", to: null, toUncertain: false },
        },
      ],
    }),
  };
}

function entriesFor(day, maxRides) {
  const { openHour, closeHour } = day.context;
  const openMin = (openHour ?? 9) * 60;
  const closeMin = grid.unfoldedCloseHour(openHour ?? 9, closeHour ?? 18) * 60;
  const minute = () => int(openMin - 30, closeMin + 60);
  const snap = (m) => Math.max(0, Math.round(m / 5) * 5);
  const entries = [];
  const rideSlugs = shuffle(day.rides.map((r) => r.attractionSlug)).slice(
    0,
    int(1, maxRides),
  );
  let n = 0;
  for (const slug of rideSlugs) {
    const ride = day.rides.find((r) => r.attractionSlug === slug);
    entries.push({
      id: `e${n++}`,
      attractionSlug: slug,
      attractionName: ride.attractionName,
      startMinute: snap(minute()),
      ...(chance(0.12) && {
        done: true,
        ...(chance(0.7) && { actualWait: int(0, 60) }),
      }),
    });
    if (chance(0.08)) {
      entries.push({
        id: `e${n++}`,
        attractionSlug: slug,
        attractionName: ride.attractionName,
        startMinute: snap(minute()),
      });
    }
  }
  const blocks = int(0, 2);
  for (let i = 0; i < blocks; i++) {
    entries.push({
      id: `b${i}`,
      custom: {
        label: `Block ${i}`,
        icon: pick(["food", "break", "shop"]),
        durationMinutes: pick([20, 30, 45, 60, 90]),
      },
      startMinute: snap(minute()),
      ...(chance(0.1) && { done: true }),
    });
  }
  if (day.shows?.length && chance(0.4)) {
    const show = pick(day.shows);
    entries.push({
      id: "s0",
      showSlug: show.showSlug,
      custom: {
        label: show.showSlug,
        icon: "show",
        durationMinutes: pick([20, 30]),
      },
      startMinute: snap(minute()),
    });
  }
  if (day.ridesInWorks?.length && chance(0.5)) {
    const works = day.ridesInWorks[0];
    entries.push({
      id: "w0",
      attractionSlug: works.attractionSlug,
      attractionName: works.attractionName,
      startMinute: snap(minute()),
    });
  }
  if (chance(0.1)) {
    entries.push({
      id: "u0",
      attractionSlug: "not-in-the-payload",
      attractionName: "Unknown",
      startMinute: snap(minute()),
    });
  }
  return shuffle(entries);
}

function clockFor(day) {
  const r = rand();
  if (r < 0.08) return { phase: "past" };
  if (r < 0.5) return { phase: "future" };
  const openMin = (day.context.openHour ?? 9) * 60;
  return {
    phase: "today",
    nowMinute: Math.min(1439, int(openMin - 60, openMin + 9 * 60)),
  };
}

function prefsFor(day) {
  const prefs = {};
  if (day.context.hasEarlyEntry && chance(0.7)) prefs.earlyEntry = true;
  if (chance(0.3)) prefs.riderHeightCm = pick([95, 110, 125, 140]);
  if (chance(0.2)) prefs.avoidWet = true;
  return prefs;
}

function randomChoice(input) {
  const keys = input.wishes.map((w) => w.key);
  const blocks = input.blocks.map((b) => b.entryId);
  const dropped = keys.filter(() => chance(0.15));
  const droppedBlocks = blocks.filter(() => chance(0.3));
  const shortBlocks = blocks.filter(
    (id) => !droppedBlocks.includes(id) && chance(0.3),
  );
  const priority = shuffle(keys.filter((k) => !dropped.includes(k))).slice(
    0,
    int(0, 3),
  );
  return { dropped, droppedBlocks, shortBlocks, priority };
}

const asChoice = (c) => ({
  dropped: new Set(c.dropped),
  droppedBlocks: new Set(c.droppedBlocks),
  shortBlocks: new Set(c.shortBlocks),
  priority: [...c.priority],
});

const plain = (value) => JSON.parse(JSON.stringify(value ?? null));

function fitAnswer(input, choice, outcome = fit.evaluateFit(input, choice)) {
  const view = fit.fitLeverView(input, choice, outcome);
  return plain({
    wishes: input.wishes.map((w) => ({
      key: w.key,
      entryId: w.entryId,
      replannable: w.ride !== null,
    })),
    order: fit.fitOrder(input, choice).map((w) => w.key),
    outcome,
    levers: view.levers,
    applied: [...view.applied].sort(),
  });
}

function scenario(dayId, payload, opts) {
  const entries = entriesFor(payload, opts.maxRides);
  const clock = opts.clock ?? clockFor(payload);
  const prefs = prefsFor(payload);
  const day = grid.withEarlyEntry(payload, prefs.earlyEntry);
  // The panel's own grid, grown to the plan, which the optimizer must not notice.
  const g = grid.growGridForSpans(
    grid.buildDayGrid(
      day.context.openHour,
      day.context.closeHour,
      undefined,
      grid.earlyEntryOpenMin(day.context),
    ),
    estimate.spansFor(day, entries),
  );
  const expect = {
    canOptimize: optimize.canOptimize(day, g),
    movable: optimize.movableEntries(entries, clock, day).map((e) => e.id),
  };
  const out = { dayId, entries, clock, prefs, expect };
  if (!expect.canOptimize || clock.phase === "past" || !g) return out;

  const headliners = optimize.headlinersToAdd(day, entries, prefs);
  expect.headliners = headliners.map((r) => r.attractionSlug);
  expect.skipped = optimize.headlinersSkipped(day, entries, prefs);
  const base = { day, grid: g, entries, clock };
  expect.scoreCurrent = plain(optimize.scoreCurrent(base));
  expect.clashCount = optimize.clashCount(day, entries, clock);
  expect.optimizeDay = plain(optimize.optimizeDay(base));
  if (headliners.length > 0) {
    expect.optimizeHeadliners = plain(
      optimize.optimizeDay({ ...base, add: headliners }),
    );
  }

  // The press: the fit question first, as `attempt` asked it.
  const choiceMode =
    headliners.length > 0 && chance(0.5) ? "headliners" : "day";
  for (const [mode, add] of [
    ["day", []],
    ["headliners", headliners],
  ]) {
    if (mode === "headliners" && add.length === 0) continue;
    const input = {
      day,
      grid: g,
      entries,
      wishes: fit.fitWishes(day, entries, add, clock),
      blocks: fit.fitBlocks(entries),
      clock,
    };
    const all = fit.fitChoiceAll();
    const needsFit = input.wishes.length > 0 && fit.needsFitHelp(input, all);
    expect[`press_${mode}`] = {
      needsFit,
      ...(needsFit && { fit: fitAnswer(input, all) }),
    };
    if (mode === choiceMode) {
      const choice = randomChoice(input);
      out[`choice_${mode}`] = choice;
      expect[`fit_${mode}`] = fitAnswer(input, asChoice(choice));
    }
  }

  // "Add to plan": the question for one ride, and the answer the assistant opens on.
  const requested = pick(day.rides).attractionSlug;
  out.requested = requested;
  const conflict = addRideFit.noRoomForRide({
    day: payload,
    entries,
    attractionSlug: requested,
    clock,
    earlyEntry: prefs.earlyEntry,
  });
  expect.noRoom = conflict !== null;
  if (conflict)
    expect.requestedFit = fitAnswer(
      conflict,
      addRideFit.requestedRideChoice(requested),
    );
  return out;
}

const days = {};
for (const file of readdirSync(payloadDir)
  .filter((f) => f.endsWith(".json"))
  .sort()) {
  days[file.replace(/\.json$/, "")] = trim(
    JSON.parse(readFileSync(join(payloadDir, file), "utf8")),
  );
}
const realIds = Object.keys(days);
for (let i = 0; i < 18; i++) days[`synthetic-${i}`] = syntheticDay(i);

const scenarios = [];
for (const id of realIds) {
  for (let i = 0; i < 4; i++)
    scenarios.push(scenario(id, days[id], { maxRides: i === 0 ? 20 : 10 }));
}
for (let i = 0; i < 18; i++) {
  const id = `synthetic-${i}`;
  for (let k = 0; k < 3; k++)
    scenarios.push(scenario(id, days[id], { maxRides: k === 0 ? 16 : 9 }));
}

const out = resolve(
  "src/planner/day-optimizer/__fixtures__/browser-parity.json.gz",
);
writeFileSync(
  out,
  gzipSync(
    JSON.stringify({
      generatedFrom: "park.fan lib/planner (browser engine)",
      days,
      scenarios,
    }),
    { level: 9 },
  ),
);
console.log(
  `${scenarios.length} scenarios over ${Object.keys(days).length} days → ${out}`,
);

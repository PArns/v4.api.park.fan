import { readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { gunzipSync, gzipSync } from "zlib";
import { noRoomForRide, requestedRideChoice } from "./add-ride-fit";
import { gridForDay, withEarlyEntry } from "./day-grid";
import { runDayOptimizer, type FitResultView } from "./engine";
import {
  evaluateFit,
  fitBlocks,
  fitChoiceAll,
  fitLeverView,
  fitOrder,
  fitWishes,
  needsFitHelp,
  type FitChoice,
  type FitInput,
} from "./fit";
import {
  canOptimize,
  clashCount,
  headlinersSkipped,
  headlinersToAdd,
  movableEntries,
  optimizeDay,
  scoreCurrent,
} from "./optimize";
import type {
  DayClock,
  OptimizerDay,
  PlannerDayPrefs,
  PlannerEntry,
} from "./types";

/**
 * The engine against answers recorded from it, over the cases the browser engine was ported with.
 *
 * `__fixtures__/browser-parity.json.gz` holds three real `/plan/day` days and eighteen seeded
 * synthetic ones (slots, early entry, works, past midnight, unreadable parks), with the call shapes
 * the planner's buttons, fit assistant and "add to plan" button use. The inputs and the first
 * answers are what park.fan's browser optimizer produced (`__fixtures__/record-browser-parity.mjs`
 * says how). A change to the engine that is meant to move answers re-records them from this file:
 *
 *     RECORD_PARITY=1 pnpm test -- browser-parity
 *
 * The diff of the fixture is then the change, and every other run holds it to the same orders,
 * start minutes, scores, levers and refusals.
 */

interface Choice {
  dropped: string[];
  droppedBlocks: string[];
  shortBlocks: string[];
  priority: string[];
}

interface Scenario {
  dayId: string;
  entries: PlannerEntry[];
  clock: DayClock;
  prefs: PlannerDayPrefs;
  choice_day?: Choice;
  choice_headliners?: Choice;
  requested?: string;
  expect: Record<string, any>;
}

const FIXTURE = join(__dirname, "__fixtures__", "browser-parity.json.gz");

const fixture: {
  generatedFrom?: string;
  days: Record<string, OptimizerDay>;
  scenarios: Scenario[];
} = JSON.parse(gunzipSync(readFileSync(FIXTURE)).toString("utf8"));

/** The answer as it would leave in JSON, which is how the browser's was recorded. */
const plain = (value: unknown) => JSON.parse(JSON.stringify(value ?? null));

const asChoice = (choice: Choice): FitChoice => ({
  dropped: new Set(choice.dropped),
  droppedBlocks: new Set(choice.droppedBlocks),
  shortBlocks: new Set(choice.shortBlocks),
  priority: [...choice.priority],
});

/** The fit answer in the shape the fixture recorded. */
function fitAnswer(
  input: FitInput,
  choice: FitChoice,
  outcome = evaluateFit(input, choice),
) {
  const view = fitLeverView(input, choice, outcome);
  return plain({
    wishes: input.wishes.map((w) => ({
      key: w.key,
      entryId: w.entryId,
      replannable: w.ride !== null,
    })),
    order: fitOrder(input, choice).map((w) => w.key),
    outcome,
    levers: view.levers,
    applied: [...view.applied].sort(),
  });
}

/** The engine's fit view in the same shape, so a request-level answer compares too. */
function viewAnswer(view: FitResultView | null) {
  if (!view) return null;
  return plain({
    wishes: view.wishes.map((w) => ({
      key: w.key,
      entryId: w.entryId,
      replannable: w.replannable,
    })),
    order: view.order,
    outcome: {
      fitted: view.fitted,
      missed: view.missed,
      pinnedMissed: view.pinnedMissed,
      endMinute: view.endMinute,
      totalWaitMinutes: view.totalWaitMinutes,
      idleMinutes: view.idleMinutes,
      entries: view.entries,
      stops: view.stops,
    },
    levers: view.levers,
    applied: view.applied ? [...view.applied].sort() : null,
  });
}

/** Every answer a scenario records, computed the way the planner asks for it. */
function answersFor(scenario: Scenario): Record<string, any> {
  const payload = fixture.days[scenario.dayId];
  const { entries, clock, prefs } = scenario;
  const day = withEarlyEntry(payload, prefs.earlyEntry);
  const grid = gridForDay(day);
  const got: Record<string, any> = {
    canOptimize: canOptimize(day, grid),
    movable: movableEntries(entries, clock, day).map((e) => e.id),
  };
  // Refused before any search: a walked day, or a day nothing can be ordered on.
  if (!got.canOptimize || clock.phase === "past" || !grid) return got;

  const headliners = headlinersToAdd(day, entries, prefs);
  got.headliners = headliners.map((r) => r.attractionSlug);
  got.skipped = headlinersSkipped(day, entries, prefs);
  const base = { day, grid, entries, clock };
  got.scoreCurrent = plain(scoreCurrent(base));
  got.clashCount = clashCount(day, entries, clock);
  got.optimizeDay = plain(optimizeDay(base));
  if (headliners.length > 0) {
    got.optimizeHeadliners = plain(optimizeDay({ ...base, add: headliners }));
  }

  for (const mode of ["day", "headliners"] as const) {
    const add = mode === "headliners" ? headliners : [];
    if (mode === "headliners" && add.length === 0) continue;
    const input: FitInput = {
      day,
      grid,
      entries,
      wishes: fitWishes(day, entries, add, clock),
      blocks: fitBlocks(entries),
      clock,
    };
    const all = fitChoiceAll();
    const needsFit = input.wishes.length > 0 && needsFitHelp(input, all);
    got[`press_${mode}`] = {
      needsFit,
      ...(needsFit && { fit: fitAnswer(input, all) }),
    };
    const choice = scenario[`choice_${mode}`];
    if (choice) got[`fit_${mode}`] = fitAnswer(input, asChoice(choice));
  }

  const requested = scenario.requested as string;
  const conflict = noRoomForRide({
    day: payload,
    entries,
    attractionSlug: requested,
    clock,
    earlyEntry: prefs.earlyEntry,
  });
  got.noRoom = conflict !== null;
  if (conflict) {
    got.requestedFit = fitAnswer(conflict, requestedRideChoice(requested));
  }
  return got;
}

if (process.env.RECORD_PARITY) {
  for (const scenario of fixture.scenarios) {
    scenario.expect = answersFor(scenario);
  }
  fixture.generatedFrom = "v4.api.park.fan src/planner/day-optimizer";
  writeFileSync(FIXTURE, gzipSync(JSON.stringify(fixture), { level: 9 }));
}

describe("day optimizer: recorded answers", () => {
  it("covers the cases worth comparing", () => {
    const ran = fixture.scenarios.filter((s) => "optimizeDay" in s.expect);
    expect(fixture.scenarios.length).toBeGreaterThanOrEqual(60);
    expect(ran.length).toBeGreaterThanOrEqual(50);
    expect(
      ran.filter((s) => s.expect.optimizeDay !== null).length,
    ).toBeGreaterThan(30);
    expect(
      ran.filter((s) => s.expect.press_headliners?.needsFit).length,
    ).toBeGreaterThan(10);
    expect(ran.filter((s) => s.expect.noRoom).length).toBeGreaterThan(10);
  });

  it.each(fixture.scenarios.map((s, i) => [`${i} ${s.dayId}`, s] as const))(
    "scenario %s",
    (_name, scenario) => {
      const payload = fixture.days[scenario.dayId];
      const { entries, clock, prefs, expect: want } = scenario;
      const got = answersFor(scenario);
      for (const key of new Set([...Object.keys(want), ...Object.keys(got)])) {
        expect([key, got[key]]).toEqual([key, want[key]]);
      }

      const asked = (mode: "day" | "headliners" | "fit", extra: object = {}) =>
        runDayOptimizer(payload, { mode, entries, prefs, ...extra }, clock);

      if (!("optimizeDay" in want)) {
        expect(asked("day").status).toBe("refused");
        return;
      }

      const headliners = want.headliners as string[];
      for (const mode of ["day", "headliners"] as const) {
        const add = mode === "headliners" ? headliners : [];
        if (mode === "headliners" && add.length === 0) continue;
        const press = want[`press_${mode}`];

        // The same press through the request the endpoint answers.
        const answer = asked(mode);
        if (press.needsFit) {
          expect(answer.status).toBe("needs-fit");
          expect(viewAnswer(answer.fit)).toEqual(press.fit);
        } else {
          const plan =
            mode === "day" ? want.optimizeDay : want.optimizeHeadliners;
          expect(answer.status).toBe(plan ? "optimized" : "unchanged");
          expect(plain(answer.plan?.stops ?? null)).toEqual(
            plan?.stops ?? null,
          );
          expect(answer.before?.totalWaitMinutes).toBe(
            want.scoreCurrent?.totalWaitMinutes,
          );
          expect(answer.before?.clashes).toBe(
            want.scoreCurrent ? want.clashCount : undefined,
          );
        }

        const choice = scenario[`choice_${mode}`];
        if (choice) {
          const fit = asked("fit", { add, choice, levers: true });
          expect(fit.status).toBe("fit");
          expect(viewAnswer(fit.fit)).toEqual(want[`fit_${mode}`]);
        }
      }

      const requested = scenario.requested as string;
      const opening = requestedRideChoice(requested);
      // The button's two requests: is there room, and the answer the assistant opens on.
      const probe = asked("fit", { add: [requested] });
      expect(probe.fit && probe.fit.fitted.length < probe.fit.wanted).toBe(
        want.noRoom,
      );
      if (want.noRoom) {
        const opened = asked("fit", {
          add: [requested],
          choice: { priority: opening.priority },
          levers: true,
        });
        expect(viewAnswer(opened.fit)).toEqual(want.requestedFit);
      }
    },
  );
});

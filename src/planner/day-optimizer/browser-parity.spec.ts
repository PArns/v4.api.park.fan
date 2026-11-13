import { readFileSync } from "fs";
import { join } from "path";
import { gunzipSync } from "zlib";
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
 * The port against the engine it was ported from.
 *
 * `__fixtures__/browser-parity.json.gz` holds what park.fan's browser optimizer answered for three
 * real `/plan/day` days and eighteen seeded synthetic ones (slots, early entry, works, past
 * midnight, unreadable parks), through the call shapes its buttons, fit assistant and "add to plan"
 * button used. Every answer here has to be the same: the same orders, start minutes, scores,
 * levers and refusals. `__fixtures__/record-browser-parity.mjs` says how the fixture was made.
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

const fixture: {
  days: Record<string, OptimizerDay>;
  scenarios: Scenario[];
} = JSON.parse(
  gunzipSync(
    readFileSync(join(__dirname, "__fixtures__", "browser-parity.json.gz")),
  ).toString("utf8"),
);

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

describe("day optimizer: parity with the browser engine", () => {
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
      const day = withEarlyEntry(payload, prefs.earlyEntry);
      const grid = gridForDay(day);

      expect(canOptimize(day, grid)).toBe(want.canOptimize);
      expect(movableEntries(entries, clock, day).map((e) => e.id)).toEqual(
        want.movable,
      );

      const asked = (mode: "day" | "headliners" | "fit", extra: object = {}) =>
        runDayOptimizer(payload, { mode, entries, prefs, ...extra }, clock);

      if (!("optimizeDay" in want)) {
        // Refused before any search: a walked day, or a day nothing can be ordered on.
        expect(asked("day").status).toBe("refused");
        return;
      }
      if (!grid) throw new Error("a day the browser planned has an axis");

      const headliners = headlinersToAdd(day, entries, prefs);
      expect(headliners.map((r) => r.attractionSlug)).toEqual(want.headliners);
      expect(headlinersSkipped(day, entries, prefs)).toBe(want.skipped);

      const base = { day, grid, entries, clock };
      expect(plain(scoreCurrent(base))).toEqual(want.scoreCurrent);
      expect(clashCount(day, entries, clock)).toBe(want.clashCount);
      expect(plain(optimizeDay(base))).toEqual(want.optimizeDay);
      if (headliners.length > 0) {
        expect(plain(optimizeDay({ ...base, add: headliners }))).toEqual(
          want.optimizeHeadliners,
        );
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
        const press = want[`press_${mode}`];
        const all = fitChoiceAll();
        expect(input.wishes.length > 0 && needsFitHelp(input, all)).toBe(
          press.needsFit,
        );
        if (press.needsFit) expect(fitAnswer(input, all)).toEqual(press.fit);

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
          expect(fitAnswer(input, asChoice(choice))).toEqual(
            want[`fit_${mode}`],
          );
          const fit = asked("fit", {
            add: add.map((r) => r.attractionSlug),
            choice,
            levers: true,
          });
          expect(fit.status).toBe("fit");
          expect(viewAnswer(fit.fit)).toEqual(want[`fit_${mode}`]);
        }
      }

      const requested = scenario.requested as string;
      const conflict = noRoomForRide({
        day: payload,
        entries,
        attractionSlug: requested,
        clock,
        earlyEntry: prefs.earlyEntry,
      });
      expect(conflict !== null).toBe(want.noRoom);
      const opening = requestedRideChoice(requested);
      if (conflict) {
        expect(fitAnswer(conflict, opening)).toEqual(want.requestedFit);
      }
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

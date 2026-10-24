/**
 * Frozen decision reports (PAR-1059), pure parts: the verdict of a decision
 * on its evaluation window and the checks that keep a green verdict from
 * resting on a lopsided sample. The service hands in the frozen matrix and
 * the paired park × date cells; docs/ml/ml-overview.md §1a.
 */
import { addIsoDays } from "../../common/utils/date.util";
import { ModelComparisonResponseDto } from "../dto/model-comparison.dto";
import {
  MlDecisionReportCellDto,
  MlDecisionReportCheckDto,
  MlDecisionReportDto,
} from "../dto/ml-overview.dto";
import {
  COMPARISON_QUESTIONS,
  cellLeads,
  fmtDe,
} from "./model-comparison.util";
import {
  DecisionDef,
  decisionCells,
  deriveDecisionState,
} from "./ml-overview.util";

/** No weekday may carry more than this share of the paired park × date cells (28 days evenly = 14.3 %). */
export const WEEKDAY_MAX_SHARE = 0.25;
/** Above this share of holiday / school-break / bridge days the window is not a typical one. */
export const HOLIDAY_MAX_SHARE = 0.4;
/**
 * Days after `evaluateOn` the job waits for the window's last date to be
 * scored; after that the report is frozen anyway, with a warning.
 */
export const WINDOW_END_GRACE_DAYS = 3;

const WEEKDAYS = ["Mo", "Di", "Mi", "Do", "Fr", "Sa", "So"];
/** Meteorological seasons, northern hemisphere (where almost every park is). */
const SEASON_OF_MONTH = [
  "Winter",
  "Winter",
  "Frühling",
  "Frühling",
  "Frühling",
  "Sommer",
  "Sommer",
  "Sommer",
  "Herbst",
  "Herbst",
  "Herbst",
  "Winter",
];
const SEASON_ORDER = ["Frühling", "Sommer", "Herbst", "Winter"];

/** One paired park × date cell of the decision, with the park's holiday flag. */
export interface PairedParkDate {
  parkId: string;
  /** YYYY-MM-DD, park-local. */
  date: string;
  /** Public holiday, school break or bridge day at the park; null = unknown (no holiday data). */
  holiday: boolean | null;
}

/**
 * The `forecast_archive_paired` keys (use case | lead | model | reference |
 * segment) behind the decision's non-served cells, for the park × date read.
 */
export function decisionPairedKeys(def: DecisionDef): string[] {
  const keys = new Set<string>();
  for (const ref of def.cells) {
    const q = COMPARISON_QUESTIONS.find((x) => x.id === ref.questionId);
    const names = q?.models[ref.modelId];
    if (!q || !names) continue;
    for (const lead of cellLeads(q, names)) {
      for (const m of names) {
        keys.add([q.useCase, lead, m, q.reference, q.segment].join("|"));
      }
    }
  }
  return [...keys].sort();
}

/** ISO weekday index, Monday = 0, of a YYYY-MM-DD date. */
function weekdayIndex(date: string): number {
  return (new Date(`${date}T12:00:00Z`).getUTCDay() + 6) % 7;
}

/** "Herbst", "Herbst und Winter", … in calendar order of the seasons present. */
export function seasonLabel(dates: string[]): string {
  const seen = new Set(
    dates.map((d) => SEASON_OF_MONTH[Number(d.slice(5, 7)) - 1]),
  );
  const list = SEASON_ORDER.filter((s) => seen.has(s));
  if (list.length === 0) return "keine Daten";
  return list.length === 1
    ? list[0]
    : `${list.slice(0, -1).join(", ")} und ${list[list.length - 1]}`;
}

const pct = (v: number) => `${fmtDe(Math.round(v * 1000) / 10)} %`;

/**
 * The report of one decision on its frozen window. Pure: the caller decides
 * that it is due and that none exists yet, and stores the result once.
 */
export function buildDecisionReport(
  def: DecisionDef & { evaluateOn: string },
  matrix: ModelComparisonResponseDto | null,
  paired: PairedParkDate[],
  createdAt: Date,
): MlDecisionReportDto {
  const cells: MlDecisionReportCellDto[] = def.cells.flatMap((ref) => {
    const q = matrix?.questions.find((x) => x.id === ref.questionId);
    const c = q?.cells[ref.modelId];
    if (!c || c.state === "today") return [];
    return [
      {
        questionId: ref.questionId,
        modelId: ref.modelId,
        state: c.state,
        diff: c.live?.diff ?? null,
        ciLow: c.live?.ciLow ?? null,
        ciHigh: c.live?.ciHigh ?? null,
        parkCiLow: c.live?.parkCiLow ?? null,
        parkCiHigh: c.live?.parkCiHigh ?? null,
        parks: c.live?.parks ?? 0,
        days: c.live?.days ?? 0,
        n: c.live?.n ?? 0,
        jointRows: q?.coverage?.jointRows ?? null,
        cellRows: q?.coverage?.cells[ref.modelId] ?? null,
      },
    ];
  });

  const counts = new Array<number>(7).fill(0);
  for (const p of paired) counts[weekdayIndex(p.date)]++;
  const total = paired.length;
  const weekdays = WEEKDAYS.map((weekday, i) => ({
    weekday,
    count: counts[i],
    share: total ? Math.round((counts[i] / total) * 1000) / 1000 : 0,
  }));
  const known = paired.filter((p) => p.holiday !== null);
  const holidayShare = known.length
    ? Math.round(
        (known.filter((p) => p.holiday).length / known.length) * 1000,
      ) / 1000
    : null;
  const dates = [...new Set(paired.map((p) => p.date))].sort();
  const months = new Set(dates.map((d) => Number(d.slice(5, 7))));

  const top = weekdays.reduce(
    (m, w) => (w.share > m.share ? w : m),
    weekdays[0],
  );
  const intendedEnd = addIsoDays(def.evaluateOn, -1);
  const lastDate = matrix?.window.lastDate ?? null;
  const checks: MlDecisionReportCheckDto[] = [
    {
      id: "weekday_mix",
      ok: total > 0 && top.share <= WEEKDAY_MAX_SHARE,
      label:
        total > 0
          ? `Größter Wochentag-Anteil: ${top.weekday} mit ${pct(top.share)} (erlaubt bis ${pct(WEEKDAY_MAX_SHARE)})`
          : "Keine gepaarten Tage im Fenster",
    },
    {
      id: "holiday_share",
      ok: holidayShare !== null && holidayShare <= HOLIDAY_MAX_SHARE,
      label:
        holidayShare !== null
          ? `Feiertage, Ferien und Brückentage: ${pct(holidayShare)} der Park-Tage (erlaubt bis ${pct(HOLIDAY_MAX_SHARE)})`
          : "Feiertage unbekannt (keine Feiertagsdaten zu den Parks)",
    },
    {
      id: "autumn_only",
      ok: months.size > 0 && [...months].some((m) => m < 9 || m > 11),
      label: `Jahreszeit des Fensters: ${seasonLabel(dates)}`,
    },
    {
      id: "window_end",
      ok: lastDate === intendedEnd,
      label: lastDate
        ? `Fenster endet am ${lastDate} (geplant ${intendedEnd})`
        : "Kein bewerteter Tag im Fenster",
    },
  ];
  const warnings = checks
    .filter((c) => !c.ok)
    .map((c) =>
      c.id === "autumn_only"
        ? "Nur Herbst gemessen: das Urteil sagt nichts über Sommer und Winter."
        : c.id === "weekday_mix"
          ? `Ein Wochentag überwiegt: ${c.label}.`
          : c.id === "holiday_share"
            ? `Kein gewöhnlicher Zeitraum: ${c.label}.`
            : `Fenster unvollständig: ${c.label}.`,
    );

  return {
    decisionId: def.id,
    evaluateOn: def.evaluateOn,
    createdAt: createdAt.toISOString(),
    window: { firstDate: matrix?.window.firstDate ?? null, lastDate },
    verdict: deriveDecisionState(decisionCells(def, matrix)),
    cells,
    parks: new Set(paired.map((p) => p.parkId)).size,
    days: dates.length,
    weekdays,
    holidayShare,
    season: seasonLabel(dates),
    checks,
    warning: warnings.length > 0,
    warnings,
  };
}

/**
 * Whether the report for `def` may be frozen today: the evaluation date has
 * arrived and the window's last date (evaluateOn − 1) is scored — or the
 * grace period ran out, in which case the `window_end` check warns.
 */
export function reportDue(
  def: DecisionDef,
  today: string,
  lastScoredDate: string | null,
): boolean {
  if (def.state || !def.evaluateOn || def.evaluateOn > today) return false;
  if (lastScoredDate && lastScoredDate >= addIsoDays(def.evaluateOn, -1)) {
    return true;
  }
  return today >= addIsoDays(def.evaluateOn, WINDOW_END_GRACE_DAYS);
}

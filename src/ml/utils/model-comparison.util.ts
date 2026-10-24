/**
 * The admin model comparison (PAR-855): one matrix of question × model, each
 * cell saying whether the model beats WHAT IS SERVED today for that question.
 *
 * Live evidence comes from `forecast_archive_paired` — every row there pairs a
 * model with the served reference on identical slots (or ride-days) of the
 * same origin instant, per park and day (`scorePairedShadows`,
 * `scorePairedModels`). A cell pools its question's leads, sums the minutes
 * per park, then divides once, and draws PAR-852's 95 % park-cluster
 * bootstrap CI. Offline evidence is the versioned
 * `src/ml/data/offline-model-verdicts.json`.
 *
 * Pure: the service hands in the aggregated rows, the verdicts and "today";
 * the spec pins the state rule, the note text and the dates.
 */
import { addIsoDays } from "../../common/utils/date.util";
import {
  BOOTSTRAP_SEED,
  CHRONOS_MIN_DAYS,
  CHRONOS_MIN_PARKS,
  gateDays,
  parkClusterBootstrap,
} from "./forecast-archive-paired.util";
import {
  ModelComparisonCellDto,
  ModelComparisonCheck,
  ModelComparisonExpectedByDto,
  ModelComparisonGroupId,
  ModelComparisonLiveDto,
  ModelComparisonModelDto,
  ModelComparisonOfflineDto,
  ModelComparisonQuestionDto,
  ModelComparisonResponseDto,
  ModelComparisonState,
} from "../dto/model-comparison.dto";

export const MIN_DAYS = CHRONOS_MIN_DAYS as 14;
export const MIN_PARKS = CHRONOS_MIN_PARKS as 10;
/** A confirmed live win counts as visible in the app from this many minutes. */
export const VISIBLE_MIN_DIFF = 1;

export const COMPARISON_MODELS: ModelComparisonModelDto[] = [
  {
    id: "persist",
    label: "Aktuelle Wartezeit",
    oneLiner: "Was gerade angezeigt wird, einfach weitergeschrieben.",
  },
  {
    id: "pcn",
    label: "PCN",
    oneLiner:
      "Unser Live-Modell für die nächsten 12 Stunden. Heute in der App.",
  },
  {
    id: "served",
    label: "Tagesplaner heute",
    oneLiner:
      "Typischer Stundenverlauf, hochgerechnet auf das erwartete Tagesniveau. Heute in der App.",
  },
  {
    id: "h5",
    label: "H5-Profil",
    oneLiner:
      "Typischer Verlauf in 15-Minuten-Schritten, ausgerichtet an Öffnung und Schließung.",
  },
  {
    id: "tfth5",
    label: "H5 + TFT-Niveau",
    oneLiner:
      "H5-Verlauf, aber bei vollen Fahrgeschäften auf die TFT-Tagesprognose skaliert.",
  },
  {
    id: "peak",
    label: "Alt-Profil skaliert",
    oneLiner:
      "Der heutige Tagesplaner-Verlauf in 15-Minuten-Schritten statt Stundenwerten.",
  },
  {
    id: "chronos",
    label: "Chronos-2",
    oneLiner:
      "Fertig trainiertes KI-Modell von Amazon, bekommt Öffnungszeiten, Ferien und Feiertage.",
  },
  {
    id: "tft",
    label: "TFT",
    oneLiner: "Unser Tagesmodell für die nächsten 60 Tage. Heute im Kalender.",
  },
  {
    id: "catboost",
    label: "CatBoost",
    oneLiner:
      "Unser ältestes Modell, liefert Stundenwerte und das Tagesniveau ab Tag 61.",
  },
  {
    id: "wkd",
    label: "Wochentag-Durchschnitt",
    oneLiner:
      "Was an diesem Wochentag in den letzten Wochen typisch war. Die Messlatte, die jedes Modell schlagen muss.",
  },
  {
    id: "fm",
    label: "Toto 2.0 / TiRex",
    oneLiner:
      "Zwei weitere fertig trainierte KI-Modelle, bisher nur offline gemessen.",
  },
];

export const COMPARISON_GROUPS: {
  id: ModelComparisonGroupId;
  label: string;
}[] = [
  { id: "live", label: "Live in der App" },
  { id: "today", label: "Heute im Tagesverlauf" },
  { id: "planner", label: "Tagesplaner" },
  { id: "calendar", label: "Kalender" },
];

/**
 * One question: which paired rows answer it. `models` maps a matrix column to
 * the `forecast_archive_paired.model` names pooled into its live cell; `null`
 * means the model applies but is not archived (offline evidence only). The
 * served model has no entry there — it is the reference of every row.
 */
export interface QuestionDef {
  id: string;
  group: ModelComparisonGroupId;
  label: string;
  sub: string;
  leadLabel: string;
  servedModel: string;
  useCase: string;
  leads: string[];
  segment: string;
  models: Record<string, string[] | null>;
}

const PLAN_LEADS_6_60 = ["d6", "d7", "d10", "d14", "d21", "d30", "d45", "d60"];

export const COMPARISON_QUESTIONS: QuestionDef[] = [
  {
    id: "live_h0_1",
    group: "live",
    label: "Wartezeit in der nächsten Stunde",
    sub: "Fahrgeschäft-Karte",
    leadLabel: "Prognose bis 1 Stunde vorher",
    servedModel: "pcn",
    useCase: "UC1",
    leads: ["h0-1"],
    segment: "all",
    models: {
      persist: ["persistence"],
      chronos: ["chronos2"],
      h5: ["shadow_h5"],
      fm: null,
    },
  },
  {
    id: "live_h1_2",
    group: "live",
    label: "Wartezeit in 1 bis 2 Stunden",
    sub: "„Was jetzt fahren?“",
    leadLabel: "Prognose 1–2 Stunden vorher",
    servedModel: "pcn",
    useCase: "UC1",
    leads: ["h1-2"],
    segment: "all",
    models: {
      persist: ["persistence"],
      chronos: ["chronos2"],
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      fm: null,
    },
  },
  {
    id: "today_h2_6",
    group: "today",
    label: "Heute, 2 bis 6 Stunden später",
    sub: "Park-Seite, Tagesverlauf",
    leadLabel: "Prognose 2–6 Stunden vorher",
    servedModel: "pcn",
    useCase: "UC2",
    leads: ["h2-6"],
    segment: "all",
    models: {
      persist: ["persistence"],
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      chronos: ["chronos2", "chronos2_grid"],
      fm: null,
    },
  },
  {
    id: "today_h6_12",
    group: "today",
    label: "Heute, 6 bis 12 Stunden später",
    sub: "Park-Seite, Tagesverlauf",
    leadLabel:
      "Prognose 6–12 Stunden vorher, meist morgens um 06:05 gespeichert",
    servedModel: "pcn",
    useCase: "UC2",
    leads: ["h6-12"],
    segment: "all",
    models: {
      persist: ["persistence"],
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      chronos: ["chronos2_grid"],
      fm: null,
    },
  },
  {
    id: "plan_d0_1",
    group: "planner",
    label: "Geplanter Tag ist heute oder morgen",
    sub: "15-Minuten-Slots",
    leadLabel:
      "Prognose am selben Tag oder 1 Tag vorher, morgens um 06:05 gespeichert",
    servedModel: "served",
    useCase: "UC3",
    leads: ["d0", "d1"],
    segment: "all",
    models: {
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      peak: ["shadow_peak_scaled"],
      chronos: ["chronos2_grid"],
      wkd: ["weekday_avg"],
      fm: null,
    },
  },
  {
    id: "plan_d2_5",
    group: "planner",
    label: "Geplanter Tag ist in 2 bis 5 Tagen",
    sub: "15-Minuten-Slots",
    leadLabel: "Prognose 2–5 Tage vorher, morgens um 06:05 gespeichert",
    servedModel: "served",
    useCase: "UC3",
    leads: ["d2", "d3", "d4", "d5"],
    segment: "all",
    models: {
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      peak: ["shadow_peak_scaled"],
      chronos: ["chronos2_grid"],
      wkd: ["weekday_avg"],
      fm: null,
    },
  },
  {
    id: "plan_d6_60",
    group: "planner",
    label: "Geplanter Tag ist in 6 Tagen bis 2 Monaten",
    sub: "15-Minuten-Slots",
    leadLabel:
      "Prognose 6, 7, 10, 14, 21, 30, 45 und 60 Tage vorher, morgens um 06:05 bzw. 07:05 gespeichert",
    servedModel: "served",
    useCase: "UC3",
    leads: PLAN_LEADS_6_60,
    segment: "all",
    models: {
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      peak: ["shadow_peak_scaled"],
      wkd: ["weekday_avg"],
    },
  },
  {
    id: "plan_besttime",
    group: "planner",
    label: "Beste Besuchszeit (bis 6 Wochen)",
    sub: "„Wann fahre ich am besten?“ – verschenkte Wartezeit",
    leadLabel:
      "Prognose 0 bis 30 Tage vorher (bis 6 Wochen), morgens gespeichert",
    servedModel: "served",
    useCase: "D3",
    leads: [
      "d0",
      "d1",
      "d2",
      "d3",
      "d4",
      "d5",
      "d6",
      "d7",
      "d10",
      "d14",
      "d21",
      "d30",
    ],
    segment: "all",
    models: {
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      peak: ["shadow_peak_scaled"],
      chronos: ["chronos2_grid"],
      wkd: ["weekday_avg"],
    },
  },
  {
    id: "cal_busier",
    group: "calendar",
    label: "Welcher Tag ist voller?",
    sub: "Kalender und „Beste Tage“ – Tagesspitze der Hauptattraktionen",
    leadLabel: "Prognose 0 bis 21 Tage vorher, morgens gespeichert",
    servedModel: "tft",
    useCase: "PEAK",
    leads: [
      "d0",
      "d1",
      "d2",
      "d3",
      "d4",
      "d5",
      "d6",
      "d7",
      "d10",
      "d14",
      "d21",
    ],
    segment: "headliner",
    models: { catboost: null, wkd: ["weekday_avg"] },
  },
  {
    id: "cal_far",
    group: "calendar",
    label: "Kalendertage 1 bis 3 Monate voraus vergleichen",
    sub: "„Samstag oder Sonntag?“ – Tagesspitze der Hauptattraktionen",
    leadLabel:
      "Prognose 30, 45, 60 und 90 Tage vorher, morgens um 07:05 gespeichert",
    servedModel: "tft",
    useCase: "PEAK",
    leads: ["d30", "d45", "d60", "d90"],
    segment: "headliner",
    models: { catboost: null, wkd: ["weekday_avg"] },
  },
];

/** One park's pooled paired sums for one (use case, lead, model, segment). */
export interface PairedAggregate {
  useCase: string;
  lead: string;
  model: string;
  segment: string;
  parkId: string;
  /** Distinct target dates this park contributed (YYYY-MM-DD). */
  dates: string[];
  n: number;
  saeModel: number;
  saeRef: number;
}

export interface OfflineVerdict {
  state: "better" | "worse" | "unclear" | "pending";
  diff?: number;
  ciLow?: number;
  ciHigh?: number;
  source: string;
  period?: string;
  note?: string;
  expectedBy?: ModelComparisonExpectedByDto;
}

export interface OfflineVerdictFile {
  questions: Record<string, Record<string, OfflineVerdict>>;
  servedNotes?: Record<string, string>;
}

/** The pooled live comparison of one cell, before it becomes a DTO. */
export interface LiveComparison {
  n: number;
  days: number;
  parks: number;
  diff: number;
  modelMae: number;
  referenceMae: number;
  ciLow: number | null;
  ciHigh: number | null;
  enoughData: boolean;
}

const round = (v: number, d = 2) => Math.round(v * 10 ** d) / 10 ** d;

/** A stable per-cell seed, so two cells never share one resampling sequence. */
export function cellSeed(key: string): number {
  let h = BOOTSTRAP_SEED;
  for (let i = 0; i < key.length; i++)
    h = Math.imul(h ^ key.charCodeAt(i), 16777619);
  return h >>> 0;
}

/**
 * Pools the rows of one cell (the question's leads × the column's model
 * names): Σ minutes per park, then divide once; CI by resampling parks.
 */
export function poolCell(
  rows: PairedAggregate[],
  seed: number,
): LiveComparison | null {
  const perPark = new Map<string, { n: number; a: number; b: number }>();
  const datesPerPark = new Map<string, Set<string>>();
  let n = 0;
  let a = 0;
  let b = 0;
  for (const r of rows) {
    if (!(r.n > 0)) continue;
    const p = perPark.get(r.parkId) ?? { n: 0, a: 0, b: 0 };
    p.n += r.n;
    p.a += r.saeModel;
    p.b += r.saeRef;
    perPark.set(r.parkId, p);
    const dates = datesPerPark.get(r.parkId) ?? new Set<string>();
    for (const d of r.dates) dates.add(d);
    datesPerPark.set(r.parkId, dates);
    n += r.n;
    a += r.saeModel;
    b += r.saeRef;
  }
  if (n === 0) return null;
  const ci = parkClusterBootstrap([...perPark.values()], undefined, seed);
  const days = gateDays(datesPerPark.values());
  return {
    n,
    days,
    parks: perPark.size,
    diff: round((a - b) / n),
    modelMae: round(a / n),
    referenceMae: round(b / n),
    ciLow: ci ? round(ci.low) : null,
    ciHigh: ci ? round(ci.high) : null,
    enoughData: days >= MIN_DAYS && perPark.size >= MIN_PARKS,
  };
}

/**
 * The state rule. The served model is `today`. A live comparison that meets
 * the gate (≥ 10 parks with ≥ 14 days each) decides: CI entirely above 0 →
 * `worse`, else `unclear` — and CI entirely below 0 → `better` only when
 * offline says better too (the owner's rule is offline AND live; a live win
 * stays `pending` while the offline run is still going and `unclear` when
 * offline does not share it, PAR-1024). Below the gate the offline
 * verdict speaks: offline `better` or a run still in progress → `pending`
 * (waiting for live confirmation), offline `worse` → `worse`, else `unclear`.
 */
export function cellState(
  isServed: boolean,
  live: LiveComparison | null,
  offline: OfflineVerdict | null,
): ModelComparisonState {
  if (isServed) return "today";
  if (live && live.enoughData && live.ciLow !== null && live.ciHigh !== null) {
    if (live.ciHigh < 0) {
      if (offline?.state === "better") return "better";
      // Offline still running: waiting on offline, not contradicted by it.
      return offline?.state === "pending" ? "pending" : "unclear";
    }
    if (live.ciLow > 0) return "worse";
    return "unclear";
  }
  if (offline?.state === "better" || offline?.state === "pending") {
    return "pending";
  }
  if (offline?.state === "worse") return "worse";
  return "unclear";
}

export function cellChecks(
  state: ModelComparisonState,
  archived: boolean,
  live: LiveComparison | null,
  offline: OfflineVerdict | null,
): ModelComparisonCellDto["checks"] {
  if (state === "today") {
    return { offline: "yes", live: "yes", visible: true };
  }
  const off: ModelComparisonCheck = !offline
    ? "open"
    : offline.state === "better"
      ? "yes"
      : offline.state === "pending"
        ? "running"
        : "no";
  let liveCheck: ModelComparisonCheck = "open";
  if (archived) {
    liveCheck =
      live && live.enoughData && live.ciHigh !== null
        ? live.ciHigh < 0
          ? "yes"
          : "no"
        : "running";
  }
  return {
    offline: off,
    live: liveCheck,
    visible:
      liveCheck === "yes" && live !== null && live.diff <= -VISIBLE_MIN_DIFF,
  };
}

/** German number: one decimal ("1,3"), two below 0.1 ("0,06"). */
export function fmtDe(v: number, signed = false): string {
  const abs = Math.abs(v);
  const s = (abs < 0.1 && abs > 0 ? abs.toFixed(2) : abs.toFixed(1)).replace(
    ".",
    ",",
  );
  if (!signed) return s;
  if (abs === 0 || Number(abs.toFixed(2)) === 0) return s;
  return `${v < 0 ? "−" : "+"}${s}`;
}

/** "2026-10-26" → "26.10." */
export function dayMonthDe(iso: string): string {
  return `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
}

const unitWord = (q: QuestionDef) =>
  q.useCase === "D3" ? "Minuten verschenkte Wartezeit" : "Minuten";

/** The cell's sentence, generated from its numbers. */
export function cellNote(
  q: QuestionDef,
  state: ModelComparisonState,
  archived: boolean,
  live: LiveComparison | null,
  offline: OfflineVerdict | null,
  servedNote: string | undefined,
): string {
  if (state === "today") {
    return servedNote ?? "Zeigen wir heute in der App.";
  }
  const scope = (l: LiveComparison) =>
    `${l.days} Tage je Park, ${l.parks} Parks.`;
  if (live && live.enoughData && live.ciLow !== null && live.ciHigh !== null) {
    if (state === "better") {
      return `${fmtDe(live.diff)} ${unitWord(q)} besser als heute, sicher. ${scope(live)}`;
    }
    if (state === "worse") {
      return `${fmtDe(live.diff)} ${unitWord(q)} schlechter als heute, sicher. ${scope(live)}`;
    }
    if (live.ciHigh < 0) {
      const off = !offline
        ? "offline nicht gemessen"
        : offline.state === "pending"
          ? "offline läuft die Messung noch"
          : "offline aber nicht bestätigt";
      return `Live ${fmtDe(live.diff)} ${unitWord(q)} besser als heute, sicher, ${off}. ${scope(live)}`;
    }
    return `Kein sicherer Unterschied zu heute (${fmtDe(live.diff, true)} ${unitWord(q)}, Spanne ${fmtDe(live.ciLow, true)} bis ${fmtDe(live.ciHigh, true)}). ${scope(live)}`;
  }
  const base =
    offline?.note ??
    (archived ? "Offline noch nicht gemessen." : "Noch nicht gemessen.");
  if (!archived) {
    return offline ? base : `${base} Dieses Modell wird live nicht archiviert.`;
  }
  if (!live) return `${base} Live noch keine Daten.`;
  return `${base} Live bisher ${fmtDe(live.diff, true)} ${unitWord(q)} gegenüber heute, ${live.days} von ${MIN_DAYS} Tagen je Park, ${live.parks} von ${MIN_PARKS} Parks.`;
}

/**
 * When the outstanding check reports. Live: the gate needs
 * `MIN_DAYS − days` more scored target dates after the window's last one, and
 * a date is scored at 12:00 UTC the day after — so lastDate + remaining + 1.
 * When only parks are missing, the earliest is the next scoring run. Offline:
 * the run's `expectedBy` from the verdict file. The earlier of the two wins.
 */
export function cellExpectedBy(
  state: ModelComparisonState,
  checks: ModelComparisonCellDto["checks"],
  live: LiveComparison | null,
  offline: OfflineVerdict | null,
  lastDate: string | null,
  today: string,
): ModelComparisonExpectedByDto | null {
  if (state === "today") return null;
  const options: ModelComparisonExpectedByDto[] = [];
  if (checks.offline === "running" && offline?.expectedBy) {
    options.push(offline.expectedBy);
  }
  if (checks.live === "running") {
    const days = live?.days ?? 0;
    const parks = live?.parks ?? 0;
    const remaining = Math.max(0, MIN_DAYS - days);
    const base = lastDate ?? addIsoDays(today, -1);
    if (remaining > 0) {
      const date = addIsoDays(base, remaining + 1);
      options.push({
        date: date < today ? today : date,
        label: `frühestens ${dayMonthDe(date < today ? today : date)} (noch ${remaining} von ${MIN_DAYS} Tagen)`,
      });
    } else {
      const date = addIsoDays(base, 1) < today ? today : addIsoDays(base, 1);
      options.push({
        date,
        label: `sobald ${MIN_PARKS} Parks dabei sind (bisher ${parks}), frühestens ${dayMonthDe(date)}`,
      });
    }
  }
  if (options.length === 0) return null;
  return options.sort((x, y) => x.date.localeCompare(y.date))[0];
}

function offlineDto(
  v: OfflineVerdict | null,
): ModelComparisonOfflineDto | null {
  if (!v) return null;
  const out: ModelComparisonOfflineDto = { source: v.source };
  if (v.diff !== undefined) out.diff = v.diff;
  if (v.ciLow !== undefined) out.ciLow = v.ciLow;
  if (v.ciHigh !== undefined) out.ciHigh = v.ciHigh;
  if (v.period !== undefined) out.period = v.period;
  return out;
}

export interface BuildComparisonInput {
  aggregates: PairedAggregate[];
  /** First scored target date per `${useCase}|${lead}|${model}|${segment}`, all time. */
  since: Map<string, string>;
  verdicts: OfflineVerdictFile;
  window: { days: number; firstDate: string | null; lastDate: string | null };
  /** Europe/Berlin calendar date the matrix is built on. */
  today: string;
  generatedAt: string;
}

export function buildModelComparison(
  input: BuildComparisonInput,
): ModelComparisonResponseDto {
  const index = new Map<string, PairedAggregate[]>();
  for (const a of input.aggregates) {
    const key = [a.useCase, a.lead, a.model, a.segment].join("|");
    const list = index.get(key) ?? [];
    list.push(a);
    index.set(key, list);
  }
  const summary = { better: 0, pending: 0, unclear: 0, worse: 0 };
  const questions: ModelComparisonQuestionDto[] = COMPARISON_QUESTIONS.map(
    (q) => {
      const cells: Record<string, ModelComparisonCellDto> = {};
      const columns = [q.servedModel, ...Object.keys(q.models)];
      for (const modelId of COMPARISON_MODELS.map((m) => m.id)) {
        if (!columns.includes(modelId)) continue;
        const isServed = modelId === q.servedModel;
        const stored = isServed ? null : q.models[modelId];
        const archived = !isServed && Array.isArray(stored);
        const rows: PairedAggregate[] = [];
        let since: string | null = null;
        for (const name of stored ?? []) {
          for (const lead of q.leads) {
            const key = [q.useCase, lead, name, q.segment].join("|");
            rows.push(...(index.get(key) ?? []));
            const s = input.since.get(key);
            if (s && (since === null || s < since)) since = s;
          }
        }
        const live = archived
          ? poolCell(rows, cellSeed(`${q.id}|${modelId}`))
          : null;
        const offline = isServed
          ? null
          : (input.verdicts.questions[q.id]?.[modelId] ?? null);
        const state = cellState(isServed, live, offline);
        const checks = cellChecks(state, archived, live, offline);
        const liveDto: ModelComparisonLiveDto | null =
          live && live.ciLow !== null && live.ciHigh !== null
            ? {
                diff: live.diff,
                ciLow: live.ciLow,
                ciHigh: live.ciHigh,
                days: live.days,
                parks: live.parks,
                n: live.n,
                modelMae: live.modelMae,
                referenceMae: live.referenceMae,
                since,
              }
            : null;
        cells[modelId] = {
          state,
          note: cellNote(
            q,
            state,
            archived,
            live,
            offline,
            input.verdicts.servedNotes?.[q.id],
          ),
          checks,
          live: liveDto,
          offline: offlineDto(offline),
          expectedBy: cellExpectedBy(
            state,
            checks,
            live,
            offline,
            input.window.lastDate,
            input.today,
          ),
        };
        if (state !== "today") summary[state]++;
      }
      return {
        id: q.id,
        group: q.group,
        label: q.label,
        sub: q.sub,
        leadLabel: q.leadLabel,
        servedModel: q.servedModel,
        metric: { unit: "min", lowerIsBetter: true },
        cells,
      };
    },
  );
  return {
    generatedAt: input.generatedAt,
    window: input.window,
    rule: { minDays: MIN_DAYS, minParks: MIN_PARKS },
    models: COMPARISON_MODELS,
    groups: COMPARISON_GROUPS,
    questions,
    summary,
  };
}

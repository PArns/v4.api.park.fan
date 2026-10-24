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
  blockBootstrap,
  gateDays,
} from "./forecast-archive-paired.util";
import {
  MIN_PAIR_GAP_MINUTES,
  PAIRED_D7_USE_CASE,
  PAIRED_JOINT_REFERENCE,
  PAIRED_LEVEL_REFERENCES,
  PAIRED_MODELS,
  PAIRED_OPTIONAL_MODELS,
  PAIRED_QUESTION_MODELS,
} from "./forecast-archive-scoring.util";
import {
  ModelComparisonCellDto,
  ModelComparisonCheck,
  ModelComparisonExpectedByDto,
  ModelComparisonGroupId,
  ModelComparisonLeadDto,
  ModelComparisonLiveDto,
  ModelComparisonModelDto,
  ModelComparisonOfflineDto,
  ModelComparisonPairDto,
  ModelComparisonQuestionDto,
  ModelComparisonResponseDto,
  ModelComparisonState,
} from "../dto/model-comparison.dto";

export const MIN_DAYS = CHRONOS_MIN_DAYS as 14;
export const MIN_PARKS = CHRONOS_MIN_PARKS as 10;
/** A confirmed live win counts as visible in the app from this many minutes. */
export const VISIBLE_MIN_DIFF = 1;
/**
 * …and, for a `rate` question (D7, percentage points of wrongly ordered day
 * pairs), from this many points. One minute is ~12 % of a typical slot MAE of
 * 7–9 min; 5 points is about the same share of a ~30–40 % base rate of
 * wrongly ordered pairs, i.e. one comparison of two days in twenty flips —
 * the smallest change a visitor comparing calendar days could notice.
 */
export const VISIBLE_MIN_DIFF_RATE = 5;

/**
 * First park-local date the forward archive captured (PAR-831, #443). A lead
 * of N days has its first scored target date on ARCHIVE_START + N, so a cell
 * whose longest lead is N cannot reach 14 scored dates before
 * ARCHIVE_START + N + 14, however the rows of its shorter leads look.
 */
export const ARCHIVE_START = "2026-10-09";

const leadDays = (lead: string) =>
  lead.startsWith("d") ? Number(lead.slice(1)) : 0;

/**
 * The earliest date a cell's live gate can be met at all: ARCHIVE_START + its
 * longest lead + 14 (the review of PAR-1036: `days` is 0 while a lead has no
 * row, which kept "today + 14" sliding forward every day).
 */
export function cellFloorDate(questionId: string, modelId: string): string {
  const q = COMPARISON_QUESTIONS.find((x) => x.id === questionId);
  if (!q) return ARCHIVE_START;
  const names = q.models[modelId] ?? [];
  const leads = names.length > 0 ? cellLeads(q, names) : q.leads;
  const maxLead = Math.max(0, ...leads.map(leadDays));
  return addIsoDays(
    MODEL_ARCHIVE_START[modelId] ?? ARCHIVE_START,
    maxLead + MIN_DAYS,
  );
}

/**
 * Columns whose archived rows start after ARCHIVE_START, because the model
 * itself does: the grid TFT shadow (PAR-1041) writes its first forecast the
 * night after its deploy. shortcut: the assumed first shadow night; set it to
 * the real first `forecast_date` of `tft_forecasts_shadow` once it exists.
 */
export const MODEL_ARCHIVE_START: Record<string, string> = {
  tft_grid: "2026-10-26",
};

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
    id: "tft_grid",
    label: "TFT mit Lücken-Raster",
    oneLiner:
      "Dasselbe Tagesmodell, aber es lernt auf jedem Kalendertag statt nur auf Öffnungstagen und kennt Öffnungsplan und Schulferien. Läuft jede Nacht nebenher, nicht in der App.",
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
  /**
   * The `forecast_archive_paired.reference` the cells read (PAR-1035/1037):
   * `served_joint` (every candidate on the same rows), or `served_tft` /
   * `served_catboost` (the ride-days whose served level came from that model).
   */
  reference: string;
  /** `min` (minutes) or `rate` (percentage points; D7). Default `min`. */
  unit?: "min" | "rate";
  models: Record<string, string[] | null>;
  /** Direct comparisons beside the cells (PAR-1035/1037); see {@link PairwiseDef}. */
  pairwise?: PairwiseDef[];
}

/**
 * One direct comparison of two columns that is not "against served": two
 * candidates on the matrix's joint rows (Chronos-2 vs H5, H5 + TFT vs H5), or
 * a model against the weekday average on the ride-days that model levelled
 * (CatBoost, TFT). `swap`: the stored row has the two the other way round.
 */
export interface PairwiseDef {
  model: string;
  reference: string;
  rows: Array<{ model: string; reference: string }>;
  swap?: boolean;
  note: string;
}

const PLAN_LEADS_6_60 = ["d6", "d7", "d10", "d14", "d21", "d30", "d45", "d60"];
const CAL_NEAR_LEADS = [
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
];

/**
 * TFT and CatBoost never level the same ride-day, so neither can be paired
 * with the other; both are paired with the weekday average on the ride-days
 * they levelled (PAR-1037). Two different sets of days — read side by side,
 * never as a difference of the two.
 */
const CAL_PAIRWISE: PairwiseDef[] = [
  {
    model: "catboost",
    reference: "wkd",
    rows: [
      {
        model: PAIRED_MODELS.weekdayAvg,
        reference: PAIRED_LEVEL_REFERENCES.catboost,
      },
    ],
    swap: true,
    note: "Nur an den Hauptattraktions-Tagen, an denen TFT fehlte und CatBoost einsprang – eine schiefe Auswahl (Lücken in TFT, nicht typische Tage), andere Tage als bei TFT. Für CatBoost als Tagesniveau entscheidet „Kalendertage 3 Monate voraus“.",
  },
  {
    model: "tft",
    reference: "wkd",
    rows: [
      {
        model: PAIRED_MODELS.weekdayAvg,
        reference: PAIRED_LEVEL_REFERENCES.tft,
      },
    ],
    swap: true,
    note: "An den Tagen mit TFT-Tagesniveau.",
  },
];

export const COMPARISON_QUESTIONS: QuestionDef[] = [
  {
    id: "live_h0_1",
    group: "live",
    label: "Wartezeit in der nächsten Stunde",
    sub: "Fahrgeschäft-Karte",
    leadLabel:
      "Prognose bis 1 Stunde vorher, aus den Ständen um 10, 12, 14, 16 und 18 Uhr",
    servedModel: "pcn",
    useCase: "UC1",
    leads: ["h0-1"],
    segment: "all",
    reference: PAIRED_JOINT_REFERENCE,
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
    leadLabel:
      "Prognose 1–2 Stunden vorher, aus den Ständen um 10, 12, 14, 16 und 18 Uhr",
    servedModel: "pcn",
    useCase: "UC1",
    leads: ["h1-2"],
    segment: "all",
    reference: PAIRED_JOINT_REFERENCE,
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
    // The 10–18 h origins archive 4 hours, and the 06:00 origin has no
    // Chronos live curve: on rows every candidate shares, this is 2–4 h.
    leadLabel:
      "Prognose 2–4 Stunden vorher, aus den Ständen um 10, 12, 14, 16 und 18 Uhr",
    servedModel: "pcn",
    useCase: "UC2",
    leads: ["h2-6"],
    segment: "all",
    reference: PAIRED_JOINT_REFERENCE,
    models: {
      persist: ["persistence"],
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      chronos: ["chronos2"],
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
    reference: PAIRED_JOINT_REFERENCE,
    // Only the 06:00 origin reaches 6–12 h, and at 06:00 almost no park has a
    // live wait: persistence is not a column of this question.
    models: {
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
    reference: PAIRED_JOINT_REFERENCE,
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
    reference: PAIRED_JOINT_REFERENCE,
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
    reference: PAIRED_JOINT_REFERENCE,
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
    reference: PAIRED_JOINT_REFERENCE,
    models: {
      h5: ["shadow_h5"],
      tfth5: ["shadow_h5_routed"],
      peak: ["shadow_peak_scaled"],
      chronos: ["chronos2_grid"],
      wkd: ["weekday_avg"],
    },
  },
  {
    id: "cal_rank",
    group: "calendar",
    label: "Welcher Tag ist voller?",
    sub: "Kalender und „Beste Tage“ – Reihenfolge der Tage eines Parks",
    leadLabel:
      "Prognose 0 bis 21 Tage vorher, morgens gespeichert; Anteil falsch geordneter Tagespaare",
    servedModel: "tft",
    useCase: PAIRED_D7_USE_CASE,
    leads: CAL_NEAR_LEADS,
    segment: "headliner",
    reference: PAIRED_LEVEL_REFERENCES.tft,
    unit: "rate",
    models: {
      tft_grid: [PAIRED_MODELS.tftGrid],
      catboost: [PAIRED_MODELS.catboostShadow],
      wkd: [PAIRED_MODELS.weekdayAvg],
      wkd: [PAIRED_MODELS.weekdayAvg],
    },
  },
  {
    id: "cal_busier",
    group: "calendar",
    label: "Wie hoch wird die Tagesspitze?",
    sub: "Kalender und „Beste Tage“ – Tagesspitze der Hauptattraktionen",
    leadLabel: "Prognose 0 bis 21 Tage vorher, morgens gespeichert",
    servedModel: "tft",
    useCase: "PEAK",
    leads: CAL_NEAR_LEADS,
    segment: "headliner",
    reference: PAIRED_LEVEL_REFERENCES.tft,
    models: {
      tft_grid: [PAIRED_MODELS.tftGrid],
      catboost: [PAIRED_MODELS.catboostShadow],
      wkd: [PAIRED_MODELS.weekdayAvg],
    },
    pairwise: CAL_PAIRWISE,
  },
  {
    id: "cal_far",
    group: "calendar",
    label: "Kalendertage 1 bis 2 Monate voraus vergleichen",
    sub: "„Samstag oder Sonntag?“ – Tagesspitze der Hauptattraktionen",
    // TFT levels the calendar up to 60 days; d90 is CatBoost's (cal_far_90).
    leadLabel:
      "Prognose 30, 45 und 60 Tage vorher, morgens um 07:05 gespeichert",
    servedModel: "tft",
    useCase: "PEAK",
    leads: ["d30", "d45", "d60"],
    segment: "headliner",
    reference: PAIRED_LEVEL_REFERENCES.tft,
    models: {
      tft_grid: [PAIRED_MODELS.tftGrid],
      catboost: [PAIRED_MODELS.catboostShadow],
      wkd: [PAIRED_MODELS.weekdayAvg],
    },
    pairwise: CAL_PAIRWISE,
  },
  {
    id: "cal_far_90",
    group: "calendar",
    label: "Kalendertage 3 Monate voraus vergleichen",
    sub: "„Samstag oder Sonntag?“ – Tagesspitze der Hauptattraktionen",
    leadLabel: "Prognose 90 Tage vorher, morgens um 07:05 gespeichert",
    servedModel: "catboost",
    useCase: "PEAK",
    leads: ["d90"],
    segment: "headliner",
    reference: PAIRED_LEVEL_REFERENCES.catboost,
    models: { wkd: ["weekday_avg"] },
  },
];

// The joint questions' direct comparisons, on the same rows as their cells.
for (const q of COMPARISON_QUESTIONS) {
  if (q.reference !== PAIRED_JOINT_REFERENCE) continue;
  const pairs: PairwiseDef[] = [];
  const h5 = PAIRED_MODELS.shadowH5;
  const chronos = q.models.chronos;
  if (q.models.h5 && chronos) {
    pairs.push({
      model: "chronos",
      reference: "h5",
      rows: chronos.map((m) => ({ model: m, reference: h5 })),
      note: "Direkt verglichen, auf denselben Zeitpunkten wie die Zellen.",
    });
  }
  if (q.models.h5 && q.models.tfth5) {
    pairs.push({
      model: "tfth5",
      reference: "h5",
      rows: q.models.tfth5.map((m) => ({ model: m, reference: h5 })),
      note: "Direkt verglichen, auf denselben Zeitpunkten wie die Zellen.",
    });
  }
  if (pairs.length > 0) q.pairwise = pairs;
}

/** Every `forecast_archive_paired.reference` the matrix reads. */
export const MATRIX_REFERENCES: string[] = [
  ...new Set(
    COMPARISON_QUESTIONS.flatMap((q) => [
      q.reference,
      ...(q.pairwise ?? []).flatMap((p) => p.rows.map((r) => r.reference)),
    ]),
  ),
];

/**
 * The leads a cell must cover (PAR-1036): the question's leads at which one
 * of the column's paired models is a candidate — `chronos2_grid` stops at d7,
 * so the best-time Chronos cell answers for d0–d7 and says so.
 */
export function cellLeads(q: QuestionDef, names: string[]): string[] {
  return q.leads.filter((lead) => {
    const candidates = PAIRED_QUESTION_MODELS[`${q.useCase}|${lead}`];
    return !candidates || names.some((n) => candidates.includes(n));
  });
}

/** One park's pooled paired sums for one (use case, lead, model, reference, segment). */
export interface PairedAggregate {
  useCase: string;
  lead: string;
  model: string;
  /** `forecast_archive_paired.reference`; absent = the question's own. */
  reference?: string;
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
  /**
   * What the offline figure was measured against (PAR-1038): `served` = the
   * question's served model; otherwise the bench reference the note names
   * (`persistence`, `wt_med`, `clim`, `chronos2`, `hourly_profile`), or null
   * when the note does not say. Only `served` lets an offline win count.
   */
  reference?: string | null;
  /** `slot_mae`, `besttime_regret`, `daypeak_mae`, `hit_rate`; null = not stated. */
  metric?: string | null;
  /** The ml-bench results directory of the run; null when not recorded. */
  runId?: string | null;
  diff?: number | null;
  ciLow?: number | null;
  ciHigh?: number | null;
  source: string;
  period?: string;
  note?: string;
  expectedBy?: ModelComparisonExpectedByDto;
}

export interface OfflineVerdictFile {
  questions: Record<string, Record<string, OfflineVerdict>>;
  servedNotes?: Record<string, string>;
}

/** One lead of a cell (PAR-1036). */
export interface LeadComparison {
  lead: string;
  n: number;
  days: number;
  parks: number;
  diff: number;
  modelMae: number;
  referenceMae: number;
  enoughData: boolean;
}

/** The pooled live comparison of one cell, before it becomes a DTO. */
export interface LiveComparison {
  n: number;
  /** The weakest required lead's {@link gateDays}; 0 when a lead has no rows. */
  days: number;
  /** The weakest required lead's park count. */
  parks: number;
  /** Mean over the leads with data of each lead's pooled figure. */
  diff: number;
  modelMae: number;
  referenceMae: number;
  /** 95 % CI, two-way park × date bootstrap — the gate (owner, PAR-1036). */
  ciLow: number | null;
  ciHigh: number | null;
  /** 95 % CI, park-cluster bootstrap — shown beside it. */
  parkCiLow?: number | null;
  parkCiHigh?: number | null;
  /** Every required lead meets the gate on its own. */
  enoughData: boolean;
  leads?: LeadComparison[];
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
 * names). Per lead: Σ minutes per park, then divide once. Across leads
 * (PAR-1036): the plain mean of the per-lead figures, so a lead with more rows
 * does not outvote the others, and the gate holds only when EVERY lead in
 * `leads` (default: the leads with rows) meets it on its own — a d6–d60 cell
 * whose d45/d60 rows do not exist yet is not decided by d6 and d7. CI: the
 * park-cluster bootstrap of that same lead-weighted figure.
 */
export function poolCell(
  rows: PairedAggregate[],
  seed: number,
  leads?: string[],
): LiveComparison | null {
  const byLead = new Map<
    string,
    {
      n: number;
      a: number;
      b: number;
      parks: Set<string>;
      dates: Map<string, Set<string>>;
    }
  >();
  for (const r of rows) {
    if (!(r.n > 0)) continue;
    const l = byLead.get(r.lead) ?? {
      n: 0,
      a: 0,
      b: 0,
      parks: new Set<string>(),
      dates: new Map<string, Set<string>>(),
    };
    l.n += r.n;
    l.a += r.saeModel;
    l.b += r.saeRef;
    l.parks.add(r.parkId);
    const d = l.dates.get(r.parkId) ?? new Set<string>();
    for (const x of r.dates) d.add(x);
    l.dates.set(r.parkId, d);
    byLead.set(r.lead, l);
  }
  if (byLead.size === 0) return null;
  const order = leads ?? [...byLead.keys()];
  const perLead: LeadComparison[] = [];
  for (const lead of order) {
    const l = byLead.get(lead);
    if (!l) continue;
    const days = gateDays(l.dates.values());
    perLead.push({
      lead,
      n: l.n,
      days,
      parks: l.parks.size,
      diff: round((l.a - l.b) / l.n),
      modelMae: round(l.a / l.n),
      referenceMae: round(l.b / l.n),
      enoughData: days >= MIN_DAYS && l.parks.size >= MIN_PARKS,
    });
  }
  if (perLead.length === 0) return null;
  const mean = (f: (l: { n: number; a: number; b: number }) => number) => {
    let sum = 0;
    for (const x of perLead) sum += f(byLead.get(x.lead)!);
    return sum / perLead.length;
  };
  const covered = new Set(perLead.map((x) => x.lead));
  const allParks = new Set<string>();
  for (const x of perLead)
    for (const p of byLead.get(x.lead)!.parks) allParks.add(p);
  const missing = order.some((lead) => !covered.has(lead));
  // One row per park × date (the service reads them so); a row with several
  // dates counts as its first one (D7: the later date of its pairs).
  const cells = rows
    .filter((r) => r.n > 0 && covered.has(r.lead))
    .map((r) => ({
      park: r.parkId,
      date: r.dates[0] ?? "",
      stratum: r.lead,
      n: r.n,
      a: r.saeModel,
      b: r.saeRef,
    }));
  const ci = blockBootstrap(cells, { twoWay: true }, undefined, seed);
  const parkCi = blockBootstrap(cells, { twoWay: false }, undefined, seed);
  return {
    n: perLead.reduce((s2, x) => s2 + x.n, 0),
    days: missing ? 0 : Math.min(...perLead.map((x) => x.days)),
    parks: missing ? 0 : Math.min(...perLead.map((x) => x.parks)),
    diff: round(mean((l) => (l.a - l.b) / l.n)),
    modelMae: round(mean((l) => l.a / l.n)),
    referenceMae: round(mean((l) => l.b / l.n)),
    ciLow: ci && allParks.size >= 2 ? round(ci.low) : null,
    ciHigh: ci && allParks.size >= 2 ? round(ci.high) : null,
    parkCiLow: parkCi ? round(parkCi.low) : null,
    parkCiHigh: parkCi ? round(parkCi.high) : null,
    enoughData: !missing && perLead.every((x) => x.enoughData),
    leads: perLead,
  };
}

/** One park-date-lead of D7 level sums, as stored (PAR-1037). */
export interface D7Row {
  parkId: string;
  lead: string;
  date: string;
  /** Headliners on that park-date. */
  n: number;
  /** Σ weekday-average peak, Σ served dayPeak, Σ truth P90 over them. */
  sumModel: number;
  sumRef: number;
  sumTruth: number;
}

/**
 * D7 — "which day is fuller?" within one park (PAR-1037): for every two dates
 * of the window at the same lead whose realised levels (mean headliner truth
 * P90) differ by at least {@link MIN_PAIR_GAP_MINUTES}, did the forecast order
 * them the same way? A predicted tie counts half. Returned as paired
 * aggregates in percent of wrongly ordered pairs (lower is better):
 * `saeModel` = the weekday average's, `saeRef` = served's, `n` = pairs, one
 * aggregate per park × lead × later date (`dates[0]` is that date, the rest its
 * partners — so the gate counts every date that took part).
 */
export function d7Aggregates(
  rows: D7Row[],
  model: string = PAIRED_MODELS.weekdayAvg,
  reference: string = PAIRED_LEVEL_REFERENCES.tft,
): PairedAggregate[] {
  const groups = new Map<string, D7Row[]>();
  for (const r of rows) {
    if (!(r.n > 0)) continue;
    const k = `${r.parkId}|${r.lead}`;
    const list = groups.get(k) ?? [];
    list.push(r);
    groups.set(k, list);
  }
  const wrong = (dp: number, dt: number) =>
    dp === 0 ? 0.5 : Math.sign(dp) === Math.sign(dt) ? 0 : 1;
  const out: PairedAggregate[] = [];
  for (const list of groups.values()) {
    list.sort((x, y) => x.date.localeCompare(y.date));
    for (let j = 1; j < list.length; j++) {
      const later = list[j];
      let pairs = 0;
      let a = 0;
      let b = 0;
      const partners: string[] = [];
      for (let i = 0; i < j; i++) {
        const earlier = list[i];
        const dt = later.sumTruth / later.n - earlier.sumTruth / earlier.n;
        if (Math.abs(dt) < MIN_PAIR_GAP_MINUTES) continue;
        pairs++;
        a += wrong(later.sumModel / later.n - earlier.sumModel / earlier.n, dt);
        b += wrong(later.sumRef / later.n - earlier.sumRef / earlier.n, dt);
        partners.push(earlier.date);
      }
      if (pairs === 0) continue;
      out.push({
        useCase: PAIRED_D7_USE_CASE,
        lead: later.lead,
        model,
        reference,
        segment: "headliner",
        parkId: later.parkId,
        dates: [later.date, ...partners],
        n: pairs,
        saeModel: 100 * a,
        saeRef: 100 * b,
      });
    }
  }
  return out;
}

/**
 * {@link d7Aggregates} per stored model: the day pairs are formed within one
 * model's rows only, because each model has its own set of ride-days (the
 * grid TFT shadow, PAR-1041, only where it has a value).
 */
export function d7AggregatesByModel(
  rows: Array<D7Row & { model: string }>,
): PairedAggregate[] {
  const models = [...new Set(rows.map((r) => r.model))];
  return models.flatMap((m) =>
    d7Aggregates(
      rows.filter((r) => r.model === m),
      m,
    ),
  );
}

/**
 * An offline `better` that counts towards the owner rule: measured against the
 * question's served model (PAR-1038). A win over persistence or the weekday
 * average says nothing about a win over what is served.
 */
export function offlineBetterThanServed(
  offline: OfflineVerdict | null,
): boolean {
  return offline?.state === "better" && offline.reference === "served";
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
      if (offlineBetterThanServed(offline)) return "better";
      // Offline still running, or offline better against another reference:
      // waiting on an offline result against served, not contradicted by one.
      return offline?.state === "pending" || offline?.state === "better"
        ? "pending"
        : "unclear";
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
  unit: "min" | "rate" = "min",
): ModelComparisonCellDto["checks"] {
  if (state === "today") {
    return { offline: "yes", live: "yes", visible: true };
  }
  const off: ModelComparisonCheck = !offline
    ? "open"
    : offline.state === "better"
      ? // A win over another reference leaves the check against served open.
        offlineBetterThanServed(offline)
        ? "yes"
        : "open"
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
      liveCheck === "yes" &&
      live !== null &&
      live.diff <=
        -(unit === "rate" ? VISIBLE_MIN_DIFF_RATE : VISIBLE_MIN_DIFF),
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
  q.useCase === "D3"
    ? "Minuten verschenkte Wartezeit"
    : q.unit === "rate"
      ? "Prozentpunkte falsch geordnete Tagespaare"
      : "Minuten";

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
          : offline.state === "better"
            ? "offline aber nur gegen einen anderen Vergleichswert gemessen, nicht gegen das, was heute läuft"
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
  /** {@link cellFloorDate}: no date before it, whatever `days` says. */
  floor?: string,
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
    const atLeast = (d: string) => {
      const x = d < today ? today : d;
      return floor && floor > x ? floor : x;
    };
    if (remaining > 0) {
      const date = atLeast(addIsoDays(base, remaining + 1));
      options.push({
        date,
        label: `frühestens ${dayMonthDe(date)} (noch ${remaining} von ${MIN_DAYS} Tagen)`,
      });
    } else {
      const date = atLeast(addIsoDays(base, 1));
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
  if (v.diff != null) out.diff = v.diff;
  if (v.ciLow != null) out.ciLow = v.ciLow;
  if (v.ciHigh != null) out.ciHigh = v.ciHigh;
  if (v.period !== undefined) out.period = v.period;
  out.reference = v.reference ?? null;
  out.referenceIsServed = v.reference === "served";
  out.metric = v.metric ?? null;
  out.runId = v.runId ?? null;
  return out;
}

function leadDtos(live: LiveComparison): ModelComparisonLeadDto[] {
  return (live.leads ?? []).map((l) => ({
    lead: l.lead,
    n: l.n,
    days: l.days,
    parks: l.parks,
    diff: l.diff,
    modelMae: l.modelMae,
    referenceMae: l.referenceMae,
  }));
}

/** A {@link PairwiseDef} pooled like a cell; `live` null without two parks. */
function pairDto(
  q: QuestionDef,
  p: PairwiseDef,
  index: Map<string, PairedAggregate[]>,
  sinceIndex: Map<string, string>,
): ModelComparisonPairDto {
  const rows: PairedAggregate[] = [];
  let since: string | null = null;
  const leads = cellLeads(
    q,
    p.rows.map((r) => r.model),
  );
  for (const r of p.rows) {
    for (const lead of leads) {
      const key = [q.useCase, lead, r.model, r.reference, q.segment].join("|");
      for (const a of index.get(key) ?? []) {
        rows.push(
          p.swap ? { ...a, saeModel: a.saeRef, saeRef: a.saeModel } : a,
        );
      }
      const s = sinceIndex.get(key);
      if (s && (since === null || s < since)) since = s;
    }
  }
  const live = poolCell(
    rows,
    cellSeed(`${q.id}|${p.model}|${p.reference}`),
    leads,
  );
  return {
    model: p.model,
    reference: p.reference,
    note: p.note,
    live:
      live && live.ciLow !== null && live.ciHigh !== null
        ? {
            diff: live.diff,
            ciLow: live.ciLow,
            ciHigh: live.ciHigh,
            ci: "park_date",
            parkCiLow: live.parkCiLow ?? null,
            parkCiHigh: live.parkCiHigh ?? null,
            days: live.days,
            parks: live.parks,
            n: live.n,
            modelMae: live.modelMae,
            referenceMae: live.referenceMae,
            since,
            leadWeighting: "equal",
            leads: leadDtos(live),
          }
        : null,
    enoughData: live?.enoughData ?? false,
  };
}

export interface BuildComparisonInput {
  aggregates: PairedAggregate[];
  /** First scored target date per `${useCase}|${lead}|${model}|${reference}|${segment}`, all time. */
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
    const key = [
      a.useCase,
      a.lead,
      a.model,
      a.reference ?? PAIRED_JOINT_REFERENCE,
      a.segment,
    ].join("|");
    const list = index.get(key) ?? [];
    list.push(a);
    index.set(key, list);
  }
  const summary = { better: 0, pending: 0, unclear: 0, worse: 0 };
  const questions: ModelComparisonQuestionDto[] = COMPARISON_QUESTIONS.map(
    (q) => {
      const cells: Record<string, ModelComparisonCellDto> = {};
      const coverage: Record<string, number> = {};
      let jointRows = 0;
      const columns = [q.servedModel, ...Object.keys(q.models)];
      for (const modelId of COMPARISON_MODELS.map((m) => m.id)) {
        if (!columns.includes(modelId)) continue;
        const isServed = modelId === q.servedModel;
        const stored = isServed ? null : q.models[modelId];
        const archived = !isServed && Array.isArray(stored);
        const rows: PairedAggregate[] = [];
        let since: string | null = null;
        const leads = cellLeads(q, stored ?? []);
        for (const name of stored ?? []) {
          for (const lead of leads) {
            const key = [q.useCase, lead, name, q.reference, q.segment].join(
              "|",
            );
            rows.push(...(index.get(key) ?? []));
            const s = input.since.get(key);
            if (s && (since === null || s < since)) since = s;
          }
        }
        const live = archived
          ? poolCell(rows, cellSeed(`${q.id}|${modelId}`), leads)
          : null;
        const offline = isServed
          ? null
          : (input.verdicts.questions[q.id]?.[modelId] ?? null);
        const state = cellState(isServed, live, offline);
        const checks = cellChecks(
          state,
          archived,
          live,
          offline,
          q.unit ?? "min",
        );
        if (live && archived) {
          coverage[modelId] = live.n;
          if (!(stored ?? []).every((m) => PAIRED_OPTIONAL_MODELS.has(m))) {
            jointRows = Math.max(jointRows, live.n);
          }
        }
        const liveDto: ModelComparisonLiveDto | null =
          live && live.ciLow !== null && live.ciHigh !== null
            ? {
                diff: live.diff,
                ciLow: live.ciLow,
                ciHigh: live.ciHigh,
                ci: "park_date",
                parkCiLow: live.parkCiLow ?? null,
                parkCiHigh: live.parkCiHigh ?? null,
                days: live.days,
                parks: live.parks,
                n: live.n,
                modelMae: live.modelMae,
                referenceMae: live.referenceMae,
                since,
                leadWeighting: "equal",
                leads: leadDtos(live),
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
            cellFloorDate(q.id, modelId),
          ),
        };
        if (state !== "today") summary[state]++;
      }
      const comparisons = (q.pairwise ?? []).map((p) =>
        pairDto(q, p, index, input.since),
      );
      return {
        id: q.id,
        group: q.group,
        label: q.label,
        sub: q.sub,
        leadLabel: q.leadLabel,
        servedModel: q.servedModel,
        metric: { unit: q.unit ?? "min", lowerIsBetter: true },
        cells,
        ...(comparisons.length > 0 ? { comparisons } : {}),
        ...(Object.keys(coverage).length > 0
          ? { coverage: { jointRows, cells: coverage } }
          : {}),
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

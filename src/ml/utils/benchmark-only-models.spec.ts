import { readdirSync, readFileSync } from "fs";
import { join } from "path";
import { COMPARISON_MODELS, COMPARISON_QUESTIONS } from "./model-comparison.util";
import { PAIRED_MODELS, PAIRED_QUESTION_MODELS } from "./forecast-archive-scoring.util";

/**
 * PAR-1067: a model whose licence excludes production use (TimesFM 3.x,
 * TabPFN-TS on TabPFN-3.5) may be benchmarked in ml-bench, never served. The
 * list lives in ml-bench/mlbench/benchmark_only_models.json, which the bench's
 * verdict writers read too; no verdict, decision or matrix entry may name one.
 */
const REGISTRY = join(__dirname, "../../../ml-bench/mlbench/benchmark_only_models.json");
const DATA_DIR = join(__dirname, "../data");

const prefixes = Object.keys(
  (JSON.parse(readFileSync(REGISTRY, "utf8")) as { prefixes: Record<string, string> }).prefixes,
);
// a model id, not prose: "TimesFM 3.0" in a note is a sentence, "timesfm3_owx" is a model
const modelIdPattern = new RegExp(`\\b(?:${prefixes.join("|")})[a-z0-9_]*`, "g");
const isBenchmarkOnly = (name: string) => prefixes.some((p) => name.toLowerCase().startsWith(p));

describe("benchmark-only models are never served (PAR-1067)", () => {
  it("the registry lists TimesFM 3 and TabPFN", () => {
    expect(isBenchmarkOnly("timesfm3_owx")).toBe(true);
    expect(isBenchmarkOnly("tabpfn_ts")).toBe(true);
    expect(isBenchmarkOnly("timesfm25")).toBe(false);
  });

  it.each(readdirSync(DATA_DIR).filter((f) => f.endsWith(".json")))(
    "src/ml/data/%s names none",
    (file) => {
      expect(readFileSync(join(DATA_DIR, file), "utf8").match(modelIdPattern)).toBeNull();
    },
  );

  it("the model comparison matrix names none", () => {
    const names = [
      ...COMPARISON_MODELS.map((m) => m.id),
      ...COMPARISON_QUESTIONS.flatMap((q) => [
        ...Object.keys(q.models),
        ...Object.values(q.models).flatMap((v) => v ?? []),
      ]),
      ...Object.values(PAIRED_MODELS),
      ...Object.values(PAIRED_QUESTION_MODELS).flat(),
    ];
    expect(names.filter(isBenchmarkOnly)).toEqual([]);
  });
});

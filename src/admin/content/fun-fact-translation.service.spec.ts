import {
  FunFactTranslationService,
  factLines,
  parseTranslations,
  sourceHash,
  translationTargets,
} from "./fun-fact-translation.service";
import { FUN_FACT_TRANSLATOR } from "./fun-fact-translation.constants";

const EN = "The castle is 50 m tall.\nTaron opened in 2016.";
const HASH = sourceHash(factLines(EN));

/** An in-memory park row, a curation that writes into it, and a model that answers `reply`. */
function setup(park: Record<string, unknown>, reply: unknown) {
  const row: Record<string, unknown> = {
    id: "p1",
    name: "Phantasialand",
    ...park,
  };
  const parks = {
    findOne: jest.fn(async () => ({ ...row })),
    update: jest.fn(async (_id: string, values: Record<string, unknown>) => {
      Object.assign(row, values);
    }),
  };
  const curation = {
    curatePark: jest.fn(
      async (
        _id: string,
        patch: { fields: Record<string, unknown> },
        _actor?: unknown,
      ) => {
        Object.assign(row, patch.fields);
      },
    ),
  };
  const settings = {
    apiKey: jest.fn(async () => "sk-ant-test"),
    budgetSpent: jest.fn(async () => false),
    addTokens: jest.fn(async () => undefined),
  };
  const create = jest.fn(async () => ({
    content: [{ type: "text", text: JSON.stringify(reply) }],
    usage: { input_tokens: 300, output_tokens: 600 },
  }));
  class Testable extends FunFactTranslationService {
    protected client() {
      return { messages: { create } } as never;
    }
  }
  const service = new Testable(
    parks as never,
    curation as never,
    settings as never,
  );
  return { service, row, curation, create, settings };
}

const two = (prefix: string) => [`${prefix} one`, `${prefix} two`];

describe("FunFactTranslationService", () => {
  it("fills every empty locale from the saved one and marks it as the machine's", async () => {
    const { service, row, curation } = setup(
      { curatedFunFactsEn: EN },
      {
        de: two("de"),
        nl: two("nl"),
        fr: two("fr"),
        es: two("es"),
        it: two("it"),
      },
    );
    await service.translate("p1", "en");
    expect(row.curatedFunFactsDe).toBe("de one\nde two");
    expect(row.curatedFunFactsIt).toBe("it one\nit two");
    expect(row.funFactsMachine).toMatchObject({
      de: { from: "en", sourceHash: HASH },
      it: { from: "en", sourceHash: HASH },
    });
    // Written like an editor's save, under the translator's name.
    expect(curation.curatePark.mock.calls[0][2]).toBe(FUN_FACT_TRANSLATOR);
  });

  it("never touches a locale a human typed", async () => {
    const { service, row, create } = setup(
      { curatedFunFactsEn: EN, curatedFunFactsDe: "Von Hand geschrieben" },
      { nl: two("nl"), fr: two("fr"), es: two("es"), it: two("it") },
    );
    await service.translate("p1", "en");
    expect(row.curatedFunFactsDe).toBe("Von Hand geschrieben");
    expect(JSON.stringify(create.mock.calls[0])).not.toContain("de (German)");
  });

  it("translates the machine's locales again when the source changed, and only those", async () => {
    const { service, row } = setup(
      {
        curatedFunFactsEn: EN,
        curatedFunFactsDe: "alt",
        curatedFunFactsNl: "Met de hand",
        curatedFunFactsFr: "vieux",
        curatedFunFactsEs: "viejo",
        curatedFunFactsIt: "vecchio",
        funFactsMachine: {
          de: { from: "en", sourceHash: "older" },
          fr: { from: "en", sourceHash: "older" },
          es: { from: "en", sourceHash: HASH },
          it: { from: "en", sourceHash: "older" },
        },
      },
      { de: two("de"), fr: two("fr"), it: two("it") },
    );
    await service.translate("p1", "en");
    expect(row.curatedFunFactsDe).toBe("de one\nde two");
    expect(row.curatedFunFactsNl).toBe("Met de hand");
    expect(row.curatedFunFactsEs).toBe("viejo");
  });

  it("discards a locale whose answer has the wrong number of lines", async () => {
    const { service, row } = setup(
      { curatedFunFactsEn: EN },
      {
        de: ["nur eine"],
        nl: two("nl"),
        fr: two("fr"),
        es: two("es"),
        it: two("it"),
      },
    );
    await service.translate("p1", "en");
    expect(row.curatedFunFactsDe).toBeUndefined();
    expect(row.curatedFunFactsNl).toBe("nl one\nnl two");
    expect((row.funFactsMachine as Record<string, unknown>).de).toBeUndefined();
  });

  it("removes the machine's text when the source was emptied", async () => {
    const { service, row, create } = setup(
      {
        curatedFunFactsEn: null,
        curatedFunFactsDe: "maschinell",
        curatedFunFactsNl: "met de hand",
        funFactsMachine: { de: { from: "en", sourceHash: HASH } },
      },
      {},
    );
    await service.translate("p1", "en");
    expect(row.curatedFunFactsDe).toBeNull();
    expect(row.curatedFunFactsNl).toBe("met de hand");
    expect(row.funFactsMachine).toBeNull();
    expect(create).not.toHaveBeenCalled();
  });

  it("does nothing without a key and throws for a retry when the budget is spent", async () => {
    const quiet = setup({ curatedFunFactsEn: EN }, {});
    quiet.settings.apiKey.mockResolvedValue(null as never);
    await quiet.service.translate("p1", "en");
    expect(quiet.create).not.toHaveBeenCalled();

    const spent = setup({ curatedFunFactsEn: EN }, {});
    spent.settings.budgetSpent.mockResolvedValue(true);
    await expect(spent.service.translate("p1", "en")).rejects.toThrow("budget");
  });
});

describe("translationTargets and parseTranslations", () => {
  it("leaves out the source and every hand-typed locale", () => {
    expect(
      translationTargets(
        { curatedFunFactsEn: EN, curatedFunFactsFr: "à la main" },
        "en",
        HASH,
      ),
    ).toEqual(["de", "nl", "es", "it"]);
  });

  it("accepts a fenced JSON answer and refuses an empty or multi-line fact", () => {
    const fenced =
      "```json\n" + JSON.stringify({ de: two("de"), nl: ["a", ""] }) + "\n```";
    const { valid, rejected } = parseTranslations(fenced, 2, ["de", "nl"]);
    expect(valid).toEqual({ de: two("de") });
    expect(rejected).toHaveLength(1);
    expect(parseTranslations("no json", 2, ["de"]).valid).toEqual({});
  });
});

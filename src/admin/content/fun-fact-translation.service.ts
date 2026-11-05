import { createHash } from "node:crypto";
import { Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import Anthropic from "@anthropic-ai/sdk";
import { Repository } from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import {
  FUN_FACT_COLUMNS,
  type FunFactLocale,
  type FunFactsMachineMarks,
} from "../../parks/utils/curated-park-facts.util";
import { AssistantSettingsService } from "../../assistant/assistant-settings.service";
import { ASSISTANT_MODEL } from "../../assistant/assistant-chat.service";
import { AdminCurationService } from "./admin-curation.service";
import { FUN_FACT_TRANSLATOR } from "./fun-fact-translation.constants";

/** The editor's limit per locale (`maxLength` of the descriptor) and one fact's sensible most. */
const MAX_FIELD_CHARS = 2000;
const MAX_FACT_CHARS = 300;

const LANGUAGE: Record<FunFactLocale, string> = {
  en: "English",
  de: "German",
  nl: "Dutch",
  fr: "French",
  es: "Spanish",
  it: "Italian",
};

const SYSTEM = `You translate short fun facts about a theme park for park.fan, a site for park fans.
You get one fact per line in the source language. Answer with only a JSON object: one key per target language code, each holding an array with exactly one translated fact per input line, in the same order.
Write every fact the way a fan would say it in that language: natural and friendly, never stiff. Keep park names, ride names, numbers and units exactly as written. Add nothing, explain nothing, never merge or split lines.`;

type Facts = Record<string, string | null | undefined>;

/** One fact per non-blank line, trimmed: the same split the payload serves. */
export function factLines(text: string | null | undefined): string[] {
  return (text ?? "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
}

/** A short hash of the source text a translation was made from. */
export function sourceHash(lines: string[]): string {
  return createHash("sha256")
    .update(lines.join("\n"))
    .digest("hex")
    .slice(0, 16);
}

/**
 * The locales a translation from `source` may write: empty ones, and machine translations made
 * from another locale or from another version of this one. A locale typed by hand (no mark) is
 * never among them.
 */
export function translationTargets(
  park: Facts & { funFactsMachine?: FunFactsMachineMarks | null },
  source: FunFactLocale,
  hash: string,
): FunFactLocale[] {
  const marks = park.funFactsMachine ?? {};
  return (Object.keys(FUN_FACT_COLUMNS) as FunFactLocale[]).filter((locale) => {
    if (locale === source) return false;
    const mark = marks[locale];
    if (mark) return mark.from !== source || mark.sourceHash !== hash;
    return factLines(park[FUN_FACT_COLUMNS[locale]]).length === 0;
  });
}

/**
 * The model's answer, checked per locale: as many facts as the source has, each one line within
 * the limits. A locale that fails is left out and named, never saved half right.
 */
export function parseTranslations(
  raw: string,
  expected: number,
  targets: readonly FunFactLocale[],
): { valid: Partial<Record<FunFactLocale, string[]>>; rejected: string[] } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw.replace(/^\s*```(?:json)?\s*|\s*```\s*$/g, ""));
  } catch {
    return { valid: {}, rejected: targets.map((t) => `${t}: not JSON`) };
  }
  const answer = (parsed ?? {}) as Record<string, unknown>;
  const valid: Partial<Record<FunFactLocale, string[]>> = {};
  const rejected: string[] = [];
  for (const locale of targets) {
    const lines = answer[locale];
    if (!Array.isArray(lines) || lines.length !== expected) {
      rejected.push(
        `${locale}: ${Array.isArray(lines) ? lines.length : 0} lines, wanted ${expected}`,
      );
      continue;
    }
    const facts = lines.map((line) =>
      typeof line === "string" ? line.trim() : "",
    );
    const bad = facts.find(
      (fact) => !fact || fact.includes("\n") || fact.length > MAX_FACT_CHARS,
    );
    if (bad !== undefined || facts.join("\n").length > MAX_FIELD_CHARS) {
      rejected.push(`${locale}: a fact empty, on several lines or too long`);
      continue;
    }
    valid[locale] = facts;
  }
  return { valid, rejected };
}

/**
 * Fills the other locales of a park's fun facts from the one an editor saved, with the model
 * Parky runs on and the key and daily budget his settings hold. Runs as a queue job, so the save
 * never waits for it; a failed call throws for the queue to retry, a bad answer is logged and
 * dropped. Writes through {@link AdminCurationService.curatePark} like an editor, so the audit
 * row, the undo and the cache publish are the same.
 */
@Injectable()
export class FunFactTranslationService {
  private readonly logger = new Logger(FunFactTranslationService.name);

  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    private readonly curation: AdminCurationService,
    private readonly settings: AssistantSettingsService,
  ) {}

  /** The client; its own method so a test can hand in a fake. */
  protected client(apiKey: string): Pick<Anthropic, "messages"> {
    return new Anthropic({ apiKey, maxRetries: 0, timeout: 60_000 });
  }

  async translate(parkId: string, source: FunFactLocale): Promise<void> {
    const park = await this.parks.findOne({ where: { id: parkId } });
    if (!park) return;
    const facts = park as unknown as Facts;
    const lines = factLines(facts[FUN_FACT_COLUMNS[source]]);
    const marks = park.funFactsMachine ?? {};

    if (lines.length === 0) {
      // The source was emptied: what the machine made from it goes too, human text stays.
      const stale = (Object.keys(marks) as FunFactLocale[]).filter(
        (locale) => marks[locale]?.from === source,
      );
      if (stale.length > 0) {
        await this.write(
          park,
          source,
          Object.fromEntries(stale.map((l) => [l, null])),
          "",
        );
      }
      return;
    }

    const hash = sourceHash(lines);
    const targets = translationTargets(park as never, source, hash);
    if (targets.length === 0) return;

    const apiKey = await this.settings.apiKey();
    if (!apiKey) {
      this.logger.warn(
        `No Claude key stored; ${park.name}'s fun facts stay untranslated`,
      );
      return;
    }
    if (await this.settings.budgetSpent()) {
      throw new Error("Today's token budget is spent; the queue retries later");
    }

    const response = await this.client(apiKey).messages.create({
      model: ASSISTANT_MODEL,
      max_tokens: 4000,
      system: SYSTEM,
      messages: [
        {
          role: "user",
          content:
            `Source language: ${source} (${LANGUAGE[source]}). ` +
            `Target languages: ${targets.map((t) => `${t} (${LANGUAGE[t]})`).join(", ")}.\n\n` +
            lines.join("\n"),
        },
      ],
    });
    await this.settings.addTokens(
      response.usage.input_tokens + response.usage.output_tokens,
    );
    const text = response.content
      .map((block) => (block.type === "text" ? block.text : ""))
      .join("");
    const { valid, rejected } = parseTranslations(text, lines.length, targets);
    if (rejected.length > 0) {
      this.logger.warn(
        `Fun-fact translation for ${park.name} discarded: ${rejected.join("; ")}`,
      );
    }
    if (Object.keys(valid).length === 0) return;
    await this.write(
      park,
      source,
      Object.fromEntries(
        Object.entries(valid).map(([locale, out]) => [locale, out.join("\n")]),
      ),
      hash,
    );
  }

  /**
   * Writes the locales that are still the machine's to write: the park is read again, so an
   * editor who typed a locale or changed the source meanwhile keeps their text.
   */
  private async write(
    before: Park,
    source: FunFactLocale,
    values: Partial<Record<FunFactLocale, string | null>>,
    hash: string,
  ): Promise<void> {
    const park = await this.parks.findOne({ where: { id: before.id } });
    if (!park) return;
    const facts = park as unknown as Facts;
    if (hash && sourceHash(factLines(facts[FUN_FACT_COLUMNS[source]])) !== hash)
      return;
    const marks = { ...(park.funFactsMachine ?? {}) };
    const fields: Record<string, string | null> = {};
    for (const [locale, value] of Object.entries(values) as Array<
      [FunFactLocale, string | null]
    >) {
      const ours =
        !!marks[locale] ||
        factLines(facts[FUN_FACT_COLUMNS[locale]]).length === 0;
      if (!ours) continue;
      fields[FUN_FACT_COLUMNS[locale]] = value;
      if (value === null) delete marks[locale];
      else marks[locale] = { from: source, sourceHash: hash };
    }
    if (Object.keys(fields).length === 0) return;
    await this.curation.curatePark(
      park.id,
      {
        fields,
        reason: `Automatic translation from ${source} (${ASSISTANT_MODEL})`,
      },
      FUN_FACT_TRANSLATOR,
    );
    await this.parks.update(park.id, {
      funFactsMachine: Object.keys(marks).length > 0 ? marks : null,
    } as never);
  }
}

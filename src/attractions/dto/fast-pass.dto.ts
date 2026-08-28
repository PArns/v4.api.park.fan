import { ApiProperty } from "@nestjs/swagger";
import { SUPPORTED_CURRENCIES } from "../utils/fast-pass.util";

/**
 * The paid queue-jump product a ride sells.
 *
 * Present only for rides where somebody has confirmed one exists. Absent covers
 * both "nobody has looked" and "we looked and there is none" — deliberately, and
 * for the reason CLAUDE.md §4 keeps repeating: our own bookkeeping must not
 * reach a visitor as the park's statement. A client rendering "kein Fastpass"
 * from an absent object would be doing exactly that.
 *
 * Composed rather than formatted here: the price is a number and a currency, and
 * "12 €" versus "€12" is a locale decision the frontend makes with
 * `Intl.NumberFormat`. The API has no business guessing which of six languages
 * the reader is in.
 */
export class FastPassDto {
  @ApiProperty({
    description:
      "What the park calls it — 'QuickPass' at Phantasialand, 'Express Pass' " +
      "at Heide Park. Never empty: falls back to the neutral 'Fast Pass' when " +
      "nobody has named the product.",
    example: "QuickPass",
  })
  name: string;

  @ApiProperty({
    description:
      "Price for this one ride, in `currency`. **0 means free** — Europa-Park's " +
      "Virtual Line is included with admission — so never test this field for " +
      "truthiness. Null means unknown, which includes the products the operator " +
      "prices per day (Disney, Universal), where a frozen number would be wrong " +
      "most days.",
    example: 12,
    required: false,
    nullable: true,
  })
  price?: number | null;

  @ApiProperty({
    description:
      "ISO-4217 code the price is quoted in, curated on the park. A positive " +
      "`price` without one is withheld — a bare number is not a price. A free " +
      "product needs none.",
    example: "EUR",
    enum: SUPPORTED_CURRENCIES,
    required: false,
    nullable: true,
  })
  currency?: string | null;

  @ApiProperty({
    description:
      "What the park's cheapest version of the product costs, for the parks " +
      "that sell one pass per visit rather than one per ride — which is nearly " +
      'all of them. Render as "from 25 €". Null whenever `price` is set: ' +
      "they answer the same question.",
    example: 25,
    required: false,
    nullable: true,
  })
  priceFrom?: number | null;

  @ApiProperty({
    description:
      "Glossary term id explaining what this kind of product buys you, for a " +
      "link out of the chip (e.g. `quick-pass`). The frontend owns the " +
      "glossary and resolves the id; null where none is curated.",
    example: "quick-pass",
    required: false,
    nullable: true,
  })
  termId?: string | null;
}

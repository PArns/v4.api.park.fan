import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  JoinColumn,
  Index,
  CreateDateColumn,
  UpdateDateColumn,
  Check,
} from "typeorm";
import { Park } from "./park.entity";

/**
 * What kind of thing a season is. The strings are API contract — the frontend
 * translates them into six languages — so renaming one is breaking.
 *
 * - `halloween` / `christmas`: the two big overlay seasons, and the ones with
 *   the most schedule detail attached to them.
 * - `summer_nights`: late openings over the summer holidays.
 * - `special_event`: anything else with a name and a date range — an
 *   anniversary, a food festival, a ride's opening week.
 * - `opening`: the park's own operating season, for parks that close for the
 *   winter. Its end date is when the park shuts, not when an overlay does.
 * - `closure`: the inverse — a stretch the park is deliberately shut, whether
 *   that is the winter or a rebuild.
 * - `maintenance`: a named ride or area is down inside an otherwise normal
 *   season (Efteling's Danse Macabre, 21.9.–2.10.2026).
 */
export const PARK_SEASON_KINDS = [
  "halloween",
  "christmas",
  "summer_nights",
  "special_event",
  "opening",
  "closure",
  "maintenance",
] as const;
export type ParkSeasonKind = (typeof PARK_SEASON_KINDS)[number];

/**
 * How settled this is.
 *
 * Not decoration. A visitor planning October needs to know the difference
 * between "the park has published these dates" and "the park did this last
 * year and has announced nothing" — and on 2026-08-19, Walibi Belgium and
 * Bobbejaanland had announced nothing for 2026 while seven other parks had.
 * Writing `announced` for all of them would have been a lie with dates on it.
 */
export const PARK_SEASON_STATUSES = [
  "confirmed",
  "announced",
  "expected",
  "cancelled",
] as const;
export type ParkSeasonStatus = (typeof PARK_SEASON_STATUSES)[number];

/**
 * Park Season Entity
 *
 * A named stretch of the year at one park: Halloween, the Christmas market,
 * the winter closure, a maintenance window.
 *
 * Why this is a table and not a derivation. `schedule_entries` already knows
 * when a park is open, and it is no help here: a park running Halloween Fright
 * Nights and a park having a normal late-closing Saturday produce the same row.
 * The difference is what the park is *doing*, which exists nowhere in any feed
 * we ingest. Until now it existed only in prose — the Halloween guide on the
 * frontend carries researched dates for nine parks in six languages, which is
 * the correct place for the story and a hopeless place for a date a park page
 * wants to render.
 *
 * Why `dates` exists next to `startDate`/`endDate`. Because a season is very
 * often not a range. Walibi Holland's 2026 calendar is the worked example, and
 * it is also why this was not modelled as two dates and left alone: Spooky Days
 * fall on the 14th, 15th, 19th, 20th and 21st of October, while Fright Nights
 * run every weekend between 3 October and 1 November plus three single dates.
 * Storing that as 3 Oct – 1 Nov would tell a visitor the park is haunted on a
 * Tuesday. The range stays as the season's outer bounds — it is what a heading
 * says and what a query filters on — and `dates`, when present, is the truth
 * about which days inside it actually count.
 *
 * Nothing syncs this. Every row is written by a person through the admin, and
 * carries the URL they read it from, because a season is a claim about the
 * world and a claim needs its evidence attached.
 */
@Entity("park_seasons")
@Index("idx_park_season_park_start", ["parkId", "startDate"])
@Index("idx_park_season_kind_start", ["kind", "startDate"])
// A season that ends before it starts is a typo, and one that reaches the
// public is worse than a rejected form: it silently drops out of every range
// query instead of showing up wrong.
@Check(`"end_date" >= "start_date"`)
export class ParkSeason {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @ManyToOne(() => Park, { onDelete: "CASCADE" })
  @JoinColumn({ name: "park_id" })
  park: Park;

  @Column({ name: "park_id", type: "uuid" })
  parkId: string;

  @Column({ type: "text" })
  kind: ParkSeasonKind;

  /**
   * The season's own name, untranslated: "Halloween Horror Nights",
   * "Traumatica", "Fright Nights", "Winterdroom".
   *
   * Deliberately not a localized map. These are brand names a park has chosen
   * and prints on its own posters; translating "Halloween Fright Nights" into
   * German would produce something no visitor has ever seen. Null falls back
   * to the frontend's own label for the `kind`, which is translated.
   */
  @Column({ type: "text", nullable: true })
  name: string | null;

  /** First day of the season, park-local. */
  @Column({ name: "start_date", type: "date" })
  startDate: string;

  /** Last day, inclusive. Equal to `startDate` for a one-day event. */
  @Column({ name: "end_date", type: "date" })
  endDate: string;

  /**
   * The individual dates inside the range that the season actually runs, as
   * `YYYY-MM-DD` strings, ordered.
   *
   * Null means "every day between start and end", which is the common case and
   * must stay distinguishable from an empty array (which would mean the season
   * runs on no day at all — a state this column has no business representing,
   * so the endpoint rejects it).
   */
  @Column({ name: "dates", type: "jsonb", nullable: true })
  dates: string[] | null;

  @Column({ type: "text", default: "announced" })
  status: ParkSeasonStatus;

  /**
   * Whether getting in needs more than a normal day ticket.
   *
   * The question a visitor actually asks about a Halloween event, and the one
   * the answer differs on park by park: Parc Astérix's twelve Nocturne dates
   * are a separate 53 € ticket, Heide-Park's long days are included in the day
   * ticket, and Europa-Park's Vampire's Club nights are a separate ticket for
   * an area inside a park you also need a ticket for.
   */
  @Column({ name: "separate_ticket", type: "boolean", default: false })
  separateTicket: boolean;

  /** Cheapest advertised price in minor-unit-free decimal, e.g. 53.00. */
  @Column({
    name: "price_from",
    type: "decimal",
    precision: 8,
    scale: 2,
    nullable: true,
  })
  priceFrom: string | null;

  /** ISO 4217, e.g. "EUR". Only meaningful next to `priceFrom`. */
  @Column({ name: "price_currency", type: "text", nullable: true })
  priceCurrency: string | null;

  /**
   * Opening and closing time on the season's days, park-local `HH:MM`.
   *
   * A separate concept from `schedule_entries`: those record what the feed
   * says the park does, these record what the park announced for the event,
   * and the two disagree for exactly the parks worth knowing about — an
   * evening event that starts after the day guests are ushered out has no
   * OPERATING row of its own at all.
   */
  @Column({ name: "opens_at", type: "text", nullable: true })
  opensAt: string | null;

  @Column({ name: "closes_at", type: "text", nullable: true })
  closesAt: string | null;

  /**
   * Which attractions this season is about, as attraction UUIDs.
   *
   * Used by `maintenance` (this ride is down) and by overlay seasons that
   * re-theme specific rides. Deliberately a plain array rather than a join
   * table: it is read whole, written whole, never queried from the other side,
   * and a join table would be three files for a list of ids.
   */
  @Column({ name: "attraction_ids", type: "jsonb", nullable: true })
  attractionIds: string[] | null;

  /** The park's own page for the event. */
  @Column({ type: "text", nullable: true })
  url: string | null;

  /**
   * Where this was established, and when it was last checked.
   *
   * `confirmedAt` is not `updatedAt`: a row can be edited to fix a typo without
   * anybody re-reading the park's announcement. The frontend shows "as of
   * <date>" from this, which is the honest thing to put next to dates a park
   * may still change.
   */
  @Column({ name: "source_url", type: "text", nullable: true })
  sourceUrl: string | null;

  @Column({ name: "confirmed_at", type: "timestamptz", nullable: true })
  confirmedAt: Date | null;

  /** Anything a future reader needs that the fields above cannot hold. */
  @Column({ type: "text", nullable: true })
  note: string | null;

  /** The admin account that last wrote this row; null for legacy writes. */
  @Column({ name: "updated_by", type: "uuid", nullable: true })
  updatedBy: string | null;

  @CreateDateColumn({ name: "created_at", type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt: Date;
}

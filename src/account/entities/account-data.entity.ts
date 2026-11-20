import { Column, Entity, PrimaryColumn, UpdateDateColumn } from "typeorm";

/** The favorites a visitor keeps, by kind, as the site's `favorites` cookie holds them. */
export interface AccountFavorites {
  parks: string[];
  attractions: string[];
  shows: string[];
  restaurants: string[];
}

/**
 * A park a visitor ticked off as visited in a trip region's hub: the region's
 * slug, the park's slug and the region's date it was ticked (`YYYY-MM-DD`).
 * The site lets a tick count for one trip's length; the date says which trip.
 */
export interface AccountVisitedPark {
  region: string;
  park: string;
  date: string;
}

/**
 * What a signed-in visitor's account carries across devices: the favorites,
 * the rides marked as ridden, the parks ticked off as visited on a trip and the
 * temperature unit, the state the site otherwise keeps in one browser's cookies
 * and storage. One row per account,
 * written whole by the account's own browser; the account's deletion removes it
 * (`AccountDataDeletion`). Trips and push alerts stay with their own ids, since
 * an alert belongs to one device's push subscription.
 */
@Entity("account_data")
export class AccountData {
  @PrimaryColumn({ name: "user_id", type: "uuid" })
  userId: string;

  @Column({
    type: "jsonb",
    default: () =>
      `'{"parks":[],"attractions":[],"shows":[],"restaurants":[]}'::jsonb`,
  })
  favorites: AccountFavorites;

  @Column({ name: "ridden_rides", type: "jsonb", default: () => "'[]'::jsonb" })
  riddenRides: string[];

  @Column({
    name: "visited_parks",
    type: "jsonb",
    default: () => "'[]'::jsonb",
  })
  visitedParks: AccountVisitedPark[];

  /** `C` or `F`; null until the visitor picked one. */
  @Column({
    name: "temperature_unit",
    type: "varchar",
    length: 1,
    nullable: true,
  })
  temperatureUnit: "C" | "F" | null;

  @UpdateDateColumn({ name: "updated_at", type: "timestamptz" })
  updatedAt: Date;
}

import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  BeforeInsert,
  BeforeUpdate,
  Index,
  JoinColumn,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { RestaurantLiveData } from "./restaurant-live-data.entity";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * Restaurant Entity
 *
 * Represents a restaurant/dining location within a park.
 * Examples: "Be Our Guest Restaurant", "Cinderella's Royal Table", "Backlot Express"
 *
 * API Mapping (from GET /v1/entity/{id}/children):
 * - id → externalId
 * - name → name
 * - location.latitude → latitude
 * - location.longitude → longitude
 * - parentId → park (FK)
 * - slug → slug (auto-generated)
 */
@Entity("restaurants")
@Index(["parkId", "slug"], { unique: true }) // Slug unique per park
// GIN trigram index for fuzzy search — managed by SearchService.onModuleInit(), not TypeORM sync
@Index("idx_restaurant_name_trgm", { synchronize: false })
export class Restaurant {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  @Index()
  externalId: string; // ThemeParks.wiki ID

  @Column()
  @Index()
  name: string;

  @Column()
  @Index()
  slug: string; // Unique per park, not globally

  @ManyToOne(() => Park, (park) => park.restaurants)
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column()
  @Index() // Optimize relation loading when fetching restaurants by park
  parkId: string;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  latitude: number;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  longitude: number;

  // Land/Area (from Queue-Times)
  @Column({ name: "land_name", nullable: true })
  landName: string;

  @Column({ name: "land_external_id", nullable: true })
  landExternalId: string;

  // Restaurant-specific fields
  @Column({ nullable: true })
  cuisineType: string; // e.g., "American", "Italian", "Asian Fusion"

  @Column({ type: "text", array: true, nullable: true })
  cuisines: string[];

  @Column({ type: "boolean", default: false })
  requiresReservation: boolean; // If reservation is required/recommended

  /**
   * When this restaurant stopped existing. Null means it is still around.
   *
   * The mirror of `Attraction.retiredAt`, and it exists for the same reason:
   * an entity that has gone away is not "closed today" and not "unknown"
   * either — both of those describe a state it could come back from. Without
   * this column a restaurant whose entity ThemeParks.wiki reclassified as an
   * `ATTRACTION` kept a second, permanent row here while the attractions table
   * grew the real one.
   *
   * The row and its `restaurant_live_data` are deliberately KEPT: the opening
   * history stays readable. What retirement changes is visibility — a retired
   * restaurant leaves the park payload, search and favorites, while its own
   * detail endpoint keeps answering.
   *
   * **Two writers, told apart by `retiredReason`**, exactly as on the
   * attraction side: a reason this sync wrote is lifted again the moment the
   * wiki calls the entity a `RESTAURANT`, and anything else survives every run.
   */
  @Column({ name: "retired_at", type: "timestamptz", nullable: true })
  @Index("idx_restaurant_retired_at", { where: "retired_at IS NULL" })
  retiredAt: Date | null;

  /**
   * Why, and on whose authority — the source URL belongs in here. A retirement
   * is a claim about the world, so it travels with its evidence.
   */
  @Column({ name: "retired_reason", type: "text", nullable: true })
  retiredReason: string | null;

  @OneToMany(
    () => RestaurantLiveData,
    (restaurantLiveData) => restaurantLiveData.restaurant,
  )
  restaurantLiveData: RestaurantLiveData[];

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;

  @BeforeInsert()
  @BeforeUpdate()
  generateSlug(): void {
    if (this.name && !this.slug) {
      this.slug = generateSlug(this.name);
    }
  }
}

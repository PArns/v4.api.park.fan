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

import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  ManyToOne,
  CreateDateColumn,
  UpdateDateColumn,
  BeforeInsert,
  BeforeUpdate,
  Index,
  JoinColumn,
  OneToMany,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";
import { QueueData } from "../../queue-data/entities/queue-data.entity";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * Attraction Entity
 *
 * Represents a ride/attraction within a park.
 * Examples: "Taron", "Space Mountain", "Flight of Passage"
 *
 * API Mapping (from GET /v1/entity/{id}/children):
 * - id → externalId
 * - name → name
 * - location.latitude → latitude
 * - location.longitude → longitude
 * - parentId → park (FK)
 * - slug → slug (from API or auto-generated)
 */
@Entity("attractions")
@Index(["parkId", "slug"], { unique: true }) // Slug unique per park
export class Attraction {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  @Index()
  externalId: string; // ThemeParks.wiki ID

  @Column()
  name: string;

  @Column()
  @Index()
  slug: string; // Unique per park, not globally

  @ManyToOne(() => Park, (park) => park.attractions)
  @JoinColumn({ name: "parkId" })
  park: Park;

  @Column()
  parkId: string;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  latitude: number;

  @Column({ type: "decimal", precision: 10, scale: 7, nullable: true })
  longitude: number;

  // Land/Area (from Queue-Times)
  @Column({ name: "land_name", type: "varchar", nullable: true })
  landName: string | null; // e.g., "Tomorrowland", "Adventureland"

  @Column({ name: "land_external_id", type: "varchar", nullable: true })
  landExternalId: string | null; // Queue-Times land ID

  @Column({ nullable: true })
  attractionType: string;

  @OneToMany(() => QueueData, (queueData) => queueData.attraction)
  queueData: QueueData[];

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;

  @BeforeInsert()
  @BeforeUpdate()
  generateSlug(): void {
    if (this.name && !this.slug) {
      this.slug = generateSlug(this.name);
    }
  }
}

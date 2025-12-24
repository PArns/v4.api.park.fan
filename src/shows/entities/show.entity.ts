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
import { ShowLiveData } from "./show-live-data.entity";
import { generateSlug } from "../../common/utils/slug.util";

/**
 * Show Entity
 *
 * Represents a show/performance within a park.
 * Examples: "Festival of the Lion King", "Fantasmic!", "Indiana Jones Stunt Spectacular"
 *
 * API Mapping (from GET /v1/entity/{id}/children):
 * - id → externalId
 * - name → name
 * - location.latitude → latitude
 * - location.longitude → longitude
 * - parentId → park (FK)
 * - slug → slug (auto-generated)
 */
@Entity("shows")
@Index(["parkId", "slug"], { unique: true }) // Slug unique per park
export class Show {
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

  @ManyToOne(() => Park, (park) => park.shows)
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

  @OneToMany(() => ShowLiveData, (liveData) => liveData.show)
  liveData: ShowLiveData[];

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

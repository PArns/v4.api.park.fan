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
// GIN trigram index for fuzzy search — managed by SearchService.onModuleInit(), not TypeORM sync
@Index("idx_show_name_trgm", { synchronize: false })
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
  @Index() // Optimize relation loading when fetching shows by park
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

  @Column({ name: "is_seasonal", default: false })
  isSeasonal: boolean;

  @Column({ name: "season_months", type: "jsonb", nullable: true })
  seasonMonths: number[] | null;

  /**
   * When this show stopped existing. Null means it is still around.
   *
   * The mirror of `Attraction.retiredAt`, and it exists for the same reason:
   * an entity that has gone away is not "closed today" and not "unknown"
   * either — both of those describe a state it could come back from. Without
   * this column a show whose entity ThemeParks.wiki reclassified as an
   * `ATTRACTION` kept a second, permanent row here while the attractions table
   * grew the real one.
   *
   * The row and its `show_live_data` are deliberately KEPT: the showtime
   * history stays readable. What retirement changes is visibility — a retired
   * show leaves the park payload, search, favorites and follows, while its own
   * detail endpoint keeps answering.
   *
   * **Two writers, told apart by `retiredReason`**, exactly as on the
   * attraction side: a reason this sync wrote is lifted again the moment the
   * wiki calls the entity a `SHOW`, and anything else survives every run.
   */
  @Column({ name: "retired_at", type: "timestamptz", nullable: true })
  @Index("idx_show_retired_at", { where: "retired_at IS NULL" })
  retiredAt: Date | null;

  /**
   * Why, and on whose authority — the source URL belongs in here. A retirement
   * is a claim about the world, so it travels with its evidence.
   */
  @Column({ name: "retired_reason", type: "text", nullable: true })
  retiredReason: string | null;

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

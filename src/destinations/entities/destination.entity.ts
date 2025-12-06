import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
  Index,
} from "typeorm";
import { Park } from "../../parks/entities/park.entity";

/**
 * Destination Entity (Resort-level)
 *
 * Represents a destination/resort containing multiple parks.
 * Examples: "Walt Disney World Resort", "Disneyland Paris Resort"
 *
 * API Mapping:
 * - id → externalId (ThemeParks.wiki UUID)
 * - name → name
 * - slug → slug (from API, fallback to generated)
 */
@Entity("destinations")
export class Destination {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  @Column({ unique: true })
  @Index()
  externalId: string; // ThemeParks.wiki ID

  @Column()
  name: string;

  @Column({ unique: true })
  @Index()
  slug: string;

  @OneToMany(() => Park, (park) => park.destination)
  parks: Park[];

  @CreateDateColumn()
  createdAt: Date;

  @UpdateDateColumn()
  updatedAt: Date;
}

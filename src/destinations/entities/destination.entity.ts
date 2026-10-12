import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  OneToMany,
  CreateDateColumn,
  UpdateDateColumn,
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

  // No @Index() here: `unique: true` already creates a UNIQUE constraint on
  // this column, and a second, non-unique btree beside it was a pure duplicate
  // (PAR-872).
  @Column({ unique: true })
  externalId: string; // ThemeParks.wiki ID

  @Column()
  name: string;

  // No @Index() here: `unique: true` already creates a UNIQUE constraint on
  // this column, and a second, non-unique btree beside it was a pure duplicate
  // (PAR-872).
  @Column({ unique: true })
  slug: string;

  @OneToMany(() => Park, (park) => park.destination)
  parks: Park[];

  @CreateDateColumn({ type: "timestamptz" })
  createdAt: Date;

  @UpdateDateColumn({ type: "timestamptz" })
  updatedAt: Date;
}

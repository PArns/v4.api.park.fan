import {
  Entity,
  PrimaryGeneratedColumn,
  Column,
  CreateDateColumn,
  Index,
} from "typeorm";

/**
 * External Entity Mapping
 *
 * Maps internal entities to external source IDs for multi-source support.
 * Enables tracking which external IDs correspond to our internal entities.
 */
@Entity("external_entity_mapping")
@Index(["externalSource", "externalEntityId"], { unique: true })
@Index(["internalEntityId", "internalEntityType"])
export class ExternalEntityMapping {
  @PrimaryGeneratedColumn("uuid")
  id: string;

  // Internal entity reference
  @Column({ name: "internal_entity_id" })
  internalEntityId: string;

  @Column({ name: "internal_entity_type", length: 50 })
  internalEntityType: "park" | "attraction" | "show" | "restaurant";

  // External source reference
  @Column({ name: "external_source", length: 50 })
  externalSource: string; // 'themeparks-wiki', 'queue-times', etc.

  @Column({ name: "external_entity_id", length: 255 })
  externalEntityId: string;

  // Matching metadata
  @Column("decimal", {
    precision: 3,
    scale: 2,
    nullable: true,
    name: "match_confidence",
  })
  matchConfidence: number; // 0.00 to 1.00

  @Column({ length: 50, nullable: true, name: "match_method" })
  matchMethod: "exact" | "fuzzy" | "manual" | "geographic";

  @Column({ default: false })
  verified: boolean;

  @CreateDateColumn({ name: "created_at" })
  createdAt: Date;
}

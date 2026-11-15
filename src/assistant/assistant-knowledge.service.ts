import { Injectable, Logger, OnModuleInit } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { AdminAuditService } from "../admin/auth/admin-audit.service";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { AssistantKnowledge } from "./entities/assistant-knowledge.entity";
import {
  KNOWLEDGE_SEED,
  knowledgeIndex,
  type KnowledgeWrite,
} from "./knowledge";

/** How long one API process serves the enabled topics before it reads them again. */
const CACHE_MS = 60_000;

/** The audit log's name for a topic, so its history is one filter away. */
export const KNOWLEDGE_AUDIT_ENTITY = "assistant_knowledge";

/** A topic as the admin and the MCP show it. */
export interface KnowledgeView {
  topic: string;
  readWhen: string;
  text: string;
  enabled: boolean;
  updatedBy: string | null;
  createdAt: Date;
  updatedAt: Date;
}

/**
 * Parky's knowledge base, the wiki the admin and the admin MCP edit: one row per topic. The chat
 * lists the enabled topics in its prompt and read_knowledge returns one; every write is audited
 * with the whole topic before and after, which is the topic's history.
 */
@Injectable()
export class AssistantKnowledgeService implements OnModuleInit {
  private readonly logger = new Logger(AssistantKnowledgeService.name);
  private cache: { at: number; topics: AssistantKnowledge[] } | null = null;

  constructor(
    @InjectRepository(AssistantKnowledge)
    private readonly repository: Repository<AssistantKnowledge>,
    private readonly audit: AdminAuditService,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.seed();
  }

  /**
   * Fills an empty base with the seed files' topics. Only an empty one: a topic deleted in the
   * admin stays deleted, and an edited one is never overwritten. A failure keeps the API up.
   */
  async seed(): Promise<number> {
    try {
      if ((await this.repository.count()) > 0) return 0;
      await this.repository.insert(
        KNOWLEDGE_SEED.map((entry) => ({
          topic: entry.id,
          readWhen: entry.readWhen,
          text: entry.text,
          enabled: true,
          updatedBy: null,
        })),
      );
      this.cache = null;
      return KNOWLEDGE_SEED.length;
    } catch (error) {
      this.logger.error(
        `Could not seed the knowledge base: ${(error as Error).message}`,
      );
      return 0;
    }
  }

  private async enabledTopics(): Promise<AssistantKnowledge[]> {
    if (this.cache && Date.now() - this.cache.at < CACHE_MS) {
      return this.cache.topics;
    }
    const topics = await this.repository.find({
      where: { enabled: true },
      order: { topic: "ASC" },
    });
    this.cache = { at: Date.now(), topics };
    return topics;
  }

  /** The prompt's list of enabled topics, one line each; empty without any. */
  async promptIndex(): Promise<string> {
    try {
      const topics = await this.enabledTopics();
      return knowledgeIndex(
        topics.map((row) => ({ id: row.topic, readWhen: row.readWhen })),
      );
    } catch (error) {
      // A chat without the list still answers; it only cannot read a topic.
      this.logger.warn(
        `Knowledge index unavailable: ${(error as Error).message}`,
      );
      return "";
    }
  }

  /** One enabled topic, for read_knowledge; null when there is none by that name. */
  async read(topic: string): Promise<AssistantKnowledge | null> {
    return (
      (await this.enabledTopics()).find((row) => row.topic === topic) ?? null
    );
  }

  /** The names of the enabled topics, for a refusal that says which exist. */
  async topicNames(): Promise<string[]> {
    return (await this.enabledTopics()).map((row) => row.topic);
  }

  /** Every topic, switched off ones too, by name. */
  async list(): Promise<KnowledgeView[]> {
    const rows = await this.repository.find({ order: { topic: "ASC" } });
    return rows.map(view);
  }

  async get(topic: string): Promise<KnowledgeView | null> {
    const row = await this.repository.findOneBy({ topic });
    return row ? view(row) : null;
  }

  /**
   * Creates the topic or replaces its fields, and audits the whole topic before and after. A
   * save that changes nothing writes nothing.
   */
  async save(
    topic: string,
    input: KnowledgeWrite,
    admin: AdminPrincipal,
  ): Promise<{ topic: KnowledgeView; created: boolean; changed: boolean }> {
    const before = await this.repository.findOneBy({ topic });
    if (
      before &&
      before.readWhen === input.readWhen &&
      before.text === input.text &&
      before.enabled === input.enabled
    ) {
      return { topic: view(before), created: false, changed: false };
    }
    const saved = await this.repository.save(
      this.repository.create({
        ...(before ?? {}),
        topic,
        readWhen: input.readWhen,
        text: input.text,
        enabled: input.enabled,
        updatedBy: admin.email,
      }),
    );
    this.cache = null;
    await this.audit.record({
      actor: admin,
      action: before
        ? "assistant.knowledge.update"
        : "assistant.knowledge.create",
      entityType: KNOWLEDGE_AUDIT_ENTITY,
      entityId: topic,
      entityLabel: topic,
      before: before ? fields(before) : null,
      after: fields(saved),
    });
    return { topic: view(saved), created: !before, changed: true };
  }

  /** Deletes the topic for good; the audit row keeps all of it, enough to write it back. */
  async remove(
    topic: string,
    admin: AdminPrincipal,
  ): Promise<KnowledgeView | null> {
    const row = await this.repository.findOneBy({ topic });
    if (!row) return null;
    await this.repository.delete({ topic });
    this.cache = null;
    await this.audit.record({
      actor: admin,
      action: "assistant.knowledge.delete",
      entityType: KNOWLEDGE_AUDIT_ENTITY,
      entityId: topic,
      entityLabel: topic,
      before: fields(row),
    });
    return view(row);
  }

  /** The topic's edits, newest first: who wrote what, with the whole topic before and after. */
  async history(topic: string, limit = 20) {
    const { entries } = await this.audit.list({
      entityType: KNOWLEDGE_AUDIT_ENTITY,
      entityId: topic,
      limit,
    });
    return entries.map((entry) => ({
      at: entry.createdAt,
      action: entry.action,
      by: entry.actorEmail,
      before: entry.before,
      after: entry.after,
    }));
  }
}

function fields(row: AssistantKnowledge): Record<string, unknown> {
  return { readWhen: row.readWhen, text: row.text, enabled: row.enabled };
}

function view(row: AssistantKnowledge): KnowledgeView {
  return {
    topic: row.topic,
    readWhen: row.readWhen,
    text: row.text,
    enabled: row.enabled,
    updatedBy: row.updatedBy,
    createdAt: row.createdAt,
    updatedAt: row.updatedAt,
  };
}

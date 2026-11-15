import type { Repository } from "typeorm";
import type { AdminAuditService } from "../admin/auth/admin-audit.service";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { AssistantKnowledgeService } from "./assistant-knowledge.service";
import type { AssistantKnowledge } from "./entities/assistant-knowledge.entity";
import { KNOWLEDGE_SEED } from "./knowledge";
import { fillKnowledge, systemPromptWith } from "./system-prompt";

const ADMIN = { email: "you@park.fan", userId: "u" } as AdminPrincipal;

/** A table in memory, with the calls the service makes. */
function table(rows: AssistantKnowledge[] = []) {
  const store = new Map(rows.map((row) => [row.topic, { ...row }]));
  const repository = {
    count: jest.fn(async () => store.size),
    insert: jest.fn(async (list: AssistantKnowledge[]) => {
      for (const row of list) store.set(row.topic, { ...row });
    }),
    find: jest.fn(async (options?: { where?: { enabled?: boolean } }) =>
      [...store.values()]
        .filter((row) => options?.where?.enabled === undefined || row.enabled)
        .sort((a, b) => a.topic.localeCompare(b.topic)),
    ),
    findOneBy: jest.fn(
      async ({ topic }: { topic: string }) => store.get(topic) ?? null,
    ),
    create: jest.fn((row: AssistantKnowledge) => row),
    save: jest.fn(async (row: AssistantKnowledge) => {
      const saved = { ...row, updatedAt: new Date() };
      store.set(row.topic, saved);
      return saved;
    }),
    delete: jest.fn(async ({ topic }: { topic: string }) => {
      store.delete(topic);
    }),
  };
  const audit = { record: jest.fn(), list: jest.fn() };
  const service = new AssistantKnowledgeService(
    repository as unknown as Repository<AssistantKnowledge>,
    audit as unknown as AdminAuditService,
  );
  return { service, repository, audit, store };
}

const row = (topic: string, enabled = true): AssistantKnowledge =>
  ({
    topic,
    readWhen: `read ${topic} when it is needed`,
    text: `the background on ${topic}, long enough to say something`,
    enabled,
    updatedBy: null,
    createdAt: new Date(),
    updatedAt: new Date(),
  }) as AssistantKnowledge;

describe("AssistantKnowledgeService", () => {
  it("seeds an empty base with the seed topics, and never again", async () => {
    const { service, store } = table();
    expect(await service.seed()).toBe(KNOWLEDGE_SEED.length);
    expect(store.has("forecasts")).toBe(true);
    expect(await service.seed()).toBe(0);
  });

  it("leaves a base that has topics alone, a deleted seed topic included", async () => {
    const { service, repository } = table([row("tickets")]);
    expect(await service.seed()).toBe(0);
    expect(repository.insert).not.toHaveBeenCalled();
  });

  it("lists only enabled topics in the prompt, and reads only those", async () => {
    const { service } = table([row("forecasts"), row("tickets", false)]);
    const index = await service.promptIndex();
    expect(index).toBe("- forecasts: read forecasts when it is needed");
    expect(await service.read("tickets")).toBeNull();
    expect((await service.read("forecasts"))?.text).toContain("background");
  });

  it("audits a save with the whole topic before and after, and the next read sees it", async () => {
    const { service, audit } = table([row("forecasts")]);
    await service.promptIndex();
    const result = await service.save(
      "forecasts",
      {
        readWhen: "read it when a forecast is doubted",
        text: "the new background, long enough to say something",
        enabled: true,
      },
      ADMIN,
    );
    expect(result).toMatchObject({ created: false, changed: true });
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "assistant.knowledge.update",
        entityType: "assistant_knowledge",
        entityId: "forecasts",
        before: expect.objectContaining({
          readWhen: "read forecasts when it is needed",
        }),
        after: expect.objectContaining({
          readWhen: "read it when a forecast is doubted",
        }),
      }),
    );
    expect(await service.promptIndex()).toContain("a forecast is doubted");
  });

  it("writes nothing for a save that changes nothing", async () => {
    const { service, audit, repository } = table([row("forecasts")]);
    const same = row("forecasts");
    const result = await service.save(
      "forecasts",
      { readWhen: same.readWhen, text: same.text, enabled: true },
      ADMIN,
    );
    expect(result.changed).toBe(false);
    expect(repository.save).not.toHaveBeenCalled();
    expect(audit.record).not.toHaveBeenCalled();
  });

  it("keeps a deleted topic whole in the audit row", async () => {
    const { service, audit, store } = table([row("tickets")]);
    expect(await service.remove("tickets", ADMIN)).not.toBeNull();
    expect(store.has("tickets")).toBe(false);
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "assistant.knowledge.delete",
        before: expect.objectContaining({ text: row("tickets").text }),
      }),
    );
    expect(await service.remove("tickets", ADMIN)).toBeNull();
  });
});

describe("the prompt's knowledge slot", () => {
  it("lists the topics under # Knowledge", () => {
    const prompt = systemPromptWith("- forecasts: when a forecast is doubted");
    expect(prompt).toMatch(
      /# Knowledge[\s\S]*Topics:\n- forecasts: when a forecast is doubted/,
    );
    expect(prompt).not.toContain("{{knowledge topics}}");
  });

  it("says when there are none, and keeps a $ in a topic as written", () => {
    expect(systemPromptWith("")).toContain("(none at the moment)");
    expect(fillKnowledge("a {{knowledge topics}} b", "- x: costs $&")).toBe(
      "a - x: costs $& b",
    );
  });
});

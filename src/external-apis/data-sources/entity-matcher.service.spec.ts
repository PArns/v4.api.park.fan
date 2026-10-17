import { EntityMatcherService } from "./entity-matcher.service";
import { EntityMetadata, EntityType } from "./interfaces/data-source.interface";

/**
 * A source can list an entity without a name (null). Since PAR-803 that threw
 * inside normalizeForMatching and aborted the whole park's mapping sync every
 * night from 2026-10-14. An absent name must neither throw nor match.
 */
describe("EntityMatcherService with nameless entities", () => {
  const service = new EntityMatcherService();
  const entity = (
    source: string,
    externalId: string,
    name: string | null,
  ): EntityMetadata => ({
    externalId,
    source,
    entityType: EntityType.ATTRACTION,
    name: name as string,
  });

  it("does not throw and does not pair two nameless entities", () => {
    const result = service.matchEntities(
      [entity("wiki", "w1", null), entity("wiki", "w2", "Taron")],
      [entity("qt", "q1", null), entity("qt", "q2", "Taron")],
    );
    const pairs = result.matched.map((m) => [
      m.entity1.externalId,
      m.entity2.externalId,
    ]);
    expect(pairs).toEqual([["w2", "q2"]]);
    expect(result.unmatched1.map((e) => e.externalId)).toContain("w1");
    expect(result.unmatched2.map((e) => e.externalId)).toContain("q1");
  });
});

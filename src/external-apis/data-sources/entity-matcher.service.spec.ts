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

/**
 * PAR-924: the wiki names Hell House "Helhuis" and The Slaughterhouse "Het
 * slachthuis". Queue-Times and Wartezeiten carry the operator's name and no
 * coordinates, so on the feed name alone both scored under the 0.75 threshold
 * and every reading of them was dropped.
 */
describe("EntityMatcherService with a curated second name", () => {
  const service = new EntityMatcherService();
  const entity = (
    source: string,
    externalId: string,
    name: string,
    altNames?: string[],
  ): EntityMetadata => ({
    externalId,
    source,
    entityType: EntityType.ATTRACTION,
    name,
    altNames,
  });

  const wiki = (altNames: Record<string, string[]> = {}) =>
    [
      ["w-hell", "Helhuis"],
      ["w-slaughter", "Het slachthuis"],
      ["w-jason", "Jason-universum"],
      ["w-quiet", "A Quiet Place"],
      ["w-final", "Final Stop"],
      ["w-museum", "Murder Museum"],
      ["w-moon", "Blood Moon"],
      ["w-circus", "Circus of Freaks"],
      ["w-taron", "Taron"],
      ["w-starflyer", "Star Flyer"],
    ].map(([id, name]) => entity("themeparks-wiki", id, name, altNames[id]));

  const qt = [
    ["qt-ride-11118", "Hell House"],
    ["qt-ride-11121", "The Slaughterhouse"],
    ["qt-ride-17663", "Jason Universe"],
    ["qt-ride-1", "A Quiet Place"],
    ["qt-ride-2", "Final Stop"],
    ["qt-ride-3", "Murder Museum"],
    ["qt-ride-4", "Blood Moon"],
    ["qt-ride-5", "Circus of Freaks"],
    ["qt-ride-6", "Taron"],
    ["qt-ride-7", "Star Flyer"],
  ].map(([id, name]) => entity("queue-times", id, name));

  const pairs = (result: ReturnType<EntityMatcherService["matchEntities"]>) =>
    Object.fromEntries(
      result.matched.map((m) => [m.entity1.externalId, m.entity2.externalId]),
    );

  it("does not find the two Dutch-named mazes on the feed name alone", () => {
    const found = pairs(service.matchEntities(wiki(), qt));
    expect(found["w-hell"]).toBeUndefined();
    expect(found["w-slaughter"]).toBeUndefined();
    expect(found["w-jason"]).toBe("qt-ride-17663");
  });

  it("maps all nine mazes once the curated names are offered", () => {
    const found = pairs(
      service.matchEntities(
        wiki({
          "w-hell": ["Hell House"],
          "w-slaughter": ["The Slaughterhouse"],
        }),
        qt,
      ),
    );
    expect(found).toEqual({
      "w-hell": "qt-ride-11118",
      "w-slaughter": "qt-ride-11121",
      "w-jason": "qt-ride-17663",
      "w-quiet": "qt-ride-1",
      "w-final": "qt-ride-2",
      "w-museum": "qt-ride-3",
      "w-moon": "qt-ride-4",
      "w-circus": "qt-ride-5",
      "w-taron": "qt-ride-6",
      "w-starflyer": "qt-ride-7",
    });
  });

  it("leaves every other pair as it was without the curated names", () => {
    const before = pairs(service.matchEntities(wiki(), qt));
    const after = pairs(
      service.matchEntities(
        wiki({
          "w-hell": ["Hell House"],
          "w-slaughter": ["The Slaughterhouse"],
        }),
        qt,
      ),
    );
    expect({ ...after, "w-hell": undefined, "w-slaughter": undefined }).toEqual(
      { ...before, "w-hell": undefined, "w-slaughter": undefined },
    );
  });
});

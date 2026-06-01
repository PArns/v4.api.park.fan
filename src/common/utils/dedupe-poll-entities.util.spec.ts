import { dedupePollEntities } from "./dedupe-poll-entities.util";
import {
  EntityLiveData,
  EntityType,
  LiveStatus,
} from "../../external-apis/data-sources/interfaces/data-source.interface";

/**
 * dedupePollEntities must collapse ONLY a true within-poll duplicate
 * (same source + externalId + entityType) and must NOT touch anything else.
 * These tests hammer the edges so we're sure it never drops a real reading.
 */
describe("dedupePollEntities", () => {
  const ent = (o: Partial<EntityLiveData> = {}): EntityLiveData => ({
    externalId: "ext-1",
    source: "themeparks-wiki",
    entityType: EntityType.ATTRACTION,
    name: "Ride",
    status: LiveStatus.OPERATING,
    waitTime: 5,
    ...o,
  });

  describe("empty / trivial inputs", () => {
    it("returns [] for undefined", () => {
      expect(dedupePollEntities(undefined)).toEqual([]);
    });
    it("returns [] for null", () => {
      expect(dedupePollEntities(null)).toEqual([]);
    });
    it("returns [] for an empty array", () => {
      expect(dedupePollEntities([])).toEqual([]);
    });
    it("returns a single entity unchanged", () => {
      const e = ent();
      const out = dedupePollEntities([e]);
      expect(out).toHaveLength(1);
      expect(out[0]).toBe(e);
    });
  });

  describe("collapses ONLY true duplicates and keeps the LAST value", () => {
    it("dedups two entries with identical (source, externalId, entityType)", () => {
      const first = ent({ waitTime: 61, status: LiveStatus.OPERATING });
      const last = ent({ waitTime: 0, status: LiveStatus.CLOSED });
      const out = dedupePollEntities([first, last]);
      expect(out).toHaveLength(1);
      // the real Bobbejaanland case: phantom OPERATING/61 then CLOSED/0 → keep last
      expect(out[0]).toBe(last);
      expect(out[0].waitTime).toBe(0);
      expect(out[0].status).toBe(LiveStatus.CLOSED);
    });

    it("collapses 3+ identical entries to the last one", () => {
      const out = dedupePollEntities([
        ent({ waitTime: 60 }),
        ent({ waitTime: 61 }),
        ent({ waitTime: 7 }),
      ]);
      expect(out).toHaveLength(1);
      expect(out[0].waitTime).toBe(7);
    });
  });

  describe("must NOT dedup genuinely distinct readings", () => {
    it("keeps the same entity reported by DIFFERENT sources (multi-source coverage)", () => {
      const out = dedupePollEntities([
        ent({ source: "themeparks-wiki", waitTime: 20 }),
        ent({ source: "queue-times", waitTime: 25 }),
        ent({ source: "wartezeiten-app", waitTime: 22 }),
      ]);
      expect(out).toHaveLength(3);
      expect(out.map((e) => e.source).sort()).toEqual([
        "queue-times",
        "themeparks-wiki",
        "wartezeiten-app",
      ]);
    });

    it("keeps different entities of the same source (different externalId)", () => {
      const out = dedupePollEntities([
        ent({ externalId: "a" }),
        ent({ externalId: "b" }),
        ent({ externalId: "c" }),
      ]);
      expect(out).toHaveLength(3);
    });

    it("keeps an attraction and a show that share an externalId (different entityType)", () => {
      const out = dedupePollEntities([
        ent({ externalId: "shared", entityType: EntityType.ATTRACTION }),
        ent({ externalId: "shared", entityType: EntityType.SHOW }),
        ent({ externalId: "shared", entityType: EntityType.RESTAURANT }),
      ]);
      expect(out).toHaveLength(3);
      expect(out.map((e) => e.entityType).sort()).toEqual([
        EntityType.ATTRACTION,
        EntityType.RESTAURANT,
        EntityType.SHOW,
      ]);
    });

    it("does not treat differing waitTime/status/name/coords as a dedup key", () => {
      // Same key but wildly different payloads — still ONE survives (the last),
      // proving the key is ONLY source+externalId+entityType (intended), while...
      const sameKey = dedupePollEntities([
        ent({ waitTime: 5, latitude: 1, name: "A" }),
        ent({ waitTime: 999, latitude: 2, name: "B" }),
      ]);
      expect(sameKey).toHaveLength(1);
      // ...and a one-char externalId difference is enough to keep both.
      const diffKey = dedupePollEntities([
        ent({ externalId: "177854" }),
        ent({ externalId: "177855" }),
      ]);
      expect(diffKey).toHaveLength(2);
    });
  });

  describe("order + extreme volumes", () => {
    it("preserves first-seen order of distinct entities", () => {
      const out = dedupePollEntities([
        ent({ externalId: "z" }),
        ent({ externalId: "m" }),
        ent({ externalId: "a" }),
      ]);
      expect(out.map((e) => e.externalId)).toEqual(["z", "m", "a"]);
    });

    it("keeps all of 1000 DISTINCT entities (no false dedup)", () => {
      const many = Array.from({ length: 1000 }, (_, i) =>
        ent({ externalId: `ride-${i}` }),
      );
      expect(dedupePollEntities(many)).toHaveLength(1000);
    });

    it("collapses 1000 IDENTICAL entities to exactly one (the last)", () => {
      const many = Array.from({ length: 1000 }, (_, i) =>
        ent({ externalId: "dup", waitTime: i }),
      );
      const out = dedupePollEntities(many);
      expect(out).toHaveLength(1);
      expect(out[0].waitTime).toBe(999);
    });

    it("handles a realistic mixed park poll (29 distinct + 1 dup → 29)", () => {
      const distinct = Array.from({ length: 29 }, (_, i) =>
        ent({ externalId: `e${i}` }),
      );
      const withDup = [...distinct, ent({ externalId: "e7", waitTime: 60 })];
      const out = dedupePollEntities(withDup);
      expect(out).toHaveLength(29);
      // the duplicated key kept its first-seen POSITION but the LAST value
      expect(out.find((e) => e.externalId === "e7")!.waitTime).toBe(60);
    });
  });
});

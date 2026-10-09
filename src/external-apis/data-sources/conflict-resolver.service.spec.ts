import { Logger } from "@nestjs/common";
import { ConflictResolverService } from "./conflict-resolver.service";
import {
  EntityLiveData,
  EntityType,
  LiveDataResponse,
  LiveStatus,
  OperatingWindow,
} from "./interfaces/data-source.interface";

/**
 * The resolver decides which wait time a ride shows when several sources
 * report it. Everything goes through the public `aggregateParkData`; the
 * helpers behind it are private and only reachable that way.
 */
describe("ConflictResolverService", () => {
  let service: ConflictResolverService;

  const entity = (
    name: string,
    waitTime: number | undefined,
    overrides: Partial<EntityLiveData> = {},
  ): EntityLiveData => ({
    externalId: `id-${name}`,
    source: "test",
    entityType: EntityType.ATTRACTION,
    name,
    status: LiveStatus.OPERATING,
    waitTime,
    ...overrides,
  });

  const response = (
    source: string,
    entities: EntityLiveData[],
    overrides: Partial<LiveDataResponse> = {},
  ): LiveDataResponse => ({
    source,
    parkExternalId: `park-${source}`,
    entities,
    fetchedAt: new Date("2026-10-08T12:00:00Z"),
    ...overrides,
  });

  const sources = (entries: Record<string, LiveDataResponse>) =>
    new Map(Object.entries(entries));

  const contributors = (e: EntityLiveData) =>
    (e as EntityLiveData & { sources: string[] }).sources;

  const wait = (result: LiveDataResponse, name: string) =>
    result.entities.find((e) => e.name === name)?.waitTime;

  beforeEach(() => {
    service = new ConflictResolverService();
    jest.spyOn(Logger.prototype, "warn").mockImplementation();
    jest.spyOn(Logger.prototype, "debug").mockImplementation();
  });

  afterEach(() => jest.restoreAllMocks());

  describe("source selection", () => {
    it("throws when no source is available", () => {
      expect(() => service.aggregateParkData(new Map())).toThrow(
        "No data sources available",
      );
    });

    it("passes a single source through and rounds its wait time", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 22)]),
        }),
      );

      expect(result.source).toBe("multi-source");
      expect(result.parkExternalId).toBe("park-themeparks-wiki");
      expect(result.entities).toHaveLength(1);
      expect(contributors(result.entities[0])).toEqual(["themeparks-wiki"]);
      expect(wait(result, "Taron")).toBe(20);
    });

    it("takes the park id from the first source that has one", () => {
      const result = service.aggregateParkData(
        sources({
          "queue-times": response("queue-times", [], {
            parkExternalId: "qt-park",
          }),
          "wartezeiten-app": response("wartezeiten-app", [], {
            parkExternalId: "wz-park",
          }),
        }),
      );

      expect(result.parkExternalId).toBe("qt-park");
    });

    it("takes lands from Queue-Times only", () => {
      const lands = [{ externalId: "l1", name: "Land", rides: [] }];
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", []),
          "queue-times": response("queue-times", [], { lands } as never),
        }),
      );

      expect(result.lands).toBe(lands);
    });

    it("returns no lands without Queue-Times", () => {
      const result = service.aggregateParkData(
        sources({ "themeparks-wiki": response("themeparks-wiki", []) }),
      );

      expect(result.lands).toEqual([]);
    });

    it("keeps entities that only one secondary source knows", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 20)]),
          "queue-times": response("queue-times", [entity("Silver Star", 30)]),
        }),
      );

      expect(result.entities.map((e) => e.name).sort()).toEqual([
        "Silver Star",
        "Taron",
      ]);
      const star = result.entities.find((e) => e.name === "Silver Star")!;
      expect(star.source).toBe("queue-times");
      expect(contributors(star)).toEqual(["queue-times"]);
    });
  });

  describe("wait time consensus", () => {
    const merge = (wiki?: number, qt?: number, wz?: number) =>
      service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [
            entity("Taron", wiki),
          ]),
          "queue-times": response("queue-times", [entity("Taron", qt)]),
          "wartezeiten-app": response("wartezeiten-app", [entity("Taron", wz)]),
        }),
      );

    it("keeps the value when two sources report the same wait", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 25)]),
          "queue-times": response("queue-times", [entity("Taron", 25)]),
        }),
      );

      expect(wait(result, "Taron")).toBe(25);
      expect(contributors(result.entities[0])).toEqual([
        "themeparks-wiki",
        "queue-times",
      ]);
    });

    it("averages two different values", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 20)]),
          "queue-times": response("queue-times", [entity("Taron", 30)]),
        }),
      );

      expect(wait(result, "Taron")).toBe(25);
    });

    it("rounds the average to the nearest five minutes", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 20)]),
          "queue-times": response("queue-times", [entity("Taron", 25)]),
        }),
      );

      expect(wait(result, "Taron")).toBe(25);
    });

    it("ignores a source that has no wait time", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 40)]),
          "queue-times": response("queue-times", [entity("Taron", undefined)]),
        }),
      );

      expect(wait(result, "Taron")).toBe(40);
      expect(contributors(result.entities[0])).toContain("queue-times");
    });

    it("leaves the wait time undefined when no source has one", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [
            entity("Taron", undefined),
          ]),
          "queue-times": response("queue-times", [entity("Taron", undefined)]),
        }),
      );

      expect(wait(result, "Taron")).toBeUndefined();
    });

    it("uses the majority when the two lowest of three agree", () => {
      expect(wait(merge(25, 25, 60), "Taron")).toBe(25);
    });

    it("uses the majority when the two highest of three agree", () => {
      expect(wait(merge(5, 40, 40), "Taron")).toBe(40);
    });

    it("uses the median when all three differ", () => {
      expect(wait(merge(5, 10, 90), "Taron")).toBe(10);
    });

    it("never rounds Disney's 13-minute walk-on", () => {
      expect(wait(merge(13, 13, 13), "Taron")).toBe(13);
    });

    it("rounds a wait under 2.5 minutes to zero", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [entity("Taron", 2)]),
        }),
      );

      expect(wait(result, "Taron")).toBe(0);
    });

    it("does not leak the temporary merge fields", () => {
      const result = merge(10, 15, 20);
      const keys = Object.keys(result.entities[0]);

      expect(keys).not.toContain("qtWaitTime");
      expect(keys).not.toContain("wzWaitTime");
      expect(keys).not.toContain("qtStatus");
      expect(keys).not.toContain("wzStatus");
    });
  });

  describe("name matching", () => {
    const mergeNames = (wikiName: string, otherName: string) =>
      service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [
            entity(wikiName, 20),
          ]),
          "queue-times": response("queue-times", [entity(otherName, 30)]),
        }),
      );

    it("matches names that differ in case", () => {
      const result = mergeNames("Silver Star", "SILVER STAR");

      expect(result.entities).toHaveLength(1);
      expect(result.entities[0].name).toBe("Silver Star");
      expect(result.entities[0].waitTime).toBe(25);
    });

    it("matches names that differ in accents", () => {
      const result = mergeNames(
        "Astérix Tonnerre de Zeus",
        "Asterix Tonnerre de Zeus",
      );

      expect(result.entities).toHaveLength(1);
    });

    it("matches names that differ in umlauts", () => {
      const result = mergeNames(
        "Blue Fire Megacoaster",
        "Blue Fire Megacoaster",
      );
      const umlaut = mergeNames("Fjörd Rafting", "Fjord Rafting");

      expect(result.entities).toHaveLength(1);
      expect(umlaut.entities).toHaveLength(1);
    });

    it("matches names that differ in an apostrophe", () => {
      const result = mergeNames("Joe's Ride", "Joes Ride");

      expect(result.entities).toHaveLength(1);
      expect(contributors(result.entities[0])).toEqual([
        "themeparks-wiki",
        "queue-times",
      ]);
    });

    it("matches a long name that differs by one character", () => {
      const result = mergeNames(
        "Voletarium Flugsimulator",
        "Voletarium Flugsimulatr",
      );

      expect(result.entities).toHaveLength(1);
    });

    it("keeps clearly different names apart", () => {
      const result = mergeNames("Silver Star", "Blue Fire");

      expect(result.entities).toHaveLength(2);
    });

    it("does not fuzzy-match short names", () => {
      const result = mergeNames("Atlas", "Atlaz");

      expect(result.entities).toHaveLength(2);
    });
  });

  describe("status override", () => {
    const withStatus = (
      wikiStatus: LiveStatus,
      qtStatus: LiveStatus,
      qtWait: number | undefined,
    ) =>
      service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [
            entity("Taron", undefined, { status: wikiStatus }),
          ]),
          "queue-times": response("queue-times", [
            entity("Taron", qtWait, { status: qtStatus }),
          ]),
        }),
      ).entities[0];

    it("overrides CLOSED when a second source operates with a real queue", () => {
      const merged = withStatus(LiveStatus.CLOSED, LiveStatus.OPERATING, 20);

      expect(merged.status).toBe(LiveStatus.OPERATING);
      expect(merged.rawStatus).toBe(LiveStatus.CLOSED);
    });

    it("overrides DOWN the same way and keeps DOWN as raw status", () => {
      const merged = withStatus(LiveStatus.DOWN, LiveStatus.OPERATING, 10);

      expect(merged.status).toBe(LiveStatus.OPERATING);
      expect(merged.rawStatus).toBe(LiveStatus.DOWN);
    });

    it("does not override when the queue is under five minutes", () => {
      const merged = withStatus(LiveStatus.CLOSED, LiveStatus.OPERATING, 4);

      expect(merged.status).toBe(LiveStatus.CLOSED);
      expect(merged.rawStatus).toBeUndefined();
    });

    it("does not override when the other source is not operating", () => {
      const merged = withStatus(LiveStatus.CLOSED, LiveStatus.DOWN, 30);

      expect(merged.status).toBe(LiveStatus.CLOSED);
      expect(merged.rawStatus).toBeUndefined();
    });

    it("does not override when the other source has no wait time", () => {
      const merged = withStatus(
        LiveStatus.CLOSED,
        LiveStatus.OPERATING,
        undefined,
      );

      expect(merged.status).toBe(LiveStatus.CLOSED);
    });

    it("leaves an operating base entity alone", () => {
      const merged = withStatus(LiveStatus.OPERATING, LiveStatus.OPERATING, 20);

      expect(merged.status).toBe(LiveStatus.OPERATING);
      expect(merged.rawStatus).toBeUndefined();
    });

    it("overrides from Wartezeiten as well", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [
            entity("Taron", undefined, { status: LiveStatus.CLOSED }),
          ]),
          "wartezeiten-app": response("wartezeiten-app", [
            entity("Taron", 15, { status: LiveStatus.OPERATING }),
          ]),
        }),
      );

      expect(result.entities[0].status).toBe(LiveStatus.OPERATING);
      expect(result.entities[0].rawStatus).toBe(LiveStatus.CLOSED);
    });
  });

  describe("operating hours", () => {
    const window = (open: string, close: string): OperatingWindow => ({
      open,
      close,
      type: "OPERATING",
    });
    const wikiHours = [window("2026-10-08T09:00:00Z", "2026-10-08T18:00:00Z")];
    const wzHours = [window("2026-10-08T10:00:00Z", "2026-10-08T19:00:00Z")];

    it("prefers Wiki hours when both sources have them", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [], {
            operatingHours: wikiHours,
          }),
          "wartezeiten-app": response("wartezeiten-app", [], {
            operatingHours: wzHours,
          }),
        }),
      );

      expect(result.operatingHours).toBe(wikiHours);
    });

    it("warns when the two sources disagree", () => {
      service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [], {
            operatingHours: wikiHours,
          }),
          "wartezeiten-app": response("wartezeiten-app", [], {
            operatingHours: wzHours,
          }),
        }),
      );

      expect(Logger.prototype.warn).toHaveBeenCalledWith(
        expect.stringContaining("Operating Hours Discrepancy"),
      );
    });

    it("does not warn when the two sources agree", () => {
      service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [], {
            operatingHours: wikiHours,
          }),
          "wartezeiten-app": response("wartezeiten-app", [], {
            operatingHours: [...wikiHours],
          }),
        }),
      );

      expect(Logger.prototype.warn).not.toHaveBeenCalled();
    });

    it("falls back to Wartezeiten when Wiki has none", () => {
      const result = service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [], {
            operatingHours: [],
          }),
          "wartezeiten-app": response("wartezeiten-app", [], {
            operatingHours: wzHours,
          }),
        }),
      );

      expect(result.operatingHours).toBe(wzHours);
    });

    it("returns undefined when no source has hours", () => {
      const result = service.aggregateParkData(
        sources({ "themeparks-wiki": response("themeparks-wiki", []) }),
      );

      expect(result.operatingHours).toBeUndefined();
    });
  });

  describe("wait time discrepancy warning", () => {
    const at = (iso: string) => ({ lastUpdated: iso });

    const aggregate = (wiki: EntityLiveData, qt: EntityLiveData): void => {
      service.aggregateParkData(
        sources({
          "themeparks-wiki": response("themeparks-wiki", [wiki]),
          "queue-times": response("queue-times", [qt]),
        }),
      );
    };

    it("warns when fresh values differ by more than 15 minutes", () => {
      aggregate(
        entity("Taron", 10, at("2026-10-08T12:00:00Z")),
        entity("Taron", 40, at("2026-10-08T12:01:00Z")),
      );

      expect(Logger.prototype.warn).toHaveBeenCalledWith(
        expect.stringContaining("diff: 30min"),
      );
    });

    it("stays quiet at exactly 15 minutes of difference", () => {
      aggregate(
        entity("Taron", 10, at("2026-10-08T12:00:00Z")),
        entity("Taron", 25, at("2026-10-08T12:00:00Z")),
      );

      expect(Logger.prototype.warn).not.toHaveBeenCalled();
    });

    it("stays quiet when the timestamps are more than 10 minutes apart", () => {
      aggregate(
        entity("Taron", 10, at("2026-10-08T12:00:00Z")),
        entity("Taron", 60, at("2026-10-08T12:11:00Z")),
      );

      expect(Logger.prototype.warn).not.toHaveBeenCalled();
    });

    it("falls back to the response time when an entity has no timestamp", () => {
      aggregate(entity("Taron", 10), entity("Taron", 60));

      expect(Logger.prototype.warn).toHaveBeenCalledWith(
        expect.stringContaining('"Taron"'),
      );
    });

    it("does not compare without a Wiki response", () => {
      service.aggregateParkData(
        sources({
          "queue-times": response("queue-times", [entity("Taron", 10)]),
          "wartezeiten-app": response("wartezeiten-app", [entity("Taron", 60)]),
        }),
      );

      expect(Logger.prototype.warn).not.toHaveBeenCalled();
    });
  });
});

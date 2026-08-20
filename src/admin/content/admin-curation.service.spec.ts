import { BadRequestException, NotFoundException } from "@nestjs/common";
import { AdminCurationService } from "./admin-curation.service";
import { invalidateParkCaches } from "../../common/cache/park-cache-invalidation";
import type { AdminPrincipal } from "../auth/admin-principal";

// ts-jest hoists this above the imports, so the binding above is the mock.
jest.mock("../../common/cache/park-cache-invalidation", () => ({
  invalidateParkCaches: jest.fn(async () => undefined),
}));

const evictMock = invalidateParkCaches as jest.MockedFunction<
  typeof invalidateParkCaches
>;

const ACTOR: AdminPrincipal = {
  userId: "user-1",
  email: "you@park.fan",
  displayName: "You",
  role: "editor",
  sessionToken: "t",
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
};

function build(
  attraction: Record<string, unknown> | null,
  park: Record<string, unknown> | null = null,
) {
  const calls: string[] = [];

  const attractions = {
    findOne: jest.fn(async (): Promise<unknown> => attraction),
    find: jest.fn(async (): Promise<unknown[]> => []),
    save: jest.fn(async (entity: unknown) => {
      calls.push("save");
      return entity;
    }),
  };
  const parks = {
    findOne: jest.fn(async (): Promise<unknown> => park),
    save: jest.fn(async (entity: unknown) => {
      calls.push("save");
      return entity;
    }),
  };
  const revalidation = {
    revalidateTags: jest.fn(async () => {
      calls.push("revalidate");
      return true;
    }),
  };
  const audit = {
    record: jest.fn(async () => ({ id: "audit-1" })),
    findOne: jest.fn(async (): Promise<unknown> => null),
    markReverted: jest.fn(async () => undefined),
  };
  const queue = {
    add: jest.fn(async (..._args: unknown[]) => {
      calls.push("delayed-sweep");
      return {};
    }),
  };

  evictMock.mockImplementation(async () => {
    calls.push("evict");
  });

  const service = new AdminCurationService(
    attractions as never,
    parks as never,
    {} as never,
    revalidation as never,
    audit as never,
    queue as never,
  );

  return { service, attractions, parks, revalidation, audit, queue, calls };
}

function anAttraction(overrides: Record<string, unknown> = {}) {
  return {
    id: "ride-1",
    parkId: "park-1",
    name: "Winni Splash",
    minimumHeight: 100,
    curatedMinimumHeight: null,
    curatedName: null,
    curatedSeasonMonths: null,
    curatedIsSeasonal: null,
    mayGetWet: null,
    curatedMayGetWet: null,
    hasSingleRider: null,
    openWithPark: false,
    rcdbId: null,
    ...overrides,
  };
}

describe("AdminCurationService", () => {
  beforeEach(() => jest.clearAllMocks());

  it("writes only the curated column, never the synced one", async () => {
    const attraction = anAttraction();
    const { service, attractions } = build(attraction);

    await service.curateAttraction(
      "ride-1",
      { fields: { curatedMinimumHeight: 0 }, reason: "Park's own conditions" },
      ACTOR,
    );

    const saved = attractions.save.mock.calls[0][0] as Record<string, unknown>;
    expect(saved.curatedMinimumHeight).toBe(0);
    // Untouched — the next children-metadata run owns this cell and would
    // overwrite anything written into it.
    expect(saved.minimumHeight).toBe(100);
  });

  it("refuses a field that is not curatable", async () => {
    const { service } = build(anAttraction());
    await expect(
      service.curateAttraction(
        "ride-1",
        { fields: { minimumHeight: 120 } },
        ACTOR,
      ),
    ).rejects.toThrow(/not a curatable field/);
  });

  it("does nothing at all when the patch changes nothing", async () => {
    // A form that PATCHes every field on every blur must not fill the audit
    // log with empty edits or evict a cache per keystroke.
    const attraction = anAttraction({ curatedName: "TARON" });
    const { service, attractions, audit, revalidation } = build(attraction);

    const result = await service.curateAttraction(
      "ride-1",
      { fields: { curatedName: "TARON" } },
      ACTOR,
    );

    expect(result.changed).toEqual([]);
    expect(attractions.save).not.toHaveBeenCalled();
    expect(audit.record).not.toHaveBeenCalled();
    expect(revalidation.revalidateTags).not.toHaveBeenCalled();
  });

  it("publishes in the only order that works", async () => {
    // Revalidating before evicting does not publish the write: the frontend
    // refetches the pre-write payload, still warm in Redis and at the edge,
    // and pins it in its own data cache for 24 hours.
    const { service, calls } = build(anAttraction());
    await service.curateAttraction(
      "ride-1",
      { fields: { curatedName: "X" } },
      ACTOR,
    );
    expect(calls).toEqual(["save", "evict", "revalidate", "delayed-sweep"]);
  });

  it("schedules one delayed sweep, not one per edit", async () => {
    const { service, queue } = build(anAttraction());
    await service.curateAttraction(
      "ride-1",
      { fields: { curatedName: "X" } },
      ACTOR,
    );
    const options = queue.add.mock.calls[0][2] as unknown as Record<
      string,
      unknown
    >;
    expect(options.jobId).toBe("revalidate-parks-after-cdn");
    expect(options.delay).toBe(16 * 60 * 1000);
  });

  it("records the reason and the source on the audit row", async () => {
    const { service, audit } = build(anAttraction());
    await service.curateAttraction(
      "ride-1",
      {
        fields: { curatedMinimumHeight: 0 },
        reason:
          "Nutzungsbedingungen say under 1.00 m may play when accompanied",
        sourceUrl: "https://www.phantasialand.de/nutzungsbedingungen",
      },
      ACTOR,
    );
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "attraction.curate",
        entityType: "attraction",
        before: { curatedMinimumHeight: null },
        after: { curatedMinimumHeight: 0 },
        reason: expect.stringContaining("Nutzungsbedingungen"),
        sourceUrl: "https://www.phantasialand.de/nutzungsbedingungen",
      }),
    );
  });

  it("does not fail a curation because a cache could not be evicted", async () => {
    // Also the SKIP_REDIS path, where the injected client has no `del`. The
    // write happened; the change surfaces on a TTL instead of immediately.
    const { service } = build(anAttraction());
    (invalidateParkCaches as jest.Mock).mockRejectedValueOnce(
      new Error("redis down"),
    );
    await expect(
      service.curateAttraction(
        "ride-1",
        { fields: { curatedName: "X" } },
        ACTOR,
      ),
    ).resolves.toMatchObject({ changed: ["curatedName"] });
  });

  it("404s for a ride that does not exist", async () => {
    const { service } = build(null);
    await expect(
      service.curateAttraction("nope", { fields: {} }, ACTOR),
    ).rejects.toBeInstanceOf(NotFoundException);
  });

  describe("coercion", () => {
    it("clears a correction when the text field is emptied", async () => {
      const attraction = anAttraction({ curatedName: "TARON" });
      const { service, attractions } = build(attraction);
      await service.curateAttraction(
        "ride-1",
        { fields: { curatedName: "  " } },
        ACTOR,
      );
      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedName).toBeNull();
    });

    it("keeps a curated 0 as a real value", async () => {
      // 0 means "no minimum at all". A form treating it as empty destroys the
      // distinction this column exists for.
      const { service, attractions } = build(anAttraction());
      await service.curateAttraction(
        "ride-1",
        { fields: { curatedMinimumHeight: 0 } },
        ACTOR,
      );
      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedMinimumHeight).toBe(0);
    });

    it("sorts and dedupes months", async () => {
      const { service, attractions } = build(anAttraction());
      await service.curateAttraction(
        "ride-1",
        { fields: { curatedSeasonMonths: [7, 4, 7, 5] } },
        ACTOR,
      );
      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedSeasonMonths).toEqual([4, 5, 7]);
    });

    it("reads an empty month list as 'no correction'", async () => {
      // "Operates in no month at all" is what retirement is for.
      const attraction = anAttraction({ curatedSeasonMonths: [4, 5] });
      const { service, attractions } = build(attraction);
      await service.curateAttraction(
        "ride-1",
        { fields: { curatedSeasonMonths: [] } },
        ACTOR,
      );
      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedSeasonMonths).toBeNull();
    });

    it("rejects a month outside 1–12", async () => {
      const { service } = build(anAttraction());
      await expect(
        service.curateAttraction(
          "ride-1",
          { fields: { curatedSeasonMonths: [0, 13] } },
          ACTOR,
        ),
      ).rejects.toBeInstanceOf(BadRequestException);
    });

    it("rejects a height outside the plausible range", async () => {
      const { service } = build(anAttraction());
      await expect(
        service.curateAttraction(
          "ride-1",
          { fields: { curatedMinimumHeight: 900 } },
          ACTOR,
        ),
      ).rejects.toThrow(/at most 250/);
    });

    it("rejects a non-boolean for a boolean field", async () => {
      const { service } = build(anAttraction());
      await expect(
        service.curateAttraction(
          "ride-1",
          { fields: { curatedMayGetWet: "yes" } },
          ACTOR,
        ),
      ).rejects.toThrow(/must be true or false/);
    });

    it("rejects an enum value outside the options", async () => {
      const park = {
        id: "park-1",
        name: "Phantasialand",
        curatedParkType: null,
      };
      const { service } = build(null, park);
      await expect(
        service.curatePark(
          "park-1",
          { fields: { curatedParkType: "FUN_PARK" } },
          ACTOR,
        ),
      ).rejects.toThrow(/must be one of/);
    });
  });

  describe("clearing a field", () => {
    it("writes the column default, not null, for a NOT NULL column", async () => {
      // `open_with_park` is `boolean NOT NULL DEFAULT false`. Writing null
      // there is an UPDATE Postgres rejects, and the editor offered to clear it
      // because a stored `false` looked like a correction.
      const attraction = anAttraction({ openWithPark: true });
      const { service, attractions } = build(attraction);

      await service.curateAttraction(
        "ride-1",
        { fields: { openWithPark: null } },
        ACTOR,
      );

      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.openWithPark).toBe(false);
    });

    it("writes null for a nullable column", async () => {
      const attraction = anAttraction({ curatedName: "TARON" });
      const { service, attractions } = build(attraction);

      await service.curateAttraction(
        "ride-1",
        { fields: { curatedName: null } },
        ACTOR,
      );

      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedName).toBeNull();
    });

    it("reads an emptied numeric input as 'no correction', not as zero", async () => {
      // A client that serialises an emptied field as "" — curl, a script —
      // means to withdraw the correction. Stored as 0 it becomes the opposite:
      // a positive claim that the ride has no minimum height.
      const attraction = anAttraction({ curatedMinimumHeight: 120 });
      const { service, attractions } = build(attraction);

      await service.curateAttraction(
        "ride-1",
        { fields: { curatedMinimumHeight: "" } },
        ACTOR,
      );

      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedMinimumHeight).toBeNull();
    });
  });

  describe("rcdbId", () => {
    it("refuses an id that already belongs to another ride", async () => {
      // One id must never sit on two attractions: the ride page would link to
      // a different ride, and the Wikidata import joins on it and would hand
      // both rows the same measurements.
      const { service, attractions } = build(anAttraction());
      attractions.findOne = jest.fn(async (options: unknown) => {
        const where =
          (options as { where?: Record<string, unknown> }).where ?? {};
        // The clash lookup asks by rcdbId; the entity lookup asks by id.
        if ("rcdbId" in where) {
          return {
            id: "ride-2",
            name: "Black Mamba",
            park: { name: "Phantasialand" },
          };
        }
        return anAttraction();
      }) as never;

      await expect(
        service.curateAttraction("ride-1", { fields: { rcdbId: 4489 } }, ACTOR),
      ).rejects.toThrow(/already belongs to "Black Mamba"/);
    });

    it("accepts an id nothing else holds", async () => {
      const { service, attractions } = build(anAttraction());
      const original = attractions.findOne;
      attractions.findOne = jest.fn(async (options: unknown) => {
        const where =
          (options as { where?: Record<string, unknown> }).where ?? {};
        if ("rcdbId" in where) return null;
        return (original as unknown as () => Promise<unknown>)();
      }) as never;

      await expect(
        service.curateAttraction("ride-1", { fields: { rcdbId: 4489 } }, ACTOR),
      ).resolves.toMatchObject({ changed: ["rcdbId"] });
    });
  });

  describe("undo", () => {
    it("puts the previous value back and marks the original reverted", async () => {
      const attraction = anAttraction({ curatedName: "TARON" });
      const { service, attractions, audit } = build(attraction);
      audit.findOne = jest.fn(async (): Promise<unknown> => ({
        id: "audit-0",
        entityType: "attraction",
        entityId: "ride-1",
        action: "attraction.curate",
        before: { curatedName: null },
        after: { curatedName: "TARON" },
        revertedBy: null,
        createdAt: new Date("2026-08-20T09:00:00Z"),
      }));

      await service.revert("audit-0", ACTOR);

      const saved = attractions.save.mock.calls[0][0] as Record<
        string,
        unknown
      >;
      expect(saved.curatedName).toBeNull();
      expect(audit.markReverted).toHaveBeenCalledWith("audit-0", "audit-1");
    });

    it("refuses to undo a change something else has already changed again", async () => {
      // Editor A sets a height null→120, editor B corrects it 120→140. Undoing
      // A's entry without this check writes null and silently discards B's
      // work, leaving B's entry standing in the log as though it were current.
      const attraction = anAttraction({ curatedMinimumHeight: 140 });
      const { service, audit, attractions } = build(attraction);
      audit.findOne = jest.fn(async (): Promise<unknown> => ({
        id: "audit-A",
        entityType: "attraction",
        entityId: "ride-1",
        action: "attraction.curate",
        before: { curatedMinimumHeight: null },
        after: { curatedMinimumHeight: 120 },
        revertedBy: null,
        createdAt: new Date("2026-08-20T10:00:00Z"),
      }));

      await expect(service.revert("audit-A", ACTOR)).rejects.toThrow(
        /overwritten since/,
      );
      expect(attractions.save).not.toHaveBeenCalled();
    });

    it("refuses to undo the same change twice", async () => {
      const { service, audit } = build(anAttraction());
      audit.findOne = jest.fn(async (): Promise<unknown> => ({
        id: "audit-0",
        entityType: "attraction",
        entityId: "ride-1",
        before: { curatedName: null },
        revertedBy: "audit-1",
        createdAt: new Date(),
      }));
      await expect(service.revert("audit-0", ACTOR)).rejects.toThrow(
        /already been undone/,
      );
    });

    it("refuses to undo something that is not a curation", async () => {
      const { service, audit } = build(anAttraction());
      audit.findOne = jest.fn(async (): Promise<unknown> => ({
        id: "audit-0",
        entityType: "system",
        entityId: null,
        action: "job.flush-cache",
        before: { scope: "all" },
        revertedBy: null,
        createdAt: new Date(),
      }));
      await expect(service.revert("audit-0", ACTOR)).rejects.toThrow(
        /only available for curation entries/,
      );
    });
  });
});

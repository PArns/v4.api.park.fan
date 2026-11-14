import { readdirSync, readFileSync } from "fs";
import { join } from "path";
import { z } from "zod";
import { RedisMock } from "../../../test/mocks/redis.mock";
import { McpServerFactory } from "../mcp-server.factory";
import type { AnyMcpTool } from "../mcp-tool";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import type { ParkSeason } from "../../parks/entities/park-season.entity";
import { findRegion } from "../../trip-regions/regions";
import { listTripSeasonsTool } from "./list-trip-seasons.tool";
import { createTripSeasonTool } from "./create-trip-season.tool";
import { updateTripSeasonTool } from "./update-trip-season.tool";
import { deleteTripSeasonTool } from "./delete-trip-season.tool";
import { importTripSeasonsTool } from "./import-trip-seasons.tool";

const TDL = "11111111-1111-4111-8111-111111111111";
const SEASON = "22222222-2222-4222-8222-222222222222";
const tdl = findRegion("japan")!.parks.find(
  (p) => p.slug === "tokyo-disneyland",
)!;

/** An editor's token with `admin:write`: the role the writes need. */
const WRITER: AdminPrincipal = {
  userId: "33333333-3333-4333-8333-333333333333",
  email: "editor@park.fan",
  displayName: "Editor",
  role: "editor",
  sessionToken: null,
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
  apiToken: {
    id: "t",
    kind: "oauth",
    clientName: "Claude",
    scope: "admin:write",
  },
};
/** An owner's read-only token: capped at viewer. */
const READER: AdminPrincipal = {
  ...WRITER,
  email: "owner@park.fan",
  role: "viewer",
  apiToken: {
    id: "r",
    kind: "oauth",
    clientName: "Claude",
    scope: "admin:read",
  },
};

const halloween = (patch: Partial<ParkSeason> = {}): ParkSeason =>
  ({
    id: SEASON,
    parkId: TDL,
    kind: "halloween",
    name: "Disney Halloween",
    startDate: "2026-09-15",
    endDate: "2026-10-31",
    dates: null,
    status: "confirmed",
    separateTicket: false,
    priceFrom: null,
    priceCurrency: null,
    opensAt: null,
    closesAt: null,
    attractionIds: null,
    url: null,
    sourceUrl: "https://www.tokyodisneyresort.jp/en/",
    confirmedAt: null,
    note: null,
    updatedBy: null,
    createdAt: new Date("2026-08-01T00:00:00Z"),
    updatedAt: new Date("2026-08-01T00:00:00Z"),
    ...patch,
  }) as ParkSeason;

/** The real season services over in-memory rows: one park, Tokyo Disneyland. */
function harness(onFile: ParkSeason[] = []) {
  const rows = new Map(onFile.map((s) => [s.id, { ...s }]));
  let next = 0;
  const seasonRepo = {
    create: (row: object) => ({ ...row }),
    save: jest.fn(async (row: ParkSeason) => {
      const saved = {
        ...row,
        id: row.id ?? `44444444-4444-4444-8444-00000000000${++next}`,
      } as ParkSeason;
      rows.set(saved.id, saved);
      return saved;
    }),
    findOne: jest.fn(async ({ where }: { where: { id: string } }) => {
      const row = rows.get(where.id);
      return row ? { ...row } : null;
    }),
    delete: jest.fn(async ({ id }: { id: string }) => rows.delete(id)),
  };
  const parks = {
    findOne: jest.fn(async ({ where }: { where: Record<string, string> }) =>
      where.id === TDL ||
      (where.slug === tdl.slug && where.citySlug === tdl.city)
        ? { id: TDL, slug: tdl.slug, timezone: "Asia/Tokyo" }
        : null,
    ),
    find: jest.fn(async () => []),
  };
  const seasons = new ParkSeasonService(
    seasonRepo as never,
    parks as never,
    { find: jest.fn(async () => []) } as never,
  );
  jest
    .spyOn(seasons, "forParksInRange")
    .mockImplementation(async () => [...rows.values()]);
  jest.spyOn(seasons, "list").mockImplementation(async (query = {}) => ({
    total: rows.size,
    seasons: [...rows.values()].filter(
      (s) => !query.parkIds || query.parkIds.includes(s.parkId),
    ),
  }));
  const curation = {
    publish: jest.fn(async () => undefined),
    publishParks: jest.fn(async () => undefined),
  };
  const audit = { record: jest.fn(async () => null) };
  const regions = {
    region: (slug: string) => findRegion(slug),
    resolveParks: jest.fn(async () => [{ park: { id: TDL } }]),
  };
  const service = new AdminSeasonService(
    seasons,
    curation as never,
    audit as never,
    regions as never,
    parks as never,
    new RedisMock() as never,
  );
  const factory = new McpServerFactory({
    get: () => service,
  } as never);
  const call = async (tool: AnyMcpTool, input: object, who = WRITER) => {
    const parsed = z.object(tool.input).parse(input);
    const result = await factory.call(tool, parsed, who);
    const text = (result.content[0] as { text: string }).text;
    return {
      isError: result.isError === true,
      text,
      body: result.isError ? null : JSON.parse(text),
    };
  };
  return { call, rows, seasonRepo, curation, audit, regions };
}

describe("import_trip_seasons", () => {
  const fresh = {
    parkSlug: "tokyo-disneyland",
    kind: "christmas",
    name: "Disney Christmas",
    startDate: "2026-11-08",
    endDate: "2026-12-25",
    status: "confirmed",
    separateTicket: false,
  };
  const rows = [
    fresh,
    {
      ...fresh,
      kind: "halloween",
      name: "disney halloween ",
      startDate: "2026-09-20",
      endDate: "2026-10-31",
    },
    { ...fresh, kind: "closure", name: "Refurbishment", endDate: null },
    { ...fresh, parkSlug: "disneyland-park" },
    { ...fresh, priceFrom: 9000 },
    fresh,
  ];

  it("reports per row in a dry run and writes nothing", async () => {
    const h = harness([halloween()]);
    const { body } = await h.call(importTripSeasonsTool, { rows });
    expect(body.dryRun).toBe(true);
    expect(body.rows.map((r: { outcome: string }) => r.outcome)).toEqual([
      "create",
      "exists",
      "invalid",
      "invalid",
      "invalid",
      "exists",
    ]);
    expect(body.rows[1].seasonId).toBe(SEASON);
    expect(body.rows[2].reason).toMatch(/no endDate/);
    expect(body.rows[3].reason).toMatch(/southern-california and paris/);
    expect(body.rows[4].reason).toBe("priceFrom needs a priceCurrency");
    expect(body.rows[5].reason).toBe("repeats row #1 of this import");
    expect(body.counts).toEqual({
      create: 1,
      exists: 2,
      invalid: 3,
      failed: 0,
    });
    expect(h.seasonRepo.save).not.toHaveBeenCalled();
    expect(h.audit.record).not.toHaveBeenCalled();
    expect(h.curation.publishParks).toHaveBeenCalledWith([]);
  });

  it("writes the new rows with an audit row each and publishes the park once", async () => {
    const h = harness([halloween()]);
    const { body } = await h.call(importTripSeasonsTool, {
      rows: [fresh, { ...fresh, kind: "special_event", name: "New Year" }],
      region: "japan",
      dryRun: false,
    });
    expect(body.rows.map((r: { outcome: string }) => r.outcome)).toEqual([
      "created",
      "created",
    ]);
    expect(h.rows.get(body.rows[0].seasonId)).toMatchObject({
      parkId: TDL,
      kind: "christmas",
      updatedBy: WRITER.userId,
    });
    expect(h.audit.record).toHaveBeenCalledTimes(2);
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({ actor: WRITER, action: "park.season.create" }),
    );
    expect(h.curation.publish).not.toHaveBeenCalled();
    expect(h.curation.publishParks).toHaveBeenCalledWith([TDL]);
  });

  it("is refused to a read-only token, which is not even shown it", async () => {
    const h = harness();
    const { isError, text } = await h.call(
      importTripSeasonsTool,
      { rows: [fresh] },
      READER,
    );
    expect(isError).toBe(true);
    expect(text).toMatch(
      /needs a token with the admin:write scope; this one has admin:read/,
    );
    expect(h.seasonRepo.save).not.toHaveBeenCalled();
    const factory = new McpServerFactory({ get: () => null } as never);
    expect(factory.visibleTools(READER).map((t) => t.name)).not.toContain(
      "import_trip_seasons",
    );
  });

  it("refuses an empty, oversized or shapeless call before any row", () => {
    const schema = z.object(importTripSeasonsTool.input);
    expect(schema.safeParse({ rows: [] }).success).toBe(false);
    expect(schema.safeParse({ rows: Array(101).fill(fresh) }).success).toBe(
      false,
    );
    expect(schema.safeParse({ rows: [{ kind: "halloween" }] }).success).toBe(
      false,
    );
  });

  it("takes every researched file as it is", () => {
    const dir = join(__dirname, "../../../scripts/trip-regions/seasons");
    const schema = z.object(importTripSeasonsTool.input);
    for (const file of readdirSync(dir).filter((f) => f.endsWith(".json"))) {
      const rows = JSON.parse(readFileSync(join(dir, file), "utf8"));
      const parsed = schema.safeParse({ rows });
      expect([file, parsed.error?.issues ?? []]).toEqual([file, []]);
    }
  });
});

describe("create_trip_season", () => {
  const season = {
    park: TDL,
    kind: "christmas",
    name: "Disney Christmas",
    startDate: "2026-11-08",
    endDate: "2026-12-25",
  };

  it("writes, audits and publishes the park", async () => {
    const h = harness();
    const { body } = await h.call(createTripSeasonTool, season);
    expect(body.created).toMatchObject({
      park: { id: TDL },
      kind: "christmas",
      status: "announced",
    });
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({ actor: WRITER, action: "park.season.create" }),
    );
    expect(h.curation.publish).toHaveBeenCalledWith(TDL, []);
  });

  it("answers the admin form's refusal", async () => {
    const h = harness();
    const { isError, text } = await h.call(createTripSeasonTool, {
      ...season,
      endDate: "2026-11-01",
    });
    expect(isError).toBe(true);
    expect(text).toBe("endDate must not be before startDate");
    expect(
      z
        .object(createTripSeasonTool.input)
        .safeParse({ ...season, kind: "party" }).success,
    ).toBe(false);
    expect(h.seasonRepo.save).not.toHaveBeenCalled();
  });

  it("is refused to a read-only token", async () => {
    const h = harness();
    expect((await h.call(createTripSeasonTool, season, READER)).isError).toBe(
      true,
    );
    expect(h.seasonRepo.save).not.toHaveBeenCalled();
  });
});

describe("update_trip_season", () => {
  it("changes the fields given and audits before and after", async () => {
    const h = harness([halloween()]);
    const { body } = await h.call(updateTripSeasonTool, {
      id: SEASON,
      endDate: "2026-11-01",
      name: null,
    });
    expect(body.updated).toMatchObject({
      endDate: "2026-11-01",
      name: null,
      kind: "halloween",
    });
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "park.season.update",
        before: expect.objectContaining({ endDate: "2026-10-31" }),
        after: expect.objectContaining({ endDate: "2026-11-01" }),
      }),
    );
    expect(h.curation.publish).toHaveBeenCalledWith(TDL, []);
  });

  it("refuses an empty change, an unknown season and a date outside the range", async () => {
    const h = harness([halloween()]);
    expect((await h.call(updateTripSeasonTool, { id: SEASON })).text).toBe(
      "Name at least one field to change",
    );
    expect(
      (await h.call(updateTripSeasonTool, { id: TDL, status: "cancelled" }))
        .text,
    ).toBe(`No season ${TDL}`);
    expect(
      (
        await h.call(updateTripSeasonTool, {
          id: SEASON,
          dates: ["2026-12-01"],
        })
      ).text,
    ).toMatch(/outside the season's range/);
    expect(h.seasonRepo.save).not.toHaveBeenCalled();
  });

  it("is refused to a read-only token", async () => {
    const h = harness([halloween()]);
    expect(
      (
        await h.call(
          updateTripSeasonTool,
          { id: SEASON, status: "cancelled" },
          READER,
        )
      ).isError,
    ).toBe(true);
  });
});

describe("delete_trip_season", () => {
  it("needs confirm", () => {
    const schema = z.object(deleteTripSeasonTool.input);
    expect(schema.safeParse({ id: SEASON }).success).toBe(false);
    expect(schema.safeParse({ id: SEASON, confirm: false }).success).toBe(
      false,
    );
  });

  it("deletes and keeps the whole row in the audit log", async () => {
    const h = harness([halloween()]);
    const { body } = await h.call(deleteTripSeasonTool, {
      id: SEASON,
      confirm: true,
    });
    expect(body.deleted.id).toBe(SEASON);
    expect(h.rows.has(SEASON)).toBe(false);
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "park.season.delete",
        before: expect.objectContaining({
          name: "Disney Halloween",
          sourceUrl: halloween().sourceUrl,
        }),
      }),
    );
    expect(h.curation.publish).toHaveBeenCalledWith(TDL, []);
  });

  it("is refused to a read-only token", async () => {
    const h = harness([halloween()]);
    expect(
      (
        await h.call(
          deleteTripSeasonTool,
          { id: SEASON, confirm: true },
          READER,
        )
      ).isError,
    ).toBe(true);
    expect(h.rows.has(SEASON)).toBe(true);
  });
});

describe("list_trip_seasons", () => {
  it("reads for a read-only token, a region by its parks", async () => {
    const h = harness([halloween()]);
    const { body } = await h.call(
      listTripSeasonsTool,
      { region: "japan" },
      READER,
    );
    expect(body.seasons.map((s: { id: string }) => s.id)).toEqual([SEASON]);
    expect(h.regions.resolveParks).toHaveBeenCalled();
    expect(
      z.object(listTripSeasonsTool.input).safeParse({ region: "atlantis" })
        .success,
    ).toBe(false);
  });
});

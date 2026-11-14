import { DataSource } from "typeorm";
import { z } from "zod";
import { RedisMock } from "../../../test/mocks/redis.mock";
import { McpServerFactory } from "../mcp-server.factory";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import { AdminSeasonService } from "../../admin/content/admin-season.service";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import type { ParkSeason } from "../../parks/entities/park-season.entity";
import { bindRideSeasonsTool } from "./bind-ride-seasons.tool";

const PARK = "11111111-1111-4111-8111-111111111111";
const OTHER_PARK = "11111111-1111-4111-8111-999999999999";
const RIDE = "55555555-5555-4555-8555-555555555555";
const FRIGHT = "22222222-2222-4222-8222-222222222221";
const WINTER = "22222222-2222-4222-8222-222222222222";
const CLOSURE = "22222222-2222-4222-8222-222222222223";
const ELSEWHERE = "22222222-2222-4222-8222-222222222224";
const CANCELLED = "22222222-2222-4222-8222-222222222225";

const EDITOR: AdminPrincipal = {
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
const VIEWER: AdminPrincipal = {
  ...EDITOR,
  role: "viewer",
  apiToken: {
    id: "r",
    kind: "oauth",
    clientName: "Claude",
    scope: "admin:read",
  },
};

const season = (id: string, patch: Partial<ParkSeason> = {}): ParkSeason =>
  ({
    id,
    parkId: PARK,
    kind: "halloween",
    name: "Fright Nights",
    startDate: "2026-10-02",
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
    sourceUrl: null,
    confirmedAt: null,
    note: null,
    updatedBy: null,
    createdAt: new Date("2026-08-01T00:00:00Z"),
    updatedAt: new Date("2026-08-01T00:00:00Z"),
    ...patch,
  }) as ParkSeason;

/** The real season services over in-memory rows, and the ride as the entity loads it. */
function harness(ride: Record<string, unknown> = {}) {
  const rows = new Map(
    [
      season(FRIGHT, {
        attractionIds: ["66666666-6666-4666-8666-666666666666"],
      }),
      season(WINTER, {
        kind: "winter",
        name: "Wintertraum",
        startDate: "2026-11-20",
        endDate: "2027-01-10",
        attractionIds: [RIDE],
      }),
      season(CLOSURE, { kind: "closure", name: null }),
      season(ELSEWHERE, { parkId: OTHER_PARK }),
      season(CANCELLED, { status: "cancelled" }),
    ].map((s) => [s.id, s]),
  );
  const seasonRepo = {
    save: jest.fn(async (row: ParkSeason) => {
      rows.set(row.id, { ...row });
      return row;
    }),
    findOne: jest.fn(async ({ where }: { where: { id: string } }) => {
      const row = rows.get(where.id);
      return row ? { ...row } : null;
    }),
  };
  const attractions = {
    find: jest.fn(async ({ where }: { where: { id: { _value: string[] } } }) =>
      where.id._value.map((id) => ({ id })),
    ),
  };
  const seasons = new ParkSeasonService(
    seasonRepo as never,
    { findOne: jest.fn() } as never,
    attractions as never,
  );
  const curation = { publish: jest.fn(async () => undefined) };
  const audit = {
    record: jest.fn(async (_entry: { action: string }) => null),
  };
  const service = new AdminSeasonService(
    seasons,
    curation as never,
    audit as never,
    {} as never,
    {} as never,
    new RedisMock() as never,
  );
  const row = {
    id: RIDE,
    parkId: PARK,
    name: "Maze of Fear",
    curatedIsSeasonal: null,
    curatedSeasonMonths: null,
    seasonBindings: [
      {
        id: WINTER,
        kind: "winter",
        name: "Wintertraum",
        status: "confirmed",
        startDate: "2026-11-20",
        endDate: "2027-01-10",
        dates: null,
      },
    ],
    ...ride,
  };
  const dataSource = {
    getRepository: () => ({ findOne: async () => row }),
  };
  const factory = new McpServerFactory({
    get: (token: unknown) => (token === DataSource ? dataSource : service),
  } as never);
  const call = async (input: object, who = EDITOR) => {
    const parsed = z.object(bindRideSeasonsTool.input).parse(input);
    const result = await factory.call(bindRideSeasonsTool, parsed, who);
    const text = (result.content[0] as { text: string }).text;
    return {
      isError: result.isError === true,
      text,
      body: result.isError ? null : JSON.parse(text),
    };
  };
  return { call, rows, seasonRepo, curation, audit };
}

describe("bind_ride_seasons", () => {
  it("shows what would change in a dry run and writes nothing", async () => {
    const h = harness();
    const { body } = await h.call({
      attraction: RIDE,
      bind: [FRIGHT, WINTER],
    });
    expect(body.dryRun).toBe(true);
    expect(
      body.changes.map((c: { season: { id: string }; changes: boolean }) => [
        c.season.id,
        c.changes,
      ]),
    ).toEqual([
      [FRIGHT, true],
      [WINTER, false],
    ]);
    expect(body.seasonsAfter.map((s: { id: string }) => s.id)).toEqual([
      FRIGHT,
      WINTER,
    ]);
    expect(body.warnings).toBeUndefined();
    expect(h.seasonRepo.save).not.toHaveBeenCalled();
    expect(h.audit.record).not.toHaveBeenCalled();
  });

  it("adds and removes the ride in each season's list, through the season edit", async () => {
    const h = harness();
    const { body } = await h.call({
      attraction: RIDE,
      bind: [FRIGHT],
      unbind: [WINTER],
      dryRun: false,
    });
    expect(body.written).toEqual([FRIGHT, WINTER]);
    expect(h.rows.get(FRIGHT)?.attractionIds).toEqual([
      "66666666-6666-4666-8666-666666666666",
      RIDE,
    ]);
    expect(h.rows.get(WINTER)?.attractionIds).toBeNull();
    expect(h.audit.record.mock.calls.map(([entry]) => entry.action)).toEqual([
      "park.season.update",
      "park.season.update",
    ]);
    // Published with the rides it bound before and after: the ride's caches go.
    expect(h.curation.publish).toHaveBeenCalledWith(PARK, [
      "66666666-6666-4666-8666-666666666666",
      RIDE,
    ]);
    expect(h.curation.publish).toHaveBeenCalledWith(PARK, [RIDE]);
    expect(body.seasonsAfter.map((s: { id: string }) => s.id)).toEqual([
      FRIGHT,
    ]);
  });

  it("refuses a season of another park, a kind that binds nothing and a cancelled one", async () => {
    const h = harness();
    expect(
      (await h.call({ attraction: RIDE, bind: [ELSEWHERE] })).text,
    ).toMatch(/another park/);
    expect((await h.call({ attraction: RIDE, bind: [CLOSURE] })).text).toMatch(
      /closure season, which binds no ride/,
    );
    expect(
      (await h.call({ attraction: RIDE, bind: [CANCELLED] })).text,
    ).toMatch(/cancelled/);
    // Leaving a cancelled season is a cleanup, not a binding.
    expect(
      (await h.call({ attraction: RIDE, unbind: [CANCELLED] })).isError,
    ).toBe(false);
    expect(
      (await h.call({ attraction: RIDE, bind: [FRIGHT], unbind: [FRIGHT] }))
        .text,
    ).toMatch(/bound and unbound at once/);
  });

  it("pauses a ride during a season with mode not_during, never in both lists", async () => {
    const h = harness();
    const dry = await h.call({
      attraction: RIDE,
      bind: [FRIGHT],
      mode: "not_during",
    });
    expect(dry.body.mode).toBe("not_during");
    expect(dry.body.seasonsBefore).toEqual([]);
    expect(dry.body.seasonsAfter.map((s: { id: string }) => s.id)).toEqual([
      FRIGHT,
    ]);
    const { body } = await h.call({
      attraction: RIDE,
      bind: [FRIGHT],
      mode: "not_during",
      dryRun: false,
    });
    expect(body.written).toEqual([FRIGHT]);
    expect(h.rows.get(FRIGHT)?.pausedAttractionIds).toEqual([RIDE]);
    // Its only-during list is left as it was.
    expect(h.rows.get(FRIGHT)?.attractionIds).toEqual([
      "66666666-6666-4666-8666-666666666666",
    ]);
    // Wintertraum lists the ride as only_during: pausing it there is refused.
    expect(
      (await h.call({ attraction: RIDE, bind: [WINTER], mode: "not_during" }))
        .text,
    ).toMatch(/already lists the ride as only_during/);
  });

  it("warns where the ride's own curated season takes precedence", async () => {
    const months = harness({ curatedSeasonMonths: [10] });
    const { body } = await months.call({ attraction: RIDE, bind: [FRIGHT] });
    expect(body.warnings).toEqual([
      expect.stringMatching(/curated season months \(10\)/),
    ]);
    const notSeasonal = harness({ curatedIsSeasonal: false });
    expect(
      (await notSeasonal.call({ attraction: RIDE, bind: [FRIGHT] })).body
        .warnings,
    ).toEqual([expect.stringMatching(/curatedIsSeasonal false/)]);
  });

  it("is an editor's tool", async () => {
    const h = harness();
    const { isError, text } = await h.call(
      { attraction: RIDE, bind: [FRIGHT] },
      VIEWER,
    );
    expect(isError).toBe(true);
    expect(text).toMatch(/admin:write/);
  });
});

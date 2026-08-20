import { BadRequestException, NotFoundException } from "@nestjs/common";
import { ParkSeasonService } from "./park-season.service";

/** A repository stub that records what it was asked to save. */
function repoStub(overrides: Record<string, unknown> = {}) {
  return {
    create: (input: unknown) => input,
    save: jest.fn(async (input: unknown) => ({
      id: "season-1",
      ...(input as object),
    })),
    findOne: jest.fn(async (): Promise<unknown> => null),
    find: jest.fn(async (): Promise<unknown[]> => []),
    delete: jest.fn(async () => ({ affected: 1 })),
    createQueryBuilder: jest.fn(),
    ...overrides,
  };
}

describe("ParkSeasonService", () => {
  const park = { id: "park-1", name: "Walibi Holland" };

  function build(attractions: Array<{ id: string }> = []) {
    const seasons = repoStub();
    const parks = repoStub({
      findOne: jest.fn(async (): Promise<unknown> => park),
    });
    const attractionRepo = repoStub({
      find: jest.fn(async (): Promise<unknown[]> => attractions),
    });
    const service = new ParkSeasonService(
      seasons as never,
      parks as never,
      attractionRepo as never,
    );
    return { service, seasons, parks, attractionRepo };
  }

  const valid = {
    kind: "halloween" as const,
    name: "Halloween Fright Nights",
    startDate: "2026-10-03",
    endDate: "2026-11-01",
  };

  it("stores a plain range", async () => {
    const { service, seasons } = build();
    await service.create("park-1", valid, "user-1");
    expect(seasons.save).toHaveBeenCalledWith(
      expect.objectContaining({
        kind: "halloween",
        name: "Halloween Fright Nights",
        startDate: "2026-10-03",
        endDate: "2026-11-01",
        // Null, not [] — "every day between start and end".
        dates: null,
        status: "announced",
        updatedBy: "user-1",
      }),
    );
  });

  it("refuses a park that does not exist", async () => {
    const { service, parks } = build();
    parks.findOne = jest.fn(async (): Promise<unknown> => null);
    await expect(service.create("nope", valid, null)).rejects.toBeInstanceOf(
      NotFoundException,
    );
  });

  describe("explicit dates", () => {
    it("keeps the days a season actually runs, sorted and deduped", async () => {
      // Walibi's Fright Nights: weekends plus three single dates. Stored as a
      // bare range it would claim the park is haunted on a Tuesday.
      const { service, seasons } = build();
      await service.create(
        "park-1",
        {
          ...valid,
          dates: ["2026-10-10", "2026-10-03", "2026-10-10", "2026-10-04"],
        },
        null,
      );
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({
          dates: ["2026-10-03", "2026-10-04", "2026-10-10"],
        }),
      );
    });

    it("rejects a date outside the season's range", async () => {
      const { service } = build();
      await expect(
        service.create("park-1", { ...valid, dates: ["2026-09-30"] }, null),
      ).rejects.toThrow(/outside the season's range/);
    });

    it("treats an empty list as 'every day', not 'no day'", async () => {
      const { service, seasons } = build();
      await service.create("park-1", { ...valid, dates: [] }, null);
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({ dates: null }),
      );
    });
  });

  describe("validation", () => {
    it("rejects an end before the start", async () => {
      const { service } = build();
      await expect(
        service.create(
          "park-1",
          { ...valid, startDate: "2026-11-01", endDate: "2026-10-03" },
          null,
        ),
      ).rejects.toThrow(/endDate must not be before startDate/);
    });

    it("rejects a date that looks right and is not real", async () => {
      // The regex alone lets 2026-02-30 through; Postgres would then answer
      // with an error from a layer that cannot say which field it was.
      const { service } = build();
      await expect(
        service.create("park-1", { ...valid, startDate: "2026-02-30" }, null),
      ).rejects.toThrow(/not a real date/);
    });

    it("rejects an unknown kind and an unknown status", async () => {
      const { service } = build();
      await expect(
        service.create("park-1", { ...valid, kind: "spooky" as never }, null),
      ).rejects.toThrow(/kind must be one of/);
      await expect(
        service.create("park-1", { ...valid, status: "maybe" as never }, null),
      ).rejects.toThrow(/status must be one of/);
    });

    it("rejects a time that is not HH:MM", async () => {
      const { service } = build();
      await expect(
        service.create("park-1", { ...valid, opensAt: "7pm" }, null),
      ).rejects.toThrow(/opensAt must be HH:MM/);
      await expect(
        service.create("park-1", { ...valid, closesAt: "25:00" }, null),
      ).rejects.toThrow(/closesAt must be HH:MM/);
    });

    it("accepts a time past midnight", async () => {
      // A Halloween event running 19:00–01:00 is the normal case, not an edge.
      const { service, seasons } = build();
      await service.create(
        "park-1",
        { ...valid, opensAt: "19:00", closesAt: "01:00" },
        null,
      );
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({ opensAt: "19:00", closesAt: "01:00" }),
      );
    });

    it("refuses a price with no currency", async () => {
      const { service } = build();
      await expect(
        service.create("park-1", { ...valid, priceFrom: 53 }, null),
      ).rejects.toThrow(/priceFrom needs a priceCurrency/);
    });

    it("stores a price with two decimals and an upper-cased currency", async () => {
      const { service, seasons } = build();
      await service.create(
        "park-1",
        { ...valid, priceFrom: 53, priceCurrency: "eur" },
        null,
      );
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({ priceFrom: "53.00", priceCurrency: "EUR" }),
      );
    });

    it("refuses a link a browser would not follow", async () => {
      // Both URLs are rendered as `<a href>` on the public park page. React
      // happens to strip a `javascript:` href, but that is the consumer's
      // property, not this API's, and this API is public.
      const { service } = build();
      await expect(
        service.create(
          "park-1",
          { ...valid, url: "javascript:alert(1)" },
          null,
        ),
      ).rejects.toThrow(/http\(s\) address/);
      await expect(
        service.create("park-1", { ...valid, sourceUrl: "not a url" }, null),
      ).rejects.toThrow(/full address/);
    });

    it("stores an ordinary link and leaves an empty one null", async () => {
      const { service, seasons } = build();
      await service.create(
        "park-1",
        {
          ...valid,
          url: "  https://www.walibi.nl/fright-nights  ",
          sourceUrl: "",
        },
        null,
      );
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({
          url: "https://www.walibi.nl/fright-nights",
          sourceUrl: null,
        }),
      );
    });

    it("rejects a currency that is not ISO 4217", async () => {
      const { service } = build();
      await expect(
        service.create(
          "park-1",
          { ...valid, priceFrom: 53, priceCurrency: "euros" },
          null,
        ),
      ).rejects.toThrow(/three-letter ISO 4217/);
    });
  });

  describe("attraction references", () => {
    it("accepts rides that are in this park", async () => {
      const { service, seasons } = build([{ id: "ride-1" }, { id: "ride-2" }]);
      await service.create(
        "park-1",
        { ...valid, kind: "maintenance", attractionIds: ["ride-1", "ride-2"] },
        null,
      );
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({ attractionIds: ["ride-1", "ride-2"] }),
      );
    });

    it("rejects a ride from another park", async () => {
      // A maintenance window naming a ride that is not in this park is a
      // copy-paste error, and it would render on a page that ride is not on.
      const { service } = build([{ id: "ride-1" }]);
      await expect(
        service.create(
          "park-1",
          { ...valid, attractionIds: ["ride-1", "ride-from-elsewhere"] },
          null,
        ),
      ).rejects.toThrow(/not in this park: ride-from-elsewhere/);
    });
  });

  describe("update", () => {
    it("validates the merged season, not just the patch", async () => {
      // Moving only endDate still has to leave a season that ends after it
      // starts — and the CHECK constraint is a much worse place to find out.
      const existing = {
        id: "season-1",
        parkId: "park-1",
        kind: "halloween",
        name: null,
        startDate: "2026-10-03",
        endDate: "2026-11-01",
        dates: null,
        status: "announced",
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
      };
      const { service, seasons } = build();
      seasons.findOne = jest.fn(async (): Promise<unknown> => existing);

      await expect(
        service.update("season-1", { endDate: "2026-09-01" }, null),
      ).rejects.toBeInstanceOf(BadRequestException);
    });

    it("leaves untouched fields alone", async () => {
      const existing = {
        id: "season-1",
        parkId: "park-1",
        kind: "halloween",
        name: "Fright Nights",
        startDate: "2026-10-03",
        endDate: "2026-11-01",
        dates: ["2026-10-03"],
        status: "announced",
        separateTicket: true,
        priceFrom: "53.00",
        priceCurrency: "EUR",
        opensAt: "19:00",
        closesAt: "01:00",
        attractionIds: null,
        url: null,
        sourceUrl: "https://walibi.nl",
        confirmedAt: null,
        note: null,
      };
      const { service, seasons } = build();
      seasons.findOne = jest.fn(async (): Promise<unknown> => existing);

      await service.update("season-1", { status: "confirmed" }, "user-2");
      expect(seasons.save).toHaveBeenCalledWith(
        expect.objectContaining({
          status: "confirmed",
          name: "Fright Nights",
          dates: ["2026-10-03"],
          priceFrom: "53.00",
          opensAt: "19:00",
          updatedBy: "user-2",
        }),
      );
    });
  });
});

/**
 * The park editor answered every request with a TypeError.
 *
 * A query builder resolves `alias.name` through the entity's metadata, so a
 * column name where a property belongs only survives as long as nothing looks
 * it up. `list()` joins the park and paginates, which sends TypeORM down the
 * route that maps ORDER BY through that metadata — `season.start_date` is not
 * a property, and it read `databaseName` off `undefined`. The failure was
 * invisible to the type checker, to lint, and to every unit test, and it took
 * out `GET /v1/admin/content/parks/:id` completely.
 *
 * A source check rather than a query: reproducing it needs a real database,
 * a join and a `take()` in one statement, and the mistake is a spelling one.
 */
describe("ParkSeasonService — column names never reach a query builder", () => {
  const sources = [
    "src/parks/services/park-season.service.ts",
    "src/admin/content/admin-content.controller.ts",
    "src/admin/auth/admin-audit.service.ts",
  ];

  it.each(sources)("orders %s by properties, not columns", (file) => {
    // eslint-disable-next-line @typescript-eslint/no-require-imports
    const source: string = require("fs").readFileSync(file, "utf8");

    const offenders = [...source.matchAll(/\.(?:add)?orderBy\(\s*"([^"]+)"/gi)]
      .map((match) => match[1])
      // A column name where a property belongs — resolvable only by luck.
      .filter((reference) => /^[a-z]+\.[a-z_]+$/i.test(reference))
      .filter((reference) => reference.includes("_"));

    // The other half of the same trap: an ORDER BY string is split at its
    // first dot and everything before it is taken for an alias, so a raw SQL
    // expression asks for a table named `CASE WHEN LOWER(COALESCE(attraction`.
    // Select the expression under a name and order by that name instead.
    const expressions = [
      ...source.matchAll(/\.(?:add)?orderBy\(\s*["`]([^"`]+)["`]/gi),
    ]
      .map((match) => match[1])
      .filter(
        (reference) => reference.includes("(") && reference.includes("."),
      );

    expect({ offenders, expressions }).toEqual({
      offenders: [],
      expressions: [],
    });
  });
});

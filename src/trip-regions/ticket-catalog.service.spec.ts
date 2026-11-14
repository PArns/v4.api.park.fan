import type { Redis } from "ioredis";
import type { Repository } from "typeorm";
import { ORLANDO, TRIP_REGIONS } from "./regions";
import type { TicketProductRecord } from "./entities/ticket-product.entity";
import { TicketCatalogService } from "./ticket-catalog.service";
import { seedCatalog, seedRows } from "./ticket-catalog";
import { ORLANDO_TICKETS, seedTickets } from "./tickets";

/** A table of rows in memory, with the unique (region, slug) index a second boot meets. */
function table(initial: TicketProductRecord[] = []) {
  const rows = [...initial];
  const insert = jest.fn(async (values: TicketProductRecord[]) => {
    const inserted = values.filter(
      (v) => !rows.some((r) => r.region === v.region && r.slug === v.slug),
    );
    rows.push(...inserted);
    return { raw: inserted.map((_, i) => ({ id: `id-${i}` })) };
  });
  const repo = {
    count: jest.fn(
      async ({ where }: { where: { region: string } }) =>
        rows.filter((r) => r.region === where.region).length,
    ),
    find: jest.fn(async ({ where }: { where: { region: string } }) =>
      rows
        .filter((r) => r.region === where.region)
        .sort((a, b) => a.sortOrder - b.sortOrder),
    ),
    createQueryBuilder: () => {
      let values: TicketProductRecord[] = [];
      const qb = {
        insert: () => qb,
        into: () => qb,
        values: (v: TicketProductRecord[]) => {
          values = v;
          return qb;
        },
        orIgnore: () => qb,
        execute: () => insert(values),
      };
      return qb;
    },
  };
  return { rows, repo, insert };
}

function redisStub() {
  const store = new Map<string, string>();
  return {
    store,
    redis: {
      get: jest.fn(async (k: string) => store.get(k) ?? null),
      set: jest.fn(async (k: string, v: string) => {
        store.set(k, v);
        return "OK";
      }),
      del: jest.fn(async (k: string) => {
        store.delete(k);
        return 1;
      }),
    },
  };
}

const stamped = (rows = seedRows(ORLANDO)): TicketProductRecord[] =>
  rows.map(
    (r) =>
      ({
        ...r,
        id: r.slug,
        createdAt: new Date("2026-11-20T10:00:00Z"),
        updatedAt: new Date("2026-11-20T10:00:00Z"),
        updatedBy: null,
      }) as TicketProductRecord,
  );

describe("TicketCatalogService", () => {
  it("seeds an empty region once, and a second call or boot inserts nothing", async () => {
    const { repo, rows, insert } = table();
    const { redis } = redisStub();
    const service = new TicketCatalogService(
      repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    expect(await service.seed(ORLANDO)).toBe(ORLANDO_TICKETS.length);
    expect(rows).toHaveLength(ORLANDO_TICKETS.length);
    expect(await service.seed(ORLANDO)).toBe(0);
    expect(insert).toHaveBeenCalledTimes(1);
    // Two instances that both saw the table empty: the index makes the second insert a no-op.
    const racing = table(stamped());
    racing.repo.count.mockResolvedValueOnce(0);
    const second = new TicketCatalogService(
      racing.repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    expect(await second.seed(ORLANDO)).toBe(0);
    expect(racing.rows).toHaveLength(ORLANDO_TICKETS.length);
  });

  it("seeds every region on boot under its own slug, each with its own products", async () => {
    const { repo, rows } = table();
    const { redis } = redisStub();
    const service = new TicketCatalogService(
      repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    await service.onApplicationBootstrap();
    for (const region of Object.values(TRIP_REGIONS)) {
      const own = rows.filter((r) => r.region === region.slug);
      expect(own.map((r) => r.slug)).toEqual(
        seedTickets(region).map((p) => p.id),
      );
      const groups = new Set(region.groups.map((g) => g.key));
      for (const row of own) expect(groups.has(row.resort)).toBe(true);
    }
    expect(rows.filter((r) => r.region === "paris")).toHaveLength(9);
    expect(rows.filter((r) => r.region === "southern-california")).toHaveLength(
      25,
    );
  });

  it("serves the table's products, versioned, from memory after the first read", async () => {
    const { repo } = table(stamped());
    const { redis, store } = redisStub();
    const service = new TicketCatalogService(
      repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    const first = await service.catalog(ORLANDO);
    expect(first.products).toHaveLength(ORLANDO_TICKETS.length);
    expect(first.version).toMatch(/^[0-9a-f]{12}$/);
    expect(first.updatedAt).toBe("2026-11-20T10:00:00.000Z");
    expect(store.size).toBe(1);
    await service.catalog(ORLANDO);
    expect(repo.find).toHaveBeenCalledTimes(1);
    const region = await service.withTickets(ORLANDO);
    expect(region.ticketsVersion).toBe(first.version);
    expect(region.tickets).toHaveLength(ORLANDO_TICKETS.length);
  });

  it("changes the version with an edit, once the catalog is evicted", async () => {
    const rows = stamped();
    const { repo } = table(rows);
    const { redis } = redisStub();
    const service = new TicketCatalogService(
      repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    const before = (await service.catalog(ORLANDO)).version;
    rows[0].rules = {
      ...rows[0].rules,
      windowTable: rows[0].rules.windowTable?.map((r) => ({
        ...r,
        window: r.window + 1,
      })),
    };
    expect((await service.catalog(ORLANDO)).version).toBe(before);
    await service.evict(ORLANDO.slug);
    expect((await service.catalog(ORLANDO)).version).not.toBe(before);
  });

  it("plans with the seed when the table cannot be read, and keeps that out of Redis", async () => {
    const { repo } = table();
    repo.find.mockRejectedValueOnce(new Error("db down"));
    const { redis, store } = redisStub();
    const service = new TicketCatalogService(
      repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    const catalog = await service.catalog(ORLANDO);
    expect(catalog.version).toBe(seedCatalog(ORLANDO).version);
    expect(store.size).toBe(0);
  });

  it("archives a product without losing it: tickets= may still name it", async () => {
    const rows = stamped();
    rows[0].active = false;
    const { repo } = table(rows);
    const { redis } = redisStub();
    const service = new TicketCatalogService(
      repo as unknown as Repository<TicketProductRecord>,
      redis as unknown as Redis,
    );
    const catalog = await service.catalog(ORLANDO);
    expect(catalog.products[0]).toMatchObject({
      id: rows[0].slug,
      archived: true,
    });
  });
});

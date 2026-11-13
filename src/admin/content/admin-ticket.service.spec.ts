import { BadRequestException, NotFoundException } from "@nestjs/common";
import type { Queue } from "bull";
import type { Repository } from "typeorm";
import { CDN_SETTLE_MS } from "../../common/revalidation/revalidation.service";
import type { RevalidationService } from "../../common/revalidation/revalidation.service";
import { ORLANDO } from "../../trip-regions/regions";
import type { TicketProductRecord } from "../../trip-regions/entities/ticket-product.entity";
import type { TicketCatalogService } from "../../trip-regions/ticket-catalog.service";
import type { TripRegionService } from "../../trip-regions/trip-region.service";
import {
  seedCatalog,
  seedRows,
  withCatalog,
} from "../../trip-regions/ticket-catalog";
import type { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminTicketService, draftOf } from "./admin-ticket.service";

const ADMIN = {
  userId: "u-1",
  email: "editor@example.com",
  displayName: "Editor",
  role: "editor",
  sessionToken: null,
} as unknown as AdminPrincipal;

function setup() {
  const rows: TicketProductRecord[] = seedRows(ORLANDO).map(
    (r) =>
      ({
        ...r,
        id: r.slug,
        createdAt: new Date("2026-11-20T10:00:00Z"),
        updatedAt: new Date("2026-11-20T10:00:00Z"),
        updatedBy: null,
      }) as TicketProductRecord,
  );
  const repo = {
    find: jest.fn(async ({ where }: { where: { region: string } }) =>
      rows
        .filter((r) => r.region === where.region)
        .sort((a, b) => a.sortOrder - b.sortOrder),
    ),
    findOne: jest.fn(
      async ({ where }: { where: { region: string; slug: string } }) =>
        rows.find((r) => r.region === where.region && r.slug === where.slug) ??
        null,
    ),
    create: jest.fn((v: Partial<TicketProductRecord>) => v),
    save: jest.fn(async (v: TicketProductRecord) => {
      const stamped = {
        ...v,
        id: v.id ?? v.slug,
        createdAt: v.createdAt ?? new Date("2026-11-21T10:00:00Z"),
        updatedAt: new Date("2026-11-21T10:00:00Z"),
      } as TicketProductRecord;
      const i = rows.findIndex((r) => r.slug === stamped.slug);
      if (i >= 0) rows[i] = stamped;
      else rows.push(stamped);
      return stamped;
    }),
    manager: {
      transaction: async (
        fn: (m: {
          update: (
            e: unknown,
            where: { slug: string },
            set: Partial<TicketProductRecord>,
          ) => Promise<void>;
        }) => Promise<void>,
      ) =>
        fn({
          update: async (_e, where, set) => {
            const row = rows.find((r) => r.slug === where.slug);
            if (row) Object.assign(row, set);
          },
        }),
    },
  };
  const catalog = { evict: jest.fn(async () => undefined) };
  const regions = {
    region: (slug: string) => {
      if (slug !== ORLANDO.slug) throw new NotFoundException();
      return ORLANDO;
    },
    regionWithTickets: async () => withCatalog(ORLANDO, seedCatalog(ORLANDO)),
    today: () => "2026-11-13",
  };
  const revalidation = { revalidateTags: jest.fn(async () => true) };
  const audit = { record: jest.fn(async () => null) };
  const queue = { add: jest.fn(async () => ({})) };
  const service = new AdminTicketService(
    repo as unknown as Repository<TicketProductRecord>,
    catalog as unknown as TicketCatalogService,
    regions as unknown as TripRegionService,
    revalidation as unknown as RevalidationService,
    audit as unknown as AdminAuditService,
    queue as unknown as Queue,
  );
  return { service, rows, repo, catalog, revalidation, audit, queue };
}

/** A new product, made from a seed row as an editor would start one. */
function newProduct(rows: TicketProductRecord[]): Record<string, unknown> {
  const base = draftOf(rows.find((r) => r.slug === "legoland-1-day")!);
  return {
    ...base,
    slug: "legoland-1-day-hopper",
    name: "LEGOLAND Florida 1-Day Hopper ticket",
    links: [
      {
        seller: "Attraction Tickets",
        url: "https://www.attractiontickets.com/en/orlando-attraction-tickets/legoland-florida",
        sponsored: true,
        markets: ["uk"],
      },
      ...(base.links as unknown[]),
    ],
  };
}

const refusedPaths = async (promise: Promise<unknown>): Promise<string[]> => {
  try {
    await promise;
  } catch (err) {
    expect(err).toBeInstanceOf(BadRequestException);
    const body = (err as BadRequestException).getResponse() as {
      problems: Array<{ path: string }>;
    };
    return body.problems.map((p) => p.path);
  }
  throw new Error("not refused");
};

describe("AdminTicketService", () => {
  it("creates a checked product, audits it, then publishes outside-in", async () => {
    const { service, rows, audit, catalog, revalidation, queue } = setup();
    const created = await service.create("orlando", newProduct(rows), ADMIN);
    expect(created).toMatchObject({
      id: "legoland-1-day-hopper",
      slug: "legoland-1-day-hopper",
      active: true,
      updatedBy: "u-1",
    });
    expect(created.links[0]).toMatchObject({ sponsored: true });
    expect(created.shopUrl).toBe(
      "https://www.legoland.com/florida/tickets-passes/tickets/",
    );
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ticket_product.create",
        entityType: "ticket_product",
        entityId: "orlando/legoland-1-day-hopper",
        after: expect.objectContaining({ slug: "legoland-1-day-hopper" }),
      }),
    );
    // Redis first, then the frontend, then again after the edge window.
    const evicted = catalog.evict.mock.invocationCallOrder[0];
    const told = revalidation.revalidateTags.mock.invocationCallOrder[0];
    const scheduled = queue.add.mock.invocationCallOrder[0];
    expect(evicted).toBeLessThan(told);
    expect(told).toBeLessThan(scheduled);
    expect(revalidation.revalidateTags).toHaveBeenCalledWith([
      "trip-tickets:orlando",
    ]);
    expect(queue.add).toHaveBeenCalledWith(
      "revalidate-tags",
      { tags: ["trip-tickets:orlando"] },
      expect.objectContaining({ delay: CDN_SETTLE_MS }),
    );
  });

  it("refuses a product with every problem by path, and writes nothing", async () => {
    const { service, rows, repo, audit, revalidation } = setup();
    const paths = await refusedPaths(
      service.create(
        "orlando",
        {
          ...newProduct(rows),
          links: [
            { seller: "x", url: "javascript:alert(1)", sponsored: false },
          ],
          parks: ["legoland-florida", "nope"],
        },
        ADMIN,
      ),
    );
    expect(paths).toEqual(
      expect.arrayContaining(["links[0].url", "links", "parks[1]"]),
    );
    expect(repo.save).not.toHaveBeenCalled();
    expect(audit.record).not.toHaveBeenCalled();
    expect(revalidation.revalidateTags).not.toHaveBeenCalled();
  });

  it("refuses a slug that exists already", async () => {
    const { service, rows } = setup();
    expect(
      await refusedPaths(
        service.create(
          "orlando",
          { ...newProduct(rows), slug: "legoland-1-day" },
          ADMIN,
        ),
      ),
    ).toContain("slug");
  });

  it("patches a field onto the stored product and audits before and after", async () => {
    const { service, audit } = setup();
    const patched = await service.update(
      "orlando",
      "wdw-date-based",
      { name: "Walt Disney World theme park ticket" },
      ADMIN,
    );
    expect(patched.name).toBe("Walt Disney World theme park ticket");
    expect(patched.windowTable).toHaveLength(10);
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ticket_product.update",
        before: expect.objectContaining({
          name: "Walt Disney World date-based theme park ticket",
        }),
        after: expect.objectContaining({
          name: "Walt Disney World theme park ticket",
        }),
      }),
    );
  });

  it("never changes a slug, and refuses a rule that breaks the product", async () => {
    const { service } = setup();
    expect(
      await refusedPaths(
        service.update(
          "orlando",
          "wdw-date-based",
          { slug: "wdw-other" },
          ADMIN,
        ),
      ),
    ).toEqual(["slug"]);
    expect(
      await refusedPaths(
        service.update(
          "orlando",
          "wdw-date-based",
          { days: { min: 1, max: 12 } },
          ADMIN,
        ),
      ),
    ).toEqual(expect.arrayContaining(["windowTable"]));
  });

  it("archives instead of deleting, once, with the reason in the audit", async () => {
    const { service, audit, revalidation } = setup();
    const archived = await service.archive(
      "orlando",
      "wdw-4-park-magic",
      "promotion over",
      ADMIN,
    );
    expect(archived.active).toBe(false);
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ticket_product.archive",
        reason: "promotion over",
        before: expect.objectContaining({ active: true }),
        after: expect.objectContaining({ active: false }),
      }),
    );
    await service.archive("orlando", "wdw-4-park-magic", undefined, ADMIN);
    expect(audit.record).toHaveBeenCalledTimes(1);
    expect(revalidation.revalidateTags).toHaveBeenCalledTimes(1);
  });

  it("orders every product of the region, and refuses a partial order", async () => {
    const { service, rows, audit } = setup();
    const slugs = rows.map((r) => r.slug);
    expect(
      await refusedPaths(service.reorder("orlando", slugs.slice(1), ADMIN)),
    ).toEqual(["slugs"]);
    const reversed = [...slugs].reverse();
    const result = await service.reorder("orlando", reversed, ADMIN);
    expect(result.tickets.map((t) => t.slug)).toEqual(reversed);
    expect(audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ticket_product.reorder",
        after: { order: reversed },
      }),
    );
  });

  it("checks a draft without saving it", async () => {
    const { service, rows, repo, audit } = setup();
    expect(await service.validate("orlando", newProduct(rows))).toEqual({
      ok: true,
      problems: [],
      dryRun: { owned: "ok", planFirst: "ok" },
    });
    const bad = await service.validate("orlando", {
      ...newProduct(rows),
      confidence: "sure",
    });
    expect(bad).toMatchObject({
      ok: false,
      problems: [expect.objectContaining({ path: "confidence" })],
    });
    expect(repo.save).not.toHaveBeenCalled();
    expect(audit.record).not.toHaveBeenCalled();
  });

  it("checks a new product's slug, and an edit's own slug as no conflict", async () => {
    const { service, rows } = setup();
    const taken = {
      ...draftOf(rows.find((r) => r.slug === "legoland-1-day")!),
    };
    // Without ?slug the body is a new product: its slug must be free.
    const asNew = await service.validate("orlando", taken);
    expect(asNew.ok).toBe(false);
    expect(asNew.problems).toEqual([
      { path: "slug", message: "legoland-1-day exists already in orlando" },
    ]);
    // An edit of that product: its own slug is fine, a partial body is merged.
    expect(
      await service.validate("orlando", taken, "legoland-1-day"),
    ).toMatchObject({ ok: true, problems: [] });
    expect(
      await service.validate(
        "orlando",
        { description: "One day at LEGOLAND" },
        "legoland-1-day",
      ),
    ).toMatchObject({ ok: true });
    expect(
      await service.validate("orlando", { slug: "other" }, "legoland-1-day"),
    ).toEqual({
      ok: false,
      problems: [
        { path: "slug", message: "is the product's id and cannot change" },
      ],
    });
    await expect(service.validate("orlando", taken, "nope")).rejects.toThrow(
      NotFoundException,
    );
  });

  it("clears an optional field a PATCH sends as null", async () => {
    const { service, rows } = setup();
    await service.update(
      "orlando",
      "seaworld-fun-card",
      { description: "A year of SeaWorld", notes: "Check the blockouts" },
      ADMIN,
    );
    const cleared = await service.update(
      "orlando",
      "seaworld-fun-card",
      {
        description: null,
        notes: null,
        blockoutUrl: null,
        until: null,
        blockouts: null,
      },
      ADMIN,
    );
    for (const key of [
      "description",
      "notes",
      "blockoutUrl",
      "until",
      "blockouts",
    ]) {
      expect(cleared).not.toHaveProperty(key);
    }
    const pass = await service.update(
      "orlando",
      "wdw-annual-pass",
      { reservationUrl: null },
      ADMIN,
    );
    expect(pass).not.toHaveProperty("reservationUrl");
    const unranked = await service.update(
      "orlando",
      "wdw-date-based",
      { suggestRank: null },
      ADMIN,
    );
    expect(unranked.suggestRank).toBeNull();
    // Nested optional fields take null too; sortOrder null keeps the place.
    const stored = rows.find((r) => r.slug === "wdw-date-based")!;
    const nested = await service.update(
      "orlando",
      "wdw-date-based",
      {
        sortOrder: null,
        onSale: [{ from: "2026-01-01", to: null }],
        links: (draftOf(stored).links as Record<string, unknown>[]).map(
          (link) => ({ ...link, markets: null, locales: null }),
        ),
        sources: [{ url: "https://disneyworld.disney.go.com/", note: null }],
      },
      ADMIN,
    );
    expect(nested.sortOrder).toBe(stored.sortOrder);
    expect(nested.onSale).toEqual([{ from: "2026-01-01" }]);
    expect(nested.links.every((l) => !("markets" in l))).toBe(true);
    expect(nested.sources).toEqual([
      { url: "https://disneyworld.disney.go.com/" },
    ]);
    // A required field cannot be cleared.
    expect(
      await refusedPaths(
        service.update("orlando", "wdw-date-based", { name: null }, ADMIN),
      ),
    ).toEqual(["name"]);
  });

  it("puts a new product last in its region unless it gives a sortOrder", async () => {
    const { service, rows } = setup();
    const last = Math.max(...rows.map((r) => r.sortOrder));
    const unplaced = newProduct(rows);
    delete unplaced.sortOrder;
    const created = await service.create("orlando", unplaced, ADMIN);
    expect(created.sortOrder).toBe(last + 10);
    const blank = await service.create(
      "orlando",
      { ...newProduct(rows), slug: "legoland-blank", sortOrder: null },
      ADMIN,
    );
    expect(blank.sortOrder).toBe(last + 20);
    const listed = await service.list("orlando");
    expect(listed.tickets[listed.tickets.length - 1].slug).toBe(
      "legoland-blank",
    );
    const placed = await service.create(
      "orlando",
      { ...newProduct(rows), slug: "legoland-first", sortOrder: 5 },
      ADMIN,
    );
    expect(placed.sortOrder).toBe(5);
    expect((await service.list("orlando")).tickets[0].slug).toBe(
      "legoland-first",
    );
  });

  it("answers 404 for a product or region it does not have", async () => {
    const { service } = setup();
    await expect(service.get("orlando", "nope")).rejects.toThrow(
      NotFoundException,
    );
    await expect(service.list("paris")).rejects.toThrow(NotFoundException);
  });
});

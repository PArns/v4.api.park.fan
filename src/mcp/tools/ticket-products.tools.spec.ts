import { NotFoundException } from "@nestjs/common";
import { z } from "zod";
import { McpServerFactory } from "../mcp-server.factory";
import type { AnyMcpTool } from "../mcp-tool";
import type { AdminPrincipal } from "../../admin/auth/admin-principal";
import {
  AdminTicketService,
  draftOf,
} from "../../admin/content/admin-ticket.service";
import { PARIS } from "../../trip-regions/regions";
import type { TicketProductRecord } from "../../trip-regions/entities/ticket-product.entity";
import {
  seedCatalog,
  seedRows,
  withCatalog,
} from "../../trip-regions/ticket-catalog";
import { listTicketProductsTool } from "./list-ticket-products.tool";
import { upsertTicketProductTool } from "./upsert-ticket-product.tool";
import { archiveTicketProductTool } from "./archive-ticket-product.tool";

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
  apiToken: { id: "t", kind: "pat", clientName: null, scope: "admin:write" },
};
const READER: AdminPrincipal = {
  ...WRITER,
  role: "viewer",
  apiToken: { id: "r", kind: "pat", clientName: null, scope: "admin:read" },
};

const OLD_NAME =
  "Disneyland Paris 1-Day dated ticket (1 Park, or 2 Parks with .h)";
const NEW_NAME = "Disneyland Paris 1-Day dated ticket (1 Park, or 2 Parks)";

/** The real ticket admin over Paris' seed, as production holds it before the rename. */
function harness() {
  const rows = seedRows(PARIS).map(
    (r) =>
      ({
        ...r,
        id: r.slug,
        name: r.slug === "dlp-1-day" ? OLD_NAME : r.name,
        createdAt: new Date("2026-11-14T14:09:58Z"),
        updatedAt: new Date("2026-11-14T14:09:58Z"),
        updatedBy: null,
      }) as TicketProductRecord,
  );
  const repo = {
    find: jest.fn(async () =>
      [...rows].sort((a, b) => a.sortOrder - b.sortOrder),
    ),
    findOne: jest.fn(async ({ where }: { where: { slug: string } }) => {
      const row = rows.find((r) => r.slug === where.slug);
      return row ? { ...row } : null;
    }),
    create: (v: object) => v,
    save: jest.fn(async (v: TicketProductRecord) => {
      const saved = {
        ...v,
        id: v.slug,
        createdAt: v.createdAt ?? new Date(),
        updatedAt: new Date(),
      } as TicketProductRecord;
      const i = rows.findIndex((r) => r.slug === saved.slug);
      if (i >= 0) rows[i] = saved;
      else rows.push(saved);
      return saved;
    }),
  };
  const catalog = { evict: jest.fn(async () => undefined) };
  const regions = {
    region: (slug: string) => {
      if (slug !== PARIS.slug) throw new NotFoundException();
      return PARIS;
    },
    regionWithTickets: async () => withCatalog(PARIS, seedCatalog(PARIS)),
    today: () => "2026-11-14",
  };
  const audit = { record: jest.fn(async () => null) };
  const revalidation = { revalidateTags: jest.fn(async () => true) };
  const service = new AdminTicketService(
    repo as never,
    catalog as never,
    regions as never,
    revalidation as never,
    audit as never,
    { add: jest.fn(async () => ({})) } as never,
  );
  const factory = new McpServerFactory({ get: () => service } as never);
  const call = async (tool: AnyMcpTool, input: object, who = WRITER) => {
    const result = await factory.call(
      tool,
      z.object(tool.input).parse(input),
      who,
    );
    const text = (result.content[0] as { text: string }).text;
    return {
      isError: result.isError === true,
      text,
      body: result.isError ? null : JSON.parse(text),
    };
  };
  const row = (slug: string) => rows.find((r) => r.slug === slug)!;
  return { call, row, repo, audit, catalog, revalidation };
}

describe("list_ticket_products", () => {
  it("reads a region's catalog, or one product whole, for a read-only token", async () => {
    const h = harness();
    const { body } = await h.call(
      listTicketProductsTool,
      { region: "paris" },
      READER,
    );
    expect(
      body.products.find((p: { slug: string }) => p.slug === "dlp-1-day").name,
    ).toBe(OLD_NAME);
    const one = await h.call(
      listTicketProductsTool,
      { region: "paris", slug: "dlp-1-day" },
      READER,
    );
    expect(one.body.product).toMatchObject({
      slug: "dlp-1-day",
      links: expect.any(Array),
    });
    expect(
      (await h.call(listTicketProductsTool, { region: "paris", slug: "nope" }))
        .text,
    ).toBe("No ticket product nope in paris");
  });
});

describe("upsert_ticket_product", () => {
  const rename = {
    region: "paris",
    slug: "dlp-1-day",
    product: { name: NEW_NAME },
  };

  it("checks a rename in a dry run and saves nothing", async () => {
    const h = harness();
    const { body } = await h.call(upsertTicketProductTool, rename);
    expect(body).toMatchObject({
      dryRun: true,
      action: "update",
      ok: true,
      problems: [],
      samplePlan: { owned: "ok", planFirst: "ok" },
    });
    expect(h.repo.save).not.toHaveBeenCalled();
    expect(h.audit.record).not.toHaveBeenCalled();
  });

  it("renames with the product before and after in the audit log, then publishes the catalog", async () => {
    const h = harness();
    const { body } = await h.call(upsertTicketProductTool, {
      ...rename,
      dryRun: false,
    });
    expect(body).toMatchObject({
      action: "update",
      product: { slug: "dlp-1-day", name: NEW_NAME },
    });
    expect(h.row("dlp-1-day")).toMatchObject({
      name: NEW_NAME,
      updatedBy: WRITER.userId,
    });
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        actor: WRITER,
        action: "ticket_product.update",
        entityId: "paris/dlp-1-day",
        before: expect.objectContaining({ name: OLD_NAME }),
        after: expect.objectContaining({ name: NEW_NAME }),
      }),
    );
    expect(h.catalog.evict).toHaveBeenCalledWith("paris");
    expect(h.revalidation.revalidateTags).toHaveBeenCalledWith([
      "trip-tickets:paris",
    ]);
  });

  it("creates a product the region lacks", async () => {
    const h = harness();
    const { slug: _slug, ...base } = draftOf(h.row("dlp-1-day"));
    const { body } = await h.call(upsertTicketProductTool, {
      region: "paris",
      slug: "dlp-1-day-copy",
      product: { ...base, name: "A second 1-day ticket" },
      dryRun: false,
    });
    expect(body).toMatchObject({
      action: "create",
      product: { slug: "dlp-1-day-copy", active: true },
    });
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({ action: "ticket_product.create" }),
    );
  });

  it("answers bad input by field and writes nothing", async () => {
    const h = harness();
    expect(
      (
        await h.call(upsertTicketProductTool, {
          ...rename,
          product: { name: "" },
        })
      ).body,
    ).toMatchObject({
      ok: false,
      problems: [expect.objectContaining({ path: "name" })],
    });
    expect(
      (
        await h.call(upsertTicketProductTool, {
          ...rename,
          product: { colour: "red" },
        })
      ).text,
    ).toBe("Invalid input: property colour should not exist");
    expect(
      (
        await h.call(upsertTicketProductTool, {
          ...rename,
          product: { slug: "dlp-2" },
        })
      ).isError,
    ).toBe(true);
    const write = await h.call(upsertTicketProductTool, {
      ...rename,
      product: { name: "" },
      dryRun: false,
    });
    expect(write.text).toMatch(/^The ticket product is not valid: name: /);
    expect(h.repo.save).not.toHaveBeenCalled();
  });

  it("is refused to a read-only token", async () => {
    const h = harness();
    const { isError, text } = await h.call(
      upsertTicketProductTool,
      { ...rename, dryRun: false },
      READER,
    );
    expect(isError).toBe(true);
    expect(text).toMatch(/admin:write/);
    expect(h.row("dlp-1-day").name).toBe(OLD_NAME);
  });
});

describe("archive_ticket_product", () => {
  const archive = { region: "paris", slug: "dlp-1-day", confirm: true };

  it("needs confirm", () => {
    const schema = z.object(archiveTicketProductTool.input);
    expect(
      schema.safeParse({ region: "paris", slug: "dlp-1-day" }).success,
    ).toBe(false);
  });

  it("takes the product off the catalog and audits it", async () => {
    const h = harness();
    const { body } = await h.call(archiveTicketProductTool, {
      ...archive,
      reason: "no longer sold",
    });
    expect(body.archived).toEqual({
      slug: "dlp-1-day",
      name: OLD_NAME,
      active: false,
    });
    expect(h.audit.record).toHaveBeenCalledWith(
      expect.objectContaining({
        action: "ticket_product.archive",
        reason: "no longer sold",
      }),
    );
  });

  it("is refused to a read-only token", async () => {
    const h = harness();
    expect(
      (await h.call(archiveTicketProductTool, archive, READER)).isError,
    ).toBe(true);
    expect(h.row("dlp-1-day").active).toBe(true);
  });
});

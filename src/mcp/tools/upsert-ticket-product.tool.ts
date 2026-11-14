import { NotFoundException } from "@nestjs/common";
import { z } from "zod";
import { defineMcpTool, McpToolError } from "../mcp-tool";
import { asToolRefusal, dryRunInput, routeBody } from "../mcp-write";
import { AdminTicketService } from "../../admin/content/admin-ticket.service";
import { TicketProductWriteDto } from "../../admin/content/dto/ticket-product.dto";
import { TRIP_REGION_SLUGS } from "../../trip-regions/regions";

/** A product body this long is no edit a person asked for. */
const MAX_PRODUCT_CHARS = 50_000;

export const upsertTicketProductTool = defineMcpTool({
  name: "upsert_ticket_product",
  title: "Create or change a ticket product",
  description:
    "Writes one product of a trip region's ticket catalog as the admin's ticket editor does. A " +
    "slug the region has is changed: only the fields in product change, null clears an optional " +
    "one (description, notes, until, suggestRank, windowDays, allowance, onSale, hopSets, caps, " +
    "blockouts, sources …), and the merged product is checked whole. A new slug is created from " +
    "product, which then needs every required field (name, resort, markets, parks, days, " +
    "windowTable, anchor, hopping, links with one official, confidence). Either way the product " +
    "is planned on a sample trip, and every problem comes back by its field path. A dry run (the " +
    "default) answers the check and that sample plan without saving; with dryRun false the write " +
    "is audited under this account with the whole product before and after, and the region's " +
    "catalog is published. Read the product with list_ticket_products first; e.g. a rename is " +
    'product { "name": "…" }. active true brings an archived product back.',
  minRole: "editor",
  writes: "change",
  input: {
    region: z.enum(TRIP_REGION_SLUGS).describe("The trip region"),
    slug: z
      .string()
      .min(1)
      .max(60)
      .describe(
        "The product's slug: an existing one to change, a new kebab-case one to create",
      ),
    product: z
      .record(z.string(), z.unknown())
      .describe(
        "The fields to write, in the shape list_ticket_products answers for one product, without slug, id, shopUrl and the stamps",
      ),
    dryRun: dryRunInput("whether the product passes and its sample plan"),
  },
  async handler({ region, slug, product, dryRun }, ctx) {
    if (JSON.stringify(product).length > MAX_PRODUCT_CHARS) {
      throw new McpToolError(
        `product is longer than ${MAX_PRODUCT_CHARS} characters`,
      );
    }
    if (product.slug !== undefined && product.slug !== slug) {
      throw new McpToolError(
        "product.slug differs from slug; a product's slug cannot change",
      );
    }
    const tickets = ctx.get(AdminTicketService);
    const exists = await tickets.get(region, slug).then(
      () => true,
      (error: unknown) => {
        if (error instanceof NotFoundException) return false;
        throw error;
      },
    );
    const body = await routeBody(
      TicketProductWriteDto,
      exists ? product : { ...product, slug },
    );
    const action = exists ? "update" : "create";

    if (dryRun) {
      const check = await asToolRefusal(() =>
        tickets.validate(region, body, exists ? slug : undefined),
      );
      return {
        dryRun: true,
        action,
        ok: check.ok,
        problems: check.problems,
        samplePlan: check.dryRun ?? null,
      };
    }
    const saved = await asToolRefusal(() =>
      exists
        ? tickets.update(region, slug, body, ctx.admin)
        : tickets.create(region, body, ctx.admin),
    );
    return { dryRun: false, action, product: saved };
  },
});

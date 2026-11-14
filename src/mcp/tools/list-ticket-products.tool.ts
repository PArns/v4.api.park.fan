import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { asToolRefusal } from "../mcp-write";
import { AdminTicketService } from "../../admin/content/admin-ticket.service";
import { TRIP_REGION_SLUGS } from "./trip-season.inputs";

export const listTicketProductsTool = defineMcpTool({
  name: "list_ticket_products",
  title: "List a region's ticket products",
  description:
    "A trip region's ticket catalog as the admin's ticket editor reads it, archived products " +
    "included, in the catalog's order: per product its slug (the id the plan names), name, " +
    "resort, parks, markets, days, hopping, confidence, active, sortOrder, suggestRank and when " +
    "and by whom it was last written, and the catalog's version. With slug, that one product in " +
    "full, every rule, link and source, in the shape upsert_ticket_product writes.",
  input: {
    region: z.enum(TRIP_REGION_SLUGS).describe("The trip region"),
    slug: z
      .string()
      .max(60)
      .optional()
      .describe("One product's slug, for the whole product"),
  },
  async handler({ region, slug }, ctx) {
    const tickets = ctx.get(AdminTicketService);
    if (slug) {
      return { product: await asToolRefusal(() => tickets.get(region, slug)) };
    }
    const catalog = await asToolRefusal(() => tickets.list(region));
    return {
      region: catalog.region,
      version: catalog.version,
      products: catalog.tickets.map((product) => ({
        slug: product.slug,
        name: product.name,
        resort: product.resort,
        parks: product.parks,
        markets: product.markets,
        days: product.days,
        hopping: product.hopping,
        confidence: product.confidence,
        active: product.active,
        sortOrder: product.sortOrder,
        suggestRank: product.suggestRank,
        updatedAt: product.updatedAt,
        updatedBy: product.updatedBy ?? null,
      })),
    };
  },
});

import { z } from "zod";
import { defineMcpTool } from "../mcp-tool";
import { asToolRefusal, confirmInput } from "../mcp-write";
import { AdminTicketService } from "../../admin/content/admin-ticket.service";
import { TRIP_REGION_SLUGS } from "./trip-season.inputs";

export const archiveTicketProductTool = defineMcpTool({
  name: "archive_ticket_product",
  title: "Archive a ticket product",
  description:
    "The ticket catalog's delete, as the admin's archive: the product leaves the region's " +
    "catalog and is never suggested again, but stays readable for the plans and shared links " +
    "that name it (a product is never deleted while plans may name it). Audited under this " +
    "account with the product before and after, and the catalog is published. " +
    "upsert_ticket_product with active true brings it back.",
  minRole: "editor",
  writes: "delete",
  input: {
    region: z.enum(TRIP_REGION_SLUGS).describe("The trip region"),
    slug: z.string().min(1).max(60).describe("The product's slug"),
    reason: z.string().max(500).optional().describe("Why, for the audit log"),
    confirm: confirmInput("takes the product off the catalog"),
  },
  async handler({ region, slug, reason }, ctx) {
    const product = await asToolRefusal(() =>
      ctx.get(AdminTicketService).archive(region, slug, reason, ctx.admin),
    );
    return {
      archived: {
        slug: product.slug,
        name: product.name,
        active: product.active,
      },
    };
  },
});

import { DataSource } from "typeorm";
import { McpToolError, type McpToolContext } from "../mcp-tool";
import { ParksService } from "../../parks/parks.service";
import { Park } from "../../parks/entities/park.entity";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";

/** A slug that several parks share, handed back so the model can pick an id. */
export interface AmbiguousPark {
  ambiguous: true;
  hint: string;
  parks: Array<{ id: string; name: string; city: string; country: string }>;
}

/**
 * Turns a park id or an unambiguous slug into the park. A slug two parks share
 * returns the candidates instead; an unknown reference throws for the model.
 */
export async function resolvePark(
  ctx: McpToolContext,
  reference: string,
): Promise<Park | AmbiguousPark> {
  const parks = ctx.get(ParksService);
  const ref = reference.trim().toLowerCase();

  let park: Park | null;
  if (isCanonicalUuid(ref)) {
    park = await parks.findById(ref);
  } else {
    const matches = await ctx
      .get(DataSource)
      .getRepository(Park)
      .find({ where: { slug: ref }, take: 5 });
    if (matches.length > 1) {
      return {
        ambiguous: true,
        hint: "Several parks have this slug; call again with one id",
        parks: matches.map((match) => ({
          id: match.id,
          name: match.name,
          city: match.city,
          country: match.country,
        })),
      };
    }
    park = matches[0] ? await parks.findById(matches[0].id) : null;
  }
  if (!park) throw new McpToolError(`No park "${reference}"; try find_parks`);
  return park;
}

/** True for the candidates answer of {@link resolvePark}. */
export function isAmbiguous(
  found: Park | AmbiguousPark,
): found is AmbiguousPark {
  return (found as AmbiguousPark).ambiguous === true;
}

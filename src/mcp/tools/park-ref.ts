import { DataSource } from "typeorm";
import { McpToolError, type McpToolContext } from "../mcp-tool";
import { ParksService } from "../../parks/parks.service";
import { Park } from "../../parks/entities/park.entity";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import { SearchService } from "../../search/search.service";
import { SearchQueryDto } from "../../search/dto/search-query.dto";

/** A slug that several parks share, handed back so the model can pick an id. */
export interface AmbiguousPark {
  ambiguous: true;
  hint: string;
  parks: Array<{ id: string; name: string; city: string; country: string }>;
}

/** Lower case, letters and digits only: "Europa Park" and "Europa-Park" meet. */
export function nameKey(name: string): string {
  return name
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

/**
 * Turns a park id, an unambiguous slug or a name into the park. A slug two parks
 * share, or a name the site's search matches more than once, returns the
 * candidates instead; an unknown reference throws for the model. A name saves
 * the search step a model otherwise spends on finding the id.
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
    if (!park) {
      const byName = await parkByName(ctx, reference);
      if (byName && "ambiguous" in byName) return byName;
      park = byName;
    }
  }
  if (!park) throw new McpToolError(`No park "${reference}"; try find_parks`);
  ctx.onPark?.(park.timezone);
  return park;
}

/** True for the candidates answer of {@link resolvePark}. */
export function isAmbiguous(
  found: Park | AmbiguousPark,
): found is AmbiguousPark {
  return (found as AmbiguousPark).ambiguous === true;
}

/**
 * The park the site's search finds for a name: the only match, or the one whose
 * name is the reference itself; several equal candidates come back to pick from.
 */
async function parkByName(
  ctx: McpToolContext,
  reference: string,
): Promise<Park | AmbiguousPark | null> {
  const found = await ctx
    .get(SearchService)
    .search(
      Object.assign(new SearchQueryDto(), {
        q: reference.trim(),
        type: ["park"],
        limit: 5,
      }),
    )
    .catch(() => null);
  const hits = (found?.results ?? []).filter((item) => item.type === "park");
  if (hits.length === 0) return null;
  const key = nameKey(reference);
  // A park is also called by its name and city: "Disneyland Paris" is the Disneyland Park in
  // Paris, never the one in Anaheim the search may list first.
  const names = (hit: { name: string; city?: string | null }) => {
    const bare = hit.name.replace(/\s*park$/i, "");
    return [
      hit.name,
      `${hit.name} ${hit.city ?? ""}`,
      `${bare} ${hit.city ?? ""}`,
    ];
  };
  const exact = hits.filter((hit) =>
    names(hit).some((name) => nameKey(name) === key),
  );
  const pick = hits.length === 1 ? hits : exact;
  if (pick.length === 1) return ctx.get(ParksService).findById(pick[0].id);
  return {
    ambiguous: true,
    hint: "Several parks match this name; call again with one id",
    parks: (exact.length > 1 ? exact : hits).map((hit) => ({
      id: hit.id,
      name: hit.name,
      city: hit.city ?? "",
      country: hit.country ?? "",
    })),
  };
}

import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { AttractionSlugAlias } from "../../attractions/entities/attraction-slug-alias.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParkSlugAlias } from "../../parks/entities/park-slug-alias.entity";
import {
  captureParkPath,
  samePath,
  type ParkGeoPath,
} from "../../parks/services/park-rename.service";
import { isCanonicalUuid } from "../../common/utils/uuid-param.util";
import {
  frontendAttractionPath,
  frontendParkPath,
} from "../../common/utils/frontend-url.util";
import { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminCurationService } from "./admin-curation.service";
import type { RedirectKind } from "./dto/redirect.dto";

/** The site's locales; a not-found path carries one as its first segment. */
const LOCALES = new Set(["en", "de", "nl", "fr", "es", "it"]);
const SLUG = /^[a-z0-9]+(?:-[a-z0-9]+)*$/;
/**
 * Pages of the frontend that sit where a ride slug would
 * (`app/[locale]/parks/.../[park]/<segment>`); an alias there never answers.
 */
const PARK_SUBPAGES = new Set([
  "wait-time-calendar",
  "with-kids",
  "average-wait-times",
]);

export interface RedirectPath extends ParkGeoPath {
  /** The ride segment, null for a park path. */
  rideSlug: string | null;
}

/**
 * A frontend path as the not-found report lists it, locale optional:
 * `/de/parks/<continent>/<country>/<city>/<park>[/<ride>]`. Null for any
 * other shape, a query, a fragment or a segment no slug can be.
 */
export function parseRedirectPath(fromPath: string): RedirectPath | null {
  if (!fromPath.startsWith("/")) return null;
  const segments = fromPath.replace(/\/$/, "").split("/").slice(1);
  if (LOCALES.has(segments[0])) segments.shift();
  if (segments[0] !== "parks") return null;
  const slugs = segments.slice(1);
  if (slugs.length < 4 || slugs.length > 5) return null;
  if (!slugs.every((slug) => SLUG.test(slug))) return null;
  const [continentSlug, countrySlug, citySlug, slug, rideSlug] = slugs;
  return {
    continentSlug,
    countrySlug,
    citySlug,
    slug,
    rideSlug: rideSlug ?? null,
  };
}

export interface RedirectView {
  id: string;
  kind: RedirectKind;
  /** The alias, without locale. */
  fromPath: string;
  /** Where it answers today, without locale. */
  toPath: string;
  targetId: string;
  createdBy: string | null;
  createdAt: Date;
}

/**
 * Hand-made slug aliases (PAR-1233): a dead park or ride URL from the
 * not-found report answered with a 301 to a page that exists. Writes the same
 * tables the rename, the merges and the sync write, so the park and ride
 * lookups resolve them with no change of their own; `createdBy` tells a
 * hand-made row from theirs, and only a hand-made one may be deleted here.
 *
 * Every write is published outside-in through `AdminCurationService.publish`
 * (the frontend holds the old 404 under the `parks`/`attractions` tags) and
 * audited as `redirect.create` / `redirect.delete`.
 */
@Injectable()
export class AdminRedirectService {
  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    @InjectRepository(ParkSlugAlias)
    private readonly parkAliases: Repository<ParkSlugAlias>,
    @InjectRepository(AttractionSlugAlias)
    private readonly attractionAliases: Repository<AttractionSlugAlias>,
    private readonly curation: AdminCurationService,
    private readonly audit: AdminAuditService,
  ) {}

  async create(
    fromPath: string,
    target: { kind: RedirectKind; id: string },
    admin: AdminPrincipal,
  ): Promise<{ created: boolean; redirect: RedirectView }> {
    if (!isCanonicalUuid(target.id)) {
      throw new BadRequestException("target.id must be a canonical uuid");
    }
    const path = parseRedirectPath(fromPath);
    if (!path) {
      throw new BadRequestException(
        "fromPath must be /[locale/]parks/<continent>/<country>/<city>/<park>[/<ride>]",
      );
    }
    return target.kind === "park"
      ? this.createParkAlias(path, target.id, admin)
      : this.createAttractionAlias(path, target.id, admin);
  }

  private async createParkAlias(
    path: RedirectPath,
    parkId: string,
    admin: AdminPrincipal,
  ): Promise<{ created: boolean; redirect: RedirectView }> {
    if (path.rideSlug) {
      throw new BadRequestException("A park redirect takes a park path");
    }
    const geo: ParkGeoPath = {
      continentSlug: path.continentSlug,
      countrySlug: path.countrySlug,
      citySlug: path.citySlug,
      slug: path.slug,
    };
    const park = await this.parks.findOne({ where: { id: parkId } });
    if (!park) throw new NotFoundException(`Park not found: ${parkId}`);
    if (park.retiredAt) {
      throw new BadRequestException(`Park ${park.name} is retired`);
    }
    const current = captureParkPath(park);
    if (!current) {
      throw new BadRequestException(`Park ${park.name} has no complete path`);
    }
    if (samePath(current, geo)) {
      throw new ConflictException("fromPath is the park's own page");
    }
    if (await this.parks.exists({ where: geo })) {
      throw new ConflictException("fromPath is a live park page");
    }
    const existing = await this.parkAliases.findOne({ where: geo });
    if (existing) {
      if (existing.parkId !== park.id) {
        throw new ConflictException(
          "fromPath already redirects to another park",
        );
      }
      return { created: false, redirect: this.parkView(existing, park) };
    }

    const alias = await this.insert(() =>
      this.parkAliases.save(
        this.parkAliases.create({
          parkId: park.id,
          ...geo,
          createdBy: admin.email,
        }),
      ),
    );
    const view = this.parkView(alias, park);
    await this.audit.record({
      actor: admin,
      action: "redirect.create",
      entityType: "park",
      entityId: park.id,
      entityLabel: park.name,
      after: {
        aliasId: alias.id,
        fromPath: view.fromPath,
        toPath: view.toPath,
      },
    });
    await this.curation.publish(park.id, []);
    return { created: true, redirect: view };
  }

  private async createAttractionAlias(
    path: RedirectPath,
    attractionId: string,
    admin: AdminPrincipal,
  ): Promise<{ created: boolean; redirect: RedirectView }> {
    const rideSlug = path.rideSlug;
    if (!rideSlug) {
      throw new BadRequestException("A ride redirect takes a ride path");
    }
    const attraction = await this.attractions.findOne({
      where: { id: attractionId },
      relations: { park: true },
    });
    if (!attraction) {
      throw new NotFoundException(`Attraction not found: ${attractionId}`);
    }
    if (attraction.retiredAt) {
      throw new BadRequestException(`Attraction ${attraction.name} is retired`);
    }
    const park = attraction.park;
    const current = park ? captureParkPath(park) : null;
    if (!current) {
      throw new BadRequestException(
        `The park of ${attraction.name} has no complete path`,
      );
    }
    // The lookup reads an alias through the ride's current park, so an alias
    // under any other park path would never answer.
    if (!samePath(current, path)) {
      throw new BadRequestException(
        `fromPath must be inside the ride's park, ${frontendParkPath(park)}`,
      );
    }
    if (PARK_SUBPAGES.has(rideSlug)) {
      throw new ConflictException("fromPath is a live park page");
    }
    if (
      await this.attractions.exists({
        where: { parkId: park.id, slug: rideSlug },
      })
    ) {
      throw new ConflictException("fromPath is a live ride page");
    }
    const existing = (
      await this.attractionAliases.find({
        where: { slug: rideSlug },
        relations: { attraction: true },
      })
    ).filter((alias) => alias.attraction?.parkId === park.id);
    const own = existing.find((alias) => alias.attractionId === attraction.id);
    if (own) {
      return {
        created: false,
        redirect: this.attractionView(own, attraction, park),
      };
    }
    if (existing.length > 0) {
      throw new ConflictException("fromPath already redirects to another ride");
    }

    const alias = await this.insert(() =>
      this.attractionAliases.save(
        this.attractionAliases.create({
          attractionId: attraction.id,
          slug: rideSlug,
          createdBy: admin.email,
        }),
      ),
    );
    const view = this.attractionView(alias, attraction, park);
    await this.audit.record({
      actor: admin,
      action: "redirect.create",
      entityType: "attraction",
      entityId: attraction.id,
      entityLabel: attraction.name,
      after: {
        aliasId: alias.id,
        fromPath: view.fromPath,
        toPath: view.toPath,
      },
    });
    await this.curation.publish(park.id, [attraction.id]);
    return { created: true, redirect: view };
  }

  /**
   * Undo a hand-made alias, found by the path it answers. A row the sync, a
   * merge or a rename recorded is refused: it is the only trace of a URL that
   * was live, and nothing would write it again.
   */
  async remove(fromPath: string, admin: AdminPrincipal): Promise<void> {
    const path = parseRedirectPath(fromPath);
    if (!path) {
      throw new BadRequestException(
        "fromPath must be /[locale/]parks/<continent>/<country>/<city>/<park>[/<ride>]",
      );
    }
    const geo: ParkGeoPath = {
      continentSlug: path.continentSlug,
      countrySlug: path.countrySlug,
      citySlug: path.citySlug,
      slug: path.slug,
    };

    if (!path.rideSlug) {
      const alias = await this.parkAliases.findOne({
        where: geo,
        relations: { park: true },
      });
      if (!alias) throw new NotFoundException("No redirect at fromPath");
      if (!alias.createdBy) {
        throw new ConflictException(
          "This redirect was recorded by a rename or merge, not by hand",
        );
      }
      await this.parkAliases.delete({ id: alias.id });
      await this.audit.record({
        actor: admin,
        action: "redirect.delete",
        entityType: "park",
        entityId: alias.parkId,
        entityLabel: alias.park?.name ?? null,
        before: {
          aliasId: alias.id,
          fromPath: frontendParkPath(alias),
          createdBy: alias.createdBy,
        },
      });
      await this.curation.publish(alias.parkId, []);
      return;
    }

    const park = await this.parks.findOne({ where: geo });
    const aliases = park
      ? (
          await this.attractionAliases.find({
            where: { slug: path.rideSlug },
            relations: { attraction: true },
          })
        ).filter((alias) => alias.attraction?.parkId === park.id)
      : [];
    if (!park || aliases.length === 0) {
      throw new NotFoundException("No redirect at fromPath");
    }
    const alias = aliases.find((row) => row.createdBy);
    if (!alias) {
      throw new ConflictException(
        "This redirect was recorded by a merge or the sync, not by hand",
      );
    }
    await this.attractionAliases.delete({ id: alias.id });
    await this.audit.record({
      actor: admin,
      action: "redirect.delete",
      entityType: "attraction",
      entityId: alias.attractionId,
      entityLabel: alias.attraction.name,
      before: {
        aliasId: alias.id,
        fromPath: frontendAttractionPath(park, alias),
        createdBy: alias.createdBy,
      },
    });
    await this.curation.publish(park.id, [alias.attractionId]);
  }

  /**
   * A unique index that wins a race the checks above lost turns into a 409.
   * Only park aliases have one on the path (continent/country/city/slug).
   * Ride aliases are unique per (attractionId, slug), not per (park, slug):
   * two editors pointing the same dead ride path at two rides at the same
   * moment both succeed, and the lookup answers with the newer row.
   * shortcut: no lock for that race with a handful of admins; add a
   * per-(park, slug) advisory lock if redirects are ever written in bulk.
   */
  private async insert<T>(write: () => Promise<T>): Promise<T> {
    try {
      return await write();
    } catch (error) {
      if ((error as { code?: string }).code === "23505") {
        throw new ConflictException("fromPath was taken meanwhile");
      }
      throw error;
    }
  }

  private parkView(alias: ParkSlugAlias, park: Park): RedirectView {
    return {
      id: alias.id,
      kind: "park",
      fromPath: frontendParkPath(alias) as string,
      toPath: frontendParkPath(park) as string,
      targetId: park.id,
      createdBy: alias.createdBy,
      createdAt: alias.createdAt,
    };
  }

  private attractionView(
    alias: AttractionSlugAlias,
    attraction: Attraction,
    park: Park,
  ): RedirectView {
    return {
      id: alias.id,
      kind: "attraction",
      fromPath: frontendAttractionPath(park, alias) as string,
      toPath: frontendAttractionPath(park, attraction) as string,
      targetId: attraction.id,
      createdBy: alias.createdBy,
      createdAt: alias.createdAt,
    };
  }
}

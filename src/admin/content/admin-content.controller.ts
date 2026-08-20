import {
  BadRequestException,
  Body,
  Controller,
  Delete,
  Get,
  HttpCode,
  HttpStatus,
  Param,
  Patch,
  Post,
  Put,
  Query,
  UseGuards,
  UseInterceptors,
} from "@nestjs/common";
import { ApiOperation, ApiSecurity, ApiTags } from "@nestjs/swagger";
import { InjectRepository } from "@nestjs/typeorm";
import { Brackets, Repository } from "typeorm";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { Park } from "../../parks/entities/park.entity";
import { ParkSeason } from "../../parks/entities/park-season.entity";
import { resolveCuratedFacts } from "../../attractions/utils/curated-attraction-facts.util";
import { resolveCuratedPark } from "../../parks/utils/curated-park-facts.util";
import { AdminAuthGuard } from "../auth/admin-auth.guard";
import { AdminMinRole, CurrentAdmin } from "../auth/admin-auth.decorators";
import {
  AdminAuditInterceptor,
  SelfAudited,
} from "../auth/admin-audit.interceptor";
import { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminCurationService } from "./admin-curation.service";
import { AdminRideProfileService } from "./admin-ride-profile.service";
import { ParkSeasonService } from "../../parks/services/park-season.service";
import {
  attractionFieldViews,
  parkFieldViews,
  ATTRACTION_CURATED_FIELDS,
  PARK_CURATED_FIELDS,
} from "./curated-field.spec-list";
import {
  CurationPatchDto,
  RideProfileWriteDto,
  SeasonPatchDto,
  SeasonWriteDto,
} from "./dto/curation.dto";

/**
 * The editing surface: parks, rides, their curated fields, their seasons.
 *
 * Separate from `AdminController` because the two answer to different people.
 * That one triggers jobs and repairs data — an operator's console. This one is
 * where somebody who knows that Winni Splash has no minimum height writes it
 * down, and it is shaped for that: every read hands back the curated fields as
 * *descriptors* (upstream value, curated value, effective value, whether they
 * disagree) rather than a flat object, so the editor renders whatever the
 * backend says is curatable and adding a column here needs no frontend change.
 *
 * Every write goes through `AdminCurationService`, which owns the part that is
 * easy to get wrong: writing only the curated cell, evicting our caches BEFORE
 * telling the frontend, and scheduling the second sweep for after the edge
 * window. And every write leaves an audit row carrying the reason and the URL
 * it was established from.
 */
@ApiTags("admin")
@ApiSecurity("admin-auth")
@Controller("admin/content")
@UseGuards(AdminAuthGuard)
@UseInterceptors(AdminAuditInterceptor)
export class AdminContentController {
  constructor(
    @InjectRepository(Park) private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
    private readonly curation: AdminCurationService,
    private readonly rideProfiles: AdminRideProfileService,
    private readonly seasons: ParkSeasonService,
    private readonly audit: AdminAuditService,
  ) {}

  // ── field descriptors ─────────────────────────────────────────────────────

  @Get("fields")
  @ApiOperation({
    summary: "Which fields are curatable, and how to render them",
    description:
      "The editor is generated from this. Adding a curated column to the " +
      "backend makes it appear in the admin with no frontend change.",
  })
  fields() {
    return {
      attraction: ATTRACTION_CURATED_FIELDS,
      park: PARK_CURATED_FIELDS,
    };
  }

  @Get("resolve")
  @ApiOperation({
    summary: "Find a park or ride by slug",
    description:
      "The media database and the blog identify things by slug; this editor " +
      "identifies them by id. Without a translation between the two, a photo " +
      "of Taron cannot link to the ride it is of.",
  })
  async resolve(
    @Query("parkSlug") parkSlug?: string,
    @Query("rideSlug") rideSlug?: string,
    @Query("citySlug") citySlug?: string,
  ) {
    if (!parkSlug) throw new BadRequestException("parkSlug is required");

    // Park slugs are not globally unique — `disneyland-park` exists in Anaheim
    // and in Paris — so a citySlug disambiguates when the caller has one, and
    // an ambiguous answer is reported rather than guessed.
    const parks = await this.parks.find({
      where: citySlug ? { slug: parkSlug, citySlug } : { slug: parkSlug },
      select: {
        id: true,
        name: true,
        slug: true,
        citySlug: true,
        curatedName: true,
      },
    });

    if (parks.length === 0)
      return { park: null, attraction: null, ambiguous: false };
    if (parks.length > 1 && !citySlug) {
      return {
        park: null,
        attraction: null,
        ambiguous: true,
        candidates: parks.map((park) => ({
          id: park.id,
          name: resolveCuratedPark(park).name,
          citySlug: park.citySlug,
        })),
      };
    }

    const park = parks[0];
    const attraction = rideSlug
      ? await this.attractions.findOne({
          where: { parkId: park.id, slug: rideSlug },
          select: { id: true, name: true, slug: true, curatedName: true },
        })
      : null;

    return {
      park: {
        id: park.id,
        name: resolveCuratedPark(park).name,
        slug: park.slug,
      },
      attraction: attraction
        ? {
            id: attraction.id,
            name: resolveCuratedFacts(attraction).name,
            slug: attraction.slug,
          }
        : null,
      ambiguous: false,
    };
  }

  // ── parks ─────────────────────────────────────────────────────────────────

  @Get("parks")
  @ApiOperation({ summary: "Parks, with what has been curated on each" })
  async listParks(
    @Query("q") q?: string,
    @Query("country") country?: string,
    @Query("curated") curated?: "only" | "none",
    @Query("limit") limit?: string,
    @Query("offset") offset?: string,
  ) {
    const qb = this.parks
      .createQueryBuilder("park")
      .orderBy("park.name", "ASC")
      .take(Math.min(Number(limit) || 200, 500))
      .skip(Number(offset) || 0);

    if (q && q.trim().length > 0) {
      const like = `%${q.trim()}%`;
      // Both names, because an editor searching for a park they renamed is
      // searching for the name they typed, not the one upstream still uses.
      qb.andWhere(
        new Brackets((w) => {
          w.where("park.name ILIKE :like", { like })
            .orWhere("park.curated_name ILIKE :like", { like })
            .orWhere("park.city ILIKE :like", { like })
            .orWhere("park.slug ILIKE :like", { like });
        }),
      );
    }
    if (country) {
      qb.andWhere("park.countryCode = :country", {
        country: country.toUpperCase(),
      });
    }
    if (curated === "only") {
      qb.andWhere(
        new Brackets((w) => {
          w.where("park.curated_name IS NOT NULL")
            .orWhere("park.curated_park_type IS NOT NULL")
            .orWhere("park.curated_no_wait_times_reason IS NOT NULL")
            .orWhere("park.curation_note IS NOT NULL");
        }),
      );
    }
    if (curated === "none") {
      // The exact inverse of the branch above, `curation_note` included: an
      // editor working the "nothing curated yet" list should not keep being
      // handed a park somebody has already written notes on.
      qb.andWhere("park.curated_name IS NULL")
        .andWhere("park.curated_park_type IS NULL")
        .andWhere("park.curated_no_wait_times_reason IS NULL")
        .andWhere("park.curation_note IS NULL");
    }

    const [rows, total] = await qb.getManyAndCount();
    const parkIds = rows.map((p) => p.id);
    const [attractionCounts, seasonCounts] = await Promise.all([
      this.attractionCounts(parkIds),
      this.seasons.countsByPark(parkIds),
    ]);

    return {
      total,
      parks: rows.map((park) => {
        const resolved = resolveCuratedPark(park);
        return {
          id: park.id,
          name: resolved.name,
          upstreamName: park.name,
          slug: park.slug,
          path: `${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`,
          city: park.city ?? null,
          country: park.country ?? null,
          countryCode: park.countryCode ?? null,
          timezone: park.timezone,
          // The list feeds a map view as well as a table. `decimal` columns
          // come back as strings from the driver, and a string latitude puts
          // every marker at 0,0 without erroring.
          latitude: toNumber(park.latitude),
          longitude: toNumber(park.longitude),
          parkType: resolved.parkType,
          noWaitTimesReason: resolved.noWaitTimesReason,
          attractionCount: attractionCounts.get(park.id) ?? 0,
          seasonCount: seasonCounts.get(park.id) ?? 0,
          curatedFieldCount: parkFieldViews(park).filter((f) => f.overridden)
            .length,
          curationNote: park.curationNote ?? null,
          updatedAt: park.updatedAt,
        };
      }),
    };
  }

  @Get("parks/:id")
  @ApiOperation({ summary: "One park, its curated fields and its seasons" })
  async getPark(@Param("id") id: string) {
    const park = await this.curation.findPark(id);
    const [seasons, history, attractionCount] = await Promise.all([
      this.seasons.list({ parkId: id, limit: 200 }),
      this.audit.list({ entityType: "park", entityId: id, limit: 25 }),
      this.attractionCounts([id]),
    ]);
    const resolved = resolveCuratedPark(park);

    return {
      id: park.id,
      name: resolved.name,
      upstreamName: park.name,
      slug: park.slug,
      path: `${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`,
      url: `https://park.fan/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`,
      city: park.city ?? null,
      country: park.country ?? null,
      countryCode: park.countryCode ?? null,
      region: park.region ?? null,
      timezone: park.timezone,
      latitude: toNumber(park.latitude),
      longitude: toNumber(park.longitude),
      externalId: park.externalId,
      dataSources: park.dataSources ?? [],
      attractionCount: attractionCount.get(park.id) ?? 0,
      fields: parkFieldViews(park),
      seasons: seasons.seasons,
      history: history.entries,
      updatedAt: park.updatedAt,
    };
  }

  @Patch("parks/:id")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({
    summary: "Write a park's curated fields",
    description:
      "`null` clears a correction and accepts upstream again. Only the curated " +
      "cell is written — never the sync-owned one, which the next run would " +
      "overwrite anyway.",
  })
  async patchPark(
    @Param("id") id: string,
    @Body() body: CurationPatchDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    const result = await this.curation.curatePark(
      id,
      { fields: body.fields, reason: body.reason, sourceUrl: body.sourceUrl },
      admin,
    );
    return {
      changed: result.changed,
      auditId: result.auditId,
      fields: parkFieldViews(result.entity),
    };
  }

  // ── attractions ───────────────────────────────────────────────────────────

  @Get("parks/:id/attractions")
  @ApiOperation({
    summary: "A park's rides, with what has been curated on each",
  })
  async listAttractions(
    @Param("id") parkId: string,
    @Query("q") q?: string,
    @Query("includeRetired") includeRetired?: string,
  ) {
    const qb = this.attractions
      .createQueryBuilder("attraction")
      .where("attraction.parkId = :parkId", { parkId })
      .orderBy("attraction.name", "ASC");

    if (includeRetired !== "true") {
      qb.andWhere("attraction.retired_at IS NULL");
    }
    if (q && q.trim().length > 0) {
      const like = `%${q.trim()}%`;
      qb.andWhere(
        new Brackets((w) => {
          w.where("attraction.name ILIKE :like", { like }).orWhere(
            "attraction.curated_name ILIKE :like",
            { like },
          );
        }),
      );
    }

    const rows = await qb.getMany();
    const withProfile = await this.rideProfiles.findIdsWithProfile(
      rows.map((row) => row.id),
    );

    return {
      total: rows.length,
      attractions: rows.map((attraction) => {
        const resolved = resolveCuratedFacts(attraction);
        return {
          id: attraction.id,
          name: resolved.name,
          upstreamName: attraction.name,
          slug: attraction.slug,
          land: resolved.landName,
          attractionType: resolved.attractionType,
          minimumHeight: resolved.minimumHeight,
          minimumHeightUnit: resolved.minimumHeightUnit,
          isSeasonal: resolved.isSeasonal,
          seasonMonths: resolved.seasonMonths,
          seasonalityCurated: resolved.seasonalityCurated,
          retiredAt: attraction.retiredAt,
          hasRideProfile: withProfile.has(attraction.id),
          curatedFieldCount: attractionFieldViews(attraction).filter(
            (f) => f.overridden,
          ).length,
          updatedAt: attraction.updatedAt,
        };
      }),
    };
  }

  @Get("attractions")
  @ApiOperation({
    summary: "Find rides across every park",
    description:
      "The command palette's other half. Matches the curated name as well as " +
      "the synced one, so a ride renamed last week is findable under the name " +
      "it was given — the public search index only learns that on its next " +
      "rebuild.",
  })
  async searchAttractions(
    @Query("q") q?: string,
    @Query("limit") limit?: string,
  ) {
    const term = q?.trim() ?? "";
    if (term.length < 2) return { total: 0, attractions: [] };

    const like = `%${term}%`;
    const [rows, total] = await this.attractions
      .createQueryBuilder("attraction")
      .leftJoinAndSelect("attraction.park", "park")
      .where("attraction.retired_at IS NULL")
      .andWhere(
        new Brackets((w) => {
          w.where("attraction.name ILIKE :like", { like }).orWhere(
            "attraction.curated_name ILIKE :like",
            { like },
          );
        }),
      )
      // Exact matches first: "Taron" must not be buried under every ride whose
      // name contains it.
      .orderBy(
        "CASE WHEN LOWER(COALESCE(attraction.curated_name, attraction.name)) = LOWER(:exact) THEN 0 ELSE 1 END",
        "ASC",
      )
      .addOrderBy("attraction.name", "ASC")
      .setParameter("exact", term)
      .take(Math.min(Number(limit) || 12, 50))
      .getManyAndCount();

    return {
      total,
      attractions: rows.map((attraction) => ({
        id: attraction.id,
        name: resolveCuratedFacts(attraction).name,
        upstreamName: attraction.name,
        slug: attraction.slug,
        park: attraction.park
          ? {
              id: attraction.park.id,
              name: resolveCuratedPark(attraction.park).name,
            }
          : null,
      })),
    };
  }

  @Get("attractions/:id")
  @ApiOperation({
    summary: "One ride, its curated fields, profile and history",
  })
  async getAttraction(@Param("id") id: string) {
    const attraction = await this.curation.findAttraction(id);
    const [profile, history] = await Promise.all([
      this.rideProfiles.find(id),
      this.audit.list({ entityType: "attraction", entityId: id, limit: 25 }),
    ]);
    const park = attraction.park;
    const resolved = resolveCuratedFacts(attraction);

    return {
      id: attraction.id,
      name: resolved.name,
      upstreamName: attraction.name,
      slug: attraction.slug,
      externalId: attraction.externalId,
      park: park
        ? {
            id: park.id,
            name: resolveCuratedPark(park).name,
            slug: park.slug,
            path: `${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}`,
          }
        : null,
      url: park
        ? `https://park.fan/parks/${park.continentSlug}/${park.countrySlug}/${park.citySlug}/${park.slug}/${attraction.slug}`
        : null,
      latitude: toNumber(attraction.latitude),
      longitude: toNumber(attraction.longitude),
      retiredAt: attraction.retiredAt,
      retiredReason: attraction.retiredReason,
      fields: attractionFieldViews(attraction),
      rideProfile: profile,
      history: history.entries,
      updatedAt: attraction.updatedAt,
    };
  }

  @Patch("attractions/:id")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({ summary: "Write a ride's curated fields" })
  async patchAttraction(
    @Param("id") id: string,
    @Body() body: CurationPatchDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    const result = await this.curation.curateAttraction(
      id,
      { fields: body.fields, reason: body.reason, sourceUrl: body.sourceUrl },
      admin,
    );
    return {
      changed: result.changed,
      auditId: result.auditId,
      fields: attractionFieldViews(result.entity),
    };
  }

  // ── ride profiles ─────────────────────────────────────────────────────────

  @Put("attractions/:id/ride-profile")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({
    summary: "Write a ride's glossary profile",
    description:
      "Term ids are checked against the glossary before the write, because an " +
      "id it does not define renders as nothing — the ride's layout simply " +
      "comes out shorter, with no error anywhere.",
  })
  async putRideProfile(
    @Param("id") id: string,
    @Body() body: RideProfileWriteDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ) {
    const attraction = await this.curation.findAttraction(id);
    const before = await this.rideProfiles.find(id);
    const { profile } = await this.rideProfiles.upsert(id, body);

    await this.audit.record({
      actor: admin,
      action: "ride-profile.write",
      entityType: "attraction",
      entityId: id,
      entityLabel: attraction.name,
      before: before
        ? {
            elements: before.elements,
            types: before.types,
            manufacturerName: before.manufacturerName,
            model: before.model,
            openedYear: before.openedYear,
            inversions: before.inversions,
            curatedStats: before.curatedStats,
          }
        : null,
      after: {
        elements: profile.elements,
        types: profile.types,
        manufacturerName: profile.manufacturerName,
        model: profile.model,
        openedYear: profile.openedYear,
        inversions: profile.inversions,
        curatedStats: profile.curatedStats,
      },
    });

    await this.curation.publish(attraction.parkId, [id]);
    return profile;
  }

  @Delete("attractions/:id/ride-profile")
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Remove a ride's glossary profile" })
  async deleteRideProfile(
    @Param("id") id: string,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<void> {
    const attraction = await this.curation.findAttraction(id);
    const before = await this.rideProfiles.find(id);
    const removed = await this.rideProfiles.remove(id);
    if (!removed) return;

    await this.audit.record({
      actor: admin,
      action: "ride-profile.delete",
      entityType: "attraction",
      entityId: id,
      entityLabel: attraction.name,
      before: before
        ? { elements: before.elements, types: before.types }
        : null,
    });
    await this.curation.publish(attraction.parkId, [id]);
  }

  // ── seasons ───────────────────────────────────────────────────────────────

  @Get("seasons")
  @ApiOperation({
    summary: "Seasons across every park",
    description:
      "The view that answers 'what is on file for Halloween 2026' — which was " +
      "previously only answerable by reading the blog post.",
  })
  async listSeasons(
    @Query("kind") kind?: string,
    @Query("status") status?: string,
    @Query("year") year?: string,
    @Query("current") current?: string,
    @Query("limit") limit?: string,
    @Query("offset") offset?: string,
  ) {
    const result = await this.seasons.list({
      kind: kind as never,
      status: status as never,
      year: year ? Number(year) : undefined,
      current: current === "true",
      limit: Number(limit) || undefined,
      offset: Number(offset) || undefined,
    });
    return {
      total: result.total,
      seasons: result.seasons.map((season) => ({
        ...season,
        park: season.park
          ? {
              id: season.park.id,
              name: resolveCuratedPark(season.park).name,
              slug: season.park.slug,
            }
          : null,
      })),
    };
  }

  @Post("parks/:id/seasons")
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.CREATED)
  @ApiOperation({ summary: "Add a season to a park" })
  async createSeason(
    @Param("id") parkId: string,
    @Body() body: SeasonWriteDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<ParkSeason> {
    const season = await this.seasons.create(parkId, body, admin.userId);
    await this.audit.record({
      actor: admin,
      action: "park.season.create",
      entityType: "park_season",
      entityId: season.id,
      entityLabel: `${season.kind} ${season.startDate}`,
      after: {
        kind: season.kind,
        startDate: season.startDate,
        endDate: season.endDate,
      },
      sourceUrl: season.sourceUrl,
      reason: season.note,
    });
    // A season changes the park page, not the ride cards, so only the park's
    // own caches need dropping.
    await this.curation.publish(parkId, []);
    return season;
  }

  @Patch("seasons/:id")
  @AdminMinRole("editor")
  @SelfAudited()
  @ApiOperation({ summary: "Edit a season" })
  async patchSeason(
    @Param("id") id: string,
    @Body() body: SeasonPatchDto,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<ParkSeason> {
    const before = await this.seasons.findOne(id);
    const season = await this.seasons.update(id, body, admin.userId);
    await this.audit.record({
      actor: admin,
      action: "park.season.update",
      entityType: "park_season",
      entityId: id,
      entityLabel: `${season.kind} ${season.startDate}`,
      before: {
        kind: before.kind,
        startDate: before.startDate,
        endDate: before.endDate,
        status: before.status,
        dates: before.dates,
      },
      after: {
        kind: season.kind,
        startDate: season.startDate,
        endDate: season.endDate,
        status: season.status,
        dates: season.dates,
      },
      sourceUrl: season.sourceUrl,
    });
    await this.curation.publish(season.parkId, []);
    return season;
  }

  @Delete("seasons/:id")
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.NO_CONTENT)
  @ApiOperation({ summary: "Delete a season" })
  async deleteSeason(
    @Param("id") id: string,
    @CurrentAdmin() admin: AdminPrincipal,
  ): Promise<void> {
    const season = await this.seasons.remove(id);
    await this.audit.record({
      actor: admin,
      action: "park.season.delete",
      entityType: "park_season",
      entityId: id,
      entityLabel: `${season.kind} ${season.startDate}`,
      before: {
        kind: season.kind,
        startDate: season.startDate,
        endDate: season.endDate,
      },
    });
    await this.curation.publish(season.parkId, []);
  }

  // ── audit ─────────────────────────────────────────────────────────────────

  @Get("history")
  @ApiOperation({ summary: "What administrators have changed, newest first" })
  async history(
    @Query("entityType") entityType?: string,
    @Query("entityId") entityId?: string,
    @Query("actorId") actorId?: string,
    @Query("action") action?: string,
    @Query("limit") limit?: string,
    @Query("offset") offset?: string,
  ) {
    return this.audit.list({
      entityType,
      entityId,
      actorId,
      action,
      limit: Number(limit) || undefined,
      offset: Number(offset) || undefined,
    });
  }

  @Post("history/:id/undo")
  @AdminMinRole("editor")
  @SelfAudited()
  @HttpCode(HttpStatus.OK)
  @ApiOperation({
    summary: "Put a curation back the way it was",
    description:
      "The undo is itself an edit and gets its own row; the original is marked " +
      "as reverted rather than deleted.",
  })
  async undo(@Param("id") id: string, @CurrentAdmin() admin: AdminPrincipal) {
    const result = await this.curation.revert(id, admin);
    return { changed: result.changed, auditId: result.auditId };
  }

  // ── helpers ───────────────────────────────────────────────────────────────

  private async attractionCounts(
    parkIds: string[],
  ): Promise<Map<string, number>> {
    if (parkIds.length === 0) return new Map();
    const rows: Array<{ parkId: string; count: string }> =
      await this.attractions
        .createQueryBuilder("attraction")
        .select("attraction.parkId", "parkId")
        .addSelect("COUNT(*)", "count")
        .where("attraction.parkId IN (:...parkIds)", { parkIds })
        .andWhere("attraction.retired_at IS NULL")
        .groupBy("attraction.parkId")
        .getRawMany();
    return new Map(rows.map((row) => [row.parkId, Number(row.count)]));
  }
}

/**
 * Coordinates arrive as strings from the Postgres driver (`decimal` columns are
 * not parsed to numbers, because they do not always fit one). A map that is
 * handed "51.7861" places its marker at 0,0 and reports no error.
 */
function toNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const parsed = typeof value === "number" ? value : Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

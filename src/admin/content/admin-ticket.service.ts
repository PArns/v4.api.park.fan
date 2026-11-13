import { InjectQueue } from "@nestjs/bull";
import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import type { Queue } from "bull";
import { QueryFailedError, Repository } from "typeorm";
import {
  CDN_SETTLE_MS,
  RevalidationService,
} from "../../common/revalidation/revalidation.service";
import { TicketProductRecord } from "../../trip-regions/entities/ticket-product.entity";
import { TicketCatalogService } from "../../trip-regions/ticket-catalog.service";
import { TripRegionService } from "../../trip-regions/trip-region.service";
import {
  catalogVersion,
  productOfRow,
  rowOfProduct,
  type TicketProductRow,
} from "../../trip-regions/ticket-catalog";
import {
  dryRunTicket,
  validateTicketProduct,
  type TicketDryRun,
  type TicketProblem,
} from "../../trip-regions/ticket-validation";
import {
  linksOf,
  shopUrlOf,
  TICKET_RULE_KEYS,
} from "../../trip-regions/tickets";
import type { RegionConfig } from "../../trip-regions/regions";
import { AdminAuditService } from "../auth/admin-audit.service";
import type { AdminPrincipal } from "../auth/admin-principal";
import type {
  AdminTicketCatalogDto,
  AdminTicketProductDto,
  TicketValidationDto,
} from "./dto/ticket-product.dto";

/** The frontend's cache tag for a region's ticket catalog. */
export const tripTicketsTag = (region: string): string =>
  `trip-tickets:${region}`;

/**
 * The ticket catalog's admin: list, read, check, create, edit, archive and
 * order a trip region's products. A write is checked whole (every problem by
 * its path), planned on a sample trip, written, audited with the product
 * before and after, and published outside-in: the catalog leaves Redis, the
 * frontend's tag is revalidated, and again after `CDN_SETTLE_MS`. Contract:
 * docs/frontend/trip-regions.md#editing-the-catalog.
 */
@Injectable()
export class AdminTicketService {
  private readonly logger = new Logger(AdminTicketService.name);

  constructor(
    @InjectRepository(TicketProductRecord)
    private readonly rows: Repository<TicketProductRecord>,
    private readonly catalog: TicketCatalogService,
    private readonly regions: TripRegionService,
    private readonly revalidation: RevalidationService,
    private readonly audit: AdminAuditService,
    @InjectQueue("manual-metadata") private readonly queue: Queue,
  ) {}

  async list(regionSlug: string): Promise<AdminTicketCatalogDto> {
    const region = this.regions.region(regionSlug);
    const rows = await this.ordered(region.slug);
    return {
      region: region.slug,
      version: catalogVersion(rows.map(productOfRow)),
      tickets: rows.map(adminView),
    };
  }

  async get(regionSlug: string, slug: string): Promise<AdminTicketProductDto> {
    const region = this.regions.region(regionSlug);
    return adminView(await this.find(region.slug, slug));
  }

  /** The checks and the dry run of a full product, nothing saved. */
  async validate(
    regionSlug: string,
    body: Record<string, unknown>,
  ): Promise<TicketValidationDto> {
    const region = await this.regions.regionWithTickets(regionSlug);
    const others = await this.slugs(region.slug);
    const existing =
      typeof body.slug === "string" && others.includes(body.slug);
    const { problems, valid } = validateTicketProduct(region, body, {
      creating: !existing,
      slugs: others,
    });
    if (!valid) return { ok: false, problems };
    const dryRun = dryRunTicket(
      region,
      valid.product,
      this.regions.today(region),
    );
    return {
      ok: dryRun.problems.length === 0,
      problems: dryRun.problems,
      dryRun: dryRunView(dryRun),
    };
  }

  async create(
    regionSlug: string,
    body: Record<string, unknown>,
    admin: AdminPrincipal,
  ): Promise<AdminTicketProductDto> {
    const region = await this.regions.regionWithTickets(regionSlug);
    const others = await this.slugs(region.slug);
    const row = this.checked(region, body, { creating: true, slugs: others });
    let saved: TicketProductRecord;
    try {
      saved = await this.rows.save(
        this.rows.create({ ...row, updatedBy: actorOf(admin) }),
      );
    } catch (err) {
      if (isUniqueViolation(err)) {
        throw refused([
          {
            path: "slug",
            message: `${row.slug} exists already in ${region.slug}`,
          },
        ]);
      }
      throw err;
    }
    await this.audit.record({
      actor: admin,
      action: "ticket_product.create",
      entityType: "ticket_product",
      entityId: `${region.slug}/${saved.slug}`,
      entityLabel: saved.name,
      after: auditView(saved),
    });
    await this.publish(region.slug);
    return adminView(saved);
  }

  /** A PATCH: the fields given merged onto the stored product, checked whole. */
  async update(
    regionSlug: string,
    slug: string,
    body: Record<string, unknown>,
    admin: AdminPrincipal,
  ): Promise<AdminTicketProductDto> {
    const region = await this.regions.regionWithTickets(regionSlug);
    const stored = await this.find(region.slug, slug);
    if (body.slug !== undefined && body.slug !== slug) {
      throw refused([
        { path: "slug", message: "is the product's id and cannot change" },
      ]);
    }
    const merged: Record<string, unknown> = { ...draftOf(stored) };
    for (const [key, value] of Object.entries(body)) {
      if (value !== undefined) merged[key] = value;
    }
    const row = this.checked(region, merged, {
      creating: false,
      slugs: await this.slugs(region.slug),
    });
    const before = auditView(stored);
    Object.assign(stored, row, { updatedBy: actorOf(admin) });
    const saved = await this.rows.save(stored);
    await this.audit.record({
      actor: admin,
      action: "ticket_product.update",
      entityType: "ticket_product",
      entityId: `${region.slug}/${slug}`,
      entityLabel: saved.name,
      before,
      after: auditView(saved),
    });
    await this.publish(region.slug);
    return adminView(saved);
  }

  /** `active = false`: off the catalog route, never suggested, still readable by `tickets=`. */
  async archive(
    regionSlug: string,
    slug: string,
    reason: string | undefined,
    admin: AdminPrincipal,
  ): Promise<AdminTicketProductDto> {
    const region = this.regions.region(regionSlug);
    const stored = await this.find(region.slug, slug);
    if (!stored.active) return adminView(stored);
    const before = auditView(stored);
    stored.active = false;
    stored.updatedBy = actorOf(admin);
    const saved = await this.rows.save(stored);
    await this.audit.record({
      actor: admin,
      action: "ticket_product.archive",
      entityType: "ticket_product",
      entityId: `${region.slug}/${slug}`,
      entityLabel: saved.name,
      before,
      after: auditView(saved),
      reason: reason ?? null,
    });
    await this.publish(region.slug);
    return adminView(saved);
  }

  /** Every product of the region in the new order: sort orders 10, 20, …. */
  async reorder(
    regionSlug: string,
    slugs: string[],
    admin: AdminPrincipal,
  ): Promise<AdminTicketCatalogDto> {
    const region = this.regions.region(regionSlug);
    const rows = await this.ordered(region.slug);
    const current = rows.map((r) => r.slug);
    const missing = current.filter((s) => !slugs.includes(s));
    const unknown = slugs.filter((s) => !current.includes(s));
    const twice = slugs.filter((s, i) => slugs.indexOf(s) !== i);
    const problems: TicketProblem[] = [
      ...missing.map((s) => ({ path: "slugs", message: `${s} is missing` })),
      ...unknown.map((s) => ({
        path: "slugs",
        message: `${s} is not a product of ${region.slug}`,
      })),
      ...twice.map((s) => ({ path: "slugs", message: `${s} is listed twice` })),
    ];
    if (problems.length > 0) throw refused(problems);
    await this.rows.manager.transaction(async (manager) => {
      for (const [i, slug] of slugs.entries()) {
        await manager.update(
          TicketProductRecord,
          { region: region.slug, slug },
          { sortOrder: (i + 1) * 10, updatedBy: actorOf(admin) },
        );
      }
    });
    await this.audit.record({
      actor: admin,
      action: "ticket_product.reorder",
      entityType: "ticket_product",
      entityId: region.slug,
      entityLabel: `${region.slug} ticket order`,
      before: { order: current },
      after: { order: slugs },
    });
    await this.publish(region.slug);
    return this.list(region.slug);
  }

  /**
   * Publishes a catalog edit outside-in (docs/rules/publish-outside-in.md):
   * the catalog leaves Redis and this instance's memory, the frontend's tag is
   * revalidated, and again after `CDN_SETTLE_MS`, when the edge copy of the
   * tickets route (five minutes, ten stale) has let go.
   */
  async publish(region: string): Promise<void> {
    await this.catalog.evict(region);
    const tags = [tripTicketsTag(region)];
    await this.revalidation.revalidateTags(tags);
    try {
      await this.queue.add(
        "revalidate-tags",
        { tags },
        {
          delay: CDN_SETTLE_MS,
          removeOnComplete: true,
          removeOnFail: true,
          // One sweep per region and minute: a burst of edits leaves one, and
          // an edit late in another's window still gets a sweep of its own.
          jobId: `revalidate-${tags[0]}-${Math.floor(Date.now() / 60_000)}`,
        },
      );
    } catch (err) {
      this.logger.warn(
        `Could not schedule the post-CDN revalidation of ${tags[0]}: ${(err as Error).message}`,
      );
    }
  }

  /** A draft checked and dry-run, as the row to write; 400 with every problem otherwise. */
  private checked(
    region: RegionConfig,
    draft: Record<string, unknown>,
    options: { creating: boolean; slugs: string[] },
  ): TicketProductRow {
    const { problems, valid } = validateTicketProduct(region, draft, options);
    if (!valid) throw refused(problems);
    const dryRun = dryRunTicket(
      region,
      valid.product,
      this.regions.today(region),
    );
    if (dryRun.problems.length > 0) throw refused(dryRun.problems);
    return rowOfProduct(region.slug, valid.product, {
      sortOrder: valid.sortOrder,
      active: valid.active,
      sources: valid.sources,
    });
  }

  private async find(
    region: string,
    slug: string,
  ): Promise<TicketProductRecord> {
    const row = await this.rows.findOne({ where: { region, slug } });
    if (!row) {
      throw new NotFoundException(`No ticket product ${slug} in ${region}`);
    }
    return row;
  }

  private ordered(region: string): Promise<TicketProductRecord[]> {
    return this.rows.find({
      where: { region },
      order: { sortOrder: "ASC", slug: "ASC" },
    });
  }

  private async slugs(region: string): Promise<string[]> {
    return (await this.ordered(region)).map((r) => r.slug);
  }
}

/** A 400 that lists every problem by its path. */
function refused(problems: TicketProblem[]): BadRequestException {
  return new BadRequestException({
    statusCode: 400,
    message: "The ticket product is not valid",
    problems,
  });
}

function isUniqueViolation(err: unknown): boolean {
  return (
    err instanceof QueryFailedError &&
    (err as QueryFailedError & { code?: string }).code === "23505"
  );
}

/** Who wrote a row: the admin user, or the login of a principal without one. */
function actorOf(admin: AdminPrincipal): string {
  return admin.userId ?? admin.email;
}

function dryRunView(d: TicketDryRun): TicketValidationDto["dryRun"] {
  return {
    owned: d.owned,
    planFirst: d.planFirst,
    ...(d.note && { note: d.note }),
  };
}

/** A row as the editor reads it: the public product and what only the admin sees. */
export function adminView(row: TicketProductRecord): AdminTicketProductDto {
  const {
    suggest: _suggest,
    archived: _archived,
    ...product
  } = productOfRow(row);
  return {
    ...product,
    links: linksOf(product),
    shopUrl: shopUrlOf(product),
    slug: row.slug,
    region: row.region,
    active: row.active,
    sortOrder: row.sortOrder,
    suggestRank: row.suggestRank,
    sources: row.sources ?? [],
    createdAt: new Date(row.createdAt).toISOString(),
    updatedAt: new Date(row.updatedAt).toISOString(),
    ...(row.updatedBy && { updatedBy: row.updatedBy }),
  } as AdminTicketProductDto;
}

/** A stored row as a write draft: what a PATCH is merged onto. */
export function draftOf(row: TicketProductRecord): Record<string, unknown> {
  return {
    slug: row.slug,
    name: row.name,
    description: row.description ?? undefined,
    resort: row.resort,
    ...Object.fromEntries(
      TICKET_RULE_KEYS.filter((k) => row.rules[k] !== undefined).map((k) => [
        k,
        row.rules[k],
      ]),
    ),
    links: row.links,
    sources: row.sources,
    blockoutUrl: row.blockoutUrl ?? undefined,
    reservationUrl: row.reservationUrl ?? undefined,
    confidence: row.confidence,
    notes: row.notes ?? undefined,
    active: row.active,
    sortOrder: row.sortOrder,
    suggestRank: row.suggestRank,
  };
}

/** The audit log's before and after: the whole product, so an edit can be typed back in. */
function auditView(row: TicketProductRecord): Record<string, unknown> {
  return draftOf(row);
}

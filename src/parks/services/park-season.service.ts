import {
  BadRequestException,
  Injectable,
  Logger,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { In, LessThanOrEqual, MoreThanOrEqual, Repository } from "typeorm";
import {
  PARK_SEASON_KINDS,
  PARK_SEASON_STATUSES,
  ParkSeason,
  type ParkSeasonKind,
  type ParkSeasonStatus,
} from "../entities/park-season.entity";
import { Park } from "../entities/park.entity";
import { Attraction } from "../../attractions/entities/attraction.entity";
import { parseHttpUrl } from "../../common/utils/http-url.util";

export interface SeasonInput {
  kind: ParkSeasonKind;
  name?: string | null;
  startDate: string;
  endDate: string;
  dates?: string[] | null;
  status?: ParkSeasonStatus;
  separateTicket?: boolean;
  priceFrom?: number | string | null;
  priceCurrency?: string | null;
  opensAt?: string | null;
  closesAt?: string | null;
  attractionIds?: string[] | null;
  url?: string | null;
  sourceUrl?: string | null;
  confirmedAt?: string | null;
  note?: string | null;
}

export interface SeasonQuery {
  parkId?: string;
  kind?: ParkSeasonKind;
  status?: ParkSeasonStatus;
  year?: number;
  /** Only seasons overlapping today. */
  current?: boolean;
  limit?: number;
  offset?: number;
}

const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TIME_RE = /^([01]\d|2[0-3]):[0-5]\d$/;

@Injectable()
export class ParkSeasonService {
  private readonly logger = new Logger(ParkSeasonService.name);

  constructor(
    @InjectRepository(ParkSeason)
    private readonly seasons: Repository<ParkSeason>,
    @InjectRepository(Park)
    private readonly parks: Repository<Park>,
    @InjectRepository(Attraction)
    private readonly attractions: Repository<Attraction>,
  ) {}

  async list(query: SeasonQuery = {}): Promise<{
    total: number;
    seasons: ParkSeason[];
  }> {
    const qb = this.seasons
      .createQueryBuilder("season")
      .leftJoinAndSelect("season.park", "park")
      .orderBy("season.start_date", "DESC")
      .addOrderBy("park.name", "ASC")
      .take(Math.min(query.limit ?? 100, 500))
      .skip(query.offset ?? 0);

    if (query.parkId)
      qb.andWhere("season.park_id = :parkId", { parkId: query.parkId });
    if (query.kind) qb.andWhere("season.kind = :kind", { kind: query.kind });
    if (query.status)
      qb.andWhere("season.status = :status", { status: query.status });

    if (query.year) {
      // Overlap, not containment: a winter closure running 4 Nov 2026 –
      // 27 Mar 2027 belongs to both years, and a query for 2027 that missed it
      // would tell an editor the park has nothing on file.
      qb.andWhere(
        "season.start_date <= :yearEnd AND season.end_date >= :yearStart",
        {
          yearStart: `${query.year}-01-01`,
          yearEnd: `${query.year}-12-31`,
        },
      );
    }

    if (query.current) {
      const today = new Date().toISOString().slice(0, 10);
      qb.andWhere("season.start_date <= :today AND season.end_date >= :today", {
        today,
      });
    }

    const [seasons, total] = await qb.getManyAndCount();
    return { seasons, total };
  }

  async findOne(id: string): Promise<ParkSeason> {
    const season = await this.seasons.findOne({
      where: { id },
      relations: ["park"],
    });
    if (!season) throw new NotFoundException(`No season ${id}`);
    return season;
  }

  async create(
    parkId: string,
    input: SeasonInput,
    actorId: string | null,
  ): Promise<ParkSeason> {
    const park = await this.parks.findOne({ where: { id: parkId } });
    if (!park) throw new NotFoundException(`No park ${parkId}`);

    const normalized = await this.normalize(parkId, input);
    const season = this.seasons.create({
      ...normalized,
      parkId,
      updatedBy: actorId,
    });
    return this.seasons.save(season);
  }

  async update(
    id: string,
    input: Partial<SeasonInput>,
    actorId: string | null,
  ): Promise<ParkSeason> {
    const season = await this.findOne(id);
    // Validate the merged result, not the patch: a request that only moves
    // `endDate` still has to end up with a season that ends after it starts,
    // and the check constraint is a much worse place to discover that.
    const merged = await this.normalize(season.parkId, {
      kind: input.kind ?? season.kind,
      name: input.name !== undefined ? input.name : season.name,
      startDate: input.startDate ?? season.startDate,
      endDate: input.endDate ?? season.endDate,
      dates: input.dates !== undefined ? input.dates : season.dates,
      status: input.status ?? season.status,
      separateTicket: input.separateTicket ?? season.separateTicket,
      priceFrom:
        input.priceFrom !== undefined ? input.priceFrom : season.priceFrom,
      priceCurrency:
        input.priceCurrency !== undefined
          ? input.priceCurrency
          : season.priceCurrency,
      opensAt: input.opensAt !== undefined ? input.opensAt : season.opensAt,
      closesAt: input.closesAt !== undefined ? input.closesAt : season.closesAt,
      attractionIds:
        input.attractionIds !== undefined
          ? input.attractionIds
          : season.attractionIds,
      url: input.url !== undefined ? input.url : season.url,
      sourceUrl:
        input.sourceUrl !== undefined ? input.sourceUrl : season.sourceUrl,
      confirmedAt:
        input.confirmedAt !== undefined
          ? input.confirmedAt
          : (season.confirmedAt?.toISOString() ?? null),
      note: input.note !== undefined ? input.note : season.note,
    });

    Object.assign(season, merged, { updatedBy: actorId });
    return this.seasons.save(season);
  }

  async remove(id: string): Promise<ParkSeason> {
    const season = await this.findOne(id);
    await this.seasons.delete({ id });
    return season;
  }

  /** Every season of a set of parks, for the park list's season counts. */
  async countsByPark(parkIds: string[]): Promise<Map<string, number>> {
    if (parkIds.length === 0) return new Map();
    const rows: Array<{ park_id: string; count: string }> = await this.seasons
      .createQueryBuilder("season")
      .select("season.park_id", "park_id")
      .addSelect("COUNT(*)", "count")
      .where({ parkId: In(parkIds) })
      .groupBy("season.park_id")
      .getRawMany();
    return new Map(rows.map((row) => [row.park_id, Number(row.count)]));
  }

  /**
   * Seasons overlapping a date, for the public read endpoint.
   *
   * Deliberately its own query rather than a field on the park payload: the
   * park response is fetched on every park page and re-polled every five
   * minutes, and seasons are day-stable. Putting them in it would pay for them
   * on every poll of every park, including the ~80 % with none on file.
   */
  async forParkOnDate(parkId: string, date: string): Promise<ParkSeason[]> {
    return this.seasons.find({
      where: {
        parkId,
        startDate: LessThanOrEqual(date),
        endDate: MoreThanOrEqual(date),
      },
      order: { startDate: "ASC" },
    });
  }

  async forParkInRange(
    parkId: string,
    from: string,
    to: string,
  ): Promise<ParkSeason[]> {
    return this.seasons
      .createQueryBuilder("season")
      .where("season.park_id = :parkId", { parkId })
      .andWhere("season.start_date <= :to AND season.end_date >= :from", {
        from,
        to,
      })
      .orderBy("season.start_date", "ASC")
      .getMany();
  }

  // ── validation ────────────────────────────────────────────────────────────

  private async normalize(
    parkId: string,
    input: SeasonInput,
  ): Promise<Partial<ParkSeason>> {
    if (!PARK_SEASON_KINDS.includes(input.kind)) {
      throw new BadRequestException(
        `kind must be one of: ${PARK_SEASON_KINDS.join(", ")}`,
      );
    }
    const status = input.status ?? "announced";
    if (!PARK_SEASON_STATUSES.includes(status)) {
      throw new BadRequestException(
        `status must be one of: ${PARK_SEASON_STATUSES.join(", ")}`,
      );
    }

    const startDate = this.requireDate(input.startDate, "startDate");
    const endDate = this.requireDate(input.endDate, "endDate");
    if (endDate < startDate) {
      throw new BadRequestException("endDate must not be before startDate");
    }

    let dates: string[] | null = null;
    if (input.dates !== null && input.dates !== undefined) {
      if (!Array.isArray(input.dates)) {
        throw new BadRequestException(
          "dates must be a list of YYYY-MM-DD strings",
        );
      }
      // An empty list would say the season runs on no day at all. Null — "every
      // day between start and end" — is the thing an editor clearing the field
      // means, and the two must stay distinguishable.
      if (input.dates.length === 0) {
        dates = null;
      } else {
        const parsed = input.dates.map((d) => this.requireDate(d, "dates"));
        for (const date of parsed) {
          if (date < startDate || date > endDate) {
            throw new BadRequestException(
              `${date} is outside the season's range (${startDate} – ${endDate})`,
            );
          }
        }
        dates = [...new Set(parsed)].sort();
      }
    }

    if (input.opensAt && !TIME_RE.test(input.opensAt)) {
      throw new BadRequestException("opensAt must be HH:MM");
    }
    if (input.closesAt && !TIME_RE.test(input.closesAt)) {
      throw new BadRequestException("closesAt must be HH:MM");
    }

    let priceFrom: string | null = null;
    if (
      input.priceFrom !== null &&
      input.priceFrom !== undefined &&
      input.priceFrom !== ""
    ) {
      const value = Number(input.priceFrom);
      if (!Number.isFinite(value) || value < 0) {
        throw new BadRequestException(
          "priceFrom must be a non-negative number",
        );
      }
      priceFrom = value.toFixed(2);
    }

    const priceCurrency = input.priceCurrency?.trim().toUpperCase() || null;
    if (priceCurrency && !/^[A-Z]{3}$/.test(priceCurrency)) {
      throw new BadRequestException(
        "priceCurrency must be a three-letter ISO 4217 code",
      );
    }
    // A price with no currency is a number nobody can act on, and a currency
    // with no price is noise on the card.
    if (priceFrom && !priceCurrency) {
      throw new BadRequestException("priceFrom needs a priceCurrency");
    }

    let attractionIds: string[] | null = null;
    if (input.attractionIds && input.attractionIds.length > 0) {
      const unique = [...new Set(input.attractionIds)];
      const found = await this.attractions.find({
        where: { id: In(unique), parkId },
        select: { id: true },
      });
      // Checked against THIS park, not globally: a maintenance window naming a
      // ride in another park is a copy-paste error, and it would render on a
      // page where that ride does not exist.
      if (found.length !== unique.length) {
        const foundIds = new Set(found.map((a) => a.id));
        const missing = unique.filter((id) => !foundIds.has(id));
        throw new BadRequestException(
          `These attractions are not in this park: ${missing.join(", ")}`,
        );
      }
      attractionIds = unique;
    }

    return {
      kind: input.kind,
      name: input.name?.trim() || null,
      startDate,
      endDate,
      dates,
      status,
      separateTicket: input.separateTicket ?? false,
      priceFrom,
      priceCurrency,
      opensAt: input.opensAt?.trim() || null,
      closesAt: input.closesAt?.trim() || null,
      attractionIds,
      // Both are rendered as links on the public park page, so they go through
      // the same scheme check as a curated website rather than being trusted
      // because an editor typed them.
      url: this.optionalUrl(input.url, "url"),
      sourceUrl: this.optionalUrl(input.sourceUrl, "sourceUrl"),
      confirmedAt: this.optionalTimestamp(input.confirmedAt, "confirmedAt"),
      note: input.note?.trim() || null,
    };
  }

  private optionalUrl(
    value: string | null | undefined,
    field: string,
  ): string | null {
    const trimmed = value?.trim();
    if (!trimmed) return null;
    return parseHttpUrl(trimmed, field);
  }

  /**
   * A timestamp an editor typed, or null.
   *
   * Parsed here rather than handed to `new Date()` at the call site: an
   * unparseable string becomes an Invalid Date, which TypeORM sends to a
   * `timestamptz` column and Postgres refuses — so a typo in one optional
   * field answered 500 with nothing naming the field, while every other date
   * on the same form answers 400 through `requireDate`.
   */
  private optionalTimestamp(
    value: string | null | undefined,
    field: string,
  ): Date | null {
    if (value === null || value === undefined || value === "") return null;
    const parsed = new Date(value);
    if (Number.isNaN(parsed.getTime())) {
      throw new BadRequestException(
        `${field} must be an ISO 8601 timestamp, e.g. 2026-08-20T09:00:00Z`,
      );
    }
    return parsed;
  }

  private requireDate(value: string, field: string): string {
    if (typeof value !== "string" || !DATE_RE.test(value)) {
      throw new BadRequestException(`${field} must be a YYYY-MM-DD date`);
    }
    // Rejects 2026-02-30 and friends, which the regex alone lets through and
    // Postgres would then take as a 400 from a layer with no useful message.
    const parsed = new Date(`${value}T00:00:00Z`);
    if (
      Number.isNaN(parsed.getTime()) ||
      parsed.toISOString().slice(0, 10) !== value
    ) {
      throw new BadRequestException(`${field} is not a real date`);
    }
    return value;
  }
}

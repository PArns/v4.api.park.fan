import {
  BadRequestException,
  ConflictException,
  Injectable,
  NotFoundException,
} from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Repository } from "typeorm";
import { stripInvisibleCharacters } from "../common/utils/visible-text.util";
import {
  MAX_PENDING_REQUESTS_PER_ACCOUNT,
  requestTier,
} from "./api-key-request.policy";
import { ApiKeyService, type ApiKeyItem } from "./api-key.service";
import type { ApiKeyUsage } from "./api-key.constants";
import type { CreateApiKeyRequestDto } from "./dto/api-key-request.dto";
import { ApiKeyRequest } from "./entities/api-key-request.entity";
import { sortPermissions } from "./permissions";
import { accountsByIds } from "../admin/auth/accounts-by-ids.util";
import type { ApiKeyRequestStatus } from "./entities/api-key-request.entity";

/** A request as the admin list shows it: with the account that asked. */
export interface ApiKeyRequestItem extends ApiKeyRequest {
  applicant: { email: string; displayName: string } | null;
}

/** What an approval sets on the key it issues. */
export interface ApprovalInput {
  name?: string;
  permissions: string[];
  usage?: ApiKeyUsage;
  requestsPerHour?: number | null;
  allowedParkIds?: string[] | null;
  expiresAt?: Date | null;
  note?: string;
  message?: string;
}

/** Text from a form, trimmed and without invisible characters; null when nothing is left. */
function clean(value: string | undefined | null): string | null {
  if (!value) return null;
  const text = stripInvisibleCharacters(value).trim();
  return text || null;
}

/**
 * Key requests from the developer dashboard: a developer files and withdraws
 * their own, the owner lists, approves (which issues a key bound to the
 * developer's account, without anyone seeing its first secret), turns down
 * with a reason, or reopens them.
 */
@Injectable()
export class ApiKeyRequestService {
  constructor(
    @InjectRepository(ApiKeyRequest)
    private readonly requests: Repository<ApiKeyRequest>,
    private readonly apiKeys: ApiKeyService,
  ) {}

  /** Files a request for a developer account. */
  async create(
    userId: string,
    input: CreateApiKeyRequestDto,
  ): Promise<ApiKeyRequest> {
    if (input.revenue.includes("none") && input.revenue.length > 1) {
      throw new BadRequestException(
        '"none" cannot stand beside a revenue model',
      );
    }
    const pending = await this.requests.count({
      where: { userId, status: "pending" },
    });
    if (pending >= MAX_PENDING_REQUESTS_PER_ACCOUNT) {
      throw new ConflictException({
        error: "too-many-pending",
        message: `At most ${MAX_PENDING_REQUESTS_PER_ACCOUNT} requests can wait at once.`,
      });
    }
    const projectName = clean(input.projectName);
    const description = clean(input.description);
    if (!projectName || !description) {
      throw new BadRequestException("A request needs a name and a description");
    }
    const revenue = [...new Set(input.revenue)];
    return this.requests.save(
      this.requests.create({
        userId,
        status: "pending",
        tier: requestTier({ source: input.source, revenue }),
        projectName,
        website: clean(input.website),
        kind: input.kind,
        platforms: [...new Set(input.platforms)],
        languages: clean(input.languages),
        description,
        source: input.source,
        repository:
          input.source === "open-source" ? clean(input.repository) : null,
        license: input.source === "open-source" ? clean(input.license) : null,
        revenue,
        permissions: sortPermissions(input.permissions),
        endpoints: clean(input.endpoints),
        volume: input.volume,
        parks: clean(input.parks),
        serverSide: input.serverSide,
        caching: clean(input.caching),
        attributionUrl: clean(input.attributionUrl),
        message: clean(input.message),
        locale: input.locale,
      }),
    );
  }

  /** A developer's own requests, newest first. */
  async listForUser(userId: string): Promise<ApiKeyRequest[]> {
    return this.requests.find({
      where: { userId },
      order: { createdAt: "DESC" },
    });
  }

  /** Every request, undecided ones first, newest first within each, with who asked. */
  async listAll(): Promise<ApiKeyRequestItem[]> {
    const rows = await this.requests
      .createQueryBuilder("r")
      .orderBy("r.status = 'pending'", "DESC")
      .addOrderBy("r.created_at", "DESC")
      .getMany();
    if (rows.length === 0) return [];
    const accounts = await accountsByIds(
      this.requests,
      rows.map((row) => row.userId),
    );
    return rows.map((row) => ({
      ...row,
      applicant: accounts.get(row.userId) ?? null,
    }));
  }

  /**
   * Moves a request from one state to another in a single conditional UPDATE, so two clicks at
   * once cannot both pass the check: the second finds nothing in `from` and gets the 409 (or the
   * 404 when there is no such request at all). Returns the row as it stands afterwards.
   */
  private async transition(
    where: { id: string; userId?: string },
    from: ApiKeyRequestStatus,
    changes: Partial<ApiKeyRequest>,
    refusal: string,
  ): Promise<ApiKeyRequest> {
    const result = await this.requests.update(
      { ...where, status: from },
      changes,
    );
    if (!result.affected) {
      const exists = await this.requests.exists({ where });
      if (!exists) throw new NotFoundException("No such request");
      throw new ConflictException(refusal);
    }
    return this.requests.findOneOrFail({ where: { id: where.id } });
  }

  /** Withdraws a developer's own undecided request. */
  withdraw(id: string, userId: string): Promise<ApiKeyRequest> {
    return this.transition(
      { id, userId },
      "pending",
      { status: "withdrawn", decidedAt: new Date() },
      "Only an undecided request can be withdrawn",
    );
  }

  /**
   * Approves an undecided request: issues its key, bound to the developer's
   * account and unrevealed, so the developer picks up the first secret on
   * the dashboard. The admin names the key's permissions; the request's only
   * suggest them. The request is claimed before the key exists, so a second
   * approval at the same moment cannot issue a second key; if issuing fails,
   * the request goes back to undecided.
   */
  async approve(
    id: string,
    input: ApprovalInput,
    decidedBy: { userId: string | null; email: string },
  ): Promise<{ request: ApiKeyRequest; key: ApiKeyItem }> {
    const row = await this.transition(
      { id },
      "pending",
      {
        status: "approved",
        decidedAt: new Date(),
        decidedBy: decidedBy.email,
        decisionNote: clean(input.message),
      },
      "Only an undecided request can be approved",
    );
    let created: Awaited<ReturnType<ApiKeyService["create"]>>;
    try {
      const account = (await accountsByIds(this.requests, [row.userId])).get(
        row.userId,
      );
      created = await this.apiKeys.create(
        {
          name: clean(input.name) ?? row.projectName,
          contact: account?.email ?? null,
          usage: input.usage ?? "non-commercial",
          note: clean(input.note),
          requestsPerHour: input.requestsPerHour,
          allowedParkIds: input.allowedParkIds,
          expiresAt: input.expiresAt,
          permissions: input.permissions,
          ownerUserId: row.userId,
          requestId: row.id,
          revealed: false,
        },
        decidedBy.userId,
      );
    } catch (error) {
      await this.requests.update(
        { id, status: "approved" },
        {
          status: "pending",
          decidedAt: null,
          decidedBy: null,
          decisionNote: null,
        },
      );
      throw error;
    }
    await this.requests.update({ id }, { keyId: created.item.id });
    return { request: { ...row, keyId: created.item.id }, key: created.item };
  }

  /** Turns down an undecided request; the reason is what the developer reads. */
  reject(
    id: string,
    reason: string | undefined,
    decidedBy: string,
  ): Promise<ApiKeyRequest> {
    return this.transition(
      { id },
      "pending",
      {
        status: "rejected",
        decidedAt: new Date(),
        decidedBy,
        decisionNote: clean(reason),
      },
      "Only an undecided request can be rejected",
    );
  }

  /** Puts a rejected request back into the queue, for a decision taken by mistake. */
  reopen(id: string): Promise<ApiKeyRequest> {
    return this.transition(
      { id },
      "rejected",
      {
        status: "pending",
        decidedAt: null,
        decidedBy: null,
        decisionNote: null,
      },
      "Only a rejected request can be reopened",
    );
  }

  /** Deletes every request of an account, whatever its state; returns how many. */
  async removeAllForUser(userId: string): Promise<number> {
    const result = await this.requests.delete({ userId });
    return result.affected ?? 0;
  }

  /** Deletes a request for good; a key issued for it stays. */
  async remove(id: string): Promise<ApiKeyRequest> {
    const row = await this.requests.findOne({ where: { id } });
    if (!row) throw new NotFoundException("No such request");
    await this.requests.delete({ id });
    return row;
  }
}

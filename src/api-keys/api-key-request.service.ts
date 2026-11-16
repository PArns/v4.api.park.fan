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
    const accounts = (await this.requests.query(
      `SELECT id, email, display_name FROM admin_users WHERE id = ANY($1::uuid[])`,
      [[...new Set(rows.map((row) => row.userId))]],
    )) as { id: string; email: string; display_name: string }[];
    const byId = new Map(accounts.map((a) => [a.id, a]));
    return rows.map((row) => {
      const account = byId.get(row.userId);
      return {
        ...row,
        applicant: account
          ? { email: account.email, displayName: account.display_name }
          : null,
      };
    });
  }

  /** Withdraws a developer's own undecided request. */
  async withdraw(id: string, userId: string): Promise<ApiKeyRequest> {
    const row = await this.requests.findOne({ where: { id, userId } });
    if (!row) throw new NotFoundException("No such request");
    if (row.status !== "pending") {
      throw new ConflictException("Only an undecided request can be withdrawn");
    }
    row.status = "withdrawn";
    row.decidedAt = new Date();
    return this.requests.save(row);
  }

  /**
   * Approves an undecided request: issues its key, bound to the developer's
   * account and unrevealed, so the developer picks up the first secret on
   * the dashboard. The admin names the key's permissions; the request's only
   * suggest them.
   */
  async approve(
    id: string,
    input: ApprovalInput,
    decidedBy: { userId: string | null; email: string },
  ): Promise<{ request: ApiKeyRequest; key: ApiKeyItem }> {
    const row = await this.requests.findOne({ where: { id } });
    if (!row) throw new NotFoundException("No such request");
    if (row.status !== "pending") {
      throw new ConflictException("Only an undecided request can be approved");
    }
    const account = (await this.requests.query(
      `SELECT email FROM admin_users WHERE id = $1::uuid`,
      [row.userId],
    )) as { email: string }[];
    const created = await this.apiKeys.create(
      {
        name: clean(input.name) ?? row.projectName,
        contact: account[0]?.email ?? null,
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
    row.status = "approved";
    row.decidedAt = new Date();
    row.decidedBy = decidedBy.email;
    row.decisionNote = clean(input.message);
    row.keyId = created.item.id;
    return { request: await this.requests.save(row), key: created.item };
  }

  /** Turns down an undecided request; the reason is what the developer reads. */
  async reject(
    id: string,
    reason: string | undefined,
    decidedBy: string,
  ): Promise<ApiKeyRequest> {
    const row = await this.requests.findOne({ where: { id } });
    if (!row) throw new NotFoundException("No such request");
    if (row.status !== "pending") {
      throw new ConflictException("Only an undecided request can be rejected");
    }
    row.status = "rejected";
    row.decidedAt = new Date();
    row.decidedBy = decidedBy;
    row.decisionNote = clean(reason);
    return this.requests.save(row);
  }

  /** Puts a rejected request back into the queue, for a decision taken by mistake. */
  async reopen(id: string): Promise<ApiKeyRequest> {
    const row = await this.requests.findOne({ where: { id } });
    if (!row) throw new NotFoundException("No such request");
    if (row.status !== "rejected") {
      throw new ConflictException("Only a rejected request can be reopened");
    }
    row.status = "pending";
    row.decidedAt = null;
    row.decidedBy = null;
    row.decisionNote = null;
    return this.requests.save(row);
  }

  /** Deletes a request for good; a key issued for it stays. */
  async remove(id: string): Promise<ApiKeyRequest> {
    const row = await this.requests.findOne({ where: { id } });
    if (!row) throw new NotFoundException("No such request");
    await this.requests.delete({ id });
    return row;
  }
}

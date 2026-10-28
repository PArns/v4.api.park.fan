import { Inject, Injectable, Logger } from "@nestjs/common";
import { InjectRepository } from "@nestjs/typeorm";
import { Redis } from "ioredis";
import { IsNull, LessThan, Repository } from "typeorm";
import { randomBytes } from "crypto";
import { REDIS_CLIENT } from "../../common/redis/redis.module";
import {
  checkRateLimit,
  hashRateLimitKey,
} from "../../common/redis/rate-limit.util";
import { AdminOAuthClient } from "./entities/admin-oauth-client.entity";
import { getOAuthRedirectHosts } from "../../config/oauth.config";
import { checkRedirectUri } from "./redirect-uri.util";
import { OAuthError } from "./oauth-error";
import { DEFAULT_ADMIN_TOKEN_SCOPE } from "../auth/admin-api-token.constants";
import { stripInvisibleCharacters } from "../../common/utils/visible-text.util";

/** Registrations one address may make per hour. Claude Code makes one per login. */
const REGISTRATIONS_PER_HOUR = 10;

/** Registrations nobody has used yet, across all callers, before new ones wait. */
const MAX_UNUSED_CLIENTS = 500;

/** A registration never used for a grant is deleted after this long. */
const UNUSED_CLIENT_TTL_MS = 24 * 60 * 60 * 1000;

const MAX_REDIRECT_URIS = 10;
const MAX_CLIENT_NAME_LENGTH = 100;

/** The RFC 7591 §3.2.1 answer to a registration. */
export interface ClientRegistrationResponse {
  client_id: string;
  client_id_issued_at: number;
  client_name: string;
  redirect_uris: string[];
  grant_types: string[];
  response_types: string[];
  token_endpoint_auth_method: "none";
  scope: string;
}

/**
 * Dynamic client registration (RFC 7591) for public clients: Claude Code,
 * claude.ai's custom connectors, the MCP Inspector. Anyone may register,
 * which is what the MCP spec expects of a server without a client directory;
 * a registration grants nothing until an admin approves a request from it.
 */
@Injectable()
export class OAuthClientService {
  private readonly logger = new Logger(OAuthClientService.name);

  constructor(
    @InjectRepository(AdminOAuthClient)
    private readonly clients: Repository<AdminOAuthClient>,
    @Inject(REDIS_CLIENT) private readonly redis: Redis,
  ) {}

  async register(
    metadata: unknown,
    addressBucket: string | null,
  ): Promise<ClientRegistrationResponse> {
    if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
      throw new OAuthError(
        "invalid_client_metadata",
        "The body must be a JSON object",
      );
    }
    const body = metadata as Record<string, unknown>;

    const redirectUris = this.redirectUris(body.redirect_uris);
    this.checkGrantTypes(body.grant_types);
    this.checkResponseTypes(body.response_types);
    const clientName = this.clientName(body.client_name);

    if (addressBucket) {
      const verdict = await checkRateLimit({
        redis: this.redis,
        logger: this.logger,
        label: "OAuth registration limiter",
        key: `admin:oauth:register:${hashRateLimitKey(addressBucket)}`,
        max: REGISTRATIONS_PER_HOUR,
        windowSeconds: 60 * 60,
      });
      if (!verdict.allowed) {
        throw new OAuthError(
          "temporarily_unavailable",
          "Too many registrations from this address; try again later",
          429,
          verdict.retryAfterSeconds,
        );
      }
    }

    await this.clients.delete({
      lastUsedAt: IsNull(),
      createdAt: LessThan(new Date(Date.now() - UNUSED_CLIENT_TTL_MS)),
    });
    const unused = await this.clients.count({
      where: { lastUsedAt: IsNull() },
    });
    if (unused >= MAX_UNUSED_CLIENTS) {
      this.logger.warn(
        `OAuth registration refused: ${unused} registrations are still unused`,
      );
      throw new OAuthError(
        "temporarily_unavailable",
        "Registration is paused; try again later",
        429,
        60 * 60,
      );
    }

    const saved = await this.clients.save(
      this.clients.create({
        clientId: `pfc_${randomBytes(16).toString("base64url")}`,
        clientName,
        redirectUris,
        lastUsedAt: null,
      }),
    );

    return {
      client_id: saved.clientId,
      client_id_issued_at: Math.floor(saved.createdAt.getTime() / 1000),
      client_name: saved.clientName,
      redirect_uris: saved.redirectUris,
      grant_types: ["authorization_code", "refresh_token"],
      response_types: ["code"],
      // Whatever the client asked for: there are no client secrets here, and
      // RFC 7591 lets the server replace a requested value.
      token_endpoint_auth_method: "none",
      scope: DEFAULT_ADMIN_TOKEN_SCOPE,
    };
  }

  async find(clientId: unknown): Promise<AdminOAuthClient | null> {
    if (typeof clientId !== "string" || !/^pfc_[\w-]{1,64}$/.test(clientId)) {
      return null;
    }
    return this.clients.findOne({ where: { clientId } });
  }

  async markUsed(clientId: string): Promise<void> {
    await this.clients.update({ clientId }, { lastUsedAt: new Date() });
  }

  private redirectUris(value: unknown): string[] {
    if (!Array.isArray(value) || value.length === 0) {
      throw new OAuthError(
        "invalid_redirect_uri",
        "redirect_uris must list at least one URI",
      );
    }
    if (value.length > MAX_REDIRECT_URIS) {
      throw new OAuthError(
        "invalid_redirect_uri",
        `At most ${MAX_REDIRECT_URIS} redirect URIs`,
      );
    }
    const allowedHosts = getOAuthRedirectHosts();
    for (const uri of value) {
      const verdict = checkRedirectUri(uri, allowedHosts);
      if (!verdict.ok) {
        throw new OAuthError("invalid_redirect_uri", verdict.reason);
      }
    }
    return [...new Set(value as string[])];
  }

  private checkGrantTypes(value: unknown): void {
    if (value === undefined) return;
    if (
      !Array.isArray(value) ||
      !value.includes("authorization_code") ||
      value.some((g) => g !== "authorization_code" && g !== "refresh_token")
    ) {
      throw new OAuthError(
        "invalid_client_metadata",
        "grant_types may only be authorization_code and refresh_token",
      );
    }
  }

  private checkResponseTypes(value: unknown): void {
    if (value === undefined) return;
    if (!Array.isArray(value) || value.some((r) => r !== "code")) {
      throw new OAuthError(
        "invalid_client_metadata",
        'response_types may only be ["code"]',
      );
    }
  }

  private clientName(value: unknown): string {
    if (value === undefined || value === null) return "Unnamed client";
    if (typeof value !== "string") {
      throw new OAuthError(
        "invalid_client_metadata",
        "client_name must be a string",
      );
    }
    const name = stripInvisibleCharacters(value).slice(
      0,
      MAX_CLIENT_NAME_LENGTH,
    );
    return name || "Unnamed client";
  }
}

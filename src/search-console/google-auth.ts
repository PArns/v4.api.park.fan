import { createSign } from "crypto";
import type { ServiceAccount } from "../config/search-console.config";
import { GOOGLE_TOKEN_URL } from "./search-console.constants";

/** What Google answered, or that it could not be reached, when asked for a token. */
export class GoogleAuthError extends Error {}

const TOKEN_TIMEOUT_MS = 15_000;

/** A token is renewed this long before it runs out. */
const RENEW_BEFORE_MS = 60_000;

const base64url = (input: Buffer | string): string =>
  Buffer.from(input).toString("base64url");

/**
 * The signed assertion a service account trades for an access token
 * (RFC 7523): RS256 over the header and claims, valid for an hour. Done with
 * Node's own `crypto`; the one thing the import needs from Google's client
 * libraries is these thirty lines.
 */
export function signAssertion(
  account: ServiceAccount,
  scope: string,
  nowSeconds: number,
): string {
  const header = base64url(JSON.stringify({ alg: "RS256", typ: "JWT" }));
  const claims = base64url(
    JSON.stringify({
      iss: account.clientEmail,
      scope,
      aud: GOOGLE_TOKEN_URL,
      iat: nowSeconds,
      exp: nowSeconds + 3600,
    }),
  );
  const signature = createSign("RSA-SHA256")
    .update(`${header}.${claims}`)
    .sign(account.privateKey);
  return `${header}.${claims}.${base64url(signature)}`;
}

/**
 * Access tokens for one service account and scope, kept until a minute before
 * they expire. Concurrent callers share one request.
 */
export class ServiceAccountTokens {
  private cached: { token: string; expiresAt: number } | null = null;
  private pending: Promise<string> | null = null;

  constructor(
    private readonly account: ServiceAccount,
    private readonly scope: string,
    private readonly now: () => number = Date.now,
  ) {}

  /** Whether these tokens were made for this account, so a changed key makes new ones. */
  isFor(account: ServiceAccount): boolean {
    return (
      account.clientEmail === this.account.clientEmail &&
      account.privateKey === this.account.privateKey
    );
  }

  async token(): Promise<string> {
    if (this.cached && this.cached.expiresAt - RENEW_BEFORE_MS > this.now()) {
      return this.cached.token;
    }
    this.pending ??= this.fetchToken().finally(() => {
      this.pending = null;
    });
    return this.pending;
  }

  /** Forget the token, after Google refused it. */
  invalidate(): void {
    this.cached = null;
  }

  private async fetchToken(): Promise<string> {
    let assertion: string;
    try {
      assertion = signAssertion(
        this.account,
        this.scope,
        Math.floor(this.now() / 1000),
      );
    } catch {
      throw new GoogleAuthError(
        "The private key of the service account could not sign: is GSC_SERVICE_ACCOUNT_JSON the whole key file?",
      );
    }
    let response: Response;
    try {
      response = await fetch(GOOGLE_TOKEN_URL, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body: new URLSearchParams({
          grant_type: "urn:ietf:params:oauth:grant-type:jwt-bearer",
          assertion,
        }),
        signal: AbortSignal.timeout(TOKEN_TIMEOUT_MS),
      });
    } catch (error) {
      throw new GoogleAuthError(
        `Google's token endpoint could not be reached: ${(error as Error).message}`,
      );
    }
    const body = (await response.json().catch(() => ({}))) as {
      access_token?: string;
      expires_in?: number;
      error?: string;
      error_description?: string;
    };
    if (!response.ok || !body.access_token) {
      throw new GoogleAuthError(
        `Google refused the service account (${response.status} ${body.error ?? "no error"}${
          body.error_description ? `: ${body.error_description}` : ""
        })`,
      );
    }
    this.cached = {
      token: body.access_token,
      expiresAt: this.now() + (body.expires_in ?? 3600) * 1000,
    };
    return body.access_token;
  }
}

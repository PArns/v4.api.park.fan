import { Injectable } from "@nestjs/common";
import { AdminTurnstileService } from "../admin/auth/admin-turnstile.service";
import { getAdminTurnstileSecret } from "../config/admin-auth.config";
import { readMasterKey } from "./assistant-secret.util";
import {
  issueSessionToken,
  sessionKey,
  verifySessionToken,
} from "./assistant-session.util";

/** The action the widget solves with; park.fan's `TURNSTILE_ACTIONS.assistantSession`. */
const TURNSTILE_ACTION = "assistant-session";

/**
 * The challenge in front of Parky: a Turnstile token in, a signed session token out.
 *
 * Without `TURNSTILE_SECRET_KEY` the challenge cannot be checked. In production that refuses
 * every session, because a check that waves everything through is no check; elsewhere (local
 * development, tests) any token passes so the widget can be worked on without Cloudflare.
 */
@Injectable()
export class AssistantChallengeService {
  constructor(private readonly turnstile: AdminTurnstileService) {}

  private key(): Buffer | null {
    const master = readMasterKey(process.env.ASSISTANT_SECRET_KEY);
    return master ? sessionKey(master) : null;
  }

  /** A session token, "failed" when the challenge was not solved, "off" when none can be issued. */
  async open(
    turnstileToken: string,
    ip: string | null,
  ): Promise<{ token: string } | "failed" | "off"> {
    const key = this.key();
    if (!key) return "off";
    if (getAdminTurnstileSecret()) {
      const verdict = await this.turnstile.verify(
        turnstileToken,
        ip,
        TURNSTILE_ACTION,
      );
      if (!verdict.success) return "failed";
    } else if (process.env.NODE_ENV === "production") {
      return "off";
    }
    return { token: issueSessionToken(key) };
  }

  /** The session's id when the token was issued by this server and is still valid, else null. */
  session(token: string | undefined): string | null {
    const key = this.key();
    return key && token ? verifySessionToken(key, token) : null;
  }
}

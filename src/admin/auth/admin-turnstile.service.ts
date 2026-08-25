import { Injectable, Logger } from "@nestjs/common";
import {
  getAdminTurnstileSecret,
  getAdminTurnstileTimeoutMs,
} from "../../config/admin-auth.config";

/**
 * The Cloudflare Turnstile check on the login, for callers who are not us.
 *
 * park.fan's admin already solves this challenge in the browser and verifies it
 * in its own `/api/admin/session` route before it forwards anything here. That
 * defends the path a browser takes and only that path: `POST
 * /v1/admin/auth/login` is public and reachable directly, so a bot that skips
 * the frontend never meets the challenge at all. This is the same check at the
 * door it actually has to pass.
 *
 * It is asked of a request only when that request does **not** carry a valid
 * `THROTTLE_BYPASS_KEYS` value — the header our frontend sends on every
 * server-side call, and the existing definition of "this is our own frontend".
 * Demanding a token from the frontend too would mean it had to stop verifying
 * and start forwarding, since a token may be redeemed exactly once, and both
 * repositories would have to be deployed in the same minute for anybody to be
 * able to sign in.
 *
 * Env:
 *  - `ADMIN_TURNSTILE_SECRET_KEY` (or `TURNSTILE_SECRET_KEY`) — the widget's
 *    secret. Unset ⇒ the check is off, see `isAdminLoginTurnstileEnforced`.
 */

const SITEVERIFY_URL =
  "https://challenges.cloudflare.com/turnstile/v0/siteverify";

export interface TurnstileVerdict {
  success: boolean;
  /** Present when success=false: a short reason, for the log and nothing else. */
  reason?: string;
}

@Injectable()
export class AdminTurnstileService {
  private readonly logger = new Logger(AdminTurnstileService.name);

  async verify(token: string, ip: string | null): Promise<TurnstileVerdict> {
    const secret = getAdminTurnstileSecret();
    // Callers gate on isAdminLoginTurnstileEnforced() before asking, so this is
    // a belt-and-braces guard rather than the live path. It refuses rather than
    // waving the request through: a verifier with no secret cannot verify, and
    // answering "fine" to that is how a check silently stops being one.
    if (!secret) return { success: false, reason: "not-configured" };
    if (!token) return { success: false, reason: "missing-token" };

    const body = new URLSearchParams({ secret, response: token });
    if (ip) body.append("remoteip", ip);

    try {
      const response = await fetch(SITEVERIFY_URL, {
        method: "POST",
        headers: { "Content-Type": "application/x-www-form-urlencoded" },
        body,
        // Cloudflare answers in tens of milliseconds. The timeout is here
        // because this call sits in front of every login: without one, a
        // siteverify that hangs holds the login open for however long the
        // platform's default socket timeout is, which turns an outage at
        // Cloudflare into an outage of our admin.
        signal: AbortSignal.timeout(getAdminTurnstileTimeoutMs()),
      });

      const data = (await response.json()) as {
        success?: boolean;
        "error-codes"?: string[];
      };
      if (data.success === true) return { success: true };
      return {
        success: false,
        reason: (data["error-codes"] ?? ["failed"]).join(","),
      };
    } catch (error) {
      // A network failure is not a pass. The alternative — letting the request
      // through when Cloudflare cannot be reached — makes the check removable
      // by anyone able to disturb that one connection.
      this.logger.error(
        `siteverify request failed: ${
          error instanceof Error ? error.message : String(error)
        }`,
      );
      return { success: false, reason: "network-error" };
    }
  }
}

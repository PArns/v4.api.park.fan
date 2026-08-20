import { BadRequestException } from "@nestjs/common";

/**
 * Accept an address only if a browser would follow it.
 *
 * Parsed rather than pattern-matched, and restricted to the two schemes that
 * navigate: every value that goes through here ends up as an `href` on a public
 * page, so a stored `javascript:` URL would be cross-site scripting with the
 * name of whoever saved it in the audit row. `data:` is the same trick under a
 * different scheme.
 *
 * The consumer is not the defence. park.fan renders these through React, which
 * refuses a `javascript:` href on its own — but this API is public, it is not
 * the only thing that may ever read it, and "the client happens to sanitize"
 * is not a property this side gets to rely on.
 */
export function parseHttpUrl(value: string, label: string): string {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new BadRequestException(
      `${label} must be a full address, including https://`,
    );
  }
  if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
    throw new BadRequestException(`${label} must be an http(s) address`);
  }
  return parsed.toString();
}

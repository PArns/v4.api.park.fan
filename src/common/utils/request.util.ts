import { isIP } from "net";
import {
  carriesCfOriginSecret,
  carriesThrottleBypassKey,
} from "../throttler/throttler.config";

/**
 * Minimal request shape for IP/header extraction (Express Request compatible).
 */
export interface RequestLike {
  headers: Record<string, string | string[] | undefined>;
  ip?: string;
  socket?: { remoteAddress?: string };
}

/**
 * Get first value from a header (e.g. first IP in X-Forwarded-For).
 */
export function getFirstHeader(req: RequestLike, name: string): string | null {
  const value = req.headers[name];
  if (!value) return null;
  const s = typeof value === "string" ? value : value[0];
  const first = (s ?? "").trim().split(",")[0]?.trim();
  return first || null;
}

/**
 * Get client IP from request, checking common proxy headers then connection.
 * Order: X-Forwarded-For (first), CF-Connecting-IP, True-Client-IP, X-Real-IP,
 * X-Forwarding-IP, req.ip, socket.remoteAddress.
 */
export function getClientIp(req: RequestLike | undefined): string | null {
  if (!req) return null;
  const raw =
    getFirstHeader(req, "x-forwarded-for") ??
    getFirstHeader(req, "cf-connecting-ip") ??
    getFirstHeader(req, "true-client-ip") ??
    getFirstHeader(req, "x-real-ip") ??
    getFirstHeader(req, "x-forwarding-ip") ??
    req.ip ??
    req.socket?.remoteAddress ??
    null;
  return raw ? normalizeIp(raw) : null;
}

/**
 * Client IP for a rate-limit key — never a value the caller can rotate.
 *
 * `getClientIp` trusts the first `X-Forwarded-For` hop, which the caller
 * writes (Cloudflare appends the real address after it), so a fresh value per
 * request would mean a fresh bucket. Here X-Forwarded-For counts only from our
 * own frontend proxy (bypass key); everyone else is keyed on Cloudflare's
 * `CF-Connecting-IP`, then the socket address.
 */
export function getRateLimitIp(req: RequestLike | undefined): string | null {
  if (!req) return null;
  if (carriesThrottleBypassKey(req.headers)) {
    const forwarded = getFirstHeader(req, "x-forwarded-for");
    const ip = forwarded ? normalizeIp(forwarded) : null;
    if (ip && isIP(ip)) return ip;
  }
  // Only a request that provably came through Cloudflare may name its own
  // address in CF-Connecting-IP; the rule `CfThrottlerGuard` applies too.
  if (carriesCfOriginSecret(req.headers)) {
    const cfIp = getFirstHeader(req, "cf-connecting-ip");
    if (cfIp && isIP(cfIp)) return cfIp;
  }
  const socketIp = req.socket?.remoteAddress;
  return socketIp ? normalizeIp(socketIp) : null;
}

/**
 * The part of an address one subscriber controls: an IPv4 address whole, an
 * IPv6 address as its /64. A host with a /64 can take a new address for every
 * request, so a limit keyed on the full IPv6 address limits nothing.
 */
export function addressBucket(ip: string | null): string | null {
  if (!ip) return null;
  if (isIP(ip) === 6) return `${expandIpv6(ip).slice(0, 4).join(":")}::/64`;
  return ip;
}

/**
 * The eight hextets of an IPv6 address, with `::` written out.
 *
 * Needed because almost every real address is compressed, and slicing the
 * first three groups off the compressed text does not give the first three
 * hextets: `2001:db8::1` splits into `["2001","db8","","1"]`, so the naive
 * version produced `2001:db8:::/48` — not a prefix, not comparable to the
 * next login's, and stored as though it were.
 */
export function expandIpv6(address: string): string[] {
  const [head, tail] = address.split("::");
  const headGroups = head ? head.split(":").filter(Boolean) : [];
  if (tail === undefined) return headGroups;

  const tailGroups = tail ? tail.split(":").filter(Boolean) : [];
  const missing = Math.max(0, 8 - headGroups.length - tailGroups.length);
  return [...headGroups, ...Array<string>(missing).fill("0"), ...tailGroups];
}

/**
 * Normalize IP for GeoIP: strip port (e.g. 1.2.3.4:8080 or [::1]:8080) and IPv4-mapped prefix.
 */
export function normalizeIp(ip: string): string {
  let trimmed = ip.trim();
  // IPv6 with port: [2001:db8::1]:8080 -> 2001:db8::1
  if (trimmed.startsWith("[") && trimmed.includes("]:")) {
    const end = trimmed.indexOf("]:");
    trimmed = trimmed.slice(1, end);
  }
  // IPv4 with port: 1.2.3.4:8080 -> 1.2.3.4 (only if dotted quad + :digits)
  else if (trimmed.includes(".") && trimmed.includes(":")) {
    const lastColon = trimmed.lastIndexOf(":");
    const after = trimmed.slice(lastColon + 1);
    if (/^\d+$/.test(after) && parseInt(after, 10) <= 65535) {
      trimmed = trimmed.slice(0, lastColon);
    }
  }
  // IPv4-mapped: ::ffff:1.2.3.4 -> 1.2.3.4
  if (trimmed.startsWith("::ffff:")) trimmed = trimmed.slice(7);
  return trimmed;
}

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

import { truncateIp } from "./admin-auth.service";

/**
 * The address that ends up in `admin_users.last_login_ip` and on a session.
 *
 * The IPv6 cases are the reason this file exists: the first version sliced the
 * first three groups off the *compressed* text, which is not the same as the
 * first three hextets for any address containing `::` — and that is nearly all
 * of them.
 */
describe("truncateIp", () => {
  it("keeps the first three octets of an IPv4 address", () => {
    expect(truncateIp("203.0.113.47")).toBe("203.0.113.0/24");
  });

  it("unwraps an IPv4-mapped IPv6 address before truncating", () => {
    expect(truncateIp("::ffff:203.0.113.47")).toBe("203.0.113.0/24");
  });

  it("expands a compressed IPv6 address before taking its /48", () => {
    expect(truncateIp("2001:db8::1")).toBe("2001:db8:0::/48");
  });

  it("takes the /48 of a fully written IPv6 address", () => {
    expect(truncateIp("2001:0db8:85a3:0000:0000:8a2e:0370:7334")).toBe(
      "2001:0db8:85a3::/48",
    );
  });

  it("handles the loopback, which compresses everything but one group", () => {
    expect(truncateIp("::1")).toBe("0:0:0::/48");
  });

  it("returns null for nothing and for a non-address", () => {
    expect(truncateIp(null)).toBeNull();
    expect(truncateIp(undefined)).toBeNull();
    expect(truncateIp("not-an-address")).toBeNull();
  });
});

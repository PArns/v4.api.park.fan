import { checkRedirectUri, isRegisteredRedirect } from "./redirect-uri.util";

const CLAUDE = ["claude.ai", "claude.com"];

describe("checkRedirectUri", () => {
  it.each([
    "https://claude.ai/api/mcp/auth_callback",
    "https://claude.com/api/mcp/auth_callback",
    "http://localhost:33418/callback",
    "http://127.0.0.1:53682/callback",
    "http://[::1]:8080/cb",
    "http://localhost/callback",
  ])("accepts %s", (uri) => {
    expect(checkRedirectUri(uri, CLAUDE)).toEqual({ ok: true });
  });

  it.each([
    ["an https host off the list", "https://evil.example/cb"],
    ["a subdomain of an allowed host", "https://evil.claude.ai/cb"],
    ["plain http elsewhere", "http://claude.ai/cb"],
    ["plain http on a LAN address", "http://192.168.1.2/cb"],
    ["a loopback look-alike", "http://localhost.evil.example/cb"],
    ["a custom scheme", "cursor://anysphere.cursor-retrieval/oauth"],
    ["javascript:", "javascript:alert(1)"],
    ["a fragment", "https://claude.ai/cb#x"],
    ["user info", "https://user:pw@claude.ai/cb"],
    ["not a URL", "/relative/path"],
    ["too long", `https://claude.ai/${"a".repeat(2001)}`],
  ])("refuses %s", (_label, uri) => {
    expect(checkRedirectUri(uri, CLAUDE).ok).toBe(false);
  });

  it("refuses a value that is not a string", () => {
    expect(checkRedirectUri(42, CLAUDE).ok).toBe(false);
    expect(checkRedirectUri("", CLAUDE).ok).toBe(false);
  });

  it("allows any https host when the list is *", () => {
    expect(checkRedirectUri("https://chatgpt.com/cb", "*").ok).toBe(true);
    expect(checkRedirectUri("http://chatgpt.com/cb", "*").ok).toBe(false);
  });
});

describe("isRegisteredRedirect", () => {
  const registered = [
    "https://claude.ai/api/mcp/auth_callback",
    "http://localhost:33418/callback",
  ];

  it("matches exactly", () => {
    expect(
      isRegisteredRedirect(
        "https://claude.ai/api/mcp/auth_callback",
        registered,
      ),
    ).toBe(true);
  });

  it("does not normalise an https URI", () => {
    expect(
      isRegisteredRedirect(
        "https://claude.ai/api/mcp/auth_callback/",
        registered,
      ),
    ).toBe(false);
    expect(
      isRegisteredRedirect(
        "https://CLAUDE.ai/api/mcp/auth_callback",
        registered,
      ),
    ).toBe(false);
    expect(
      isRegisteredRedirect(
        "https://claude.ai/api/mcp/auth_callback?x=1",
        registered,
      ),
    ).toBe(false);
  });

  it("lets a loopback URI name another port (RFC 8252 §7.3)", () => {
    expect(
      isRegisteredRedirect("http://localhost:5555/callback", registered),
    ).toBe(true);
  });

  it("keeps a loopback URI's host and path", () => {
    expect(
      isRegisteredRedirect("http://127.0.0.1:33418/callback", registered),
    ).toBe(false);
    expect(
      isRegisteredRedirect("http://localhost:33418/other", registered),
    ).toBe(false);
    expect(
      isRegisteredRedirect("http://localhost:33418/callback#f", registered),
    ).toBe(false);
  });

  it("never stretches an https URI's port", () => {
    expect(
      isRegisteredRedirect(
        "https://claude.ai:8443/api/mcp/auth_callback",
        registered,
      ),
    ).toBe(false);
  });
});

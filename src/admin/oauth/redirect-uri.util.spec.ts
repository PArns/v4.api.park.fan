import { checkRedirectUri, isRegisteredRedirect } from "./redirect-uri.util";

const CLAUDE = {
  hosts: ["claude.ai", "claude.com"],
  paths: ["/api/mcp/auth_callback"],
};

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
    [
      "a subdomain of an allowed host",
      "https://evil.claude.ai/api/mcp/auth_callback",
    ],
    ["another path on an allowed host", "https://claude.ai/share/abc"],
    [
      "the callback with a query",
      "https://claude.ai/api/mcp/auth_callback?next=/x",
    ],
    [
      "the callback on another port",
      "https://claude.ai:8443/api/mcp/auth_callback",
    ],
    [
      "the callback with a trailing slash",
      "https://claude.ai/api/mcp/auth_callback/",
    ],
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

  it("allows any https host when the host list is *, still on the pinned paths", () => {
    const anyHost = { hosts: "*" as const, paths: ["/cb"] };
    expect(checkRedirectUri("https://chatgpt.com/cb", anyHost).ok).toBe(true);
    expect(checkRedirectUri("https://chatgpt.com/other", anyHost).ok).toBe(
      false,
    );
    expect(checkRedirectUri("http://chatgpt.com/cb", anyHost).ok).toBe(false);
  });

  it("allows any path when the path list is *", () => {
    const anyPath = { hosts: ["claude.ai"], paths: "*" as const };
    expect(checkRedirectUri("https://claude.ai/anything?x=1", anyPath).ok).toBe(
      true,
    );
  });

  it("keeps loopback redirects free of the path list", () => {
    expect(
      checkRedirectUri("http://localhost:6274/oauth/callback", CLAUDE).ok,
    ).toBe(true);
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

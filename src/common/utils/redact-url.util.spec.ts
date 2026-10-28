import { redactUrl } from "./redact-url.util";

describe("redactUrl", () => {
  it("hides the deprecated shared admin pass", () => {
    // Two things log a URL, and the legacy path's transport is `?pass=<secret>`:
    // a full-privilege credential that neither expires nor can be revoked,
    // written into the application log on every scripted call.
    expect(redactUrl("/v1/admin/flush-cache?pass=S3CR3T")).toBe(
      "/v1/admin/flush-cache?pass=***",
    );
  });

  it("leaves a URL without a query string alone", () => {
    expect(redactUrl("/v1/admin/parks")).toBe("/v1/admin/parks");
  });

  it("leaves harmless parameters readable", () => {
    // The log is worth reading; redacting everything would make it useless.
    expect(redactUrl("/v1/parks?limit=20&offset=40")).toBe(
      "/v1/parks?limit=20&offset=40",
    );
  });

  it("redacts only the secret among several parameters", () => {
    expect(redactUrl("/v1/admin/sync?pass=S3CR3T&force=true")).toBe(
      "/v1/admin/sync?pass=***&force=true",
    );
  });

  it("covers the other names a secret travels under", () => {
    expect(redactUrl("/x?token=abc")).toBe("/x?token=***");
    expect(redactUrl("/x?PASSWORD=abc")).toBe("/x?PASSWORD=***");
    expect(redactUrl("/x?secret=abc")).toBe("/x?secret=***");
  });

  it("redacts a push endpoint — a capability, same as a credential", () => {
    // GET /v1/push/ride-alerts|show-follows carries it in the query string;
    // anyone holding it can send that browser a notification.
    expect(
      redactUrl(
        "/v1/push/ride-alerts?endpoint=https://fcm.googleapis.com/fcm/send/e1",
      ),
    ).toBe("/v1/push/ride-alerts?endpoint=***");
  });

  it("passes a URL it has nothing to redact through verbatim", () => {
    // Including the malformed ones. Rewriting a URL that needs no redaction
    // would mean the log no longer shows what was actually requested.
    expect(redactUrl("/v1/admin/x?")).toBe("/v1/admin/x?");
    expect(redactUrl("/v1/admin/x?=&&")).toBe("/v1/admin/x?=&&");
  });

  it("keeps the path when a redacted parameter was the only one", () => {
    expect(redactUrl("/v1/admin/x?pass=a")).toBe("/v1/admin/x?pass=***");
  });

  it("hides OAuth secrets a client put in the query string", () => {
    expect(
      redactUrl(
        "/v1/oauth/token?code=abc&code_verifier=def&refresh_token=pfr_x&client_id=pfc_1",
      ),
    ).toBe(
      "/v1/oauth/token?code=***&code_verifier=***&refresh_token=***&client_id=pfc_1",
    );
    expect(redactUrl("/v1/mcp?access_token=pfo_x")).toBe(
      "/v1/mcp?access_token=***",
    );
  });

  it("hides the id of a pending OAuth request in the path", () => {
    expect(redactUrl("/v1/admin/oauth/requests/AbC-123_xyz")).toBe(
      "/v1/admin/oauth/requests/***",
    );
    expect(redactUrl("/v1/admin/oauth/requests/AbC-123_xyz/approve")).toBe(
      "/v1/admin/oauth/requests/***/approve",
    );
    expect(redactUrl("/V1/ADMIN/OAUTH/REQUESTS/AbC/deny?pass=x")).toBe(
      "/V1/ADMIN/OAUTH/REQUESTS/***/deny?pass=***",
    );
  });

  it("hides an address named in the query string", () => {
    expect(redactUrl("/v1/location/nearby?ip=203.0.113.47&radius=50")).toBe(
      "/v1/location/nearby?ip=***&radius=50",
    );
    expect(redactUrl("/v1/favorites?IP=2001:db8::1")).toBe(
      "/v1/favorites?IP=***",
    );
  });
});

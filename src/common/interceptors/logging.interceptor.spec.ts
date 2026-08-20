import { redactUrl } from "./logging.interceptor";

describe("redactUrl", () => {
  it("hides the deprecated shared admin pass", () => {
    // This interceptor logs every URL containing `/admin`, and the legacy path's
    // transport is `?pass=<secret>` — a full-privilege credential that neither
    // expires nor can be revoked, written into the application log on every
    // scripted call.
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

  it("passes a URL it has nothing to redact through verbatim", () => {
    // Including the malformed ones. Rewriting a URL that needs no redaction
    // would mean the log no longer shows what was actually requested.
    expect(redactUrl("/v1/admin/x?")).toBe("/v1/admin/x?");
    expect(redactUrl("/v1/admin/x?=&&")).toBe("/v1/admin/x?=&&");
  });

  it("keeps the path when a redacted parameter was the only one", () => {
    expect(redactUrl("/v1/admin/x?pass=a")).toBe("/v1/admin/x?pass=***");
  });
});

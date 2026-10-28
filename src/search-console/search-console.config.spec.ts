import {
  getSearchConsoleImportDays,
  getSearchConsoleSetup,
  parseServiceAccount,
  siteHosts,
} from "../config/search-console.config";

const KEY = {
  type: "service_account",
  client_email: "gsc@park-fan.iam.gserviceaccount.com",
  private_key: "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
};
const RAW = JSON.stringify(KEY);

describe("parseServiceAccount", () => {
  it("reads raw JSON and base64 of it alike", () => {
    const expected = {
      clientEmail: KEY.client_email,
      privateKey: KEY.private_key,
    };
    expect(parseServiceAccount(RAW)).toEqual(expected);
    expect(parseServiceAccount(Buffer.from(RAW).toString("base64"))).toEqual(
      expected,
    );
    expect(parseServiceAccount(`  ${RAW}\n`)).toEqual(expected);
  });

  it("turns the literal \\n of a key that went through a shell into newlines", () => {
    const escaped = RAW.replace(/\\n/g, "\\\\n");
    expect(parseServiceAccount(escaped).privateKey).toBe(KEY.private_key);
  });

  it("refuses what is not a key file, naming the problem and never the content", () => {
    expect(() => parseServiceAccount("hello")).toThrow(
      /neither JSON nor base64/,
    );
    expect(() => parseServiceAccount("{nope")).toThrow(/not valid JSON/);
    expect(() => parseServiceAccount(JSON.stringify({ a: 1 }))).toThrow(
      /client_email and private_key/,
    );
    try {
      parseServiceAccount(
        JSON.stringify({ client_email: "a@b.c", private_key: "SECRET" }),
      );
    } catch (error) {
      expect((error as Error).message).not.toContain("SECRET");
    }
  });
});

describe("siteHosts", () => {
  it("takes the bare and the www host of a domain property", () => {
    expect(siteHosts("sc-domain:park.fan")).toEqual([
      "park.fan",
      "www.park.fan",
    ]);
  });

  it("takes the host of a URL-prefix property", () => {
    expect(siteHosts("https://www.park.fan/")).toEqual(["www.park.fan"]);
  });

  it("refuses what is no property", () => {
    expect(siteHosts("sc-domain:")).toBeNull();
    expect(siteHosts("park.fan")).toBeNull();
    expect(siteHosts("ftp://park.fan/")).toBeNull();
  });
});

describe("getSearchConsoleSetup", () => {
  it("is off, and says nothing, without credentials", () => {
    expect(getSearchConsoleSetup({})).toEqual({
      configured: false,
      reason: null,
      siteUrl: null,
    });
    expect(
      getSearchConsoleSetup({ GSC_SITE_URL: "sc-domain:park.fan" }),
    ).toMatchObject({ configured: false, reason: null });
  });

  it("is on with a key and a property", () => {
    const setup = getSearchConsoleSetup({
      GSC_SERVICE_ACCOUNT_JSON: RAW,
      GSC_SITE_URL: "sc-domain:park.fan",
    });
    expect(setup).toMatchObject({
      configured: true,
      siteUrl: "sc-domain:park.fan",
      hosts: ["park.fan", "www.park.fan"],
      account: { clientEmail: KEY.client_email },
    });
  });

  it("is off, with the reason, for a key without a property or a malformed one", () => {
    expect(
      getSearchConsoleSetup({ GSC_SERVICE_ACCOUNT_JSON: RAW }),
    ).toMatchObject({
      configured: false,
      reason: expect.stringContaining("GSC_SITE_URL is not set"),
    });
    expect(
      getSearchConsoleSetup({
        GSC_SERVICE_ACCOUNT_JSON: RAW,
        GSC_SITE_URL: "park.fan",
      }),
    ).toMatchObject({
      configured: false,
      reason: expect.stringContaining("park.fan"),
    });
    expect(
      getSearchConsoleSetup({
        GSC_SERVICE_ACCOUNT_JSON: "{oops",
        GSC_SITE_URL: "sc-domain:park.fan",
      }),
    ).toMatchObject({
      configured: false,
      reason: expect.stringContaining("JSON"),
    });
  });
});

describe("getSearchConsoleImportDays", () => {
  it("defaults to three and keeps a value from 1 to 30", () => {
    expect(getSearchConsoleImportDays({})).toBe(3);
    expect(getSearchConsoleImportDays({ GSC_IMPORT_DAYS: "7" })).toBe(7);
    expect(getSearchConsoleImportDays({ GSC_IMPORT_DAYS: "0" })).toBe(3);
    expect(getSearchConsoleImportDays({ GSC_IMPORT_DAYS: "31" })).toBe(3);
    expect(getSearchConsoleImportDays({ GSC_IMPORT_DAYS: "x" })).toBe(3);
  });
});

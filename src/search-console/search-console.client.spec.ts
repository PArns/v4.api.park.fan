import { generateKeyPairSync } from "crypto";
import {
  SearchConsoleApiError,
  SearchConsoleClient,
} from "./search-console.client";
import { GSC_ROW_LIMIT } from "./search-console.constants";

const { privateKey } = generateKeyPairSync("rsa", {
  modulusLength: 2048,
  publicKeyEncoding: { type: "spki", format: "pem" },
  privateKeyEncoding: { type: "pkcs8", format: "pem" },
});

const ENV = {
  GSC_SERVICE_ACCOUNT_JSON: JSON.stringify({
    client_email: "gsc@park-fan.iam.gserviceaccount.com",
    private_key: privateKey,
  }),
  GSC_SITE_URL: "sc-domain:park.fan",
};

const json = (
  body: object,
  status = 200,
  headers: Record<string, string> = {},
) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });

const row = (n: number) => ({
  keys: [`https://park.fan/de/p${n}`, "deu", "MOBILE"],
  clicks: 1,
  impressions: 2,
  ctr: 0.5,
  position: 3,
});

/**
 * The client against a mocked Google: the token exchange and the Search
 * Console API are one `fetch` stub that answers by URL.
 */
describe("SearchConsoleClient", () => {
  const saved = { ...process.env };
  let fetchMock: jest.SpyInstance;
  let api: jest.Mock;
  let client: SearchConsoleClient;

  beforeEach(() => {
    Object.assign(process.env, ENV);
    api = jest.fn();
    fetchMock = jest
      .spyOn(globalThis, "fetch")
      .mockImplementation(
        async (input: string | URL | Request, init?: RequestInit) => {
          const url = String(input);
          if (url.startsWith("https://oauth2.googleapis.com/")) {
            return json({ access_token: "ya29.t", expires_in: 3600 });
          }
          return api(url, init);
        },
      );
    client = new SearchConsoleClient();
    client.wait = jest.fn(async () => {});
  });
  afterEach(() => {
    jest.restoreAllMocks();
    process.env = { ...saved };
  });

  it("asks the property's searchAnalytics endpoint for one day, final numbers only", async () => {
    api.mockResolvedValue(json({ rows: [row(1)] }));
    const rows = await client.fetchDay("2026-10-20", [
      "page",
      "country",
      "device",
    ]);

    expect(rows).toHaveLength(1);
    const [url, init] = api.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(
      "https://www.googleapis.com/webmasters/v3/sites/sc-domain%3Apark.fan/searchAnalytics/query",
    );
    expect((init.headers as Record<string, string>).Authorization).toBe(
      "Bearer ya29.t",
    );
    expect(JSON.parse(String(init.body))).toEqual({
      startDate: "2026-10-20",
      endDate: "2026-10-20",
      dimensions: ["page", "country", "device"],
      type: "web",
      rowLimit: GSC_ROW_LIMIT,
      startRow: 0,
      dataState: "final",
    });
  });

  it("asks for the search type it is given", async () => {
    api.mockResolvedValue(json({ rows: [row(1)] }));
    await client.fetchDay("2026-10-20", ["page"], "discover");
    const body = JSON.parse(String((api.mock.calls[0][1] as RequestInit).body));
    expect(body.type).toBe("discover");
  });

  it("spaces requests at least 100 ms apart, so 10 a second at most", async () => {
    api.mockImplementation(async () => json({ rows: [] }));
    await client.fetchDay("2026-10-20", ["page"]);
    await client.fetchDay("2026-10-20", ["country"]);
    const waits = (client.wait as jest.Mock).mock.calls.map(
      ([ms]) => ms as number,
    );
    expect(waits).toHaveLength(1);
    expect(waits[0]).toBeGreaterThan(0);
    expect(waits[0]).toBeLessThanOrEqual(100);
  });

  it("inspects a URL against the property with the same account and scope", async () => {
    api.mockResolvedValue(
      json({ inspectionResult: { indexStatusResult: { verdict: "PASS" } } }),
    );
    const answer = await client.inspectUrl("https://park.fan/de/parks");
    expect(answer.inspectionResult?.indexStatusResult?.verdict).toBe("PASS");
    const [url, init] = api.mock.calls[0] as [string, RequestInit];
    expect(url).toBe(
      "https://searchconsole.googleapis.com/v1/urlInspection/index:inspect",
    );
    expect(JSON.parse(String(init.body))).toEqual({
      inspectionUrl: "https://park.fan/de/parks",
      siteUrl: "sc-domain:park.fan",
      languageCode: "en-US",
    });
    const assertion = fetchMock.mock.calls.find(([u]) =>
      String(u).startsWith("https://oauth2.googleapis.com/"),
    )![1] as RequestInit;
    expect(String(assertion.body)).toContain("grant_type");
  });

  it("maps an inspection refusal like any other: 403 fatal, 400 not", async () => {
    api.mockResolvedValueOnce(json({ error: { message: "no" } }, 403));
    await expect(client.inspectUrl("https://park.fan/x")).rejects.toMatchObject(
      { fatal: true },
    );
    api.mockResolvedValueOnce(
      json({ error: { message: "URL is not in the property" } }, 400),
    );
    await expect(client.inspectUrl("https://park.fan/x")).rejects.toMatchObject(
      { fatal: false, status: 400 },
    );
  });

  it("pages through a day until a page is not full", async () => {
    const full = Array.from({ length: GSC_ROW_LIMIT }, (_, i) => row(i));
    api
      .mockResolvedValueOnce(json({ rows: full }))
      .mockResolvedValueOnce(json({ rows: full }))
      .mockResolvedValueOnce(json({ rows: [row(1), row(2)] }));
    const rows = await client.fetchDay("2026-10-20", ["page"]);
    expect(rows).toHaveLength(GSC_ROW_LIMIT * 2 + 2);
    const starts = api.mock.calls.map(
      ([, init]) => JSON.parse(String((init as RequestInit).body)).startRow,
    );
    expect(starts).toEqual([0, GSC_ROW_LIMIT, GSC_ROW_LIMIT * 2]);
  });

  it("answers an empty day as no rows", async () => {
    api.mockResolvedValue(json({}));
    expect(await client.fetchDay("2026-10-27", ["page"])).toEqual([]);
  });

  it("retries a 429 and a 5xx after a pause, honouring Retry-After", async () => {
    api
      .mockResolvedValueOnce(
        json({ error: { message: "rate" } }, 429, { "Retry-After": "7" }),
      )
      .mockResolvedValueOnce(json({ error: { message: "oops" } }, 503))
      .mockResolvedValueOnce(json({ rows: [row(1)] }));
    expect(await client.fetchDay("2026-10-20", ["page"])).toHaveLength(1);
    // The pacing between requests waits too; only the pauses for the retries matter here.
    const pauses = (client.wait as jest.Mock).mock.calls
      .map(([ms]) => ms as number)
      .filter((ms) => ms >= 1000);
    expect(pauses).toEqual([7000, 2000]);
  });

  it("gives up on a 5xx after four tries, as a failure that is not fatal", async () => {
    api.mockResolvedValue(json({ error: { message: "backend error" } }, 500));
    const failure = (await client
      .fetchDay("2026-10-20", ["page"])
      .catch((error: SearchConsoleApiError) => error)) as SearchConsoleApiError;
    expect(failure).toBeInstanceOf(SearchConsoleApiError);
    expect(failure).toMatchObject({ status: 500, fatal: false });
    expect(api).toHaveBeenCalledTimes(4);
  });

  it("retries a network failure and then reports it as unreachable", async () => {
    api.mockRejectedValue(new Error("socket hang up"));
    const failure = (await client
      .fetchDay("2026-10-20", ["page"])
      .catch((error: SearchConsoleApiError) => error)) as SearchConsoleApiError;
    expect(failure).toMatchObject({ status: null, fatal: false });
    expect(failure.message).toContain("socket hang up");
    expect(api).toHaveBeenCalledTimes(4);
  });

  it("refreshes the token once on a 401", async () => {
    api
      .mockResolvedValueOnce(json({ error: { message: "expired" } }, 401))
      .mockResolvedValueOnce(json({ rows: [row(1)] }));
    expect(await client.fetchDay("2026-10-20", ["page"])).toHaveLength(1);
    const tokenCalls = fetchMock.mock.calls.filter(([url]) =>
      String(url).startsWith("https://oauth2.googleapis.com/"),
    );
    expect(tokenCalls).toHaveLength(2);
  });

  it("tells the owner what to do about a 403, and stops the run", async () => {
    api.mockResolvedValue(
      json(
        {
          error: {
            message: "User does not have sufficient permission for site",
          },
        },
        403,
      ),
    );
    const failure = (await client
      .fetchDay("2026-10-20", ["page"])
      .catch((error: SearchConsoleApiError) => error)) as SearchConsoleApiError;
    expect(failure.fatal).toBe(true);
    expect(failure.message).toContain("gsc@park-fan.iam.gserviceaccount.com");
    expect(failure.message).toContain("sc-domain:park.fan");
    expect(failure.message).toContain("add the account as a user");
    expect(api).toHaveBeenCalledTimes(1);
  });

  it("names the property it could not find on a 404", async () => {
    api.mockResolvedValue(json({ error: { message: "not found" } }, 404));
    const failure = (await client
      .fetchDay("2026-10-20", ["page"])
      .catch((error: SearchConsoleApiError) => error)) as SearchConsoleApiError;
    expect(failure.fatal).toBe(true);
    expect(failure.message).toContain("GSC_SITE_URL");
  });

  it("stops at once when Google refuses the service account", async () => {
    fetchMock.mockImplementation(async () =>
      json(
        { error: "invalid_grant", error_description: "Invalid JWT Signature." },
        400,
      ),
    );
    const failure = (await client
      .fetchDay("2026-10-20", ["page"])
      .catch((error: SearchConsoleApiError) => error)) as SearchConsoleApiError;
    expect(failure.fatal).toBe(true);
    expect(failure.message).toContain("Invalid JWT Signature");
  });

  it("refuses to ask without credentials", async () => {
    delete process.env.GSC_SERVICE_ACCOUNT_JSON;
    await expect(client.fetchDay("2026-10-20", ["page"])).rejects.toMatchObject(
      {
        fatal: true,
      },
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });
});

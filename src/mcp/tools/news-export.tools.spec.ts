import { McpToolError } from "../mcp-tool";
import { newsExportStatusTool, runNewsExportTool } from "./news-export.tools";
import { adminContext, callAs } from "./__fixtures__/admin-context";

const post = (slug: string) => ({
  slug,
  title: `Post ${slug}`,
  publishedAt: "2026-11-28T09:00:00+01:00",
});

function report(overrides: Record<string, unknown> = {}) {
  return {
    checkedAt: "2026-11-28T09:40:00.000Z",
    deployedAt: "2026-11-28T02:17:47.933Z",
    workflowUrl:
      "https://github.com/PArns/park.fan/actions/workflows/publish-media.yml",
    feeds: [
      {
        feed: "news",
        tone: "warn",
        generatedAt: "2026-11-28T07:00:00.000Z",
        newestExported: post("old"),
        newestOnSite: post("new"),
        missing: [1, 2, 3].map((n) => ({
          translationKey: `key-${n}`,
          title: `Missing ${n}`,
          publishedAt: "2026-11-28T08:00:00+01:00",
          locales: ["de", "en"],
        })),
        behindMinutes: 74.6,
        unreadable: [],
        somethingNew: true,
      },
      {
        feed: "guides",
        tone: "ok",
        generatedAt: "2026-11-28T07:00:00.000Z",
        newestExported: null,
        newestOnSite: null,
        missing: [],
        behindMinutes: null,
        unreadable: [{ locale: "fr", message: "HTTP 404" }],
      },
    ],
    ...overrides,
  };
}

const json = (body: unknown, status = 200) =>
  new Response(JSON.stringify(body), {
    status,
    headers: { "content-type": "application/json" },
  });

describe("news export tools", () => {
  let fetchMock: jest.SpyInstance;

  beforeEach(() => {
    process.env.REVALIDATE_SECRET = "s3cret";
    process.env.REVALIDATE_URL = "https://park.example/api/revalidate";
    fetchMock = jest.spyOn(globalThis, "fetch");
  });

  afterEach(() => {
    delete process.env.REVALIDATE_SECRET;
    delete process.env.REVALIDATE_URL;
    fetchMock.mockRestore();
  });

  describe("news_export_status", () => {
    it("asks the revalidation origin with the secret and trims the report", async () => {
      fetchMock.mockResolvedValue(json(report()));
      const { ctx } = adminContext("viewer", []);

      const answer = (await newsExportStatusTool.handler(
        { limit: 2 },
        ctx,
      )) as Record<string, any>;

      const [url, init] = fetchMock.mock.calls[0];
      expect(url).toBe("https://park.example/api/news-export/status");
      expect(init).toMatchObject({
        method: "GET",
        redirect: "manual",
        headers: expect.objectContaining({ Authorization: "Bearer s3cret" }),
      });
      expect(answer).toMatchObject({
        configured: true,
        checkedAt: "2026-11-28T09:40:00.000Z",
        workflowUrl: expect.stringContaining("publish-media.yml"),
      });
      expect(answer.feeds[0]).toEqual({
        feed: "news",
        tone: "warn",
        generatedAt: "2026-11-28T07:00:00.000Z",
        behindMinutes: 75,
        newestExported: post("old"),
        newestOnSite: post("new"),
        missing: {
          total: 3,
          items: [
            expect.objectContaining({ translationKey: "key-1" }),
            expect.objectContaining({ translationKey: "key-2" }),
          ],
        },
        unreadable: [],
      });
      expect(answer.feeds[1].unreadable).toEqual([
        { locale: "fr", message: "HTTP 404" },
      ]);
    });

    it("says it is not set up, and asks nobody, without the secret", async () => {
      delete process.env.REVALIDATE_SECRET;
      const { ctx } = adminContext("viewer", []);

      const answer = await newsExportStatusTool.handler({ limit: 10 }, ctx);

      expect(answer).toMatchObject({ configured: false });
      expect(fetchMock).not.toHaveBeenCalled();
    });

    it("turns another status, a report it cannot read and no answer into errors for the model", async () => {
      const { ctx } = adminContext("viewer", []);
      const call = () => newsExportStatusTool.handler({ limit: 10 }, ctx);

      fetchMock.mockResolvedValueOnce(json({ error: "Unauthorized" }, 401));
      await expect(call()).rejects.toEqual(
        new McpToolError(
          "park.fan answered /api/news-export/status with HTTP 401",
        ),
      );

      const red = report();
      (red.feeds[0] as Record<string, unknown>).tone = "red";
      fetchMock.mockResolvedValueOnce(json(red));
      await expect(call()).rejects.toBeInstanceOf(McpToolError);

      fetchMock.mockResolvedValueOnce(json(report({ checkedAt: "yesterday" })));
      await expect(call()).rejects.toBeInstanceOf(McpToolError);

      fetchMock.mockRejectedValueOnce(
        Object.assign(new Error("timed out"), { name: "TimeoutError" }),
      );
      await expect(call()).rejects.toEqual(
        new McpToolError(
          "park.fan did not answer /api/news-export/status within 15 s",
        ),
      );
    });
  });

  describe("run_news_export", () => {
    const workflowUrl =
      "https://github.com/PArns/park.fan/actions/workflows/publish-media.yml";

    it("says what it would start on a dry run, and starts nothing", async () => {
      const { ctx, audit } = adminContext("editor", []);

      const answer = await runNewsExportTool.handler({ dryRun: true }, ctx);

      expect(answer).toEqual({
        dryRun: true,
        wouldStart: "publish-media.yml",
        via: "POST https://park.example/api/news-export/run",
        takesMinutes: 20,
      });
      expect(fetchMock).not.toHaveBeenCalled();
      expect(audit).toEqual([]);
    });

    it("starts the workflow with the secret and records it", async () => {
      fetchMock.mockResolvedValue(json({ started: true, workflowUrl }, 202));
      const { ctx, audit } = adminContext("editor", []);

      const answer = await runNewsExportTool.handler({ dryRun: false }, ctx);

      const [url, init] = fetchMock.mock.calls[0];
      expect(url).toBe("https://park.example/api/news-export/run");
      expect(init).toMatchObject({
        method: "POST",
        headers: expect.objectContaining({ Authorization: "Bearer s3cret" }),
      });
      expect(answer).toEqual({
        dryRun: false,
        started: true,
        workflowUrl,
        takesMinutes: 20,
      });
      expect(audit).toEqual([
        expect.objectContaining({
          action: "news-export.run",
          entityType: "system",
          entityLabel: "publish-media.yml",
          after: { workflowUrl },
        }),
      ]);
    });

    it("hands the site's reason to the model and records nothing when it refuses", async () => {
      fetchMock.mockResolvedValue(
        json({ started: false, reason: "a run is already in progress" }, 409),
      );
      const { ctx, audit } = adminContext("editor", []);

      await expect(
        runNewsExportTool.handler({ dryRun: false }, ctx),
      ).rejects.toEqual(
        new McpToolError(
          "park.fan did not start publish-media.yml (HTTP 409): a run is already in progress",
        ),
      );
      expect(audit).toEqual([]);
    });

    it("keeps a started run when the page link fails the check, and never guesses on a bare 200", async () => {
      fetchMock.mockResolvedValueOnce(
        json({ started: true, workflowUrl: "javascript:alert(1)" }),
      );
      const { ctx, audit } = adminContext("editor", []);
      await expect(
        runNewsExportTool.handler({ dryRun: false }, ctx),
      ).resolves.toMatchObject({ started: true, workflowUrl: null });
      expect(audit).toHaveLength(1);

      fetchMock.mockResolvedValueOnce(new Response("ok", { status: 200 }));
      await expect(
        runNewsExportTool.handler({ dryRun: false }, ctx),
      ).rejects.toEqual(
        new McpToolError(
          "park.fan answered HTTP 200 without saying publish-media.yml started; " +
            "look at its runs on GitHub before starting another",
        ),
      );
      expect(audit).toHaveLength(1);
    });

    it("is refused below editor and without the secret", async () => {
      expect(
        (await callAs(runNewsExportTool, "viewer", { dryRun: true })).isError,
      ).toBe(true);
      delete process.env.REVALIDATE_SECRET;
      const { ctx } = adminContext("editor", []);
      await expect(
        runNewsExportTool.handler({ dryRun: true }, ctx),
      ).rejects.toBeInstanceOf(McpToolError);
      expect(fetchMock).not.toHaveBeenCalled();
    });
  });
});

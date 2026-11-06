import type { Redis } from "ioredis";
import { AssistantPostsClient } from "./assistant-posts.client";
import { getFrontendUrl } from "../../config/oauth.config";

describe("AssistantPostsClient.post", () => {
  let redis: { get: jest.Mock; set: jest.Mock };
  let fetchMock: jest.SpyInstance;

  beforeEach(() => {
    redis = { get: jest.fn(async () => null), set: jest.fn(async () => "OK") };
    fetchMock = jest
      .spyOn(global, "fetch")
      .mockResolvedValue(new Response(JSON.stringify({ text: "…" })));
  });
  afterEach(() => jest.restoreAllMocks());

  const client = () => new AssistantPostsClient(redis as unknown as Redis);

  it("asks the frontend with the question as q, and caches under it", async () => {
    await client().post("de", "europa-park-guide", "fast pass");
    const query = "locale=de&slug=europa-park-guide&q=fast+pass";
    expect(fetchMock.mock.calls[0][0]).toBe(
      `${getFrontendUrl()}/api/assistant-post?${query}`,
    );
    expect(redis.set.mock.calls[0][0]).toBe(
      `assistant:posts:assistant-post:${query}`,
    );
  });

  it("leaves q out without a question", async () => {
    await client().post("en", "europa-park-guide");
    expect(fetchMock.mock.calls[0][0]).toBe(
      `${getFrontendUrl()}/api/assistant-post?locale=en&slug=europa-park-guide`,
    );
  });
});

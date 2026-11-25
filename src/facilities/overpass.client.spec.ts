import { BadGatewayException } from "@nestjs/common";
import axios from "axios";
import { OverpassClient, OVERPASS_BACKOFF_MS } from "./overpass.client";

jest.mock("axios", () => ({
  __esModule: true,
  default: {
    post: jest.fn(),
    isAxiosError: (e: { isAxiosError?: boolean }) => e?.isAxiosError === true,
  },
}));

const post = axios.post as jest.Mock;
const refused = (status: number) => ({
  isAxiosError: true,
  response: { status },
});

class Client extends OverpassClient {
  waits: number[] = [];
  protected override sleep(ms: number): Promise<void> {
    this.waits.push(ms);
    return Promise.resolve();
  }
}

describe("OverpassClient", () => {
  beforeEach(() => post.mockReset());

  it("names the tool in its User-Agent and no person", async () => {
    post.mockResolvedValue({ data: { elements: [] } });
    await new Client().query("q");
    const agent = post.mock.calls[0][2].headers["User-Agent"] as string;
    expect(agent).toBe("park.fan-facilities/1.0 (+https://park.fan)");
    expect(agent).not.toContain("@");
  });

  it.each([429, 502, 504])(
    "waits 15 s, then twice as long, and sends a %i again",
    async (status) => {
      post
        .mockRejectedValueOnce(refused(status))
        .mockRejectedValueOnce(refused(status))
        .mockResolvedValue({ data: { elements: [{ type: "node", id: 1 }] } });
      const client = new Client();
      expect((await client.query("q")).elements).toHaveLength(1);
      expect(client.waits).toEqual([15_000, 30_000]);
    },
  );

  it("gives up after the last wait", async () => {
    post.mockRejectedValue(refused(429));
    const client = new Client();
    await expect(client.query("q")).rejects.toThrow(BadGatewayException);
    expect(client.waits).toEqual(OVERPASS_BACKOFF_MS);
    expect(post).toHaveBeenCalledTimes(OVERPASS_BACKOFF_MS.length + 1);
  });

  it("does not repeat a query the server rejects as wrong", async () => {
    post.mockRejectedValue(refused(400));
    const client = new Client();
    await expect(client.query("q")).rejects.toThrow(BadGatewayException);
    expect(client.waits).toEqual([]);
    expect(post).toHaveBeenCalledTimes(1);
  });

  it("refuses a partial answer instead of repeating or returning it", async () => {
    post.mockResolvedValue({
      data: { elements: [], remark: "runtime error: Query timed out" },
    });
    const client = new Client();
    await expect(client.query("q")).rejects.toThrow(/timed out/);
    expect(post).toHaveBeenCalledTimes(1);
  });
});

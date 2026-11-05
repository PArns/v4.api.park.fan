import { OAuthController } from "./oauth.controller";

const FIREFOX =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:131.0) Gecko/20100101 Firefox/131.0";

/**
 * What is pinned: the origin parked for the consent page is the address
 * Cloudflare saw, never the X-Forwarded-For the starter writes itself, and a
 * starter without a User-Agent compares as another browser.
 */
describe("OAuthController.authorize", () => {
  function build() {
    const authorization = {
      authorize: jest.fn(async () => ({
        kind: "redirect",
        location: "https://park.fan/admin/oauth/consent?request=r",
      })),
    };
    const geoip = {
      lookupCountry: (ip: string) => (ip === "203.0.113.9" ? "us" : "de"),
    };
    const controller = new OAuthController(
      authorization as never,
      {} as never,
      geoip as never,
    );
    const response = { setHeader: jest.fn(), redirect: jest.fn() };
    return { controller, authorization, response };
  }

  function request(headers: Record<string, string>) {
    return {
      headers,
      socket: { remoteAddress: "10.0.0.2" },
    } as never;
  }

  it("parks the country of the Cloudflare address and the browser family", async () => {
    const { controller, authorization, response } = build();
    await controller.authorize(
      {},
      request({
        "cf-connecting-ip": "203.0.113.9",
        "x-forwarded-for": "198.51.100.7",
        "user-agent": FIREFOX,
      }),
      response as never,
    );
    expect(authorization.authorize).toHaveBeenCalledWith({}, "203.0.113.9", {
      country: "us",
      browser: "firefox",
    });
  });

  it("counts a starter without a User-Agent as another browser", async () => {
    const { controller, authorization, response } = build();
    await controller.authorize(
      {},
      request({ "cf-connecting-ip": "203.0.113.9" }),
      response as never,
    );
    expect(authorization.authorize).toHaveBeenCalledWith({}, "203.0.113.9", {
      country: "us",
      browser: "other",
    });
  });
});

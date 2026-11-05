import { UnauthorizedException } from "@nestjs/common";
import type { AdminPrincipal } from "../auth/admin-principal";
import { AdminOAuthConsentController } from "./admin-oauth-consent.controller";

const SELF = "11111111-1111-4111-8111-111111111111";
const ADMIN = { userId: SELF } as AdminPrincipal;

/**
 * What is pinned: an approval proves the account's identity first, with the
 * token-creation contract, and a wrong proof leaves the request pending.
 */
describe("AdminOAuthConsentController.approve", () => {
  function build() {
    const authorization = {
      approve: jest.fn(async () => ({ redirectTo: "https://c/cb?code=x" })),
    };
    const accounts = {
      confirmIdentity: jest.fn(async (): Promise<void> => {}),
    };
    const controller = new AdminOAuthConsentController(
      authorization as never,
      accounts as never,
      { lookupCountry: () => null } as never,
    );
    return { controller, authorization, accounts };
  }

  it("confirms the identity, then approves", async () => {
    const { controller, authorization, accounts } = build();
    await expect(
      controller.approve("req", ADMIN, { password: "pw", totpCode: "123456" }),
    ).resolves.toEqual({ redirectTo: "https://c/cb?code=x" });
    expect(accounts.confirmIdentity).toHaveBeenCalledWith(
      SELF,
      "pw",
      "123456",
      "oauth-approve",
    );
    expect(authorization.approve).toHaveBeenCalledWith("req", ADMIN);
  });

  it("leaves the request pending when the proof is refused", async () => {
    const { controller, authorization, accounts } = build();
    accounts.confirmIdentity.mockRejectedValueOnce(
      new UnauthorizedException({ error: "invalid-password" }),
    );
    await expect(
      controller.approve("req", ADMIN, { password: "bad" }),
    ).rejects.toBeInstanceOf(UnauthorizedException);
    expect(authorization.approve).not.toHaveBeenCalled();
  });
});

/**
 * What is pinned: the consent page learns the admin's country from the
 * address and the browser from the session, not from the proxy's User-Agent.
 */
describe("AdminOAuthConsentController.describe", () => {
  it("passes the reader's country and session browser along", async () => {
    const authorization = { describe: jest.fn(async () => ({})) };
    const geoip = {
      lookupCountry: jest.fn((ip: string) =>
        ip === "192.0.2.7" ? "de" : null,
      ),
    };
    const controller = new AdminOAuthConsentController(
      authorization as never,
      {} as never,
      geoip as never,
    );
    await controller.describe("req", {
      ...ADMIN,
      ip: "192.0.2.7",
      sessionUserAgent:
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:131.0) Gecko/20100101 Firefox/131.0",
    });
    expect(authorization.describe).toHaveBeenCalledWith("req", {
      country: "de",
      browser: "firefox",
    });
  });
});

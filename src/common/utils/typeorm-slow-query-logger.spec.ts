import { Logger } from "@nestjs/common";
import {
  SlowQueryFileLogger,
  withoutAddresses,
} from "./typeorm-slow-query-logger";
import { logToFile } from "./file-logger.util";

jest.mock("./file-logger.util", () => ({ logToFile: jest.fn() }));

/**
 * A statement's parameters reach two logs, the slow-query file and the error
 * line of a failed query, and the audit row's `actor_ip` is one of them.
 */
describe("withoutAddresses", () => {
  it("cuts an address to its network and leaves other values alone", () => {
    expect(
      withoutAddresses([
        "auth.token.create",
        "203.0.113.47",
        "2001:db8:1:2::9",
        "::ffff:198.51.100.7",
        42,
        null,
        "not an address",
      ]),
    ).toEqual([
      "auth.token.create",
      "203.0.113.0/24",
      "2001:db8:1::/48",
      "198.51.100.0/24",
      42,
      null,
      "not an address",
    ]);
  });

  it("looks inside an array parameter", () => {
    expect(withoutAddresses([["203.0.113.47", "x"]])).toEqual([
      ["203.0.113.0/24", "x"],
    ]);
  });

  it("passes no parameters through", () => {
    expect(withoutAddresses(undefined)).toBeUndefined();
  });
});

describe("SlowQueryFileLogger", () => {
  afterEach(() => jest.restoreAllMocks());

  it("writes a slow statement's parameters without the address", () => {
    new SlowQueryFileLogger().logQuerySlow(
      2400,
      "INSERT INTO admin_audit_log (actor_ip) VALUES ($1)",
      ["203.0.113.47"],
    );
    expect(JSON.stringify((logToFile as jest.Mock).mock.calls)).not.toContain(
      "203.0.113.47",
    );
    expect(JSON.stringify((logToFile as jest.Mock).mock.calls)).toContain(
      "203.0.113.0/24",
    );
  });

  it("logs a failed statement's parameters without the address", () => {
    const error = jest.spyOn(Logger.prototype, "error").mockImplementation();
    new SlowQueryFileLogger().logQueryError(
      "boom",
      "INSERT INTO admin_audit_log (actor_ip) VALUES ($1)",
      ["203.0.113.47"],
    );
    expect(JSON.stringify(error.mock.calls)).not.toContain("203.0.113.47");
  });
});

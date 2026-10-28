import type { Request, Response } from "express";
import type { AdminPrincipal } from "../admin/auth/admin-principal";
import { McpController } from "./mcp.controller";

/** What is pinned: a request slot is given back when the server cannot be built. */
describe("McpController.handle", () => {
  const admin = { userId: "u1", apiToken: { id: "t1" } } as AdminPrincipal;

  function response() {
    const res: Record<string, jest.Mock> = {
      setHeader: jest.fn(),
      status: jest.fn(() => res),
      json: jest.fn(),
      on: jest.fn(),
    };
    return res as unknown as Response & typeof res;
  }

  it("releases the slot when building the server throws", async () => {
    const controller = new McpController({
      create: () => {
        throw new Error("boom");
      },
    } as never);
    const request = { body: {} } as Request;
    // More failures than slots: a leak would turn the later ones into 429s.
    for (let i = 0; i < 6; i++) {
      await expect(
        controller.handle(request, response(), admin),
      ).rejects.toThrow("boom");
    }
  });
});

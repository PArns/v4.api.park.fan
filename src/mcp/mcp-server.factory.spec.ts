import { z } from "zod";
import {
  MAX_TOOL_RESULT_CHARS,
  McpServerFactory,
  serialize,
} from "./mcp-server.factory";
import { defineMcpTool, McpToolError } from "./mcp-tool";
import type { AdminPrincipal } from "../admin/auth/admin-principal";

const VIEWER: AdminPrincipal = {
  userId: "11111111-1111-4111-8111-111111111111",
  email: "you@park.fan",
  displayName: "You",
  role: "viewer",
  sessionToken: null,
  legacy: false,
  ip: null,
  mustChangePassword: false,
  mustEnrolTotp: false,
  apiToken: { id: "t", kind: "pat", clientName: null, scope: "admin:read" },
};

class GreetingService {
  greet(name: string): string {
    return `hello ${name}`;
  }
}

const greetTool = defineMcpTool({
  name: "greet",
  title: "Greet",
  description: "Greets",
  input: { name: z.string() },
  async handler({ name }, ctx) {
    return { text: ctx.get(GreetingService).greet(name), who: ctx.admin.email };
  },
});

const editorTool = defineMcpTool({
  name: "edit_something",
  title: "Edit",
  description: "Needs an editor",
  minRole: "editor",
  input: {},
  async handler() {
    return { edited: true };
  },
});

function factory() {
  const moduleRef = { get: jest.fn(() => new GreetingService()) };
  return { factory: new McpServerFactory(moduleRef as never), moduleRef };
}

describe("McpServerFactory", () => {
  it("lists only the tools the role may use", () => {
    const { factory: f } = factory();
    expect(
      f.visibleTools(VIEWER, [greetTool, editorTool]).map((t) => t.name),
    ).toEqual(["greet"]);
    expect(
      f.visibleTools({ ...VIEWER, role: "editor" }, [greetTool, editorTool]),
    ).toHaveLength(2);
  });

  it("refuses a tool above the role even when called directly", async () => {
    const { factory: f } = factory();
    const result = await f.call(editorTool, {}, VIEWER);
    expect(result.isError).toBe(true);
  });

  it("hands the handler its input, the principal and any provider", async () => {
    const { factory: f, moduleRef } = factory();
    const result = await f.call(greetTool, { name: "Taron" }, VIEWER);
    expect(result.isError).toBeUndefined();
    expect(JSON.parse((result.content[0] as { text: string }).text)).toEqual({
      text: "hello Taron",
      who: "you@park.fan",
    });
    expect(moduleRef.get).toHaveBeenCalledWith(GreetingService, {
      strict: false,
    });
  });

  it("passes a tool's own failure to the model", async () => {
    const { factory: f } = factory();
    const tool = defineMcpTool({
      name: "lookup",
      title: "Lookup",
      description: "x",
      input: {},
      async handler() {
        throw new McpToolError("No park with that id");
      },
    });
    const result = await f.call(tool, {}, VIEWER);
    expect(result).toEqual({
      isError: true,
      content: [{ type: "text", text: "No park with that id" }],
    });
  });

  it("keeps any other failure's message out of the answer", async () => {
    const { factory: f } = factory();
    const tool = defineMcpTool({
      name: "broken",
      title: "Broken",
      description: "x",
      input: {},
      async handler() {
        throw new Error('relation "secret_table" does not exist at 10.0.0.5');
      },
    });
    const result = await f.call(tool, {}, VIEWER);
    expect(result.isError).toBe(true);
    const text = (result.content[0] as { text: string }).text;
    expect(text).not.toContain("secret_table");
    expect(text).not.toContain("10.0.0.5");
  });

  it("builds a server with every visible tool registered", () => {
    const { factory: f } = factory();
    const server = f.create(VIEWER, [greetTool, editorTool]);
    const registered = (
      server as unknown as { _registeredTools: Record<string, unknown> }
    )._registeredTools;
    expect(Object.keys(registered)).toEqual(["greet"]);
  });
});

describe("serialize", () => {
  it("writes compact JSON", () => {
    expect(serialize({ a: 1, b: [1, 2] })).toBe('{"a":1,"b":[1,2]}');
    expect(serialize(undefined)).toBe("null");
  });

  it("cuts an oversized answer and says how to narrow it", () => {
    const text = serialize({ blob: "x".repeat(MAX_TOOL_RESULT_CHARS * 2) });
    expect(text.length).toBeLessThan(MAX_TOOL_RESULT_CHARS + 200);
    expect(text).toContain("[truncated");
  });
});

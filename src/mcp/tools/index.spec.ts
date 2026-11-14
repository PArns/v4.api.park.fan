import { z } from "zod";
import { MCP_TOOLS } from "./index";

/**
 * The registry is what a model reads to choose a tool, so its shape is a
 * contract: unique snake_case names, a description long enough to choose by,
 * and every argument described and bounded.
 */
describe("MCP_TOOLS", () => {
  it("has unique snake_case names", () => {
    const names = MCP_TOOLS.map((tool) => tool.name);
    expect(new Set(names).size).toBe(names.length);
    for (const name of names) expect(name).toMatch(/^[a-z][a-z0-9_]{2,47}$/);
  });

  it.each(MCP_TOOLS.map((tool) => [tool.name, tool]))(
    "%s describes itself and every argument",
    (_name, tool) => {
      expect(tool.title.length).toBeGreaterThan(2);
      expect(tool.description.length).toBeGreaterThan(80);
      const undescribed = Object.entries(tool.input)
        .filter(([, schema]) => !schema.description)
        .map(([field]) => field);
      expect(undescribed).toEqual([]);
    },
  );

  it.each(MCP_TOOLS.map((tool) => [tool.name, tool]))(
    "%s accepts its defaults and turns into a JSON schema",
    (_name, tool) => {
      const schema = z.object(tool.input);
      expect(() => z.toJSONSchema(schema)).not.toThrow();
    },
  );

  it("opens every read to a viewer and every write only to an editor's write token", () => {
    for (const tool of MCP_TOOLS) {
      if (tool.writes) expect(tool.minRole).toBe("editor");
      else expect(tool.minRole ?? "viewer").toBe("viewer");
    }
  });

  it("makes every delete name its confirm", () => {
    for (const tool of MCP_TOOLS.filter((t) => t.writes === "delete")) {
      expect(tool.input.confirm).toBeDefined();
    }
  });
});

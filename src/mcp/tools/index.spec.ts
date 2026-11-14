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

  it("opens a read to a viewer and a write to an editor, or both to an owner where the admin does", () => {
    for (const tool of MCP_TOOLS) {
      const role = tool.minRole ?? "viewer";
      if (tool.writes) expect(["editor", "owner"]).toContain(role);
      else expect(["viewer", "owner"]).toContain(role);
    }
  });

  it("makes every delete name its confirm or show a dry run first", () => {
    for (const tool of MCP_TOOLS.filter((t) => t.writes === "delete")) {
      const dryRun = tool.input.dryRun?.safeParse(undefined).data;
      expect({
        tool: tool.name,
        guarded: Boolean(tool.input.confirm) || dryRun === true,
      }).toEqual({
        tool: tool.name,
        guarded: true,
      });
    }
  });
});

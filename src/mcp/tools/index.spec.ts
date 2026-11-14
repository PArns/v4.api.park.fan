import { z } from "zod";
import { MCP_TOOLS } from "./index";
import { roleAtLeast } from "../../admin/auth/entities/admin-user.entity";

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

  it("asks editor or above of every tool that writes", () => {
    const below = MCP_TOOLS.filter(
      (tool) => tool.writes && !roleAtLeast(tool.minRole ?? "viewer", "editor"),
    ).map((tool) => tool.name);
    expect(below).toEqual([]);
  });

  it("shows a delete before it runs it, or has it confirmed", () => {
    const unguarded = MCP_TOOLS.filter((tool) => tool.writes === "delete")
      .filter((tool) => {
        const dryRun = tool.input.dryRun;
        return (
          !(dryRun && dryRun.safeParse(undefined).data === true) &&
          !tool.input.confirm
        );
      })
      .map((tool) => tool.name);
    expect(unguarded).toEqual([]);
  });
});

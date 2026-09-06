import { readdirSync, readFileSync } from "fs";
import { join } from "path";

/**
 * Every provider a module exports must also be declared by it.
 *
 * Nest checks this at boot and nowhere else, which is exactly why it reached
 * production once: `npm run build` type-checks files rather than the module
 * graph, and the unit suite constructs services directly instead of booting an
 * application. `DowntimeRecoveryService` landed in `exports` and not in
 * `providers`, Nest threw `UnknownExportException` in the dependency scanner,
 * and the container sat in a restart loop until it was pushed again.
 *
 * A full `Test.createTestingModule({ imports: [AppModule] })` would catch more,
 * but it needs Postgres and Redis, so it cannot run in the dev container (see
 * `docs/development/full-db-validation-checklist.md` for why that matters here).
 * This reads the decorators instead: no database, no boot, and it catches the
 * one mistake that has actually happened.
 */
describe("module graph", () => {
  const files = collectModules(join(__dirname, ".."));

  it("finds the modules", () => {
    expect(files.length).toBeGreaterThan(5);
  });

  it("exports only what each module provides or imports", () => {
    const problems: string[] = [];

    for (const file of files) {
      const source = readFileSync(file, "utf8");

      const entries = (key: string): string[] => {
        const match = new RegExp(
          `\\n  ${key}:\\s*\\[([\\s\\S]*?)\\n  \\]`,
        ).exec(source);
        if (!match) return [];
        return (
          match[1]
            .split("\n")
            // Strip a trailing comment before the comma, so
            // `TypeOrmModule, // CRITICAL: …` reads as `TypeOrmModule`.
            .map((line) =>
              line
                .replace(/\/\/.*$/, "")
                .trim()
                .replace(/,$/, ""),
            )
            .filter((line) => line.length > 0)
        );
      };

      const providers = new Set(entries("providers"));
      const imports = entries("imports");

      for (const exported of entries("exports")) {
        if (providers.has(exported)) continue;
        // A re-exported module is legitimate, and so is anything the module
        // imports — `TypeOrmModule.forFeature([...])` included.
        if (exported.endsWith("Module")) continue;
        if (imports.some((imported) => imported.includes(exported))) continue;
        problems.push(`${file.replace(/.*\/src\//, "src/")}: ${exported}`);
      }
    }

    expect(problems).toEqual([]);
  });
});

/** Every `*.module.ts` under a directory, without pulling in a glob dependency. */
function collectModules(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...collectModules(full));
    else if (entry.name.endsWith(".module.ts")) out.push(full);
  }
  return out;
}

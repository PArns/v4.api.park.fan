import { readdirSync, readFileSync } from "fs";
import { join } from "path";

/**
 * A backtick inside a SQL template literal ends the literal.
 *
 * It reads as a parse error a hundred lines away from the cause, and it is very
 * easy to write, because these files are full of prose comments about SQL and
 * the natural way to name a column in prose is `like_this`. It happened three
 * times while writing `closure-gap.sql.ts`.
 *
 * The build catches it, so this spec is not about safety — it is about pointing
 * at the actual line, in a file whose comment lines are the only place it can
 * occur.
 */
describe("SQL template literals", () => {
  const files = collect(join(__dirname, "..", ".."));

  it("finds the SQL modules", () => {
    expect(files.length).toBeGreaterThan(3);
  });

  it("has no backtick inside a SQL comment line", () => {
    const offenders: string[] = [];
    for (const file of files) {
      readFileSync(file, "utf8")
        .split("\n")
        .forEach((line, i) => {
          if (line.trim().startsWith("--") && line.includes("`")) {
            offenders.push(`${file.replace(/.*\/src\//, "src/")}:${i + 1}`);
          }
        });
    }
    expect(offenders).toEqual([]);
  });
});

/** Every `*.sql.ts` under a directory. */
function collect(dir: string): string[] {
  const out: string[] = [];
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const full = join(dir, entry.name);
    if (entry.isDirectory()) out.push(...collect(full));
    else if (entry.name.endsWith(".sql.ts")) out.push(full);
  }
  return out;
}

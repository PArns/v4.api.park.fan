import { existsSync, readdirSync, readFileSync } from "fs";
import { join, resolve } from "path";

import {
  CHANGELOG_FILE,
  FRAGMENT_DIR,
  UNRELEASED_HEADING,
  checkFragment,
  isFragmentCandidate,
  parseFragments,
  spliceIntoChangelog,
} from "./changelog-fragments.util";

const ROOT = resolve(__dirname, "..", "..", "..");

const GOOD = `### Fixed — a park merge keeps the losing park's schedule

One sentence of prose, because an entry without one says nothing.
`;

describe("checkFragment", () => {
  it("accepts a fragment with a typed heading and a body", () => {
    expect(checkFragment("PAR-257.md", GOOD)).toBeNull();
  });

  it("accepts every type the changelog uses", () => {
    for (const type of ["Added", "Changed", "Fixed", "Removed", "Security"]) {
      expect(checkFragment("PAR-1.md", `### ${type} — t\n\nbody\n`)).toBeNull();
    }
  });

  it("rejects a file name the merge cannot key on", () => {
    expect(checkFragment("par-257.md", GOOD)).toMatch(/file name/);
    expect(checkFragment("PAR-257.markdown", GOOD)).toMatch(/file name/);
    expect(checkFragment("notes.md", GOOD)).toMatch(/file name/);
    expect(checkFragment("PAR-257.md.bak", GOOD)).toMatch(/file name/);
  });

  it("rejects a leading zero, so two files cannot mean one issue", () => {
    expect(checkFragment("PAR-00257.md", GOOD)).toMatch(/file name/);
    expect(checkFragment("PAR-0.md", GOOD)).toMatch(/file name/);
  });

  it("allows a `## ` line inside a fenced block", () => {
    const quoting = [
      "### Documented — where an entry goes",
      "",
      "```markdown",
      "## [Unreleased]",
      "```",
      "",
      "and the prose after it.",
      "",
    ].join("\n");
    expect(checkFragment("PAR-1.md", quoting)).toBeNull();
  });

  it("rejects a heading the changelog's structure does not have", () => {
    expect(checkFragment("PAR-1.md", "### Broken — t\n\nbody\n")).toMatch(
      /first line/,
    );
    expect(checkFragment("PAR-1.md", "## Added — t\n\nbody\n")).toMatch(
      /first line/,
    );
    expect(checkFragment("PAR-1.md", "### Added - t\n\nbody\n")).toMatch(
      /first line/,
    );
    expect(checkFragment("PAR-1.md", "### Added — \n\nbody\n")).toMatch(
      /first line/,
    );
  });

  it("rejects an entry with nothing under its heading", () => {
    expect(checkFragment("PAR-1.md", "### Added — t\n\n  \n")).toBe(
      "entry has a heading and no body",
    );
  });

  it("rejects an empty file", () => {
    expect(checkFragment("PAR-1.md", "\n\n")).toBe("file is empty");
  });

  it("rejects a fragment that would cut [Unreleased] in half", () => {
    const problem = checkFragment(
      "PAR-1.md",
      "### Added — t\n\nbody\n\n## [4.7.0]\n",
    );
    expect(problem).toMatch(/may not open a section \(line 5\)/);
  });

  it("does not mind leading blank lines before the heading", () => {
    expect(checkFragment("PAR-1.md", `\n\n${GOOD}`)).toBeNull();
  });
});

describe("isFragmentCandidate", () => {
  it("skips the README that explains the directory, and tool dotfiles", () => {
    expect(isFragmentCandidate("README.md")).toBe(false);
    expect(isFragmentCandidate(".gitkeep")).toBe(false);
    expect(isFragmentCandidate("PAR-257.md")).toBe(true);
  });
});

/**
 * The filter and the check, in the order the CLI runs them over a directory
 * listing. Testing `checkFragment` alone says nothing about a name the filter
 * drops first — and a dropped name is the worst outcome this directory has,
 * because the entry is committed, `check` reports nothing wrong, and the
 * release loses it without a word.
 */
describe("a directory listing, filtered and then checked", () => {
  const listing = (files: string[]) =>
    parseFragments(
      files
        .filter(isFragmentCandidate)
        .map((file) => ({ file, content: GOOD })),
    );

  it("says something about every name that is not an entry", () => {
    const { fragments, problems } = listing([
      "README.md",
      ".gitkeep",
      "PAR-257.md",
      "PAR-258.markdown",
      "PAR-259.MD",
      "PAR-260",
      "PAR-261.md.bak",
      "notes.txt",
    ]);

    expect(fragments.map((f) => f.file)).toEqual(["PAR-257.md"]);
    expect(problems.map((p) => p.file).sort()).toEqual([
      "PAR-258.markdown",
      "PAR-259.MD",
      "PAR-260",
      "PAR-261.md.bak",
      "notes.txt",
    ]);
  });

  it("is silent only about the two names that are not entries on purpose", () => {
    const { fragments, problems } = listing(["README.md", ".DS_Store"]);
    expect(fragments).toEqual([]);
    expect(problems).toEqual([]);
  });
});

describe("parseFragments", () => {
  it("orders by issue number, newest first", () => {
    const { fragments, problems } = parseFragments([
      { file: "PAR-9.md", content: "### Added — nine\n\nbody\n" },
      { file: "PAR-257.md", content: "### Fixed — two five seven\n\nbody\n" },
      { file: "PAR-88.md", content: "### Changed — eighty-eight\n\nbody\n" },
    ]);

    expect(problems).toEqual([]);
    expect(fragments.map((f) => f.issue)).toEqual([257, 88, 9]);
  });

  it("separates the unusable ones instead of dropping them", () => {
    const { fragments, problems } = parseFragments([
      { file: "PAR-257.md", content: GOOD },
      { file: "PAR-258.md", content: "### Nope — t\n\nbody\n" },
    ]);

    expect(fragments.map((f) => f.file)).toEqual(["PAR-257.md"]);
    expect(problems).toEqual([
      { file: "PAR-258.md", problem: expect.stringMatching(/first line/) },
    ]);
  });
});

describe("spliceIntoChangelog", () => {
  const tail = [
    "### Added — quiet hours",
    "",
    "prose about quiet hours",
    "",
    "## [4.6.2] – 2026-02-08",
    "",
    "older",
    "",
  ].join("\n");
  const changelog = `# Changelog\n\n---\n\n${UNRELEASED_HEADING}\n\n${tail}`;

  it("puts the fragments above the entries that are already there", () => {
    const { fragments } = parseFragments([
      { file: "PAR-257.md", content: "### Fixed — newer\n\nbody\n" },
      { file: "PAR-88.md", content: "### Added — older fragment\n\nbody\n" },
    ]);

    const merged = spliceIntoChangelog(changelog, fragments);

    expect(merged).toBe(
      `# Changelog\n\n---\n\n${UNRELEASED_HEADING}\n\n` +
        "### Fixed — newer\n\nbody\n\n" +
        "### Added — older fragment\n\nbody\n\n" +
        tail,
    );
  });

  it("leaves everything below the heading byte-for-byte", () => {
    const { fragments } = parseFragments([
      { file: "PAR-1.md", content: "### Fixed — one\n\nbody\n" },
    ]);

    const merged = spliceIntoChangelog(changelog, fragments);

    expect(merged.endsWith(`\n\n${tail}`)).toBe(true);
    expect(merged.slice(merged.indexOf(tail))).toBe(tail);
  });

  it("returns the file untouched when there is nothing to fold in", () => {
    expect(spliceIntoChangelog(changelog, [])).toBe(changelog);
  });

  it("refuses a changelog without the heading rather than guessing", () => {
    expect(() =>
      spliceIntoChangelog("# Changelog\n\n## [4.6.2]\n", [
        { file: "PAR-1.md", issue: 1, body: "### Fixed — one\n\nbody" },
      ]),
    ).toThrow(/Unreleased/);
  });
});

/**
 * The half that measures the repository rather than a fixture. After a release
 * merge the directory is empty and these two say nothing about fragments — the
 * fixtures above are the population that is never empty (📚 G-72). What they do
 * guard is the directory and the rule pointing at it, which is what a revert of
 * this change would quietly remove.
 */
describe("the repository's own changelog directory", () => {
  const dir = join(ROOT, FRAGMENT_DIR);

  it("exists and explains itself", () => {
    expect(existsSync(dir)).toBe(true);
    expect(existsSync(join(dir, "README.md"))).toBe(true);
  });

  it("holds only fragments the merge can read", () => {
    const inputs = readdirSync(dir)
      .filter(isFragmentCandidate)
      .map((file) => ({
        file,
        content: readFileSync(join(dir, file), "utf8"),
      }));

    expect(parseFragments(inputs).problems).toEqual([]);
  });

  it("is where claude.md sends a new entry", () => {
    const rule = readFileSync(join(ROOT, "claude.md"), "utf8");
    expect(rule).toContain(FRAGMENT_DIR);
  });

  it("still has a changelog with an [Unreleased] heading to fold into", () => {
    const changelog = readFileSync(join(ROOT, CHANGELOG_FILE), "utf8");
    expect(changelog).toContain(`\n${UNRELEASED_HEADING}\n`);
  });
});

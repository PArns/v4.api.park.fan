import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "fs";
import { tmpdir } from "os";
import { join, resolve } from "path";

import {
  CHANGELOG_FILE,
  FRAGMENT_DIR,
  UNRELEASED_HEADING,
  checkFragment,
  hasUnreleasedHeading,
  isFragmentCandidate,
  parseFragments,
  readFragmentDirectory,
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

  it("rejects a `# ` heading too, which splits the file one level higher", () => {
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\nbody\n\n# Changelog\n"),
    ).toMatch(/may not open a section \(line 5\)/);
  });

  it("rejects an indented heading, which CommonMark still reads as one", () => {
    for (const indent of [" ", "  ", "   "]) {
      expect(
        checkFragment(
          "PAR-1.md",
          `### Added — t\n\nbody\n\n${indent}## [4.7.0]\n`,
        ),
      ).toMatch(/may not open a section \(line 5\)/);
    }
    // Four spaces is a code block, not a heading, and splits nothing.
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\nbody\n\n    ## [4.7.0]\n"),
    ).toBeNull();
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

  it("still sees a section heading after the fence has closed", () => {
    const after = [
      "### Documented — where an entry goes",
      "",
      "```markdown",
      "## [Unreleased]",
      "```",
      "",
      "## [4.7.0]",
      "",
    ].join("\n");
    expect(checkFragment("PAR-1.md", after)).toMatch(
      /may not open a section \(line 7\)/,
    );
  });

  it("rejects a setext heading, which is an h1/h2 with another syntax", () => {
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\nNext release\n---\nmore\n"),
    ).toMatch(/may not open a section \(line 3\)/);
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\nNext release\n===\nmore\n"),
    ).toMatch(/may not open a section \(line 3\)/);
  });

  it("allows the `---` shapes that are not headings", () => {
    // A blank line above makes it a thematic break, which splits nothing.
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\nbody\n\n---\n\nmore\n"),
    ).toBeNull();
    // A table delimiter row and a list are not paragraphs either.
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\n| a |\n| --- |\n| b |\n"),
    ).toBeNull();
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\n- item\n---\n"),
    ).toBeNull();
  });

  it("does not take an indented fence for a fence", () => {
    // Four spaces in it is an indented code block. Toggling on it left the
    // fence state open and rejected a perfectly good entry, which stops the
    // release for every other fragment too.
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\nbody\n\n    ```\n    x\n"),
    ).toBeNull();
  });

  it("reports a fence that is never closed instead of going blind after it", () => {
    // Without this the fence swallows the rest of the file and the `## ` below
    // it leaves the check altogether.
    expect(
      checkFragment("PAR-1.md", "### Added — t\n\n```\nx\n\n## [4.7.0]\n"),
    ).toBe("a code fence is never closed");
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

  it("collapses the blank run under the heading to one, and nothing else", () => {
    // The one normalisation the docstring claims. Pinned because "carries
    // everything below over as it stands" was too broad a sentence for it.
    const roomy = `# Changelog\n\n${UNRELEASED_HEADING}\n\n\n\n${tail}`;
    const { fragments } = parseFragments([
      { file: "PAR-1.md", content: "### Fixed — one\n\nbody\n" },
    ]);

    const merged = spliceIntoChangelog(roomy, fragments);

    expect(merged).toBe(
      `# Changelog\n\n${UNRELEASED_HEADING}\n\n### Fixed — one\n\nbody\n\n${tail}`,
    );
  });

  it("refuses a changelog without the heading rather than guessing", () => {
    expect(() =>
      spliceIntoChangelog("# Changelog\n\n## [4.6.2]\n", [
        { file: "PAR-1.md", issue: 1, body: "### Fixed — one\n\nbody" },
      ]),
    ).toThrow(/Unreleased/);
  });
});

describe("readFragmentDirectory", () => {
  let dir: string;

  beforeEach(() => {
    dir = mkdtempSync(join(tmpdir(), "changelog-d-"));
  });

  afterEach(() => {
    rmSync(dir, { recursive: true, force: true });
  });

  it("reports a directory instead of dying on EISDIR", () => {
    writeFileSync(join(dir, "PAR-257.md"), GOOD);
    mkdirSync(join(dir, "archive"));

    const { fragments, problems } = readFragmentDirectory(dir);

    expect(fragments.map((f) => f.file)).toEqual(["PAR-257.md"]);
    expect(problems).toEqual([{ file: "archive", problem: "not a file" }]);
  });

  it("treats a directory that is not there as an empty release", () => {
    expect(readFragmentDirectory(join(dir, "gone"))).toEqual({
      fragments: [],
      problems: [],
    });
  });

  it("reads the real names on disk, README and dotfiles aside", () => {
    writeFileSync(join(dir, "README.md"), "# explains the directory\n");
    writeFileSync(join(dir, ".gitkeep"), "");
    writeFileSync(join(dir, "PAR-9.md"), "### Added — nine\n\nbody\n");
    writeFileSync(join(dir, "PAR-88.markdown"), GOOD);

    const { fragments, problems } = readFragmentDirectory(dir);

    expect(fragments.map((f) => f.issue)).toEqual([9]);
    expect(problems).toEqual([
      { file: "PAR-88.markdown", problem: expect.stringMatching(/file name/) },
    ]);
  });
});

/**
 * The half that measures the repository rather than a fixture.
 *
 * Only the second of these four runs out of subject matter: after a release
 * merge the directory holds no fragments and it asserts over an empty set. The
 * fixtures above are the population that is never empty (📚 G-72). The other
 * three guard the three things a revert or a rename would take away without a
 * red test — the directory itself, the rule in `claude.md` with the two command
 * names it promises, and the heading the merge folds under.
 */
describe("the repository's own changelog directory", () => {
  const dir = join(ROOT, FRAGMENT_DIR);

  it("exists and explains itself", () => {
    expect(existsSync(dir)).toBe(true);
    expect(existsSync(join(dir, "README.md"))).toBe(true);
  });

  it("holds only fragments the merge can read", () => {
    // Through readFragmentDirectory, which is what the release runs — reading
    // the directory a second way here is how the two drifted apart once.
    expect(readFragmentDirectory(dir).problems).toEqual([]);
  });

  it("is where claude.md sends a new entry, by the names package.json has", () => {
    const rule = readFileSync(join(ROOT, "claude.md"), "utf8");
    const readme = readFileSync(join(dir, "README.md"), "utf8");
    const { scripts } = JSON.parse(
      readFileSync(join(ROOT, "package.json"), "utf8"),
    );

    expect(rule).toContain(FRAGMENT_DIR);
    // Both documents name both commands. Renaming a script without them is how
    // a rule starts lying over a green suite.
    for (const command of ["changelog:check", "changelog:merge"]) {
      expect(scripts[command]).toBeDefined();
      expect(rule).toContain(command);
      expect(readme).toContain(command);
    }
  });

  it("still has a changelog with an [Unreleased] heading to fold into", () => {
    const changelog = readFileSync(join(ROOT, CHANGELOG_FILE), "utf8");
    expect(hasUnreleasedHeading(changelog)).toBe(true);
  });
});

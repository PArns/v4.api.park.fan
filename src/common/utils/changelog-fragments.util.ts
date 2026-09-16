/**
 * One changelog entry per pull request, in its own file.
 *
 * Every entry used to be written directly under `## [Unreleased]` in
 * `docs/changelog.md`. Two pull requests open at the same time therefore always
 * touched the same line, and git conflicted there whatever their code did: on
 * 2026-09-15 and 2026-09-16 seven merges were sent back for a rebase and not one
 * of them had a conflict outside that file (PAR-257).
 *
 * A fragment sidesteps the collision by construction — two pull requests add two
 * files. `changelog-merge.ts` folds them back under `## [Unreleased]` at release
 * time, and the entries already in `docs/changelog.md` are copied through
 * byte-for-byte.
 *
 * The rules are pure functions over text. `readFragmentDirectory` is the one
 * that reads disk, and it lives here rather than in the CLI so the spec walks
 * the same directory listing the release does — the first version had the walk
 * in both and an entry that was a directory crashed one of them with `EISDIR`.
 */
import { readFileSync, readdirSync } from "fs";
import { join } from "path";

/** Where a pull request puts its entry, relative to the repository root. */
export const FRAGMENT_DIR = "docs/changelog.d";

/** The assembled changelog, relative to the repository root. */
export const CHANGELOG_FILE = "docs/changelog.md";

/** The heading the fragments are folded in under. */
export const UNRELEASED_HEADING = "## [Unreleased]";

/**
 * Keep a Changelog's six, plus the two this repository has settled on
 * (`Documented` and `Performance` both appear in `docs/changelog.md`).
 *
 * Deliberately narrower than the 161 entries already in the file, which also
 * hold one `Docs`, one `Weather`, one `ML` and one `Fixed/Changed` — one use
 * each, against 82 `Fixed` and 48 `Added`. A new entry uses `Documented`
 * rather than `Docs`; two spellings of one type is how the list stops meaning
 * anything.
 */
export const FRAGMENT_TYPES = [
  "Added",
  "Changed",
  "Deprecated",
  "Removed",
  "Fixed",
  "Security",
  "Documented",
  "Performance",
] as const;

/**
 * `PAR-257.md` — the issue number is the whole file name, which is what makes
 * two pull requests unable to write the same file.
 *
 * No leading zero: `PAR-257.md` and `PAR-00257.md` would otherwise both parse
 * as issue 257 and both be merged, which is the one way two files could still
 * mean one entry.
 */
export const FRAGMENT_FILENAME = /^PAR-([1-9]\d*)\.md$/;

/** The em dash is the separator every existing entry uses. */
const FRAGMENT_HEADING = new RegExp(
  `^### (?:${FRAGMENT_TYPES.join("|")}) — \\S.*$`,
);

export interface Fragment {
  /** File name inside {@link FRAGMENT_DIR}, e.g. `PAR-257.md`. */
  file: string;
  /** The issue number the file is named after. */
  issue: number;
  /** The entry itself, trimmed, starting at its `### ` heading. */
  body: string;
}

export interface FragmentProblem {
  file: string;
  problem: string;
}

export interface FragmentInput {
  file: string;
  content: string;
}

/**
 * Whether a directory entry is meant to be an entry at all.
 *
 * Deliberately almost everything: `README.md` explains the directory, and a
 * dotfile belongs to a tool rather than to a release. Every other name is
 * handed to {@link checkFragment}, **including the ones it will reject** —
 * `PAR-257.markdown`, `PAR-257.MD`, `PAR-257` and `PAR-257.md.bak` are the
 * typos somebody actually makes, and a filter that skipped them would turn the
 * one failure this directory exists to prevent into the quietest one there is:
 * the entry sits in git, `check` says nothing is wrong, and the release drops
 * it. A wrong name is loud here or it is invisible.
 */
export function isFragmentCandidate(file: string): boolean {
  return file !== "README.md" && !file.startsWith(".");
}

/**
 * Returns the reason this file is not a usable fragment, or `null` if it is.
 *
 * The failures are the ones that either lose the entry or break the assembled
 * file: a name the merge cannot key on, an empty file, a heading the
 * changelog's own structure does not have, an entry with nothing under its
 * heading, a fragment that opens a `# ` or `## ` section of its own and would
 * therefore cut `[Unreleased]` in half, and a code fence left open.
 */
export function checkFragment(file: string, content: string): string | null {
  if (!FRAGMENT_FILENAME.test(file)) {
    return `file name must be PAR-<issue>.md`;
  }

  const lines = content.split("\n");
  const headingAt = lines.findIndex((line) => line.trim() !== "");
  if (headingAt === -1) {
    return "file is empty";
  }
  if (!FRAGMENT_HEADING.test(lines[headingAt].trimEnd())) {
    return `first line must be "### <${FRAGMENT_TYPES.join("|")}> — <title>"`;
  }

  const rest = lines.slice(headingAt + 1);
  if (!rest.some((line) => line.trim() !== "")) {
    return "entry has a heading and no body";
  }

  const section = findSectionHeading(rest);
  if (section.at !== -1) {
    return `a fragment is one entry and may not open a section (line ${
      headingAt + section.at + 2
    })`;
  }
  if (section.unclosedFence) {
    return "a code fence is never closed";
  }

  return null;
}

/** An ATX heading of level 1 or 2. Up to three leading spaces is still one. */
const ATX_SECTION = /^ {0,3}#{1,2} /;

/** A fence opener or closer. Four spaces in is an indented code block, not one. */
const FENCE = /^ {0,3}(```|~~~)/;

/** The underline of a setext heading: `===` is an h1, `---` an h2. */
const SETEXT_UNDERLINE = /^ {0,3}(-{1,}|={1,})[ \t]*$/;

/**
 * A line that a setext underline can turn into a heading.
 *
 * Only a paragraph does that. Under a list item, a table row or a blockquote,
 * `---` closes the block and is a thematic break — treating those as headings
 * would reject a valid entry, and a rejected fragment stops the whole release.
 */
const SETEXT_SUBJECT = /^ {0,3}(?![-*+>|#]|\d+[.)]|```|~~~)\S/;

/**
 * The index of the first line that opens a section, or `-1`.
 *
 * "Section" is anything that ends up as an `<h1>` or `<h2>` in the assembled
 * file, because that is what cuts `[Unreleased]` in half — not one syntax for
 * it. Three shapes qualify: `# `/`## `, the same indented by up to three
 * spaces, and a setext underline under a paragraph. Level 1 counts as well as
 * level 2: it splits the file one level *above* `[Unreleased]`, which is worse
 * than the case the check was first written for.
 *
 * Fenced blocks are skipped, because an entry about the changelog's own
 * structure quotes a `## ` line inside one — the spec case `allows a '## ' line
 * inside a fenced block` is exactly that — and rejecting it would be a check
 * refusing the entry it exists to protect. An **unclosed** fence is reported
 * instead of skipped: it would otherwise swallow the rest of the file and take
 * every heading after it out of the check, which is how the first version of
 * this function let `### Added — t\n\n```\n\n## [4.7.0]` through.
 */
function findSectionHeading(lines: string[]): {
  at: number;
  unclosedFence: boolean;
} {
  let fenced = false;
  let found = -1;

  for (let i = 0; i < lines.length; i++) {
    if (FENCE.test(lines[i])) {
      fenced = !fenced;
      continue;
    }
    if (fenced || found !== -1) {
      continue;
    }
    if (ATX_SECTION.test(lines[i])) {
      found = i;
      continue;
    }
    if (
      SETEXT_UNDERLINE.test(lines[i]) &&
      i > 0 &&
      SETEXT_SUBJECT.test(lines[i - 1])
    ) {
      // The heading is the text line, not its underline.
      found = i - 1;
    }
  }

  return { at: found, unclosedFence: fenced };
}

/**
 * Parses a directory listing into fragments, newest first.
 *
 * "Newest" is the issue number descending. It is an approximation of when the
 * entry was written and not a merge log — which is exactly as true of the
 * hand-maintained order it replaces, where each pull request wrote itself to
 * the top before its own merge and the merges then landed in another order.
 */
export function parseFragments(inputs: FragmentInput[]): {
  fragments: Fragment[];
  problems: FragmentProblem[];
} {
  const fragments: Fragment[] = [];
  const problems: FragmentProblem[] = [];

  for (const { file, content } of inputs) {
    const problem = checkFragment(file, content);
    if (problem) {
      problems.push({ file, problem });
      continue;
    }
    const issue = Number(FRAGMENT_FILENAME.exec(file)![1]);
    fragments.push({ file, issue, body: content.trim() });
  }

  fragments.sort((a, b) => b.issue - a.issue);
  return { fragments, problems };
}

/**
 * Reads the fragment directory and parses what is in it.
 *
 * `withFileTypes`, because the filter deliberately lets every name through and
 * a directory handed to `readFileSync` throws `EISDIR` — a stack trace where
 * the point of this module is a line naming the file and what is wrong with it.
 * A missing directory is an empty release, not an error.
 */
export function readFragmentDirectory(dir: string): {
  fragments: Fragment[];
  problems: FragmentProblem[];
} {
  let entries;
  try {
    entries = readdirSync(dir, { withFileTypes: true });
  } catch (error) {
    // Only "there is no directory" is an empty release. A permission or I/O
    // error read as one would let the release run with no entries and say
    // nothing, which is the silent loss the rest of this module is against.
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { fragments: [], problems: [] };
    }
    throw error;
  }

  const inputs: FragmentInput[] = [];
  const problems: FragmentProblem[] = [];

  for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
    if (!isFragmentCandidate(entry.name)) {
      continue;
    }
    if (!entry.isFile()) {
      problems.push({ file: entry.name, problem: "not a file" });
      continue;
    }
    inputs.push({
      file: entry.name,
      content: readFileSync(join(dir, entry.name), "utf8"),
    });
  }

  const parsed = parseFragments(inputs);
  return {
    fragments: parsed.fragments,
    problems: [...problems, ...parsed.problems],
  };
}

/** The heading `spliceIntoChangelog` folds under, wherever it is asked about. */
const UNRELEASED_LINE = /^## \[Unreleased\][^\n]*$/m;

/**
 * Whether the changelog still has the heading the merge needs.
 *
 * One function rather than the same regular expression at each caller: the
 * merge, the `check` command and the spec all ask this, and the last time this
 * module stated a rule in two places the two answered differently.
 */
export function hasUnreleasedHeading(changelog: string): boolean {
  return UNRELEASED_LINE.test(changelog);
}

/**
 * Folds the fragments in under `## [Unreleased]` and returns the new file.
 *
 * The entries below the heading are carried over as they stand — this function
 * inserts text and never rewrites them, and 62 files under `docs/` are not
 * prettier-clean on `main` and are meant to stay that way. The one thing it
 * does normalise is the run of blank lines between the heading and the first
 * entry, which becomes exactly one.
 */
export function spliceIntoChangelog(
  changelog: string,
  fragments: Fragment[],
): string {
  if (fragments.length === 0) {
    return changelog;
  }

  const heading = UNRELEASED_LINE.exec(changelog);
  if (!heading) {
    throw new Error(
      `${CHANGELOG_FILE} has no "${UNRELEASED_HEADING}" heading to fold into`,
    );
  }

  const cut = heading.index + heading[0].length;
  const before = changelog.slice(0, cut);
  const after = changelog.slice(cut).replace(/^\n+/, "");
  const block = fragments.map((fragment) => fragment.body).join("\n\n");

  return after.length > 0
    ? `${before}\n\n${block}\n\n${after}`
    : `${before}\n\n${block}\n`;
}

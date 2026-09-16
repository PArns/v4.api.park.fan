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
 * files. `mergeFragments` folds them back under `## [Unreleased]` at release
 * time, and the entries already in `docs/changelog.md` are copied through
 * byte-for-byte.
 */

/** Where a pull request puts its entry, relative to the repository root. */
export const FRAGMENT_DIR = "docs/changelog.d";

/** The assembled changelog, relative to the repository root. */
export const CHANGELOG_FILE = "docs/changelog.md";

/** The heading the fragments are folded in under. */
export const UNRELEASED_HEADING = "## [Unreleased]";

/**
 * Keep a Changelog's six, plus the two this repository has been using for
 * years (`Documented` and `Performance` both appear in `docs/changelog.md`).
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
 * heading, and a fragment that opens a `## ` section of its own and would
 * therefore cut `[Unreleased]` in half.
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
  if (section !== -1) {
    return `a fragment is one entry and may not open a section (line ${
      headingAt + section + 2
    })`;
  }

  return null;
}

/**
 * The index of the first real `## ` heading, or `-1`.
 *
 * Fenced blocks are skipped, because an entry about the changelog's own
 * structure quotes `## [Unreleased]` inside one — `docs/changelog.d/README.md`
 * is written exactly that way — and rejecting it would be a check refusing the
 * entry it was written to protect.
 */
function findSectionHeading(lines: string[]): number {
  let fenced = false;
  for (let i = 0; i < lines.length; i++) {
    if (/^\s*(```|~~~)/.test(lines[i])) {
      fenced = !fenced;
      continue;
    }
    if (!fenced && /^## /.test(lines[i])) {
      return i;
    }
  }
  return -1;
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
 * Folds the fragments in under `## [Unreleased]` and returns the new file.
 *
 * Everything below the heading is carried over as it stands — this function
 * inserts text and never rewrites the existing entries, of which 62 files under
 * `docs/` are not prettier-clean on `main` and are meant to stay that way.
 */
export function spliceIntoChangelog(
  changelog: string,
  fragments: Fragment[],
): string {
  if (fragments.length === 0) {
    return changelog;
  }

  const heading = /^## \[Unreleased\][^\n]*$/m.exec(changelog);
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
